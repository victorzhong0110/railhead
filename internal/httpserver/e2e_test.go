package httpserver_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorzhong0110/railhead/internal/billing"
	"github.com/victorzhong0110/railhead/internal/breaker"
	"github.com/victorzhong0110/railhead/internal/cache"
	"github.com/victorzhong0110/railhead/internal/config"
	"github.com/victorzhong0110/railhead/internal/dispatch"
	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/httpserver"
	"github.com/victorzhong0110/railhead/internal/metrics"
	"github.com/victorzhong0110/railhead/internal/provider/mock"
	"github.com/victorzhong0110/railhead/internal/provider/openai"
	"github.com/victorzhong0110/railhead/internal/ratelimit"
	"github.com/victorzhong0110/railhead/internal/router"
	"github.com/victorzhong0110/railhead/internal/store"
	"github.com/victorzhong0110/railhead/internal/testutil"
)

func TestGatewayLifecycle(t *testing.T) {
	pool := testutil.Pool(t)
	primaryEngine := mock.New(mock.Options{DefaultCompletion: 8})
	backupEngine := mock.New(mock.Options{DefaultCompletion: 8})
	primary := httptest.NewServer(mock.Handler(primaryEngine))
	backup := httptest.NewServer(mock.Handler(backupEngine))
	t.Cleanup(primary.Close)
	t.Cleanup(backup.Close)

	rt := router.New(
		&dispatch.Dispatcher{OpenAI: openai.New(), Mock: mock.New(mock.Options{})},
		breaker.NewGroup(breaker.Options{Threshold: 2, Cooldown: time.Hour}),
		metrics.New(),
		3,
		time.Millisecond,
	)
	rt.SetChannels([]domain.Channel{
		{Name: "primary", Kind: "openai", BaseURL: primary.URL + "/v1", APIKey: "mock", Models: []string{"railhead-mock"}, Priority: 10, Weight: 1, Timeout: 2 * time.Second, Enabled: true},
		{Name: "backup", Kind: "openai", BaseURL: backup.URL + "/v1", APIKey: "mock", Models: []string{"railhead-mock"}, Priority: 1, Weight: 1, Timeout: 2 * time.Second, Enabled: true},
	})
	gw := httptest.NewServer(httpserver.New(httpserver.Deps{
		Config: config.Config{
			AdminToken:       "test-admin",
			CacheEnabled:     true,
			CacheTTL:         time.Minute,
			KeyCacheTTL:      time.Second,
			DefaultMaxTokens: 8,
			MaxTokensCap:     128,
			ReserveMargin:    0,
			RequestTimeout:   5 * time.Second,
		},
		Store:   store.New(pool),
		Billing: billing.New(pool),
		Limiter: ratelimit.New(nil),
		Conc:    ratelimit.NewConcurrency(nil),
		Router:  rt,
		Cache:   cache.NewMemory(),
		Metrics: metrics.New(),
	}).Handler())
	t.Cleanup(gw.Close)

	t.Run("rejects missing key", func(t *testing.T) {
		res, _ := postChat(t, gw.URL, "", chatBody(false, nil))
		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status %d", res.StatusCode)
		}
	})

	raw, id := createKey(t, gw.URL, "life", 100_000, 0, 0, 0)
	zero := 0.0
	res, body := postChat(t, gw.URL, raw, chatBody(false, &zero))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	if res.Header.Get("X-Railhead-Channel") != "primary" {
		t.Fatalf("channel %s", res.Header.Get("X-Railhead-Channel"))
	}
	if res.Header.Get("X-Railhead-Cache") != "MISS" {
		t.Fatalf("cache %s", res.Header.Get("X-Railhead-Cache"))
	}
	var completion domain.ChatResponse
	if err := json.Unmarshal(body, &completion); err != nil {
		t.Fatal(err)
	}
	if completion.Usage.TotalTokens <= 0 {
		t.Fatalf("usage %+v", completion.Usage)
	}
	rec := reconcile(t, gw.URL, id)
	if rec.Drift != 0 || rec.QuotaBalance < 0 || rec.Settled != int64(completion.Usage.TotalTokens) {
		t.Fatalf("reconcile %+v usage %d", rec, completion.Usage.TotalTokens)
	}

	res, body = postChat(t, gw.URL, raw, chatBody(false, &zero))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("cached status %d %s", res.StatusCode, body)
	}
	if res.Header.Get("X-Railhead-Cache") != "HIT" {
		t.Fatalf("expected cache hit, got %s", res.Header.Get("X-Railhead-Cache"))
	}
	rec2 := reconcile(t, gw.URL, id)
	if rec2.QuotaBalance != rec.QuotaBalance || rec2.Settled != rec.Settled || rec2.Drift != 0 {
		t.Fatalf("cache hit changed billing: before %+v after %+v", rec, rec2)
	}

	poor, _ := createKey(t, gw.URL, "poor", 1, 0, 0, 0)
	res, _ = postChat(t, gw.URL, poor, chatBody(false, nil))
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("quota status %d", res.StatusCode)
	}

	limited, limitedID := createKey(t, gw.URL, "rpm", 100_000, 1, 0, 0)
	res, _ = postChat(t, gw.URL, limited, chatBody(false, nil))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("first rpm status %d", res.StatusCode)
	}
	res, body = postChat(t, gw.URL, limited, chatBody(false, nil))
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second rpm status %d body %s", res.StatusCode, body)
	}
	if rec := reconcile(t, gw.URL, limitedID); rec.Drift != 0 || rec.QuotaBalance < 0 {
		t.Fatalf("rpm reconcile %+v", rec)
	}

	res, body = postChat(t, gw.URL, raw, chatBody(true, nil))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d %s", res.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("data: [DONE]")) || !bytes.Contains(body, []byte(`"usage"`)) {
		t.Fatalf("stream body %s", body)
	}
	if rec := reconcile(t, gw.URL, id); rec.Drift != 0 || rec.Inflight != 0 {
		t.Fatalf("after stream %+v", rec)
	}
}

func TestFailoverAndConcurrency(t *testing.T) {
	pool := testutil.Pool(t)
	primaryEngine := mock.New(mock.Options{ErrorRate: 1, DefaultCompletion: 4})
	backupEngine := mock.New(mock.Options{Latency: 200 * time.Millisecond, DefaultCompletion: 4})
	primary := httptest.NewServer(mock.Handler(primaryEngine))
	backup := httptest.NewServer(mock.Handler(backupEngine))
	t.Cleanup(primary.Close)
	t.Cleanup(backup.Close)

	rt := router.New(
		&dispatch.Dispatcher{OpenAI: openai.New(), Mock: mock.New(mock.Options{})},
		breaker.NewGroup(breaker.Options{Threshold: 5, Cooldown: time.Hour}),
		nil,
		3,
		time.Millisecond,
	)
	rt.SetChannels([]domain.Channel{
		{Name: "primary", Kind: "openai", BaseURL: primary.URL + "/v1", Models: []string{"railhead-mock"}, Priority: 10, Weight: 1, Timeout: 2 * time.Second, Enabled: true},
		{Name: "backup", Kind: "openai", BaseURL: backup.URL + "/v1", Models: []string{"railhead-mock"}, Priority: 1, Weight: 1, Timeout: 2 * time.Second, Enabled: true},
	})
	gw := httptest.NewServer(httpserver.New(httpserver.Deps{
		Config: config.Config{
			AdminToken: "test-admin", CacheEnabled: false, KeyCacheTTL: time.Second,
			DefaultMaxTokens: 4, MaxTokensCap: 64, RequestTimeout: 5 * time.Second,
		},
		Store:   store.New(pool),
		Billing: billing.New(pool),
		Limiter: ratelimit.New(nil),
		Conc:    ratelimit.NewConcurrency(nil),
		Router:  rt,
		Cache:   cache.NewMemory(),
	}).Handler())
	t.Cleanup(gw.Close)

	raw, id := createKey(t, gw.URL, "fail", 100_000, 0, 0, 1)
	var wg sync.WaitGroup
	start := make(chan struct{})
	codes := make([]int, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			res, _ := postChat(t, gw.URL, raw, chatBody(false, nil))
			codes[i] = res.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()
	okN, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			okN++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("unexpected status %d", c)
		}
	}
	if okN != 1 || limited != 1 {
		t.Fatalf("codes %v", codes)
	}
	// 并发位释放后，故障转移应当打到 backup，并且只扣一次。
	res, body := postChat(t, gw.URL, raw, chatBody(false, nil))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("failover status %d %s", res.StatusCode, body)
	}
	if res.Header.Get("X-Railhead-Channel") != "backup" {
		t.Fatalf("channel %s", res.Header.Get("X-Railhead-Channel"))
	}
	rec := reconcile(t, gw.URL, id)
	if rec.Drift != 0 || rec.QuotaBalance < 0 || rec.Inflight != 0 || rec.Unbilled != 0 {
		t.Fatalf("failover reconcile %+v", rec)
	}
}

// 下面的辅助函数把测试数据和 HTTP 细节收拢，避免每个子测试重复拼 JSON。

func chatBody(stream bool, temperature *float64) string {
	temp := "null"
	if temperature != nil {
		temp = fmt.Sprintf("%g", *temperature)
	}
	return fmt.Sprintf(`{"model":"railhead-mock","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"max_tokens":8,"temperature":%s,"stream":%t}`, temp, stream)
}

func postChat(t *testing.T, base, key, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, b
}

func createKey(t *testing.T, base, name string, quota int64, rpm, tpm, conc int) (string, int64) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"name": name, "quota": quota, "rpm_limit": rpm, "tpm_limit": tpm, "concurrency_limit": conc,
	})
	req, err := http.NewRequest(http.MethodPost, base+"/admin/keys", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", "test-admin")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create key %d %s", res.StatusCode, b)
	}
	var out struct {
		Key  string `json:"key"`
		Item struct {
			ID int64 `json:"id"`
		} `json:"item"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out.Key, out.Item.ID
}

func reconcile(t *testing.T, base string, id int64) domain.ReconcileResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/admin/keys/%d/reconcile", base, id), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Admin-Token", "test-admin")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var rec domain.ReconcileResult
	if err := json.NewDecoder(bufio.NewReader(res.Body)).Decode(&rec); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("reconcile status %d %+v", res.StatusCode, rec)
	}
	return rec
}
