// Command benchcheck 在已经跑起来的网关上做两件压测脚本不方便精确完成的事：
// 并发扣费之后对账，以及把主渠道错误率打到 100% 之后观察故障转移。
// 结果打到 stdout，是一行 JSON。退出码不是 0 表示不变量被打破。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: benchcheck billing|failover")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "billing":
		os.Exit(runBilling(os.Args[2:]))
	case "failover":
		os.Exit(runFailover(os.Args[2:]))
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(2)
	}
}

func runBilling(args []string) int {
	fs := flag.NewFlagSet("billing", flag.ExitOnError)
	gateway := fs.String("gateway", "http://127.0.0.1:18080", "gateway base URL")
	admin := fs.String("admin", "dev-admin-token", "admin token")
	model := fs.String("model", "railhead-mock", "model name")
	concurrency := fs.Int("concurrency", 50, "parallel workers")
	requests := fs.Int("requests", 200, "total requests")
	quota := fs.Int64("quota", 500, "quota granted to the fresh key")
	maxTokens := fs.Int("max-tokens", 8, "max_tokens")
	fs.Parse(args)

	raw, id, err := createKey(*gateway, *admin, "billing-check", *quota, 0, 0, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"billing check"}],"max_tokens":%d}`, *model, *maxTokens)
	codes := make([]int, *requests)
	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	start := time.Now()
	for i := 0; i < *requests; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			codes[i] = postStatus(*gateway, raw, body)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	rec, err := reconcile(*gateway, *admin, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	hist := map[string]int{}
	for _, c := range codes {
		hist[fmt.Sprintf("%d", c)]++
	}
	out := map[string]any{
		"mode":                 "billing",
		"requests":             *requests,
		"concurrency":          *concurrency,
		"quota_granted":        *quota,
		"elapsed_ms":           elapsed.Milliseconds(),
		"status_counts":        hist,
		"reconcile":            rec,
		"balance_non_negative": rec.QuotaBalance >= 0,
		"drift_zero":           rec.Drift == 0,
		"inflight_zero":        rec.Inflight == 0,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	if rec.Drift != 0 || rec.QuotaBalance < 0 || rec.Inflight != 0 {
		return 1
	}
	return 0
}

func runFailover(args []string) int {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	gateway := fs.String("gateway", "http://127.0.0.1:18080", "gateway base URL")
	admin := fs.String("admin", "dev-admin-token", "admin token")
	mockA := fs.String("mock-a", "http://127.0.0.1:18091", "primary mock base URL")
	model := fs.String("model", "railhead-mock", "model name")
	before := fs.Int("before", 20, "requests while primary is healthy")
	after := fs.Int("after", 40, "requests after primary error rate is 1")
	fs.Parse(args)

	raw, id, err := createKey(*gateway, *admin, "failover-check", 5_000_000, 0, 0, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := setFault(*mockA, 0, 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	// 给熔断器一点时间从上一轮实验里冷却。本命令自己的密钥是新的，渠道状态是进程级的。
	healthy := collect(*gateway, raw, *model, *before)
	if err := setFault(*mockA, 1, 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	failed := collect(*gateway, raw, *model, *after)
	// 故障注入结束后把主渠道恢复，避免影响后续场景。
	_ = setFault(*mockA, 0, 0)
	rec, err := reconcile(*gateway, *admin, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	out := map[string]any{
		"mode":         "failover",
		"healthy":      summarize(healthy),
		"primary_down": summarize(failed),
		"reconcile":    rec,
		"drift_zero":   rec.Drift == 0,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	if rec.Drift != 0 || rec.QuotaBalance < 0 {
		return 1
	}
	sum := summarize(failed)
	if sum.Successes == 0 {
		fmt.Fprintln(os.Stderr, "failover produced zero successes")
		return 1
	}
	return 0
}

type sample struct {
	status  int
	channel string
	latency time.Duration
}

type summary struct {
	Requests  int            `json:"requests"`
	Successes int            `json:"successes"`
	Errors    int            `json:"errors"`
	Channels  map[string]int `json:"channels"`
	P50MS     int64          `json:"p50_ms"`
	P95MS     int64          `json:"p95_ms"`
	P99MS     int64          `json:"p99_ms"`
}

func collect(gateway, key, model string, n int) []sample {
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"failover"}],"max_tokens":8}`, model)
	out := make([]sample, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		req, _ := http.NewRequest(http.MethodPost, gateway+"/v1/chat/completions", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := http.DefaultClient.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			out[i] = sample{status: 0, latency: elapsed}
			continue
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		out[i] = sample{status: res.StatusCode, channel: res.Header.Get("X-Railhead-Channel"), latency: elapsed}
	}
	return out
}

func summarize(samples []sample) summary {
	s := summary{Requests: len(samples), Channels: map[string]int{}}
	lat := make([]int64, 0, len(samples))
	for _, item := range samples {
		lat = append(lat, item.latency.Milliseconds())
		if item.status == http.StatusOK {
			s.Successes++
			name := item.channel
			if name == "" {
				name = "(none)"
			}
			s.Channels[name]++
		} else {
			s.Errors++
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	s.P50MS = percentile(lat, 0.50)
	s.P95MS = percentile(lat, 0.95)
	s.P99MS = percentile(lat, 0.99)
	return s
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

type reconcileResult struct {
	QuotaBalance  int64 `json:"quota_balance"`
	QuotaGranted  int64 `json:"quota_granted"`
	Settled       int64 `json:"settled"`
	Inflight      int64 `json:"inflight"`
	Unbilled      int64 `json:"unbilled"`
	Drift         int64 `json:"drift"`
	SettledCount  int64 `json:"settled_count"`
	RefundedCount int64 `json:"refunded_count"`
}

func createKey(gateway, admin, name string, quota int64, rpm, tpm, conc int) (string, int64, error) {
	payload, _ := json.Marshal(map[string]any{
		"name": name, "quota": quota, "rpm_limit": rpm, "tpm_limit": tpm, "concurrency_limit": conc,
	})
	req, err := http.NewRequest(http.MethodPost, gateway+"/admin/keys", bytes.NewReader(payload))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", admin)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusCreated {
		return "", 0, fmt.Errorf("create key: %d %s", res.StatusCode, b)
	}
	var out struct {
		Key  string `json:"key"`
		Item struct {
			ID int64 `json:"id"`
		} `json:"item"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "", 0, err
	}
	return out.Key, out.Item.ID, nil
}

func reconcile(gateway, admin string, id int64) (reconcileResult, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/admin/keys/%d/reconcile", gateway, id), nil)
	if err != nil {
		return reconcileResult{}, err
	}
	req.Header.Set("X-Admin-Token", admin)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return reconcileResult{}, err
	}
	defer res.Body.Close()
	var rec reconcileResult
	if err := json.NewDecoder(res.Body).Decode(&rec); err != nil {
		return reconcileResult{}, err
	}
	if res.StatusCode != http.StatusOK {
		return rec, fmt.Errorf("reconcile status %d", res.StatusCode)
	}
	return rec, nil
}

func setFault(base string, errorRate float64, latencyMS int) error {
	payload, _ := json.Marshal(map[string]any{"error_rate": errorRate, "latency_ms": latencyMS})
	res, err := http.Post(base+"/admin/fault", "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("set fault: %d %s", res.StatusCode, b)
	}
	return nil
}

func postStatus(gateway, key, body string) int {
	req, err := http.NewRequest(http.MethodPost, gateway+"/v1/chat/completions", bytes.NewReader([]byte(body)))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return res.StatusCode
}
