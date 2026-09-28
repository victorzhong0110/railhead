// Command replay 按公开 LLM trace 的到达时间和 token 长度回放请求。
// 它不制造匀速负载：每条记录按 TIMESTAMP 的相对间隔（可加速）发出。
// 密钥按 Zipf 分配。结果打到 stdout，是一份 JSON。
package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/tokens"
)

type traceRow struct {
	offset time.Duration
	ctx    int
	gen    int
}

type sample struct {
	at           time.Time
	status       int
	latency      time.Duration
	ttft         time.Duration
	upstreamUs   int64
	mockTTFTUs   int64
	stream       bool
	channel      string
	key          int
	okTokens     int
	rateLimited  bool
	upstreamFail bool
}

func main() {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	tracePath := fs.String("trace", "testdata/azure_llm_2024_conv_sample.csv", "CSV path")
	base := fs.String("gateway", "http://127.0.0.1:18083", "gateway or nginx base URL")
	admin := fs.String("admin", "dev-admin-token", "admin token")
	model := fs.String("model", "railhead-mock", "model")
	speed := fs.Float64("speed", 1, "time compression; 20 means 20x faster than the trace")
	from := fs.Duration("from", 0, "skip this much trace time from the first timestamp")
	dur := fs.Duration("duration", 2*time.Minute, "how much trace time to replay")
	wall := fs.Duration("wall", 0, "if > 0, repeat the window until this much wall clock has been scheduled")
	keysN := fs.Int("keys", 64, "number of API keys")
	zipfS := fs.Float64("zipf", 1.0, "Zipf exponent")
	heavy := fs.Int("heavy", 2, "how many lowest-rank keys get a tight RPM limit")
	heavyRPM := fs.Int("heavy-rpm", 60, "RPM limit for heavy keys")
	streamFrac := fs.Float64("stream-frac", 0.6, "fraction of requests that stream")
	inflight := fs.Int("inflight", 256, "max in-flight HTTP calls")
	quota := fs.Int64("quota", 20_000_000, "quota per key")
	progress := fs.String("progress", "", "optional JSONL path of 1-second buckets")
	eventsPath := fs.String("events", "", "optional JSONL of every request")
	fs.Parse(os.Args[1:])

	if *speed <= 0 {
		fmt.Fprintln(os.Stderr, "speed must be positive")
		os.Exit(2)
	}
	rows, err := loadTrace(*tracePath, *from, *dur)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(rows) == 0 {
		fmt.Fprintln(os.Stderr, "trace window is empty")
		os.Exit(1)
	}
	client := &http.Client{Timeout: 45 * time.Second, Transport: &http.Transport{
		MaxIdleConns: 512, MaxIdleConnsPerHost: 512, IdleConnTimeout: 30 * time.Second,
	}}
	keys, ids, err := createKeys(client, *base, *admin, *keysN, *heavy, *heavyRPM, *quota)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cdf := zipfCDF(*keysN, *zipfS)

	copies := 1
	window := rows[len(rows)-1].offset
	if window <= 0 {
		window = time.Millisecond
	}
	windowWall := time.Duration(float64(window) / *speed)
	if *wall > 0 && windowWall > 0 {
		copies = int(*wall/windowWall) + 1
	}

	var mu sync.Mutex
	samples := make([]sample, 0, len(rows)*copies)
	var wg sync.WaitGroup
	sem := make(chan struct{}, *inflight)
	schedStart := time.Now()
	var progressStop chan struct{}
	if *progress != "" {
		progressStop = make(chan struct{})
		go writeProgress(*progress, schedStart, &mu, &samples, progressStop)
	}

	for copy := 0; copy < copies; copy++ {
		baseOff := time.Duration(copy) * windowWall
		for i, row := range rows {
			at := baseOff + time.Duration(float64(row.offset) / *speed)
			keyIdx := pickZipf(cdf, hash32(copy, i))
			stream := hash32(i, copy+17)%1000 < uint32(*streamFrac*1000)
			waitUntil(schedStart.Add(at))
			sem <- struct{}{}
			wg.Add(1)
			go func(row traceRow, keyIdx int, stream bool) {
				defer wg.Done()
				defer func() { <-sem }()
				sm := doOne(client, *base, keys[keyIdx], *model, row, stream, keyIdx)
				mu.Lock()
				samples = append(samples, sm)
				mu.Unlock()
			}(row, keyIdx, stream)
		}
	}
	wg.Wait()
	if progressStop != nil {
		close(progressStop)
		time.Sleep(50 * time.Millisecond)
	}
	elapsed := time.Since(schedStart)
	if *eventsPath != "" {
		if err := dumpEvents(*eventsPath, schedStart, samples); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	recs := make([]map[string]any, 0, len(ids))
	var driftSum int64
	neg := 0
	nonzero := 0
	for _, id := range ids {
		rec, err := reconcile(client, *base, *admin, id)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reconcile", id, err)
			os.Exit(1)
		}
		recs = append(recs, rec)
		d := int64Field(rec, "drift")
		driftSum += d
		if d != 0 {
			nonzero++
		}
		if int64Field(rec, "quota_balance") < 0 {
			neg++
		}
	}

	out := summarize(samples, elapsed, len(rows), copies, *speed, driftSum, nonzero, neg)
	out["trace_rows_per_copy"] = len(rows)
	out["keys"] = *keysN
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	if driftSum != 0 || neg > 0 {
		os.Exit(1)
	}
}

func loadTrace(path string, from, dur time.Duration) ([]traceRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}
	for _, name := range []string{"TIMESTAMP", "ContextTokens", "GeneratedTokens"} {
		if _, ok := col[name]; !ok {
			return nil, fmt.Errorf("trace missing column %s", name)
		}
	}
	var origin time.Time
	var rows []traceRow
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ts, err := parseTS(rec[col["TIMESTAMP"]])
		if err != nil {
			continue
		}
		if origin.IsZero() {
			origin = ts
		}
		off := ts.Sub(origin)
		if off < from || off >= from+dur {
			continue
		}
		ctxN, _ := strconv.Atoi(strings.TrimSpace(rec[col["ContextTokens"]]))
		gen, _ := strconv.Atoi(strings.TrimSpace(rec[col["GeneratedTokens"]]))
		if ctxN < 1 {
			ctxN = 1
		}
		if gen < 1 {
			gen = 1
		}
		rows = append(rows, traceRow{offset: off - from, ctx: ctxN, gen: gen})
	}
	return rows, nil
}

func parseTS(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999-07:00", "2006-01-02 15:04:05.999999Z07:00", "2006-01-02 15:04:05Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Azure 的写法是 "2024-05-12 00:00:00.001163+00:00"。
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07:00", s); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02 15:04:05-07:00", s)
}

func createKeys(client *http.Client, base, admin string, n, heavy, heavyRPM int, quota int64) ([]string, []int64, error) {
	keys := make([]string, n)
	ids := make([]int64, n)
	for i := 0; i < n; i++ {
		rpm := 0
		if i < heavy {
			rpm = heavyRPM
		}
		body, _ := json.Marshal(map[string]any{
			"name":  fmt.Sprintf("replay-%d-%d", time.Now().UnixNano(), i),
			"quota": quota, "rpm_limit": rpm, "tpm_limit": 0, "concurrency_limit": 0,
		})
		req, _ := http.NewRequest(http.MethodPost, base+"/admin/keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Admin-Token", admin)
		res, err := client.Do(req)
		if err != nil {
			return nil, nil, err
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			return nil, nil, fmt.Errorf("create key %d: %d %s", i, res.StatusCode, b)
		}
		var out struct {
			Key  string `json:"key"`
			Item struct {
				ID int64 `json:"id"`
			} `json:"item"`
		}
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, nil, err
		}
		keys[i] = out.Key
		ids[i] = out.Item.ID
	}
	return keys, ids, nil
}

func doOne(client *http.Client, base, key, model string, row traceRow, stream bool, keyIdx int) sample {
	prompt := promptFor(row.ctx)
	maxTok := row.gen
	payload, _ := json.Marshal(map[string]any{
		"model":       model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"max_tokens":  maxTok,
		"temperature": 0.7,
		"stream":      stream,
	})
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	start := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return sample{at: start, status: 0, latency: time.Since(start), stream: stream, key: keyIdx, upstreamFail: true}
	}
	defer res.Body.Close()
	sm := sample{
		at: start, status: res.StatusCode, stream: stream, key: keyIdx,
		channel: res.Header.Get("X-Railhead-Channel"),
	}
	sm.mockTTFTUs = parseInt64Header(res.Header.Get("X-Mock-TTFT-Us"))
	sm.upstreamUs = parseInt64Header(res.Header.Get("X-Mock-Upstream-Us"))
	if stream && res.StatusCode == http.StatusOK {
		buf := make([]byte, 1)
		_, _ = res.Body.Read(buf)
		sm.ttft = time.Since(start)
		rest, _ := io.ReadAll(res.Body)
		body := append(buf, rest...)
		sm.latency = time.Since(start)
		sm.okTokens = usageTokens(body)
		return sm
	}
	body, _ := io.ReadAll(res.Body)
	sm.latency = time.Since(start)
	if res.StatusCode == http.StatusOK {
		var parsed domain.ChatResponse
		_ = json.Unmarshal(body, &parsed)
		if sm.upstreamUs == 0 {
			sm.upstreamUs = parsed.MockUpstreamUs
		}
		if sm.mockTTFTUs == 0 {
			sm.mockTTFTUs = parsed.MockTTFTUs
		}
		sm.okTokens = parsed.Usage.TotalTokens
		if sm.okTokens == 0 {
			sm.okTokens = parsed.Usage.PromptTokens + parsed.Usage.CompletionTokens
		}
	} else if res.StatusCode == http.StatusTooManyRequests {
		sm.rateLimited = true
	} else if res.StatusCode >= 500 || res.StatusCode == 0 {
		sm.upstreamFail = true
	}
	return sm
}

func promptFor(ctxTokens int) string {
	// CountPrompt(单条 user) = 8 + CountText(content)，CountText ≈ runes/2。
	need := ctxTokens - 8
	if need < 1 {
		need = 1
	}
	runes := need*2 - 1
	if runes > 20000 {
		runes = 20000
	}
	s := strings.Repeat("a", runes)
	// 校正到估算值恰好等于目标，避免边界差 1。
	for tokens.CountPrompt([]domain.Message{{Role: "user", Content: domain.Content{Text: s}}}) < ctxTokens && len(s) < 20000 {
		s += "a"
	}
	return s
}

func usageTokens(body []byte) int {
	// 流里最后一块带 usage。从后往前找 prompt_tokens。
	idx := bytes.LastIndex(body, []byte(`"total_tokens"`))
	if idx < 0 {
		return 0
	}
	frag := body[idx:]
	var wrap struct {
		Usage struct {
			Total int `json:"total_tokens"`
		} `json:"usage"`
	}
	// 片段不是完整 JSON。直接扫数字。
	colon := bytes.IndexByte(frag, ':')
	if colon < 0 {
		return 0
	}
	n := 0
	for _, c := range frag[colon+1:] {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else if n > 0 {
			break
		}
	}
	_ = wrap
	return n
}

func summarize(samples []sample, elapsed time.Duration, rows, copies int, speed float64, drift int64, nonzero, neg int) map[string]any {
	status := map[string]int{}
	var lats, overs, ttftOvers []float64
	var ok, limited, upFail, other int
	var okTokens int
	channels := map[string]int{}
	for _, s := range samples {
		status[strconv.Itoa(s.status)]++
		if s.status == 200 {
			ok++
			okTokens += s.okTokens
			if s.channel != "" {
				channels[s.channel]++
			}
			ms := float64(s.latency.Microseconds()) / 1000
			lats = append(lats, ms)
			if s.upstreamUs > 0 {
				overs = append(overs, ms-float64(s.upstreamUs)/1000)
			}
			if s.stream && s.ttft > 0 && s.mockTTFTUs > 0 {
				ttftOvers = append(ttftOvers, float64(s.ttft.Microseconds()-s.mockTTFTUs)/1000)
			}
		} else if s.rateLimited {
			limited++
		} else if s.upstreamFail || s.status >= 500 || s.status == 0 {
			upFail++
		} else {
			other++
		}
	}
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-9
	}
	return map[string]any{
		"requests":                      len(samples),
		"trace_copies":                  copies,
		"speed":                         speed,
		"wall_s":                        sec,
		"offered_qps":                   float64(len(samples)) / sec,
		"goodput_qps":                   float64(ok) / sec,
		"goodput_tokens_per_s":          float64(okTokens) / sec,
		"success":                       ok,
		"rate_limited":                  limited,
		"upstream_or_gateway_errors":    upFail,
		"other_non_200":                 other,
		"status_counts":                 status,
		"channels_ok":                   channels,
		"latency_ms":                    pct(lats),
		"overhead_ms":                   pct(overs),
		"ttft_overhead_ms":              pct(ttftOvers),
		"billing_drift_sum":             drift,
		"billing_keys_nonzero_drift":    nonzero,
		"billing_negative_balance_keys": neg,
	}
}

func pct(xs []float64) map[string]any {
	if len(xs) == 0 {
		return map[string]any{"n": 0}
	}
	sort.Float64s(xs)
	at := func(p float64) float64 {
		idx := int(float64(len(xs)-1) * p)
		return xs[idx]
	}
	sum := 0.0
	for _, v := range xs {
		sum += v
	}
	return map[string]any{
		"n": len(xs), "avg": sum / float64(len(xs)),
		"p50": at(0.50), "p95": at(0.95), "p99": at(0.99),
		"min": xs[0], "max": xs[len(xs)-1],
	}
}

func zipfCDF(n int, s float64) []float64 {
	cdf := make([]float64, n)
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1 / math.Pow(float64(i), s)
		cdf[i-1] = sum
	}
	for i := range cdf {
		cdf[i] /= sum
	}
	return cdf
}

func pickZipf(cdf []float64, h uint32) int {
	u := float64(h%1_000_000) / 1_000_000
	i := sort.Search(len(cdf), func(i int) bool { return cdf[i] >= u })
	if i >= len(cdf) {
		i = len(cdf) - 1
	}
	return i
}

func hash32(a, b int) uint32 {
	x := uint32(a)*2654435761 + uint32(b)*2246822519
	x ^= x >> 16
	return x
}

func waitUntil(t time.Time) {
	d := time.Until(t)
	if d > 0 {
		time.Sleep(d)
	}
}

func reconcile(client *http.Client, base, admin string, id int64) (map[string]any, error) {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/admin/keys/%d/reconcile", base, id), nil)
	req.Header.Set("X-Admin-Token", admin)
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status %d %s", res.StatusCode, b)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func int64Field(m map[string]any, k string) int64 {
	switch v := m[k].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func parseInt64Header(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func dumpEvents(path string, start time.Time, samples []sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, s := range samples {
		if err := enc.Encode(map[string]any{
			"t_ms":    s.at.Sub(start).Milliseconds(),
			"unix_ms": s.at.UnixMilli(),
			"status":  s.status, "stream": s.stream, "key": s.key, "channel": s.channel,
			"latency_us":   s.latency.Microseconds(),
			"ttft_us":      s.ttft.Microseconds(),
			"upstream_us":  s.upstreamUs,
			"mock_ttft_us": s.mockTTFTUs,
		}); err != nil {
			return err
		}
	}
	return nil
}

func writeProgress(path string, start time.Time, mu *sync.Mutex, samples *[]sample, stop <-chan struct{}) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	seen := 0
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			mu.Lock()
			all := *samples
			mu.Unlock()
			ok, bad, limited := 0, 0, 0
			for _, s := range all[seen:] {
				switch {
				case s.status == 200:
					ok++
				case s.rateLimited:
					limited++
				default:
					bad++
				}
			}
			seen = len(all)
			_ = enc.Encode(map[string]any{
				"t_ms": time.Since(start).Milliseconds(), "wall": now.UTC().Format(time.RFC3339Nano),
				"ok": ok, "limited": limited, "bad": bad,
			})
		}
	}
}
