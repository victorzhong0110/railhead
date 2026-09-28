// Package metrics 把网关运行情况暴露成 Prometheus 指标。
// 标签只用有限集合（结果、是否流式、渠道名）。模型名由用户传入，放进标签会造成高基数。
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 熔断状态映射成数字，方便画图：0 关闭，1 半开，2 打开。
const (
	breakerClosed   = 0
	breakerHalfOpen = 1
	breakerOpen     = 2
)

// Metrics 使用独立 Registry，测试里多次 New 不会和全局注册表冲突。
type Metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	upstream *prometheus.HistogramVec
	attempts *prometheus.CounterVec
	tokens   *prometheus.CounterVec
	cache    *prometheus.CounterVec
	limited  *prometheus.CounterVec
	breaker  *prometheus.GaugeVec
	inflight prometheus.Gauge
}

// New 注册全部指标。
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "railhead_requests_total",
			Help: "用户请求次数，按结果和是否流式划分。",
		}, []string{"result", "stream"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "railhead_request_duration_seconds",
			Help:    "用户请求耗时，含鉴权、限流、计费和上游。",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"stream"}),
		upstream: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "railhead_upstream_duration_seconds",
			Help:    "单次上游调用耗时。",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"channel"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "railhead_upstream_attempts_total",
			Help: "上游调用次数，含故障转移里的失败尝试。",
		}, []string{"channel", "result"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "railhead_tokens_total",
			Help: "结算时记录的 token 数。",
		}, []string{"direction"}),
		cache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "railhead_cache_total",
			Help: "响应缓存查找结果。",
		}, []string{"result"}),
		limited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "railhead_limited_total",
			Help: "被限流或并发限制拒绝的次数。",
		}, []string{"kind"}),
		breaker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "railhead_breaker_state",
			Help: "渠道熔断状态：0 关闭，1 半开，2 打开。",
		}, []string{"channel"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "railhead_inflight_requests",
			Help: "当前正在处理的用户请求数。",
		}),
	}
	reg.MustRegister(m.requests, m.duration, m.upstream, m.attempts, m.tokens, m.cache, m.limited, m.breaker, m.inflight)
	return m
}

// Handler 是 /metrics。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveRequest 记录一次用户请求。
func (m *Metrics) ObserveRequest(result string, stream bool, d time.Duration) {
	if m == nil {
		return
	}
	flag := strconv.FormatBool(stream)
	m.requests.WithLabelValues(result, flag).Inc()
	m.duration.WithLabelValues(flag).Observe(d.Seconds())
}

// ObserveUpstream 记录一次上游调用。
func (m *Metrics) ObserveUpstream(channel string, d time.Duration, success bool) {
	if m == nil {
		return
	}
	m.upstream.WithLabelValues(channel).Observe(d.Seconds())
	result := "error"
	if success {
		result = "success"
	}
	m.attempts.WithLabelValues(channel, result).Inc()
}

// Tokens 累加结算 token。
func (m *Metrics) Tokens(prompt, completion int) {
	if m == nil {
		return
	}
	if prompt > 0 {
		m.tokens.WithLabelValues("prompt").Add(float64(prompt))
	}
	if completion > 0 {
		m.tokens.WithLabelValues("completion").Add(float64(completion))
	}
}

// Cache 记录 hit、miss 或 bypass。
func (m *Metrics) Cache(result string) {
	if m == nil {
		return
	}
	m.cache.WithLabelValues(result).Inc()
}

// Limited 记录 rpm、tpm 或 concurrency。
func (m *Metrics) Limited(kind string) {
	if m == nil {
		return
	}
	m.limited.WithLabelValues(kind).Inc()
}

// Breaker 更新渠道熔断状态。
func (m *Metrics) Breaker(channel, state string) {
	if m == nil {
		return
	}
	value := float64(breakerClosed)
	switch state {
	case "half_open":
		value = breakerHalfOpen
	case "open":
		value = breakerOpen
	}
	m.breaker.WithLabelValues(channel).Set(value)
}

// InflightAdd 加减在飞请求。
func (m *Metrics) InflightAdd(n int) {
	if m == nil {
		return
	}
	m.inflight.Add(float64(n))
}
