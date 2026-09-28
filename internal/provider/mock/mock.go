// Package mock 是一个可控的假上游。
// 延迟、错误率和流式速度都能在运行中修改，用来做单测和故障注入，不必打到真实模型。
// prompt token 用 internal/tokens 的同一公式，这样网关预扣和 mock 账单可以对上。
package mock

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/tokens"
)

// Error 是 mock 主动制造的失败。网关适配层会把它转成 provider.UpstreamError。
// mock 不引用 provider，避免 provider → mock → provider 的循环依赖。
type Error struct {
	Status    int
	Retryable bool
	Message   string
	Err       error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("mock status %d", e.Status)
}

func (e *Error) Unwrap() error { return e.Err }

// Engine 是假模型。所有方法都可以并发调用。
type Engine struct {
	mu                sync.Mutex
	latency           time.Duration
	streamDelay       time.Duration
	errorRate         float64
	errorStatus       int
	defaultCompletion int
	// 生成式延迟：首 token 时间 + 每个输出 token 的解码时间。两者都为 0 时退回固定 latency。
	ttft        time.Duration
	decode      time.Duration
	tailProb    float64
	tailFactor  float64
	rate429     float64
	rate500     float64
	rateTimeout float64
	timeout     time.Duration
	requests    atomic.Int64
	errors      atomic.Int64
}

// Options 是假模型的初始参数。
type Options struct {
	Latency           time.Duration
	StreamDelay       time.Duration
	ErrorRate         float64
	ErrorStatus       int
	DefaultCompletion int
	TTFT              time.Duration
	DecodePerToken    time.Duration
	TailProb          float64
	TailFactor        float64
	Rate429           float64
	Rate500           float64
	RateTimeout       float64
	Timeout           time.Duration
}

// New 创建一个假模型。
func New(opt Options) *Engine {
	if opt.ErrorStatus == 0 {
		opt.ErrorStatus = 500
	}
	if opt.DefaultCompletion <= 0 {
		opt.DefaultCompletion = 16
	}
	if opt.TailFactor < 1 {
		opt.TailFactor = 1
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 2 * time.Second
	}
	return &Engine{
		latency:           opt.Latency,
		streamDelay:       opt.StreamDelay,
		errorRate:         opt.ErrorRate,
		errorStatus:       opt.ErrorStatus,
		defaultCompletion: opt.DefaultCompletion,
		ttft:              opt.TTFT,
		decode:            opt.DecodePerToken,
		tailProb:          opt.TailProb,
		tailFactor:        opt.TailFactor,
		rate429:           opt.Rate429,
		rate500:           opt.Rate500,
		rateTimeout:       opt.RateTimeout,
		timeout:           opt.Timeout,
	}
}

// Fault 是一次运行中的参数修改。nil 字段保持原值。
type Fault struct {
	Latency        *time.Duration
	StreamDelay    *time.Duration
	ErrorRate      *float64
	ErrorStatus    *int
	TTFT           *time.Duration
	DecodePerToken *time.Duration
	TailProb       *float64
	TailFactor     *float64
	Rate429        *float64
	Rate500        *float64
	RateTimeout    *float64
	Timeout        *time.Duration
}

// Update 修改运行中的故障参数。nil 字段保持原值。
func (e *Engine) Update(latency *time.Duration, streamDelay *time.Duration, errorRate *float64, errorStatus *int) {
	e.Apply(Fault{Latency: latency, StreamDelay: streamDelay, ErrorRate: errorRate, ErrorStatus: errorStatus})
}

// Apply 应用一组故障参数。
func (e *Engine) Apply(f Fault) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if f.Latency != nil {
		e.latency = *f.Latency
	}
	if f.StreamDelay != nil {
		e.streamDelay = *f.StreamDelay
	}
	if f.ErrorRate != nil {
		e.errorRate = *f.ErrorRate
	}
	if f.ErrorStatus != nil && *f.ErrorStatus > 0 {
		e.errorStatus = *f.ErrorStatus
	}
	if f.TTFT != nil {
		e.ttft = *f.TTFT
	}
	if f.DecodePerToken != nil {
		e.decode = *f.DecodePerToken
	}
	if f.TailProb != nil {
		e.tailProb = *f.TailProb
	}
	if f.TailFactor != nil && *f.TailFactor >= 1 {
		e.tailFactor = *f.TailFactor
	}
	if f.Rate429 != nil {
		e.rate429 = *f.Rate429
	}
	if f.Rate500 != nil {
		e.rate500 = *f.Rate500
	}
	if f.RateTimeout != nil {
		e.rateTimeout = *f.RateTimeout
	}
	if f.Timeout != nil && *f.Timeout > 0 {
		e.timeout = *f.Timeout
	}
}

// Stats 返回累计请求数和注入的错误数。
func (e *Engine) Stats() (requests, errors int64) {
	return e.requests.Load(), e.errors.Load()
}

type snap struct {
	latency     time.Duration
	streamDelay time.Duration
	errorRate   float64
	errorStatus int
	completion  int
	ttft        time.Duration
	decode      time.Duration
	tailProb    float64
	tailFactor  float64
	rate429     float64
	rate500     float64
	rateTimeout float64
	timeout     time.Duration
}

func (e *Engine) snapshot() snap {
	e.mu.Lock()
	defer e.mu.Unlock()
	return snap{
		latency: e.latency, streamDelay: e.streamDelay, errorRate: e.errorRate, errorStatus: e.errorStatus,
		completion: e.defaultCompletion, ttft: e.ttft, decode: e.decode, tailProb: e.tailProb, tailFactor: e.tailFactor,
		rate429: e.rate429, rate500: e.rate500, rateTimeout: e.rateTimeout, timeout: e.timeout,
	}
}

func (s snap) generative() bool {
	return s.ttft > 0 || s.decode > 0 || s.rate429 > 0 || s.rate500 > 0 || s.rateTimeout > 0
}

// plan 决定这次调用是失败，还是按输出长度睡多久。
// 失败率按 429、500、超时的顺序从一次均匀随机数里切，三者不会叠加。
func (s snap) plan(n int) (ttft, perToken time.Duration, fail error) {
	if n < 1 {
		n = 1
	}
	roll := rand.Float64()
	switch {
	case s.rate429 > 0 && roll < s.rate429:
		return 0, 0, &Error{Status: 429, Retryable: true, Message: "mock 注入了 429"}
	case s.rate500 > 0 && roll < s.rate429+s.rate500:
		return 0, 0, &Error{Status: 500, Retryable: true, Message: "mock 注入了 500"}
	case s.rateTimeout > 0 && roll < s.rate429+s.rate500+s.rateTimeout:
		return 0, 0, &Error{Status: 504, Retryable: true, Message: "mock 注入了超时"}
	}
	ttft = s.ttft + s.latency
	if s.tailProb > 0 && s.tailFactor > 1 && rand.Float64() < s.tailProb {
		ttft = time.Duration(float64(ttft) * s.tailFactor)
		if ttft <= 0 {
			ttft = s.ttft
		}
	}
	perToken = s.decode
	if perToken <= 0 {
		perToken = s.streamDelay
	}
	return ttft, perToken, nil
}

// Complete 生成一次非流式补全。生成式模式下总延迟 = TTFT + 输出 token 数 × 每 token 解码时间。
func (e *Engine) Complete(ctx context.Context, req domain.ChatRequest) (*domain.ChatResponse, error) {
	e.requests.Add(1)
	s := e.snapshot()
	text, usage := e.build(req, s.completion)
	if s.generative() {
		ttft, perToken, fail := s.plan(usage.CompletionTokens)
		if fail != nil {
			e.errors.Add(1)
			if s.rateTimeout > 0 && strings.Contains(fail.Error(), "超时") {
				if err := sleep(ctx, s.timeout); err != nil {
					return nil, err
				}
			}
			return nil, fail
		}
		total := ttft + time.Duration(usage.CompletionTokens)*perToken
		if err := sleep(ctx, total); err != nil {
			return nil, err
		}
		finish := "stop"
		return &domain.ChatResponse{
			ID: newID(), Object: "chat.completion", Created: time.Now().Unix(), Model: req.Model,
			Choices: []domain.Choice{{
				Index: 0, Message: &domain.Message{Role: "assistant", Content: domain.Content{Text: text}}, FinishReason: &finish,
			}},
			Usage:          usage,
			MockUpstreamUs: total.Microseconds(),
			MockTTFTUs:     ttft.Microseconds(),
		}, nil
	}
	if err := sleep(ctx, s.latency); err != nil {
		return nil, err
	}
	if fail, err := injected(s.errorRate, s.errorStatus); fail {
		e.errors.Add(1)
		return nil, err
	}
	finish := "stop"
	return &domain.ChatResponse{
		ID: newID(), Object: "chat.completion", Created: time.Now().Unix(), Model: req.Model,
		Choices: []domain.Choice{{
			Index: 0, Message: &domain.Message{Role: "assistant", Content: domain.Content{Text: text}}, FinishReason: &finish,
		}},
		Usage: usage,
	}, nil
}

// Stream 生成一次流式补全。错误在返回流之前注入，这样网关还能故障转移。
// 生成式模式下，返回流之前只睡 TTFT；每个 token 的间隔是解码时间。
func (e *Engine) Stream(ctx context.Context, req domain.ChatRequest) (*memStream, error) {
	e.requests.Add(1)
	s := e.snapshot()
	text, usage := e.build(req, s.completion)
	if s.generative() {
		ttft, perToken, fail := s.plan(usage.CompletionTokens)
		if fail != nil {
			e.errors.Add(1)
			if strings.Contains(fail.Error(), "超时") {
				if err := sleep(ctx, s.timeout); err != nil {
					return nil, err
				}
			}
			return nil, fail
		}
		if err := sleep(ctx, ttft); err != nil {
			return nil, err
		}
		return &memStream{
			ctx: ctx, delay: perToken, chunks: splitChunks(req.Model, text, usage),
			ttft: ttft, upstream: ttft + time.Duration(usage.CompletionTokens)*perToken,
		}, nil
	}
	if err := sleep(ctx, s.latency); err != nil {
		return nil, err
	}
	if fail, err := injected(s.errorRate, s.errorStatus); fail {
		e.errors.Add(1)
		return nil, err
	}
	return &memStream{
		ctx: ctx, delay: s.streamDelay, chunks: splitChunks(req.Model, text, usage),
	}, nil
}

func (e *Engine) build(req domain.ChatRequest, def int) (string, domain.Usage) {
	n := def
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		n = *req.MaxTokens
	}
	if n > 4096 {
		n = 4096
	}
	if n < 1 {
		n = 1
	}
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = "tok"
	}
	text := strings.Join(parts, " ")
	prompt := tokens.CountPrompt(req.Messages)
	usage := domain.Usage{PromptTokens: prompt, CompletionTokens: n, TotalTokens: prompt + n}
	return text, usage
}

func injected(rate float64, status int) (bool, error) {
	if rate <= 0 {
		return false, nil
	}
	if rate < 1 && rand.Float64() >= rate {
		return false, nil
	}
	if status < 400 {
		status = 500
	}
	retryable := status == 429 || status >= 500
	return true, &Error{
		Status:    status,
		Retryable: retryable,
		Message:   fmt.Sprintf("mock 注入了 %d", status),
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return &Error{Status: 504, Retryable: true, Message: "mock 在延迟期间被取消", Err: ctx.Err()}
	case <-timer.C:
		return nil
	}
}

func splitChunks(model, text string, usage domain.Usage) []*domain.Chunk {
	id := newID()
	created := time.Now().Unix()
	words := strings.Split(text, " ")
	chunks := make([]*domain.Chunk, 0, len(words)+2)
	role := "assistant"
	chunks = append(chunks, &domain.Chunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []domain.Choice{{Index: 0, Delta: &domain.Delta{Role: role}}},
	})
	for i, w := range words {
		piece := w
		if i > 0 {
			piece = " " + w
		}
		chunks = append(chunks, &domain.Chunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
			Choices: []domain.Choice{{Index: 0, Delta: &domain.Delta{Content: piece}}},
		})
	}
	finish := "stop"
	u := usage
	chunks = append(chunks, &domain.Chunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []domain.Choice{{Index: 0, Delta: &domain.Delta{}, FinishReason: &finish}},
		Usage:   &u,
	})
	return chunks
}

func newID() string {
	return fmt.Sprintf("chatcmpl-mock-%d", time.Now().UnixNano())
}

type memStream struct {
	ctx      context.Context
	delay    time.Duration
	chunks   []*domain.Chunk
	i        int
	ttft     time.Duration
	upstream time.Duration
}

// Timing 返回这次流的首 token 时间和估算的上游总时间，供 HTTP 层写响应头。
func (s *memStream) Timing() (ttft, upstream time.Duration) {
	if s == nil {
		return 0, 0
	}
	return s.ttft, s.upstream
}

func (s *memStream) Recv() (*domain.Chunk, error) {
	if s.i > 0 && s.delay > 0 {
		timer := time.NewTimer(s.delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return nil, s.ctx.Err()
		case <-timer.C:
		}
	}
	if s.i >= len(s.chunks) {
		return nil, io.EOF
	}
	c := s.chunks[s.i]
	s.i++
	return c, nil
}

func (s *memStream) Close() error { return nil }
