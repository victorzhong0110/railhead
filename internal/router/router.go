// Package router 选择渠道，并在可重试的失败上做故障转移。
//
// 一次用户请求里的策略是：
//  1. 在当前优先级最高、熔断器允许的渠道里按权重随机挑一个。
//  2. 失败且错误可重试：记一次熔断失败，这次请求不再用这条渠道，立刻换下一条。
//     这一步是故障转移，不等待。
//  3. 这一轮渠道都失败了，还有尝试次数：指数退避加抖动，清空本轮排除名单，再来一轮。
//  4. 不可重试的错误（上游 400）立刻返回，并把熔断器记为成功。渠道是活的，错在请求。
//  5. 客户端自己取消时，不记成功也不记失败，只把半开探测位还回去。
//
// 重试发生在同一次预扣之内，所以不会因为换渠道而扣两次费。
package router

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/victorzhong0110/railhead/internal/breaker"
	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/metrics"
	"github.com/victorzhong0110/railhead/internal/provider"
)

// ErrNoChannel 表示没有任何渠道能接这个模型（没配置，或全部熔断打开）。
var ErrNoChannel = errors.New("no channel available for model")

// Router 持有渠道快照和熔断器。快照会定期从数据库换掉。
type Router struct {
	mu          sync.RWMutex
	channels    []domain.Channel
	breakers    *breaker.Group
	upstream    provider.Upstream
	metrics     *metrics.Metrics
	maxAttempts int
	baseBackoff time.Duration
}

// New 创建路由器。maxAttempts 是一次用户请求里最多打上游多少次。
func New(upstream provider.Upstream, breakers *breaker.Group, m *metrics.Metrics, maxAttempts int, baseBackoff time.Duration) *Router {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 20 * time.Millisecond
	}
	if breakers == nil {
		breakers = breaker.NewGroup(breaker.Options{Threshold: 5, Cooldown: 15 * time.Second})
	}
	return &Router{
		breakers:    breakers,
		upstream:    upstream,
		metrics:     m,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
	}
}

// SetChannels 替换渠道快照。调用方传入的切片之后还可以改，这里会拷一份。
func (r *Router) SetChannels(channels []domain.Channel) {
	cp := make([]domain.Channel, len(channels))
	copy(cp, channels)
	r.mu.Lock()
	r.channels = cp
	r.mu.Unlock()
}

// Breakers 暴露熔断器组，方便指标协程做快照。
func (r *Router) Breakers() *breaker.Group { return r.breakers }

// Channels 返回当前渠道快照的副本。
func (r *Router) Channels() []domain.Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.Channel, len(r.channels))
	copy(out, r.channels)
	return out
}

// Chat 做非流式调用，失败时按策略换渠道。
// 非流式在 invoke 返回时响应体已经读完，所以可以立刻取消这次尝试的 context。
func (r *Router) Chat(ctx context.Context, req domain.ChatRequest) (*domain.ChatResponse, domain.Channel, error) {
	var resp *domain.ChatResponse
	ch, release, err := r.failover(ctx, req.Model, func(ctx context.Context, ch domain.Channel) error {
		out, callErr := r.upstream.Chat(ctx, ch, req)
		if callErr != nil {
			return callErr
		}
		resp = out
		return nil
	})
	if release != nil {
		release()
	}
	return resp, ch, err
}

// ChatStream 在流建立成功后返回。流已经开始写给客户端之后不能再换渠道，
// 所以这里只重试「还没拿到 200」的失败。
//
// 成功时不能马上取消尝试的 context：HTTP 流的 body 还挂在这个 context 上，
// 一取消，后面的 SSE 读就会变成 context canceled。取消推迟到 Stream.Close。
func (r *Router) ChatStream(ctx context.Context, req domain.ChatRequest) (provider.Stream, domain.Channel, error) {
	var st provider.Stream
	ch, release, err := r.failover(ctx, req.Model, func(ctx context.Context, ch domain.Channel) error {
		out, callErr := r.upstream.ChatStream(ctx, ch, req)
		if callErr != nil {
			return callErr
		}
		st = out
		return nil
	})
	if err != nil {
		if release != nil {
			release()
		}
		return nil, ch, err
	}
	return &boundStream{Stream: st, cancel: release}, ch, nil
}

// boundStream 把上游流和它的超时 context 绑在一起。调用方必须 Close。
type boundStream struct {
	provider.Stream
	cancel func()
	once   sync.Once
}

// Header 转给内层流。boundStream 包了一层之后，类型断言必须落在这一层上。
func (b *boundStream) Header(key string) string {
	type headerReader interface{ Header(string) string }
	if h, ok := b.Stream.(headerReader); ok {
		return h.Header(key)
	}
	return ""
}

func (b *boundStream) Close() error {
	var err error
	if b.Stream != nil {
		err = b.Stream.Close()
	}
	b.once.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
	})
	return err
}

func (r *Router) failover(ctx context.Context, model string, invoke func(context.Context, domain.Channel) error) (domain.Channel, func(), error) {
	excluded := map[string]struct{}{}
	var lastErr error
	calls := 0
	for calls < r.maxAttempts {
		if err := ctx.Err(); err != nil {
			return domain.Channel{}, nil, err
		}
		ch, ok := r.pick(model, excluded)
		if !ok {
			if calls == 0 && len(excluded) == 0 {
				return domain.Channel{}, nil, ErrNoChannel
			}
			// 排除名单是空的，说明不是「这一轮用完了」，而是熔断器把所有渠道都挡住了。
			if len(excluded) == 0 {
				break
			}
			if err := sleep(ctx, r.backoff(calls)); err != nil {
				return domain.Channel{}, nil, err
			}
			excluded = map[string]struct{}{}
			continue
		}
		calls++
		timeout := ch.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		err := invoke(callCtx, ch)
		elapsed := time.Since(start)
		if err == nil {
			r.breakers.Success(ch.Name)
			if r.metrics != nil {
				r.metrics.ObserveUpstream(ch.Name, elapsed, true)
			}
			// 成功时把 cancel 交给调用方。流式响应还要继续读 body。
			return ch, cancel, nil
		}
		cancel()
		if ctx.Err() != nil {
			r.breakers.Abandon(ch.Name)
			if r.metrics != nil {
				r.metrics.ObserveUpstream(ch.Name, elapsed, false)
			}
			return domain.Channel{}, nil, ctx.Err()
		}
		lastErr = err
		if r.metrics != nil {
			r.metrics.ObserveUpstream(ch.Name, elapsed, false)
		}
		if !retryable(err) {
			// 渠道正常响应了一个客户端错误，不能把它算成宕机。
			r.breakers.Success(ch.Name)
			return ch, nil, err
		}
		r.breakers.Failure(ch.Name)
		excluded[ch.Name] = struct{}{}
	}
	if lastErr == nil {
		return domain.Channel{}, nil, ErrNoChannel
	}
	return domain.Channel{}, nil, lastErr
}

func (r *Router) pick(model string, excluded map[string]struct{}) (domain.Channel, bool) {
	r.mu.RLock()
	channels := r.channels
	r.mu.RUnlock()

	best := -1 << 30
	var group []domain.Channel
	for _, ch := range channels {
		if !ch.Enabled || !ch.Supports(model) {
			continue
		}
		if _, skip := excluded[ch.Name]; skip {
			continue
		}
		if ch.Weight < 1 {
			ch.Weight = 1
		}
		if !r.breakers.Available(ch.Name) {
			continue
		}
		if ch.Priority > best {
			best = ch.Priority
			group = []domain.Channel{ch}
			continue
		}
		if ch.Priority == best {
			group = append(group, ch)
		}
	}
	// 加权随机，但半开探测位可能被别人抢走，所以选不中就换同组的下一条。
	for len(group) > 0 {
		i := weightedIndex(group)
		ch := group[i]
		if r.breakers.TryAcquire(ch.Name) {
			return ch, true
		}
		group = append(group[:i], group[i+1:]...)
	}
	return domain.Channel{}, false
}

func weightedIndex(channels []domain.Channel) int {
	total := 0
	for _, ch := range channels {
		w := ch.Weight
		if w < 1 {
			w = 1
		}
		total += w
	}
	if total <= 0 {
		return 0
	}
	n := rand.IntN(total)
	for i, ch := range channels {
		w := ch.Weight
		if w < 1 {
			w = 1
		}
		n -= w
		if n < 0 {
			return i
		}
	}
	return len(channels) - 1
}

func (r *Router) backoff(calls int) time.Duration {
	if calls < 1 {
		calls = 1
	}
	shift := calls - 1
	if shift > 4 {
		shift = 4
	}
	d := r.baseBackoff * time.Duration(1<<shift)
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	if d <= 0 {
		return 0
	}
	// 等长抖动：落在 [d/2, d)，避免重试在同一时刻撞上上游。
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func retryable(err error) bool {
	var ue *provider.UpstreamError
	if errors.As(err, &ue) {
		return ue.Retryable
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
