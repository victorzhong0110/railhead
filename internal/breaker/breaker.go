// Package breaker 是每个渠道一个的连续失败熔断器。
//
// 状态机：
//
//	closed --连续失败达到阈值--> open --冷却结束--> half-open --探测成功--> closed
//	                                                    |
//	                                                    +--探测失败--> open
//
// 半开时只放行一个探测请求。探测方如果中途取消，用 Abandon 把探测位还回去，
// 不把它算成成功或失败。探测超时未归还时，过了 ProbeTimeout 允许下一次探测，
// 避免进程在探测过程中崩溃后渠道永远卡在半开。
package breaker

import (
	"sync"
	"time"
)

const (
	StateClosed   = "closed"
	StateOpen     = "open"
	StateHalfOpen = "half_open"
)

// Breaker 是单渠道熔断器。零值不能直接用，请用 New。
type Breaker struct {
	mu            sync.Mutex
	threshold     int
	cooldown      time.Duration
	probeTimeout  time.Duration
	now           func() time.Time
	state         string
	consecutive   int
	openedAt      time.Time
	probeOut      bool
	probeDeadline time.Time
}

// Options 是一组渠道共用的熔断参数。
type Options struct {
	Threshold    int
	Cooldown     time.Duration
	ProbeTimeout time.Duration
	Now          func() time.Time
	// OnState 在状态发生变化时调用。回调里不要再调用本组熔断器，以免死锁。
	OnState func(name, state string)
}

func New(threshold int, cooldown, probeTimeout time.Duration, now func() time.Time) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = 15 * time.Second
	}
	if probeTimeout <= 0 {
		probeTimeout = 30 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &Breaker{
		threshold:    threshold,
		cooldown:     cooldown,
		probeTimeout: probeTimeout,
		now:          now,
		state:        StateClosed,
	}
}

// State 返回当前状态名。
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh(b.now())
	return b.state
}

// Available 是只读判断：这个渠道现在能不能被选中。
// 它不会占用半开探测位。真正发请求前还要再调用 TryAcquire。
func (b *Breaker) Available() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ok, _ := b.usable(b.now())
	return ok
}

// TryAcquire 在即将发请求时调用。关闭状态下总是成功。
// 半开或冷却刚结束时，只有第一个调用者能拿走探测位。
func (b *Breaker) TryAcquire() (ok bool, state string, changed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	usable, needProbe := b.usable(now)
	if !usable {
		return false, b.state, false
	}
	if !needProbe {
		return true, b.state, false
	}
	prev := b.state
	b.state = StateHalfOpen
	b.probeOut = true
	b.probeDeadline = now.Add(b.probeTimeout)
	return true, b.state, prev != b.state
}

// Success 把渠道打回关闭，并清掉连续失败计数。
func (b *Breaker) Success() (state string, changed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	changed = b.state != StateClosed || b.consecutive != 0 || b.probeOut
	b.state = StateClosed
	b.consecutive = 0
	b.probeOut = false
	return b.state, changed
}

// Failure 记录一次上游失败。半开探测失败会立即重新打开。
func (b *Breaker) Failure() (state string, changed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		b.probeOut = false
		b.consecutive = b.threshold
		return b.state, true
	}
	b.consecutive++
	if b.consecutive >= b.threshold && b.state != StateOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		b.probeOut = false
		return b.state, true
	}
	return b.state, false
}

// Abandon 在调用方取消（客户端断开）时归还探测位，不计入成功或失败。
func (b *Breaker) Abandon() (state string, changed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen && b.probeOut {
		b.probeOut = false
		return b.state, true
	}
	return b.state, false
}

func (b *Breaker) refresh(now time.Time) {
	_, _ = b.usable(now)
}

// usable 报告现在能否选中，以及选中后是否必须占用探测位。
// 打开状态过了冷却时间，逻辑上进入可探测，但状态要等 TryAcquire 才改成半开。
func (b *Breaker) usable(now time.Time) (ok bool, needProbe bool) {
	switch b.state {
	case StateClosed:
		return true, false
	case StateOpen:
		if now.Sub(b.openedAt) >= b.cooldown {
			return true, true
		}
		return false, false
	case StateHalfOpen:
		if b.probeOut && now.Before(b.probeDeadline) {
			return false, false
		}
		return true, true
	default:
		return false, false
	}
}

// Group 按渠道名持有熔断器。渠道是运行中才出现的，所以用懒创建。
type Group struct {
	mu      sync.Mutex
	m       map[string]*Breaker
	opt     Options
	onState func(name, state string)
}

// NewGroup 创建一组熔断器。
func NewGroup(opt Options) *Group {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &Group{
		m:       map[string]*Breaker{},
		opt:     opt,
		onState: opt.OnState,
	}
}

func (g *Group) get(name string) *Breaker {
	g.mu.Lock()
	defer g.mu.Unlock()
	b, ok := g.m[name]
	if !ok {
		b = New(g.opt.Threshold, g.opt.Cooldown, g.opt.ProbeTimeout, g.opt.Now)
		g.m[name] = b
		if g.onState != nil {
			// 在持有 group 锁时回调。回调不得再进 Group，否则死锁。
			g.onState(name, StateClosed)
		}
	}
	return b
}

func (g *Group) emit(name, state string, changed bool) {
	if changed && g.onState != nil {
		g.onState(name, state)
	}
}

// Available 判断渠道是否可选。
func (g *Group) Available(name string) bool {
	return g.get(name).Available()
}

// TryAcquire 占用一次调用许可。
func (g *Group) TryAcquire(name string) bool {
	ok, state, changed := g.get(name).TryAcquire()
	g.emit(name, state, changed)
	return ok
}

// Success 上报成功。
func (g *Group) Success(name string) {
	state, changed := g.get(name).Success()
	g.emit(name, state, changed)
}

// Failure 上报失败。
func (g *Group) Failure(name string) {
	state, changed := g.get(name).Failure()
	g.emit(name, state, changed)
}

// Abandon 归还探测位。
func (g *Group) Abandon(name string) {
	state, changed := g.get(name).Abandon()
	g.emit(name, state, changed)
}

// Snapshot 返回每个渠道的当前状态，给指标采集用。
func (g *Group) Snapshot() map[string]string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]string, len(g.m))
	now := g.opt.Now()
	for name, b := range g.m {
		b.mu.Lock()
		b.refresh(now)
		out[name] = b.state
		b.mu.Unlock()
	}
	return out
}
