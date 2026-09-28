package breaker

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBreakerOpensAndProbes(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	b := New(3, time.Second, 5*time.Second, clk.Now)

	if !b.Available() {
		t.Fatal("new breaker should be available")
	}
	b.Failure()
	b.Failure()
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed before threshold", b.State())
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("state = %s, want open", b.State())
	}
	if ok, _, _ := b.TryAcquire(); ok {
		t.Fatal("open breaker must reject")
	}

	clk.Advance(time.Second)
	ok1, state, _ := b.TryAcquire()
	if !ok1 || state != StateHalfOpen {
		t.Fatalf("first probe ok=%v state=%s", ok1, state)
	}
	if ok2, _, _ := b.TryAcquire(); ok2 {
		t.Fatal("second probe must be rejected while the first is in flight")
	}

	if st, _ := b.Failure(); st != StateOpen {
		t.Fatalf("failed probe state = %s", st)
	}
	if ok, _, _ := b.TryAcquire(); ok {
		t.Fatal("breaker should be open again immediately after a failed probe")
	}

	clk.Advance(time.Second)
	if ok, _, _ := b.TryAcquire(); !ok {
		t.Fatal("cooldown elapsed, probe should be allowed")
	}
	if st, _ := b.Success(); st != StateClosed {
		t.Fatalf("successful probe state = %s", st)
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %s", got)
	}
}

func TestProbeTimeoutDoesNotStick(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	b := New(1, time.Second, 2*time.Second, clk.Now)
	b.Failure()
	clk.Advance(time.Second)
	if ok, _, _ := b.TryAcquire(); !ok {
		t.Fatal("probe")
	}
	// 探测请求的进程消失了，没有 Success/Failure。
	clk.Advance(2 * time.Second)
	if ok, _, _ := b.TryAcquire(); !ok {
		t.Fatal("expired probe must allow another probe")
	}
}

func TestAbandonReleasesProbe(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	b := New(1, time.Minute, time.Minute, clk.Now)
	b.Failure()
	clk.Advance(time.Minute)
	if ok, _, _ := b.TryAcquire(); !ok {
		t.Fatal("probe")
	}
	b.Abandon()
	if ok, _, _ := b.TryAcquire(); !ok {
		t.Fatal("abandon should free the probe slot")
	}
}
