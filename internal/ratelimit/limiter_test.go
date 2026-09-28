package ratelimit

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

func TestMemoryTokenBucket(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	m := NewMemory(clk.Now)
	const key = "user"
	for i := 0; i < 5; i++ {
		if !m.Allow(key, 1, 5, 5, time.Second) {
			t.Fatalf("request %d should pass", i)
		}
	}
	if m.Allow(key, 1, 5, 5, time.Second) {
		t.Fatal("bucket should be empty")
	}
	// 每秒补 5 个，200ms 应该补回 1 个。
	clk.Advance(200 * time.Millisecond)
	if !m.Allow(key, 1, 5, 5, time.Second) {
		t.Fatal("one token should have been refilled")
	}
	if m.Allow(key, 1, 5, 5, time.Second) {
		t.Fatal("only one token should have been refilled")
	}
}

func TestMemoryRejectsCostAboveCapacity(t *testing.T) {
	m := NewMemory(time.Now)
	if m.Allow("k", 10, 5, 5, time.Second) {
		t.Fatal("a single cost larger than the bucket must be rejected")
	}
}

func TestMemoryConcurrencyLeaseExpires(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	c := newMemConcurrency(clk.Now)
	if !c.Acquire("k", "a", 1, time.Second) {
		t.Fatal("first acquire")
	}
	if c.Acquire("k", "b", 1, time.Second) {
		t.Fatal("limit 1")
	}
	clk.Advance(time.Second + time.Millisecond)
	if !c.Acquire("k", "b", 1, time.Second) {
		t.Fatal("expired lease should free the slot")
	}
	c.Release("k", "b")
	if !c.Acquire("k", "c", 1, time.Second) {
		t.Fatal("explicit release should free the slot")
	}
}

func TestRedisTokenBucketAndConcurrency(t *testing.T) {
	raw := os.Getenv("REDIS_URL")
	if raw == "" {
		t.Skip("REDIS_URL is not set")
	}
	opt, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	key := "railhead:test:rl:" + time.Now().Format("150405.000")
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	lim := newRedisLimiter(rdb)
	for i := 0; i < 3; i++ {
		ok, err := lim.Allow(ctx, key, 1, 3, 3, time.Minute)
		if err != nil || !ok {
			t.Fatalf("allow %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, err := lim.Allow(ctx, key, 1, 3, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("4th token should be rejected")
	}

	ck := "railhead:test:conc:" + time.Now().Format("150405.000")
	t.Cleanup(func() { rdb.Del(context.Background(), ck) })
	conc := newRedisConcurrency(rdb)
	ok, err = conc.Acquire(ctx, ck, "r1", 1, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire r1 ok=%v err=%v", ok, err)
	}
	ok, err = conc.Acquire(ctx, ck, "r2", 1, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second holder should be rejected")
	}
	if err := conc.Release(ctx, ck, "r1"); err != nil {
		t.Fatal(err)
	}
	ok, err = conc.Acquire(ctx, ck, "r2", 1, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire after release ok=%v err=%v", ok, err)
	}
}
