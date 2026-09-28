package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	// tokens 是放大 scale 倍之后的余额，用来保留补令牌时的小数部分。
	tokens int64
	ts     time.Time
}

// Memory 是单进程令牌桶。
type Memory struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

// NewMemory 创建内存令牌桶。now 注入进去，测试才能快进时间。
func NewMemory(now func() time.Time) *Memory {
	if now == nil {
		now = time.Now
	}
	return &Memory{buckets: map[string]*bucket{}, now: now}
}

// Allow 的算法和 Redis Lua 一致：只把「已经够换出整数令牌」的时间从 ts 里扣掉，
// 剩下的零头留给下一次，避免因为整除把补充速率越算越少。
func (m *Memory) Allow(key string, cost, capacity, refill int, interval time.Duration) bool {
	if capacity <= 0 {
		return true
	}
	if interval <= 0 || refill <= 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	b, ok := m.buckets[key]
	if !ok {
		b = &bucket{tokens: int64(capacity) * scale, ts: now}
		m.buckets[key] = b
	} else {
		refillBucket(b, now, int64(capacity), int64(refill), interval)
	}
	need := int64(cost) * scale
	if b.tokens < need {
		return false
	}
	b.tokens -= need
	return true
}

func refillBucket(b *bucket, now time.Time, capacity, refill int64, interval time.Duration) {
	elapsed := now.Sub(b.ts)
	if elapsed <= 0 {
		return
	}
	// add 的单位是 milli-token。
	add := elapsed.Nanoseconds() * refill * scale / interval.Nanoseconds()
	if add <= 0 {
		return
	}
	capMilli := capacity * scale
	b.tokens += add
	if b.tokens > capMilli {
		b.tokens = capMilli
	}
	// 只前进被换成令牌的那段时间，零头留在 ts 和 now 的间隙里。
	consumed := time.Duration(add * interval.Nanoseconds() / (refill * scale))
	b.ts = b.ts.Add(consumed)
}

type memSlot struct {
	expiry time.Time
}

type memConcurrency struct {
	mu    sync.Mutex
	slots map[string]map[string]memSlot
	now   func() time.Time
}

func newMemConcurrency(now func() time.Time) *memConcurrency {
	if now == nil {
		now = time.Now
	}
	return &memConcurrency{slots: map[string]map[string]memSlot{}, now: now}
}

func (m *memConcurrency) Acquire(key, member string, limit int, lease time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	bucket := m.slots[key]
	if bucket == nil {
		bucket = map[string]memSlot{}
		m.slots[key] = bucket
	}
	for id, slot := range bucket {
		if !slot.expiry.After(now) {
			delete(bucket, id)
		}
	}
	if len(bucket) >= limit {
		return false
	}
	bucket[member] = memSlot{expiry: now.Add(lease)}
	return true
}

func (m *memConcurrency) Release(key, member string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.slots[key]
	if bucket == nil {
		return
	}
	delete(bucket, member)
}
