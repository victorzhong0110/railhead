// Package ratelimit 实现两种限制：
//
//  1. 令牌桶（RPM 按次，TPM 按估算 token）。Redis 用 Lua 保证原子，失败时退回本进程内存桶。
//  2. 并发租约。Redis 用有过期时间的有序集合，进程崩溃后租约会自己过期。
//
// 内存实现和 Redis 用同一套整数算法（token * 1000），方便对照着讲。
// 限额 <= 0 表示不限制。这是有意的：压测时可以把限额设为 0，单独看网关吞吐。
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const scale int64 = 1000

// Limiter 同时持有 Redis 和内存两个后端。Redis 出错时本次请求改走内存，不拒绝流量。
// 这是 fail-open：Redis 抖动时限额退化为单机，而不是整个网关 500。
type Limiter struct {
	mem   *Memory
	redis *redisLimiter
}

// New 创建限流器。rdb 可以为 nil，此时一直使用内存。
func New(rdb *redis.Client) *Limiter {
	return &Limiter{
		mem:   NewMemory(time.Now),
		redis: newRedisLimiter(rdb),
	}
}

// Allow 从名为 key 的桶里取 cost 个令牌。
// capacity 是桶大小，每 interval 补充 refill 个令牌。
func (l *Limiter) Allow(ctx context.Context, key string, cost, capacity, refill int, interval time.Duration) bool {
	if capacity <= 0 {
		return true
	}
	if cost < 0 {
		cost = 0
	}
	if l.redis != nil {
		ok, err := l.redis.Allow(ctx, key, cost, capacity, refill, interval)
		if err == nil {
			return ok
		}
		slog.Warn("redis 令牌桶失败，退回内存", "key", key, "err", err)
	}
	return l.mem.Allow(key, cost, capacity, refill, interval)
}

// Lease 是一次并发占用。Release 可以重复调用。
type Lease struct {
	release func()
	once    bool
}

// Release 归还并发位。
func (l *Lease) Release() {
	if l == nil || l.release == nil || l.once {
		return
	}
	l.once = true
	l.release()
}

// Concurrency 限制同一密钥同时在飞的请求数。
type Concurrency struct {
	mem   *memConcurrency
	redis *redisConcurrency
}

// NewConcurrency 创建并发限制器。rdb 可以为 nil。
func NewConcurrency(rdb *redis.Client) *Concurrency {
	return &Concurrency{
		mem:   newMemConcurrency(time.Now),
		redis: newRedisConcurrency(rdb),
	}
}

// Acquire 尝试占用一个并发位。limit <= 0 时直接放行，返回的 Lease 释放是空操作。
// lease 是崩溃后的自动释放时间，应大于单次请求的最长生命周期。
func (c *Concurrency) Acquire(ctx context.Context, key, member string, limit int, lease time.Duration) (*Lease, bool) {
	if limit <= 0 {
		return &Lease{}, true
	}
	if c.redis != nil {
		ok, err := c.redis.Acquire(ctx, key, member, limit, lease)
		if err == nil {
			if !ok {
				return nil, false
			}
			return &Lease{release: func() {
				rctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := c.redis.Release(rctx, key, member); err != nil {
					slog.Warn("归还 redis 并发位失败", "key", key, "err", err)
				}
			}}, true
		}
		slog.Warn("redis 并发限制失败，退回内存", "key", key, "err", err)
	}
	if !c.mem.Acquire(key, member, limit, lease) {
		return nil, false
	}
	return &Lease{release: func() { c.mem.Release(key, member) }}, true
}
