package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketLua 与 Memory.Allow 对齐。时间用 Redis TIME，不信任调用方时钟。
// 返回 {1 或 0, 剩余整数令牌}。
const tokenBucketLua = `
local scale = 1000
local capacity = tonumber(ARGV[1]) * scale
local refill = tonumber(ARGV[2]) * scale
local interval = tonumber(ARGV[3])
local cost = tonumber(ARGV[4]) * scale
local ttl = tonumber(ARGV[5])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
else
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  if elapsed > 0 and interval > 0 and refill > 0 then
    local add = math.floor(elapsed * refill / interval)
    if add > 0 then
      tokens = math.min(capacity, tokens + add)
      local consumed = math.floor(add * interval / refill)
      ts = ts + consumed
    end
  end
end

local allowed = 0
if cost <= 0 or tokens >= cost then
  tokens = tokens - cost
  if tokens < 0 then tokens = 0 end
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, math.floor(tokens / scale)}
`

// concurrencyAcquireLua 用 ZSET 成员表示在飞请求，分数是过期时间戳。
// 崩溃后没人 ZREM，下一次获取会先把过期成员删掉，所以租约就是故障时的自动释放。
const concurrencyAcquireLua = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local limit = tonumber(ARGV[1])
local lease = tonumber(ARGV[2])
local member = ARGV[3]
local ttl = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local n = redis.call('ZCARD', KEYS[1])
if n >= limit then
  return 0
end
redis.call('ZADD', KEYS[1], now + lease, member)
redis.call('PEXPIRE', KEYS[1], ttl)
return 1
`

const concurrencyReleaseLua = `
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

type redisLimiter struct {
	rdb    *redis.Client
	script *redis.Script
}

func newRedisLimiter(rdb *redis.Client) *redisLimiter {
	if rdb == nil {
		return nil
	}
	return &redisLimiter{rdb: rdb, script: redis.NewScript(tokenBucketLua)}
}

func (r *redisLimiter) Allow(ctx context.Context, key string, cost, capacity, refill int, interval time.Duration) (bool, error) {
	ttl := interval * 2
	if ttl < time.Second {
		ttl = time.Second
	}
	res, err := r.script.Run(ctx, r.rdb, []string{key},
		capacity,
		refill,
		interval.Milliseconds(),
		cost,
		ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return false, err
	}
	if len(res) == 0 {
		return false, redis.Nil
	}
	allowed, ok := asInt(res[0])
	if !ok {
		return false, redis.Nil
	}
	return allowed == 1, nil
}

type redisConcurrency struct {
	rdb     *redis.Client
	acquire *redis.Script
	release *redis.Script
}

func newRedisConcurrency(rdb *redis.Client) *redisConcurrency {
	if rdb == nil {
		return nil
	}
	return &redisConcurrency{
		rdb:     rdb,
		acquire: redis.NewScript(concurrencyAcquireLua),
		release: redis.NewScript(concurrencyReleaseLua),
	}
}

func (r *redisConcurrency) Acquire(ctx context.Context, key, member string, limit int, lease time.Duration) (bool, error) {
	ttl := lease * 2
	if ttl < time.Second {
		ttl = time.Second
	}
	n, err := r.acquire.Run(ctx, r.rdb, []string{key},
		limit,
		lease.Milliseconds(),
		member,
		ttl.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (r *redisConcurrency) Release(ctx context.Context, key, member string) error {
	return r.release.Run(ctx, r.rdb, []string{key}, member).Err()
}

func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
