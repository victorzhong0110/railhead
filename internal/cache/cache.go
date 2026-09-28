// Package cache 缓存非流式、温度为 0 的响应。
//
// 缓存键包含密钥 id。两把密钥问了同一句话，也不会读到对方的答案。
// 命中率会低一些，但不会把 A 用户的补全内容泄露给 B。这是有意的取舍。
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// Cache 是响应缓存。Get 的第二个返回值表示是否命中；出错时调用方应当成未命中，不要失败请求。
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Key 把「哪把密钥 + 哪些会影响补全的字段」哈希成一个缓存键。
func Key(apiKeyID int64, req domain.ChatRequest) (string, error) {
	canon := struct {
		KeyID       int64            `json:"key_id"`
		Model       string           `json:"model"`
		Messages    []domain.Message `json:"messages"`
		Temperature *float64         `json:"temperature,omitempty"`
		TopP        *float64         `json:"top_p,omitempty"`
		MaxTokens   *int             `json:"max_tokens,omitempty"`
		Stop        []string         `json:"stop,omitempty"`
	}{
		KeyID:       apiKeyID,
		Model:       req.Model,
		Messages:    req.Messages,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   req.MaxTokens,
		Stop:        req.Stop,
	}
	b, err := json.Marshal(canon)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "railhead:cache:" + hex.EncodeToString(sum[:]), nil
}

// Eligible 判断这次请求能不能进缓存。
// 只缓存非流式且 temperature 明确为 0 的请求。默认温度是 1，结果不稳定，缓存了会返回错误答案。
func Eligible(enabled bool, req domain.ChatRequest) bool {
	if !enabled || req.Stream {
		return false
	}
	return req.Temperature != nil && *req.Temperature == 0
}

// Memory 是单进程缓存，Redis 不可用时使用。
type Memory struct {
	mu  sync.Mutex
	m   map[string]memItem
	now func() time.Time
}

type memItem struct {
	val []byte
	exp time.Time
}

// NewMemory 创建内存缓存。
func NewMemory() *Memory {
	return &Memory{m: map[string]memItem{}, now: time.Now}
}

// Get 返回副本，调用方改字节不会污染缓存。
func (m *Memory) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.m[key]
	if !ok || !item.exp.After(m.now()) {
		delete(m.m, key)
		return nil, false, nil
	}
	return append([]byte(nil), item.val...), true, nil
}

// Set 写入副本。
func (m *Memory) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = memItem{val: append([]byte(nil), value...), exp: m.now().Add(ttl)}
	return nil
}

// Redis 使用 STRING 保存完整响应 JSON。
type Redis struct {
	rdb *redis.Client
}

// NewRedis 创建 Redis 缓存。
func NewRedis(rdb *redis.Client) *Redis {
	return &Redis{rdb: rdb}
}

// Get 在键不存在时返回未命中，而不是错误。
func (c *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Set 写入并设置过期时间。
func (c *Redis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	return c.rdb.Set(ctx, key, value, ttl).Err()
}
