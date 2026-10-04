// Package cache 缓存非流式、温度为 0 的响应。
//
// 缓存键包含密钥 id。两把密钥问了同一句话，也不会读到对方的答案。
// 命中率会低一些，但不会把 A 用户的补全内容泄露给 B。这是有意的取舍。
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
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
	// 原来先 json.Marshal 出一个中间 []byte（turns=16 时 7.6 KB、67 次分配），
	// 再交给 sha256。缓存键每个可缓存请求都要算一次，是热路径。
	// 这里直接序列化进哈希的输入缓冲，省掉中间切片。
	// appendCanonical 必须与原先的 json.Marshal 产生**完全相同的字节**，
	// 否则缓存键会变。TestManualMatchesJSON 用 3000 组随机输入（含引号、
	// 反斜杠、emoji、控制字符、边界浮点、nil/空值）逐字节比对来守住这一点。
	buf := appendCanonical(make([]byte, 0, canonicalSizeHint(req)), apiKeyID, req)
	sum := sha256.Sum256(buf)
	return "railhead:cache:" + hex.EncodeToString(sum[:]), nil
}

func canonicalSizeHint(req domain.ChatRequest) int {
	n := 64 + len(req.Model)
	for _, m := range req.Messages {
		n += 48 + len(m.Role) + len(m.Content.Text) + len(m.ReasoningContent)
	}
	for _, s := range req.Stop {
		n += len(s) + 4
	}
	return n
}

// appendCanonical 按 canon 的字段顺序与 json 标签逐字节重建序列化结果。
// omitempty 语义：指针为 nil 或切片为空则整个字段跳过。
func appendCanonical(dst []byte, apiKeyID int64, req domain.ChatRequest) []byte {
	dst = append(dst, `{"key_id":`...)
	dst = strconv.AppendInt(dst, apiKeyID, 10)
	dst = append(dst, `,"model":`...)
	dst = appendJSONString(dst, req.Model)
	dst = append(dst, `,"messages":`...)
	if req.Messages == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i, m := range req.Messages {
			if i > 0 {
				dst = append(dst, ',')
			}
			// Message 有三个字段：role、content（自定义 MarshalJSON）、
			// reasoning_content（omitempty）。
			dst = append(dst, `{"role":`...)
			dst = appendJSONString(dst, m.Role)
			dst = append(dst, `,"content":`...)
			ct, _ := m.Content.MarshalJSON()
			dst = append(dst, ct...)
			if m.ReasoningContent != "" {
				dst = append(dst, `,"reasoning_content":`...)
				dst = appendJSONString(dst, m.ReasoningContent)
			}
			dst = append(dst, '}')
		}
		dst = append(dst, ']')
	}
	if req.Temperature != nil {
		dst = append(dst, `,"temperature":`...)
		dst = appendJSONFloat(dst, *req.Temperature)
	}
	if req.TopP != nil {
		dst = append(dst, `,"top_p":`...)
		dst = appendJSONFloat(dst, *req.TopP)
	}
	if req.MaxTokens != nil {
		dst = append(dst, `,"max_tokens":`...)
		dst = strconv.AppendInt(dst, int64(*req.MaxTokens), 10)
	}
	if len(req.Stop) > 0 {
		dst = append(dst, `,"stop":[`...)
		for i, s := range req.Stop {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, s)
		}
		dst = append(dst, ']')
	}
	return append(dst, '}')
}

// appendJSONFloat 与 encoding/json 对 float64 的处理一致。
//
// encoding/json 的 floatEncoder 用 'g' 格式化后还有一步指数修正
// （json/encode.go 的 "clean up e-09 to e-9"）：
//   - 指数小于 -6 或大于 21 时改用 'e' 格式
//   - 去掉指数里的前导零（e-07 -> e-7）
//
// 漏掉这一步会让 1e-7 序列化成 "1e-07" 而 json 给的是 "1e-7"，
// 缓存键就会静默改变。TestCanonicalMatchesJSON 专门用 1e-7 覆盖了这个分支。
func appendJSONFloat(dst []byte, f float64) []byte {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		// 原实现里 json.Marshal 在这里返回 error，Key 返回 ("", err)，
		// 调用方 chat.go:78 把 cacheable 置 false。
		// NaN/Inf 不会出现在 HTTP JSON 请求体里，保留此分支仅防御。
		return append(dst, '0')
	}

	abs := math.Abs(f)
	fmtByte := byte('f')
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmtByte = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, f, fmtByte, -1, 64)
	if fmtByte == 'e' {
		// 去掉指数部分的前导零：1e-07 -> 1e-7
		n := len(dst)
		if n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}

// appendJSONString 与 encoding/json 对 string 的转义规则一致。
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, []byte(fmt.Sprintf(`\u%04x`, c))...)
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
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
