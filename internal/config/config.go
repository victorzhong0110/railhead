// Package config 从环境变量读取进程配置。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// ChannelFile 是 CHANNELS_JSON / CHANNELS_FILE 里的一条渠道。
type ChannelFile struct {
	Name          string   `json:"name"`
	Kind          string   `json:"kind"`
	BaseURL       string   `json:"base_url"`
	APIKey        string   `json:"api_key"`
	Models        []string `json:"models"`
	UpstreamModel string   `json:"upstream_model,omitempty"`
	Priority      int      `json:"priority"`
	Weight        int      `json:"weight"`
	TimeoutMS     int      `json:"timeout_ms"`
	Enabled       *bool    `json:"enabled,omitempty"`
}

// Config 是进程级配置。限额为 0 表示这一项不限制。
type Config struct {
	HTTPAddr            string
	DatabaseURL         string
	DatabaseMaxConns    int
	RedisURL            string
	AdminToken          string
	SeedAPIKey          string
	SeedKeyName         string
	SeedQuota           int64
	SeedRPM             int
	SeedTPM             int
	SeedConcurrency     int
	Channels            []domain.Channel
	CacheEnabled        bool
	CacheTTL            time.Duration
	KeyCacheTTL         time.Duration
	MaxAttempts         int
	RetryBase           time.Duration
	RequestTimeout      time.Duration
	BreakerThreshold    int
	BreakerCooldown     time.Duration
	BreakerProbeTimeout time.Duration
	DefaultMaxTokens    int
	MaxTokensCap        int
	ReserveMargin       int
	ChannelReload       time.Duration
	StaleReservationAge time.Duration
	StaleSweepInterval  time.Duration
	// BillingShards 是每把密钥的余额行数。1 等价于所有请求抢同一行。
	BillingShards int
	// BillingBatch 大于 0 时，同一分片上的预扣会攒到这个窗口再一次提交。
	BillingBatch time.Duration
	LogLevel     string
	EnablePprof  bool
	PprofAddr    string
}

// Load 读取环境变量。缺省值按「本机 docker compose 能直接跑通」来选。
func Load() (Config, error) {
	c := Config{
		HTTPAddr:            getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		DatabaseMaxConns:    getenvInt("DATABASE_MAX_CONNS", 32),
		RedisURL:            os.Getenv("REDIS_URL"),
		AdminToken:          os.Getenv("ADMIN_TOKEN"),
		SeedAPIKey:          os.Getenv("SEED_API_KEY"),
		SeedKeyName:         getenv("SEED_KEY_NAME", "demo"),
		SeedQuota:           getenvInt64("SEED_QUOTA", 1_000_000),
		SeedRPM:             getenvInt("SEED_RPM", 600),
		SeedTPM:             getenvInt("SEED_TPM", 200_000),
		SeedConcurrency:     getenvInt("SEED_CONCURRENCY", 64),
		CacheEnabled:        getenvBool("CACHE_ENABLED", true),
		CacheTTL:            getenvDuration("CACHE_TTL", 60*time.Second),
		KeyCacheTTL:         getenvDuration("KEY_CACHE_TTL", 5*time.Second),
		MaxAttempts:         getenvInt("MAX_ATTEMPTS", 3),
		RetryBase:           getenvDuration("RETRY_BASE", 20*time.Millisecond),
		RequestTimeout:      getenvDuration("REQUEST_TIMEOUT", 120*time.Second),
		BreakerThreshold:    getenvInt("BREAKER_THRESHOLD", 5),
		BreakerCooldown:     getenvDuration("BREAKER_COOLDOWN", 15*time.Second),
		BreakerProbeTimeout: getenvDuration("BREAKER_PROBE_TIMEOUT", 30*time.Second),
		DefaultMaxTokens:    getenvInt("DEFAULT_MAX_TOKENS", 64),
		MaxTokensCap:        getenvInt("MAX_TOKENS_CAP", 4096),
		ReserveMargin:       getenvInt("RESERVE_MARGIN", 0),
		ChannelReload:       getenvDuration("CHANNEL_RELOAD", 5*time.Second),
		StaleReservationAge: getenvDuration("STALE_RESERVATION_AGE", 3*time.Minute),
		StaleSweepInterval:  getenvDuration("STALE_SWEEP_INTERVAL", 30*time.Second),
		BillingShards:       getenvInt("BILLING_SHARDS", 16),
		BillingBatch:        getenvDuration("BILLING_BATCH", 2*time.Millisecond),
		LogLevel:            getenv("LOG_LEVEL", "info"),
		EnablePprof:         getenvBool("ENABLE_PPROF", false),
		PprofAddr:           getenv("PPROF_ADDR", "127.0.0.1:16060"),
	}
	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL 不能为空")
	}
	channels, err := loadChannels()
	if err != nil {
		return Config{}, err
	}
	c.Channels = channels
	return c, nil
}

func loadChannels() ([]domain.Channel, error) {
	raw := os.Getenv("CHANNELS_JSON")
	if path := os.Getenv("CHANNELS_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取 CHANNELS_FILE: %w", err)
		}
		raw = string(b)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var file []ChannelFile
	if err := json.Unmarshal([]byte(raw), &file); err != nil {
		return nil, fmt.Errorf("解析渠道配置: %w", err)
	}
	out := make([]domain.Channel, 0, len(file))
	for _, ch := range file {
		if ch.Name == "" {
			return nil, fmt.Errorf("渠道缺少 name")
		}
		kind := ch.Kind
		if kind == "" {
			kind = "openai"
		}
		if kind != "openai" && kind != "mock" {
			return nil, fmt.Errorf("渠道 %s 的 kind 只能是 openai 或 mock", ch.Name)
		}
		enabled := true
		if ch.Enabled != nil {
			enabled = *ch.Enabled
		}
		timeout := 30 * time.Second
		if ch.TimeoutMS > 0 {
			timeout = time.Duration(ch.TimeoutMS) * time.Millisecond
		}
		weight := ch.Weight
		if weight < 1 {
			weight = 1
		}
		apiKey, err := expandEnv(ch.APIKey)
		if err != nil {
			return nil, fmt.Errorf("渠道 %s: %w", ch.Name, err)
		}
		out = append(out, domain.Channel{
			Name:          ch.Name,
			Kind:          kind,
			BaseURL:       ch.BaseURL,
			APIKey:        apiKey,
			Models:        ch.Models,
			UpstreamModel: ch.UpstreamModel,
			Priority:      ch.Priority,
			Weight:        weight,
			Timeout:       timeout,
			Enabled:       enabled,
		})
	}
	return out, nil
}

// expandEnv 只替换整段占位符 ${NAME}。密钥放在环境变量里，不写进仓库。
func expandEnv(s string) (string, error) {
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") && len(s) > 3 {
		name := s[2 : len(s)-1]
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "{} $") {
			return "", fmt.Errorf("api_key 占位符不合法")
		}
		v := os.Getenv(name)
		if v == "" {
			return "", fmt.Errorf("环境变量 %s 未设置", name)
		}
		return v, nil
	}
	return s, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
