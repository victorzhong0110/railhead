// Package httpserver 是网关的 HTTP 层：鉴权、OpenAI 兼容接口、管理接口和指标。
// 业务规则（选渠道、扣费、限流）都在各自的包里，这个包只负责把它们按请求生命周期串起来。
package httpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/victorzhong0110/railhead/internal/auth"
	"github.com/victorzhong0110/railhead/internal/billing"
	"github.com/victorzhong0110/railhead/internal/cache"
	"github.com/victorzhong0110/railhead/internal/config"
	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/metrics"
	"github.com/victorzhong0110/railhead/internal/ratelimit"
	"github.com/victorzhong0110/railhead/internal/router"
	"github.com/victorzhong0110/railhead/internal/store"
)

type ctxKey int

const (
	ctxKeyID ctxKey = iota
	ctxAPIKey
)

// Server 是已经组装好的网关。
type Server struct {
	cfg     config.Config
	store   *store.Store
	billing *billing.Service
	limiter *ratelimit.Limiter
	conc    *ratelimit.Concurrency
	router  *router.Router
	cache   cache.Cache
	metrics *metrics.Metrics
	keys    *keyCache
}

// Deps 是 main 和测试共用的依赖。
type Deps struct {
	Config  config.Config
	Store   *store.Store
	Billing *billing.Service
	Limiter *ratelimit.Limiter
	Conc    *ratelimit.Concurrency
	Router  *router.Router
	Cache   cache.Cache
	Metrics *metrics.Metrics
}

// New 组装 HTTP 服务。
func New(d Deps) *Server {
	if d.Cache == nil {
		d.Cache = cache.NewMemory()
	}
	if d.Metrics == nil {
		d.Metrics = metrics.New()
	}
	ttl := d.Config.KeyCacheTTL
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return &Server{
		cfg:     d.Config,
		store:   d.Store,
		billing: d.Billing,
		limiter: d.Limiter,
		conc:    d.Conc,
		router:  d.Router,
		cache:   d.Cache,
		metrics: d.Metrics,
		keys:    newKeyCache(ttl),
	}
}

// Handler 返回带恢复、请求号和访问日志的路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics.Handler().ServeHTTP)
	mux.HandleFunc("GET /v1/models", s.withUser(s.listModels))
	mux.HandleFunc("POST /v1/chat/completions", s.withUser(s.chatCompletions))
	mux.HandleFunc("POST /admin/keys", s.withAdmin(s.createKey))
	mux.HandleFunc("GET /admin/keys/{id}", s.withAdmin(s.getKey))
	mux.HandleFunc("POST /admin/keys/{id}/topup", s.withAdmin(s.topup))
	mux.HandleFunc("GET /admin/keys/{id}/reconcile", s.withAdmin(s.reconcile))
	mux.HandleFunc("POST /admin/channels/reload", s.withAdmin(s.reloadChannels))
	return recoverMW(requestIDMW(accessLogMW(mux)))
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.store.Pool().Ping(ctx); err != nil {
		http.Error(w, "database not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) listModels(w http.ResponseWriter, r *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	seen := map[string]struct{}{}
	var data []model
	for _, name := range s.modelNames() {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		data = append(data, model{ID: name, Object: "model", OwnedBy: "railhead"})
	}
	if data == nil {
		data = []model{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) withUser(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw := bearer(r.Header.Get("Authorization"))
		if raw == "" {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "缺少 Authorization: Bearer 密钥")
			return
		}
		key, err := s.lookupKey(r.Context(), raw)
		if err != nil || !key.Enabled {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "密钥无效或已停用")
			return
		}
		ctx := context.WithValue(r.Context(), ctxAPIKey, key)
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) lookupKey(ctx context.Context, raw string) (domain.APIKey, error) {
	hash := auth.Hash(raw)
	if key, ok := s.keys.get(hash); ok {
		return key, nil
	}
	key, err := s.store.FindByHash(ctx, hash)
	if err != nil {
		return domain.APIKey{}, err
	}
	s.keys.set(hash, key)
	return key, nil
}

func (s *Server) withAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(r.Header.Get("X-Admin-Token"), s.cfg.AdminToken) {
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_admin_token", "管理令牌无效")
			return
		}
		next(w, r)
	}
}

func adminOK(got, want string) bool {
	if want == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func bearer(header string) string {
	const p = "Bearer "
	if len(header) <= len(p) || header[:len(p)] != p {
		return ""
	}
	return header[len(p):]
}

func apiKeyFrom(ctx context.Context) domain.APIKey {
	key, _ := ctx.Value(ctxAPIKey).(domain.APIKey)
	return key
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyID).(string)
	if id == "" {
		return uuid.NewString()
	}
	return id
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(domain.ErrorResponse{Error: domain.APIError{Message: msg, Type: typ, Code: code}})
}

func requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), ctxKeyID, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic", "err", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "panic", "网关内部错误")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func accessLogMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(sw, r)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		// 成功请求默认不打日志。高 QPS 下每条 info 日志会变成瓶颈。
		if status >= 400 || slog.Default().Enabled(r.Context(), slog.LevelDebug) {
			slog.Debug("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"latency_ms", time.Since(start).Milliseconds(),
				"request_id", requestIDFrom(r.Context()),
			)
			if status >= 500 {
				slog.Error("http",
					"method", r.Method,
					"path", r.URL.Path,
					"status", status,
					"latency_ms", time.Since(start).Milliseconds(),
					"request_id", requestIDFrom(r.Context()),
				)
			}
		}
	})
}

type keyCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]keyEntry
}

type keyEntry struct {
	key domain.APIKey
	exp time.Time
}

func newKeyCache(ttl time.Duration) *keyCache {
	return &keyCache{ttl: ttl, m: map[string]keyEntry{}}
}

func (c *keyCache) get(hash string) (domain.APIKey, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[hash]
	if !ok || time.Now().After(e.exp) {
		delete(c.m, hash)
		return domain.APIKey{}, false
	}
	return e.key, true
}

func (c *keyCache) set(hash string, key domain.APIKey) {
	c.mu.Lock()
	c.m[hash] = keyEntry{key: key, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

func (s *Server) modelNames() []string {
	var names []string
	for _, ch := range s.router.Channels() {
		if !ch.Enabled {
			continue
		}
		for _, name := range ch.Models {
			if name == "" || name == "*" {
				continue
			}
			names = append(names, name)
		}
	}
	return names
}
