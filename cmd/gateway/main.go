// Command gateway 是进程入口。它只做组装：连数据库、连 Redis、灌入渠道和种子密钥、启动后台任务、监听 HTTP。
// 请求怎么走，看 internal/httpserver 的 chatCompletions。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/victorzhong0110/railhead/internal/auth"
	"github.com/victorzhong0110/railhead/internal/billing"
	"github.com/victorzhong0110/railhead/internal/breaker"
	"github.com/victorzhong0110/railhead/internal/cache"
	"github.com/victorzhong0110/railhead/internal/config"
	"github.com/victorzhong0110/railhead/internal/dispatch"
	"github.com/victorzhong0110/railhead/internal/httpserver"
	"github.com/victorzhong0110/railhead/internal/metrics"
	"github.com/victorzhong0110/railhead/internal/provider/mock"
	"github.com/victorzhong0110/railhead/internal/provider/openai"
	"github.com/victorzhong0110/railhead/internal/ratelimit"
	"github.com/victorzhong0110/railhead/internal/router"
	"github.com/victorzhong0110/railhead/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("配置无效", "err", err)
		os.Exit(1)
	}
	setupLog(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		slog.Error("解析数据库地址失败", "err", err)
		os.Exit(1)
	}
	if cfg.DatabaseMaxConns > 0 {
		poolCfg.MaxConns = int32(cfg.DatabaseMaxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		slog.Error("连接数据库失败", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		slog.Error("数据库不可用", "err", err)
		os.Exit(1)
	}

	st := store.New(pool)
	st.SetShards(cfg.BillingShards)
	if err := st.Migrate(ctx); err != nil {
		slog.Error("迁移失败", "err", err)
		os.Exit(1)
	}
	if len(cfg.Channels) > 0 {
		if err := st.UpsertChannels(ctx, cfg.Channels); err != nil {
			slog.Error("写入渠道失败", "err", err)
			os.Exit(1)
		}
	}
	if cfg.SeedAPIKey != "" {
		key, err := st.InsertKey(ctx, cfg.SeedAPIKey, cfg.SeedKeyName, cfg.SeedQuota, cfg.SeedRPM, cfg.SeedTPM, cfg.SeedConcurrency)
		if err != nil {
			slog.Error("写入种子密钥失败", "err", err)
			os.Exit(1)
		}
		slog.Info("种子密钥已就绪", "id", key.ID, "prefix", auth.Prefix(cfg.SeedAPIKey), "balance", key.QuotaBalance)
	}

	var rdb *redis.Client
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			slog.Error("解析 Redis 地址失败", "err", err)
			os.Exit(1)
		}
		rdb = redis.NewClient(opt)
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = rdb.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			// 不退出。限流和缓存会在每次调用失败时退回内存，Redis 恢复后自动再用。
			slog.Warn("Redis 当前不可用，限流将退回本进程内存", "err", err)
		}
	}

	var responseCache cache.Cache
	if rdb != nil {
		responseCache = cache.NewRedis(rdb)
	} else {
		responseCache = cache.NewMemory()
	}

	m := metrics.New()
	br := breaker.NewGroup(breaker.Options{
		Threshold:    cfg.BreakerThreshold,
		Cooldown:     cfg.BreakerCooldown,
		ProbeTimeout: cfg.BreakerProbeTimeout,
		OnState: func(name, state string) {
			m.Breaker(name, state)
			slog.Info("熔断器状态变化", "channel", name, "state", state)
		},
	})
	rt := router.New(
		&dispatch.Dispatcher{OpenAI: openai.New(), Mock: mock.New(mock.Options{})},
		br,
		m,
		cfg.MaxAttempts,
		cfg.RetryBase,
	)
	if err := reloadChannels(ctx, st, rt); err != nil {
		slog.Error("加载渠道失败", "err", err)
		os.Exit(1)
	}

	bill := billing.NewWithOptions(pool, billing.Options{
		Shards:      cfg.BillingShards,
		BatchWindow: cfg.BillingBatch,
	})
	srv := httpserver.New(httpserver.Deps{
		Config:  cfg,
		Store:   st,
		Billing: bill,
		Limiter: ratelimit.New(rdb),
		Conc:    ratelimit.NewConcurrency(rdb),
		Router:  rt,
		Cache:   responseCache,
		Metrics: m,
	})

	if cfg.EnablePprof {
		go servePprof(cfg.PprofAddr)
	}

	go reloadLoop(ctx, st, rt, cfg.ChannelReload)
	go sweepLoop(ctx, bill, cfg.StaleSweepInterval, cfg.StaleReservationAge)
	go breakerGaugeLoop(ctx, br, m)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("网关监听", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP 服务退出", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Error("关闭 HTTP 服务失败", "err", err)
	}
	slog.Info("网关已退出")
}

func setupLog(level string) {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})))
}

func reloadChannels(ctx context.Context, st *store.Store, rt *router.Router) error {
	channels, err := st.ListChannels(ctx)
	if err != nil {
		return err
	}
	rt.SetChannels(channels)
	slog.Info("渠道已加载", "count", len(channels))
	return nil
}

func reloadLoop(ctx context.Context, st *store.Store, rt *router.Router, every time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := reloadChannels(ctx, st, rt); err != nil && ctx.Err() == nil {
				slog.Warn("刷新渠道失败", "err", err)
			}
		}
	}
}

func sweepLoop(ctx context.Context, bill *billing.Service, every, age time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := bill.ReleaseStale(ctx, age, 0)
			if err != nil && ctx.Err() == nil {
				slog.Warn("清扫过期预扣失败", "err", err)
				continue
			}
			if n > 0 {
				slog.Warn("清扫了崩溃留下的预扣", "count", n, "older_than", age.String())
			}
		}
	}
}

func breakerGaugeLoop(ctx context.Context, br *breaker.Group, m *metrics.Metrics) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for name, state := range br.Snapshot() {
				m.Breaker(name, state)
			}
		}
	}
}

func servePprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	slog.Info("pprof 监听", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("pprof 退出", "err", err)
	}
}
