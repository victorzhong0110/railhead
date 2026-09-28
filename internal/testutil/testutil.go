// Package testutil 给集成测试提供数据库和密钥。没有 DATABASE_URL 时跳过，而不是失败。
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/store"
)

// Pool 连接 DATABASE_URL 并执行迁移。
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.New(pool).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return pool
}

// MustKey 创建一把额度固定的密钥。名字带上时间，避免测试之间撞名（名字本身不唯一，只是方便看）。
func MustKey(t *testing.T, pool *pgxpool.Pool, name string, quota int64, rpm, tpm, conc int) domain.APIKey {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, key, err := store.New(pool).CreateKey(ctx, fmt.Sprintf("%s-%d", name, time.Now().UnixNano()), quota, rpm, tpm, conc)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
