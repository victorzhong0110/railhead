// Package store 负责建表、密钥和渠道。扣费事务在 internal/billing，避免一个包同时背两套不变量。
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/victorzhong0110/railhead/internal/auth"
	"github.com/victorzhong0110/railhead/internal/domain"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// 多实例同时启动时用同一把会话级咨询锁串行化迁移。
const migrateLockID int64 = 84261001

// Store 是数据库访问入口。
type Store struct {
	pool   *pgxpool.Pool
	shards int
}

// New 包装一个连接池。默认 1 个分片，和「整把密钥一行余额」等价。
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, shards: 1}
}

// SetShards 决定新密钥和充值把额度摊到几行。必须和 billing.Options.Shards 一致。
func (s *Store) SetShards(n int) {
	if n < 1 {
		n = 1
	}
	if n > 64 {
		n = 64
	}
	s.shards = n
}

// Pool 给计费包使用。计费和密钥分属两个包，但共享同一个池。
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Migrate 按文件名顺序执行尚未应用的 SQL。重复调用是安全的。
// 多实例同时启动时，CREATE TABLE IF NOT EXISTS 仍可能在系统目录上冲突，
// 所以整段迁移先拿会话级咨询锁，再在同一条连接上建表。
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("获取迁移锁: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockID)

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("创建 schema_migrations: %w", err)
	}
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.applyMigration(ctx, conn, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, conn *pgxpool.Conn, name string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit(ctx)
	}
	body, err := migrationsFS.ReadFile("migrations/" + name)
	if err != nil {
		return err
	}
	for _, stmt := range splitSQL(string(body)) {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("执行 %s: %w", name, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING`, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func splitSQL(in string) []string {
	var b strings.Builder
	for _, line := range strings.Split(in, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	parts := strings.Split(b.String(), ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		stmt := strings.TrimSpace(p)
		if stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

// CreateKey 生成一把新密钥。明文只出现在返回值里。
func (s *Store) CreateKey(ctx context.Context, name string, quota int64, rpm, tpm, conc int) (string, domain.APIKey, error) {
	raw, err := auth.NewRawKey()
	if err != nil {
		return "", domain.APIKey{}, err
	}
	key, err := s.InsertKey(ctx, raw, name, quota, rpm, tpm, conc)
	if err != nil {
		return "", domain.APIKey{}, err
	}
	return raw, key, nil
}

// InsertKey 写入一把已知明文的密钥。种子密钥走这里；已存在则原样返回，不重置余额。
func (s *Store) InsertKey(ctx context.Context, raw, name string, quota int64, rpm, tpm, conc int) (domain.APIKey, error) {
	if quota < 0 {
		return domain.APIKey{}, fmt.Errorf("quota 不能为负")
	}
	hash := auth.Hash(raw)
	prefix := auth.Prefix(raw)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.APIKey{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		INSERT INTO api_keys (key_hash, key_prefix, name, quota_balance, quota_granted, rpm_limit, tpm_limit, concurrency_limit)
		VALUES ($1, $2, $3, $4, $4, $5, $6, $7)
		ON CONFLICT (key_hash) DO NOTHING
		RETURNING id
	`, hash, prefix, name, quota, rpm, tpm, conc)
	var id int64
	err = row.Scan(&id)
	if err == nil {
		if err := spreadQuotaTx(ctx, tx, id, quota, s.shards); err != nil {
			return domain.APIKey{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.APIKey{}, err
		}
		return s.GetKey(ctx, id)
	}
	if err != pgx.ErrNoRows {
		return domain.APIKey{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.APIKey{}, err
	}
	return s.FindByHash(ctx, hash)
}

// FindByHash 按哈希查找密钥。
func (s *Store) FindByHash(ctx context.Context, hash string) (domain.APIKey, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT k.id, k.key_hash, k.key_prefix, k.name,
		       COALESCE((SELECT SUM(balance) FROM quota_shards s WHERE s.key_id = k.id), k.quota_balance),
		       k.quota_granted, k.rpm_limit, k.tpm_limit, k.concurrency_limit, k.enabled
		FROM api_keys k WHERE k.key_hash=$1
	`, hash)
	key, err := scanKey(row)
	if err == pgx.ErrNoRows {
		return domain.APIKey{}, ErrNotFound
	}
	return key, err
}

// GetKey 按 id 读取密钥。
func (s *Store) GetKey(ctx context.Context, id int64) (domain.APIKey, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT k.id, k.key_hash, k.key_prefix, k.name,
		       COALESCE((SELECT SUM(balance) FROM quota_shards s WHERE s.key_id = k.id), k.quota_balance),
		       k.quota_granted, k.rpm_limit, k.tpm_limit, k.concurrency_limit, k.enabled
		FROM api_keys k WHERE k.id=$1
	`, id)
	key, err := scanKey(row)
	if err == pgx.ErrNoRows {
		return domain.APIKey{}, ErrNotFound
	}
	return key, err
}

// TopUp 同时增加余额和已授予额度，对账不变量保持不变。
func (s *Store) TopUp(ctx context.Context, id, amount int64) (domain.APIKey, error) {
	if amount <= 0 {
		return domain.APIKey{}, fmt.Errorf("充值金额必须为正")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.APIKey{}, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE api_keys
		SET quota_granted = quota_granted + $1
		WHERE id = $2
	`, amount, id)
	if err != nil {
		return domain.APIKey{}, err
	}
	if tag.RowsAffected() == 0 {
		return domain.APIKey{}, ErrNotFound
	}
	if err := spreadQuotaTx(ctx, tx, id, amount, s.shards); err != nil {
		return domain.APIKey{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE api_keys
		SET quota_balance = COALESCE((SELECT SUM(balance) FROM quota_shards WHERE key_id=$1), 0)
		WHERE id=$1
	`, id); err != nil {
		return domain.APIKey{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.APIKey{}, err
	}
	return s.GetKey(ctx, id)
}

// spreadQuotaTx 把 amount 个 token 摊到 shards 行上。已有行则累加。
func spreadQuotaTx(ctx context.Context, tx pgx.Tx, keyID, amount int64, shards int) error {
	if shards < 1 {
		shards = 1
	}
	if amount < 0 {
		return fmt.Errorf("quota 不能为负")
	}
	base := amount / int64(shards)
	rem := amount % int64(shards)
	for i := 0; i < shards; i++ {
		add := base
		if int64(i) < rem {
			add++
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota_shards (key_id, shard, balance) VALUES ($1, $2, $3)
			ON CONFLICT (key_id, shard) DO UPDATE SET balance = quota_shards.balance + EXCLUDED.balance
		`, keyID, i, add); err != nil {
			return err
		}
	}
	return nil
}

func scanKey(row pgx.Row) (domain.APIKey, error) {
	var k domain.APIKey
	err := row.Scan(&k.ID, &k.KeyHash, &k.KeyPrefix, &k.Name, &k.QuotaBalance, &k.QuotaGranted, &k.RPMLimit, &k.TPMLimit, &k.ConcurrencyLimit, &k.Enabled)
	return k, err
}

// UpsertChannels 按 name 写入渠道配置。重复启动会更新地址和优先级，不会复制出第二行。
func (s *Store) UpsertChannels(ctx context.Context, channels []domain.Channel) error {
	for _, ch := range channels {
		timeoutMS := int(ch.Timeout.Milliseconds())
		if timeoutMS <= 0 {
			timeoutMS = 30000
		}
		weight := ch.Weight
		if weight < 1 {
			weight = 1
		}
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO channels (name, kind, base_url, api_key, models, upstream_model, priority, weight, timeout_ms, enabled)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (name) DO UPDATE SET
				kind = EXCLUDED.kind,
				base_url = EXCLUDED.base_url,
				api_key = EXCLUDED.api_key,
				models = EXCLUDED.models,
				upstream_model = EXCLUDED.upstream_model,
				priority = EXCLUDED.priority,
				weight = EXCLUDED.weight,
				timeout_ms = EXCLUDED.timeout_ms,
				enabled = EXCLUDED.enabled,
				updated_at = now()
		`, ch.Name, ch.Kind, ch.BaseURL, ch.APIKey, strings.Join(ch.Models, ","), ch.UpstreamModel, ch.Priority, weight, timeoutMS, ch.Enabled); err != nil {
			return fmt.Errorf("写入渠道 %s: %w", ch.Name, err)
		}
	}
	return nil
}

// ListChannels 返回全部渠道，优先级高的在前。
func (s *Store) ListChannels(ctx context.Context) ([]domain.Channel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, kind, base_url, api_key, models, upstream_model, priority, weight, timeout_ms, enabled
		FROM channels
		ORDER BY priority DESC, name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Channel
	for rows.Next() {
		var ch domain.Channel
		var models string
		var timeoutMS int
		if err := rows.Scan(&ch.ID, &ch.Name, &ch.Kind, &ch.BaseURL, &ch.APIKey, &models, &ch.UpstreamModel, &ch.Priority, &ch.Weight, &timeoutMS, &ch.Enabled); err != nil {
			return nil, err
		}
		ch.Models = splitModels(models)
		ch.Timeout = time.Duration(timeoutMS) * time.Millisecond
		out = append(out, ch)
	}
	return out, rows.Err()
}

func splitModels(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ErrNotFound 表示密钥不存在。
var ErrNotFound = fmt.Errorf("api key not found")
