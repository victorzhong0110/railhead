// Package billing 实现「预扣 → 结算 / 退款」。
//
// 不变量（对每一把密钥都成立）：
//
//	quota_granted = SUM(quota_shards.balance) + Σ settled.charged + Σ reserved.reserved
//
// 每个分片的余额都有 CHECK (balance >= 0)。超卖在这里是硬错误。
// 少扣（unbilled）只会在「实扣超过预扣且所有分片都不够补」时出现。
//
// 并发：预扣只更新一个分片行（条件 UPDATE），不再锁 api_keys。
// 同一分片上的请求仍会排队，但一把密钥可以有多行，不同请求打到不同行。
// 单次预扣装不进任何一个分片时，才按分片号顺序把多行锁起来拼余额（慢路径）。
// 锁顺序始终是分片号从小到大，避免死锁。
//
// BILLING_BATCH 大于 0 时，同一个分片上几毫秒内的预扣合成一条 UPDATE。
// 合并发生在返回成功之前，崩溃会整批回滚，不会丢额度，也不会超卖。
package billing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/victorzhong0110/railhead/internal/domain"
)

var (
	// ErrInsufficientQuota 表示余额不够这次预扣。调用方应返回 429 insufficient_quota。
	ErrInsufficientQuota = errors.New("insufficient quota")
	// ErrKeyNotFound 表示密钥 id 不存在。
	ErrKeyNotFound = errors.New("api key not found")
	// ErrKeyDisabled 表示密钥被停用。
	ErrKeyDisabled = errors.New("api key disabled")
	// ErrDuplicateRequest 表示这个 request_id 已经预扣过。
	ErrDuplicateRequest = errors.New("duplicate request")
	// ErrNotFound 表示没有这张预扣单。
	ErrNotFound = errors.New("reservation not found")
)

// Service 执行扣费事务。
type Service struct {
	pool        *pgxpool.Pool
	shards      int
	batchWindow time.Duration
	lanes       laneMap
}

// Options 控制分片数和合并窗口。Shards <= 1 且 BatchWindow <= 0 时，行为就是「一行余额、一次一单」。
type Options struct {
	Shards      int
	BatchWindow time.Duration
}

// New 用已有连接池创建计费服务。默认单分片、不合并，给测试和旧路径用。
func New(pool *pgxpool.Pool) *Service {
	return NewWithOptions(pool, Options{})
}

// NewWithOptions 按配置创建计费服务。
func NewWithOptions(pool *pgxpool.Pool, opt Options) *Service {
	if opt.Shards < 1 {
		opt.Shards = 1
	}
	if opt.Shards > 64 {
		opt.Shards = 64
	}
	if opt.BatchWindow < 0 {
		opt.BatchWindow = 0
	}
	return &Service{pool: pool, shards: opt.Shards, batchWindow: opt.BatchWindow}
}

// Reserve 预扣 amount 个 token。成功后余额已经减少，调用方必须在结束时 Settle 或 Refund。
func (s *Service) Reserve(ctx context.Context, keyID int64, requestID string, amount int64, model string) error {
	if amount < 0 {
		return fmt.Errorf("预扣金额不能为负")
	}
	if requestID == "" {
		return fmt.Errorf("request_id 不能为空")
	}
	start := shardOf(requestID, s.shards)
	for i := 0; i < s.shards; i++ {
		shard := (start + i) % s.shards
		err := s.reserveOnShard(ctx, keyID, requestID, amount, model, shard)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errShardShort) {
			return err
		}
	}
	// 单个分片都装不下，但加起来可能够。慢路径按分片号顺序拼。
	return s.reserveSpill(ctx, keyID, requestID, amount, model)
}

// Settle 按实际用量结清一张预扣单。可以安全地调用多次：已经 settled 的单会直接返回。
//
// actual 是上游报告的总 token。它小于预扣时把差额退回；大于预扣时尝试补扣。
// 补扣不允许把余额打成负数，不够的部分记到 unbilled，留给对账和告警，而不是欠成负数。
//
// 如果清扫任务已经把这张单退款（进程曾被判断为崩溃），这里仍会按 actual 重新扣一次。
// 这样「清扫太激进」最多导致一次短暂的额度释放，请求真正成功后还会补上，不会免费送出。
func (s *Service) Settle(ctx context.Context, requestID string, actual int64, channel string, promptTokens, completionTokens int) error {
	if actual < 0 {
		actual = 0
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	keyID, reserved, status, shard, err := lockReservation(ctx, tx, requestID)
	if err != nil {
		return err
	}
	if status == "settled" {
		return tx.Commit(ctx)
	}

	charged, unbilled, err := settleOnShards(ctx, tx, keyID, shard, status, reserved, actual)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status='settled', charged=$1, unbilled=$2,
		    prompt_tokens=$3, completion_tokens=$4,
		    channel_name=$5, updated_at=now()
		WHERE request_id=$6
	`, charged, unbilled, promptTokens, completionTokens, channel, requestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// settleNumbers 把三种状态的金额算清楚，方便单测不连数据库也能覆盖边界。
func settleNumbers(status string, reserved, actual, balance int64) (charged, unbilled, newBalance int64) {
	switch status {
	case "reserved":
		if actual <= reserved {
			return actual, 0, balance + (reserved - actual)
		}
		extra := actual - reserved
		if extra <= balance {
			return actual, 0, balance - extra
		}
		charged = reserved + balance
		return charged, actual - charged, 0
	case "refunded":
		// 预扣已经全部退回，这次要按实际用量重新扣。
		if actual <= balance {
			return actual, 0, balance - actual
		}
		return balance, actual - balance, 0
	default:
		// 未知状态不当作成功。调用方会看到 panic 以外的路径：这里返回原余额和 0，
		// 但 Settle 在调用前已经排除了 settled。未知状态应在调用前拦住。
		return 0, actual, balance
	}
}

// Refund 把仍处于 reserved 的预扣全额退回。已结算或已退款的单保持原样。
func (s *Service) Refund(ctx context.Context, requestID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := refundTx(ctx, tx, requestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func refundTx(ctx context.Context, tx pgx.Tx, requestID string) error {
	keyID, reserved, status, shard, err := lockReservation(ctx, tx, requestID)
	if err != nil {
		return err
	}
	if status != "reserved" {
		return nil
	}
	// 先锁预扣单，再按分片号把钱退回。只动这一行分片，不锁整把密钥。
	if err := addShard(ctx, tx, keyID, shard, reserved); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE reservations
		SET status='refunded', charged=0, unbilled=0, updated_at=now()
		WHERE request_id=$1
	`, requestID)
	return err
}

func lockReservation(ctx context.Context, tx pgx.Tx, requestID string) (keyID, reserved int64, status string, shard int, err error) {
	err = tx.QueryRow(ctx, `
		SELECT api_key_id, reserved, status, shard FROM reservations WHERE request_id=$1 FOR UPDATE
	`, requestID).Scan(&keyID, &reserved, &status, &shard)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, "", 0, ErrNotFound
	}
	return keyID, reserved, status, shard, err
}

// ReleaseStale 退回创建时间早于 olderThan 的在途预扣。
// 用来处理「预扣成功之后进程崩溃，没有人来结算」的泄漏。
// olderThan 必须大于一次请求的最长生命周期，否则可能把还在跑的请求退掉。
// 即便退早了，随后的 Settle 也会按实际用量重新扣（见 Settle 对 refunded 的处理）。
// keyID 为 0 时清扫全部密钥；测试可以只清扫自己的密钥，避免并行测试互相退款。
func (s *Service) ReleaseStale(ctx context.Context, olderThan time.Duration, keyID int64) (int, error) {
	if olderThan < 0 {
		olderThan = 0
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT request_id FROM reservations
		WHERE status='reserved'
		  AND created_at < now() - ($1::bigint * interval '1 millisecond')
		  AND ($2::bigint = 0 OR api_key_id = $2)
		FOR UPDATE SKIP LOCKED
	`, olderThan.Milliseconds(), keyID)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, id := range ids {
		if err := refundTx(ctx, tx, id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

// Reconcile 计算不变量的偏差。Drift 为 0 表示这把密钥没有多扣、也没有丢额度。
func (s *Service) Reconcile(ctx context.Context, keyID int64) (domain.ReconcileResult, error) {
	var rec domain.ReconcileResult
	err := s.pool.QueryRow(ctx, `
		SELECT
			k.id,
			k.quota_granted,
			COALESCE((SELECT SUM(balance) FROM quota_shards s WHERE s.key_id = k.id), 0),
			COALESCE(SUM(r.charged) FILTER (WHERE r.status = 'settled'), 0),
			COALESCE(SUM(r.reserved) FILTER (WHERE r.status = 'reserved'), 0),
			COALESCE(SUM(r.unbilled) FILTER (WHERE r.status = 'settled'), 0),
			COALESCE(COUNT(*) FILTER (WHERE r.status = 'reserved'), 0),
			COALESCE(COUNT(*) FILTER (WHERE r.status = 'settled'), 0),
			COALESCE(COUNT(*) FILTER (WHERE r.status = 'refunded'), 0)
		FROM api_keys k
		LEFT JOIN reservations r ON r.api_key_id = k.id
		WHERE k.id = $1
		GROUP BY k.id
	`, keyID).Scan(
		&rec.APIKeyID,
		&rec.QuotaGranted,
		&rec.QuotaBalance,
		&rec.Settled,
		&rec.Inflight,
		&rec.Unbilled,
		&rec.ReservedCount,
		&rec.SettledCount,
		&rec.RefundedCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ReconcileResult{}, ErrKeyNotFound
	}
	if err != nil {
		return domain.ReconcileResult{}, err
	}
	rec.Drift = rec.QuotaGranted - (rec.QuotaBalance + rec.Settled + rec.Inflight)
	return rec, nil
}
