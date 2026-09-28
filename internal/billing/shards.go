package billing

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// errShardShort 表示这一分片装不下本次预扣，调用方应换下一片。
var errShardShort = errors.New("shard short")

func shardOf(requestID string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(requestID))
	return int(h.Sum32() % uint32(n))
}

func (s *Service) reserveOnShard(ctx context.Context, keyID int64, requestID string, amount int64, model string, shard int) error {
	if s.batchWindow > 0 {
		return s.reserveBatched(ctx, keyID, requestID, amount, model, shard)
	}
	return s.reserveDirect(ctx, keyID, requestID, amount, model, shard)
}

func (s *Service) reserveDirect(ctx context.Context, keyID int64, requestID string, amount int64, model string, shard int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := ensureEnabled(ctx, tx, keyID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota_shards
		SET balance = balance - $1
		WHERE key_id=$2 AND shard=$3 AND balance >= $1
	`, amount, keyID, shard)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errShardShort
	}
	if err := insertReservation(ctx, tx, requestID, keyID, model, amount, shard); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// reserveSpill 在所有分片都单独装不下时，按分片号顺序把余额拼起来扣。
// 锁顺序固定为 shard 升序，和只锁一行的快路径不会形成死锁：快路径只持有一行。
func (s *Service) reserveSpill(ctx context.Context, keyID int64, requestID string, amount int64, model string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := ensureEnabled(ctx, tx, keyID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT shard, balance FROM quota_shards
		WHERE key_id=$1
		ORDER BY shard
		FOR UPDATE
	`, keyID)
	if err != nil {
		return err
	}
	type piece struct {
		shard   int
		balance int64
	}
	var parts []piece
	var sum int64
	for rows.Next() {
		var p piece
		if err := rows.Scan(&p.shard, &p.balance); err != nil {
			rows.Close()
			return err
		}
		parts = append(parts, p)
		sum += p.balance
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if sum < amount {
		return ErrInsufficientQuota
	}
	need := amount
	refundShard := 0
	if len(parts) > 0 {
		refundShard = parts[0].shard
	}
	for _, p := range parts {
		if need == 0 {
			break
		}
		take := p.balance
		if take > need {
			take = need
		}
		if take == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota_shards SET balance = balance - $1 WHERE key_id=$2 AND shard=$3
		`, take, keyID, p.shard); err != nil {
			return err
		}
		need -= take
	}
	if err := insertReservation(ctx, tx, requestID, keyID, model, amount, refundShard); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func ensureEnabled(ctx context.Context, tx pgx.Tx, keyID int64) error {
	var enabled bool
	err := tx.QueryRow(ctx, `SELECT enabled FROM api_keys WHERE id=$1`, keyID).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}
	if !enabled {
		return ErrKeyDisabled
	}
	return nil
}

func insertReservation(ctx context.Context, tx pgx.Tx, requestID string, keyID int64, model string, amount int64, shard int) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO reservations (request_id, api_key_id, model, reserved, status, shard)
		VALUES ($1, $2, $3, $4, 'reserved', $5)
	`, requestID, keyID, model, amount, shard)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrDuplicateRequest
	}
	return err
}

func addShard(ctx context.Context, tx pgx.Tx, keyID int64, shard int, delta int64) error {
	if delta == 0 {
		return nil
	}
	if delta < 0 {
		return fmt.Errorf("addShard 只接受非负增量")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota_shards SET balance = balance + $1 WHERE key_id=$2 AND shard=$3
	`, delta, keyID, shard)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO quota_shards (key_id, shard, balance) VALUES ($1, $2, $3)
	`, keyID, shard, delta)
	return err
}

// settleOnShards 按实际用量调整分片余额。actual <= reserved 时把差额退回预扣所在的分片。
// actual 更大时，从该分片再扣，不够就按分片号去别的分片补，仍然不够的部分记为 unbilled。
func settleOnShards(ctx context.Context, tx pgx.Tx, keyID int64, shard int, status string, reserved, actual int64) (charged, unbilled int64, err error) {
	switch status {
	case "reserved":
		if actual <= reserved {
			if err := addShard(ctx, tx, keyID, shard, reserved-actual); err != nil {
				return 0, 0, err
			}
			return actual, 0, nil
		}
		extra := actual - reserved
		left, err := takeAcross(ctx, tx, keyID, shard, extra)
		if err != nil {
			return 0, 0, err
		}
		return actual - left, left, nil
	case "refunded":
		left, err := takeAcross(ctx, tx, keyID, shard, actual)
		if err != nil {
			return 0, 0, err
		}
		return actual - left, left, nil
	default:
		return 0, actual, fmt.Errorf("无法结算状态 %s", status)
	}
}

// takeAcross 从 preferred 分片开始扣 amount，不够再按分片号锁住其余行。返回没扣到的部分。
func takeAcross(ctx context.Context, tx pgx.Tx, keyID int64, preferred int, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota_shards SET balance = balance - $1
		WHERE key_id=$2 AND shard=$3 AND balance >= $1
	`, amount, keyID, preferred)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 1 {
		return 0, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT shard, balance FROM quota_shards
		WHERE key_id=$1
		ORDER BY shard
		FOR UPDATE
	`, keyID)
	if err != nil {
		return 0, err
	}
	type piece struct {
		shard   int
		balance int64
	}
	var parts []piece
	for rows.Next() {
		var p piece
		if err := rows.Scan(&p.shard, &p.balance); err != nil {
			rows.Close()
			return 0, err
		}
		parts = append(parts, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	need := amount
	for _, p := range parts {
		if need == 0 {
			break
		}
		take := p.balance
		if take > need {
			take = need
		}
		if take == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota_shards SET balance = balance - $1 WHERE key_id=$2 AND shard=$3
		`, take, keyID, p.shard); err != nil {
			return 0, err
		}
		need -= take
	}
	return need, nil
}

// --- 合并提交 ---

type laneMap struct {
	mu    sync.Mutex
	lanes map[laneKey]*lane
}

type laneKey struct {
	keyID int64
	shard int
}

type reserveItem struct {
	ctx       context.Context
	requestID string
	amount    int64
	model     string
	result    chan error
}

type lane struct {
	svc       *Service
	keyID     int64
	shard     int
	mu        sync.Mutex
	queue     []reserveItem
	scheduled bool
}

func (s *Service) reserveBatched(ctx context.Context, keyID int64, requestID string, amount int64, model string, shard int) error {
	item := reserveItem{
		ctx:       ctx,
		requestID: requestID,
		amount:    amount,
		model:     model,
		result:    make(chan error, 1),
	}
	s.lane(keyID, shard).enqueue(item, s.batchWindow)
	select {
	case err := <-item.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) lane(keyID int64, shard int) *lane {
	s.lanes.mu.Lock()
	defer s.lanes.mu.Unlock()
	if s.lanes.lanes == nil {
		s.lanes.lanes = map[laneKey]*lane{}
	}
	k := laneKey{keyID: keyID, shard: shard}
	ln := s.lanes.lanes[k]
	if ln == nil {
		ln = &lane{svc: s, keyID: keyID, shard: shard}
		s.lanes.lanes[k] = ln
	}
	return ln
}

func (l *lane) enqueue(item reserveItem, window time.Duration) {
	l.mu.Lock()
	l.queue = append(l.queue, item)
	if !l.scheduled {
		l.scheduled = true
		time.AfterFunc(window, l.flush)
	}
	l.mu.Unlock()
}

func (l *lane) flush() {
	l.mu.Lock()
	batch := l.queue
	l.queue = nil
	l.scheduled = false
	l.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	l.svc.flushReserve(l.keyID, l.shard, batch)
}

func (s *Service) flushReserve(keyID int64, shard int, batch []reserveItem) {
	live := make([]reserveItem, 0, len(batch))
	for _, item := range batch {
		if item.ctx.Err() != nil {
			item.result <- item.ctx.Err()
			continue
		}
		live = append(live, item)
	}
	if len(live) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted, rejected, fatal, err := s.flushReserveTx(ctx, keyID, shard, live)
	if err != nil {
		// 整批回滚之后逐条重试。逐条路径会把「装不下」和真正的错误区分开。
		for _, item := range live {
			item.result <- s.reserveDirect(item.ctx, keyID, item.requestID, item.amount, item.model, shard)
		}
		return
	}
	for _, item := range accepted {
		item.result <- nil
	}
	sendErr := fatal
	if sendErr == nil {
		sendErr = errShardShort
	}
	for _, item := range rejected {
		item.result <- sendErr
	}
}

func (s *Service) flushReserveTx(ctx context.Context, keyID int64, shard int, batch []reserveItem) (accepted, rejected []reserveItem, fatal, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback(ctx)
	if err := ensureEnabled(ctx, tx, keyID); err != nil {
		return nil, batch, err, nil
	}
	var balance int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM quota_shards WHERE key_id=$1 AND shard=$2 FOR UPDATE
	`, keyID, shard).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		balance = 0
		err = nil
	}
	if err != nil {
		return nil, nil, nil, err
	}
	var take int64
	remain := balance
	for _, item := range batch {
		if item.amount <= remain {
			remain -= item.amount
			take += item.amount
			accepted = append(accepted, item)
		} else {
			rejected = append(rejected, item)
		}
	}
	if take > 0 {
		tag, err := tx.Exec(ctx, `
			UPDATE quota_shards SET balance = balance - $1
			WHERE key_id=$2 AND shard=$3 AND balance >= $1
		`, take, keyID, shard)
		if err != nil {
			return nil, nil, nil, err
		}
		if tag.RowsAffected() != 1 {
			return nil, nil, nil, errShardShort
		}
		if err := insertReservationBatch(ctx, tx, keyID, shard, accepted); err != nil {
			return nil, nil, nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, nil, err
	}
	return accepted, rejected, nil, nil
}

func insertReservationBatch(ctx context.Context, tx pgx.Tx, keyID int64, shard int, items []reserveItem) error {
	if len(items) == 0 {
		return nil
	}
	var b strings.Builder
	args := make([]any, 0, len(items)*5)
	b.WriteString(`INSERT INTO reservations (request_id, api_key_id, model, reserved, status, shard) VALUES `)
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		base := i * 5
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,'reserved',$%d)", base+1, base+2, base+3, base+4, base+5)
		args = append(args, item.requestID, keyID, item.model, item.amount, shard)
	}
	_, err := tx.Exec(ctx, b.String(), args...)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrDuplicateRequest
	}
	return err
}
