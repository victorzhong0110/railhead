package billing

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/store"
	"github.com/victorzhong0110/railhead/internal/testutil"
)

func TestSettleNumbers(t *testing.T) {
	cases := []struct {
		name                          string
		status                        string
		reserved, actual, balance     int64
		charged, unbilled, newBalance int64
	}{
		{"exact", "reserved", 10, 10, 0, 10, 0, 0},
		{"refund unused", "reserved", 10, 4, 0, 4, 0, 6},
		{"extra fits", "reserved", 10, 15, 20, 15, 0, 15},
		{"extra clamped", "reserved", 10, 50, 5, 15, 35, 0},
		{"after refund", "refunded", 10, 7, 100, 7, 0, 93},
		{"after refund clamped", "refunded", 10, 40, 12, 12, 28, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			charged, unbilled, bal := settleNumbers(tc.status, tc.reserved, tc.actual, tc.balance)
			if charged != tc.charged || unbilled != tc.unbilled || bal != tc.newBalance {
				t.Fatalf("got charged=%d unbilled=%d balance=%d", charged, unbilled, bal)
			}
			if bal < 0 {
				t.Fatal("balance went negative")
			}
		})
	}
}

func TestReserveSettleRefundReconcile(t *testing.T) {
	pool := testutil.Pool(t)
	svc := New(pool)
	ctx := context.Background()
	key := testutil.MustKey(t, pool, "bill-basic", 100, 0, 0, 0)

	r1 := fmt.Sprintf("r1-%d", key.ID)
	r2 := fmt.Sprintf("r2-%d", key.ID)
	if err := svc.Reserve(ctx, key.ID, r1, 40, "m"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Reserve(ctx, key.ID, r1, 40, "m"); err == nil {
		t.Fatal("duplicate request id must fail")
	}
	rec, err := svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	if rec.Inflight != 40 || rec.QuotaBalance != 60 {
		t.Fatalf("after reserve: %+v", rec)
	}

	if err := svc.Settle(ctx, r1, 25, "ch", 10, 15); err != nil {
		t.Fatal(err)
	}
	if err := svc.Settle(ctx, r1, 25, "ch", 10, 15); err != nil {
		t.Fatal("second settle should be a no-op")
	}
	rec, err = svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	if rec.Settled != 25 || rec.QuotaBalance != 75 || rec.Inflight != 0 {
		t.Fatalf("after settle: %+v", rec)
	}

	if err := svc.Reserve(ctx, key.ID, r2, 30, "m"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Refund(ctx, r2); err != nil {
		t.Fatal(err)
	}
	if err := svc.Refund(ctx, r2); err != nil {
		t.Fatal("second refund should be a no-op")
	}
	rec, err = svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	// 第一笔已经结算掉 25，第二笔全额退回，余额应回到 75 而不是最初的 100。
	if rec.QuotaBalance != 75 || rec.Settled != 25 || rec.RefundedCount != 1 {
		t.Fatalf("after refund: %+v", rec)
	}
}

func TestSettleClampsWhenActualExceedsReserve(t *testing.T) {
	pool := testutil.Pool(t)
	svc := New(pool)
	ctx := context.Background()
	key := testutil.MustKey(t, pool, "bill-clamp", 50, 0, 0, 0)
	c1 := fmt.Sprintf("c1-%d", key.ID)
	if err := svc.Reserve(ctx, key.ID, c1, 40, "m"); err != nil {
		t.Fatal(err)
	}
	// 余额只剩 10，实际用量 80，超出预扣 40。只能再补 10，unbilled=30，余额归零。
	if err := svc.Settle(ctx, c1, 80, "ch", 10, 70); err != nil {
		t.Fatal(err)
	}
	rec, err := svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	if rec.QuotaBalance != 0 || rec.Settled != 50 || rec.Unbilled != 30 {
		t.Fatalf("clamp result: %+v", rec)
	}
}

func TestConcurrentReserveDoesNotOverspend(t *testing.T) {
	pool := testutil.Pool(t)
	svc := New(pool)
	ctx := context.Background()
	const (
		granted = int64(100)
		cost    = int64(30)
		n       = 30
	)
	key := testutil.MustKey(t, pool, "bill-race", granted, 0, 0, 0)

	var success atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("race-%d-%d", key.ID, i)
			err := svc.Reserve(ctx, key.ID, id, cost, "m")
			if err == nil {
				success.Add(1)
				if serr := svc.Settle(ctx, id, cost, "ch", 10, 20); serr != nil {
					t.Errorf("settle %s: %v", id, serr)
				}
				return
			}
			if err != ErrInsufficientQuota {
				t.Errorf("reserve %s: %v", id, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	rec, err := svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	got := success.Load()
	if got > 3 {
		t.Fatalf("successes=%d, 30*4=120 exceeds granted 100", got)
	}
	if rec.QuotaBalance < 0 {
		t.Fatalf("negative balance: %+v", rec)
	}
	if rec.QuotaBalance+rec.Settled != granted || rec.Inflight != 0 {
		t.Fatalf("books do not balance: successes=%d %+v", got, rec)
	}
	if int64(got)*cost+rec.QuotaBalance != granted {
		t.Fatalf("overspend or leak: successes=%d %+v", got, rec)
	}
}

func TestReleaseStaleRestoresBalance(t *testing.T) {
	pool := testutil.Pool(t)
	svc := New(pool)
	ctx := context.Background()
	key := testutil.MustKey(t, pool, "bill-stale", 80, 0, 0, 0)
	staleID := fmt.Sprintf("stale-%d", key.ID)
	if err := svc.Reserve(ctx, key.ID, staleID, 80, "m"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	n, err := svc.ReleaseStale(ctx, 0, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("released %d", n)
	}
	rec, err := svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	if rec.QuotaBalance != 80 || rec.Inflight != 0 {
		t.Fatalf("after sweep: %+v", rec)
	}
	// 清扫退款之后请求其实成功了，结算必须把用量补扣回来。
	if err := svc.Settle(ctx, staleID, 20, "ch", 8, 12); err != nil {
		t.Fatal(err)
	}
	rec, err = svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	if rec.QuotaBalance != 60 || rec.Settled != 20 {
		t.Fatalf("settle after refund: %+v", rec)
	}
}

func TestShardedBatchDoesNotOverspend(t *testing.T) {
	pool := testutil.Pool(t)
	const shards = 8
	st := storeWithShards(t, pool, shards)
	svc := NewWithOptions(pool, Options{Shards: shards, BatchWindow: 3 * time.Millisecond})
	ctx := context.Background()
	const (
		granted = int64(240)
		cost    = int64(30)
		n       = 40
	)
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, key, err := st.CreateKey(ctx2, fmt.Sprintf("bill-shard-%d", time.Now().UnixNano()), granted, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	var success atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("sh-%d-%d", key.ID, i)
			err := svc.Reserve(ctx, key.ID, id, cost, "m")
			if err == nil {
				success.Add(1)
				if serr := svc.Settle(ctx, id, cost, "ch", 10, 20); serr != nil {
					t.Errorf("settle %s: %v", id, serr)
				}
				return
			}
			if err != ErrInsufficientQuota {
				t.Errorf("reserve %s: %v", id, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	rec, err := svc.Reconcile(ctx, key.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDrift(t, rec)
	got := int64(success.Load())
	if got*cost > granted {
		t.Fatalf("overspend successes=%d granted=%d", got, granted)
	}
	if rec.QuotaBalance+rec.Settled != granted || rec.Inflight != 0 {
		t.Fatalf("books do not balance: successes=%d %+v", got, rec)
	}
}

func storeWithShards(t *testing.T, pool *pgxpool.Pool, n int) *store.Store {
	t.Helper()
	st := store.New(pool)
	st.SetShards(n)
	return st
}

func assertDrift(t *testing.T, rec domain.ReconcileResult) {
	t.Helper()
	if rec.Drift != 0 {
		t.Fatalf("drift=%d result=%+v", rec.Drift, rec)
	}
	if rec.QuotaBalance < 0 {
		t.Fatalf("negative balance: %+v", rec)
	}
}
