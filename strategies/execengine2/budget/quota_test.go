package budget

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

func TestQuotaPreservesInflightSpendsAndWindowEpoch(t *testing.T) {
	t.Parallel()
	q, err := NewQuota(100, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	q.now = func() time.Time { return now }
	token := q.Snapshot()
	if !q.Take(5, model.LimitNormal) {
		t.Fatal("initial sends denied")
	}
	// External account activity has reduced the broker snapshot to 50. These
	// five local sends occurred while that snapshot was still in flight.
	q.Set(50, now.Add(time.Minute), now, token)
	if got := q.Remaining(); got != 45 || q.Take(40, model.LimitNormal) {
		t.Fatalf("metrics lost in-flight sends or spent the hedge reserve: remaining=%d", got)
	}
	if wait := q.RetryAfter(40); wait != time.Minute {
		t.Fatalf("quota denial retry delay=%s, want broker reset in one minute", wait)
	}
	oldWindow := q.Snapshot()
	now = now.Add(2 * time.Minute)
	if !q.Take(1, model.LimitMust) || q.Remaining() != 99 {
		t.Fatal("first mandatory send did not charge the fresh window")
	}
	// Even a still-future resetAt cannot revive a metrics token from an older
	// local window. The next send must retain this window's existing debit.
	q.Set(0, now.Add(10*time.Second), now, oldWindow)
	now = now.Add(11 * time.Second)
	if !q.Take(1, model.LimitNormal) || q.Remaining() != 98 {
		t.Fatalf("old response corrupted the fresh window: remaining=%d", q.Remaining())
	}
	// A genuine new broker window can replenish the old remaining count.
	q.Set(100, now.Add(time.Minute), now, q.Snapshot())
	if q.Remaining() != 100 {
		t.Fatalf("fresh broker window was not adopted: remaining=%d", q.Remaining())
	}
}

func TestQuotaAtomicAdmissionAndMandatoryReserve(t *testing.T) {
	t.Parallel()
	q, err := NewQuota(100, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if q.Take(1, model.LimitNormal) {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 90 || q.Remaining() != 10 {
		t.Fatalf("shared admission spent reserve: admitted=%d remaining=%d", admitted.Load(), q.Remaining())
	}
	if !q.Take(11, model.LimitMust) || q.Remaining() != -1 {
		t.Fatal("mandatory hedge was blocked at the reserve")
	}
}
