package execengine2

import (
	"context"
	"testing"
	"time"
)

type reservationRetryLimit struct {
	*hedgeRetryLimit
	retryOps int64
}

func (l *reservationRetryLimit) RetryAfter(ops int64) time.Duration {
	l.retryOps = ops
	return time.Second
}

// A reservation-only limit may also supply a retry delay. Solo modes must ask
// about the one reservation they denied, not Admitter's two-slot hedge headroom.
func TestReservationRetryUsesOpeningOrderCount(t *testing.T) {
	for _, mode := range []Mode{ModeLimitA, ModeLimitB, ModeTwoLimits, ModeMarket} {
		e, broker, limit, _ := newHedgeRetryTest(t, 0, 1)
		limit.left = 0
		retry := &reservationRetryLimit{hedgeRetryLimit: limit}
		e.limit = retry
		count := orderCount(mode)
		if err := e.openLimitsOrMarket(context.Background(), Plan{Action: 1}, mode, 1, count, time.Now()); err != nil {
			t.Fatal(err)
		}
		if retry.retryOps != int64(count) || len(broker.requests) != 0 {
			t.Fatalf("mode=%v: retry requested %d attempts, want %d; placements=%v", mode, retry.retryOps, count, broker.requests)
		}
	}
}
