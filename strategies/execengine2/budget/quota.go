package budget

import (
	"errors"
	"sync"
	"time"

	"QuantCore/strategies/execengine"
	"QuantCore/strategies/execengine2/internal/model"
)

// Quota adapts the existing quota window and metrics accounting to SendLimit.
// The outer mutex makes normal Take's Allow+Spend atomic against sends/metrics.
// Separate Allow and mandatory Take preserve v1's check-then-book contract.
type Quota struct {
	mu      sync.Mutex
	limiter *execengine.QuotaLimiter
	now     func() time.Time
}

// NewQuota keeps reserve attempts for mandatory hedges and restores limit each
// window even when metrics are unavailable. Feed it with finambroker.RefreshQuota.
func NewQuota(limit, reserve int, window time.Duration) (*Quota, error) {
	if limit <= 0 || reserve < 0 || window <= 0 {
		return nil, errors.New("quota needs a positive limit/window and nonnegative reserve")
	}
	q := &Quota{limiter: execengine.NewQuotaLimiterBudget(reserve, limit, window), now: time.Now}
	q.limiter.Spend(q.now(), 0)
	return q, nil
}

// Allow preserves v1's discretionary burst gate without spending the burst.
func (q *Quota) Allow(ops int64) bool {
	if ops <= 0 {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	ok, _ := q.limiter.Allow(q.now(), int(ops))
	return ok
}

// RetryAfter translates the broker window into a delay for the event clock.
func (q *Quota) RetryAfter(ops int64) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	_, retryAt := q.limiter.Allow(now, int(ops))
	return retryAt.Sub(now)
}

// Take charges every admitted attempt; mandatory hedges may use the reserve.
func (q *Quota) Take(ops int64, class model.LimitKind) bool {
	if ops <= 0 {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	if class != model.LimitMust {
		if ok, _ := q.limiter.Allow(now, int(ops)); !ok {
			return false
		}
	}
	q.limiter.Spend(now, int(ops))
	return true
}

// Snapshot must be captured before starting a quota metrics request.
func (q *Quota) Snapshot() execengine.QuotaToken {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.limiter.Snapshot()
}

// Set retains sends made during the request and rejects superseded windows.
func (q *Quota) Set(remaining int, resetAt, now time.Time, token execengine.QuotaToken) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.limiter.Set(remaining, resetAt, now, token)
}

func (q *Quota) Remaining() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	remaining, _ := q.limiter.Remaining()
	return int64(remaining)
}
