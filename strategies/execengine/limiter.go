package execengine

import (
	"time"

	"QuantCore/trade/quota"
)

// noLimit is the default permissive limiter (unit tests): every burst is allowed. The
// engine's failure backoff remains the safety net against request storms.
type noLimit struct{}

func (noLimit) Allow(time.Time, int) (bool, time.Time) { return true, time.Time{} }
func (noLimit) Spend(time.Time, int)                   {}

// Preserve the v1 quota API while both engines share the same implementation.
const (
	DefaultPlaceOrderBudget = quota.DefaultPlaceOrderBudget
	DefaultQuotaWindow      = quota.DefaultQuotaWindow
)

type QuotaLimiter = quota.QuotaLimiter
type QuotaToken = quota.QuotaToken

func NewQuotaLimiter(margin int) *QuotaLimiter { return quota.NewQuotaLimiter(margin) }

func NewQuotaLimiterBudget(margin, windowLimit int, window time.Duration) *QuotaLimiter {
	return quota.NewQuotaLimiterBudget(margin, windowLimit, window)
}
