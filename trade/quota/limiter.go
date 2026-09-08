// Package quota owns shared broker-window quota accounting, independent of execution engines.
package quota

import (
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultPlaceOrderBudget is Finam's documented per-minute placeOrder quota. It is
	// the FAIL-SAFE bootstrap budget a QuotaLimiter self-manages until (and if ever) the
	// usage-metrics refresher reports the broker's real remaining count — so opens are
	// gated to keep the hedge reserve even when the refresher has not polled yet, is
	// misconfigured, or has died. Conservative by construction (≤ the real limit): a
	// wrong-low guess only throttles opens, it never starves a mandatory hedge.
	DefaultPlaceOrderBudget = 200
	// DefaultQuotaWindow is the broker's quota window (200 requests per MINUTE).
	DefaultQuotaWindow = time.Minute
)

// QuotaLimiter tracks the broker's remaining order-placement budget for the current
// window and keeps `margin` ops in reserve so a burst of clip opens can never consume the
// quota a mandatory taker hedge needs. A background refresher calls Set with fresh numbers
// from the usage-metrics RPC; between refreshes the limiter keeps its own running count:
// the engine calls Spend for EVERY placement RPC it actually issues — gated clip opens and
// re-pegs, but also the ungated taker hedges/top-ups and failed attempts, which a broker
// poll alone would only reveal seconds later.
//
// FAIL-SAFE: when constructed with a window budget (NewQuotaLimiterBudget), the limiter is
// self-sufficient — it bootstraps from that budget and re-bootstraps every window, so it
// NEVER blind-permits opens (the old fail-OPEN default that let a fast burst starve a
// hedge into a naked leg). A real Set only refines the estimate; if the refresher never
// runs or dies, the self-managed budget still reserves the hedge headroom. Constructed with
// NewQuotaLimiter (no budget) it keeps the legacy behaviour: permissive until the first Set.
//
// It is concurrency-safe: Set runs on the refresher goroutine, Allow/Spend on the engine
// goroutine.
type QuotaLimiter struct {
	mu           sync.Mutex
	margin       int
	windowLimit  int           // >0 → fail-safe self-managed budget; 0 → legacy (permissive until Set)
	window       time.Duration // self-reset period for the bootstrap budget
	remaining    int
	resetAt      time.Time
	known        bool  // a REAL broker Set has arrived (authoritative); false → bootstrap/legacy
	totalSpent   int64 // monotonic count of every op ever booked via Spend; never reset
	lastSetToken int64 // totalSpent of the last Set actually APPLIED, to reject a late out-of-order response
	epoch        int64 // bumped on every ACTUAL window rollover (refreshWindow); see QuotaToken
}

// QuotaToken is an opaque snapshot of the limiter's bookkeeping state, returned by
// Snapshot() before an outbound usage-metrics RPC and handed back to Set() alongside that
// RPC's answer. spent lets Set re-subtract ops that landed while the RPC was in flight.
// epoch lets Set tell whether the window rolled over locally while the RPC was in flight —
// totalSpent alone cannot: a rollover with zero spends before it leaves totalSpent
// unchanged, so two different epochs can share the same spent value. When a rollover DOES
// happen between Snapshot and Set, the answer describes an already-superseded window, and
// no amount of ops-arithmetic can reconcile a remaining count across that boundary — it
// must be rejected outright, not adjusted.
type QuotaToken struct {
	spent int64
	epoch int64
}

// NewQuotaLimiter builds a legacy QuotaLimiter (permissive until the first broker Set)
// that keeps margin ops in reserve once quota data arrives. Prefer NewQuotaLimiterBudget
// for live trading so opens are gated even before/without a usage-metrics refresher.
func NewQuotaLimiter(margin int) *QuotaLimiter { return &QuotaLimiter{margin: margin} }

// NewQuotaLimiterBudget builds a FAIL-SAFE QuotaLimiter that self-manages windowLimit ops
// per window (re-bootstrapping every window) until/while the broker refresher feeds real
// numbers via Set — so a mandatory hedge is never starved by opens even if the refresher
// is absent or dies. margin ops stay in reserve for hedges/cancels/refresh-lag.
func NewQuotaLimiterBudget(margin, windowLimit int, window time.Duration) *QuotaLimiter {
	if window <= 0 {
		window = DefaultQuotaWindow
	}
	return &QuotaLimiter{margin: margin, windowLimit: windowLimit, window: window}
}

// Snapshot returns a QuotaToken: the caller (RefreshQuota) reads it BEFORE issuing the
// usage-metrics RPC, then hands it to Set alongside the RPC's answer.
func (q *QuotaLimiter) Snapshot() QuotaToken {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QuotaToken{spent: q.totalSpent, epoch: q.epoch}
}

// Set installs the latest known remaining budget and window reset from the broker. now is
// the PROCESSING clock reading at apply time (same domain as Allow/Spend — see clock.go);
// token is what Snapshot returned before the RPC that produced this answer was issued.
//
// Set is authoritative but not blind:
//   - A token older than the last one actually applied means this is a late, out-of-order
//     response (RefreshQuota's poll loop is strictly sequential, but a defensive guard costs
//     nothing) — ignored.
//   - A resetAt that has already passed by the time this Set arrives carries a dead number:
//     ignored, deferring to refreshWindow's self-heal on the next Allow/Spend instead of
//     adopting an already-stale window. This guard only applies in fail-safe mode
//     (windowLimit>0) — legacy mode has no self-heal to defer to.
//   - A token whose epoch is behind the CURRENT epoch means the window rolled over locally
//     (refreshWindow, from an ordinary Spend/Allow) while this RPC was still in flight — the
//     usage-metrics RPC can take up to quotaRPCTimeout, easily longer than a fail-safe
//     window's own rollover cadence near a boundary. The answer describes an already-
//     superseded window: rejected outright, not adjusted — re-subtracting ops spent in the
//     NEW window from the OLD window's remaining is meaningless arithmetic, and applying it
//     anyway would walk resetAt backwards, so the next Allow/Spend sees it as elapsed and
//     full-resets the budget, erasing every op the true, newer window already spent. (A
//     broker resetAt merely earlier than the local bootstrap GUESS, with no rollover in
//     between, is a legitimate correction — see TestQuotaLimiterSetOverridesBootstrap — so
//     resetAt comparison alone cannot be the discriminator; the epoch is.)
//   - Ops booked via Spend since token are re-subtracted from remaining: a Spend that raced
//     the metrics RPC is real and must survive.
//   - Within the SAME epoch (resetAt unchanged from the current one), the result is clamped
//     to never exceed the local view — a broker snapshot cannot un-know a spend local
//     tracking already recorded. A NEW epoch is not clamped: a fresh window's full budget
//     does not inherit the old epoch's low remaining.
func (q *QuotaLimiter) Set(remaining int, resetAt, now time.Time, token QuotaToken) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.known && token.spent < q.lastSetToken {
		return // a late, out-of-order poll response — a newer one already landed
	}
	if q.windowLimit > 0 && !resetAt.After(now) {
		return // this answer's own window had already rolled over by the time it arrived
	}
	if q.windowLimit > 0 && token.epoch < q.epoch {
		return // a local rollover already superseded the epoch this answer describes
	}
	computed := remaining - int(q.totalSpent-token.spent)
	if q.known && q.resetAt.Equal(resetAt) && computed > q.remaining {
		computed = q.remaining // same epoch: never raise our belief above local tracking
	}
	q.remaining, q.resetAt, q.known, q.lastSetToken = computed, resetAt, true, token.spent
}

// refreshWindow re-bootstraps the self-managed budget when its window has elapsed (fail-safe
// mode only). It also self-heals a dead refresher: once the broker-reported resetAt passes
// with no fresh Set, the window is assumed rolled over and the budget restored. Caller holds
// mu. Bumps epoch on every actual rollover — see QuotaToken.
func (q *QuotaLimiter) refreshWindow(now time.Time) {
	if q.windowLimit <= 0 {
		return // legacy limiter: no self-managed budget
	}
	if q.resetAt.IsZero() || !now.Before(q.resetAt) {
		q.remaining = q.windowLimit
		q.resetAt = now.Add(q.window)
		q.epoch++
	}
}

// Allow reports whether ops discretionary placements fit the remaining budget with the
// margin still in reserve. It only checks — the decrement happens in Spend, at the moment
// an RPC is actually issued — so a granted-but-never-placed op is not lost from the budget.
func (q *QuotaLimiter) Allow(now time.Time, ops int) (bool, time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.refreshWindow(now) // fail-safe: lazily restore the budget when the window rolls over
	if !q.known && q.windowLimit <= 0 {
		return true, time.Time{} // legacy, no data yet — rely on the engine's failure backoff
	}
	if q.remaining >= ops+q.margin {
		return true, q.resetAt
	}
	return false, q.resetAt
}

// Spend books ops placement RPCs the engine actually issued against the budget. Failed
// attempts count too — the broker metered the request whether or not the order stuck.
// It rolls the bootstrap window over (same as Allow) BEFORE debiting: Spend is also the
// ungated taker-hedge path, which never calls Allow first, so a hedge landing as the
// first RPC after a window boundary must still debit the FRESH window — otherwise a
// later Allow's own rollover would silently reset remaining and forget this spend.
func (q *QuotaLimiter) Spend(now time.Time, ops int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.totalSpent += int64(ops)        // monotonic — Set reads this to re-subtract spends that raced its RPC
	q.refreshWindow(now)              // fail-safe: lazily restore the budget when the window rolls over
	if q.known || q.windowLimit > 0 { // track in both real and bootstrap modes
		q.remaining -= ops
	}
}

// Remaining reports the current local view of the remaining budget (the last broker
// refresh, or the self-managed bootstrap, minus everything spent since) and whether the
// number is meaningful yet (real data OR a bootstrap budget).
func (q *QuotaLimiter) Remaining() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.remaining, q.known || q.windowLimit > 0
}

// String renders the local remaining-budget view for status blocks: the number, or "n/a"
// until the first broker refresh has arrived (legacy limiter with no bootstrap).
func (q *QuotaLimiter) String() string {
	rem, ok := q.Remaining()
	if !ok {
		return "n/a"
	}
	return strconv.Itoa(rem)
}
