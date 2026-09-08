package finambroker

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"QuantCore/strategies/execengine"
	"QuantCore/trade/quota"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPlacerCriticalCarriesTypedSeverity pins that p.critical routes through modlog's
// typed Level (see execengine's mirror test) rather than a hand-spelled "CRITICAL:"
// prefix.
func TestPlacerCriticalCarriesTypedSeverity(t *testing.T) {
	var buf bytes.Buffer
	mlog.SetOutput(&buf)
	defer mlog.SetOutput(os.Stderr)

	p := &placer{logTag: "[test]"}
	p.critical("client id %s UNRESOLVED", "abc123")

	got := buf.String()
	if !strings.Contains(got, "[CRITICAL]") || !strings.Contains(got, "client id abc123 UNRESOLVED") {
		t.Fatalf("critical output = %q, want a [CRITICAL] tag and the formatted message", got)
	}
}

// The placer closes the LOST-RESPONSE race: a placement whose RPC dies in transport is
// resolved by client id against the account's ACTIVE order list before anything is
// reported to the engine — "placed, here is its id" when found, or the original ambiguous
// error left unchanged when absent (GetOrders excludes terminal orders, so absence is
// inconclusive, never a confirmed non-placement — see trade/finam.GetOrders).

// testPlacer builds a placer with injected seams and a deterministic clock.
func testPlacer(place func(cid string) (*orders.OrderState, error), find func(cid string) (string, bool, error)) *placer {
	p := &placer{
		nonce: "abc123",
		sleep: func(time.Duration) {},
		nowFn: func() time.Time { return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC) },
	}
	p.place = func(_ orderKind, _ string, _ int, _ float64, cid string) (*orders.OrderState, error) {
		return place(cid)
	}
	p.find = func(cid, _ string, _ bool, _ int) (string, bool, error) { return find(cid) }
	return p
}

func TestPlacerSuccessSkipsResolution(t *testing.T) {
	finds := 0
	p := testPlacer(
		func(cid string) (*orders.OrderState, error) {
			if cid == "" {
				t.Fatal("every placement must carry a client id")
			}
			return &orders.OrderState{OrderId: "ord1"}, nil
		},
		func(string) (string, bool, error) { finds++; return "", false, nil },
	)
	id, err := p.placeResolved(kindLimitBid, "SI", 2, 100)
	if err != nil || id != "ord1" {
		t.Fatalf("id=%q err=%v, want ord1/nil", id, err)
	}
	if finds != 0 {
		t.Fatal("a clean success must not consult the order list")
	}
}

func TestPlacerDefinitiveRejectionSkipsResolution(t *testing.T) {
	finds := 0
	rej := status.Error(codes.InvalidArgument, "price out of band")
	p := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, rej },
		func(string) (string, bool, error) { finds++; return "", false, nil },
	)
	if _, err := p.placeResolved(kindLimitBid, "SI", 2, 100); !errors.Is(err, rej) {
		t.Fatalf("a business rejection must propagate as-is, got %v", err)
	}
	if finds != 0 {
		t.Fatal("a definitive broker answer needs no resolution — nothing was placed")
	}
}

// The core of the race: the RPC timed out but the order DID land. The placer must find it
// by client id and hand it to the engine as a normal, tracked placement.
func TestPlacerAmbiguousErrorAdoptsDeliveredGhost(t *testing.T) {
	var placedCID string
	p := testPlacer(
		func(cid string) (*orders.OrderState, error) {
			placedCID = cid
			return nil, status.Error(codes.DeadlineExceeded, "rpc timeout")
		},
		func(cid string) (string, bool, error) {
			if cid != placedCID {
				t.Fatalf("resolution must probe the placed client id, got %q want %q", cid, placedCID)
			}
			return "ghost7", true, nil
		},
	)
	id, err := p.placeResolved(kindMarketBuy, "UF", 2, 0)
	if err != nil || id != "ghost7" {
		t.Fatalf("the delivered ghost must be adopted: id=%q err=%v", id, err)
	}
}

func TestPlacerAmbiguousErrorAbsentFromActiveListFails(t *testing.T) {
	rpcErr := status.Error(codes.Unavailable, "connection reset")
	p := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, rpcErr },
		func(string) (string, bool, error) { return "", false, nil }, // not in the ACTIVE list
	)
	if _, err := p.placeResolved(kindLimitAsk, "SI", 2, 101); !errors.Is(err, rpcErr) {
		t.Fatalf("absence from the active list must fail with the original error, got %v", err)
	}
}

// A placement that timed out in transport but actually landed AND already filled (or was
// cancelled) before the resolution probe ran is INDISTINGUISHABLE, from GetOrders alone,
// from one that never reached the broker — GetOrders excludes terminal orders. The original
// ambiguous error must still propagate, unpromoted to a definitive rejection: execengine's
// MaybeDelivered must keep classifying it as ambiguous, so the reject-retry ladder does NOT
// shrink-and-retry (which would double the position on top of the real fill).
func TestPlacerTimeoutThenAlreadyExecutedGhostIsNotConfirmedAbsent(t *testing.T) {
	rpcErr := status.Error(codes.DeadlineExceeded, "placement rpc timeout")
	p := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, rpcErr },
		// The order filled and left the active list before every probe ran — same
		// observation GetOrders would report for "never placed".
		func(string) (string, bool, error) { return "", false, nil },
	)
	_, err := p.placeResolved(kindMarketBuy, "SI", 3, 0)
	if !errors.Is(err, rpcErr) {
		t.Fatalf("must fail with the original error, got %v", err)
	}
	if !execengine.MaybeDelivered(err) {
		t.Fatal("an order absent only from the ACTIVE list must stay ambiguous — promoting it to definitive would let the reject-retry ladder double the real fill")
	}
}

// The order list answering only on the LAST probe still resolves (the ghost needed a
// moment to appear); an order list that never answers across every round reports a
// distinct unresolved failure so the log trail shows the ambiguity.
func TestPlacerResolutionRetriesAndUnresolvedIsLoud(t *testing.T) {
	calls := 0
	p := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, status.Error(codes.Unavailable, "down") },
		func(string) (string, bool, error) {
			calls++
			if calls < ghostProbes {
				return "", false, errors.New("orders list down too")
			}
			return "late9", true, nil
		},
	)
	id, err := p.placeResolved(kindMarketSell, "UF", 1, 0)
	if err != nil || id != "late9" {
		t.Fatalf("a late-appearing ghost must still be adopted: id=%q err=%v", id, err)
	}

	calls = 0
	p2 := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, status.Error(codes.Unavailable, "down") },
		func(string) (string, bool, error) { calls++; return "", false, errors.New("orders list down too") },
	)
	_, err = p2.placeResolved(kindMarketSell, "UF", 1, 0)
	if err == nil {
		t.Fatal("an unresolvable placement must fail")
	}
	if want := ghostProbes * cidRetryRounds; calls != want {
		t.Fatalf("resolution must exhaust every round's probes before giving up, got %d want %d", calls, want)
	}
}

// The core of the same-cid fix: every placement retry and every resolution probe across
// ALL rounds reuses the ORIGINAL client id — placeResolved must never mint a new one while
// still trying to resolve an ambiguous outcome, since a fresh id on a retry is exactly what
// risked placing a second, independently-tracked order beside one that actually landed.
func TestPlacerRetriesPlacementWithTheSameClientID(t *testing.T) {
	var placeCIDs, findCIDs []string
	placements := 0
	p := testPlacer(
		func(cid string) (*orders.OrderState, error) {
			placeCIDs = append(placeCIDs, cid)
			placements++
			if placements < cidRetryRounds {
				return nil, status.Error(codes.Unavailable, "down")
			}
			return &orders.OrderState{OrderId: "landed-on-retry"}, nil
		},
		func(cid string) (string, bool, error) {
			findCIDs = append(findCIDs, cid)
			return "", false, nil // every probe comes back clean and absent
		},
	)
	id, err := p.placeResolved(kindMarketBuy, "SI", 2, 0)
	if err != nil || id != "landed-on-retry" {
		t.Fatalf("a placement that lands on a same-cid retry must be adopted: id=%q err=%v", id, err)
	}
	if placements != cidRetryRounds {
		t.Fatalf("placements = %d, want %d (must succeed on the last round, not retry past it)", placements, cidRetryRounds)
	}
	allCIDs := append(append([]string{}, placeCIDs...), findCIDs...)
	for _, cid := range allCIDs {
		if cid != placeCIDs[0] {
			t.Fatalf("every placement/probe must reuse the original client id %q, got %q", placeCIDs[0], cid)
		}
	}
}

// If every same-cid retry keeps coming back ambiguous but resolution DOES confirm absence
// each time, placeResolved must give up after cidRetryRounds — not retry forever — and
// report the ORIGINAL ambiguity, not a promoted definitive rejection.
func TestPlacerExhaustsCidRetryRoundsWhenNeverResolved(t *testing.T) {
	placements := 0
	p := testPlacer(
		func(string) (*orders.OrderState, error) {
			placements++
			return nil, status.Error(codes.DeadlineExceeded, "rpc timeout")
		},
		func(string) (string, bool, error) { return "", false, nil },
	)
	_, err := p.placeResolved(kindMarketBuy, "SI", 2, 0)
	if !execengine.MaybeDelivered(err) {
		t.Fatal("exhausting cidRetryRounds must stay ambiguous, not become a definitive rejection")
	}
	if placements != cidRetryRounds {
		t.Fatalf("placements = %d, want %d", placements, cidRetryRounds)
	}
}

func TestPlacerClientIDsUniqueAndWithinCap(t *testing.T) {
	p := testPlacer(nil, nil)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		cid := p.nextClientID()
		if len(cid) > 20 {
			t.Fatalf("client id %q exceeds the API's 20-char cap", cid)
		}
		if seen[cid] {
			t.Fatalf("client id %q minted twice", cid)
		}
		seen[cid] = true
	}
}

// TestPlacerNextClientIDConcurrentlyUnique proves nextClientID is safe to call from
// multiple goroutines at once: taker-only mode mints both legs' client id off the same
// *placer while racing their first attempt, so a plain p.seq++ (a non-atomic
// read-modify-write) risks a torn increment or a collision.
func TestPlacerNextClientIDConcurrentlyUnique(t *testing.T) {
	p := testPlacer(nil, nil)
	const n = 100
	ids := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ids[i] = p.nextClientID()
		}()
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("client id %q minted twice under concurrent calls", id)
		}
		seen[id] = true
	}
}

// placeResolved's log lines must carry the adapter's logTag in the actual log output —
// the bug this package split fixed: a bare mlog.Printf with no access to
// EngineConfig.LogTag made ambiguous-placement and CRITICAL-unresolved lines
// indistinguishable between two strategies sharing execengine.log.
func TestPlacerLogLinesCarryLogTag(t *testing.T) {
	var buf bytes.Buffer
	orig := mlog.Writer()
	mlog.SetOutput(&buf)
	t.Cleanup(func() { mlog.SetOutput(orig) })

	p := testPlacer(
		func(string) (*orders.OrderState, error) { return nil, status.Error(codes.Unavailable, "down") },
		func(string) (string, bool, error) { return "", false, errors.New("orders list down too") },
	)
	p.logTag = "[strategy_a]"
	if _, err := p.placeResolved(kindLimitBid, "SI", 2, 100); err == nil {
		t.Fatal("setup: expected the unresolved-placement error path")
	}

	out := buf.String()
	if !strings.Contains(out, "[execengine][strategy_a]") {
		t.Fatalf("log output missing the strategy tag, got:\n%s", out)
	}
	if !strings.Contains(out, "CRITICAL") {
		t.Fatalf("log output missing the CRITICAL unresolved line, got:\n%s", out)
	}
}

// setOnlyQuota implements ONLY QuotaUpdater's own two methods — not execengine.Limiter's
// Allow/Spend — to prove RefreshQuota is wired against the narrow QuotaUpdater surface, not
// the concrete quota.QuotaLimiter type or the full Limiter interface.
type setOnlyQuota struct {
	remaining int
	resetAt   time.Time
	token     quota.QuotaToken
}

func (s *setOnlyQuota) Snapshot() quota.QuotaToken { return s.token }

func (s *setOnlyQuota) Set(remaining int, resetAt, _ time.Time, token quota.QuotaToken) {
	s.remaining, s.resetAt, s.token = remaining, resetAt, token
}

var _ QuotaUpdater = (*setOnlyQuota)(nil)
var _ QuotaUpdater = (*quota.QuotaLimiter)(nil)
