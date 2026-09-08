package execengine2_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	v1 "QuantCore/strategies/execengine"
	v2 "QuantCore/strategies/execengine2"
)

// This is the same Allow/Take adapter as budget.Quota, with an injected clock
// so a broker return can deterministically cross a quota window boundary.
type parityClockQuota struct {
	*v1.QuotaLimiter
	clock *parityClock
}

func (q parityClockQuota) Allow(n int64) bool {
	ok, _ := q.QuotaLimiter.Allow(q.clock.Now(), int(n))
	return ok
}
func (q parityClockQuota) RetryAfter(n int64) time.Duration {
	now := q.clock.Now()
	_, retryAt := q.QuotaLimiter.Allow(now, int(n))
	return retryAt.Sub(now)
}
func (q parityClockQuota) Take(n int64, kind v2.LimitKind) bool {
	if kind != v2.LimitMust && !q.Allow(n) {
		return false
	}
	q.Spend(q.clock.Now(), int(n))
	return true
}
func (q parityClockQuota) Remaining() int64 {
	n, _ := q.QuotaLimiter.Remaining()
	return int64(n)
}

type parityWindowBrokerV1 struct {
	parityBrokerV1
	clock *parityClock
	end   time.Time
}

func (b parityWindowBrokerV1) PlaceBid(s string, n int, price float64) (string, error) {
	id, err := b.parityBrokerV1.PlaceBid(s, n, price)
	b.clock.now = b.end
	return id, err
}

type parityWindowBrokerV2 struct {
	parityBrokerV2
	clock *parityClock
	end   time.Time
}

func (b parityWindowBrokerV2) Place(ctx context.Context, req v2.OrderRequest) (string, error) {
	id, err := b.parityBrokerV2.Place(ctx, req)
	b.clock.now = b.end
	return id, err
}

func TestPlacementCrossingQuotaWindowMatchesV1(t *testing.T) {
	var remaining [2]int64
	for version, old := range []bool{true, false} {
		now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
		clock := &parityClock{now}
		b := &parityTransport{}
		s := &parityStrategy{b: b, action: 1, lots: 6}
		e := &parityEngine{t: t, b: b, s: s, parityClock: clock, base: now}
		q := parityClockQuota{v1.NewQuotaLimiterBudget(0, 100, time.Second), clock}
		q.Spend(now, 0)
		if old {
			broker := parityWindowBrokerV1{parityBrokerV1{b}, clock, now.Add(2 * time.Second)}
			e.old = v1.NewEngine(v1.EngineConfig{LegA: "A", LegB: "B", OrderVol: 6}, broker, broker, parityStrategyV1{s})
			e.old.SetClock(clock)
			e.old.SetLimiter(q.QuotaLimiter)
		} else {
			broker := parityWindowBrokerV2{parityBrokerV2{b}, clock, now.Add(2 * time.Second)}
			var err error
			e.new, err = v2.NewEngine(v2.Config{LegA: "A", LegB: "B", Lots: 6},
				v2.Setup{Broker: broker, Strategy: parityStrategyV2{s}, Limit: q, Clock: clock, Logger: parityLogger{}})
			if err != nil {
				t.Fatal(err)
			}
		}
		e.book("A", now, 100, 102)
		e.book("B", now, 200, 202)
		e.signal(now)
		remaining[version] = q.Remaining()
	}
	if remaining[0] != 98 || remaining[1] != remaining[0] {
		t.Fatalf("requests must count in the processing window after their return: v1=%d v2=%d", remaining[0], remaining[1])
	}
}

func TestQuotaResetBackoffUsesEventClockLikeV1(t *testing.T) {
	var outcomes [2]parityOutcome
	for version, old := range []bool{true, false} {
		at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
		clock := &parityClock{at.Add(time.Hour)}
		b := &parityTransport{}
		s := &parityStrategy{b: b, action: 1, lots: 6}
		e := &parityEngine{t: t, b: b, s: s, parityClock: clock, base: at}
		q := parityClockQuota{v1.NewQuotaLimiterBudget(0, 100, time.Second), clock}
		q.Spend(clock.Now(), 0)
		q.Set(0, clock.Now().Add(time.Second), clock.Now(), q.Snapshot())
		if old {
			broker := parityBrokerV1{b}
			e.old = v1.NewEngine(v1.EngineConfig{LegA: "A", LegB: "B", OrderVol: 6}, broker, broker, parityStrategyV1{s})
			e.old.SetClock(clock)
			e.old.SetLimiter(q.QuotaLimiter)
		} else {
			var err error
			e.new, err = v2.NewEngine(v2.Config{LegA: "A", LegB: "B", Lots: 6},
				v2.Setup{Broker: parityBrokerV2{b}, Strategy: parityStrategyV2{s}, Limit: q, Clock: clock, Logger: parityLogger{}})
			if err != nil {
				t.Fatal(err)
			}
		}
		e.book("A", at, 100, 102)
		e.book("B", at, 200, 202)
		e.signal(at)
		if len(b.calls) != 0 {
			t.Fatal("exhausted quota admitted an opening")
		}
		clock.now = clock.now.Add(time.Second)
		e.signal(at.Add(time.Second))
		outcomes[version] = e.parityOutcome()
	}
	if len(outcomes[0].Calls) != 2 || !reflect.DeepEqual(outcomes[0], outcomes[1]) {
		t.Fatalf("opening must resume at quota reset in event time\nv1: %+v\nv2: %+v", outcomes[0], outcomes[1])
	}
}
