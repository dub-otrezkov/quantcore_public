package execengine2_test

import (
	"reflect"
	"testing"
	"time"

	v1 "QuantCore/strategies/execengine"
	v2 "QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/budget"
)

func TestQuotaAdmissionAndActualSpendsMatchV1(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		solo  bool
	}{
		{"dual closing ladder charges only issued orders", 4, false},
		{"solo opening still requires two discretionary slots", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var outcomes [2]parityOutcome
			var remaining [2]int64
			for version, old := range []bool{true, false} {
				now := time.Now()
				b := &parityTransport{}
				s := &parityStrategy{b: b, action: -1, lots: 6, position: 6, close: true}
				if !tc.solo {
					b.maxAcceptedLots = 2
				}
				e := &parityEngine{t: t, b: b, s: s, parityClock: &parityClock{now}, base: now}
				c1 := v1.EngineConfig{LegA: "A", LegB: "B", OrderVol: 6, HedgeRetries: 1,
					SoloMakerLeg: tc.solo, RejectRetryLotStep: 2, RejectRetryMinLots: 2}
				c2 := v2.Config{LegA: "A", LegB: "B", Lots: 6, HedgeTries: 1,
					RejectRetryLotStep: 2, RejectRetryMinLots: 2}
				if tc.solo {
					c2.Mode = v2.ModeLimitA
					s.action, s.close, s.position = 1, false, 0
				}
				var readRemaining func() int64
				if old {
					broker := parityBrokerV1{b}
					e.old = v1.NewEngine(c1, broker, broker, parityStrategyV1{s})
					e.old.SetClock(e.parityClock)
					e.old.SetFillSink(paritySinkV1{b})
					limiter := v1.NewQuotaLimiterBudget(0, tc.limit, time.Hour)
					e.old.SetLimiter(limiter)
					readRemaining = func() int64 { n, _ := limiter.Remaining(); return int64(n) }
				} else {
					limiter, err := budget.NewQuota(tc.limit, 0, time.Hour)
					if err != nil {
						t.Fatal(err)
					}
					e.new, err = v2.NewEngine(c2, v2.Setup{Broker: parityBrokerV2{b},
						Strategy: parityStrategyV2{s}, Changes: parityUpdatesV2{b}, Limit: limiter, Clock: e.parityClock, Logger: parityLogger{}})
					if err != nil {
						t.Fatal(err)
					}
					readRemaining = limiter.Remaining
				}
				e.book("A", now, 100, 102)
				e.book("B", now, 200, 202)
				e.signal(now)
				outcomes[version], remaining[version] = e.parityOutcome(), readRemaining()
			}
			if !reflect.DeepEqual(outcomes[0], outcomes[1]) || remaining[0] != remaining[1] {
				t.Fatalf("quota parity differs\nv1: %+v remaining=%d\nv2: %+v remaining=%d",
					outcomes[0], remaining[0], outcomes[1], remaining[1])
			}
		})
	}
}
