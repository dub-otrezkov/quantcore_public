package execengine2_test

import (
	"reflect"
	"sort"
	"testing"
	"time"

	v1 "QuantCore/strategies/execengine"
	v2 "QuantCore/strategies/execengine2"
)

type definitiveMarketParityV1 struct{ parityBrokerV1 }

func (b definitiveMarketParityV1) Buy(symbol string, lots int) (string, error) {
	id, err := b.place(symbol, "market", true, lots, 0)
	return id, v1.NewDefinitiveReject(err)
}

func (b definitiveMarketParityV1) Sell(symbol string, lots int) (string, error) {
	id, err := b.place(symbol, "market", false, lots, 0)
	return id, v1.NewDefinitiveReject(err)
}

func TestDefinitiveMarketOpeningRetriesAndCommitMatchV1(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		step, tries, lots, ratio int
		close                    bool
	}{
		{"first parallel attempts consume HedgeTries", 0, 1, 6, 1, false},
		{"both rejected entry legs retain original size", 2, 1, 6, 1, false},
		{"plain retry budget excludes parallel attempts", 2, 2, 6, 1, false},
		{"closing legs may shrink and accumulate", 2, 1, 6, 1, true},
		{"accepted sibling permits shrinking entry hedge", 2, 1, 2, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var outcomes [2]parityOutcome
			for version, old := range []bool{true, false} {
				now := time.Now()
				b := &parityTransport{maxAcceptedLots: 2}
				s := &parityStrategy{b: b, action: 1, lots: tc.lots}
				if tc.close {
					s.action, s.position, s.close = -1, tc.lots, true
				}
				e := &parityEngine{t: t, b: b, s: s, parityClock: &parityClock{now}, base: now}
				if old {
					broker := definitiveMarketParityV1{parityBrokerV1{b}}
					e.old = v1.NewEngine(v1.EngineConfig{LegA: "A", LegB: "B", OrderVol: tc.lots, HedgeRatio: tc.ratio,
						TakerOnly: true, HedgeRetries: tc.tries, RejectRetryLotStep: tc.step}, broker, broker, parityStrategyV1{s})
					e.old.SetClock(e.parityClock)
					e.old.SetFillSink(paritySinkV1{b})
				} else {
					var err error
					e.new, err = v2.NewEngine(v2.Config{LegA: "A", LegB: "B", Lots: tc.lots, Ratio: tc.ratio,
						Mode: v2.ModeMarket, HedgeTries: tc.tries, RejectRetryLotStep: tc.step},
						v2.Setup{Broker: parityBrokerV2{b}, Strategy: parityStrategyV2{s}, Changes: parityUpdatesV2{b}, Clock: e.parityClock, Logger: parityLogger{}})
					if err != nil {
						t.Fatal(err)
					}
				}
				e.book("A", now, 100, 102)
				e.book("B", now, 200, 202)
				e.signal(now)
				outcomes[version] = e.parityOutcome()
				if len(outcomes[version].Calls) < 2 {
					t.Fatal("opening did not issue both initial attempts")
				}
				// Only the two initial calls may race. Later retry order, quantities,
				// Commit and SaveLots remain observable parts of the v1 contract.
				sort.Strings(outcomes[version].Calls[:2])
				if !old && !tc.close && tc.ratio == 1 && e.new.Info().Hedges != 2 {
					t.Fatalf("both rejected legs must remain explicit debt: %+v", e.new.Info())
				}
			}
			if !reflect.DeepEqual(outcomes[0], outcomes[1]) {
				t.Fatalf("market retry parity differs\nv1: %+v\nv2: %+v", outcomes[0], outcomes[1])
			}
		})
	}
}
