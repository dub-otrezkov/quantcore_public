//go:build sim

package brokersim_test

import (
	"testing"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"

	"QuantCore/brokersim"
	"QuantCore/trade/finam"
)

func TestV2MarketPairThroughGateway(t *testing.T) {
	for _, rejectFirst := range []bool{false, true} {
		name := "both_accepted"
		if rejectFirst {
			name = "one_rejected_then_recovered"
		}
		t.Run(name, func(t *testing.T) {
			h := newEngineHarnessWith(t, brokersim.Config{OrdersListIncludesTerminal: true}, v2HarnessEngine{},
				withOrderVol(2), withMaxPos(2), func(cfg *harnessBuild) {
					cfg.ec.TakerOnly = true
					cfg.ec.HedgeRetries = 1
				})
			h.setBook(legA, 100, 50, 102, 50)
			h.setBook(legB, 50, 100, 52, 100)
			if rejectFirst {
				// Hold the first rejection long enough to observe its sibling at
				// the real gRPC server before the first RPC can return. The fault
				// applies to whichever leg arrives first, avoiding order assumptions.
				h.fault(brokersim.Fault{
					Method: "PlaceOrder", Action: "delay", Count: 1,
					Delay: brokersim.Duration(2 * time.Second),
				})
				h.fault(brokersim.Fault{
					Method: "PlaceOrder", Action: "error", Code: codesInvalidArgument, Count: 1,
				})
			}
			h.setIntent(+1, false, 2)
			if rejectFirst {
				// Query the server directly: the harness event-loop lock is held
				// while both placements are joined, so h.snap would hide this race.
				waitFor(t, "first market RPC", func() bool { return h.srv.Sim.PlaceCount() >= 1 })
				deadline := time.Now().Add(750 * time.Millisecond)
				for h.srv.Sim.PlaceCount() < 2 && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if got := h.srv.Sim.PlaceCount(); got != 2 {
					t.Fatalf("concurrent market RPC count = %d, want both legs before delayed rejection", got)
				}
			}
			h.waitFor("full market pair", 6*time.Second, func(s engineSnap) bool {
				return s.pos == 2 && s.posB == -2 && !s.working
			})
			h.hold()
			h.assertConverged(6 * time.Second)

			wantCalls := 2
			if rejectFirst {
				wantCalls++ // only the definitively rejected leg is retried
			}
			if got := h.srv.Sim.PlaceCount(); got != wantCalls {
				t.Fatalf("PlaceOrder calls = %d, want %d", got, wantCalls)
			}
			if got := h.srv.Sim.CancelCount(); got != 0 {
				t.Fatalf("market orders must not be canceled: CancelOrder calls = %d", got)
			}
			response, err := finam.GetOrders(h.client)
			if err != nil {
				t.Fatal(err)
			}
			if len(response.GetOrders()) != 2 {
				t.Fatalf("created broker orders = %d, want exactly two", len(response.GetOrders()))
			}
			seen := make(map[string]bool)
			for _, state := range response.GetOrders() {
				order := state.GetOrder()
				symbol := order.GetSymbol()
				wantSide := v1.Side_SIDE_BUY
				if symbol == legB {
					wantSide = v1.Side_SIDE_SELL
				}
				if (symbol != legA && symbol != legB) || seen[symbol] ||
					order.GetType() != ordersMarket || order.GetSide() != wantSide ||
					finam.ExecutedLots(state) != 2 {
					t.Fatalf("unexpected market pair order: %+v", state)
				}
				seen[symbol] = true
			}
			if a, b := h.brokerPos(); a != 2 || b != -2 {
				t.Fatalf("broker positions = (%d, %d), want (2, -2)", a, b)
			}
		})
	}
}
