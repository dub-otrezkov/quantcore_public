package execengine2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

func TestUnknownChunkDoesNotParkRemainderOrDuplicateLaterRecovery(t *testing.T) {
	for _, filled := range []int{0, 2, 4} {
		t.Run(fmt.Sprintf("terminal-filled-%d", filled), func(t *testing.T) {
			t.Parallel()
			f := newTestSet(t, nil)
			b := &recoveringBroker{fakeBroker: f.broker}
			unknownIssued, resolved := false, false
			lookupErr := errors.New("original order is absent from active list")
			b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, call int) (string, error) {
				if req.Kind == execengine2.OrderLimit {
					return "maker", nil
				}
				if !unknownIssued {
					if req.Lots > 4 {
						return "", execengine2.NotPlaced(errors.New("too large"))
					}
					unknownIssued = true
					return "", execengine2.OrderUnknown("unknown-four", context.DeadlineExceeded)
				}
				return fmt.Sprintf("market-%d", call), nil
			}
			b.resume = func(_ context.Context, req execengine2.OrderRequest, id string) (string, error) {
				if id != "unknown-four" || req.Lots != 4 || req.Symbol != "B" {
					t.Fatalf("resumed the remainder instead of the unknown chunk: id=%s req=%+v", id, req)
				}
				if !resolved {
					return "", execengine2.OrderUnknown(id, lookupErr)
				}
				return "resolved-four", nil
			}
			var err error
			f.engine, err = execengine2.NewEngine(execengine2.Config{
				LegA: "A", LegB: "B", Lots: 10, Mode: execengine2.ModeLimitA,
				RejectRetryLotStep: 3, RetryWait: time.Second,
			}, execengine2.Setup{
				Broker: b, Limit: f.budget, Clock: f.clock, Strategy: f.decider,
				Changes: f.sink, Logger: testLog{},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, symbol := range []string{"A", "B"} {
				if err := f.engine.OnBook(context.Background(), symbol, f.now, 99, 100); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.OnFill(context.Background(), execengine2.Fill{FillID: "maker-fill", OrderID: "maker", Lots: 10, Price: 99}); err != nil {
				t.Fatal(err)
			}
			if len(b.places) != 4 || f.engine.Info().UnknownOrders != 1 || f.engine.Info().Hedges != 1 {
				t.Fatalf("initial chunk split was lost: info=%+v calls=%v", f.engine.Info(), b.places)
			}
			// The unknown four lots cannot pay the six that were never sent,
			// even though both portions belong to the same leg and clip.
			f.clock.now = f.now.Add(time.Second)
			if err := f.engine.OnTick(context.Background(), f.clock.now); !errors.Is(err, lookupErr) {
				t.Fatalf("unresolved lookup error = %v", err)
			}
			if len(b.places) != 5 || b.places[4].req.Lots != 6 || f.engine.Info().Hedges != 0 {
				t.Fatalf("unplaced remainder stayed parked: info=%+v calls=%v", f.engine.Info(), b.places)
			}
			if f.engine.CheckPositions(10, -6) || f.engine.Info().UnknownOrders != 1 {
				t.Fatal("paying the remainder cleared the unresolved chunk's barrier")
			}
			if err := f.engine.OnOrderStatus(context.Background(), "market-5", execengine2.OrderStatus{Done: true, Filled: 6}); err != nil {
				t.Fatal(err)
			}
			// If the original chunk becomes visible later, repair only its
			// confirmed shortfall. The six paid lots must not be sent again.
			resolved = true
			b.status["resolved-four"] = execengine2.OrderStatus{Done: true, Filled: filled}
			f.clock.now = f.now.Add(3 * time.Second)
			if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
				t.Fatal(err)
			}
			wantPlaces := 5
			if filled < 4 {
				wantPlaces++
				if len(b.places) != wantPlaces || b.places[5].req.Lots != 4-filled {
					t.Fatalf("wrong terminal shortfall: calls=%v", b.places)
				}
				if err := f.engine.OnOrderStatus(context.Background(), "market-6", execengine2.OrderStatus{Done: true, Filled: 4 - filled}); err != nil {
					t.Fatal(err)
				}
			}
			a, legB := inventory(f.sink)
			if len(b.places) != wantPlaces || a != 10 || legB != -10 || len(f.decider.commits) != 1 || !f.engine.CheckPositions(10, -10) {
				t.Fatalf("recovery lost or duplicated volume: info=%+v inventory=(%d,%d) calls=%v", f.engine.Info(), a, legB, b.places)
			}
		})
	}
}
