package execengine2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

func TestUnknownOpeningBacksOffWithoutReleasingBarrier(t *testing.T) {
	t.Parallel()
	f, b := newRecoverySet(t, execengine2.ModeMarket)
	recovered := false
	b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
		if req.Symbol == "B" {
			return "", execengine2.OrderUnknown("original-b", context.DeadlineExceeded)
		}
		if !recovered {
			return "", execengine2.NotPlaced(errors.New("opening rejected"))
		}
		return "hedge-a", nil
	}
	var attempts []time.Duration
	b.resume = func(_ context.Context, req execengine2.OrderRequest, id string) (string, error) {
		if id != "original-b" || req.Symbol != "B" || req.Lots != 2 {
			t.Fatalf("recovery changed the original request: %s %+v", id, req)
		}
		attempts = append(attempts, f.clock.now.Sub(f.now))
		if recovered {
			return "recovered-b", nil
		}
		return "", execengine2.OrderUnknown(id, errors.New("lookup unavailable"))
	}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err == nil {
		t.Fatal("expected unresolved opening")
	}
	// An executed B leg with a rejected A leg is not a full target pair. Even
	// repeated position checks cannot settle it or reset the recovery schedule.
	for second := 1; second <= 3600; second++ {
		f.clock.now = f.now.Add(time.Duration(second) * time.Second)
		_ = f.engine.OnTick(context.Background(), f.clock.now)
		if f.engine.CheckPositions(0, -2) || f.engine.Info().UnknownOrders != 1 {
			t.Fatalf("partial inventory released the unknown order at second %d", second)
		}
	}
	if len(attempts) != 64 {
		t.Fatalf("recovery attempts in one hour = %d, want 64 with a one-minute cap", len(attempts))
	}
	for i, second := range []int{1, 3, 7, 15, 31, 63, 123} {
		if attempts[i] != time.Duration(second)*time.Second {
			t.Fatalf("attempt %d at %s, want %ds", i+1, attempts[i], second)
		}
	}
	for i := 6; i < len(attempts); i++ {
		if attempts[i]-attempts[i-1] != time.Minute {
			t.Fatalf("recovery interval after cap = %s", attempts[i]-attempts[i-1])
		}
	}
	if len(b.places) != 2 || len(f.decider.commits) != 0 || len(f.sink.positions) != 0 {
		t.Fatal("unresolved opening placed a replacement or credited inventory")
	}
	// Backoff must keep recovery possible: a later lookup can still adopt the
	// original order and hedge exactly the other leg once.
	recovered = true
	b.status["recovered-b"] = execengine2.OrderStatus{Done: true, Filled: 2}
	f.clock.now = f.now.Add(3603 * time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if f.engine.Info().UnknownOrders != 0 || len(b.places) != 3 || f.engine.Position() != 2 || len(f.decider.commits) != 1 {
		t.Fatalf("delayed recovery did not complete exactly once: info=%+v calls=%v", f.engine.Info(), b.places)
	}
	a, legB := inventory(f.sink)
	if a != 2 || legB != -2 {
		t.Fatalf("recovered inventory = (%d,%d), want (2,-2)", a, legB)
	}
}
