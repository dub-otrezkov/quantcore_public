package execengine2_test

import (
	"context"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

func TestTerminalTakerShortfallKeepsPreviouslySeenExecutions(t *testing.T) {
	f := newTestSet(t, func(cfg *execengine2.Config, _ *fakeBroker) {
		cfg.Lots = 10
		cfg.Mode = execengine2.ModeLimitA
	})
	ctx := context.Background()
	if err := f.engine.OnSignal(ctx, execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	for _, fill := range []execengine2.Fill{
		{OrderID: "o1", FillID: "maker", Lots: 10, Price: 99},
		{OrderID: "o2", FillID: "hedge-part", Lots: 7, Price: 199},
	} {
		if err := f.engine.OnFill(ctx, fill); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.engine.OnOrderStatus(ctx, "o2", execengine2.OrderStatus{Done: true, Filled: 3}); err != nil {
		t.Fatal(err)
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 3 || f.broker.places[2].req.Lots != 3 {
		t.Fatalf("seven proven executions leave only three owed lots: requests=%+v", f.broker.places)
	}
	var gotB int
	for _, delta := range f.sink.positions {
		if delta.Symbol == "B" {
			gotB += delta.Lots
		}
	}
	if gotB != -10 {
		t.Fatalf("hedge inventory=%d, want -10", gotB)
	}
}
