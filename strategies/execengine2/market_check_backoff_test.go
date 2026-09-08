package execengine2_test

import (
	"context"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

func TestMarketStatusBacksOffUnresolvedOrdersForAnHour(t *testing.T) {
	t.Parallel()
	for _, healthy := range []bool{false, true} {
		name := "status errors"
		if healthy {
			name = "healthy but unresolved"
		}
		t.Run(name, func(t *testing.T) {
			f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
				cfg.Mode = execengine2.ModeMarket
				if healthy {
					broker.status["o1"] = execengine2.OrderStatus{}
					broker.status["o2"] = execengine2.OrderStatus{}
				}
			})
			ctx := context.Background()
			if err := f.engine.OnSignal(ctx, execengine2.Signal{Time: f.now}); err != nil {
				t.Fatal(err)
			}
			for second := 0; second <= 3600; second++ {
				f.clock.now = f.now.Add(time.Duration(second) * time.Second)
				before := len(f.broker.statuses)
				err := f.engine.OnTick(ctx, f.clock.now)
				if gotError, wantError := err != nil, !healthy && len(f.broker.statuses) > before; gotError != wantError {
					t.Fatalf("at %ds status error=%v, want error=%v", second, err, wantError)
				}
				if second < 10 && len(f.broker.statuses) != 0 {
					t.Fatalf("market status was polled before its initial grace: %v", f.broker.statuses)
				}
			}
			// Each order is checked at 10, 13, 19, 31, 55, 103 seconds, then
			// every minute: 64 calls per order, versus roughly 2,400 combined
			// calls with the previous fixed three-second schedule.
			counts := make(map[string]int)
			for _, id := range f.broker.statuses {
				counts[id]++
			}
			if len(counts) != 2 || counts["o1"] != 64 || counts["o2"] != 64 {
				t.Fatalf("hourly status calls = %v, want 64 per unresolved market", counts)
			}
			if len(f.broker.places) != 2 || f.engine.Info().MarketOrders != 2 {
				t.Fatalf("status retries changed orders: places=%d info=%+v", len(f.broker.places), f.engine.Info())
			}
			for _, id := range []string{"o1", "o2"} {
				if err := f.engine.OnOrderStatus(ctx, id, execengine2.OrderStatus{Filled: 2, Done: true}); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.now = f.now.Add(2 * time.Hour)
			if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
				t.Fatal(err)
			}
			if len(f.broker.statuses) != 128 || f.engine.Info().MarketOrders != 0 {
				t.Fatalf("terminal market continued polling: statuses=%d info=%+v", len(f.broker.statuses), f.engine.Info())
			}
		})
	}
}
