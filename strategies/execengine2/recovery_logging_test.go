package execengine2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

type recoveryLog struct {
	testLog
	warnings []string
}

func (l *recoveryLog) Warnf(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func TestUnavailableRecoveryWarningsFollowRetrySchedule(t *testing.T) {
	for _, tc := range []struct {
		name      string
		canResume bool
		clientID  string
		message   string
	}{
		{name: "missing resumer", clientID: "unknown-b", message: "broker cannot resume placements"},
		{name: "missing client id", canResume: true, message: "no client id for an unknown placement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTestSet(t, nil)
			f.broker.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
				if req.Kind == execengine2.OrderMarket {
					return "", execengine2.OrderUnknown(tc.clientID, context.DeadlineExceeded)
				}
				return "maker", nil
			}
			var broker execengine2.Broker = f.broker
			resuming := &recoveringBroker{fakeBroker: f.broker, resume: func(context.Context, execengine2.OrderRequest, string) (string, error) {
				t.Fatal("attempted recovery without a client ID")
				return "", context.Canceled
			}}
			if tc.canResume {
				broker = resuming
			}
			log := &recoveryLog{}
			engine, err := execengine2.NewEngine(execengine2.Config{
				LegA: "A", LegB: "B", Lots: 2, Mode: execengine2.ModeLimitA, RetryWait: time.Second,
			}, execengine2.Setup{
				Broker: broker, Clock: f.clock, Strategy: f.decider, Changes: f.sink, Logger: log,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, symbol := range []string{"A", "B"} {
				if err := engine.OnBook(context.Background(), symbol, f.now, 99, 100); err != nil {
					t.Fatal(err)
				}
			}
			if err := engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
				t.Fatal(err)
			}
			if err := engine.OnFill(context.Background(), execengine2.Fill{FillID: "fill", OrderID: "maker", Lots: 2, Price: 99}); err != nil {
				t.Fatal(err)
			}
			for _, tick := range []struct {
				after time.Duration
				count int
			}{
				{999 * time.Millisecond, 0},
				{time.Second, 1},
				{1001 * time.Millisecond, 1},
				{2 * time.Second, 2},
			} {
				f.clock.now = f.now.Add(tick.after)
				if err := engine.OnTick(context.Background(), f.clock.now); err != nil {
					t.Fatal(err)
				}
				if len(log.warnings) != tick.count {
					t.Fatalf("at %s warnings=%v, want %d", tick.after, log.warnings, tick.count)
				}
			}
			for _, warning := range log.warnings {
				if !strings.Contains(warning, tc.message) || !strings.Contains(warning, "B") || !strings.Contains(warning, tc.clientID) {
					t.Fatalf("warning lost recovery diagnosis or order context: %q", warning)
				}
			}
			if engine.Info().UnknownOrders != 1 || len(f.broker.places) != 2 || len(resuming.resumes) != 0 {
				t.Fatal("warning cleared the barrier or caused an unsafe resend")
			}
			if err := engine.Stop(context.Background(), "manual"); err != nil {
				t.Fatal(err)
			}
			if err := engine.OnTick(context.Background(), f.now.Add(3*time.Second)); err != nil {
				t.Fatal(err)
			}
			if len(log.warnings) != 2 {
				t.Fatal("stopped engine kept emitting recovery warnings")
			}
		})
	}
}
