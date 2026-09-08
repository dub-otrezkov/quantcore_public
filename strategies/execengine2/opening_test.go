package execengine2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

// Observe every callback that would account for a placement. Neither a clock
// read nor an account update may run while the other opening RPC is pending.
type openingCallbacks struct {
	fixture   *testSet
	completed *atomic.Int32
	early     atomic.Bool
}

func (o *openingCallbacks) check() {
	if o.completed.Load() != 2 {
		o.early.Store(true)
	}
}

func (o *openingCallbacks) Now() time.Time {
	o.check()
	return o.fixture.now
}

func (o *openingCallbacks) Apply(change execengine2.PositionChange) {
	o.check()
	o.fixture.sink.Apply(change)
}

func (o *openingCallbacks) Amend(change execengine2.PriceChange) {
	o.check()
	o.fixture.sink.Amend(change)
}

func TestMarketOpeningSendsBothLegsBeforeEitherReply(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan placeCall, 2)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	returnedA := make(chan struct{})
	var completed atomic.Int32
	f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
		cfg.Mode = execengine2.ModeMarket
		broker.placeFunc = func(ctx context.Context, req execengine2.OrderRequest, _ int) (string, error) {
			started <- placeCall{ctx: ctx, req: req}
			release := releaseA
			if req.Leg == execengine2.LegB {
				release = releaseB
			}
			select {
			case <-release:
			case <-ctx.Done():
				return "", execengine2.OrderUnknown(req.Symbol, ctx.Err())
			}
			completed.Add(1)
			if req.Leg == execengine2.LegA {
				close(returnedA)
			}
			return req.Symbol, nil
		}
	})
	callbacks := &openingCallbacks{fixture: f, completed: &completed}
	engine, err := execengine2.NewEngine(execengine2.Config{
		LegA: "A", LegB: "B", Lots: 2, Mode: execengine2.ModeMarket,
	}, execengine2.Setup{
		Broker: f.broker, Limit: f.budget, Clock: callbacks,
		Strategy: f.decider, Changes: callbacks, Logger: testLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, symbol := range []string{"A", "B"} {
		if err := engine.OnBook(ctx, symbol, f.now, 99, 100); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- engine.OnSignal(ctx, execengine2.Signal{Time: f.now}) }()
	seen := make(map[execengine2.Leg]bool)
	for range 2 {
		select {
		case call := <-started:
			seen[call.req.Leg] = true
			if call.ctx != ctx || call.req.Kind != execengine2.OrderMarket {
				t.Errorf("opening call = %+v, context preserved = %v", call.req, call.ctx == ctx)
			}
		case <-ctx.Done():
			t.Fatal("both legs must reach Broker.Place before either reply is released")
		}
	}
	if !seen[execengine2.LegA] || !seen[execengine2.LegB] || f.budget.Remaining() != 20 {
		t.Fatalf("opening legs=%v budget=%d, attempts are charged after both replies as in v1", seen, f.budget.Remaining())
	}
	close(releaseA)
	select {
	case <-returnedA:
	case <-ctx.Done():
		t.Fatal("first placement did not return")
	}
	select {
	case err := <-done:
		t.Fatalf("OnSignal returned before the second placement: %v", err)
	default:
	}
	close(releaseB)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("opening did not join both placements")
	}
	if callbacks.early.Load() {
		t.Fatal("placement accounting ran before both broker calls completed")
	}
	if len(f.sink.positions) != 2 || engine.Position() != 2 || engine.Info().MarketOrders != 2 {
		t.Fatalf("positions=%+v engine=%+v", f.sink.positions, engine.Info())
	}
	if f.budget.Remaining() != 18 {
		t.Fatalf("budget=%d, want exactly two placement attempts", f.budget.Remaining())
	}
}

func TestMarketOpeningAccountsForSuccessfulLegAfterOtherRejects(t *testing.T) {
	t.Parallel()
	for _, rejectedLeg := range []execengine2.Leg{execengine2.LegA, execengine2.LegB} {
		t.Run(fmt.Sprint(rejectedLeg), func(t *testing.T) {
			t.Parallel()
			rejected := errors.New("opening rejected")
			f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
				cfg.Mode = execengine2.ModeMarket
				cfg.HedgeTries = 2 // the initial parallel attempt counts toward the limit
				broker.placeFunc = func(_ context.Context, req execengine2.OrderRequest, call int) (string, error) {
					if call <= 2 && req.Leg == rejectedLeg {
						return "", execengine2.NotPlaced(rejected)
					}
					return fmt.Sprintf("%s-%d", req.Symbol, call), nil
				}
			})
			err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now})
			if !errors.Is(err, rejected) {
				t.Fatalf("opening error=%v, want definitive rejection", err)
			}
			if len(f.broker.places) != 3 || f.broker.places[2].req.Leg != rejectedLeg ||
				f.broker.places[2].req.Role != execengine2.RoleFix {
				t.Fatalf("placements=%+v, want both opening legs and one recovery of rejected leg", f.broker.places)
			}
			positions := make(map[string]int)
			for _, change := range f.sink.positions {
				positions[change.Symbol] += change.Lots
			}
			if len(f.sink.positions) != 2 || positions["A"] != 2 || positions["B"] != -2 ||
				f.engine.Position() != 2 || f.engine.HasTrade() || f.engine.Info().MarketOrders != 2 {
				t.Fatalf("accounting=%+v engine=%+v", f.sink.positions, f.engine.Info())
			}
			if f.budget.Remaining() != 17 {
				t.Fatalf("budget=%d, want three spent attempts", f.budget.Remaining())
			}
		})
	}
}

func TestMarketOpeningKeepsOtherSuccessAfterInvalidOrderID(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
		cfg.Mode = execengine2.ModeMarket
		broker.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
			if req.Leg == execengine2.LegA {
				return "", nil
			}
			return "accepted-b", nil
		}
	})
	err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now})
	if err == nil || !f.engine.HasOrder("accepted-b") || len(f.broker.places) != 2 {
		t.Fatalf("err=%v acceptedB=%v placements=%+v", err, f.engine.HasOrder("accepted-b"), f.broker.places)
	}
	if len(f.sink.positions) != 2 || f.sink.positions[1].Symbol != "B" || f.sink.positions[1].Lots != -2 {
		t.Fatalf("accepted market placements were lost: %+v", f.sink.positions)
	}
}

func TestMarketOpeningPreservesUnknownLegAndOtherSuccess(t *testing.T) {
	t.Parallel()
	for _, unknownLeg := range []execengine2.Leg{execengine2.LegA, execengine2.LegB} {
		t.Run(fmt.Sprint(unknownLeg), func(t *testing.T) {
			t.Parallel()
			f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
				cfg.Mode = execengine2.ModeMarket
				broker.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
					if req.Leg == unknownLeg {
						return "", execengine2.OrderUnknown("pending-"+req.Symbol, context.DeadlineExceeded)
					}
					return "accepted-" + req.Symbol, nil
				}
			})
			err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now})
			info := f.engine.Info()
			if !errors.Is(err, context.DeadlineExceeded) || info.UnknownOrders != 1 ||
				info.MarketOrders != 1 || info.Code != execengine2.StateCheckNeeded || len(f.broker.places) != 2 {
				t.Fatalf("err=%v state=%+v placements=%+v", err, info, f.broker.places)
			}
			acceptedSymbol, acceptedLots := "B", -2
			if unknownLeg == execengine2.LegB {
				acceptedSymbol, acceptedLots = "A", 2
			}
			if len(f.sink.positions) != 1 || f.sink.positions[0].Symbol != acceptedSymbol ||
				f.sink.positions[0].Lots != acceptedLots || !f.engine.HasOrder("accepted-"+acceptedSymbol) {
				t.Fatalf("successful leg accounting=%+v, unknown leg must remain uncredited", f.sink.positions)
			}
			if f.engine.CheckPositions(0, 0) || f.budget.Remaining() != 18 {
				t.Fatalf("unknown placement lost its barrier or budget: %+v", f.engine.Info())
			}
		})
	}
}

func TestMarketOpeningDoesNotSendEitherLegWithoutBothAttempts(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(cfg *execengine2.Config, _ *fakeBroker) {
		cfg.Mode = execengine2.ModeMarket
	})
	f.budget.Reset(4) // One normal attempt remains above the reserve of three.
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 0 || f.engine.HasTrade() || len(f.sink.positions) != 0 || f.budget.Remaining() != 4 {
		t.Fatalf("placements=%+v accounting=%+v engine=%+v", f.broker.places, f.sink.positions, f.engine.Info())
	}
}
