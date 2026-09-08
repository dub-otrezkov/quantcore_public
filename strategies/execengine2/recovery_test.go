package execengine2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

type resumeCall struct {
	id  string
	req execengine2.OrderRequest
}

type recoveringBroker struct {
	*fakeBroker
	resumes []resumeCall
	resume  func(context.Context, execengine2.OrderRequest, string) (string, error)
}

func (b *recoveringBroker) ResumePlacement(ctx context.Context, req execengine2.OrderRequest, id string) (string, error) {
	b.resumes = append(b.resumes, resumeCall{id: id, req: req})
	return b.resume(ctx, req, id)
}

func newRecoverySet(t *testing.T, mode execengine2.Mode) (*testSet, *recoveringBroker) {
	t.Helper()
	f := newTestSet(t, nil)
	b := &recoveringBroker{fakeBroker: f.broker}
	var err error
	f.engine, err = execengine2.NewEngine(execengine2.Config{
		LegA: "A", LegB: "B", Lots: 2, Mode: mode, BookMaxAge: time.Minute,
		HedgeTries: 1, RetryWait: time.Second,
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
	return f, b
}

func TestUnknownMarketLegResumesAndHedgesTerminalShortfall(t *testing.T) {
	t.Parallel()
	f, b := newRecoverySet(t, execengine2.ModeMarket)
	b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, call int) (string, error) {
		if req.Symbol == "B" && req.Lots == 2 {
			return "", execengine2.OrderUnknown("original-b", context.DeadlineExceeded)
		}
		return fmt.Sprintf("%s-%d", req.Symbol, call), nil
	}
	b.resume = func(_ context.Context, req execengine2.OrderRequest, id string) (string, error) {
		if id != "original-b" || req.Symbol != "B" || req.Lots != 2 {
			t.Fatalf("resumed a different request: id=%s req=%+v", id, req)
		}
		return "recovered-b", nil
	}
	b.status["recovered-b"] = execengine2.OrderStatus{Done: true, Filled: 1}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err == nil {
		t.Fatal("unknown leg error was lost")
	}
	if !f.engine.HasTrade() || f.engine.Info().UnknownOrders != 1 || len(b.places) != 2 {
		t.Fatalf("lost unresolved opening: info=%+v calls=%v", f.engine.Info(), b.places)
	}
	if f.engine.CheckPositions(0, 0) {
		t.Fatal("position snapshot released an unresolved market order")
	}
	if err := f.engine.StopTrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnTick(context.Background(), f.now); err != nil || len(b.resumes) != 0 || len(b.places) != 2 {
		t.Fatalf("retried early or hedged an unknown order: err=%v", err)
	}
	for _, call := range b.places {
		if call.req.Symbol == "A" {
			// The success id follows the actual concurrent call order.
			for _, id := range []string{"A-1", "A-2"} {
				if f.engine.HasOrder(id) {
					if err := f.engine.OnOrderStatus(context.Background(), id, execengine2.OrderStatus{Done: true, Filled: 2}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(b.resumes) != 1 || f.engine.Info().UnknownOrders != 0 || f.engine.Info().Hedges != 0 {
		t.Fatalf("terminal partial fill was not repaired during adoption: %+v", f.engine.Info())
	}
	if f.engine.CheckPositions(2, -1) {
		t.Fatal("unfinished hedge passed reconciliation")
	}
	if len(b.places) != 3 || b.places[2].req.Symbol != "B" || b.places[2].req.Lots != 1 {
		t.Fatalf("wrong same-event recovery volume: %+v", b.places)
	}
	if f.engine.Position() != 2 || len(f.decider.commits) != 1 || f.engine.HasTrade() {
		t.Fatalf("recovered clip was not committed once: %+v commits=%v", f.engine.Info(), f.decider.commits)
	}
	if err := f.engine.OnOrderStatus(context.Background(), "recovered-b", execengine2.OrderStatus{Done: true, Filled: 1}); err != nil {
		t.Fatal(err)
	}
	fill := execengine2.Fill{FillID: "recovered-execution", OrderID: "recovered-b", Lots: 1, Price: 98}
	for range 2 {
		if err := f.engine.OnFill(context.Background(), fill); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.sink.prices) != 1 || f.sink.prices[0].OrderID != "recovered-b" ||
		f.sink.prices[0].Lots != 1 || f.sink.prices[0].From != 99 || f.sink.prices[0].To != 98 {
		t.Fatalf("resumed execution did not amend exactly once: %+v", f.sink.prices)
	}
	if err := f.engine.OnOrderStatus(context.Background(), "B-3", execengine2.OrderStatus{Done: true, Filled: 1}); err != nil {
		t.Fatal(err)
	}
	cancels, statuses := len(b.cancels), len(b.statuses)
	f.clock.now = f.now.Add(2 * time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(b.places) != 3 || len(b.resumes) != 1 || len(b.cancels) != cancels || len(b.statuses) != statuses ||
		len(f.decider.commits) != 1 || len(f.sink.positions) != 4 {
		t.Fatalf("replay or tick repeated recovery: info=%+v calls=%+v deltas=%+v", f.engine.Info(), b.places, f.sink.positions)
	}
	if !f.engine.CheckPositions(2, -2) {
		t.Fatalf("completed recovery stayed blocked: %+v", f.engine.Info())
	}
	a, legB := inventory(f.sink)
	if a != 2 || legB != -2 {
		t.Fatalf("inventory was duplicated: A=%d B=%d deltas=%+v", a, legB, f.sink.positions)
	}
}

func inventory(s *fakeUpdates) (a, b int) {
	for _, change := range s.positions {
		if change.Symbol == "A" {
			a += change.Lots
		} else {
			b += change.Lots
		}
	}
	return a, b
}

func TestUnknownCreditedHedgeAdoptionDoesNotCreditTwice(t *testing.T) {
	t.Parallel()
	f, b := newRecoverySet(t, execengine2.ModeTwoLimits)
	b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, call int) (string, error) {
		if req.Kind == execengine2.OrderMarket {
			return "", execengine2.OrderUnknown("hedge-id", context.DeadlineExceeded)
		}
		return fmt.Sprintf("o%d", call), nil
	}
	b.resume = func(context.Context, execengine2.OrderRequest, string) (string, error) { return "hedge", nil }
	b.status["hedge"] = execengine2.OrderStatus{Done: true, Filled: 2}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{FillID: "maker", OrderID: "o1", Lots: 2, Price: 99}); err != nil {
		t.Fatal(err)
	}
	if f.engine.Position() != 2 || f.engine.CheckPositions(2, -1) {
		t.Fatalf("provisional hedge lost its recovery barrier: %+v", f.engine.Info())
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{FillID: "replay", OrderID: "hedge", Lots: 2, Price: 100}); err != nil {
		t.Fatal(err)
	}
	a, legB := inventory(f.sink)
	if a != 2 || legB != -2 || len(f.sink.positions) != 2 || len(f.decider.commits) != 1 {
		t.Fatalf("adoption/replay duplicated accounting: %+v", f.sink.positions)
	}
	if !f.engine.CheckPositions(2, -2) {
		t.Fatalf("confirmed resumed hedge remained blocked: %+v", f.engine.Info())
	}
}

func TestUnknownMarketRetryPacingAndKillSwitch(t *testing.T) {
	t.Parallel()
	f, b := newRecoverySet(t, execengine2.ModeMarket)
	b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
		return "", execengine2.OrderUnknown("cid-"+req.Symbol, context.DeadlineExceeded)
	}
	b.resume = func(context.Context, execengine2.OrderRequest, string) (string, error) {
		return "", errors.New("still unavailable")
	}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err == nil {
		t.Fatal("expected opening errors")
	}
	if !f.engine.HasTrade() || f.engine.Info().UnknownOrders != 2 {
		t.Fatalf("empty-looking unknown clip was discarded: %+v", f.engine.Info())
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err == nil || len(b.resumes) != 2 {
		t.Fatalf("both unknown legs must remain recoverable: err=%v calls=%v", err, b.resumes)
	}
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil || len(b.resumes) != 2 {
		t.Fatal("retry pacing was ignored")
	}
	if err := f.engine.Stop(context.Background(), "kill switch"); err != nil {
		t.Fatal(err)
	}
	f.clock.now = f.now.Add(time.Hour)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(b.resumes) != 2 || len(b.places) != 2 || f.engine.Code() != execengine2.StateStopped {
		t.Fatal("kill switch resumed unknown orders")
	}
}

func TestMakerFillsDuringUnknownHedgeAreBalancedAfterResume(t *testing.T) {
	t.Parallel()
	f, b := newRecoverySet(t, execengine2.ModeTwoLimits)
	b.placeFunc = func(_ context.Context, _ execengine2.OrderRequest, call int) (string, error) {
		if call == 3 {
			return "", execengine2.OrderUnknown("first-hedge", context.DeadlineExceeded)
		}
		return fmt.Sprintf("o%d", call), nil
	}
	b.resume = func(context.Context, execengine2.OrderRequest, string) (string, error) { return "resumed", nil }
	b.status["resumed"] = execengine2.OrderStatus{Done: true, Filled: 1}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := f.engine.OnFill(context.Background(), execengine2.Fill{
			FillID: fmt.Sprintf("maker-%d", i), OrderID: "o1", Lots: 1, Price: 99,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.places) != 3 || f.engine.Info().UnknownOrders != 1 {
		t.Fatalf("unknown hedge was blindly duplicated: calls=%v", b.places)
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(b.places) != 4 || b.places[3].req.Lots != 1 || b.places[3].req.Symbol != "B" {
		t.Fatalf("additional maker fill lost its hedge: calls=%v info=%+v", b.places, f.engine.Info())
	}
	if f.engine.Position() != 2 || f.engine.HasTrade() {
		t.Fatalf("balanced clip did not finish: %+v", f.engine.Info())
	}
	a, legB := inventory(f.sink)
	if a != 2 || legB != -2 {
		t.Fatalf("wrong inventory after delayed hedge: (%d,%d)", a, legB)
	}
}

type cancelRetryBroker struct{ *fakeBroker }

func (b *cancelRetryBroker) Cancel(_ context.Context, id string) (execengine2.CancelResult, error) {
	b.cancels = append(b.cancels, id)
	return execengine2.CancelResult{}, errors.New("cancel unavailable")
}

func TestStoppingClipRespectsCancelRetryWait(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	var err error
	f.engine, err = execengine2.NewEngine(execengine2.Config{
		LegA: "A", LegB: "B", Lots: 2, RetryWait: time.Second,
		TradeTimeout: time.Millisecond, BookMaxAge: time.Second,
	}, execengine2.Setup{
		Broker: &cancelRetryBroker{f.broker}, Clock: f.clock, Strategy: f.decider, Logger: testLog{},
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
	if err := f.engine.StopTrade(context.Background()); err == nil {
		t.Fatal("expected unresolved cancellations")
	}
	if err := f.engine.OnTick(context.Background(), f.now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.cancels) != 4 {
		t.Fatalf("stopping clip bypassed cancel backoff: %v", f.broker.cancels)
	}
	if err := f.engine.OnBook(context.Background(), "A", f.now.Add(2*time.Second), 98, 99); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.cancels) != 4 {
		t.Fatalf("stale book bypassed cancel backoff: %v", f.broker.cancels)
	}
	if err := f.engine.OnTick(context.Background(), f.now.Add(time.Second)); err == nil {
		t.Fatal("scheduled cancels were not retried")
	}
	if len(f.broker.cancels) != 6 {
		t.Fatalf("scheduled cancel retries=%v", f.broker.cancels)
	}
}

type inventoryStrategy struct {
	*fakeStrategy
	sink *fakeUpdates
	base int
}

func TestPositionConfirmedHedgeChunkResumesRemainingDebt(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
		cfg.Lots = 10
		cfg.RejectRetryLotStep = 3
		broker.placeFunc = func(_ context.Context, _ execengine2.OrderRequest, call int) (string, error) {
			if call == 3 || call == 4 {
				return "", execengine2.NotPlaced(errors.New("too large"))
			}
			if call == 5 {
				return "", execengine2.OrderUnknown("chunk", context.DeadlineExceeded)
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{FillID: "maker", OrderID: "o1", Lots: 10, Price: 99}); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 5 || f.engine.Info().UnknownOrders != 1 || f.engine.Info().Hedges != 1 {
		t.Fatalf("unknown chunk or remainder lost/duplicated: %+v calls=%v", f.engine.Info(), f.broker.places)
	}
	if f.engine.CheckPositions(10, -4) || f.engine.Info().UnknownOrders != 0 {
		t.Fatal("full chunk confirmation must preserve its unplaced remainder")
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 6 || f.broker.places[5].req.Lots != 6 || f.engine.Position() != 10 {
		t.Fatalf("confirmed chunk did not schedule exact remainder: %+v calls=%v", f.engine.Info(), f.broker.places)
	}
}

func (s inventoryStrategy) Position() int {
	a, _ := inventory(s.sink)
	return s.base + a
}

func TestFullPositionSnapshotConfirmsUnknownMarketOpening(t *testing.T) {
	for _, inventoryPosition := range []bool{false, true} {
		for _, bothUnknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("inventory=%v/both_unknown=%v", inventoryPosition, bothUnknown), func(t *testing.T) {
				f, b := newRecoverySet(t, execengine2.ModeMarket)
				f.decider.pos = 3
				var strategy execengine2.Strategy = f.decider
				if inventoryPosition {
					strategy = inventoryStrategy{fakeStrategy: f.decider, sink: f.sink, base: 3}
				}
				var err error
				f.engine, err = execengine2.NewEngine(execengine2.Config{
					LegA: "A", LegB: "B", Lots: 2, Mode: execengine2.ModeMarket,
				}, execengine2.Setup{
					Broker: b, Strategy: strategy, Changes: f.sink, Clock: f.clock, Logger: testLog{},
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, symbol := range []string{"A", "B"} {
					if err := f.engine.OnBook(context.Background(), symbol, f.now, 99, 100); err != nil {
						t.Fatal(err)
					}
				}
				b.placeFunc = func(_ context.Context, req execengine2.OrderRequest, _ int) (string, error) {
					if bothUnknown || req.Symbol == "B" {
						return "", execengine2.OrderUnknown("cid-"+req.Symbol, context.DeadlineExceeded)
					}
					return "known-a", nil
				}
				if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err == nil {
					t.Fatal("expected unknown opening")
				}
				if !bothUnknown {
					if err := f.engine.OnOrderStatus(context.Background(), "known-a", execengine2.OrderStatus{Done: true, Filled: 2}); err != nil {
						t.Fatal(err)
					}
				}
				unknown := f.engine.Info().UnknownOrders
				if f.engine.CheckPositions(3, -3) || f.engine.CheckPositions(5, -4) || f.engine.Info().UnknownOrders != unknown {
					t.Fatal("baseline or partial position incorrectly confirmed an unknown order")
				}
				if !f.engine.CheckPositions(5, -5) || f.engine.Position() != 5 || f.engine.HasTrade() {
					t.Fatalf("full target did not confirm the original pair: %+v", f.engine.Info())
				}
				if !f.engine.CheckPositions(5, -5) || len(f.decider.commits) != 1 || len(b.places) != 2 {
					t.Fatal("position confirmation duplicated a commit or placement")
				}
				a, legB := inventory(f.sink)
				if a != 2 || legB != -2 {
					t.Fatalf("confirmed opening inventory=(%d,%d), want (2,-2)", a, legB)
				}
			})
		}
	}
}
