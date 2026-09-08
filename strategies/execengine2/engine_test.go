package execengine2_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/budget"
)

type placeCall struct {
	ctx context.Context
	req execengine2.OrderRequest
}

type fakeBroker struct {
	mu        sync.Mutex
	places    []placeCall
	cancels   []string
	statuses  []string
	placeFunc func(context.Context, execengine2.OrderRequest, int) (string, error)
	cancelled map[string]execengine2.CancelResult
	status    map[string]execengine2.OrderStatus
}

func (b *fakeBroker) Place(ctx context.Context, req execengine2.OrderRequest) (string, error) {
	b.mu.Lock()
	b.places = append(b.places, placeCall{ctx: ctx, req: req})
	call := len(b.places)
	b.mu.Unlock()
	if b.placeFunc != nil {
		return b.placeFunc(ctx, req, call)
	}
	return fmt.Sprintf("o%d", call), nil
}

func (b *fakeBroker) Cancel(_ context.Context, orderID string) (execengine2.CancelResult, error) {
	b.cancels = append(b.cancels, orderID)
	if result, ok := b.cancelled[orderID]; ok {
		return result, nil
	}
	return execengine2.CancelResult{}, nil
}

func (b *fakeBroker) Status(_ context.Context, orderID string) (execengine2.OrderStatus, error) {
	b.statuses = append(b.statuses, orderID)
	if status, ok := b.status[orderID]; ok {
		return status, nil
	}
	return execengine2.OrderStatus{}, errors.New("status unavailable")
}

type fakeStrategy struct {
	intent  execengine2.Plan
	pos     int
	commits []execengine2.Plan
}

func (d *fakeStrategy) Peek(execengine2.Signal) execengine2.Plan { return d.intent }

func (d *fakeStrategy) Commit(intent execengine2.Plan, _ time.Time) execengine2.Result {
	d.commits = append(d.commits, intent)
	if intent.Action > 0 {
		d.pos += intent.Lots
	} else {
		d.pos -= intent.Lots
	}
	return execengine2.Result{Code: intent.Action}
}

func (d *fakeStrategy) Position() int { return d.pos }

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type fakeUpdates struct {
	positions []execengine2.PositionChange
	prices    []execengine2.PriceChange
}

func (s *fakeUpdates) Apply(delta execengine2.PositionChange) {
	s.positions = append(s.positions, delta)
}

func (s *fakeUpdates) Amend(change execengine2.PriceChange) {
	s.prices = append(s.prices, change)
}

type testLog struct{}

func (testLog) Infof(string, ...any)     {}
func (testLog) Warnf(string, ...any)     {}
func (testLog) Criticalf(string, ...any) {}

type testSet struct {
	engine  *execengine2.Engine
	broker  *fakeBroker
	decider *fakeStrategy
	budget  *budget.Atomic
	clock   *fakeClock
	sink    *fakeUpdates
	now     time.Time
}

func newTestSet(t *testing.T, mutate func(*execengine2.Config, *fakeBroker)) *testSet {
	t.Helper()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	cfg := execengine2.Config{
		LegA: "A", LegB: "B", Lots: 2, Mode: execengine2.ModeTwoLimits,
		HedgeTries: 1, RetryWait: time.Second, BookMaxAge: time.Minute,
	}
	broker := &fakeBroker{
		cancelled: make(map[string]execengine2.CancelResult),
		status:    make(map[string]execengine2.OrderStatus),
	}
	if mutate != nil {
		mutate(&cfg, broker)
	}
	decider := &fakeStrategy{intent: execengine2.Plan{Action: 1}}
	b, err := budget.New(20, 3)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: now}
	sink := &fakeUpdates{}
	engine, err := execengine2.NewEngine(cfg, execengine2.Setup{
		Broker: broker, Limit: b, Clock: clock, Strategy: decider, Changes: sink, Logger: testLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.OnBook(context.Background(), "A", now, 99, 100); err != nil {
		t.Fatal(err)
	}
	if err := engine.OnBook(context.Background(), "B", now, 199, 200); err != nil {
		t.Fatal(err)
	}
	return &testSet{
		engine: engine, broker: broker, decider: decider, budget: b, clock: clock, sink: sink, now: now,
	}
}

func TestTwoLimitsFirstFill(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 2 || !f.engine.HasTrade() {
		t.Fatalf("opening calls=%d working=%v", len(f.broker.places), f.engine.HasTrade())
	}
	err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99, At: f.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.broker.cancels) != 2 || f.broker.cancels[0] != "o2" || f.broker.cancels[1] != "o1" {
		t.Fatalf("cancels = %v, want [o2 o1]", f.broker.cancels)
	}
	if len(f.broker.places) != 3 {
		t.Fatalf("placement calls = %d, want 3", len(f.broker.places))
	}
	hedge := f.broker.places[2].req
	if hedge.Kind != execengine2.OrderMarket || hedge.Leg != execengine2.LegB ||
		hedge.Side != execengine2.SideSell || hedge.Lots != 2 {
		t.Fatalf("hedge = %+v", hedge)
	}
	if f.engine.HasTrade() || f.engine.Position() != 2 || len(f.decider.commits) != 1 {
		t.Fatalf(
			"working=%v position=%d commits=%v",
			f.engine.HasTrade(), f.engine.Position(), f.decider.commits,
		)
	}
	if got := f.budget.Remaining(); got != 17 {
		t.Fatalf("remaining budget = %d, want 17", got)
	}
	if len(f.sink.positions) != 2 || f.sink.positions[0].Lots != 2 || f.sink.positions[1].Lots != -2 {
		t.Fatalf("inventory deltas = %+v", f.sink.positions)
	}
}

func TestMarketOrderBlocksNewTrade(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99,
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnSignal(
		context.Background(), execengine2.Signal{Time: f.now.Add(time.Second)},
	); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 3 {
		t.Fatalf("new clip opened with pending taker; placements=%d", len(f.broker.places))
	}
	if err := f.engine.OnOrderStatus(
		context.Background(), "o3", execengine2.OrderStatus{Filled: 2, Done: true},
	); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnSignal(
		context.Background(), execengine2.Signal{Time: f.now.Add(2 * time.Second)},
	); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 5 {
		t.Fatalf("confirmed taker did not unblock next clip; placements=%d", len(f.broker.places))
	}
}

func TestClearOpenErrorClosesFirstOrder(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(_ *execengine2.Config, broker *fakeBroker) {
		broker.placeFunc = func(
			_ context.Context, _ execengine2.OrderRequest, call int,
		) (string, error) {
			if call == 2 {
				return "", execengine2.NotPlaced(errors.New("rejected"))
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now})
	if err == nil {
		t.Fatal("opening rejection was not returned")
	}
	if f.engine.HasTrade() || f.engine.Code() != execengine2.StateReady {
		t.Fatalf("working=%v state=%s", f.engine.HasTrade(), f.engine.Code())
	}
	if len(f.broker.cancels) != 1 || f.broker.cancels[0] != "o1" {
		t.Fatalf("partial opening was not retired: %v", f.broker.cancels)
	}
}

func TestUnknownOpenNeedsCheck(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(_ *execengine2.Config, broker *fakeBroker) {
		broker.placeFunc = func(
			_ context.Context, _ execengine2.OrderRequest, call int,
		) (string, error) {
			if call == 2 {
				return "", execengine2.OrderUnknown("cid-2", context.DeadlineExceeded)
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now})
	if err == nil || f.engine.Code() != execengine2.StateCheckNeeded {
		t.Fatalf("err=%v state=%s", err, f.engine.Code())
	}
	if len(f.broker.places) != 2 {
		t.Fatalf("ambiguous placement was blindly retried; calls=%d", len(f.broker.places))
	}
	if !f.engine.CheckPositions(0, 0) || f.engine.Code() != execengine2.StateReady {
		t.Fatal("clean broker reconciliation did not restore healthy state")
	}
}

func TestUnknownHedgeIsBlindCredited(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(_ *execengine2.Config, broker *fakeBroker) {
		broker.placeFunc = func(
			_ context.Context, _ execengine2.OrderRequest, call int,
		) (string, error) {
			if call == 3 {
				return "", execengine2.OrderUnknown("cid-3", context.DeadlineExceeded)
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99,
	}); err != nil {
		t.Fatal(err)
	}
	info := f.engine.Info()
	if info.Code != execengine2.StateCheckNeeded || info.HasTrade || info.MarketOrders != 0 ||
		f.engine.Position() != 2 {
		t.Fatalf("unverified hedge snapshot = %+v position=%d", info, f.engine.Position())
	}
	if len(f.sink.positions) != 2 || f.sink.positions[1].Lots != -2 {
		t.Fatalf("blind hedge accounting = %+v", f.sink.positions)
	}
	if !f.engine.CheckPositions(2, -2) || f.engine.Info().UnknownOrders != 0 {
		t.Fatal("full inventory confirmation did not resolve the credited hedge")
	}
}

func TestReusedTakerIDNeedsStableReconcile(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(_ *execengine2.Config, broker *fakeBroker) {
		broker.placeFunc = func(
			_ context.Context, _ execengine2.OrderRequest, call int,
		) (string, error) {
			if call == 3 {
				return "o2", nil
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99,
	}); err != nil {
		t.Fatal(err)
	}
	if f.engine.CheckPositions(2, -2) || f.engine.Code() != execengine2.StateCheckNeeded {
		t.Fatal("reused id was released by the first clean position check")
	}
	if !f.engine.CheckPositions(2, -2) || f.engine.Code() != execengine2.StateReady {
		t.Fatal("stable repeated position check did not release reused-id quarantine")
	}
}

func TestMarketShortFillGetsHedge(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99,
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnOrderStatus(
		context.Background(), "o3", execengine2.OrderStatus{Filled: 1, Done: true},
	); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 4 || f.broker.places[3].req.Lots != 1 ||
		f.broker.places[3].req.Role != execengine2.RoleFix || f.engine.Info().Hedges != 0 {
		t.Fatalf("shortfall was not hedged in the status event: info=%+v calls=%+v", f.engine.Info(), f.broker.places)
	}
	if len(f.sink.positions) != 4 || f.sink.positions[2].Lots != 1 || f.sink.positions[3].Lots != -1 {
		t.Fatalf("shortfall accounting = %+v", f.sink.positions)
	}
	if err := f.engine.OnOrderStatus(context.Background(), "o3", execengine2.OrderStatus{Filled: 1, Done: true}); err != nil {
		t.Fatal(err)
	}
	fill := execengine2.Fill{FillID: "late-taker", OrderID: "o3", Lots: 1, Price: 198}
	for range 2 {
		if err := f.engine.OnFill(context.Background(), fill); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.sink.prices) != 1 || f.sink.prices[0].OrderID != "o3" ||
		f.sink.prices[0].Lots != 1 || f.sink.prices[0].From != 199 || f.sink.prices[0].To != 198 {
		t.Fatalf("late execution did not amend exactly once: %+v", f.sink.prices)
	}
	if err := f.engine.OnOrderStatus(context.Background(), "o4", execengine2.OrderStatus{Filled: 1, Done: true}); err != nil {
		t.Fatal(err)
	}
	cancels, statuses := len(f.broker.cancels), len(f.broker.statuses)
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	a, b := inventory(f.sink)
	if len(f.broker.places) != 4 || len(f.broker.cancels) != cancels || len(f.broker.statuses) != statuses ||
		len(f.sink.positions) != 4 || a != 2 || b != -2 || f.engine.Info().Hedges != 0 {
		t.Fatalf("terminal replay or retry duplicated the repair: info=%+v calls=%+v inventory=(%d,%d)", f.engine.Info(), f.broker.places, a, b)
	}
}

func TestNonDeadMarketStatusWaitsForFillStream(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	ctx := context.Background()
	if err := f.engine.OnSignal(ctx, execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(ctx, execengine2.Fill{FillID: "maker", OrderID: "o1", Lots: 2, Price: 99}); err != nil {
		t.Fatal(err)
	}
	before := f.engine.Info()
	if before.MarketOrders != 1 || len(f.sink.positions) != 2 || len(f.sink.prices) != 0 {
		t.Fatalf("expected an accepted, unconfirmed hedge: info=%+v deltas=%+v", before, f.sink.positions)
	}
	places, cancels, statuses := len(f.broker.places), len(f.broker.cancels), len(f.broker.statuses)
	// Finam FILLED/EXECUTED stream statuses carry the v1 dead=false flag.
	if err := f.engine.OnOrderStatus(ctx, "o3", execengine2.OrderStatus{Filled: 2, Done: false}); err != nil {
		t.Fatal(err)
	}
	if f.engine.Info() != before || len(f.sink.positions) != 2 || len(f.sink.prices) != 0 ||
		len(f.broker.places) != places || len(f.broker.cancels) != cancels || len(f.broker.statuses) != statuses {
		t.Fatalf("non-dead status changed the hedge: info=%+v deltas=%+v", f.engine.Info(), f.sink.positions)
	}
	if err := f.engine.OnFill(ctx, execengine2.Fill{FillID: "hedge", OrderID: "o3", Lots: 2, Price: 198}); err != nil {
		t.Fatal(err)
	}
	a, b := inventory(f.sink)
	if f.engine.Info().MarketOrders != 0 || len(f.sink.positions) != 2 || a != 2 || b != -2 ||
		len(f.broker.places) != places || len(f.broker.cancels) != cancels || len(f.broker.statuses) != statuses {
		t.Fatalf("fill did not confirm the existing hedge: info=%+v inventory=(%d,%d)", f.engine.Info(), a, b)
	}
	if len(f.sink.prices) != 1 || f.sink.prices[0] != (execengine2.PriceChange{
		OrderID: "o3", Symbol: "B", Lots: 2, From: 199, To: 198,
	}) {
		t.Fatalf("actual fill amendment = %+v", f.sink.prices)
	}
}

func TestFailedHedgeIsSaved(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, func(cfg *execengine2.Config, broker *fakeBroker) {
		cfg.HedgeTries = 1
		broker.placeFunc = func(
			_ context.Context, _ execengine2.OrderRequest, call int,
		) (string, error) {
			if call == 3 {
				return "", execengine2.NotPlaced(errors.New("temporary reject"))
			}
			return fmt.Sprintf("o%d", call), nil
		}
	})
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "trade-a", OrderID: "o1", Lots: 2, Price: 99,
	})
	if err == nil {
		t.Fatal("failed hedge was not returned")
	}
	if snap := f.engine.Info(); snap.Code != execengine2.StateFixing ||
		snap.Hedges != 1 {
		t.Fatalf("snapshot after debt = %+v", snap)
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(context.Background(), f.clock.now); err != nil {
		t.Fatal(err)
	}
	snap := f.engine.Info()
	if snap.Hedges != 0 || snap.MarketOrders != 1 || f.engine.Position() != 2 {
		t.Fatalf("snapshot after recovery = %+v position=%d", snap, f.engine.Position())
	}
}

func TestStopDoesNotHedgeLateFill(t *testing.T) {
	t.Parallel()
	f := newTestSet(t, nil)
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.Stop(context.Background(), "manual"); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: "late-a", OrderID: "o1", Lots: 2, Price: 99,
	}); err != nil {
		t.Fatal(err)
	}
	if len(f.broker.places) != 2 {
		t.Fatalf("halted engine placed a hedge: %+v", f.broker.places)
	}
	if f.engine.Code() != execengine2.StateStopped || len(f.sink.positions) != 1 ||
		f.sink.positions[0].Lots != 2 {
		t.Fatalf("late fill after stop: state=%s changes=%+v", f.engine.Code(), f.sink.positions)
	}
}

func TestContextGetsToBroker(t *testing.T) {
	t.Parallel()
	type key string
	const trace key = "trace"
	f := newTestSet(t, nil)
	ctx := context.WithValue(context.Background(), trace, "request-7")
	if err := f.engine.OnSignal(ctx, execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.broker.places {
		if got := call.ctx.Value(trace); got != "request-7" {
			t.Fatalf("broker context value = %v", got)
		}
	}
}
