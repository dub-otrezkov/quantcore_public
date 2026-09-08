package execengine2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
)

type lifecycleBroker struct {
	*fakeBroker
	blocked       map[string]bool
	retired       map[string]bool
	beforeMarket  []string
	unsafeMarkets int
	denyMust      bool
	resumeID      string
	resumes       int
}

func (b *lifecycleBroker) Place(ctx context.Context, req execengine2.OrderRequest) (string, error) {
	if req.Kind == execengine2.OrderMarket {
		for _, id := range b.beforeMarket {
			if !b.retired[id] {
				b.unsafeMarkets++
			}
		}
	}
	return b.fakeBroker.Place(ctx, req)
}

func (b *lifecycleBroker) Cancel(ctx context.Context, id string) (execengine2.CancelResult, error) {
	if b.blocked[id] {
		b.cancels = append(b.cancels, id)
		return execengine2.CancelResult{}, errors.New("cancel unavailable")
	}
	b.retired[id] = true
	return b.fakeBroker.Cancel(ctx, id)
}

func (b *lifecycleBroker) Status(ctx context.Context, id string) (execengine2.OrderStatus, error) {
	if b.blocked[id] {
		return execengine2.OrderStatus{}, errors.New("status unavailable")
	}
	return b.fakeBroker.Status(ctx, id)
}

func (b *lifecycleBroker) Take(_ int64, class execengine2.LimitKind) bool {
	return !b.denyMust || class != execengine2.LimitMust
}
func (*lifecycleBroker) Remaining() int64 { return 100 }
func (b *lifecycleBroker) ResumePlacement(context.Context, execengine2.OrderRequest, string) (string, error) {
	b.resumes++
	return b.resumeID, nil
}

func newLifecycleSet(t *testing.T, cfg execengine2.Config) (*testSet, *lifecycleBroker) {
	t.Helper()
	f := newTestSet(t, nil)
	b := &lifecycleBroker{fakeBroker: f.broker, blocked: map[string]bool{}, retired: map[string]bool{}}
	cfg.LegA, cfg.LegB, cfg.HedgeTries, cfg.RetryWait = "A", "B", 1, time.Second
	var err error
	f.engine, err = execengine2.NewEngine(cfg, execengine2.Setup{
		Broker: b, Limit: b, Clock: f.clock, Strategy: f.decider, Changes: f.sink, Logger: testLog{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnBook(context.Background(), "A", f.now, 99, 100); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnBook(context.Background(), "B", f.now, 199, 200); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnSignal(context.Background(), execengine2.Signal{Time: f.now}); err != nil {
		t.Fatal(err)
	}
	return f, b
}

func lifecycleInventory(f *testSet) (a, b int) {
	for _, delta := range f.sink.positions {
		if delta.Symbol == "A" {
			a += delta.Lots
		} else {
			b += delta.Lots
		}
	}
	return a, b
}

func TestLifecycleDeferredCounterpartKeepsSettlementPolicy(t *testing.T) {
	for _, keepPartial := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep_partial=%v", keepPartial), func(t *testing.T) {
			f, b := newLifecycleSet(t, execengine2.Config{
				Lots: 4, Mode: execengine2.ModeTwoLimits, KeepPartialOpenOnTimeout: keepPartial,
			})
			ctx := context.Background()
			b.beforeMarket = []string{"o1", "o2"}
			b.blocked["o1"], b.blocked["o2"] = true, true
			b.cancelled["o1"] = execengine2.CancelResult{Filled: 1}
			b.cancelled["o2"] = execengine2.CancelResult{Filled: 3}
			if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "first", Lots: 1, Price: 99}); err == nil {
				t.Fatal("unconfirmed counterpart cancellation was not reported")
			}
			b.blocked["o2"] = false
			f.clock.now = f.now.Add(time.Second)
			if err := f.engine.OnTick(ctx, f.clock.now); err == nil {
				t.Fatal("first maker cancellation is still unresolved")
			}
			if len(b.places) != 2 || len(f.decider.commits) != 0 || !f.engine.HasTrade() {
				t.Fatalf("settlement acted before both cancellations were known: calls=%+v info=%+v", b.places, f.engine.Info())
			}
			if err := f.engine.OnBook(ctx, "A", f.clock.now, 101, 102); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.OnBook(ctx, "B", f.clock.now, 201, 202); err != nil {
				t.Fatal(err)
			}
			b.blocked["o1"] = false
			f.clock.now = f.now.Add(2 * time.Second)
			if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
				t.Fatal(err)
			}
			wantLots, wantCommits := 4, 1
			if keepPartial {
				wantLots, wantCommits = 3, 0
			}
			a, hedgeB := lifecycleInventory(f)
			if a != wantLots || hedgeB != -wantLots || len(f.decider.commits) != wantCommits || f.engine.HasTrade() || b.unsafeMarkets != 0 {
				t.Fatalf("deferred cancellation lost settlement policy: inventory=(%d,%d) commits=%+v info=%+v unsafe=%d", a, hedgeB, f.decider.commits, f.engine.Info(), b.unsafeMarkets)
			}
			for i, call := range b.places[2:] {
				wantPrice := 102.0
				if call.req.Symbol == "B" {
					wantPrice = 201
				}
				if call.req.Price != wantPrice {
					t.Fatalf("settlement retained an old/passive price: %+v", call.req)
				}
				if err := f.engine.OnFill(ctx, execengine2.Fill{
					OrderID: fmt.Sprintf("o%d", i+3), FillID: "market-confirm",
					Lots: call.req.Lots, Price: wantPrice + 0.5,
				}); err != nil {
					t.Fatal(err)
				}
				if len(f.sink.prices) != i+1 {
					t.Fatalf("market fill did not amend its estimate exactly once: %+v", f.sink.prices)
				}
				amend := f.sink.prices[i]
				if amend.Symbol != call.req.Symbol || amend.Lots != call.req.Lots || amend.From != wantPrice || amend.To != wantPrice+0.5 {
					t.Fatalf("wrong market price amendment: %+v for %+v", amend, call.req)
				}
			}
			// A lower replay and late confirmation of the retired counterpart must
			// neither change inventory nor cause another hedge.
			calls := len(b.places)
			if err := f.engine.OnOrderStatus(ctx, "o2", execengine2.OrderStatus{Done: true, Filled: 1}); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o2", FillID: "late-b", Lots: 3, Price: 200}); err != nil {
				t.Fatal(err)
			}
			if a, hedgeB = lifecycleInventory(f); a != wantLots || hedgeB != -wantLots || len(b.places) != calls {
				t.Fatalf("terminal replay/late fill changed settled quantity: (%d,%d), calls=%d", a, hedgeB, len(b.places))
			}
		})
	}
}

func TestLifecycleExplicitDropDoesNotCommitWhenDebtIsPaid(t *testing.T) {
	f, b := newLifecycleSet(t, execengine2.Config{Lots: 2, Mode: execengine2.ModeLimitA})
	b.placeFunc = func(_ context.Context, _ execengine2.OrderRequest, call int) (string, error) {
		if call == 2 {
			return "", execengine2.NotPlaced(errors.New("hedge rejected"))
		}
		return fmt.Sprintf("o%d", call), nil
	}
	b.cancelled["o1"] = execengine2.CancelResult{Filled: 2}
	ctx := context.Background()
	if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "maker", Lots: 2, Price: 99}); err == nil {
		t.Fatal("expected hedge rejection")
	}
	if err := f.engine.StopTrade(ctx); err != nil {
		t.Fatal(err)
	}
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
		t.Fatal(err)
	}
	a, hedgeB := lifecycleInventory(f)
	if len(f.decider.commits) != 0 || f.engine.HasTrade() || a != 2 || hedgeB != -2 {
		t.Fatalf("explicit drop became a committed clip during recovery: commits=%+v inventory=(%d,%d) info=%+v", f.decider.commits, a, hedgeB, f.engine.Info())
	}
}

func TestLifecycleInternalBudgetHaltCancelsWithinFillEvent(t *testing.T) {
	f, b := newLifecycleSet(t, execengine2.Config{Lots: 4, Mode: execengine2.ModeLimitA})
	b.denyMust = true
	b.cancelled["o1"] = execengine2.CancelResult{Filled: 1}
	ctx := context.Background()
	if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "maker", Lots: 1, Price: 99}); err == nil {
		t.Fatal("mandatory budget denial was not reported")
	}
	if f.engine.Code() != execengine2.StateStopped || !b.retired["o1"] || len(b.places) != 1 {
		t.Fatalf("internal halt left a live maker until another event: cancels=%v info=%+v", b.cancels, f.engine.Info())
	}
	if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "late", Lots: 1, Price: 99}); err != nil {
		t.Fatal(err)
	}
	if len(b.places) != 1 || len(f.decider.commits) != 0 {
		t.Fatal("late execution bypassed the kill switch")
	}
}

func TestLifecycleUnknownAdoptionHaltCancelsWithinTick(t *testing.T) {
	for _, returnedID := range []string{"", "o1"} {
		t.Run("returned="+returnedID, func(t *testing.T) {
			f, b := newLifecycleSet(t, execengine2.Config{Lots: 4, Mode: execengine2.ModeLimitA})
			b.resumeID = returnedID
			b.placeFunc = func(context.Context, execengine2.OrderRequest, int) (string, error) {
				return "", execengine2.OrderUnknown("hedge-cid", context.DeadlineExceeded)
			}
			b.cancelled["o1"] = execengine2.CancelResult{Filled: 1}
			ctx := context.Background()
			if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "maker", Lots: 1, Price: 99}); err != nil {
				t.Fatal(err)
			}
			f.clock.now = f.now.Add(time.Second)
			if err := f.engine.OnTick(ctx, f.clock.now); err == nil {
				t.Fatal("unusable recovered order ID was accepted")
			}
			if f.engine.Code() != execengine2.StateStopped || !b.retired["o1"] || b.resumes != 1 || f.engine.Info().UnknownOrders != 1 {
				t.Fatalf("adoption halt lost its maker or unknown obligation: cancels=%v resumes=%d info=%+v", b.cancels, b.resumes, f.engine.Info())
			}
			if err := f.engine.OnTick(ctx, f.now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if b.resumes != 1 || len(b.places) != 2 || len(f.decider.commits) != 0 {
				t.Fatal("halted recovery sent another placement or committed a clip")
			}
		})
	}
}

func TestLifecycleDeferredDropHedgesStreamOnceDuringCancelOutage(t *testing.T) {
	f, b := newLifecycleSet(t, execengine2.Config{Lots: 1, Mode: execengine2.ModeTwoLimits})
	b.blocked["o1"], b.blocked["o2"] = true, true
	ctx := context.Background()
	if err := f.engine.StopTrade(ctx); err == nil {
		t.Fatal("expected unresolved cancellations")
	}
	fill := execengine2.Fill{OrderID: "o1", FillID: "outage-fill", Lots: 1, Price: 99}
	if err := f.engine.OnFill(ctx, fill); err != nil {
		t.Fatal(err)
	}
	a, hedgeB := lifecycleInventory(f)
	if len(b.places) != 3 || a != 1 || hedgeB != -1 || b.places[2].req.TradeID != b.places[0].req.TradeID {
		t.Fatalf("proven stream fill remained naked or lost its owner: inventory=(%d,%d) places=%+v", a, hedgeB, b.places)
	}
	b.cancelled["o1"] = execengine2.CancelResult{Filled: 1}
	b.blocked["o1"], b.blocked["o2"] = false, false
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.OnFill(ctx, fill); err != nil {
		t.Fatal(err)
	}
	a, hedgeB = lifecycleInventory(f)
	if len(b.places) != 3 || a != 1 || hedgeB != -1 || f.engine.HasTrade() || len(f.decider.commits) != 0 {
		t.Fatalf("cancel acknowledgement re-hedged the proven fill: inventory=(%d,%d) places=%+v info=%+v", a, hedgeB, b.places, f.engine.Info())
	}
}

func TestLifecycleCountedUnknownDropAllowsIdleReconciliation(t *testing.T) {
	f, b := newLifecycleSet(t, execengine2.Config{Lots: 1, Mode: execengine2.ModeTwoLimits})
	b.blocked["o2"] = true
	b.placeFunc = func(context.Context, execengine2.OrderRequest, int) (string, error) {
		return "", execengine2.OrderUnknown("counted-hedge", context.DeadlineExceeded)
	}
	ctx := context.Background()
	if err := f.engine.OnOrderStatus(ctx, "o1", execengine2.OrderStatus{Done: true, Filled: 1}); err == nil {
		t.Fatal("expected unresolved counterpart cancellation")
	}
	b.blocked["o2"] = false
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.StopTrade(ctx); err != nil {
		t.Fatal(err)
	}
	if f.engine.HasTrade() || f.engine.Info().UnknownOrders != 1 || len(f.decider.commits) != 0 || len(b.places) != 3 {
		t.Fatalf("counted unknown/drop created an idle-reconciliation deadlock: info=%+v commits=%+v", f.engine.Info(), f.decider.commits)
	}
	// The simulator's strategy reads its already-updated ledger, without Commit.
	f.decider.pos = 1
	if !f.engine.CheckPositions(1, -1) || f.engine.Info().UnknownOrders != 0 || len(b.places) != 3 {
		t.Fatalf("authoritative paired position failed to release only the barrier: %+v", f.engine.Info())
	}
}

func TestLifecycleStoppingStreamCatchesExistingCounterpartWithoutExtraHedge(t *testing.T) {
	f, b := newLifecycleSet(t, execengine2.Config{Lots: 4, Mode: execengine2.ModeTwoLimits})
	b.blocked["o1"] = true
	b.beforeMarket = []string{"o1", "o2"}
	b.cancelled["o2"] = execengine2.CancelResult{Filled: 3}
	ctx := context.Background()
	if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "first-a", Lots: 1, Price: 99}); err == nil {
		t.Fatal("expected unconfirmed maker cancellation")
	}
	if err := f.engine.OnFill(ctx, execengine2.Fill{OrderID: "o1", FillID: "catch-up-a", Lots: 2, Price: 99}); err != nil {
		t.Fatal(err)
	}
	a, hedgeB := lifecycleInventory(f)
	if len(b.places) != 2 || a != 3 || hedgeB != -3 {
		t.Fatalf("stream catch-up was mistaken for a fresh obligation: inventory=(%d,%d) places=%+v", a, hedgeB, b.places)
	}
	b.cancelled["o1"] = execengine2.CancelResult{Filled: 3}
	b.blocked["o1"] = false
	f.clock.now = f.now.Add(time.Second)
	if err := f.engine.OnTick(ctx, f.clock.now); err != nil {
		t.Fatal(err)
	}
	a, hedgeB = lifecycleInventory(f)
	if a != 4 || hedgeB != -4 || len(f.decider.commits) != 1 || f.decider.commits[0].Lots != 4 || b.unsafeMarkets != 0 {
		t.Fatalf("cancel-outage catch-up exceeded the target: inventory=(%d,%d) commits=%+v unsafe=%d", a, hedgeB, f.decider.commits, b.unsafeMarkets)
	}
}
