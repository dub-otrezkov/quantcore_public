package execengine2

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type hedgeRetryBroker struct {
	requests []OrderRequest
	place    func(OrderRequest, int) (string, error)
}

func (b *hedgeRetryBroker) Place(_ context.Context, req OrderRequest) (string, error) {
	b.requests = append(b.requests, req)
	if b.place != nil {
		return b.place(req, len(b.requests))
	}
	return fmt.Sprintf("hedge-%d", len(b.requests)), nil
}

func (*hedgeRetryBroker) Cancel(context.Context, string) (CancelResult, error) {
	return CancelResult{}, errors.New("unexpected cancel")
}

func (*hedgeRetryBroker) Status(context.Context, string) (OrderStatus, error) {
	return OrderStatus{}, errors.New("unexpected status")
}

type hedgeRetryStrategy struct{}

func (hedgeRetryStrategy) Peek(Signal) Plan              { return Plan{} }
func (hedgeRetryStrategy) Commit(Plan, time.Time) Result { return Result{} }
func (hedgeRetryStrategy) Position() int                 { return 0 }

type hedgeRetryLimit struct {
	left    int64
	classes []LimitKind
}

func (b *hedgeRetryLimit) Take(ops int64, class LimitKind) bool {
	b.classes = append(b.classes, class)
	if ops != 1 || b.left < ops {
		return false
	}
	b.left -= ops
	return true
}

func (b *hedgeRetryLimit) Remaining() int64 { return b.left }

type hedgeRetryUpdates struct{ lots int }

func (u *hedgeRetryUpdates) Apply(change PositionChange) { u.lots += change.Lots }
func (*hedgeRetryUpdates) Amend(PriceChange)             {}

func newHedgeRetryTest(t *testing.T, step, tries int) (*Engine, *hedgeRetryBroker, *hedgeRetryLimit, *hedgeRetryUpdates) {
	t.Helper()
	broker := &hedgeRetryBroker{}
	limit := &hedgeRetryLimit{left: 100}
	updates := &hedgeRetryUpdates{}
	e, err := NewEngine(Config{
		LegA: "A", LegB: "B", Lots: 10,
		RejectRetryLotStep: step, HedgeTries: tries,
	}, Setup{Broker: broker, Limit: limit, Strategy: hedgeRetryStrategy{}, Changes: updates})
	if err != nil {
		t.Fatal(err)
	}
	return e, broker, limit, updates
}

func hedgeRetryRequest(lots int) OrderRequest {
	return OrderRequest{
		Symbol: "B", Leg: LegB, Kind: OrderMarket, Role: RoleHedge,
		Side: SideSell, Lots: lots, Price: 200, TradeID: 7,
	}
}

func TestHedgeRemainderAfterUnusableIDGetsScheduled(t *testing.T) {
	t.Parallel()
	e, broker, _, updates := newHedgeRetryTest(t, 3, 1)
	broker.place = func(_ OrderRequest, call int) (string, error) {
		switch call {
		case 1, 2:
			return "", NotPlaced(errors.New("too large"))
		case 3:
			return "", nil // accepted four lots, but no usable order ID
		default:
			return "remaining", nil
		}
	}
	if err := e.placeHedge(context.Background(), hedgeRetryRequest(10)); err != nil {
		t.Fatal(err)
	}
	if e.Info().Hedges != 1 || updates.lots != -4 {
		t.Fatalf("accepted chunk lost its remainder: info=%+v lots=%d", e.Info(), updates.lots)
	}
	if err := e.OnTick(context.Background(), e.clock.Now().Add(e.config.RetryWait)); err != nil {
		t.Fatal(err)
	}
	if len(broker.requests) != 4 || broker.requests[3].Lots != 6 || e.Info().Hedges != 0 || updates.lots != -10 {
		t.Fatalf("remainder was not recovered exactly once: requests=%v info=%+v lots=%d", broker.requests, e.Info(), updates.lots)
	}
}

func checkHedgeRetryAttempts(t *testing.T, broker *hedgeRetryBroker, limit *hedgeRetryLimit, want []int) {
	t.Helper()
	got := make([]int, len(broker.requests))
	for i, req := range broker.requests {
		got[i] = req.Lots
		if req != hedgeRetryRequest(req.Lots) {
			t.Fatalf("hedge request metadata changed: %+v", req)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("placement sizes = %v, want %v", got, want)
	}
	for i, class := range limit.classes {
		if class != LimitMust {
			t.Fatalf("attempt %d used budget class %v", i, class)
		}
	}
	if len(limit.classes) < len(broker.requests) {
		t.Fatalf("%d placements consumed only %d budget checks", len(broker.requests), len(limit.classes))
	}
}

func TestHedgeShrinksAndAccumulatesFullObligation(t *testing.T) {
	t.Parallel()
	e, broker, limit, updates := newHedgeRetryTest(t, 2, 1)
	broker.place = func(req OrderRequest, call int) (string, error) {
		if req.Lots > 4 {
			return "", NotPlaced(errors.New("size rejected"))
		}
		return fmt.Sprintf("hedge-%d", call), nil
	}
	if err := e.placeHedge(context.Background(), hedgeRetryRequest(10)); err != nil {
		t.Fatal(err)
	}
	checkHedgeRetryAttempts(t, broker, limit, []int{10, 8, 6, 4, 4, 2})
	if updates.lots != -10 || len(e.hedges.All()) != 0 || e.hedges.MarketCount() != 3 {
		t.Fatalf("lots=%d debts=%+v pending=%d", updates.lots, e.hedges.All(), e.hedges.MarketCount())
	}
	if limit.left != 94 {
		t.Fatalf("remaining budget = %d, want 94", limit.left)
	}
}

func TestHedgeShrinkKeepsOnlyUnplacedDebt(t *testing.T) {
	t.Parallel()
	e, broker, limit, updates := newHedgeRetryTest(t, 3, 2)
	rejected := errors.New("size rejected")
	broker.place = func(_ OrderRequest, call int) (string, error) {
		if call == 3 {
			return "accepted-four", nil
		}
		return "", NotPlaced(rejected)
	}
	if err := e.placeHedge(context.Background(), hedgeRetryRequest(10)); !errors.Is(err, rejected) {
		t.Fatalf("placement error = %v", err)
	}
	checkHedgeRetryAttempts(t, broker, limit, []int{10, 7, 4, 4, 1, 6, 6})
	debts := e.hedges.All()
	if len(debts) != 1 || debts[0].Request != hedgeRetryRequest(6) || updates.lots != -4 {
		t.Fatalf("accepted lots=%d debts=%+v", updates.lots, debts)
	}
	if e.Code() != StateFixing || e.hedges.MarketCount() != 1 {
		t.Fatalf("state=%s pending=%d", e.Code(), e.hedges.MarketCount())
	}
}

func TestHedgeUnknownInterruptsLadderAndPreservesOtherLots(t *testing.T) {
	t.Parallel()
	e, broker, limit, updates := newHedgeRetryTest(t, 3, 3)
	broker.place = func(_ OrderRequest, call int) (string, error) {
		switch call {
		case 3:
			return "accepted-four", nil
		case 4:
			return "", OrderUnknown("ambiguous-four", context.DeadlineExceeded)
		default:
			return "", NotPlaced(errors.New("size rejected"))
		}
	}
	if err := e.placeHedge(context.Background(), hedgeRetryRequest(10)); err != nil {
		t.Fatal(err)
	}
	checkHedgeRetryAttempts(t, broker, limit, []int{10, 7, 4, 4})
	debts := e.hedges.All()
	if len(debts) != 1 || debts[0].Request != hedgeRetryRequest(2) || updates.lots != -8 {
		t.Fatalf("accepted and ambiguous lots=%d debts=%+v", updates.lots, debts)
	}
	if e.Code() != StateCheckNeeded || e.hedges.MarketCount() != 1 {
		t.Fatalf("state=%s pending=%d", e.Code(), e.hedges.MarketCount())
	}
}

func TestHedgeShrinkBudgetDenialPreservesRemainder(t *testing.T) {
	t.Parallel()
	e, broker, limit, updates := newHedgeRetryTest(t, 3, 2)
	limit.left = 3
	broker.place = func(_ OrderRequest, call int) (string, error) {
		if call == 3 {
			return "accepted-four", nil
		}
		return "", NotPlaced(errors.New("size rejected"))
	}
	if err := e.placeHedge(context.Background(), hedgeRetryRequest(10)); err == nil {
		t.Fatal("budget denial was not returned")
	}
	checkHedgeRetryAttempts(t, broker, limit, []int{10, 7, 4})
	debts := e.hedges.All()
	if len(debts) != 1 || debts[0].Request != hedgeRetryRequest(6) || updates.lots != -4 {
		t.Fatalf("accepted lots=%d debts=%+v", updates.lots, debts)
	}
	if e.Code() != StateStopped || len(limit.classes) != 4 || limit.left != 0 {
		t.Fatalf("state=%s budget checks=%d remaining=%d", e.Code(), len(limit.classes), limit.left)
	}
}

func TestHedgeRejectLadderAndOrdinaryAttemptsAreBounded(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		step int
		want []int
	}{
		{name: "disabled", step: 0, want: []int{5, 5}},
		{name: "shrinking", step: 2, want: []int{5, 3, 1, 5, 5}},
		{name: "floor", step: 10, want: []int{5, 1, 5, 5}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, broker, limit, updates := newHedgeRetryTest(t, test.step, 2)
			broker.place = func(OrderRequest, int) (string, error) {
				return "", NotPlaced(errors.New("unavailable"))
			}
			if err := e.placeHedge(context.Background(), hedgeRetryRequest(5)); err == nil {
				t.Fatal("exhausted attempts returned no error")
			}
			checkHedgeRetryAttempts(t, broker, limit, test.want)
			debts := e.hedges.All()
			if len(debts) != 1 || debts[0].Request.Lots != 5 || updates.lots != 0 {
				t.Fatalf("accounted lots=%d debts=%+v", updates.lots, debts)
			}
		})
	}
}

func TestRejectRetryLotStepMustNotBeNegative(t *testing.T) {
	t.Parallel()
	_, err := NewEngine(Config{
		LegA: "A", LegB: "B", Lots: 1, RejectRetryLotStep: -1,
	}, Setup{Broker: &hedgeRetryBroker{}, Strategy: hedgeRetryStrategy{}})
	if err == nil {
		t.Fatal("negative reject retry step was accepted")
	}
}
