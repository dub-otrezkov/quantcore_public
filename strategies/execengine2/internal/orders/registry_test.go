package orders_test

import (
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/internal/orders"
)

func makerRequest(lots int) execengine2.OrderRequest {
	return execengine2.OrderRequest{
		Symbol: "A", Side: execengine2.SideBuy, Kind: execengine2.OrderLimit,
		Role: execengine2.RoleTrade, Leg: execengine2.LegA, Lots: lots, Price: 100,
	}
}

func TestZeroValueAndFillID(t *testing.T) {
	t.Parallel()
	var r orders.List
	if _, err := r.Add("o1", makerRequest(3), time.Time{}, 100, false); err != nil {
		t.Fatal(err)
	}
	fill := execengine2.Fill{FillID: "trade-1", OrderID: "o1", Lots: 1, Price: 100}
	first := r.AddFill(fill)
	second := r.AddFill(fill)
	if first.Lots != 1 || !second.Again || second.Lots != 0 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestCancelThenLateFill(t *testing.T) {
	t.Parallel()
	var r orders.List
	if _, err := r.Add("o1", makerRequest(4), time.Time{}, 100, false); err != nil {
		t.Fatal(err)
	}
	terminal := r.Close("o1", 2)
	if terminal.Lots != 2 {
		t.Fatalf("terminal delta = %d, want 2", terminal.Lots)
	}
	late := r.AddFill(execengine2.Fill{
		FillID: "trade-1", OrderID: "o1", Lots: 2, Price: 99.5,
	})
	if late.Lots != 0 || late.PriceLots != 2 || late.Conflict {
		t.Fatalf("late fill = %+v", late)
	}
	contradiction := r.AddFill(execengine2.Fill{
		FillID: "trade-2", OrderID: "o1", Lots: 1, Price: 99.5,
	})
	if contradiction.Lots != 1 || !contradiction.Conflict {
		t.Fatalf("beyond-terminal fill = %+v", contradiction)
	}
}

func TestShortMarketFill(t *testing.T) {
	t.Parallel()
	var r orders.List
	req := makerRequest(5)
	req.Kind = execengine2.OrderMarket
	req.Role = execengine2.RoleHedge
	registered, err := r.Add("t1", req, time.Time{}, 101, true)
	if err != nil {
		t.Fatal(err)
	}
	if registered.Lots != 5 {
		t.Fatalf("placement delta = %d, want 5", registered.Lots)
	}
	terminal := r.Close("t1", 3)
	if terminal.Lots != -2 {
		t.Fatalf("terminal correction = %d, want -2", terminal.Lots)
	}
}

func TestTerminalReplayCannotUndoAccountedExecutions(t *testing.T) {
	for name, market := range map[string]bool{"maker": false, "taker": true} {
		t.Run(name, func(t *testing.T) {
			var r orders.List
			req := makerRequest(10)
			if market {
				req.Kind = execengine2.OrderMarket
			}
			if _, err := r.Add("o1", req, time.Time{}, 100, market); err != nil {
				t.Fatal(err)
			}
			wantFirst := 7
			if market {
				wantFirst = -3 // The first terminal result still reverses optimistic credit.
			}
			if first := r.Close("o1", 7); first.Lots != wantFirst {
				t.Fatalf("first terminal = %+v, want quantity delta %d", first, wantFirst)
			}
			for _, replayed := range []int{3, 7, 10} {
				change := r.Close("o1", replayed)
				if change.Lots != 0 || change.Order.Used != 7 || change.Order.EndFilled != 7 ||
					change.Conflict != (replayed != 7) {
					t.Fatalf("replayed terminal %d changed the settled account: %+v", replayed, change)
				}
			}
			late := r.AddFill(execengine2.Fill{OrderID: "o1", FillID: "late", Lots: 7, Price: 101})
			if late.Lots != 0 || late.PriceLots != 7 || late.Conflict {
				t.Fatalf("late confirmations must amend price without minting quantity: %+v", late)
			}
			beyond := r.AddFill(execengine2.Fill{OrderID: "o1", FillID: "beyond", Lots: 1, Price: 101})
			if beyond.Lots != 1 || !beyond.Conflict {
				t.Fatalf("a genuine beyond-terminal execution must remain visible: %+v", beyond)
			}
			if replay := r.Close("o1", 7); replay.Lots != 0 || replay.Order.Used != 8 || !replay.Conflict {
				t.Fatalf("terminal replay discarded a later proven fill: %+v", replay)
			}
		})
	}
}

func TestCloseListIsPrivate(t *testing.T) {
	t.Parallel()
	var r orders.List
	if _, err := r.Add("b", makerRequest(1), time.Time{}, 100, false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add("a", makerRequest(1), time.Time{}, 100, false); err != nil {
		t.Fatal(err)
	}
	r.NeedClose("b")
	r.NeedClose("a")
	got := r.OrdersToClose()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("pending retires = %v", got)
	}
	got[0] = "mutated"
	if again := r.OrdersToClose(); again[0] != "a" {
		t.Fatalf("caller mutated registry state: %v", again)
	}
}
