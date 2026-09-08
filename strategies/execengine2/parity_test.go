package execengine2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "QuantCore/strategies/execengine"
	v2 "QuantCore/strategies/execengine2"
)

type parityOrder struct {
	ID, Sym, Kind string
	Buy           bool
	Lots          int
	Price         float64
	Filled        int
	Reported      int
	Done          bool
}
type parityTransport struct {
	mu              sync.Mutex
	orders          []parityOrder
	calls           []string
	accounting      []string
	catch           bool
	rejectOnce      bool
	maxAcceptedLots int
}

func (b *parityTransport) place(sym, kind string, buy bool, lots int, price float64) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rejectOnce || (b.maxAcceptedLots > 0 && lots > b.maxAcceptedLots) {
		b.rejectOnce = false
		b.calls = append(b.calls, fmt.Sprintf("reject %s %s lots=%d", sym, kind, lots))
		return "", fmt.Errorf("definitive test rejection")
	}
	id := fmt.Sprint(len(b.orders) + 1)
	b.orders = append(b.orders, parityOrder{ID: id, Sym: sym, Kind: kind, Buy: buy, Lots: lots, Price: price})
	if kind == "market" {
		b.orders[len(b.orders)-1].Filled = lots
		b.orders[len(b.orders)-1].Done = true
	}
	b.calls = append(b.calls, fmt.Sprintf("place %s %s buy=%v lots=%d price=%g", sym, kind, buy, lots, price))
	return id, nil
}
func (b *parityTransport) cancel(id string) (int, error) {
	b.calls = append(b.calls, "cancel "+id)
	for i := range b.orders {
		if b.orders[i].ID == id {
			if b.catch && b.orders[i].Sym == "A" {
				b.orders[i].Filled++
				b.catch = false
			}
			b.orders[i].Done = true
			return b.orders[i].Filled, nil
		}
	}
	panic(id)
}
func (b *parityTransport) status(id string) (int, bool, error) {
	b.calls = append(b.calls, "status "+id)
	for _, r := range b.orders {
		if r.ID == id {
			return r.Filled, r.Done, nil
		}
	}
	panic(id)
}

type parityBrokerV1 struct{ *parityTransport }

func (b parityBrokerV1) PlaceBid(s string, l int, p float64) (string, error) {
	id, err := b.place(s, "limit", true, l, p)
	return id, v1.NewDefinitiveReject(err)
}
func (b parityBrokerV1) PlaceAsk(s string, l int, p float64) (string, error) {
	id, err := b.place(s, "limit", false, l, p)
	return id, v1.NewDefinitiveReject(err)
}
func (b parityBrokerV1) Buy(s string, l int) (string, error) { return b.place(s, "market", true, l, 0) }
func (b parityBrokerV1) Sell(s string, l int) (string, error) {
	return b.place(s, "market", false, l, 0)
}
func (b parityBrokerV1) Cancel(id string) (int, error)       { return b.cancel(id) }
func (b parityBrokerV1) Status(id string) (int, bool, error) { return b.status(id) }

type parityBrokerV2 struct{ *parityTransport }

// Market requests have no limit price in v1's broker API. Their accounting
// price is compared separately, including subsequent actual-price amendments.
type paritySinkV1 struct{ b *parityTransport }

func (s paritySinkV1) Fill(symbol string, buy bool, lots int, price float64) {
	s.b.accounting = append(s.b.accounting, fmt.Sprintf("fill %s buy=%v lots=%d price=%g", symbol, buy, lots, price))
}
func (s paritySinkV1) Amend(symbol string, buy bool, lots int, from, to float64) {
	s.b.accounting = append(s.b.accounting, fmt.Sprintf("amend %s buy=%v lots=%d from=%g to=%g", symbol, buy, lots, from, to))
}

type parityUpdatesV2 struct{ b *parityTransport }

func (s parityUpdatesV2) Apply(change v2.PositionChange) {
	buy, lots := change.Lots > 0, change.Lots
	if lots < 0 {
		lots = -lots
	}
	paritySinkV1(s).Fill(change.Symbol, buy, lots, change.Price)
}
func (s parityUpdatesV2) Amend(change v2.PriceChange) {
	for _, order := range s.b.orders {
		if order.ID == change.OrderID {
			paritySinkV1(s).Amend(change.Symbol, order.Buy, change.Lots, change.From, change.To)
			return
		}
	}
	panic("amend for unknown order: " + change.OrderID)
}

func (b parityBrokerV2) Place(_ context.Context, r v2.OrderRequest) (string, error) {
	kind := "limit"
	price := r.Price
	if r.Kind == v2.OrderMarket {
		kind = "market"
		price = 0
	}
	id, err := b.place(r.Symbol, kind, r.Side == v2.SideBuy, r.Lots, price)
	return id, v2.NotPlaced(err)
}
func (b parityBrokerV2) Cancel(_ context.Context, id string) (v2.CancelResult, error) {
	n, e := b.cancel(id)
	return v2.CancelResult{Filled: n}, e
}
func (b parityBrokerV2) Status(_ context.Context, id string) (v2.OrderStatus, error) {
	n, d, e := b.status(id)
	return v2.OrderStatus{Filled: n, Done: d}, e
}

type parityStrategy struct {
	b                      *parityTransport
	action, lots, position int
	close                  bool
	mode                   int
	commits                []int
	saves                  int
}

func (s *parityStrategy) SaveLots() {
	s.saves++
	s.b.accounting = append(s.b.accounting, fmt.Sprintf("save position=%d", s.position))
}

type parityStrategyV1 struct{ *parityStrategy }

func (s parityStrategyV1) Peek(v1.RowState) v1.Intent {
	modes := []v1.ExecMode{v1.ExecDefault, v1.ExecDualPassive, v1.ExecSoloMaker, v1.ExecSoloMakerLegB, v1.ExecTaker}
	return v1.Intent{Action: s.action, Lots: s.lots, IsClose: s.close, ExecMode: modes[s.mode]}
}
func (s parityStrategyV1) Commit(p v1.Intent, _ time.Time) v1.Decision {
	s.commits = append(s.commits, p.Lots)
	s.position += p.Action * p.Lots
	s.b.accounting = append(s.b.accounting, fmt.Sprintf("commit action=%d lots=%d position=%d", p.Action, p.Lots, s.position))
	return v1.Decision{}
}
func (s parityStrategyV1) Position() int { return s.position }

type parityStrategyV2 struct{ *parityStrategy }

func (s parityStrategyV2) Peek(v2.Signal) v2.Plan {
	modes := []v2.Mode{v2.ModeDefault, v2.ModeTwoLimits, v2.ModeLimitA, v2.ModeLimitB, v2.ModeMarket}
	return v2.Plan{Action: s.action, Lots: s.lots, IsClose: s.close, Mode: modes[s.mode]}
}
func (s parityStrategyV2) Commit(p v2.Plan, _ time.Time) v2.Result {
	s.commits = append(s.commits, p.Lots)
	s.position += p.Action * p.Lots
	s.b.accounting = append(s.b.accounting, fmt.Sprintf("commit action=%d lots=%d position=%d", p.Action, p.Lots, s.position))
	return v2.Result{}
}
func (s parityStrategyV2) Position() int { return s.position }

type parityClock struct{ now time.Time }

func (c *parityClock) Now() time.Time { return c.now }

type parityLogger struct{}

func (parityLogger) Infof(string, ...any)     {}
func (parityLogger) Warnf(string, ...any)     {}
func (parityLogger) Criticalf(string, ...any) {}

type parityEngine struct {
	t           *testing.T
	old         *v1.Engine
	new         *v2.Engine
	b           *parityTransport
	s           *parityStrategy
	parityClock *parityClock
	base        time.Time
	errors      []string
	trace       []parityOutcome
	fillEvents  int
}

func (e *parityEngine) error(err error) {
	if err != nil {
		e.errors = append(e.errors, err.Error())
	}
}
func (e *parityEngine) book(sym string, at time.Time, bid, ask float64) {
	defer e.observe()
	if e.old != nil {
		e.old.OnBook(sym, at, bid, ask)
	} else {
		e.error(e.new.OnBook(context.Background(), sym, at, bid, ask))
	}
}
func (e *parityEngine) signal(at time.Time) {
	defer e.observe()
	if e.old != nil {
		e.old.OnState(v1.RowState{Time: at})
	} else {
		e.error(e.new.OnSignal(context.Background(), v2.Signal{Time: at}))
	}
}
func (e *parityEngine) tick(at time.Time) {
	defer e.observe()
	if e.old != nil {
		e.old.OnTick(at)
	} else {
		e.error(e.new.OnTick(context.Background(), at))
	}
}
func (e *parityEngine) pull(at time.Time) {
	defer e.observe()
	if e.old != nil {
		e.old.PullIfUnwanted(v1.RowState{Time: at})
	} else {
		e.error(e.new.PullIfUnwanted(context.Background(), v2.Signal{Time: at}))
	}
}
func (e *parityEngine) cancel() {
	defer e.observe()
	if e.old != nil {
		e.old.CancelClip()
	} else {
		e.error(e.new.StopTrade(context.Background()))
	}
}
func (e *parityEngine) fill(n int) {
	if len(e.b.orders) == 0 {
		e.t.Fatal("cannot replay a fill: opening placed no order")
	}
	e.fillOrder(0, n, e.b.orders[0].Price)
}
func (e *parityEngine) fillOrder(index, n int, price float64) {
	defer e.observe()
	if index >= len(e.b.orders) {
		e.t.Fatal("cannot replay a fill: missing broker order")
	}
	r := &e.b.orders[index]
	r.Reported += n
	r.Filled = max(r.Filled, r.Reported)
	if r.Filled == r.Lots {
		r.Done = true
	}
	e.fillEvents++
	if e.old != nil {
		e.old.OnFill(e.parityClock.now, r.ID, r.Sym, r.Buy, n, price)
	} else {
		side := v2.SideSell
		if r.Buy {
			side = v2.SideBuy
		}
		e.error(e.new.OnFill(context.Background(), v2.Fill{FillID: fmt.Sprint(e.fillEvents), OrderID: r.ID, Symbol: r.Sym, Side: side, Lots: n, Price: price, At: e.parityClock.now}))
	}
}

func (e *parityEngine) orderStatus(index int, dead bool) {
	defer e.observe()
	if index >= len(e.b.orders) {
		e.t.Fatal("cannot replay a status: missing broker order")
	}
	r := &e.b.orders[index]
	if dead {
		r.Done = true
	}
	// The maker stream maps CANCELLED/REJECTED/EXPIRED to v1 dead and v2 Done.
	// FILLED/EXECUTED map to false: OnFill will account and commit those orders.
	// This differs from Broker.Status, where Done is generic terminality. v1's
	// dead flag has no count; v2 receives that count directly. Requested Lots
	// remains unchanged when an order dies partially filled.
	if e.old != nil {
		e.old.OnOrderStatus(r.ID, dead)
	} else {
		e.error(e.new.OnOrderStatus(context.Background(), r.ID, v2.OrderStatus{Done: dead, Filled: r.Filled}))
	}
}

type parityOutcome struct {
	Accounting                             []string
	Saves                                  int
	Calls                                  []string
	Commits                                []int
	Position, BrokerA, BrokerB, LiveMakers int
	Working, Halted                        bool
}

func (e *parityEngine) parityOutcome() parityOutcome {
	r := parityOutcome{Calls: append([]string(nil), e.b.calls...), Commits: append([]int(nil), e.s.commits...), Accounting: append([]string(nil), e.b.accounting...), Saves: e.s.saves, Position: e.s.position}
	for _, o := range e.b.orders {
		sign := 1
		if !o.Buy {
			sign = -1
		}
		if o.Sym == "A" {
			r.BrokerA += sign * o.Filled
		} else {
			r.BrokerB += sign * o.Filled
		}
		if o.Kind == "limit" && !o.Done && o.Filled < o.Lots {
			r.LiveMakers++
		}
	}
	if e.old != nil {
		r.Working = e.old.Working()
		r.Halted = e.old.Halted()
	} else {
		r.Working = e.new.HasTrade()
		r.Halted = e.new.Code() == v2.StateStopped
	}
	return r
}
func (e *parityEngine) observe() { e.trace = append(e.trace, e.parityOutcome()) }

func runParityCase(t *testing.T, old bool, name string) []parityOutcome {
	t.Helper()
	e := &parityEngine{t: t, b: &parityTransport{}, s: &parityStrategy{action: 1, lots: 6}, base: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)}
	e.s.b = e.b
	e.parityClock = &parityClock{e.base}
	c1 := v1.EngineConfig{LegA: "A", LegB: "B", OrderVol: 6, HedgeRetries: 1}
	c2 := v2.Config{LegA: "A", LegB: "B", Lots: 6, HedgeTries: 1}
	switch name {
	case "partial_timeout", "keep_partial_timeout", "cancel_partial":
		c1.FillTimeout = time.Second
		c2.TradeTimeout = time.Second
		c1.SoloMakerLeg = true
		c2.Mode = v2.ModeLimitA
		if name == "keep_partial_timeout" {
			c1.KeepPartialOpenOnTimeout = true
			c2.KeepPartialOpenOnTimeout = true
		}
	case "force_close_timeout":
		c1.FillTimeout = time.Second
		c2.TradeTimeout = time.Second
		c1.ForceCloseOnTimeout = true
		c2.ForceCloseOnTimeout = true
		e.s.action = -1
		e.s.close = true
		e.s.position = 6
	case "fresh_hedge_price", "taker_amend", "taker_dead_short_same_event", "taker_dead_streak":
		c1.SoloMakerLeg = true
		c2.Mode = v2.ModeLimitA
	case "maker_terminal_partial", "maker_terminal_unfilled", "maker_filled_status_before_fill":
		c1.SoloMakerLeg = true
		c2.Mode = v2.ModeLimitA
	case "ratio_leg_b", "ratio_two_limits":
		c1.SoloMakerLeg = true
		c1.HedgeRatio = 3
		c2.Mode = v2.ModeLimitA
		c2.Ratio = 3
		e.s.mode = 3
		if name == "ratio_two_limits" {
			e.s.mode = 1
		}
	case "clock_domains", "pull_unwanted_minrest":
		c1.MinRest = time.Second
		c2.MinRest = time.Second
		if name == "clock_domains" {
			e.parityClock.now = e.base.Add(time.Hour)
		}
	case "stale_opt_out", "stale_enabled":
		c1.MaxStaleness = time.Second
		c2.BookMaxAge = time.Second
		if name == "stale_enabled" {
			c1.PullOnStaleBook = true
			c2.PullOnStaleBook = true
		}
	case "disable_repeg":
		c1.DisableRepeg = true
		c2.DisableRepeg = true
	case "closing_ladder":
		c1.SoloMakerLeg = true
		c2.Mode = v2.ModeLimitA
		c1.RejectRetryLotStep = 2
		c2.RejectRetryLotStep = 2
		c1.RejectRetryMinLots = 2
		c2.RejectRetryMinLots = 2
		e.s.action = -1
		e.s.close = true
		e.s.position = 6
		e.b.maxAcceptedLots = 2
	}
	if old {
		b := parityBrokerV1{e.b}
		e.old = v1.NewEngine(c1, b, b, parityStrategyV1{e.s})
		e.old.SetClock(e.parityClock)
		e.old.SetFillSink(paritySinkV1{e.b})
	} else {
		var err error
		e.new, err = v2.NewEngine(c2, v2.Setup{Broker: parityBrokerV2{e.b}, Strategy: parityStrategyV2{e.s}, Changes: parityUpdatesV2{e.b}, Clock: e.parityClock, Logger: parityLogger{}})
		if err != nil {
			t.Fatal(err)
		}
	}
	e.book("A", e.base, 100, 102)
	e.book("B", e.base, 200, 202)
	if name == "out_of_order_book" {
		e.book("A", e.base.Add(-time.Second), 102, 104)
	}
	e.signal(e.base)
	switch name {
	case "signal_lost":
		e.s.action = 0
		e.signal(e.base.Add(time.Second))
	case "execution_shape_changed":
		e.s.mode = 4
		e.signal(e.base.Add(time.Second))
	case "favorable_touch":
		e.parityClock.now = e.base.Add(time.Second)
		e.book("A", e.parityClock.now, 99, 102)
	case "partial_timeout", "keep_partial_timeout":
		e.fill(1)
		e.parityClock.now = e.base.Add(time.Second)
		e.tick(e.parityClock.now)
	case "force_close_timeout":
		e.parityClock.now = e.base.Add(time.Second)
		e.tick(e.parityClock.now)
	case "ratio_leg_b":
		e.fill(1)
	case "repeg_cancel_caught_partial", "cancelack_maker_price":
		e.b.catch = true
		e.parityClock.now = e.base.Add(time.Second)
		e.book("A", e.parityClock.now, 101, 102)
		e.tick(e.parityClock.now.Add(time.Second))
		if name == "cancelack_maker_price" {
			e.fillOrder(0, 1, 99)
		}
	case "clock_domains":
		e.book("A", e.base.Add(2*time.Second), 101, 102)
	case "stale_opt_out", "stale_enabled":
		e.parityClock.now = e.base.Add(2 * time.Second)
		e.tick(e.parityClock.now)
	case "disable_repeg":
		e.parityClock.now = e.base.Add(time.Second)
		e.book("A", e.parityClock.now, 101, 102)
	case "repeg_rejection_backoff":
		e.b.rejectOnce = true
		e.parityClock.now = e.base.Add(time.Second)
		e.book("A", e.parityClock.now, 101, 102)
		e.signal(e.parityClock.now.Add(time.Millisecond))
	case "pull_unwanted_minrest":
		e.s.action = 0
		e.pull(e.base.Add(500 * time.Millisecond))
		e.pull(e.base.Add(time.Second))
	case "cancel_partial":
		e.fill(1)
		e.cancel()
	case "counterparty_ahead":
		e.b.orders[1].Filled = 3
		e.fill(1)
	case "fresh_hedge_price":
		e.parityClock.now = e.base.Add(time.Millisecond)
		e.book("B", e.parityClock.now, 205, 207)
		e.fill(1)
	case "taker_amend":
		e.fill(6)
		e.fillOrder(1, 6, 199)
	case "maker_terminal_partial", "maker_terminal_unfilled", "maker_terminal_dual":
		if name == "maker_terminal_partial" {
			e.fill(1)
		}
		e.orderStatus(0, false)
		e.orderStatus(0, true)
		e.orderStatus(0, true)
	case "maker_filled_status_before_fill":
		maker := &e.b.orders[0]
		maker.Filled, maker.Done = maker.Lots, true
		e.orderStatus(0, false) // FILLED is terminal at the broker, but not dead in the stream.
		e.fill(maker.Lots)
	case "maker_dead_b_before_stream":
		e.b.orders[0].Filled = 1
		e.b.orders[1].Filled = 3
		e.orderStatus(1, true)
		e.orderStatus(1, true)
		e.fillOrder(0, 1, 99)
		e.fillOrder(1, 3, 203)
	case "taker_dead_short_same_event":
		e.fill(6)
		e.b.orders[1].Filled = 2
		e.orderStatus(1, true)
		e.orderStatus(1, true)
		e.parityClock.now = e.base.Add(time.Second)
		e.tick(e.parityClock.now)
		e.parityClock.now = e.base.Add(2 * time.Second)
		e.tick(e.parityClock.now)
	case "taker_dead_streak":
		e.fill(6)
		for order := 1; order <= 3; order++ {
			if order >= len(e.b.orders) {
				t.Fatalf("dead taker %d did not create its immediate replacement", order-1)
			}
			e.b.orders[order].Filled = 0
			e.orderStatus(order, true)
		}
		e.parityClock.now = e.base.Add(time.Second)
		e.tick(e.parityClock.now)
		e.parityClock.now = e.base.Add(2 * time.Second)
		e.tick(e.parityClock.now)
	}
	if len(e.errors) > 0 {
		t.Logf("v2 returned: %v", e.errors)
	}
	return e.trace
}

// TestV1V2OpeningParity replays identical public events and broker outcomes.
// Every event must leave the same observable trace; a later reconciliation must
// not hide a transient extra order, lost cancellation, or uncommitted position.
func TestV1V2OpeningParity(t *testing.T) {
	for _, name := range []string{"signal_lost", "execution_shape_changed", "favorable_touch", "partial_timeout", "force_close_timeout", "ratio_leg_b", "ratio_two_limits", "repeg_cancel_caught_partial", "clock_domains", "stale_opt_out", "disable_repeg", "repeg_rejection_backoff", "out_of_order_book", "keep_partial_timeout", "stale_enabled", "pull_unwanted_minrest", "cancel_partial", "counterparty_ahead", "closing_ladder", "fresh_hedge_price", "taker_amend", "cancelack_maker_price"} {
		t.Run(name, func(t *testing.T) {
			want := runParityCase(t, true, name)
			got := runParityCase(t, false, name)
			if len(want) != len(got) {
				t.Fatalf("event count: v1=%d v2=%d", len(want), len(got))
			}
			for event := range want {
				if !reflect.DeepEqual(want[event], got[event]) {
					oldJSON, _ := json.Marshal(want[event])
					newJSON, _ := json.Marshal(got[event])
					t.Fatalf("behavior differs after event %d\nv1: %s\nv2: %s", event+1, oldJSON, newJSON)
				}
			}
		})
	}
}
