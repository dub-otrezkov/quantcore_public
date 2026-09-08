package trade_test

import (
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/internal/trade"
)

func startDual(t *testing.T, target int) (*trade.Trade, time.Time) {
	t.Helper()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	m := &trade.Trade{}
	reqs, err := m.Start(
		execengine2.Plan{Action: 1, Lots: target}, execengine2.ModeTwoLimits,
		"A", "B", execengine2.Prices{Bid: 99, Ask: 100},
		execengine2.Prices{Bid: 199, Ask: 200}, target, 1, now, time.Minute, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 2 || reqs[0].Side != execengine2.SideBuy || reqs[1].Side != execengine2.SideSell {
		t.Fatalf("requests = %+v", reqs)
	}
	if err := m.Attach(execengine2.LegA, "a1", now); err != nil {
		t.Fatal(err)
	}
	if err := m.Attach(execengine2.LegB, "b1", now); err != nil {
		t.Fatal(err)
	}
	return m, now
}

func TestFirstFillClosesOtherOrder(t *testing.T) {
	t.Parallel()
	m, now := startDual(t, 2)
	result := m.AddFill("a1", 2)
	if !result.First || result.CloseOrderID != "b1" {
		t.Fatalf("first fill result = %+v", result)
	}
	m.CloseOrder("b1")
	req, ok, err := m.Hedge(execengine2.RoleHedge)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || req.Leg != execengine2.LegB || req.Lots != 2 || req.Kind != execengine2.OrderMarket {
		t.Fatalf("balance request = %+v, ok=%v", req, ok)
	}
	if err := m.Attach(execengine2.LegB, "h1", now); err == nil {
		// Вторая лимитная заявка остаётся b1. Хедж хранится отдельно.
		t.Fatal("clip accepted a second attached leg order")
	}
	// Успешный хедж учитывается по ID сделки и стороне пары.
	if !m.AddMarket(req.TradeID, req.Leg, 2) {
		t.Fatal("clip rejected its correlated hedge")
	}
	if !m.Full() {
		t.Fatal("balanced target clip was not full")
	}
	intent, ok := m.Finish(false)
	if !ok || intent.Lots != 2 {
		t.Fatalf("finalize = %+v, %v", intent, ok)
	}
}

func TestTwoFillsMakeHedge(t *testing.T) {
	t.Parallel()
	m, _ := startDual(t, 4)
	m.AddFill("a1", 1)
	m.AddFill("b1", 3)
	m.CloseOrder("b1")
	req, ok, err := m.Hedge(execengine2.RoleHedge)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || req.Leg != execengine2.LegA || req.Lots != 2 {
		t.Fatalf("balance request = %+v, ok=%v", req, ok)
	}
}

func TestBadRatioReturnsError(t *testing.T) {
	t.Parallel()
	now := time.Now()
	m := &trade.Trade{}
	reqs, err := m.Start(
		execengine2.Plan{Action: 1}, execengine2.ModeMarket, "A", "B",
		execengine2.Prices{Bid: 99, Ask: 100}, execengine2.Prices{Bid: 199, Ask: 200},
		1, 10, now, 0, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Attach(execengine2.LegA, "a", now); err != nil {
		t.Fatal(err)
	}
	if err := m.Attach(execengine2.LegB, "b", now); err != nil {
		t.Fatal(err)
	}
	m.AddFill("b", 7)
	if _, _, err := m.Hedge(execengine2.RoleHedge); err == nil {
		t.Fatal("unrepresentable ratio imbalance was accepted")
	}
	if reqs[1].Lots != 10 {
		t.Fatalf("leg B lots = %d, want 10", reqs[1].Lots)
	}
}

func TestPriceChangeWait(t *testing.T) {
	t.Parallel()
	m, now := startDual(t, 2)
	touch := execengine2.Prices{Bid: 100, Ask: 101}
	if _, ok := m.NewPrice(execengine2.LegA, touch, now.Add(time.Second), 2*time.Second, 2*time.Second); ok {
		t.Fatal("repeg ignored min rest")
	}
	candidate, ok := m.NewPrice(execengine2.LegA, touch, now.Add(3*time.Second), 2*time.Second, 2*time.Second)
	if !ok || candidate.Request.Price != 100 {
		t.Fatalf("candidate = %+v, ok=%v", candidate, ok)
	}
	m.CloseOrder("a1")
	if err := m.SetNewOrder(candidate, "a2", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.NewPrice(
		execengine2.LegA, execengine2.Prices{Bid: 101, Ask: 102},
		now.Add(4*time.Second), 2*time.Second, 0,
	); ok {
		t.Fatal("repeg ignored per-leg throttle")
	}
}

func TestBasePositionSurvivesPlacementAccounting(t *testing.T) {
	t.Parallel()
	var m trade.Trade
	now := time.Now()
	const initialPosition = -3
	reqs, err := m.Start(
		execengine2.Plan{Action: 1}, execengine2.ModeMarket, "A", "B",
		execengine2.Prices{Bid: 99, Ask: 100}, execengine2.Prices{Bid: 199, Ask: 200},
		2, 1, now, 0, initialPosition,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range reqs {
		if !m.AddMarket(req.TradeID, req.Leg, req.Lots) {
			t.Fatal("market placement was not accounted")
		}
	}
	if !m.FixMarket(reqs[1].TradeID, execengine2.LegB, -1) {
		t.Fatal("partial execution was not accounted")
	}
	info, ok := m.Info()
	if !ok || info.BasePosition != initialPosition || info.FilledA != 2 || info.FilledB != 1 {
		t.Fatalf("initial position changed during placement accounting: %+v", info)
	}
}
