// Package trade хранит одну текущую парную сделку целиком.
package trade

import (
	"errors"
	"fmt"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

type legData struct {
	req       model.OrderRequest
	orderID   string
	hasOrder  bool
	open      bool
	filled    int
	sentAt    time.Time
	lastPrice time.Time
}

type pair struct {
	id           uint64
	plan         model.Plan
	mode         model.Mode
	lotsA        int
	ratio        int
	legA         legData
	legB         legData
	first        model.Leg
	stopping     bool
	settlement   Settlement
	endAt        time.Time
	basePosition int
}

// Settlement survives deferred cancels: a retry must keep the original intent.
type Settlement uint8

const (
	Drop     Settlement = iota // equalize realized fills, without booking a lot
	Resolve                    // complete and book if cancellation catches any fill
	Complete                   // complete and book even an entirely unfilled close
)

// Info — копия текущей сделки для чтения.
type Info struct {
	ID            uint64
	Plan          model.Plan
	Mode          model.Mode
	LotsA         int
	Ratio         int
	First         model.Leg
	FilledA       int
	FilledB       int
	OrderA        string
	OrderB        string
	OpenA         bool
	OpenB         bool
	Stopping      bool
	Settlement    Settlement
	LastPlacement time.Time
	EndAt         time.Time
	BasePosition  int
}

// FillResult описывает итог исполнения лимитной заявки.
// CloseOrderID — заявка второй стороны, которую надо снять.
type FillResult struct {
	Known        bool
	Leg          model.Leg
	First        bool
	CloseOrderID string
}

// PriceOrder описывает безопасную замену цены заявки.
type PriceOrder struct {
	Leg        model.Leg
	OldOrderID string
	Request    model.OrderRequest
}

// Trade можно использовать с нулевым значением. Он хранит не больше одной сделки.
type Trade struct {
	nextID  uint64
	current *pair
}

// Start создаёт сделку до отправки заявок и возвращает заявки на отправку.
// Поэтому частично открытая сделка не теряется при ошибке брокера.
func (m *Trade) Start(
	plan model.Plan,
	mode model.Mode,
	legA string,
	legB string,
	pricesA model.Prices,
	pricesB model.Prices,
	lotsA int,
	ratio int,
	now time.Time,
	maxTime time.Duration,
	basePosition int,
) ([]model.OrderRequest, error) {
	if m.current != nil {
		return nil, errors.New("clip already working")
	}
	if plan.Action == 0 {
		return nil, errors.New("hold intent cannot start a clip")
	}
	if lotsA <= 0 || ratio <= 0 {
		return nil, errors.New("clip target and hedge ratio must be positive")
	}
	if !pricesA.Valid() || !pricesB.Valid() {
		return nil, errors.New("both valid touches are required")
	}
	if mode == model.ModeDefault {
		return nil, errors.New("clip mode must be resolved before Start")
	}
	if ratio > 1 && mode == model.ModeTwoLimits {
		return nil, errors.New("dual-passive clips require a 1:1 hedge ratio")
	}

	m.nextID++
	sign := 1
	if plan.Action < 0 {
		sign = -1
	}
	sideA := model.SideBuy
	if sign < 0 {
		sideA = model.SideSell
	}
	sideB := sideA.Other()
	c := &pair{
		id:           m.nextID,
		plan:         plan,
		mode:         mode,
		lotsA:        lotsA,
		ratio:        ratio,
		basePosition: basePosition,
	}
	if maxTime > 0 {
		c.endAt = now.Add(maxTime)
	}
	c.legA.req = model.OrderRequest{
		Symbol: legA, Side: sideA, Role: model.RoleTrade, Leg: model.LegA,
		Lots: lotsA, TradeID: c.id,
	}
	c.legB.req = model.OrderRequest{
		Symbol: legB, Side: sideB, Role: model.RoleTrade, Leg: model.LegB,
		Lots: lotsA * ratio, TradeID: c.id,
	}

	switch mode {
	case model.ModeTwoLimits:
		setLimit(&c.legA, pricesA)
		setLimit(&c.legB, pricesB)
	case model.ModeLimitA:
		setLimit(&c.legA, pricesA)
		setMarket(&c.legB, pricesB)
	case model.ModeLimitB:
		setMarket(&c.legA, pricesA)
		setLimit(&c.legB, pricesB)
	case model.ModeMarket:
		setMarket(&c.legA, pricesA)
		setMarket(&c.legB, pricesB)
	default:
		return nil, fmt.Errorf("unsupported execution mode %d", mode)
	}
	m.current = c

	orders := make([]model.OrderRequest, 0, 2)
	if mode == model.ModeTwoLimits || mode == model.ModeLimitA {
		orders = append(orders, c.legA.req)
	}
	if mode == model.ModeTwoLimits || mode == model.ModeLimitB {
		orders = append(orders, c.legB.req)
	}
	if mode == model.ModeMarket {
		orders = append(orders, c.legA.req, c.legB.req)
	}
	return orders, nil
}

func setLimit(leg *legData, touch model.Prices) {
	leg.req.Kind = model.OrderLimit
	leg.req.Role = model.RoleTrade
	leg.req.Price = touch.Price(leg.req.Side)
}

func setMarket(leg *legData, touch model.Prices) {
	leg.req.Kind = model.OrderMarket
	leg.req.Role = model.RoleHedge
	// Price для рыночной заявки нужна только для примерного учёта.
	// Broker не должен использовать её как лимитную цену.
	leg.req.Price = touch.MarketPrice(leg.req.Side)
}

// Attach сохраняет ID заявки, который вернул брокер.
func (m *Trade) Attach(leg model.Leg, orderID string, at time.Time) error {
	c := m.current
	if c == nil {
		return errors.New("no working clip")
	}
	ls := getLeg(c, leg)
	if ls == nil {
		return errors.New("invalid leg")
	}
	if ls.hasOrder {
		return fmt.Errorf("leg %d already has order %q", leg, ls.orderID)
	}
	if orderID == "" {
		return errors.New("empty order id")
	}
	ls.orderID = orderID
	ls.hasOrder = true
	ls.open = ls.req.Kind == model.OrderLimit
	ls.sentAt = at
	return nil
}

// AddFill добавляет исполненный объём к заявке сделки.
func (m *Trade) AddFill(orderID string, lots int) FillResult {
	c := m.current
	if c == nil || lots <= 0 {
		return FillResult{}
	}
	leg, ls := findOrder(c, orderID)
	if ls == nil {
		return FillResult{}
	}
	ls.filled += lots
	result := FillResult{Known: true, Leg: leg}
	if ls.req.Kind != model.OrderLimit {
		return result
	}
	if c.first == model.LegNone {
		c.first = leg
		result.First = true
		other := getLeg(c, leg.Other())
		if other != nil && other.open {
			result.CloseOrderID = other.orderID
		}
	}
	return result
}

// AddMarket сразу учитывает рыночную заявку этой сделки.
// tradeID не даёт старому ответу изменить новую сделку.
func (m *Trade) AddMarket(tradeID uint64, leg model.Leg, lots int) bool {
	c := m.current
	if c == nil || c.id != tradeID || lots <= 0 {
		return false
	}
	ls := getLeg(c, leg)
	if ls == nil {
		return false
	}
	ls.filled += lots
	return true
}

// FixMarket правит ранее учтённый объём рыночной заявки.
func (m *Trade) FixMarket(tradeID uint64, leg model.Leg, change int) bool {
	c := m.current
	if c == nil || c.id != tradeID || change == 0 {
		return false
	}
	ls := getLeg(c, leg)
	if ls == nil || ls.filled+change < 0 {
		return false
	}
	ls.filled += change
	return true
}

// CloseOrder отмечает, что лимитная заявка больше не может исполниться.
func (m *Trade) CloseOrder(orderID string) bool {
	c := m.current
	if c == nil {
		return false
	}
	_, ls := findOrder(c, orderID)
	if ls == nil {
		return false
	}
	ls.open = false
	return true
}

// Hedge возвращает рыночную заявку, нужную для выравнивания пары.
func (m *Trade) Hedge(role model.OrderRole) (model.OrderRequest, bool, error) {
	c := m.current
	if c == nil {
		return model.OrderRequest{}, false, nil
	}
	settling := c.stopping && role != model.RoleLateFill
	if settling && (c.legA.open || c.legB.open) {
		return model.OrderRequest{}, false, nil
	}
	want := c.legA.filled
	if c.legB.filled > want*c.ratio {
		if c.legB.filled%c.ratio != 0 {
			return model.OrderRequest{}, false, fmt.Errorf("leg B execution %d is not divisible by hedge ratio %d", c.legB.filled, c.ratio)
		}
		want = c.legB.filled / c.ratio
	}
	if settling && (c.settlement == Complete || c.settlement == Resolve && want > 0) {
		want = max(want, c.lotsA)
	}
	for _, leg := range []*legData{&c.legA, &c.legB} {
		target := want
		if leg == &c.legB {
			target *= c.ratio
		}
		if leg.filled < target {
			req := leg.req
			req.Kind, req.Role, req.Lots = model.OrderMarket, role, target-leg.filled
			return req, true, nil
		}
	}
	return model.OrderRequest{}, false, nil
}

// IsPaired проверяет нужное отношение объёмов пары.
func (m *Trade) IsPaired() bool {
	c := m.current
	return c != nil && c.legB.filled == c.legA.filled*c.ratio
}

// Full проверяет, набран ли весь нужный объём.
func (m *Trade) Full() bool {
	c := m.current
	return c != nil && c.legA.filled >= c.lotsA && m.IsPaired()
}

// Stop начинает остановку и возвращает открытые лимитные заявки.
func (m *Trade) Stop(policy Settlement) []string {
	c := m.current
	if c == nil {
		return nil
	}
	if !c.stopping {
		c.stopping = true
		c.settlement = policy
	}
	ids := make([]string, 0, 2)
	if c.legA.open {
		ids = append(ids, c.legA.orderID)
	}
	if c.legB.open {
		ids = append(ids, c.legB.orderID)
	}
	return ids
}

// Finish завершает ровную полную или частичную сделку и чистит состояние.
func (m *Trade) Finish(allowPartial bool) (model.Plan, bool) {
	c := m.current
	if c == nil || !m.IsPaired() || c.legA.filled == 0 {
		return model.Plan{}, false
	}
	if c.stopping && (c.legA.open || c.legB.open || c.settlement == Drop) {
		return model.Plan{}, false
	}
	if !allowPartial && c.legA.filled < c.lotsA {
		return model.Plan{}, false
	}
	plan := c.plan
	plan.Lots = c.legA.filled
	m.current = nil
	return plan, true
}

// FinishMarket books the worked intent, including explicitly queued debts.
// The caller must have a definite outcome for both initial market placements.
func (m *Trade) FinishMarket() (model.Plan, bool) {
	c := m.current
	if c == nil || c.mode != model.ModeMarket {
		return model.Plan{}, false
	}
	plan := c.plan
	plan.Lots = c.lotsA
	m.current = nil
	return plan, true
}

// DropSettled removes an explicitly abandoned pair only after every maker retires.
func (m *Trade) DropSettled() bool {
	c := m.current
	if c == nil || !c.stopping || c.settlement != Drop || c.legA.open || c.legB.open || !m.IsPaired() {
		return false
	}
	m.current = nil
	return true
}

// DropEmpty убирает пустую сделку без заявок и исполнений.
func (m *Trade) DropEmpty() bool {
	c := m.current
	if c == nil || c.legA.filled != 0 || c.legB.filled != 0 || c.legA.open || c.legB.open {
		return false
	}
	m.current = nil
	return true
}

// TooLate проверяет общий таймаут сделки.
func (m *Trade) TooLate(now time.Time) bool {
	c := m.current
	return c != nil && !c.endAt.IsZero() && !now.Before(c.endAt)
}

// NewPrice готовит безопасную замену цены открытой заявки.
func (m *Trade) NewPrice(
	leg model.Leg,
	touch model.Prices,
	now time.Time,
	wait time.Duration,
	minRest time.Duration,
) (PriceOrder, bool) {
	c := m.current
	if c == nil || c.first != model.LegNone || c.stopping {
		return PriceOrder{}, false
	}
	ls := getLeg(c, leg)
	if ls == nil || !ls.open || ls.req.Kind != model.OrderLimit {
		return PriceOrder{}, false
	}
	price := touch.Price(ls.req.Side)
	behind := ls.req.Side == model.SideBuy && price > ls.req.Price ||
		ls.req.Side == model.SideSell && price < ls.req.Price
	if price <= 0 || !behind || now.Sub(ls.sentAt) < minRest ||
		(!ls.lastPrice.IsZero() && now.Sub(ls.lastPrice) < wait) {
		return PriceOrder{}, false
	}
	req := ls.req
	req.Price = price
	return PriceOrder{Leg: leg, OldOrderID: ls.orderID, Request: req}, true
}

// SetNewOrder сохраняет новую заявку после успешной замены.
func (m *Trade) SetNewOrder(next PriceOrder, orderID string, at time.Time) error {
	c := m.current
	if c == nil {
		return errors.New("no working clip")
	}
	ls := getLeg(c, next.Leg)
	if ls == nil || ls.orderID != next.OldOrderID || ls.open {
		return errors.New("repeg candidate is no longer current")
	}
	if orderID == "" {
		return errors.New("empty replacement order id")
	}
	ls.req = next.Request
	ls.orderID = orderID
	ls.hasOrder = true
	ls.open = true
	ls.sentAt = at
	ls.lastPrice = at
	return nil
}

// Info возвращает копию текущей сделки.
func (m *Trade) Info() (Info, bool) {
	c := m.current
	if c == nil {
		return Info{}, false
	}
	return Info{
		ID: c.id, Plan: c.plan, Mode: c.mode, LotsA: c.lotsA, Ratio: c.ratio,
		First: c.first, FilledA: c.legA.filled, FilledB: c.legB.filled,
		OrderA: c.legA.orderID, OrderB: c.legB.orderID,
		OpenA: c.legA.open, OpenB: c.legB.open,
		Stopping: c.stopping, Settlement: c.settlement, LastPlacement: maxTime(c.legA.sentAt, c.legB.sentAt), EndAt: c.endAt, BasePosition: c.basePosition,
	}, true
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func getLeg(c *pair, leg model.Leg) *legData {
	if leg == model.LegA {
		return &c.legA
	}
	if leg == model.LegB {
		return &c.legB
	}
	return nil
}

func findOrder(c *pair, orderID string) (model.Leg, *legData) {
	if c.legA.hasOrder && c.legA.orderID == orderID {
		return model.LegA, &c.legA
	}
	if c.legB.hasOrder && c.legB.orderID == orderID {
		return model.LegB, &c.legB
	}
	return model.LegNone, nil
}
