package execengine2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"QuantCore/strategies/execengine2/internal/hedges"
	"QuantCore/strategies/execengine2/internal/model"
	"QuantCore/strategies/execengine2/internal/orders"
	"QuantCore/strategies/execengine2/internal/quotes"
	"QuantCore/strategies/execengine2/internal/run"
	"QuantCore/strategies/execengine2/internal/trade"
)

// Setup содержит всё, что нужно движку для запуска.
type Setup struct {
	Broker   Broker
	Limit    SendLimit
	Clock    Clock
	Strategy Strategy
	Changes  Updates
	Logger   Logger
}

// Engine принимает события и передаёт работу нужной части.
// Каждое изменяемое состояние хранится только в одной части.
//
// Все методы Engine надо вызывать из одной горутины цикла событий.
// ModeMarket отправляет две заявки параллельно и ждёт оба ответа; состояние
// меняется только в вызывающей горутине. Постоянных горутин и мьютексов нет.
type Engine struct {
	config   Config
	broker   Broker
	limit    SendLimit
	clock    Clock
	strategy Strategy
	updates  Updates
	logger   Logger

	quotes *quotes.Book
	trade  *trade.Trade
	orders *orders.List
	hedges *hedges.List
	state  *run.State

	reusedIDBarrier bool
	now             time.Time // monotonic event time; processing time belongs to the send limit
}

// Info — короткий снимок состояния движка.
type Info struct {
	Code          RunState
	Reason        string
	HasTrade      bool
	Position      int
	LimitLeft     int64
	MarketOrders  int
	UnknownOrders int
	Hedges        int
	OrdersToClose int
}

// NewEngine проверяет настройки и собирает движок.
func NewEngine(config Config, deps Setup) (*Engine, error) {
	config = config.normalized()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid execengine2 config: %w", err)
	}
	if deps.Broker == nil {
		return nil, errors.New("execengine2 broker is required")
	}
	if deps.Strategy == nil {
		return nil, errors.New("execengine2 decider is required")
	}
	if deps.Limit == nil {
		deps.Limit = noLimit{}
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	if deps.Logger == nil {
		deps.Logger = newLogger(config.LogTag)
	}
	return &Engine{
		config: config, broker: deps.Broker, limit: deps.Limit, clock: deps.Clock,
		strategy: deps.Strategy, updates: deps.Changes, logger: deps.Logger,
		quotes: quotes.New(config.LegA, config.LegB), trade: &trade.Trade{},
		orders: &orders.List{}, hedges: &hedges.List{},
		state: &run.State{},
	}, nil
}

// OnBook принимает новые цены и при необходимости меняет лимитную заявку.
func (e *Engine) OnBook(
	ctx context.Context,
	symbol string,
	at time.Time,
	bestBid float64,
	bestAsk float64,
) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.advanceNow(at)
	if !e.quotes.Update(symbol, at, bestBid, bestAsk) {
		return nil
	}
	clip, active := e.trade.Info()
	if !active || clip.Stopping {
		return nil
	}
	staleA, staleB := e.quotes.TooOld(at, e.config.BookMaxAge)
	if e.config.PullOnStaleBook && (staleA || staleB) {
		return e.resolveTrade(ctx, "invalid or stale order book")
	}
	if e.config.DisableRepeg || e.state.Info().Code == run.Stopped {
		return nil
	}
	leg := model.LegA
	if symbol == e.config.LegB {
		leg = model.LegB
	}
	next, ok := e.trade.NewPrice(
		leg, e.quotes.Prices(leg), at, e.config.PriceWait, e.config.MinRest,
	)
	if !ok {
		return nil
	}
	gate, checkOnly := e.limit.(Admitter)
	allowed := false
	if checkOnly {
		allowed = gate.Allow(1)
	} else {
		allowed = e.limit.Take(1, model.LimitNormal)
	}
	if !allowed {
		e.logger.Warnf("repeg of %s denied by placement budget", symbol)
		return nil
	}

	change, err := e.closeOrder(ctx, next.OldOrderID)
	if err != nil {
		return fmt.Errorf("retiring %s for repeg: %w", next.OldOrderID, err)
	}
	if change.Lots != 0 {
		e.trade.AddFill(change.Order.ID, change.Lots)
		e.trade.Stop(trade.Complete)
		return e.stopTrade(ctx, "repeg cancellation caught a fill", true)
	}

	orderID, err := e.broker.Place(ctx, next.Request)
	if checkOnly {
		e.limit.Take(1, model.LimitMust)
	}
	if err != nil {
		e.placeFailed(err, "repeg placement")
		e.quotes.BlockOpen(at.Add(e.config.RetryWait))
		abortErr := e.stopTrade(ctx, "repeg placement failed", !OrderMayExist(err))
		return errors.Join(fmt.Errorf("placing repeg: %w", err), abortErr)
	}
	if err := e.addOrder(ctx, orderID, next.Request); err != nil {
		return err
	}
	if err := e.trade.SetNewOrder(next, orderID, e.now); err != nil {
		_ = e.halt(ctx, "replacement order could not be attached to its clip: "+err.Error())
		return err
	}
	e.logger.Infof("repegged %s from %s to %s at %.6f", symbol, next.OldOrderID, orderID, next.Request.Price)
	return nil
}

// OnSignal читает новый сигнал стратегии и начинает сделку, если это безопасно.
func (e *Engine) OnSignal(ctx context.Context, state Signal) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.advanceNow(state.Time)
	if e.state.Info().Code != run.Stopped {
		e.hedges.ConfirmFills()
	}
	if !e.state.CanOpen() {
		return nil
	}
	if clip, active := e.trade.Info(); active {
		plan := e.strategy.Peek(state)
		if plan.Action != clip.Plan.Action || e.executionMode(plan) != clip.Mode {
			return e.resolveTrade(ctx, "strategy intent changed")
		}
		return nil
	}
	if e.hedges.HasWork() ||
		!e.quotes.Ready(state.Time, e.config.BookMaxAge) {
		return nil
	}
	plan := e.strategy.Peek(state)
	if plan.Action == 0 {
		return nil
	}
	lots := plan.Lots
	if lots == 0 {
		lots = e.config.Lots
	}
	if lots <= 0 {
		return errors.New("decider returned a non-positive clip size")
	}
	mode := e.executionMode(plan)
	count := orderCount(mode)
	if count == 0 {
		return fmt.Errorf("unsupported execution mode %d", mode)
	}
	return e.openLimitsOrMarket(ctx, plan, mode, lots, count, state.Time)
}

func (e *Engine) openLimitsOrMarket(ctx context.Context, plan Plan, mode Mode, lots, count int, at time.Time) error {
	gate, checkOnly := e.limit.(Admitter)
	ops := int64(count)
	if checkOnly {
		// v1 requires two slots even for a solo maker: headroom for its future
		// taker hedge, not the clip's order count. Allow reserves nothing; each
		// actual RPC is charged below. Reservation-only limits take count slots.
		ops = 2
	}
opening:
	for {
		allowed := false
		if checkOnly {
			allowed = gate.Allow(ops)
		} else {
			allowed = e.limit.Take(ops, model.LimitNormal)
		}
		if !allowed {
			delay := e.config.RetryWait
			if quota, ok := e.limit.(RetryDelayer); ok {
				if wait := quota.RetryAfter(ops); wait > 0 {
					delay = wait
				}
			}
			e.quotes.BlockOpen(at.Add(delay))
			e.logger.Warnf("clip open denied by placement budget; remaining=%d", e.limit.Remaining())
			return nil
		}

		requests, err := e.trade.Start(
			plan, mode, e.config.LegA, e.config.LegB,
			e.quotes.Prices(model.LegA), e.quotes.Prices(model.LegB),
			lots, e.config.Ratio, at, e.config.TradeTimeout, e.strategy.Position(),
		)
		if err != nil {
			return err
		}
		if mode == model.ModeMarket {
			return e.openMarket(ctx, requests, at, checkOnly)
		}
		for i, req := range requests {
			orderID, placeErr := e.broker.Place(ctx, req)
			if checkOnly {
				e.limit.Take(1, model.LimitMust)
			}
			if placeErr != nil {
				e.placeFailed(placeErr, "opening placement")
				e.quotes.BlockOpen(at.Add(e.config.RetryWait))
				abortErr := e.stopTrade(ctx, "opening placement failed", !OrderMayExist(placeErr))
				next := lots - e.config.RejectRetryLotStep
				if i == 0 && plan.IsClose && !OrderMayExist(placeErr) && abortErr == nil &&
					e.config.RejectRetryLotStep > 0 && next >= e.config.RejectRetryMinLots {
					lots = next
					continue opening
				}
				return errors.Join(fmt.Errorf("placing opening order on %s: %w", req.Symbol, placeErr), abortErr)
			}
			if err := e.trade.Attach(req.Leg, orderID, e.now); err != nil {
				_ = e.halt(ctx, "placed order could not be attached to its clip: "+err.Error())
				return err
			}
			if err := e.addOrder(ctx, orderID, req); err != nil {
				return err
			}
		}
		return nil
	}
}

// OnFill принимает одно исполнение нашей заявки.
func (e *Engine) OnFill(ctx context.Context, fill Fill) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.advanceNow(fill.At)
	change := e.orders.AddFill(fill)
	if !change.Known || change.Again {
		return nil
	}
	e.sendChange(change, "fill")
	if change.Conflict {
		e.logger.Criticalf("order %s executed beyond its terminal acknowledgement", fill.OrderID)
		e.state.NeedCheck("execution contradicted a terminal order acknowledgement")
	}
	if change.Extra > 0 {
		e.logger.Criticalf("order %s reported %d impossible lots beyond placed size; dropped", fill.OrderID, change.Extra)
	}
	if change.Order.Request.Kind == model.OrderMarket {
		e.hedges.SeeFill(fill.OrderID, change.Order.Filled)
		if change.Lots != 0 {
			e.trade.FixMarket(
				change.Order.Request.TradeID,
				change.Order.Request.Leg,
				change.Lots,
			)
		}
		return nil
	}
	return e.useLimitFill(ctx, change, true)
}

// OnOrderStatus принимает состояние заявки напрямую от брокера.
// For v1-compatible order streams, Done is the old dead flag (Finam IsDeadStatus:
// cancellation/rejection/expiry); Filled is the broker's cumulative executed
// count, including fills not yet delivered by OnFill. FILLED/EXECUTED events use
// Done=false and are handled by OnFill, as in v1. Do not replace this stream
// filter with Broker.Status's broader terminal flag. A newly dead maker pulls a
// working clip without Commit; an existing settlement keeps its chosen policy.
// The confirmed terminal count avoids another Cancel for that ID.
func (e *Engine) OnOrderStatus(ctx context.Context, orderID string, status OrderStatus) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	snap, own := e.orders.Info(orderID)
	if !own || !status.Done {
		return nil
	}
	if snap.Request.Kind == model.OrderMarket {
		return e.useMarketStatus(ctx, orderID, status)
	}
	change := e.orders.Close(orderID, status.Filled)
	if clip, active := e.trade.Info(); active && !snap.Done &&
		(clip.OpenA && clip.OrderA == orderID || clip.OpenB && clip.OrderB == orderID) {
		// Retire in the same A-then-B accounting order as cancellation. The
		// known terminal result replaces that leg's RPC, not its ledger position.
		return e.stopTrade(ctx, "resting maker became terminal", true, change)
	}
	e.trade.CloseOrder(orderID)
	e.sendChange(change, "maker terminal status")
	if change.Lots != 0 {
		e.trade.AddFill(orderID, change.Lots)
	}
	return nil
}

// OnTick проверяет таймауты, рыночные заявки и отложенную работу.
func (e *Engine) OnTick(ctx context.Context, now time.Time) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.advanceNow(now)
	if e.state.Info().Code == run.Stopped {
		if e.state.FixDue(now) {
			return e.fixWork(ctx, now)
		}
		return nil
	}
	result := e.resumeUnknown(ctx, now)
	if clip, active := e.trade.Info(); active && !clip.Stopping {
		staleA, staleB := e.quotes.TooOld(now, e.config.BookMaxAge)
		if e.config.PullOnStaleBook && (staleA || staleB) || e.trade.TooLate(now) {
			result = errors.Join(result, e.resolveTrade(ctx, "clip timeout or stale book"))
		}
	}
	result = errors.Join(result, e.checkMarketOrders(ctx, now))
	if e.state.FixDue(now) {
		result = errors.Join(result, e.fixWork(ctx, now))
	}
	if clip, ok := e.trade.Info(); ok && e.hedges.UnknownCount() == 0 &&
		len(e.hedges.All()) == 0 && len(e.orders.OrdersToClose()) == 0 &&
		e.state.Info().Code != run.Stopped {
		if clip.Stopping {
			result = errors.Join(result, e.stopTrade(ctx, "finishing recovered clip", true))
		} else {
			// Maker fills may have accumulated while an earlier hedge was unknown.
			result = errors.Join(result, e.hedgeTrade(ctx, model.RoleHedge))
			if e.trade.Full() {
				result = errors.Join(result, e.completeTrade(ctx))
			}
		}
	}
	return result
}

// StopTrade безопасно останавливает текущую сделку.
func (e *Engine) StopTrade(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	return e.stopTrade(ctx, "cancel requested", true)
}

// Stop запрещает новые сделки и снимает открытые заявки.
// После kill-switch движок больше не отправляет новые заявки, включая хеджи.
func (e *Engine) Stop(ctx context.Context, reason string) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	return e.halt(ctx, reason)
}

// PullIfUnwanted is the ledger-based strategy's cancel-only signal path.
func (e *Engine) PullIfUnwanted(ctx context.Context, state Signal) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.advanceNow(state.Time)
	clip, active := e.trade.Info()
	if !active || state.Time.Sub(clip.LastPlacement) < e.config.MinRest ||
		e.strategy.Peek(state).Action == clip.Plan.Action {
		return nil
	}
	return e.StopTrade(ctx)
}

func (e *Engine) advanceNow(at time.Time) {
	if at.After(e.now) {
		e.now = at
	}
}

func (e *Engine) executionMode(plan Plan) Mode {
	mode := plan.Mode
	if mode == ModeDefault {
		mode = e.config.Mode
	}
	if e.config.Ratio > 1 && (mode == ModeTwoLimits || mode == ModeLimitB) {
		e.logger.Criticalf("mode %d cannot rest leg B at ratio %d; using leg A maker", mode, e.config.Ratio)
		return ModeLimitA
	}
	return mode
}

// CheckPositions сравнивает позицию брокера с позицией стратегии.
func (e *Engine) CheckPositions(actualA, actualB int) bool {
	e.confirmUnknownPositions(actualA, actualB)
	expectedA := e.strategy.Position()
	expectedB := -expectedA * e.config.Ratio
	hasWork := e.HasTrade() || len(e.orders.OrdersToClose()) > 0 || e.hedges.HasWork()
	if e.reusedIDBarrier && e.state.Info().Code == run.CheckNeeded && !hasWork &&
		actualA == expectedA && actualB == expectedB {
		e.reusedIDBarrier = false
		e.logger.Warnf("reused order id passed the first clean position check; waiting for one stable repeat")
		return false
	}
	ok := e.state.CheckPositions(actualA, actualB, expectedA, expectedB, hasWork)
	if !ok {
		e.logger.Warnf(
			"reconcile mismatch: actual=(%d,%d) expected=(%d,%d) obligations=%v",
			actualA, actualB, expectedA, expectedB, hasWork,
		)
	}
	return ok
}

// HasTrade сообщает, есть ли сейчас незаконченная сделка.
func (e *Engine) HasTrade() bool {
	_, ok := e.trade.Info()
	return ok
}

// HasOrder проверяет, принадлежит ли заявка этому движку.
func (e *Engine) HasOrder(orderID string) bool { return e.orders.HasOrder(orderID) }

// Position возвращает позицию стратегии.
func (e *Engine) Position() int { return e.strategy.Position() }

// Code возвращает текущее состояние движка.
func (e *Engine) Code() RunState { return e.state.Info().Code }

// Info возвращает снимок состояния движка.
func (e *Engine) Info() Info {
	state := e.state.Info()
	return Info{
		Code: state.Code, Reason: state.Reason, HasTrade: e.HasTrade(),
		Position: e.Position(), LimitLeft: e.limit.Remaining(),
		MarketOrders: e.hedges.MarketCount(), Hedges: len(e.hedges.All()),
		UnknownOrders: e.hedges.UnknownCount(),
		OrdersToClose: len(e.orders.OrdersToClose()),
	}
}

func orderCount(mode model.Mode) int {
	switch mode {
	case model.ModeTwoLimits, model.ModeMarket:
		return 2
	case model.ModeLimitA, model.ModeLimitB:
		return 1
	default:
		return 0
	}
}

func (e *Engine) addOrder(ctx context.Context, orderID string, req model.OrderRequest) error {
	countNow := req.Kind == model.OrderMarket
	change, err := e.orders.Add(orderID, req, e.clock.Now(), req.Price, countNow)
	if err != nil {
		if countNow {
			e.acceptUntrackedMarket(req, orderID, "broker returned an unusable taker order id: "+err.Error())
			if orderID != "" {
				e.reusedIDBarrier = true
			}
			return nil
		}
		_ = e.halt(ctx, "broker returned an unusable order id: "+err.Error())
		return err
	}
	e.sendChange(change, "taker placement")
	if !countNow {
		return nil
	}
	if err := e.hedges.AddMarket(orderID, req, e.clock.Now()); err != nil {
		_ = e.halt(ctx, "taker could not be tracked: "+err.Error())
		return err
	}
	e.trade.AddMarket(req.TradeID, req.Leg, req.Lots)
	return nil
}

func (e *Engine) acceptUntrackedMarket(req model.OrderRequest, orderID, reason string) {
	e.sendChange(orders.Change{
		Known: true,
		Lots:  req.Lots,
		Order: orders.Info{
			ID:         orderID,
			Request:    req,
			GuessPrice: req.Price,
		},
	}, "unverified taker placement")
	e.trade.AddMarket(req.TradeID, req.Leg, req.Lots)
	e.state.NeedCheck(reason)
	e.logger.Criticalf("%s", reason)
}

func (e *Engine) sendChange(change orders.Change, reason string) {
	if e.updates == nil || !change.Known {
		return
	}
	price := change.FillPrice
	if price == 0 {
		price = change.Order.GuessPrice
	}
	if change.Lots != 0 {
		e.updates.Apply(model.PositionChange{
			OrderID: change.Order.ID,
			Symbol:  change.Order.Request.Symbol,
			Lots:    change.Lots * change.Order.Request.Side.Sign(),
			Price:   price,
			Reason:  reason,
		})
	}
	if change.PriceLots > 0 && change.FillPrice != 0 &&
		change.FillPrice != change.Order.GuessPrice {
		e.updates.Amend(model.PriceChange{
			OrderID: change.Order.ID,
			Symbol:  change.Order.Request.Symbol,
			Lots:    change.PriceLots,
			From:    change.Order.GuessPrice,
			To:      change.FillPrice,
		})
	}
}

func (e *Engine) useLimitFill(
	ctx context.Context,
	change orders.Change,
	closeOther bool,
) error {
	if change.Lots <= 0 {
		return nil
	}
	transition := e.trade.AddFill(change.Order.ID, change.Lots)
	if e.state.Info().Code == run.Stopped {
		return nil
	}
	if !transition.Known {
		return e.fixLateFill(ctx, change.Order.Request, change.Lots)
	}
	if clip, _ := e.trade.Info(); clip.Stopping {
		if !change.Order.Done && e.hedges.UnknownCount() == 0 && len(e.hedges.All()) == 0 {
			// A newly proved stream execution is an independent obligation even
			// while cancel RPCs are unavailable. Hedge only a new lead on its leg;
			// catching up to an already known counterpart needs no extra order.
			req, ok, err := e.trade.Hedge(model.RoleLateFill)
			if err != nil {
				return errors.Join(err, e.halt(ctx, err.Error()))
			}
			if ok && req.Leg != change.Order.Request.Leg {
				return e.placeHedge(ctx, req)
			}
		}
		return nil // retire every maker before calculating settlement from acks
	}
	if closeOther && transition.First && transition.CloseOrderID != "" {
		other, err := e.closeOrder(ctx, transition.CloseOrderID)
		if err != nil {
			clip, _ := e.trade.Info()
			e.trade.Stop(e.settlement(clip.Plan))
			return err
		}
		if other.Lots != 0 {
			e.trade.AddFill(other.Order.ID, other.Lots)
			clip, _ := e.trade.Info()
			ahead := transition.Leg == model.LegA && clip.FilledB > clip.FilledA*clip.Ratio ||
				transition.Leg == model.LegB && clip.FilledA*clip.Ratio > clip.FilledB
			if ahead {
				return e.resolveTrade(ctx, "counterpart executed ahead of first maker")
			}
		}
	}
	if err := e.hedgeTrade(ctx, model.RoleHedge); err != nil {
		return err
	}
	if e.trade.Full() {
		return e.completeTrade(ctx)
	}
	return nil
}

func (e *Engine) fixLateFill(ctx context.Context, maker model.OrderRequest, lots int) error {
	req := model.OrderRequest{
		Kind: model.OrderMarket, Role: model.RoleLateFill,
		Side: maker.Side.Other(), TradeID: maker.TradeID,
	}
	switch maker.Leg {
	case model.LegA:
		req.Leg = model.LegB
		req.Symbol = e.config.LegB
		req.Lots = lots * e.config.Ratio
		req.Price = e.quotes.Prices(model.LegB).MarketPrice(req.Side)
	case model.LegB:
		if lots%e.config.Ratio != 0 {
			reason := fmt.Sprintf("stray leg B fill %d is not divisible by hedge ratio %d", lots, e.config.Ratio)
			_ = e.halt(ctx, reason)
			return errors.New(reason)
		}
		req.Leg = model.LegA
		req.Symbol = e.config.LegA
		req.Lots = lots / e.config.Ratio
		req.Price = e.quotes.Prices(model.LegA).MarketPrice(req.Side)
	default:
		return errors.New("stray maker order has no pair leg")
	}
	e.logger.Criticalf("stray maker fill on %s x%d; placing mandatory hedge", maker.Symbol, lots)
	return e.placeHedge(ctx, req)
}

func (e *Engine) hedgeTrade(ctx context.Context, role model.OrderRole) error {
	for range 2 {
		if e.hedges.UnknownCount() > 0 || len(e.hedges.All()) > 0 {
			return nil // pending work must be resolved before sizing another hedge
		}
		req, ok, err := e.trade.Hedge(role)
		if err != nil {
			_ = e.halt(ctx, err.Error())
			return err
		}
		if !ok {
			return nil
		}
		if err := e.placeHedge(ctx, req); err != nil {
			return err
		}
	}
	if !e.trade.IsPaired() {
		return errors.New("clip remained imbalanced after two hedge effects")
	}
	return nil
}

func (e *Engine) placeHedge(ctx context.Context, req model.OrderRequest) error {
	return e.placeHedgeAttempts(ctx, req, e.config.HedgeTries, true)
}

func (e *Engine) placeHedgeAttempts(ctx context.Context, req model.OrderRequest, tries int, canShrink bool) error {
	if e.state.Info().Code == run.Stopped {
		return errors.New("engine halted")
	}
	remaining := req.Lots
	_, chargeAfter := e.limit.(Admitter)
	size := remaining
	shrinking := canShrink && e.config.RejectRetryLotStep > 0 && size >= e.config.RejectRetryMinLots
	var lastErr error
	// The ladder terminates: each success pays at least one owed lot, and each
	// rejection reduces size until one lot is rejected. HedgeTries then bounds
	// the ordinary full-remainder attempts, just as when the ladder is disabled.
	for shrinking || tries > 0 {
		if !shrinking {
			size = remaining
			tries--
		}
		if !chargeAfter && !e.limit.Take(1, model.LimitMust) {
			reason := "placement budget rejected a mandatory hedge"
			req.Lots = remaining
			e.hedges.Add(req, errors.New(reason))
			_ = e.halt(ctx, reason)
			return errors.New(reason)
		}
		part := req
		part.Lots = size
		part.Price = e.quotes.Prices(part.Leg).MarketPrice(part.Side)
		orderID, err := e.broker.Place(ctx, part)
		if chargeAfter {
			e.limit.Take(1, model.LimitMust)
		}
		if err == nil {
			remaining -= size
			if err := e.addOrder(ctx, orderID, part); err != nil {
				if remaining > 0 {
					req.Lots = remaining
					e.hedges.Add(req, err)
				}
				return err
			}
			if remaining == 0 {
				return nil
			}
			if e.state.Info().Code == run.CheckNeeded || e.state.Info().Code == run.Stopped {
				req.Lots = remaining
				e.hedges.Add(req, nil)
				e.state.StartFix(e.clock.Now(), e.config.RetryWait, "remaining hedge volume after unverified acceptance")
				return nil
			}
			size = min(size, remaining)
			shrinking = shrinking && size >= e.config.RejectRetryMinLots
			continue
		}
		lastErr = err
		if OrderMayExist(err) {
			orderID, _ := ErrorClientID(err)
			e.rememberUnknown(part, err, true)
			e.acceptUntrackedMarket(part, orderID, "mandatory placement outcome is unknown")
			remaining -= size
			if remaining > 0 {
				req.Lots = remaining
				e.hedges.Add(req, err)
				e.state.StartFix(e.clock.Now(), e.config.RetryWait, "unplaced hedge remainder after ambiguous placement")
			}
			return nil
		}
		if shrinking {
			if size-e.config.RejectRetryLotStep < e.config.RejectRetryMinLots {
				shrinking = false
			} else {
				size -= e.config.RejectRetryLotStep
			}
		}
	}
	req.Lots = remaining
	e.hedges.Add(req, lastErr)
	e.state.StartFix(e.clock.Now(), e.config.RetryWait, "mandatory hedge placement failed")
	e.logger.Warnf("queued hedge debt on %s x%d after exhausting placement retries", req.Symbol, req.Lots)
	return lastErr
}

func (e *Engine) closeOrder(ctx context.Context, orderID string) (orders.Change, error) {
	snap, ok := e.orders.Info(orderID)
	if !ok {
		return orders.Change{}, fmt.Errorf("cannot retire unknown order %q", orderID)
	}
	if snap.Done {
		e.trade.CloseOrder(orderID)
		return orders.Change{}, nil
	}
	result, cancelErr := e.broker.Cancel(ctx, orderID)
	if cancelErr != nil && !e.orders.Closing(orderID) {
		result, cancelErr = e.broker.Cancel(ctx, orderID)
	}
	if cancelErr != nil {
		status, statusErr := e.broker.Status(ctx, orderID)
		if statusErr != nil || !status.Done {
			e.orders.NeedClose(orderID)
			e.state.StartFix(e.clock.Now(), e.config.RetryWait, "maker retirement unresolved")
			return orders.Change{}, errors.Join(cancelErr, statusErr)
		}
		result.Filled = status.Filled
	}
	change := e.orders.Close(orderID, result.Filled)
	e.trade.CloseOrder(orderID)
	e.sendChange(change, "maker retirement")
	if change.Conflict {
		e.logger.Criticalf("terminal result for %s contradicts already reported executions", orderID)
	}
	if snap.Request.Kind == model.OrderMarket && change.Lots != 0 {
		e.trade.FixMarket(snap.Request.TradeID, snap.Request.Leg, change.Lots)
	}
	return change, nil
}

func (e *Engine) resolveTrade(ctx context.Context, reason string) error {
	clip, active := e.trade.Info()
	if !active {
		return nil
	}
	e.trade.Stop(e.settlement(clip.Plan))
	return e.stopTrade(ctx, reason, true)
}

func (e *Engine) settlement(plan Plan) trade.Settlement {
	if !plan.IsClose && e.config.KeepPartialOpenOnTimeout {
		return trade.Drop
	}
	if plan.IsClose && e.config.ForceCloseOnTimeout {
		return trade.Complete
	}
	return trade.Resolve
}

func (e *Engine) completeTrade(ctx context.Context) error {
	e.trade.Stop(trade.Complete)
	return e.stopTrade(ctx, "target filled", true)
}

func (e *Engine) stopTrade(ctx context.Context, reason string, allowBalance bool, terminal ...orders.Change) error {
	if e.state.Info().Code == run.Stopped || e.hedges.UnknownCount() > 0 {
		allowBalance = false
	}
	ids := e.trade.Stop(trade.Drop)
	var result error
	for _, orderID := range ids {
		var change orders.Change
		var err error
		if len(terminal) > 0 && terminal[0].Order.ID == orderID {
			change = terminal[0]
			e.trade.CloseOrder(orderID)
			e.sendChange(change, "maker terminal status")
		} else {
			change, err = e.closeOrder(ctx, orderID)
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if change.Lots != 0 {
			result = errors.Join(result, e.useLimitFill(ctx, change, false))
		}
	}
	if !allowBalance {
		e.state.NeedCheck(reason + ": placement outcome unknown")
		// A cancelled ledger-based clip need not own the unknown hedge. Its
		// request and provisional credit remain in the independent recovery list.
		e.trade.DropSettled()
		// A fully credited clip can be booked without another placement. The
		// unknown-order barrier still blocks new exposure until confirmation.
		if e.state.Info().Code != run.Stopped && len(e.orders.OrdersToClose()) == 0 && e.trade.Full() {
			e.finishTrade(e.now)
		}
		if e.hedges.UnknownCount() == 0 {
			e.trade.DropEmpty()
		}
		return result
	}
	if len(e.orders.OrdersToClose()) > 0 {
		return result
	}
	if len(e.hedges.All()) == 0 {
		result = errors.Join(result, e.hedgeTrade(ctx, model.RoleFix))
	}
	if len(e.hedges.All()) == 0 && e.hedges.UnknownCount() == 0 && e.trade.IsPaired() {
		if !e.trade.DropSettled() {
			e.finishTrade(e.now)
		}
	}
	e.trade.DropEmpty()
	return result
}

func (e *Engine) finishTrade(at time.Time) {
	plan, ok := e.trade.Finish(false)
	if !ok {
		return
	}
	e.commit(plan, at)
}

func (e *Engine) commit(plan Plan, at time.Time) {
	e.strategy.Commit(plan, at)
	if saver, ok := e.strategy.(Saver); ok {
		saver.SaveLots()
	}
	e.logger.Infof("committed clip action=%d lots=%d position=%d", plan.Action, plan.Lots, e.strategy.Position())
}

func (e *Engine) useMarketStatus(ctx context.Context, orderID string, status model.OrderStatus) error {
	if snap, ok := e.orders.Info(orderID); ok && status.Done && status.Filled < snap.Filled {
		// A terminal acknowledgement cannot erase executions already proved by
		// the fill stream. Size the replacement from the same count the order
		// registry retains, or the difference would be hedged a second time.
		e.logger.Criticalf("taker %s terminal count %d is below %d stream executions; retaining the stream count", orderID, status.Filled, snap.Filled)
		status.Filled = snap.Filled
	}
	result := e.hedges.SetStatus(orderID, status)
	if !result.Known {
		return nil
	}
	if status.Done {
		change := e.orders.Close(orderID, status.Filled)
		e.sendChange(change, "taker terminal correction")
		if change.Lots != 0 {
			e.trade.FixMarket(
				change.Order.Request.TradeID,
				change.Order.Request.Leg,
				change.Lots,
			)
		}
	}
	if result.Missing.Lots > 0 {
		if result.RetryNow && e.state.Info().Code != run.Stopped {
			// Pay only this proven shortfall. placeHedge owns retries and queues
			// any unplaced remainder; unrelated recovery work keeps its schedule.
			return e.placeHedge(ctx, result.Missing)
		}
		e.hedges.Add(result.Missing, nil)
		e.state.StartFix(e.clock.Now(), e.config.RetryWait, "taker executed fewer lots than requested")
		e.logger.Warnf(
			"queued taker shortfall on %s x%d for recovery",
			result.Missing.Symbol, result.Missing.Lots,
		)
	}
	return nil
}

func (e *Engine) checkMarketOrders(ctx context.Context, now time.Time) error {
	checks := e.hedges.Checks(now, e.config.MarketCheckAfter, e.config.MarketCheckEvery, e.config.RetryMax)
	var result error
	for _, check := range checks {
		status, err := e.broker.Status(ctx, check.OrderID)
		if err != nil {
			e.state.StartFix(now, e.config.RetryWait, "taker confirmation unavailable")
			result = errors.Join(result, err)
			continue
		}
		result = errors.Join(result, e.useMarketStatus(ctx, check.OrderID, status))
	}
	return result
}

func (e *Engine) fixWork(ctx context.Context, now time.Time) error {
	didWork := false
	var result error
	for _, orderID := range e.orders.OrdersToClose() {
		change, err := e.closeOrder(ctx, orderID)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		didWork = true
		if change.Lots != 0 {
			result = errors.Join(result, e.useLimitFill(ctx, change, true))
		}
	}
	for _, debt := range e.hedges.All() {
		if e.state.Info().Code == run.Stopped {
			break
		}
		// Transfer ownership to placeHedge: its shrinking ladder records only
		// the unplaced remainder, including after ambiguity or budget denial.
		// These lots exclude accepted/unknown chunks and remain safe to send;
		// hedgeTrade still blocks inferring a new obligation from unknown fills.
		e.hedges.Done(debt.ID)
		pending := e.hedges.MarketCount()
		err := e.placeHedge(ctx, debt.Request)
		result = errors.Join(result, err)
		didWork = didWork || err == nil || e.hedges.MarketCount() > pending
	}
	remaining := len(e.orders.OrdersToClose()) + len(e.hedges.All()) + e.hedges.MarketCount() + e.hedges.UnknownCount()
	if e.state.Info().Code == run.Stopped {
		remaining = len(e.orders.OrdersToClose())
	}
	e.state.FixDone(
		now, remaining, didWork, e.config.RetryWait, e.config.RetryMax,
	)
	if e.trade.Full() {
		result = errors.Join(result, e.completeTrade(ctx))
	}
	return result
}

func (e *Engine) placeFailed(err error, operation string) {
	if OrderMayExist(err) {
		e.state.NeedCheck(operation + " outcome is unknown")
		e.logger.Criticalf("%s is ambiguous: %v", operation, err)
		return
	}
	e.logger.Warnf("%s was definitively rejected: %v", operation, err)
}

func (e *Engine) halt(ctx context.Context, reason string) error {
	if e.state.Stop(reason) {
		e.logger.Criticalf("engine halted: %s", reason)
	}
	return e.stopTrade(ctx, "engine halted", false)
}
