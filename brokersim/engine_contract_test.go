//go:build sim

package brokersim_test

import (
	"context"
	"testing"
	"time"

	"QuantCore/finambroker"
	"QuantCore/strategies/execengine"
	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/budget"
	"QuantCore/trade/finam"
)

type engineState uint8

const (
	engineReady engineState = iota
	engineRecovering
	engineCheckNeeded
	engineStopped
)

type engineSnapshot struct {
	state    engineState
	working  bool
	position int
}

// harnessEngine is the only engine surface brokersim scenarios use. Both
// production engines stay unchanged; their adapters below absorb API differences.
type harnessEngine interface {
	OnBook(symbol string, at time.Time, bid, ask float64)
	OnSignal(at time.Time)
	OnFill(tradeID, orderID, symbol string, buy bool, lots int, price float64, at time.Time)
	// dead is finambroker.IsDeadStatus; FILLED/EXECUTED are handled by OnFill.
	OnOrderStatus(orderID string, filled int, dead bool)
	OnTick(time.Time)
	StopTrade()
	Stop(reason string)
	Reconcile(actualA, actualB int)
	Snapshot() engineSnapshot
}

type v1EngineAdapter struct {
	engine *execengine.Engine
	dedup  execengine.TradeDedup
}

func (a *v1EngineAdapter) OnBook(symbol string, at time.Time, bid, ask float64) {
	a.engine.OnBook(symbol, at, bid, ask)
}

func (a *v1EngineAdapter) OnSignal(at time.Time) {
	a.engine.OnState(execengine.RowState{Time: at})
}

func (a *v1EngineAdapter) OnFill(
	tradeID, orderID, symbol string,
	buy bool,
	lots int,
	price float64,
	at time.Time,
) {
	if !a.dedup.Seen(tradeID) {
		a.engine.OnFill(at, orderID, symbol, buy, lots, price)
	}
}

func (a *v1EngineAdapter) OnOrderStatus(orderID string, _ int, dead bool) {
	a.engine.OnOrderStatus(orderID, dead)
}

func (a *v1EngineAdapter) OnTick(at time.Time) { a.engine.OnTick(at) }
func (a *v1EngineAdapter) StopTrade()          { a.engine.CancelClip() }
func (a *v1EngineAdapter) Stop(reason string)  { a.engine.Halt(reason) }
func (a *v1EngineAdapter) Reconcile(aPos, bPos int) {
	a.engine.Reconcile(aPos, bPos)
}

func (a *v1EngineAdapter) Snapshot() engineSnapshot {
	state := engineReady
	switch {
	case a.engine.Halted():
		state = engineStopped
	case a.engine.Impaired():
		state = engineRecovering
	case a.engine.Suspect():
		state = engineCheckNeeded
	}
	return engineSnapshot{state: state, working: a.engine.Working(), position: a.engine.Position()}
}

type v2EngineAdapter struct{ engine *execengine2.Engine }

func (a *v2EngineAdapter) OnBook(symbol string, at time.Time, bid, ask float64) {
	_ = a.engine.OnBook(context.Background(), symbol, at, bid, ask)
}

func (a *v2EngineAdapter) OnSignal(at time.Time) {
	_ = a.engine.OnSignal(context.Background(), execengine2.Signal{Time: at})
}

func (a *v2EngineAdapter) OnFill(
	tradeID, orderID, symbol string,
	buy bool,
	lots int,
	price float64,
	at time.Time,
) {
	side := execengine2.SideSell
	if buy {
		side = execengine2.SideBuy
	}
	_ = a.engine.OnFill(context.Background(), execengine2.Fill{
		FillID: tradeID, OrderID: orderID, Symbol: symbol,
		Side: side, Lots: lots, Price: price, At: at,
	})
}

func (a *v2EngineAdapter) OnOrderStatus(orderID string, filled int, dead bool) {
	_ = a.engine.OnOrderStatus(context.Background(), orderID, execengine2.OrderStatus{
		Filled: filled,
		Done:   dead,
	})
}

func (a *v2EngineAdapter) OnTick(at time.Time) {
	_ = a.engine.OnTick(context.Background(), at)
}

func (a *v2EngineAdapter) StopTrade()         { _ = a.engine.StopTrade(context.Background()) }
func (a *v2EngineAdapter) Stop(reason string) { _ = a.engine.Stop(context.Background(), reason) }
func (a *v2EngineAdapter) Reconcile(aPos, bPos int) {
	a.engine.CheckPositions(aPos, bPos)
}

func (a *v2EngineAdapter) Snapshot() engineSnapshot {
	info := a.engine.Info()
	state := engineReady
	switch info.Code {
	case execengine2.StateFixing:
		state = engineRecovering
	case execengine2.StateCheckNeeded:
		state = engineCheckNeeded
	case execengine2.StateStopped:
		state = engineStopped
	}
	return engineSnapshot{state: state, working: info.HasTrade, position: info.Position}
}

type v1ScriptStrategy struct{ state *scriptDecider }

func (s v1ScriptStrategy) Peek(execengine.RowState) execengine.Intent {
	plan := s.state.peek()
	return execengine.Intent{Action: plan.action, IsClose: plan.isClose, Lots: plan.lots}
}

func (s v1ScriptStrategy) Commit(plan execengine.Intent, _ time.Time) execengine.Decision {
	return execengine.Decision{Decision: plan.Action}
}

func (s v1ScriptStrategy) Position() int { return s.state.posA }

type v1ScriptUpdates struct{ state *scriptDecider }

func (u v1ScriptUpdates) Fill(symbol string, buy bool, lots int, _ float64) {
	if !buy {
		lots = -lots
	}
	u.state.apply(symbol, lots)
}

func (v1ScriptUpdates) Amend(string, bool, int, float64, float64) {}

type v2ScriptStrategy struct{ state *scriptDecider }

func (s v2ScriptStrategy) Peek(execengine2.Signal) execengine2.Plan {
	plan := s.state.peek()
	return execengine2.Plan{Action: plan.action, IsClose: plan.isClose, Lots: plan.lots}
}

func (s v2ScriptStrategy) Commit(plan execengine2.Plan, _ time.Time) execengine2.Result {
	return execengine2.Result{Code: plan.Action}
}

func (s v2ScriptStrategy) Position() int { return s.state.posA }

type v2ScriptUpdates struct{ state *scriptDecider }

func (u v2ScriptUpdates) Apply(change execengine2.PositionChange) {
	u.state.apply(change.Symbol, change.Lots)
}

func (v2ScriptUpdates) Amend(execengine2.PriceChange) {}

// harnessEngineFactory is the one version-specific construction seam. The
// brokersim loop receives only harnessEngine after this point.
type harnessEngineFactory interface {
	Name() string
	Build(
		t *testing.T,
		client *finam.Client,
		strategy *scriptDecider,
		cfg harnessBuild,
		stop <-chan struct{},
	) (harnessEngine, func(int, time.Time))
}

type v1HarnessEngine struct{}

func (v1HarnessEngine) Name() string { return "execengine" }

func (v1HarnessEngine) Build(
	t *testing.T,
	client *finam.Client,
	strategy *scriptDecider,
	cfg harnessBuild,
	stop <-chan struct{},
) (harnessEngine, func(int, time.Time)) {
	t.Helper()
	engine := execengine.NewEngine(
		cfg.ec, finambroker.NewMaker(client, ""), finambroker.NewTaker(client, ""),
		v1ScriptStrategy{state: strategy},
	)
	engine.SetFillSink(v1ScriptUpdates{state: strategy})

	var setQuota func(int, time.Time)
	if cfg.limiterQuota > 0 {
		var limiter *execengine.QuotaLimiter
		if cfg.limiterBudget > 0 {
			limiter = execengine.NewQuotaLimiterBudget(cfg.limiterQuota, cfg.limiterBudget, cfg.limiterWindow)
		} else {
			limiter = execengine.NewQuotaLimiter(cfg.limiterQuota)
		}
		engine.SetLimiter(limiter)
		setQuota = func(remaining int, resetAt time.Time) {
			limiter.Set(remaining, resetAt, time.Now(), limiter.Snapshot())
		}
		if cfg.refreshQuota {
			ctx, cancel := context.WithCancel(context.Background())
			go func() { <-stop; cancel() }()
			go finambroker.RefreshQuota(ctx, client, limiter)
		}
	}
	return &v1EngineAdapter{engine: engine}, setQuota
}

type v2HarnessEngine struct{}

func (v2HarnessEngine) Name() string { return "execengine2" }

type current = v2HarnessEngine

func (v2HarnessEngine) Build(
	t *testing.T,
	client *finam.Client,
	strategy *scriptDecider,
	cfg harnessBuild,
	stop <-chan struct{},
) (harnessEngine, func(int, time.Time)) {
	t.Helper()
	limit, reserve := 1_000_000, 0
	if cfg.limiterQuota > 0 {
		limit, reserve = execengine.DefaultPlaceOrderBudget, cfg.limiterQuota
	}
	if cfg.limiterBudget > 0 {
		limit = cfg.limiterBudget
	}
	window := cfg.limiterWindow
	if window <= 0 {
		window = execengine.DefaultQuotaWindow
	}
	sendBudget, err := budget.NewQuota(limit, reserve, window)
	if err != nil {
		t.Fatalf("execengine2 budget: %v", err)
	}
	broker, err := finambroker.NewGateway(client, sendBudget, "[extreme]")
	if err != nil {
		t.Fatalf("execengine2 gateway: %v", err)
	}
	mode := execengine2.ModeTwoLimits
	if cfg.ec.TakerOnly {
		mode = execengine2.ModeMarket
	} else if cfg.ec.SoloMakerLeg {
		mode = execengine2.ModeLimitA
	}
	engine, err := execengine2.NewEngine(execengine2.Config{
		LegA: cfg.ec.LegA, LegB: cfg.ec.LegB, Lots: cfg.ec.OrderVol, Mode: mode,
		BookMaxAge: cfg.ec.MaxStaleness, PriceWait: cfg.ec.RepegThrottle, MinRest: cfg.ec.MinRest,
		TradeTimeout: cfg.ec.FillTimeout, RetryWait: cfg.ec.PlaceRetryBackoff,
		Ratio: cfg.ec.HedgeRatio, ForceCloseOnTimeout: cfg.ec.ForceCloseOnTimeout,
		KeepPartialOpenOnTimeout: cfg.ec.KeepPartialOpenOnTimeout,
		DisableRepeg:             cfg.ec.DisableRepeg, PullOnStaleBook: cfg.ec.PullOnStaleBook,
		RejectRetryLotStep: cfg.ec.RejectRetryLotStep, RejectRetryMinLots: cfg.ec.RejectRetryMinLots,
		MarketCheckAfter: 2 * time.Second, MarketCheckEvery: 300 * time.Millisecond,
		HedgeTries: cfg.ec.HedgeRetries, LogTag: cfg.ec.LogTag,
	}, execengine2.Setup{
		Broker: broker, Limit: sendBudget,
		Strategy: v2ScriptStrategy{state: strategy},
		Changes:  v2ScriptUpdates{state: strategy},
	})
	if err != nil {
		t.Fatalf("execengine2.NewEngine: %v", err)
	}
	if cfg.refreshQuota {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-stop; cancel() }()
		go finambroker.RefreshQuota(ctx, client, sendBudget)
	}
	var setQuota func(int, time.Time)
	if cfg.limiterQuota > 0 {
		setQuota = func(remaining int, resetAt time.Time) {
			sendBudget.Set(remaining, resetAt, time.Now(), sendBudget.Snapshot())
		}
	}
	return &v2EngineAdapter{engine: engine}, setQuota
}

var (
	_ harnessEngine = (*v1EngineAdapter)(nil)
	_ harnessEngine = (*v2EngineAdapter)(nil)
)
