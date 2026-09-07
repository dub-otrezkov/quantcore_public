package execengine2

import (
	"context"
	"errors"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
	"QuantCore/strategies/execengine2/internal/orders"
	"QuantCore/strategies/execengine2/internal/run"
)

func (e *Engine) rememberUnknown(req model.OrderRequest, err error, counted bool) {
	if req.Kind != model.OrderMarket || !OrderMayExist(err) {
		return
	}
	clientID, _ := ErrorClientID(err)
	e.hedges.AddUnknown(clientID, req, counted, e.clock.Now().Add(e.config.RetryWait))
}

// A full inventory snapshot can confirm an already executed order that has
// disappeared from the broker's active list. Partial/baseline positions cannot
// prove that an uncredited market order will never fill.
func (e *Engine) confirmUnknownPositions(actualA, actualB int) {
	if e.hedges.UnknownCount() == 0 || e.state.Info().Code == run.Stopped ||
		len(e.orders.OrdersToClose()) != 0 || e.hedges.MarketCount() != 0 {
		return
	}
	clip, active := e.trade.Info()
	creditedA := e.strategy.Position()
	creditedB := -creditedA * e.config.Ratio
	sign := 1
	if active {
		if clip.Plan.Action < 0 {
			sign = -1
		}
		creditedA = clip.BasePosition + sign*clip.FilledA
		creditedB = -clip.BasePosition*e.config.Ratio - sign*clip.FilledB
	}
	if actualA == creditedA && actualB == creditedB {
		before := e.hedges.UnknownCount()
		e.hedges.ConfirmCounted()
		if e.hedges.UnknownCount() < before && len(e.hedges.All()) > 0 {
			e.state.StartFix(e.clock.Now(), e.config.RetryWait, "position confirmed a hedge with remaining debt")
		}
	}
	if !active || clip.Mode != model.ModeMarket || !clip.Stopping || len(e.hedges.All()) != 0 {
		return
	}
	targetA := clip.BasePosition + sign*clip.LotsA
	if actualA != targetA || actualB != -targetA*e.config.Ratio {
		return
	}
	pending := e.hedges.UnknownOrders()
	projectedA, projectedB := clip.FilledA, clip.FilledB
	for _, unknown := range pending {
		if unknown.Request.TradeID != clip.ID {
			return
		}
		if !unknown.Counted {
			if unknown.Request.Leg == model.LegA {
				projectedA += unknown.Request.Lots
			} else {
				projectedB += unknown.Request.Lots
			}
		}
	}
	if projectedA != clip.LotsA || projectedB != clip.LotsA*e.config.Ratio {
		return
	}
	for _, unknown := range pending {
		if !unknown.Counted {
			e.sendChange(orders.Change{
				Known: true, Lots: unknown.Request.Lots,
				Order: orders.Info{ID: unknown.ClientID, Request: unknown.Request, GuessPrice: unknown.Request.Price},
			}, "taker confirmed by full position snapshot")
			e.trade.AddMarket(unknown.Request.TradeID, unknown.Request.Leg, unknown.Request.Lots)
		}
		e.hedges.ResolveUnknown(unknown.ID)
	}
	e.finishTrade(e.clock.Now(), false)
}

// resumeUnknown only resumes the original idempotent placement. In particular,
// a matching position snapshot is not proof that an unknown order cannot fill.
func (e *Engine) resumeUnknown(ctx context.Context, now time.Time) error {
	if e.state.Info().Code == run.Stopped {
		return nil
	}
	resumer, canResume := e.broker.(PlacementResumer)
	var result error
	// The existing retry schedule also paces diagnostics for orders that cannot
	// be resumed, so their reconciliation barrier has an operational explanation.
	for _, pending := range e.hedges.UnknownDue(now, e.config.RetryWait, e.config.RetryMax) {
		if !canResume {
			e.logger.Warnf("broker cannot resume placements: %s on %s stays behind the reconciliation barrier",
				pending.ClientID, pending.Request.Symbol)
			continue
		}
		if pending.ClientID == "" {
			e.logger.Warnf("no client id for an unknown placement on %s: it stays behind the reconciliation barrier",
				pending.Request.Symbol)
			continue // no safe idempotency key: keep the reconciliation barrier
		}
		orderID, err := resumer.ResumePlacement(ctx, pending.Request, pending.ClientID)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		// Adopt without calling addOrder: a previously credited hedge must not
		// credit inventory or the clip a second time.
		change, err := e.orders.Add(orderID, pending.Request, now, pending.Request.Price, true)
		if err != nil {
			e.halt("resumed market order has an unusable id: " + err.Error())
			return errors.Join(result, err)
		}
		if err := e.hedges.AddMarket(orderID, pending.Request, now); err != nil {
			e.halt("resumed market order could not be tracked: " + err.Error())
			return errors.Join(result, err)
		}
		if !pending.Counted {
			e.sendChange(change, "resumed taker placement")
			e.trade.AddMarket(pending.Request.TradeID, pending.Request.Leg, pending.Request.Lots)
		}
		e.hedges.ResolveUnknown(pending.ID)
		// Status may already be terminal (including zero/partial execution).
		status, err := e.broker.Status(ctx, orderID)
		if err != nil {
			result = errors.Join(result, err)
		} else {
			result = errors.Join(result, e.useMarketStatus(ctx, orderID, status))
		}
		e.state.StartFix(now, e.config.RetryWait, "resumed market order requires confirmation")
	}
	return result
}
