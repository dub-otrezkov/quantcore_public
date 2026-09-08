package execengine2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
	"QuantCore/strategies/execengine2/internal/run"
	"QuantCore/strategies/execengine2/internal/trade"
)

// openMarket joins both broker calls before applying either result. The caller
// has already admitted both attempts and started the trade in the event loop.
func (e *Engine) openMarket(ctx context.Context, requests []model.OrderRequest, at time.Time, chargeAfter bool) error {
	if len(requests) != 2 {
		return errors.New("market opening requires exactly two orders")
	}
	type placement struct {
		orderID string
		err     error
	}
	var results [2]placement
	var calls sync.WaitGroup
	calls.Add(len(results))
	for i, req := range requests {
		go func() {
			defer calls.Done()
			results[i].orderID, results[i].err = e.broker.Place(ctx, req)
		}()
	}
	calls.Wait()

	var result error
	anyUnknown := false
	for i, placed := range results {
		if chargeAfter {
			e.limit.Take(1, model.LimitMust)
		}
		req := requests[i]
		if placed.err != nil {
			e.placeFailed(placed.err, "opening placement")
			if OrderMayExist(placed.err) {
				anyUnknown = true
				e.rememberUnknown(req, placed.err, false)
			}
			result = errors.Join(result, fmt.Errorf("placing opening order on %s: %w", req.Symbol, placed.err))
			continue
		}
		if err := e.trade.Attach(req.Leg, placed.orderID, e.clock.Now()); err != nil {
			_ = e.halt(ctx, "placed order could not be attached to its clip: "+err.Error())
			result = errors.Join(result, err)
		}
		// Even an unusable ID must not discard an accepted market placement or
		// prevent accounting for the other result.
		result = errors.Join(result, e.addOrder(ctx, placed.orderID, req))
	}
	if result != nil {
		e.quotes.BlockOpen(at.Add(e.config.RetryWait))
		if !anyUnknown && e.state.Info().Code != run.Stopped {
			clip, _ := e.trade.Info()
			canShrink := clip.Plan.IsClose || results[0].err == nil || results[1].err == nil
			for i, placed := range results {
				if placed.err != nil {
					req := requests[i]
					req.Role = model.RoleFix
					result = errors.Join(result, e.placeHedgeAttempts(ctx, req, e.config.HedgeTries-1, canShrink))
				}
			}
			if e.state.Info().Code != run.Stopped {
				if plan, ok := e.trade.FinishMarket(); ok {
					e.commit(plan, at)
				}
			}
			return result
		}
		e.trade.Stop(trade.Complete)
		return errors.Join(result, e.stopTrade(ctx, "opening placement failed", !anyUnknown))
	}
	if e.trade.Full() {
		e.finishTrade(at)
	}
	return nil
}
