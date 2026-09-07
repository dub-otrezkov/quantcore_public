package finambroker

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/strategies/execengine2"
	"QuantCore/trade/finam"
)

const (
	idRandomBytes = 7
	idRandomChars = 12
	idCountChars  = 7
	maxIDCount    = uint64(78_364_164_095) // 36^7 - 1
	sendTries     = cidRetryRounds
	findTries     = ghostProbes
	findWait      = ghostProbeGap
)

type clientIDs struct {
	prefix string
	count  atomic.Uint64
}

func newClientIDs(source io.Reader) (*clientIDs, error) {
	var raw [idRandomBytes]byte
	if _, err := io.ReadFull(source, raw[:]); err != nil {
		return nil, fmt.Errorf("reading client-id nonce: %w", err)
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:])
	prefix := strings.ToLower(encoded)
	if len(prefix) != idRandomChars {
		return nil, fmt.Errorf("client-id nonce length = %d, want %d", len(prefix), idRandomChars)
	}
	return &clientIDs{prefix: prefix}, nil
}

func (g *clientIDs) Next() (string, error) {
	count := g.count.Add(1)
	if count > maxIDCount {
		return "", errors.New("client-order-id sequence exhausted")
	}
	encoded := strconv.FormatUint(count, 36)
	id := "q" + g.prefix + strings.Repeat("0", idCountChars-len(encoded)) + encoded
	if len(id) != 20 {
		return "", fmt.Errorf("client order id length = %d, want 20", len(id))
	}
	return id, nil
}

type apiOrder struct {
	id     string
	symbol string
	side   execengine2.Side
	kind   execengine2.OrderKind
	lots   int
	price  float64
	filled int
	done   bool
}

type ordersAPI interface {
	Place(context.Context, execengine2.OrderRequest, string) (apiOrder, error)
	Find(context.Context, string) (apiOrder, bool, error)
	Cancel(context.Context, string) (apiOrder, error)
	Status(context.Context, string) (apiOrder, error)
}

type waiter interface {
	Wait(context.Context, time.Duration) error
}

type timerWait struct{}

func (timerWait) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Gateway — единая точка работы execengine2 с заявками Finam.
// В нём хранятся API, генератор ID, таймер и метка лога.
type Gateway struct {
	api    ordersAPI
	ids    *clientIDs
	waiter waiter
	limit  execengine2.SendLimit
	logTag string
	now    func() time.Time

	placementMu sync.Mutex
	placements  map[string]placementWindow
}

type placementWindow struct {
	request    execengine2.OrderRequest
	deadline   time.Time
	lookupOnly bool
}

// NewGateway создаёт Broker для Finam. limit должен быть тем же объектом,
// который передан в Engine: Gateway списывает из него внутренние повторы.
func NewGateway(
	client *finam.Client,
	limit execengine2.SendLimit,
	logTag string,
) (*Gateway, error) {
	if client == nil {
		return nil, errors.New("finam client is required")
	}
	if limit == nil {
		return nil, errors.New("send limit is required")
	}
	ids, err := newClientIDs(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		api: &finamAPI{client: client}, ids: ids, waiter: timerWait{}, limit: limit, logTag: logTag,
	}, nil
}

func (g *Gateway) logf(format string, args ...any) {
	mlog.Printf("[execengine2]"+g.logTag+" "+format, args...)
}

func (g *Gateway) critical(format string, args ...any) {
	mlog.Critical("[execengine2]"+g.logTag+" "+format, args...)
}

// Place повторяет отправку только с тем же client ID.
// При отмене ctx проверка сразу останавливается.
func (g *Gateway) Place(
	ctx context.Context,
	req execengine2.OrderRequest,
) (string, error) {
	if ctx == nil {
		return "", errors.New("nil context")
	}
	if err := checkRequest(req); err != nil {
		return "", execengine2.NotPlaced(err)
	}
	if err := ctx.Err(); err != nil {
		return "", execengine2.NotPlaced(err)
	}
	clientID, err := g.ids.Next()
	if err != nil {
		return "", execengine2.NotPlaced(err)
	}
	deadline := placementRetryDeadline(g.timeNow())
	orderID, lookupOnly, err := g.placeWithClientID(ctx, req, clientID, deadline)
	if err != nil && execengine2.OrderMayExist(err) {
		g.rememberPlacement(clientID, req, deadline)
		if lookupOnly {
			g.stopPlacementResends(clientID)
		}
	}
	return orderID, err
}

// ResumePlacement resolves a previous ambiguous placement without minting another ID.
// Looking it up is free; every resend consumes the mandatory recovery budget.
// Each call makes at most one resend without waiting; Engine.OnTick paces recovery.
// AlreadyExists disables further resends but leaves the placement unresolved.
func (g *Gateway) ResumePlacement(
	ctx context.Context,
	req execengine2.OrderRequest,
	clientID string,
) (string, error) {
	if ctx == nil {
		return "", execengine2.OrderUnknown(clientID, errors.New("nil context"))
	}
	if clientID == "" {
		return "", execengine2.OrderUnknown(clientID, errors.New("empty client order id"))
	}
	if err := checkRequest(req); err != nil {
		return "", execengine2.OrderUnknown(clientID, err)
	}
	if err := ctx.Err(); err != nil {
		return "", execengine2.OrderUnknown(clientID, err)
	}
	pending, known := g.pendingPlacement(clientID)
	if known && req != pending.request {
		return "", execengine2.OrderUnknown(clientID, errors.New("resumption does not match original request"))
	}
	order, found, err := g.api.Find(ctx, clientID)
	if err == nil && found {
		if !sameOrder(order, req) {
			g.critical("client id %s resolved to a mismatched order; refusing adoption", clientID)
			return "", execengine2.OrderUnknown(clientID, errors.New("resolved order does not match request"))
		}
		g.forgetPlacement(clientID)
		return order.id, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", execengine2.OrderUnknown(clientID, errors.Join(err, ctxErr))
	}
	// An absent active order may already have filled or been canceled. Only an
	// idempotent resend within the original day can continue this placement.
	if !known || !g.timeNow().Before(pending.deadline) {
		g.forgetPlacement(clientID)
		return "", execengine2.OrderUnknown(clientID, errors.Join(err, errors.New("client ID resend window is unknown or expired")))
	}
	if pending.lookupOnly {
		return "", execengine2.OrderUnknown(clientID, errors.Join(err, errors.New("client ID already exists; placement requires lookup or reconciliation")))
	}
	if g.limit == nil || !g.limit.Take(1, execengine2.LimitMust) {
		return "", execengine2.OrderUnknown(clientID, errors.Join(err, errors.New("send limit blocked retry")))
	}
	order, placeErr := g.api.Place(ctx, req, clientID)
	if placeErr != nil {
		if status.Code(placeErr) == codes.AlreadyExists {
			g.stopPlacementResends(clientID)
		}
		// Even a definitive rejection here cannot disprove the original delivery.
		return "", execengine2.OrderUnknown(clientID, errors.Join(err, placeErr, ctx.Err()))
	}
	if !sameOrder(order, req) {
		g.critical("client id %s returned a mismatched placement response", clientID)
		return "", execengine2.OrderUnknown(clientID, errors.New("successful placement response does not match request"))
	}
	g.forgetPlacement(clientID)
	return order.id, nil
}

func (g *Gateway) timeNow() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// Finam does not document the client-ID retention timezone. Do not carry a
// resend across either UTC or Moscow midnight, including inside Place itself.
// A recovered ID without this gateway's original window is lookup-only.
func placementRetryDeadline(now time.Time) time.Time {
	utc := now.UTC()
	deadline := time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
	moscowMidnight := deadline.Add(-3 * time.Hour)
	if now.Before(moscowMidnight) {
		return moscowMidnight
	}
	return deadline
}

func (g *Gateway) rememberPlacement(clientID string, req execengine2.OrderRequest, deadline time.Time) {
	g.placementMu.Lock()
	defer g.placementMu.Unlock()
	if g.placements == nil {
		g.placements = make(map[string]placementWindow)
	}
	now := g.timeNow()
	for id, pending := range g.placements {
		if !now.Before(pending.deadline) {
			delete(g.placements, id)
		}
	}
	g.placements[clientID] = placementWindow{request: req, deadline: deadline}
}

func (g *Gateway) pendingPlacement(clientID string) (placementWindow, bool) {
	g.placementMu.Lock()
	defer g.placementMu.Unlock()
	pending, known := g.placements[clientID]
	return pending, known
}

func (g *Gateway) forgetPlacement(clientID string) {
	g.placementMu.Lock()
	defer g.placementMu.Unlock()
	delete(g.placements, clientID)
}

func (g *Gateway) stopPlacementResends(clientID string) {
	g.placementMu.Lock()
	defer g.placementMu.Unlock()
	if pending, known := g.placements[clientID]; known {
		pending.lookupOnly = true
		g.placements[clientID] = pending
	}
}

func (g *Gateway) placeWithClientID(
	ctx context.Context,
	req execengine2.OrderRequest,
	clientID string,
	deadline time.Time,
) (orderID string, lookupOnly bool, err error) {
	var sendErr error
	limitKind := retryKind(req)
	for round := 0; round < sendTries; round++ {
		if err := ctx.Err(); err != nil {
			if sendErr == nil {
				return "", lookupOnly, execengine2.NotPlaced(err)
			}
			return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, err))
		}
		if round > 0 {
			if !g.timeNow().Before(deadline) {
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, errors.New("client ID resend window expired")))
			}
			if g.limit == nil || !g.limit.Take(1, limitKind) {
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, errors.New("send limit blocked retry")))
			}
		}
		order, placeErr := g.api.Place(ctx, req, clientID)
		if placeErr == nil {
			if !sameOrder(order, req) {
				err := errors.New("successful placement response does not match request")
				g.critical("client id %s returned a mismatched placement response", clientID)
				return "", lookupOnly, execengine2.OrderUnknown(clientID, err)
			}
			return order.id, lookupOnly, nil
		}
		if !ambiguous(placeErr) {
			if sendErr != nil {
				// A later rejection cannot disprove delivery of an earlier attempt.
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, placeErr))
			}
			return "", lookupOnly, execengine2.NotPlaced(placeErr)
		}
		sendErr = errors.Join(sendErr, placeErr)
		lookupOnly = status.Code(placeErr) == codes.AlreadyExists
		for range findTries {
			if waitErr := g.waiter.Wait(ctx, findWait); waitErr != nil {
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, waitErr))
			}
			if err := ctx.Err(); err != nil {
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, err))
			}
			order, found, findErr := g.api.Find(ctx, clientID)
			if findErr != nil {
				continue
			}
			if !found {
				continue // active-list absence does not prove the order never existed
			}
			if !sameOrder(order, req) {
				g.critical("client id %s resolved to a mismatched order; refusing adoption", clientID)
				continue
			}
			g.logf("client id %s resolved to broker order %s", clientID, order.id)
			return order.id, lookupOnly, nil
		}
		if lookupOnly {
			// The ID is occupied, possibly by an already terminal order absent from
			// the active list. Another send cannot help and must not spend budget.
			return "", lookupOnly, execengine2.OrderUnknown(clientID, sendErr)
		}
		if round < sendTries-1 {
			if waitErr := g.waiter.Wait(ctx, findWait); waitErr != nil {
				return "", lookupOnly, execengine2.OrderUnknown(clientID, errors.Join(sendErr, waitErr))
			}
		}
	}
	return "", lookupOnly, execengine2.OrderUnknown(clientID, sendErr)
}

func retryKind(req execengine2.OrderRequest) execengine2.LimitKind {
	switch req.Role {
	case execengine2.RoleHedge, execengine2.RoleFix, execengine2.RoleLateFill:
		return execengine2.LimitMust
	default:
		return execengine2.LimitNormal
	}
}

// Cancel отменяет заявку.
func (g *Gateway) Cancel(ctx context.Context, orderID string) (execengine2.CancelResult, error) {
	if ctx == nil {
		return execengine2.CancelResult{}, errors.New("nil context")
	}
	if orderID == "" {
		return execengine2.CancelResult{}, errors.New("empty order id")
	}
	order, err := g.api.Cancel(ctx, orderID)
	if err != nil {
		return execengine2.CancelResult{}, err
	}
	return execengine2.CancelResult{Filled: order.filled}, nil
}

// Status получает состояние заявки.
func (g *Gateway) Status(ctx context.Context, orderID string) (execengine2.OrderStatus, error) {
	if ctx == nil {
		return execengine2.OrderStatus{}, errors.New("nil context")
	}
	if orderID == "" {
		return execengine2.OrderStatus{}, errors.New("empty order id")
	}
	order, err := g.api.Status(ctx, orderID)
	if err != nil {
		return execengine2.OrderStatus{}, err
	}
	return execengine2.OrderStatus{Filled: order.filled, Done: order.done}, nil
}

func checkRequest(req execengine2.OrderRequest) error {
	if req.Symbol == "" || req.Lots <= 0 {
		return errors.New("order needs a symbol and positive lots")
	}
	if req.Side != execengine2.SideBuy && req.Side != execengine2.SideSell {
		return errors.New("bad order side")
	}
	if req.Kind != execengine2.OrderLimit && req.Kind != execengine2.OrderMarket {
		return errors.New("bad order kind")
	}
	if req.Kind == execengine2.OrderLimit && req.Price <= 0 {
		return errors.New("limit order needs a positive price")
	}
	return nil
}

func sameOrder(order apiOrder, req execengine2.OrderRequest) bool {
	if order.id == "" || order.symbol != req.Symbol || order.side != req.Side ||
		order.kind != req.Kind || order.lots != req.Lots {
		return false
	}
	return req.Kind != execengine2.OrderLimit || math.Abs(order.price-req.Price) <= 1e-6
}

type finamAPI struct {
	client *finam.Client
}

func (a *finamAPI) Place(
	ctx context.Context,
	req execengine2.OrderRequest,
	clientID string,
) (apiOrder, error) {
	ticker := finam.Ticker{Symbol: req.Symbol, Vol: req.Lots}
	var (
		state *orders.OrderState
		err   error
	)
	switch {
	case req.Kind == execengine2.OrderLimit && req.Side == execengine2.SideBuy:
		state, err = finam.PlaceLimitOrderBuyContext(ctx, a.client, ticker, req.Price, clientID)
	case req.Kind == execengine2.OrderLimit:
		state, err = finam.PlaceLimitOrderSellContext(ctx, a.client, ticker, req.Price, clientID)
	case req.Side == execengine2.SideBuy:
		state, err = finam.PlaceMarketOrderBuyContext(ctx, a.client, ticker, clientID)
	default:
		state, err = finam.PlaceMarketOrderSellContext(ctx, a.client, ticker, clientID)
	}
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func (a *finamAPI) Find(
	ctx context.Context,
	clientID string,
) (apiOrder, bool, error) {
	state, found, err := finam.FindOrderByClientIDContext(ctx, a.client, clientID)
	if err != nil || !found {
		return apiOrder{}, found, err
	}
	order, err := fromFinam(state)
	return order, err == nil, err
}

func (a *finamAPI) Cancel(ctx context.Context, orderID string) (apiOrder, error) {
	state, err := finam.CancelOrderContext(ctx, a.client, orderID)
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func (a *finamAPI) Status(ctx context.Context, orderID string) (apiOrder, error) {
	state, err := finam.GetOrderContext(ctx, a.client, orderID)
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func fromFinam(state *orders.OrderState) (apiOrder, error) {
	if state == nil || state.GetOrder() == nil {
		return apiOrder{}, errors.New("finam returned an empty order")
	}
	finamOrder := state.GetOrder()
	side := execengine2.SideSell
	if finam.SideMatches(state, true) {
		side = execengine2.SideBuy
	}
	kind := execengine2.OrderLimit
	switch finamOrder.GetType() {
	case orders.OrderType_ORDER_TYPE_LIMIT:
	case orders.OrderType_ORDER_TYPE_MARKET:
		kind = execengine2.OrderMarket
	default:
		return apiOrder{}, errors.New("finam returned an unknown order type")
	}
	return apiOrder{
		id: state.GetOrderId(), symbol: finamOrder.GetSymbol(), side: side, kind: kind,
		price: finam.ParseDecimal(finamOrder.GetLimitPrice().GetValue()),
		lots:  finam.InitialLots(state), filled: finam.ExecutedLots(state),
		done: terminalOrderStatus(state.GetStatus()),
	}, nil
}

var _ execengine2.Broker = (*Gateway)(nil)
var _ execengine2.PlacementResumer = (*Gateway)(nil)
