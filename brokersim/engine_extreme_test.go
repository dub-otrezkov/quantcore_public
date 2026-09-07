//go:build sim

package brokersim_test

// Интеграционный экстрим-харнесс гоняет движок через общий harnessEngine.
// Адаптеры execengine/execengine2 подключают настоящие движки и Finam-шлюзы
// поверх реального gRPC-клиента trade/finam против brokersim. Скриптовый
// Decider диктует входы/выходы детерминированно, а
// control-plane сима вбрасывает экстремальные последовательности сбоев. После
// каждого сценария проверяются ИНВАРИАНТЫ движка против наземной правды брокера:
//
//   - вера движка о позиции (Engine.Position / leg B net) == фактические позиции
//     брокера по обеим ногам (нет тихого расхождения);
//   - ноги сбалансированы (legA == -legB) когда движок сообщает здоровье;
//   - impaired-режим ВХОДИТ на неподтверждённых операциях и САМ ВЫХОДИТ, когда
//     брокер ответил (retireQ и debts дренированы), через unverified -> чистый
//     reconcile;
//   - suspect/unverified снимаются сами по схождении позиций.
//
// Эти пути в юнит-тестах движка покрыты фейками интерфейсов; здесь они гоняются
// против реального транспорта и реальной брокерской семантики, где расходятся
// предположения и данные.

import (
	"math"
	"os"
	"sync"
	"testing"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"

	"QuantCore/brokersim"
	"QuantCore/finambroker"
	"QuantCore/strategies/execengine"
	"QuantCore/trade/finam"
)

const (
	codesInvalidArgument = codes.InvalidArgument
	codesUnavailable     = codes.Unavailable
	ordersMarket         = orders.OrderType_ORDER_TYPE_MARKET
)

const (
	legA = "LEGA@RTSX"
	legB = "LEGB@RTSX"
)

// scriptDecider — управляемый Decider + FillSink. Позиция ведётся из кредитов
// движка (как basis-леджер): posA/posB — подписанные нетто-лоты по каждой ноге.
// Engine.Position() == posA (нога A), reconcile ждёт legB == -posA.
type scriptDecider struct {
	intent     engineIntent
	maxPos     int
	posA, posB int
}

type engineIntent struct {
	action  int
	isClose bool
	lots    int
}

func (d *scriptDecider) peek() engineIntent {
	in := d.intent
	// Уважать кап на открытии, чтобы не открывать бесконечно.
	if in.action != 0 && !in.isClose && abs(d.posA) >= d.maxPos {
		return engineIntent{}
	}
	return in
}

// Updates: движок кредитует обе ноги по мере действий.
func (d *scriptDecider) apply(symbol string, lots int) {
	switch symbol {
	case legA:
		d.posA += lots
	case legB:
		d.posB += lots
	}
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// engineHarness гоняет реальный Engine в одной горутине event-loop'а; ВСЕ вызовы
// e.* сериализованы h.mu (движок не потокобезопасен). Тест управляет интентом и
// читает снимок через h.mu.
type engineHarness struct {
	t            *testing.T
	srv          *brokersim.Server
	client       *finam.Client
	e            harnessEngine
	dm           *scriptDecider
	setQuota     func(int, time.Time) // nil, если лимитер не установлен
	raceHits     int                  // сколько раз отмена реально обогнала fill-событие (под mu)
	tickInterval time.Duration        // период quiet-market тика event-loop'а
	activeAction int                  // направление текущего клипа для общего StopTrade
	mu           sync.Mutex
	stop         chan struct{}
	done         chan struct{}
}

// raceCancelVsFill детерминированно воспроизводит историческую гонку cancel/fill:
// печатает фил (мейкер исполняется у брокера, событие ещё в пути по стриму), затем
// НЕМЕДЛЕННО под mu отменяет клип, пока движок ещё думает makerFilled==0. Отмена
// обязана поймать in-flight фил через Cancel->Status и захеджить — не бросить ногу.
// Считает raceHits, когда окно гонки было реально открыто (позиция ещё не сдвинулась).
func (h *engineHarness) raceCancelVsFill(price float64, lots int, buyPrint bool) {
	before := h.snap()
	h.publicPrint(legA, price, float64(lots), buyPrint) // брокер исполняет мейкера; trade-событие async
	h.mu.Lock()
	if h.e.Snapshot().working && h.dm.posA == before.pos {
		h.raceHits++ // fill-событие ещё не обработано -> отмена его обгоняет
	}
	h.dm.intent = engineIntent{} // hold
	h.e.StopTrade()
	h.mu.Unlock()
}

// harnessBuild — параметры per-test конфигурации движка/харнесса.
type harnessBuild struct {
	ec            execengine.EngineConfig
	maxPos        int
	limiterQuota  int           // >0 → установить QuotaLimiter с этим margin
	limiterBudget int           // >0 → fail-safe NewQuotaLimiterBudget с этим бюджетом окна
	limiterWindow time.Duration // окно fail-safe бюджета
	refreshQuota  bool          // запустить RefreshQuota (питает лимитер из GetUsageMetrics сима)
	tick          time.Duration // период quiet-market тика; 0 → 150ms
}

type harnessOpt func(*harnessBuild)

func withTick(d time.Duration) harnessOpt { return func(b *harnessBuild) { b.tick = d } }

func withOrderVol(n int) harnessOpt { return func(b *harnessBuild) { b.ec.OrderVol = n } }
func withMaxPos(n int) harnessOpt   { return func(b *harnessBuild) { b.maxPos = n } }
func withRepeg() harnessOpt         { return func(b *harnessBuild) { b.ec.DisableRepeg = false } }
func withFillTimeout(d time.Duration) harnessOpt {
	return func(b *harnessBuild) { b.ec.FillTimeout = d }
}
func withStaleBook(d time.Duration) harnessOpt {
	return func(b *harnessBuild) { b.ec.PullOnStaleBook = true; b.ec.MaxStaleness = d }
}
func withRepegThrottle(d time.Duration) harnessOpt {
	return func(b *harnessBuild) { b.ec.DisableRepeg = false; b.ec.RepegThrottle = d }
}
func withLimiter(margin int) harnessOpt { return func(b *harnessBuild) { b.limiterQuota = margin } }

// withQuotaRefresh ставит лимитер И запускает RefreshQuota (как прод): движок
// сам резервирует headroom под хеджи, опрашивая реальную квоту сима.
func withQuotaRefresh(margin int) harnessOpt {
	return func(b *harnessBuild) { b.limiterQuota = margin; b.refreshQuota = true }
}

// withQuotaBudget ставит fail-safe бюджетный лимитер БЕЗ RefreshQuota — проверка,
// что самоуправляемый бюджет сам защищает хеджи (фикс fail-open).
func withQuotaBudget(margin, budget int, window time.Duration) harnessOpt {
	return func(b *harnessBuild) { b.limiterQuota = margin; b.limiterBudget = budget; b.limiterWindow = window }
}

func newEngineHarness(t *testing.T, cfg brokersim.Config, opts ...harnessOpt) *engineHarness {
	return newEngineHarnessWith(t, cfg, current{}, opts...)
}

func newEngineHarnessWith(
	t *testing.T,
	cfg brokersim.Config,
	factory harnessEngineFactory,
	opts ...harnessOpt,
) *engineHarness {
	t.Helper()
	if len(cfg.Accounts) == 0 {
		cfg.Accounts = []brokersim.AccountConfig{{Secret: testSecret, AccountID: testAccount, InitialCash: 5_000_000}}
	}
	if len(cfg.Symbols) == 0 {
		cfg.Symbols = []brokersim.SymbolConfig{
			{Symbol: legA, MinStep: 1},
			{Symbol: legB, MinStep: 0.001, Decimals: 3},
		}
	}
	srv, err := brokersim.Start(cfg, "127.0.0.1:0", "")
	if err != nil {
		t.Fatalf("start sim: %v", err)
	}
	t.Cleanup(srv.Close)
	t.Setenv(finam.EnvAddr, srv.Addr())
	client, err := finam.NewClient(finam.Config{Secret: testSecret, AccountID: testAccount})
	if err != nil {
		t.Fatalf("finam.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	b := &harnessBuild{
		maxPos: 1,
		ec: execengine.EngineConfig{
			LegA: legA, LegB: legB, OrderVol: 1,
			HedgeRetries:             3,
			ReconcileInterval:        time.Second,
			PlaceRetryBackoff:        300 * time.Millisecond,
			DisableRepeg:             true,
			ForceCloseOnTimeout:      true,
			KeepPartialOpenOnTimeout: true,
			TakerConfirmTimeout:      2 * time.Second,
			LogTag:                   "[extreme]",
		},
	}
	for _, o := range opts {
		o(b)
	}

	dm := &scriptDecider{maxPos: b.maxPos}
	h := &engineHarness{
		t: t, srv: srv, client: client, dm: dm,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	h.e, h.setQuota = factory.Build(t, client, dm, *b, h.stop)
	h.tickInterval = b.tick
	if h.tickInterval == 0 {
		h.tickInterval = 150 * time.Millisecond
	}
	go h.run()
	t.Cleanup(func() { close(h.stop); <-h.done })
	return h
}

// run — единственная горутина, трогающая движок (как event-loop раннера).
func (h *engineHarness) run() {
	defer close(h.done)
	datedBook, _ := finam.SubscribeFullOrderBook(h.client, finam.Ticker{Symbol: legA, Vol: 1})
	perpBook, _ := finam.SubscribeFullOrderBook(h.client, finam.Ticker{Symbol: legB, Vol: 1})
	fills, _ := finam.SubscribeTrades(h.client)
	orderStates, _ := finam.SubscribeOrders(h.client)

	tick := time.NewTicker(h.tickInterval)
	defer tick.Stop()
	recTick := time.NewTicker(time.Second)
	defer recTick.Stop()

	const reconcileGrace = 700 * time.Millisecond
	var lastFill time.Time

	// manageLocked воспроизводит basis-раннер через общий контракт: тик обслуживает
	// таймауты/восстановление, новый сигнал открывает клип, а смена направления
	// снимает его через StopTrade. Вызывать под mu.
	manageLocked := func(ts time.Time) {
		h.e.OnTick(ts)
		if h.e.Snapshot().working {
			if wanted := h.dm.peek(); wanted.action != h.activeAction {
				h.e.StopTrade()
				if !h.e.Snapshot().working {
					h.activeAction = 0
				}
			}
			return
		}
		h.activeAction = 0
		plan := h.dm.peek()
		h.e.OnSignal(ts)
		if h.e.Snapshot().working {
			h.activeAction = plan.action
		}
	}

	feedBook := func(sym string, b finam.FullOrderBookData) {
		if len(b.Bids) == 0 || len(b.Asks) == 0 {
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.e.OnBook(sym, b.Timestamp, b.Bids[0].Price, b.Asks[0].Price)
		manageLocked(b.Timestamp)
	}

	for {
		select {
		case <-h.stop:
			return
		case b, ok := <-datedBook:
			if ok {
				feedBook(legA, b)
			}
		case b, ok := <-perpBook:
			if ok {
				feedBook(legB, b)
			}
		case f, ok := <-fills:
			if ok {
				h.mu.Lock()
				h.e.OnFill(
					f.GetTradeId(), f.GetOrderId(), f.GetSymbol(),
					f.GetSide() == v1.Side_SIDE_BUY,
					int(finam.ParseDecimal(f.GetSize().GetValue())),
					finam.ParseDecimal(f.GetPrice().GetValue()), time.Now().UTC(),
				)
				h.mu.Unlock()
				lastFill = time.Now()
			}
		case o, ok := <-orderStates:
			if ok && o != nil {
				h.mu.Lock()
				h.e.OnOrderStatus(
					o.GetOrderId(), finam.ExecutedLots(o), finambroker.IsDeadStatus(o.GetStatus()),
				)
				h.mu.Unlock()
			}
		case now := <-tick.C:
			// Статичная книга (без авторынка) не шлёт book-события, поэтому тик —
			// «quiet-market floor»: гонит те же OnTick/OnState/PullEntry, что и
			// book-события, чтобы скриптовый интент реализовался и клип управлялся.
			h.mu.Lock()
			manageLocked(now.UTC())
			h.mu.Unlock()
		case <-recTick.C:
			h.mu.Lock()
			snap := h.e.Snapshot()
			idle := !snap.working && snap.state != engineStopped &&
				snap.state != engineRecovering && time.Since(lastFill) > reconcileGrace
			h.mu.Unlock()
			if idle {
				// fetch без блокировки движка, затем Reconcile под mu
				a, _, ea := finam.GetPosition(h.client, legA)
				b, _, eb := finam.GetPosition(h.client, legB)
				if ea == nil && eb == nil {
					h.mu.Lock()
					h.e.Reconcile(int(math.Round(a.Quantity)), int(math.Round(b.Quantity)))
					h.mu.Unlock()
				}
			}
		}
	}
}

// ---- управление и снимки ----

func (h *engineHarness) setIntent(action int, isClose bool, lots int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dm.intent = engineIntent{action: action, isClose: isClose, lots: lots}
}

func (h *engineHarness) hold() { h.setIntent(0, false, 0) }

func (h *engineHarness) setMaxPos(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dm.maxPos = n
}

// halt дёргает оператор-kill-switch движка (под mu).
func (h *engineHarness) halt() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.e.Stop("test")
}

// quotaSet выставляет остаток квоты установленного лимитера (для теста гейтинга) —
// имитирует ответ RefreshQuota: token снят ДО "RPC", now — после.
func (h *engineHarness) quotaSet(remaining int, resetAt time.Time) {
	if h.setQuota == nil {
		h.t.Fatal("quotaSet: лимитер не установлен (withLimiter)")
	}
	h.setQuota(remaining, resetAt)
}

// placeForeignLimit ставит лимитник НАПРЯМУЮ через клиент (минуя движок), так что
// его id отсутствует в Engine.own — чужой ордер на общем счёте. Возвращает id.
func (h *engineHarness) placeForeignLimit(sym string, buy bool, price float64) string {
	h.t.Helper()
	tk := finam.Ticker{Symbol: sym, Vol: 1}
	var st interface{ GetOrderId() string }
	var err error
	if buy {
		st, err = finam.PlaceLimitOrderBuy(h.client, tk, price, "foreign-"+sym[:3])
	} else {
		st, err = finam.PlaceLimitOrderSell(h.client, tk, price, "foreign-"+sym[:3])
	}
	if err != nil {
		h.t.Fatalf("placeForeignLimit: %v", err)
	}
	return st.GetOrderId()
}

// injectFill вбрасывает фабрикованный филл на ордер (брокер противоречит ack).
func (h *engineHarness) injectFill(orderID string, lots, price float64) {
	if err := h.srv.Sim.InjectFill(orderID, lots, price); err != nil {
		h.t.Fatalf("InjectFill: %v", err)
	}
}

type engineSnap struct {
	pos, posB         int
	working, impaired bool
	suspect, halted   bool
	label             string
}

func (h *engineHarness) snap() engineSnap {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := h.e.Snapshot()
	label := ""
	switch snap.state {
	case engineRecovering:
		label = "impaired"
	case engineCheckNeeded:
		label = "suspect"
	case engineStopped:
		label = "halted"
	}
	return engineSnap{
		pos: snap.position, posB: h.dm.posB,
		working: snap.working, impaired: snap.state == engineRecovering,
		suspect: snap.state == engineCheckNeeded,
		halted:  snap.state == engineStopped,
		label:   label,
	}
}

// brokerPos читает фактические позиции брокера по обеим ногам.
func (h *engineHarness) brokerPos() (int, int) {
	h.t.Helper()
	a, _, err := finam.GetPosition(h.client, legA)
	if err != nil {
		h.t.Fatalf("GetPosition %s: %v", legA, err)
	}
	b, _, err := finam.GetPosition(h.client, legB)
	if err != nil {
		h.t.Fatalf("GetPosition %s: %v", legB, err)
	}
	return int(math.Round(a.Quantity)), int(math.Round(b.Quantity))
}

func (h *engineHarness) setBook(sym string, bid, bidVol, ask, askVol float64) {
	if err := h.srv.Sim.SetBook(sym, []brokersim.Level{{Price: bid, Size: bidVol}}, []brokersim.Level{{Price: ask, Size: askVol}}); err != nil {
		h.t.Fatalf("SetBook %s: %v", sym, err)
	}
}

// setBookRaw задаёт книгу целиком (в т.ч. односторонюю/пустую). Односторонний
// апдейт клиент отбрасывает (feedBook требует обе стороны), поэтому touch движка
// замирает валидным, а внутренняя книга сима — та, что задали (для тонкой
// ликвидности / пустой стороны, о которую тейкер реджектится).
func (h *engineHarness) setBookRaw(sym string, bids, asks []brokersim.Level) {
	if err := h.srv.Sim.SetBook(sym, bids, asks); err != nil {
		h.t.Fatalf("SetBook %s: %v", sym, err)
	}
}

// silenceTrades глушит стрим сделок: филлы исполняются у брокера, но событие не
// доходит до клиента (движок их не видит).
func (h *engineHarness) silenceTrades() {
	h.fault(brokersim.Fault{Method: "SubscribeTrades", Action: "silence", Count: -1})
}

// replaySilencedTrades снимает все сбои и обрывает стрим сделок -> реконнект ->
// реплей ранее заглушённых сделок (свежих для харнесс-дедупа TradeDedup).
func (h *engineHarness) replaySilencedTrades() {
	h.clearFaults()
	h.fault(brokersim.Fault{Method: "SubscribeTrades", Action: "kill_stream", Count: 1})
	time.Sleep(2500 * time.Millisecond) // реконнект (~1s) + реплей
}

// publicPrint печатает публичную сделку — задевает стоящий пассив с кроссящейся ценой.
func (h *engineHarness) publicPrint(sym string, price, size float64, buy bool) {
	if err := h.srv.Sim.PublicTrade(sym, price, size, buy); err != nil {
		h.t.Fatalf("PublicTrade %s: %v", sym, err)
	}
}

func (h *engineHarness) fault(f brokersim.Fault) {
	if _, err := h.srv.Sim.AddFault(f); err != nil {
		h.t.Fatalf("AddFault: %v", err)
	}
}
func (h *engineHarness) clearFaults() { h.srv.Sim.RemoveFault(-1) }

// waitFor поллит cond до дедлайна, печатая последний снимок при таймауте.
func (h *engineHarness) waitFor(what string, timeout time.Duration, cond func(engineSnap) bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var s engineSnap
	for time.Now().Before(deadline) {
		s = h.snap()
		if cond(s) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	ba, bb := h.brokerPos()
	h.t.Fatalf("timeout waiting for %s; engine=%+v broker legA=%d legB=%d", what, s, ba, bb)
}

// assertConverged — центральный инвариант: за timeout движок приходит в
// здоровое согласованное состояние — не impaired, не working, не suspect, его
// вера о позиции совпадает с брокером, ноги сбалансированы (legA == -legB).
func (h *engineHarness) assertConverged(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := h.snap()
		ba, bb := h.brokerPos()
		if !s.working && !s.impaired && !s.suspect && !s.halted &&
			s.pos == ba && s.posB == bb && ba == -bb {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s := h.snap()
	ba, bb := h.brokerPos()
	h.t.Fatalf("did not converge: engine=%+v broker legA=%d legB=%d (want engine.pos==legA==-legB, posB==legB, healthy)", s, ba, bb)
}

// driveEntry прогоняет открывающий клип на lots до исполнения ноги A: интент,
// дождаться стоящего мейкера, печать по его цене -> мейкер-фил -> тейкер-хедж.
func (h *engineHarness) driveEntry(dir, lots int) {
	h.t.Helper()
	h.setIntent(dir, false, lots)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// мейкер ноги A стоит на своей стороне тача (bid для лонга, ask для шорта).
	// Печать, кроссящая его на весь размер, исполняет мейкера.
	if dir > 0 {
		h.publicPrint(legA, 100, float64(lots), false) // sell hits the resting bid@100
	} else {
		h.publicPrint(legA, 102, float64(lots), true) // buy hits the resting ask@102
	}
}

// TestEngineHappyPath — валидация wiring: чистый вход в лонг (мейкер-фил +
// тейкер-хедж), затем выход, оба против реального сима. Инварианты сходятся.
func TestEngineHappyPath(t *testing.T) {
	for _, factory := range []harnessEngineFactory{v1HarnessEngine{}, v2HarnessEngine{}} {
		t.Run(factory.Name(), func(t *testing.T) { testEngineHappyPath(t, factory) })
	}
}

func testEngineHappyPath(t *testing.T, factory harnessEngineFactory) {
	h := newEngineHarnessWith(t, brokersim.Config{}, factory)
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.driveEntry(+1, 1)
	h.waitFor("position +1", 5*time.Second, func(s engineSnap) bool { return s.pos == 1 })
	h.hold()
	h.assertConverged(8 * time.Second)
	if ba, bb := h.brokerPos(); ba != 1 || bb != -1 {
		t.Fatalf("after entry broker legA=%d legB=%d, want +1/-1", ba, bb)
	}

	// Выход: закрывающий клип (ForceCloseOnTimeout гарантирует редукцию).
	h.setIntent(-1, true, 1)
	h.waitFor("clip working (close)", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.publicPrint(legA, 102, 1, true) // buy hits the resting ask (closing a long sells... dir=-1 -> ask on legA)
	h.waitFor("flat", 6*time.Second, func(s engineSnap) bool { return s.pos == 0 })
	h.hold()
	h.assertConverged(8 * time.Second)
}

// repairToBelief — оператор приводит позиции брокера к внутренней вере движка
// (posA/posB) через control-plane. Единственный законный ремонт: на сомнительной
// позиции движок не угадывает, чинит человек/оператор.
func (h *engineHarness) repairToBelief() {
	s := h.snap()
	if err := h.srv.Sim.SetPosition(testAccount, legA, float64(s.pos), 100); err != nil {
		h.t.Fatalf("repair legA: %v", err)
	}
	if err := h.srv.Sim.SetPosition(testAccount, legB, float64(s.posB), 50); err != nil {
		h.t.Fatalf("repair legB: %v", err)
	}
}

// repairToBalanced — оператор приводит брокера к 1:1 вокруг СПРЕД-позиции движка
// (legA=pos, legB=-pos), а не к сырой вере posB (которая после докредита сверх
// терминала сама может быть не-1:1). Это цель reconcile: legB == -pos.
func (h *engineHarness) repairToBalanced() {
	s := h.snap()
	if err := h.srv.Sim.SetPosition(testAccount, legA, float64(s.pos), 100); err != nil {
		h.t.Fatalf("repair legA: %v", err)
	}
	if err := h.srv.Sim.SetPosition(testAccount, legB, float64(-s.pos), 50); err != nil {
		h.t.Fatalf("repair legB: %v", err)
	}
}

// assertHealthyBalanced ждёт, пока движок станет здоров и его АВТОРИТЕТНАЯ
// спред-позиция (Position) сойдётся с брокером 1:1 (legA==pos, legB==-pos).
// posB (вспомогательный учёт теста) намеренно НЕ проверяется — после докредита
// сверх терминала он может расходиться с спред-видом движка.
func (h *engineHarness) assertHealthyBalanced(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := h.snap()
		ba, bb := h.brokerPos()
		if !s.working && !s.impaired && !s.suspect && !s.halted && s.pos == ba && ba == -bb {
			return
		}
		if !s.working && !s.impaired && !s.halted {
			h.repairToBalanced()
		}
		time.Sleep(200 * time.Millisecond)
	}
	s := h.snap()
	ba, bb := h.brokerPos()
	h.t.Fatalf("не сошёлся к здоровому 1:1: engine=%+v broker legA=%d legB=%d", s, ba, bb)
}

// assertNoSilentDivergence — слабый, но КЛЮЧЕВОЙ инвариант: если движок сообщает
// здоровье (не impaired/suspect/working/halted), его вера о позиции обязана
// совпадать с брокером и ноги обязаны быть сбалансированы. Молчаливое
// расхождение (здоров, но belief != broker или ноги не 1:1) — дефект.
func (h *engineHarness) assertNoSilentDivergence() {
	h.t.Helper()
	s := h.snap()
	if s.working || s.impaired || s.suspect || s.halted {
		return // движок явно флагует незавершённость — это законно
	}
	ba, bb := h.brokerPos()
	if s.pos != ba || s.posB != bb || ba != -bb {
		h.t.Fatalf("SILENT DIVERGENCE: движок здоров (%+v), но belief pos=%d/posB=%d vs broker legA=%d legB=%d (не 1:1 или не совпало)", s, s.pos, s.posB, ba, bb)
	}
}

// A. Чистый отказ хеджа (брокер отвечает reject) -> retry -> hedge-debt ->
// impaired -> САМ выходит по снятии сбоя. Рынок НЕ исполнил тейкер (ответ был),
// поэтому оверхеджа нет: debt дренируется, ноги 1:1.
func TestEngineHedgeRejectThenImpairedRecovery(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Отклонять КАЖДУЮ постановку (в т.ч. все ретраи хеджа) чистой ошибкой — но
	// клип уже стоит, так что бьёт по тейкеру, не по мейкеру.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "error", Code: codesInvalidArgument, Count: -1})
	h.publicPrint(legA, 100, 1, false) // fill legA maker -> engine tries to hedge legB -> rejected x HedgeRetries
	h.waitFor("impaired (hedge debt)", 6*time.Second, func(s engineSnap) bool { return s.impaired })
	h.clearFaults()
	h.hold()
	// Долг дренируется, тейкер ставится, ноги 1:1, unverified снимается чистым reconcile.
	h.assertConverged(12 * time.Second)
	if ba, bb := h.brokerPos(); ba != 1 || bb != -1 {
		t.Fatalf("after recovery broker legA=%d legB=%d, want +1/-1", ba, bb)
	}
}

// B. Потерянный ответ ТЕЙКЕРА (drop_after_apply): маркет-хедж исполняется дважды
// (первый — исполнился+ответ потерян, placer перехеджировал) -> брокер
// оверхеджирован -> reconcile ФЛАГУЕТ (suspect), движок не торгует на
// сомнительной позиции. Оператор чинит -> resume. Инвариант: НЕ молчит.
// Этот регрессионный контракт относится к execengine v1.
//
// Таймаут 25s (не 10s): placeResolved теперь ретраит ТЕМ ЖЕ client_order_id до
// cidRetryRounds раз (2026-08-25, ревью quantcore_public#3 #1) прежде чем
// сдаться — для мгновенно исполняющегося маркет-ордера (терминален сразу,
// GetOrders его никогда не увидит) это НЕ спасает от оверхеджа, только
// откладывает его: каждый раунд — реальная попытка постановки тем же cid
// (коллизия -> AlreadyExists) плюс ghostProbes проб по ghostProbeGap. Только
// после исчерпания всех раундов внешний retry-с-новым-cid (через deferHedge)
// создаёт второй настоящий ордер, который reconcile и ловит — тем же путём,
// что и раньше, просто на ~10s позже.
func TestEngineLostHedgeResponseFlagsSuspect(t *testing.T) {
	h := newEngineHarnessWith(t, brokersim.Config{}, v1HarnessEngine{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Один потерянный ответ на постановку тейкера: маркет исполнится и исчезнет,
	// placer «усыновить» тейкер не сможет (терминальный) даже после cid-ретраев,
	// внешний retry с новым cid — оверхедж.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "drop_after_apply", Count: 1})
	h.publicPrint(legA, 100, 1, false)
	// Движок обязан заметить оверхедж и уйти в suspect (двухпроходный reconcile),
	// а НЕ продолжить как ни в чём не бывало.
	h.waitFor("suspect (over-hedge flagged)", 25*time.Second, func(s engineSnap) bool { return s.suspect })
	h.assertNoSilentDivergence()
	// Оператор приводит брокера к вере движка -> reconcile сходится -> resume.
	h.hold()
	h.repairToBelief()
	h.assertConverged(12 * time.Second)
}

// B (execengine2). A lost market response must recover from the full inventory snapshot even
// when the executed order has disappeared from the active list. Recovery must
// neither duplicate the hedge nor require an operator to repair positions.
func TestEngineLostHedgeResponseRecoversWithoutDuplicate(t *testing.T) {
	h := newEngineHarnessWith(t, brokersim.Config{}, v2HarnessEngine{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.hold() // only this clip may execute while recovery is being observed
	// The market order executes, but neither its response nor an active-list
	// lookup supplies an order ID to the engine.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "drop_after_apply", Count: 1})
	h.publicPrint(legA, 100, 1, false)
	h.waitFor("hedge credited", 25*time.Second, func(s engineSnap) bool { return s.pos == 1 && s.posB == -1 })
	h.assertNoSilentDivergence()
	h.assertConverged(12 * time.Second)
	if a, b := h.brokerPos(); a != 1 || b != -1 {
		t.Fatalf("lost response duplicated the hedge: positions=(%d,%d)", a, b)
	}
}

// C. Потерянная отмена (cancel+status недоступны) -> deferRetire -> retireQ ->
// impaired -> дренируется по снятии сбоя -> флэт.
func TestEngineLostCancelThenImpairedRecovery(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// И отмена, и статус-фолбэк недоступны -> движок не может подтвердить снятие
	// -> deferRetire -> impaired (retireQ). Ордер при этом НЕ исполнен.
	h.fault(brokersim.Fault{Method: "CancelOrder", Action: "error", Count: -1})
	h.fault(brokersim.Fault{Method: "GetOrder", Action: "error", Count: -1})
	h.hold() // снять желание -> движок отменяет клип
	h.waitFor("impaired (retireQ)", 8*time.Second, func(s engineSnap) bool { return s.impaired })
	h.clearFaults()
	// retireQ дренируется (отмена/статус теперь отвечают), клип снят, флэт.
	h.assertConverged(12 * time.Second)
	if ba, bb := h.brokerPos(); ba != 0 || bb != 0 {
		t.Fatalf("after recovery broker legA=%d legB=%d, want flat", ba, bb)
	}
}

// D. Расхождение позиций (фантом на счёте, не от движка) -> двухпроходный
// reconcile -> suspect -> снятие фантома -> автовозобновление. Флэтовый движок.
func TestEngineReconcileDivergenceRecovers(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)
	h.hold()
	h.assertConverged(5 * time.Second) // старт: флэт, здоров

	// Фантомная позиция на счёте, о которой движок не знает.
	if err := h.srv.Sim.SetPosition(testAccount, legA, 3, 100); err != nil {
		t.Fatal(err)
	}
	h.waitFor("suspect (phantom)", 8*time.Second, func(s engineSnap) bool { return s.suspect })
	// Снять фантом -> позиции сходятся -> движок сам возобновляется.
	if err := h.srv.Sim.SetPosition(testAccount, legA, 0, 0); err != nil {
		t.Fatal(err)
	}
	h.assertConverged(10 * time.Second)
}

// E. Шторм обрывов стримов во время работающего клипа: движок сохраняет учёт —
// фил долетает после реконнекта, хедж ставится, ноги 1:1.
func TestEngineStreamKillStormDuringClip(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Рвём все три стрима по разу — обёртки клиента переподключатся.
	h.fault(brokersim.Fault{Method: "SubscribeOrders", Action: "kill_stream", Count: 1})
	h.fault(brokersim.Fault{Method: "SubscribeTrades", Action: "kill_stream", Count: 1})
	h.fault(brokersim.Fault{Method: "SubscribeOrderBook", Action: "kill_stream", Count: 2})
	time.Sleep(1500 * time.Millisecond) // дать реконнектам произойти
	h.publicPrint(legA, 100, 1, false)  // фил мейкера во время/после шторма
	h.waitFor("position +1", 8*time.Second, func(s engineSnap) bool { return s.pos == 1 })
	h.clearFaults()
	h.hold()
	h.assertConverged(12 * time.Second)
	if ba, bb := h.brokerPos(); ba != 1 || bb != -1 {
		t.Fatalf("after storm broker legA=%d legB=%d, want +1/-1", ba, bb)
	}
}

// assertConvergesWithRepair ждёт полной согласованности, ПЕРИОДИЧЕСКИ применяя
// оператор-ремонт (repairToBelief), когда движок устаканился, но всё ещё
// сомневается/расходится (оверхедж от потерянного ответа не самозаживает —
// брокера к вере движка приводит оператор). Устойчиво к таймингу двухпроходного
// reconcile: не зависит от точного момента появления suspect.
func (h *engineHarness) assertConvergesWithRepair(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := h.snap()
		ba, bb := h.brokerPos()
		if !s.working && !s.impaired && !s.suspect && !s.halted &&
			s.pos == ba && s.posB == bb && ba == -bb {
			return
		}
		// Устаканился (не impaired/working/halted), но не согласован -> оператор
		// приводит брокера к вере движка; следующий reconcile сойдётся.
		if !s.working && !s.impaired && !s.halted {
			h.repairToBelief()
		}
		time.Sleep(200 * time.Millisecond)
	}
	s := h.snap()
	ba, bb := h.brokerPos()
	h.t.Fatalf("did not converge with repair: engine=%+v broker legA=%d legB=%d", s, ba, bb)
}

// waitSettledSoft поллит до !impaired && !working (лучшая попытка, без Fatal).
func (h *engineHarness) waitSettledSoft(timeout time.Duration) engineSnap {
	deadline := time.Now().Add(timeout)
	var s engineSnap
	for time.Now().Before(deadline) {
		s = h.snap()
		if !s.impaired && !s.working {
			return s
		}
		time.Sleep(30 * time.Millisecond)
	}
	return s
}

// waitWorkingSoft ждёт открытия клипа (лучшая попытка); false если не открылся.
func (h *engineHarness) waitWorkingSoft(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.snap().working {
			return true
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

// F. Комбинированный шторм: одновременно потерянный ответ хеджа + недоступная
// отмена + обрыв стрима сделок во время работающего клипа. Движок обязан прийти
// в согласованное состояние (сойтись ЛИБО флагнуть -> ремонт -> сойтись),
// НИКОГДА не расходясь молча.
func TestEngineCombinedStorm(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Три беды сразу под исполнение клипа.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "drop_after_apply", Count: 1})
	h.fault(brokersim.Fault{Method: "CancelOrder", Action: "error", Count: 2})
	h.fault(brokersim.Fault{Method: "SubscribeTrades", Action: "kill_stream", Count: 1})
	h.publicPrint(legA, 100, 1, false)

	// Дать буре отгреметь, затем всё снять и свести к согласованности —
	// оператор-ремонтом, если оверхедж/сомнение всплыли (устойчиво к таймингу
	// двухпроходного reconcile).
	time.Sleep(2 * time.Second)
	h.clearFaults()
	h.hold()
	h.assertConvergesWithRepair(20 * time.Second)
}

// G. Soak-фаззер: N циклов вход/выход под случайными быстрыми сбоями. На КАЖДОЙ
// границе цикла (сбои сняты, движок устаканился, при сомнении — ремонт)
// проверяется инвариант «нет молчаливого расхождения». Детерминированный seed —
// падение воспроизводимо. Ловит НЕизвестные экстремальные комбинации, которые
// точечные сценарии не перечисляют.
func TestEngineFaultStormSoak(t *testing.T) {
	// Opt-in: soak тяжёлый (реконнекты стримов + устаканивание impaired на цикл),
	// поэтому дефолтный `go test ./...` его пропускает. Включить: BROKERSIM_SOAK=1.
	if os.Getenv("BROKERSIM_SOAK") == "" {
		t.Skip("soak fuzz: задайте BROKERSIM_SOAK=1 для запуска")
	}
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	// Только быстрые сбои (drop_after_apply исключён — его 3s-проба замедлила бы
	// цикл; потерянный ответ покрыт B (v1/v2) и F). Псевдослучайно, но
	// детерминированно: индекс шага задаёт выбор.
	methods := []string{"PlaceOrder", "CancelOrder", "GetOrder", "GetOrders", "SubscribeTrades", "SubscribeOrders", "SubscribeOrderBook"}
	actions := []string{"error", "delay", "kill_stream"}
	const cycles = 10

	for i := 0; i < cycles; i++ {
		m := methods[(i*3+1)%len(methods)]
		a := actions[(i*5+2)%len(actions)]
		f := brokersim.Fault{Method: m, Action: a, Count: 1 + i%2, Code: codesUnavailable}
		if a == "delay" {
			f.Delay = brokersim.Duration(250 * time.Millisecond)
		}
		h.fault(f)

		// Лучшая попытка вход: если клип открылся — исполнить мейкера.
		h.setIntent(+1, false, 1)
		if h.waitWorkingSoft(3 * time.Second) {
			h.publicPrint(legA, 100, 1, false)
		}
		time.Sleep(400 * time.Millisecond)
		// Лучшая попытка выход.
		h.setIntent(-1, true, 1)
		if h.waitWorkingSoft(3 * time.Second) {
			h.publicPrint(legA, 102, 1, true)
		}
		time.Sleep(400 * time.Millisecond)

		// Граница: снять сбои, устаканиться, при сомнении починить, проверить инвариант.
		h.clearFaults()
		h.hold()
		s := h.waitSettledSoft(12 * time.Second)
		if s.suspect {
			h.repairToBelief()
			h.waitSettledSoft(12 * time.Second)
		}
		h.assertNoSilentDivergence()
	}

	// Финал: снять всё и свести к полной согласованности (сильнее, чем
	// поцикловый инвариант) — ремонт-до-схождения.
	h.clearFaults()
	h.hold()
	h.assertConvergesWithRepair(20 * time.Second)
}

// ============================================================================
// Дополнительные экстремальные/опасные сценарии (дизайн: adversarial workflow).
// Каждый бьёт по конкретной ветке движка, где расходятся ПРЕДПОЛОЖЕНИЯ и ДАННЫЕ.
// ============================================================================

// #3. Мейкер исполнился, но его fill-событие потеряно; фил выучен ТОЛЬКО из
// cancel-ack при сносе клипа. Реплей при реконнекте не должен посчитать его
// дважды (рутинная гонка cancel/fill 2026-07-15).
func TestCancelAckLearnedFillThenReplayDedup(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.silenceTrades()
	h.publicPrint(legA, 100, 1, false) // legA мейкер исполняется +1 у брокера, событие заглушено
	h.hold()                           // PullIfUnwanted -> CancelClip: retireOrder(legA) учит executed=1 из Status, хеджит legB
	h.waitFor("hedged via cancel-ack", 6*time.Second, func(s engineSnap) bool { return s.pos == 1 && s.posB == -1 })
	h.replaySilencedTrades() // реконнект реплеит заглушённый legA-фил (свежий для дедупа)
	h.assertConverged(10 * time.Second)
	if ba, bb := h.brokerPos(); ba != 1 || bb != -1 {
		t.Fatalf("broker legA=%d legB=%d, want +1/-1 (реплей не должен удвоить)", ba, bb)
	}
}

// #9. Halt() — оператор-kill-switch: фил на всё ещё стоящую ногу ПОСЛЕ halt
// должен быть залогирован CRITICAL и оставлен оператору, НЕ захеджирован.
func TestFillAfterHaltIsNotHedged(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Отмена не подтвердится -> мейкер остаётся стоять у брокера (retireOrder
	// отложит; enterImpaired подавлен halted).
	h.fault(brokersim.Fault{Method: "CancelOrder", Action: "error", Count: -1})
	h.fault(brokersim.Fault{Method: "GetOrder", Action: "error", Count: -1})
	h.halt()
	h.publicPrint(legA, 100, 1, false) // фил стоящего legA-мейкера у брокера
	time.Sleep(2 * time.Second)
	h.clearFaults()

	s := h.snap()
	if !s.halted {
		t.Fatalf("движок обязан остаться halted, snap=%+v", s)
	}
	ba, bb := h.brokerPos()
	if bb != 0 {
		t.Fatalf("legB=%d, want 0 — тейкер НЕ должен ставиться под halt", bb)
	}
	if ba != 1 || s.pos != 1 || s.posB != 0 {
		t.Fatalf("belief pos=%d posB=%d vs broker legA=%d legB=%d — вера должна совпадать per-leg (нога голая, но явно под halt)", s.pos, s.posB, ba, bb)
	}
	h.assertNoSilentDivergence() // halted -> ранний выход, голая нога легитимна
}

// #10. Halt поверх impaired с неоплаченным hedge-debt: долг-тейкер обязан быть
// ЗАМОРОЖЕН kill-switch'ом, даже когда постановка снова работает.
func TestHaltOverImpairedFreezesHedgeDebt(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Сбой ставим ПОСЛЕ открытия клипа: мейкеры уже стоят, бьёт по тейкер-хеджу.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "error", Count: -1})
	h.publicPrint(legA, 100, 1, false) // мейкер фил -> хедж падает HedgeRetries раз -> deferHedge -> impaired
	h.waitFor("impaired (hedge debt)", 8*time.Second, func(s engineSnap) bool { return s.impaired })
	h.halt()
	h.clearFaults() // постановка снова работала бы — но долг заморожен
	time.Sleep(3 * time.Second)

	s := h.snap()
	if s.label != "halted" {
		t.Fatalf("label=%q, want halted (долг заморожен, не impaired)", s.label)
	}
	ba, bb := h.brokerPos()
	if bb != 0 {
		t.Fatalf("legB=%d, want 0 — долг-тейкер НЕ должен ставиться под halt", bb)
	}
	if s.pos != 1 || s.posB != 0 || ba != 1 {
		t.Fatalf("belief pos=%d posB=%d vs broker legA=%d legB=%d", s.pos, s.posB, ba, bb)
	}
	h.assertNoSilentDivergence()
}

// #11. Квота=0 блокирует НОВЫЕ опены, но НЕ обязательный хедж уже работающего
// клипа. Если бы хедж-путь когда-либо гейтился лимитером — голая нога.
func TestQuotaZeroBlocksOpensButNotWorkingClipHedge(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withMaxPos(2), withLimiter(5))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	// Клип открывается, пока лимитер ещё пермиссивен (known=false -> Allow пускает).
	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Исчерпать headroom: remaining=1, Allow(2) требует 1>=2+5 -> отказ всем опенам.
	h.quotaSet(1, time.Now().Add(time.Hour))
	h.publicPrint(legA, 100, 1, false) // хедж обязан сработать несмотря на лимитер
	h.waitFor("hedged despite quota", 6*time.Second, func(s engineSnap) bool { return s.pos == 1 && s.posB == -1 })
	h.assertConverged(8 * time.Second)

	// Из простоя с исчерпанной квотой новый опен ОТКАЗАН.
	h.setMaxPos(2)
	h.setIntent(+1, false, 1)
	time.Sleep(2 * time.Second)
	if h.snap().working {
		t.Fatal("новый клип не должен открываться при исчерпанной квоте")
	}
	if ba, _ := h.brokerPos(); ba != 1 {
		t.Fatalf("legA=%d, want 1 (второй опен подавлен)", ba)
	}
}

// #12. Анти-паттерн, ради которого весь reconcile: после стойкого РЕАЛЬНОГО
// расхождения (пережившего два прохода) движок НЕ должен «услужливо» перенять
// позицию брокера. Обязан вечно оставаться suspect и НИЧЕГО не угадывать.
func TestReconcilePersistentDivergenceStaysSuspectNeverAutoGuesses(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)
	h.hold()
	h.assertConverged(5 * time.Second) // флэт+здоров

	if err := h.srv.Sim.SetPosition(testAccount, legA, 3, 100); err != nil {
		t.Fatal(err)
	}
	// Держим hold: reconcile из простоя должен обнаружить фантом (открой мы клип
	// раньше — reconcile отказан при working, и suspect не наступит). После
	// suspect провоцируем интентом — и проверяем, что клип НЕ открывается.
	h.waitFor("suspect (phantom)", 8*time.Second, func(s engineSnap) bool { return s.suspect })
	h.setIntent(+1, false, 1) // активная провокация: движок обязан отказать

	// ~7s с фантомом: suspect держится, позиция НЕ становится 3, клип не открывается.
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		s := h.snap()
		if !s.suspect || s.pos != 0 || s.posB != 0 || s.working {
			t.Fatalf("движок НЕ должен угадывать/торговать на сомнительной позиции: snap=%+v", s)
		}
		if ba, _ := h.brokerPos(); ba != 3 {
			t.Fatalf("фантом должен держаться (legA=%d)", ba)
		}
		time.Sleep(300 * time.Millisecond)
	}
	// Снять фантом истинной правдой -> движок сам сходится.
	if err := h.srv.Sim.SetPosition(testAccount, legA, 0, 0); err != nil {
		t.Fatal(err)
	}
	h.hold()
	h.assertConverged(10 * time.Second)
}

// #16. Флип позиции лонг -> флэт -> шорт: смена знака на обеих ногах и через
// realized-PnL/avg сима. Ошибка стороны в хедже закрытия/открытия -> голая нога.
func TestPositionFlipLongToShortAccountingTracks(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.driveEntry(+1, 1) // лонг -> +1/-1
	h.waitFor("long", 5*time.Second, func(s engineSnap) bool { return s.pos == 1 })
	h.hold()
	h.assertConverged(8 * time.Second)
	if ba, bb := h.brokerPos(); ba != 1 || bb != -1 {
		t.Fatalf("после входа legA=%d legB=%d, want +1/-1", ba, bb)
	}

	// Закрытие -> флэт.
	h.setIntent(-1, true, 1)
	h.waitFor("close working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.publicPrint(legA, 102, 1, true) // legA ask fill -> движок покупает legB -> флэт
	h.waitFor("flat", 6*time.Second, func(s engineSnap) bool { return s.pos == 0 })
	h.hold()
	h.assertConverged(8 * time.Second)
	if ba, bb := h.brokerPos(); ba != 0 || bb != 0 {
		t.Fatalf("после закрытия legA=%d legB=%d, want flat", ba, bb)
	}

	// Открытие шорта -> -1/+1.
	h.setIntent(-1, false, 1)
	h.waitFor("short working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.publicPrint(legA, 102, 1, true) // legA ask fill (продажа legA) -> движок покупает legB
	h.waitFor("short", 6*time.Second, func(s engineSnap) bool { return s.pos == -1 })
	h.hold()
	h.assertConverged(8 * time.Second)
	if ba, bb := h.brokerPos(); ba != -1 || bb != 1 {
		t.Fatalf("после шорта legA=%d legB=%d, want -1/+1", ba, bb)
	}
}

// #17. Чужой филл на общем счёте (ордер, который движок не ставил) не должен
// считаться и не должен провоцировать хедж; расхождение всплывает как suspect.
func TestForeignFillOnSharedAccountNotCounted(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)
	h.hold()
	h.assertConverged(5 * time.Second) // флэт+здоров

	// Чужой лимитник на legB напрямую (id вне Engine.own), затем его форс-фил.
	fid := h.placeForeignLimit(legB, true, 50)
	h.injectFill(fid, 1, 50) // AccountTrade стримится движку; broker legB=+1
	time.Sleep(1 * time.Second)

	s := h.snap()
	if s.pos != 0 || s.posB != 0 {
		t.Fatalf("чужой филл НЕ должен двигать позицию: pos=%d posB=%d", s.pos, s.posB)
	}
	if ba, _ := h.brokerPos(); ba != 0 {
		t.Fatalf("legA=%d, want 0 — движок не должен хеджить чужой филл", ba)
	}
	// Расхождение брокер/вера -> двухпроходный reconcile -> suspect -> ремонт.
	h.waitFor("suspect (foreign divergence)", 8*time.Second, func(s engineSnap) bool { return s.suspect })
	h.assertConvergesWithRepair(12 * time.Second)
}

// #1. Multi-lot клип, где counterpart исполнился БОЛЬШЕ первого фила мейкера,
// обязан свестись К TARGET, а не удвоиться за кап (инцидент 6:6 2026-07-16).
func TestCounterpartRanAheadResolvesToTargetNotOverCap(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withOrderVol(2), withMaxPos(2))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 2)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.silenceTrades()
	h.publicPrint(legB, 52, 2, true) // legB ask (counterpart) исполняется 2/2, его trade-событие заглушено
	time.Sleep(200 * time.Millisecond)
	h.clearFaults()                    // снять глушение (legB-trade уже сброшен и не переотправится до реконнекта)
	h.publicPrint(legA, 100, 1, false) // legA исполняется 1, доставлено ПЕРВЫМ -> legA мейкер(1), counterpart legB=2 -> counterpart-ahead
	h.waitFor("resolved to target 2", 8*time.Second, func(s engineSnap) bool { return s.pos == 2 })
	h.replaySilencedTrades() // реплей заглушённого legB(2) — дедуп счёта, ноль изменения
	h.hold()
	h.assertConverged(12 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 2 || bb != -2 {
		t.Fatalf("legA=%d legB=%d, want +2/-2 РОВНО (не оверкап 3/6)", ba, bb)
	}
}

// #2. Пере-квотирование (repeg): пассив, который УЖЕ исполнился пока его снимали
// и пере-постят, не должен удвоить позицию. Весь repeg-fold путь иначе 0% покрыт.
func TestRepegFoldCatchesFilledPassiveNoDoublePost(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withRepeg())
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.silenceTrades()
	h.publicPrint(legA, 100, 1, false) // legA bid исполняется 1/1, событие заглушено (makerID ещё "")
	h.setBook(legA, 101, 50, 102, 50)  // bid@101 вытесняет стоящий bid@100 -> repeg -> retireOrder ловит фил -> fold
	h.waitFor("folded to +1", 8*time.Second, func(s engineSnap) bool { return s.pos == 1 && s.posB == -1 })
	h.replaySilencedTrades()
	h.hold()
	h.assertConverged(10 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 РОВНО (второй legA-ордер НЕ поверх фила)", ba, bb)
	}
}

// #4. Stale-book pull срабатывает, когда мейкер УЖЕ исполнился у брокера, но
// событие не пришло: снести клип как неисполненный = голая нога. cancel-ack
// обязан выучить фил и захеджить.
func TestStaleBookPullRacesUnseenMakerFill(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withStaleBook(600*time.Millisecond))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.silenceTrades()
	h.publicPrint(legA, 100, 1, false) // legA исполняется у брокера, событие заглушено (makerFilled=0)
	// ~600ms без book-апдейтов -> checkStaleBooks -> abandonClip -> retireOrder ловит executed=1 -> хедж legB
	h.waitFor("caught fill hedged", 8*time.Second, func(s engineSnap) bool { return s.pos == 1 && s.posB == -1 })
	h.replaySilencedTrades()
	h.assertConverged(10 * time.Second)
	h.assertNoSilentDivergence()
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 (пойманный фил захеджен, не брошен)", ba, bb)
	}
}

// #6. Нога хеджа БЕЗ ликвидности: каждая постановка принята+кредитована, затем
// отвергнута биржей. Серия dead-short (>=3) -> deferHedge -> impaired (не halt);
// un-credit'ы не накапливают фантом. Рефилл -> долг оплачен -> 1:1.
func TestEmptyHedgeBookDeadShortStreakToImpairedDebt(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// legB без бидов: market SELL 0-fill -> REJECTED. Односторонний апдейт клиент
	// отбрасывает, touch legB замирает валидным (клип не затронут).
	h.setBookRaw(legB, nil, []brokersim.Level{{Price: 52, Size: 10}})
	h.publicPrint(legA, 100, 1, false) // мейкер фил -> хедж мрёт на 0 многократно -> streak>=3 -> deferHedge
	h.waitFor("impaired (dead streak)", 10*time.Second, func(s engineSnap) bool { return s.impaired })
	if h.snap().halted {
		t.Fatal("должен быть impaired, НЕ halted")
	}
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 10}}, []brokersim.Level{{Price: 52, Size: 10}}) // рефилл
	h.assertConverged(15 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 (долг оплачен по рефиллу)", ba, bb)
	}
}

// #7. Отмена не подтверждается -> ордер остаётся ЖИВ -> публичная печать
// исполняет его ВО ВРЕМЯ простоя связи. Stray-фил хеджится один раз; отложенное
// подтверждение НЕ должно добавить второй хедж/кредит.
func TestDeferredOrderFillsMidOutageAccountDedup(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.fault(brokersim.Fault{Method: "CancelOrder", Action: "error", Count: -1})
	h.fault(brokersim.Fault{Method: "GetOrder", Action: "error", Count: -1}) // PlaceOrder ЗДОРОВ
	h.hold()                                                                 // CancelClip -> defer обе ноги -> impaired (legA bid@100 стоит)
	h.waitFor("impaired (retireQ)", 8*time.Second, func(s engineSnap) bool { return s.impaired })
	h.publicPrint(legA, 100, 1, false) // фил отложенного legA-мейкера ВО ВРЕМЯ простоя
	h.waitFor("stray hedged once", 8*time.Second, func(s engineSnap) bool { return s.posB == -1 })
	h.clearFaults()
	h.assertConverged(15 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 (отложенное подтверждение — no-op, не +2/-2)", ba, bb)
	}
}

// #8. Impaired-во-время-impaired: единичный мейкер-фил под ТОТАЛЬНЫМ простоем
// (place+cancel) обязан открыть И deferRetire, И hedge-debt разом; обе очереди
// дренируются в один recovered-проход.
func TestImpairedDuringImpairedDualObligationDrain(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{})
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.fault(brokersim.Fault{Method: "CancelOrder", Action: "error", Count: -1})
	h.fault(brokersim.Fault{Method: "GetOrder", Action: "error", Count: -1})
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "error", Count: -1})
	h.publicPrint(legA, 100, 1, false) // мейкер фил под тотальным простоем
	h.waitFor("impaired", 8*time.Second, func(s engineSnap) bool { return s.impaired })
	h.clearFaults()
	h.hold()
	h.assertConverged(15 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 (обе очереди дренированы)", ba, bb)
	}
}

// #15. Multi-lot market-хедж по ТОНКОЙ книге частично исполняется, остаток
// отменён биржей. settleTaker обязан РОВНО реверснуть un-executed кредит и
// перехеджить РОВНО недобор — финал -2, никогда не -1 (голо) и не -3 (оверхедж).
func TestTakerPartialLiquidityShortfallExactlyReHedged(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{ExecLatency: brokersim.Duration(300 * time.Millisecond)},
		withOrderVol(2), withMaxPos(2))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 1}}, []brokersim.Level{{Price: 52, Size: 100}}) // тонкий бид: 1 лот

	h.setIntent(+1, false, 2)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.publicPrint(legA, 100, 2, false) // 2-лотовый мейкер -> хедж legB 2-лот market SELL: 1@50 fill, 1 CANCELED
	h.waitFor("un-credit to -1", 8*time.Second, func(s engineSnap) bool { return s.posB == -1 })
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 10}}, []brokersim.Level{{Price: 52, Size: 100}}) // рефилл -> недобор перехеджится
	h.assertConverged(12 * time.Second)
	ba, bb := h.brokerPos()
	if ba != 2 || bb != -2 {
		t.Fatalf("legA=%d legB=%d, want +2/-2 (недобор перехеджен ровно)", ba, bb)
	}
}

// #13. Тейкер-хедж принят с ПУСТЫМ order id: кредитуется вслепую и флагуется
// unverified; его настоящие филлы приходят под неотслеживаемым id (чужие).
// Пока unverified — новые клипы не открываются; снимается ТОЛЬКО чистым reconcile.
func TestTakerEmptyOrderIdBlindCreditClearsOnlyOnReconcile(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withMaxPos(2))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Следующая постановка (тейкер-хедж) вернёт пустой id, исполнив рыночный ордер внутри.
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "blank_order_id", Count: 1})
	h.publicPrint(legA, 100, 1, false) // мейкер фил -> хедж -> пустой-id тейкер -> unverified + слепой кредит; broker legB=-1
	h.waitFor("unverified (suspect)", 8*time.Second, func(s engineSnap) bool { return s.suspect })
	// Пока unverified — движок держит hold, ничего не открывает; belief +1/-1
	// совпадает с брокером (флагнуто, не молча), а настоящий фил под чужим id — no-op.
	h.assertNoSilentDivergence()
	h.hold()
	h.assertConverged(15 * time.Second) // belief +1/-1 == broker -> чистый reconcile снимает unverified
	ba, bb := h.brokerPos()
	if ba != 1 || bb != -1 {
		t.Fatalf("legA=%d legB=%d, want +1/-1 (ровно один слепой кредит, чужой фил — no-op)", ba, bb)
	}
}

// #18. Брокер вернул для нового хеджа ПЕРЕИСПОЛЬЗОВАННЫЙ order id (уже выданный
// другому ордеру). Движок не должен дать филлам переиспользованного id перезапустить
// чужой аккаунт: клобберит аккаунт, флагует unverified, никаких новых клипов.
func TestBrokerReusesOrderIdForTakerAccountClobberUnverified(t *testing.T) {
	h := newEngineHarness(t, brokersim.Config{}, withMaxPos(2))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBook(legB, 50, 100, 52, 100)

	h.setIntent(+1, false, 1)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	// Тейкер-хедж вернёт id уже выданного ордера (предыдущий ордер счёта).
	h.fault(brokersim.Fault{Method: "PlaceOrder", Action: "reuse_order_id", Count: 1})
	h.publicPrint(legA, 100, 1, false) // мейкер фил -> хедж -> reused-id тейкер -> REUSED, клоббер, unverified, posB=-1
	h.waitFor("unverified (suspect)", 8*time.Second, func(s engineSnap) bool { return s.suspect })
	h.setIntent(+1, false, 1)
	time.Sleep(1500 * time.Millisecond)
	if h.snap().working {
		t.Fatal("новый клип НЕ должен открываться, пока unverified")
	}
	h.assertNoSilentDivergence()
	h.hold()
	h.assertConvergesWithRepair(15 * time.Second) // клоббер мог оставить ногу — ремонт добивает
}

// findTakerOrder ищет id тейкер-ордера ноги B по критериям (initial/executed) —
// нужен OrdersListIncludesTerminal=true, чтобы видеть терминальные.
func (h *engineHarness) findTakerOrder(wantInitial, wantExecuted int) string {
	h.t.Helper()
	resp, err := finam.GetOrders(h.client)
	if err != nil {
		h.t.Fatalf("GetOrders: %v", err)
	}
	for _, st := range resp.GetOrders() {
		o := st.GetOrder()
		if o.GetSymbol() != legB || o.GetSide() != v1.Side_SIDE_SELL {
			continue
		}
		if o.GetType() != ordersMarket {
			continue
		}
		init := int(finam.ParseDecimal(st.GetInitialQuantity().GetValue()))
		exec := finam.ExecutedLots(st)
		if init == wantInitial && exec == wantExecuted {
			return st.GetOrderId()
		}
	}
	return ""
}

// #14. После того как движок урегулировал хедж как терминально dead-short,
// брокер доставляет ЕЩЁ филлы на тот же ордер — противоречит собственному ack.
// OnFill докредитовывает лот сверх терминала, reconcile флагует 1:1-нарушение
// (suspect); дубликат вброса не добавляет кредита.
func TestFillsBeyondConfirmedDeadTakerBrokerContradictsAck(t *testing.T) {
	incl := true
	h := newEngineHarness(t, brokersim.Config{OrdersListIncludesTerminal: incl}, withOrderVol(2), withMaxPos(2))
	h.setBook(legA, 100, 50, 102, 50)
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 1}}, []brokersim.Level{{Price: 52, Size: 100}}) // тонкий бид: 1

	h.setIntent(+1, false, 2)
	h.waitFor("clip working", 5*time.Second, func(s engineSnap) bool { return s.working })
	h.publicPrint(legA, 100, 2, false) // 2-лот мейкер -> хедж 2-лот SELL: 1@50 fill, 1 CANCELED -> settleTaker(1) dead-short, недобор 1
	// недобор перехеджится по рефиллу -> broker legA+2/legB-2
	h.waitFor("un-credit to -1", 8*time.Second, func(s engineSnap) bool { return s.posB == -1 })
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 10}}, []brokersim.Level{{Price: 52, Size: 100}})
	h.waitFor("shortfall re-hedged to -2", 12*time.Second, func(s engineSnap) bool { return s.posB == -2 })
	h.hold()
	h.assertConverged(10 * time.Second) // +2/-2, здоров

	// Найти dead-short тейкер (placed 2, executed 1) и вбросить 1 лот СВЕРХ терминала.
	deadTaker := h.findTakerOrder(2, 1)
	if deadTaker == "" {
		t.Fatal("не найден dead-short тейкер (initial=2, executed=1)")
	}
	h.injectFill(deadTaker, 1, 50) // broker legB=-3; OnFill докредит beyond-terminal -> posB=-3
	h.waitFor("beyond-terminal suspect", 10*time.Second, func(s engineSnap) bool { return s.suspect })
	// Вброс создал РЕАЛЬНЫЙ оверхедж (legB=-3 vs legA=2): движок докредитовал,
	// чтобы вера следила за брокером, и флагует 1:1-нарушение (suspect). Это НЕ
	// молчаливое расхождение — belief posB следит за брокером.
	if bb := mustLegB(h); h.snap().posB != bb {
		t.Fatalf("belief posB=%d != broker legB=%d (докредит должен следить за брокером)", h.snap().posB, bb)
	}
	// Дубликат вброса не должен добавить кредита (placed-size / max clamp).
	posBefore := h.snap().posB
	h.injectFill(deadTaker, 1, 50)
	time.Sleep(1500 * time.Millisecond)
	if h.snap().posB != posBefore {
		t.Fatalf("дубликат вброса изменил posB: %d -> %d (clamp должен держать)", posBefore, h.snap().posB)
	}
	// Оператор сплющивает лишний лот к 1:1 -> reconcile сходится -> resume.
	h.assertHealthyBalanced(15 * time.Second)
}

// mustLegB возвращает фактическую позицию брокера по ноге B.
func mustLegB(h *engineHarness) int { _, bb := h.brokerPos(); return bb }

// TestEngineHonestBrokerNeverSelfFreezes — РЕШАЮЩИЙ тест гипотезы «замороженные
// режимы от корнер-кейса движка». Брокер ЧЕСТНЫЙ (ноль сбоев), книги ГЛУБОКИЕ
// (хеджи всегда исполняются — нет impaired от нехватки ликвидности). Под
// агрессивными гонками таймингов (fill vs cancel, частичные филлы, repeg, флипы)
// движок ОБЯЗАН никогда не входить в impaired и никогда не застревать в стойком
// suspect — и то и другое при честном брокере было бы САМОиндуцированным
// расхождением (тот самый корнер-кейс). Транзиентный one-pass suspect от
// in-flight фила допустим, но обязан сняться следующим проходом reconcile.
func TestEngineHonestBrokerNeverSelfFreezes(t *testing.T) {
	if os.Getenv("BROKERSIM_SOAK") == "" {
		t.Skip("honest-broker fuzz: задайте BROKERSIM_SOAK=1")
	}
	h := newEngineHarness(t, brokersim.Config{ExecLatency: brokersim.Duration(15 * time.Millisecond)},
		withOrderVol(2), withMaxPos(3), withRepeg())
	// Глубокие книги: тейкер-хеджи ВСЕГДА исполняются (никакого impaired от 0-fill).
	h.setBookRaw(legA, []brokersim.Level{{Price: 100, Size: 100000}}, []brokersim.Level{{Price: 102, Size: 100000}})
	h.setBookRaw(legB, []brokersim.Level{{Price: 50, Size: 100000}}, []brokersim.Level{{Price: 52, Size: 100000}})

	// Наблюдатель: флагует ЛЮБОЙ impaired и меряет непрерывную длительность suspect.
	var mu sync.Mutex
	everImpaired, everHalted := false, false
	var suspectSince time.Time
	maxSuspectDwell := time.Duration(0)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(25 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				s := h.snap()
				mu.Lock()
				if s.impaired {
					everImpaired = true
				}
				if s.halted {
					everHalted = true
				}
				if s.suspect {
					if suspectSince.IsZero() {
						suspectSince = time.Now()
					} else if d := time.Since(suspectSince); d > maxSuspectDwell {
						maxSuspectDwell = d
					}
				} else {
					suspectSince = time.Time{}
				}
				mu.Unlock()
			}
		}
	}()

	// Детерминированные псевдослучайные выборы: индекс шага задаёт всё.
	pick := func(i, mod int) int { return (i*2654435761 + 1013904223) % mod }
	if pick(1, 7) < 0 {
		t.Fatal("unreachable")
	}

	const cycles = 120
	for i := 0; i < cycles; i++ {
		dir := 1
		if i%2 == 0 {
			dir = -1
		}
		lots := 1 + (i % 2) // 1 или 2
		race := i % 6

		h.setIntent(dir, false, lots)
		opened := h.waitWorkingSoft(2 * time.Second)
		price := 100.0
		if dir < 0 {
			price = 102.0
		}
		buyPrint := dir < 0 // печать, кроссящая мейкера ноги A

		if opened {
			switch race {
			case 0: // полный фил
				h.publicPrint(legA, price, float64(lots), buyPrint)
			case 1: // частичный фил, затем добить
				h.publicPrint(legA, price, 1, buyPrint)
				time.Sleep(20 * time.Millisecond)
				if lots > 1 {
					h.publicPrint(legA, price, float64(lots-1), buyPrint)
				}
			case 2: // отмена ДО фила (hold сразу)
				h.hold()
			case 3: // ДЕТЕРМИНИРОВАННАЯ гонка fill vs cancel (исторический корнер-кейс)
				h.raceCancelVsFill(price, lots, buyPrint)
			case 4: // repeg: сдвинуть книгу ноги A, затем фил
				if dir > 0 {
					h.setBookRaw(legA, []brokersim.Level{{Price: 101, Size: 100000}}, []brokersim.Level{{Price: 102, Size: 100000}})
					h.publicPrint(legA, 101, float64(lots), false)
				} else {
					h.setBookRaw(legA, []brokersim.Level{{Price: 100, Size: 100000}}, []brokersim.Level{{Price: 101, Size: 100000}})
					h.publicPrint(legA, 101, float64(lots), true)
				}
				// вернуть книгу
				h.setBookRaw(legA, []brokersim.Level{{Price: 100, Size: 100000}}, []brokersim.Level{{Price: 102, Size: 100000}})
			case 5: // фил, затем сразу закрыть (флип-нагрузка)
				h.publicPrint(legA, price, float64(lots), buyPrint)
				time.Sleep(30 * time.Millisecond)
				h.setIntent(-dir, true, lots)
				if h.waitWorkingSoft(1 * time.Second) {
					cp := 102.0
					cb := true
					if -dir < 0 {
						cp, cb = 102.0, true
					} else {
						cp, cb = 100.0, false
					}
					h.publicPrint(legA, cp, float64(lots), cb)
				}
			}
		}

		time.Sleep(60 * time.Millisecond)

		// Периодический settle: дать reconcile отработать; при честном брокере
		// impaired недопустим, а suspect обязан быть транзиентным.
		if i%8 == 7 {
			h.hold()
			// свести к флэту (закрыть, что открыто)
			for tries := 0; tries < 3; tries++ {
				s := h.snap()
				if s.pos == 0 && !s.working {
					break
				}
				if !s.working && s.pos != 0 {
					h.setIntent(-sign(s.pos), true, abs(s.pos))
					if h.waitWorkingSoft(1500 * time.Millisecond) {
						pr, pb := 102.0, true
						if s.pos < 0 {
							pr, pb = 102.0, true
						} else {
							pr, pb = 102.0, true // закрытие лонга: продажа ноги A по ask
						}
						_ = pr
						_ = pb
						h.publicPrint(legA, 102, float64(abs(s.pos)), true)
						h.publicPrint(legA, 100, float64(abs(s.pos)), false)
					}
				}
				time.Sleep(300 * time.Millisecond)
			}
			mu.Lock()
			imp, hlt, dwell := everImpaired, everHalted, maxSuspectDwell
			mu.Unlock()
			if imp {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: движок вошёл в IMPAIRED при ЧЕСТНОМ брокере — самоиндуцированное расхождение (корнер-кейс)", i)
			}
			if hlt {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: движок HALTED сам, без оператора — невозможно (нет внутренних Halt)", i)
			}
			if dwell > 4*time.Second {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: suspect держался %v (>4s) при честном брокере — стойкое САМОрасхождение (корнер-кейс)", i, dwell)
			}
		}
	}

	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if everImpaired {
		t.Fatal("движок входил в IMPAIRED при честном брокере — корнер-кейс самоиндуцированного расхождения")
	}
	if everHalted {
		t.Fatal("движок HALTED без оператора")
	}
	if maxSuspectDwell > 4*time.Second {
		t.Fatalf("suspect держался %v (>4s) при честном брокере — стойкое саморасхождение", maxSuspectDwell)
	}
	// Доказательство, что гонка cancel/fill РЕАЛЬНО воспроизводилась (иначе зелёный
	// тест был бы бессмысленным — окно гонки просто не открывалось).
	h.mu.Lock()
	hits := h.raceHits
	h.mu.Unlock()
	if hits == 0 {
		t.Fatal("гонка cancel/fill ни разу не воспроизвелась — тест не задел корнер-кейс")
	}
	t.Logf("OK: %d циклов, честный брокер, ноль impaired/halted, max suspect-dwell=%v; гонок cancel/fill поймано=%d",
		cycles, maxSuspectDwell, hits)
}

func sign(v int) int {
	if v < 0 {
		return -1
	}
	return 1
}

// setGappedBook ставит глубокую книгу ноги с bid=mid, ask=mid+2 (тейкер-хеджи
// всегда исполняются — impaired от ликвидности исключён).
func (h *engineHarness) setGappedBook(sym string, mid float64) {
	h.setBookRaw(sym,
		[]brokersim.Level{{Price: mid, Size: 1_000_000}},
		[]brokersim.Level{{Price: mid + 2, Size: 1_000_000}})
}

// TestEngineFastVolatileNoSelfFreeze — быстрый темп + РЕЗКАЯ волатильность.
// Каждый цикл книга обеих ног прыгает крупным гэпом (тач движется violently ->
// repeg гонится, RepegThrottle 20ms), темп высокий (тик 30ms, сон 8ms), стоит
// квота-лимитер (гейтинг опенов под нагрузкой). Брокер ЧЕСТНЫЙ, книги глубокие.
// Инвариант: движок не входит в impaired/halted и не застревает в suspect —
// иначе это самоиндуцированное расхождение под скоростью/волатильностью.
func TestEngineFastVolatileNoSelfFreeze(t *testing.T) {
	if os.Getenv("BROKERSIM_SOAK") == "" {
		t.Skip("fast-volatile fuzz: задайте BROKERSIM_SOAK=1")
	}
	// PlaceQuotaLimit поднят: под быстрой торговлей дефолтные 200/мин выжигаются
	// в минуту, и честный сим начинает отвергать постановку хеджа
	// (ResourceExhausted -> impaired). Это РЕАЛЬНОЕ внешнее условие быстрой
	// торговли (в проде headroom под хеджи резервирует QuotaLimiter+RefreshQuota),
	// но здесь мы ИЗОЛИРУЕМ ось «скорость+волатильность» от квотного боттлнека.
	h := newEngineHarness(t, brokersim.Config{
		ExecLatency:     brokersim.Duration(8 * time.Millisecond),
		PlaceQuotaLimit: 1_000_000,
	}, withOrderVol(2), withMaxPos(3), withRepegThrottle(20*time.Millisecond),
		withTick(30*time.Millisecond)) // быстрее гоняем клип/repeg

	legAmid, legBmid := 100.0, 50.0
	h.setGappedBook(legA, legAmid)
	h.setGappedBook(legB, legBmid)

	var mu sync.Mutex
	everImpaired, everHalted := false, false
	var suspectSince time.Time
	maxSuspectDwell := time.Duration(0)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				s := h.snap()
				mu.Lock()
				if s.impaired {
					everImpaired = true
				}
				if s.halted {
					everHalted = true
				}
				if s.suspect {
					if suspectSince.IsZero() {
						suspectSince = time.Now()
					} else if d := time.Since(suspectSince); d > maxSuspectDwell {
						maxSuspectDwell = d
					}
				} else {
					suspectSince = time.Time{}
				}
				mu.Unlock()
			}
		}
	}()

	// Детерминированный «резкий» гэп: большие скачки цены каждый цикл.
	clamp := func(v, lo, hi float64) float64 {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	const cycles = 300
	traded := 0
	for i := 0; i < cycles; i++ {
		// РЕЗКАЯ ВОЛАТИЛЬНОСТЬ: гэп обеих ног на большой знакопеременный шаг.
		gapA := float64((i*13)%37) - 18 // [-18, +18]
		gapB := float64((i*7)%19) - 9   // [-9, +9]
		legAmid = clamp(legAmid+gapA, 40, 400)
		legBmid = clamp(legBmid+gapB, 20, 200)
		h.setGappedBook(legA, legAmid)
		h.setGappedBook(legB, legBmid)

		dir := 1
		if i%2 == 0 {
			dir = -1
		}
		lots := 1 + i%2
		price := legAmid  // мейкер лонга стоит на bid=legAmid
		buyPrint := false // sell-печать кроссит bid
		if dir < 0 {
			price = legAmid + 2 // мейкер шорта на ask
			buyPrint = true
		}

		h.setIntent(dir, false, lots)
		if h.waitWorkingSoft(400 * time.Millisecond) {
			switch i % 5 {
			case 0: // полный фил на текущем (гэпнутом) таче
				h.publicPrint(legA, price, float64(lots), buyPrint)
				traded++
			case 1: // гонка fill vs cancel
				h.raceCancelVsFill(price, lots, buyPrint)
			case 2: // отмена
				h.hold()
			case 3: // ЕЩЁ гэп, пока клип работает -> repeg гонится -> затем отмена
				legAmid = clamp(legAmid+float64((i%2)*20-10), 40, 400)
				h.setGappedBook(legA, legAmid)
				time.Sleep(15 * time.Millisecond)
				h.hold()
			case 4: // фил, затем сразу закрыть
				h.publicPrint(legA, price, float64(lots), buyPrint)
				traded++
				time.Sleep(10 * time.Millisecond)
				h.setIntent(-dir, true, lots)
				if h.waitWorkingSoft(500 * time.Millisecond) {
					cp, cb := legAmid+2, true // закрытие: печать по ask/bid
					if -dir < 0 {
						cp, cb = legAmid+2, true
					} else {
						cp, cb = legAmid, false
					}
					h.publicPrint(legA, cp, float64(lots), cb)
				}
			}
		}
		time.Sleep(8 * time.Millisecond) // высокий темп

		if i%20 == 19 {
			// Settle: стабилизировать книгу, свести к флэту, проверить инварианты.
			h.hold()
			for tries := 0; tries < 4; tries++ {
				s := h.snap()
				if s.pos == 0 && !s.working {
					break
				}
				if !s.working && s.pos != 0 {
					h.setIntent(-sign(s.pos), true, abs(s.pos))
					if h.waitWorkingSoft(1 * time.Second) {
						h.publicPrint(legA, legAmid+2, float64(abs(s.pos)), true)
						h.publicPrint(legA, legAmid, float64(abs(s.pos)), false)
					}
				}
				time.Sleep(300 * time.Millisecond)
			}
			mu.Lock()
			imp, hlt, dwell := everImpaired, everHalted, maxSuspectDwell
			mu.Unlock()
			if imp {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: IMPAIRED при честном брокере/быстром волатильном рынке — самоиндуцированное расхождение", i)
			}
			if hlt {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: HALTED без оператора — невозможно", i)
			}
			if dwell > 5*time.Second {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: suspect держался %v (>5s) — стойкое саморасхождение под волатильностью", i, dwell)
			}
		}
	}

	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if everImpaired {
		t.Fatal("IMPAIRED при честном брокере под скоростью/волатильностью — корнер-кейс")
	}
	if everHalted {
		t.Fatal("HALTED без оператора")
	}
	if maxSuspectDwell > 5*time.Second {
		t.Fatalf("suspect держался %v (>5s) — стойкое саморасхождение", maxSuspectDwell)
	}
	if traded == 0 {
		t.Fatal("ни одного фила — тест не нагрузил движок")
	}
	t.Logf("OK: %d циклов, быстрый волатильный рынок, филлов=%d, ноль impaired/halted, max suspect-dwell=%v",
		cycles, traded, maxSuspectDwell)
}

// TestEngineFastTradingQuotaReservationNoHedgeStarvation — прод-механизм под
// быстрой торговлей: квота сима 200/мин (как у брокера), лимитер + RefreshQuota
// РЕЗЕРВИРУЮТ headroom под обязательные хеджи. Под быстрым потоком опены должны
// притормаживаться, но хедж НИКОГДА не голодать -> НЕТ impaired-от-квоты
// (в отличие от прогона без резервирования, где сим отвергал хедж
// ResourceExhausted -> impaired). Доказывает, что резервирование спасает
// несимметрию под скоростью.
func TestEngineFastTradingQuotaReservationNoHedgeStarvation(t *testing.T) {
	if os.Getenv("BROKERSIM_SOAK") == "" {
		t.Skip("fast-quota fuzz: задайте BROKERSIM_SOAK=1")
	}
	h := newEngineHarness(t, brokersim.Config{
		ExecLatency:      brokersim.Duration(8 * time.Millisecond),
		PlaceQuotaLimit:  200, // как у брокера
		PlaceQuotaWindow: brokersim.Duration(time.Minute),
	}, withOrderVol(1), withMaxPos(3), withQuotaRefresh(20), withTick(30*time.Millisecond))
	h.setGappedBook(legA, 100)
	h.setGappedBook(legB, 50)

	var mu sync.Mutex
	everImpaired, everHalted := false, false
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				s := h.snap()
				mu.Lock()
				if s.impaired {
					everImpaired = true
				}
				if s.halted {
					everHalted = true
				}
				mu.Unlock()
			}
		}
	}()

	// Быстрый поток открытий/филлов/закрытий — давит на квоту постановки.
	const cycles = 400
	traded := 0
	for i := 0; i < cycles; i++ {
		dir := 1
		if i%2 == 0 {
			dir = -1
		}
		price, buyPrint := 100.0, false
		if dir < 0 {
			price, buyPrint = 102.0, true
		}
		h.setIntent(dir, false, 1)
		if h.waitWorkingSoft(300 * time.Millisecond) {
			h.publicPrint(legA, price, 1, buyPrint)
			traded++
			time.Sleep(8 * time.Millisecond)
			// сразу закрыть
			h.setIntent(-dir, true, 1)
			if h.waitWorkingSoft(300 * time.Millisecond) {
				cp, cb := 102.0, true
				if -dir > 0 {
					cp, cb = 100.0, false
				}
				h.publicPrint(legA, cp, 1, cb)
			}
		}
		h.hold()
		time.Sleep(5 * time.Millisecond)

		if i%25 == 24 {
			mu.Lock()
			imp, hlt := everImpaired, everHalted
			mu.Unlock()
			if imp {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: IMPAIRED под быстрой торговлей — резервирование квоты НЕ спасло хедж от голодания", i)
			}
			if hlt {
				close(stop)
				wg.Wait()
				t.Fatalf("cycle %d: HALTED без оператора", i)
			}
		}
	}

	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if everImpaired {
		t.Fatal("IMPAIRED под быстрой торговлей — резервирование квоты не спасло хедж")
	}
	if everHalted {
		t.Fatal("HALTED без оператора")
	}
	if traded == 0 {
		t.Fatal("ни одного фила")
	}
	t.Logf("OK: %d циклов быстрой торговли @ квота 200/мин, филлов=%d, резервирование удержало — ноль impaired/halted", cycles, traded)
}

// TestEngineFastTradingBootstrapLimiterProtectsHedge — доказательство ФИКСА.
// Квота сима 200 (окно большое, чтобы не сбрасывалось за прогон). Лимитер —
// fail-safe бюджетный, БЕЗ RefreshQuota (эмулируем «рефрешер не запущен/умер»).
// Раньше это = fail-open (blind-permissive) -> опены выжигали квоту -> хедж
// отвергался -> impaired (голая нога). После фикса самоуправляемый бюджет сам
// гейтит опены, резервируя headroom под хеджи -> ноль impaired.
func TestEngineFastTradingBootstrapLimiterProtectsHedge(t *testing.T) {
	if os.Getenv("BROKERSIM_SOAK") == "" {
		t.Skip("bootstrap-limiter fuzz: задайте BROKERSIM_SOAK=1")
	}
	h := newEngineHarness(t, brokersim.Config{
		ExecLatency:      brokersim.Duration(8 * time.Millisecond),
		PlaceQuotaLimit:  200,
		PlaceQuotaWindow: brokersim.Duration(10 * time.Minute), // не сбрасывается за прогон
	}, withOrderVol(1), withMaxPos(3),
		withQuotaBudget(20, 200, 10*time.Minute), // fail-safe, БЕЗ RefreshQuota
		withTick(30*time.Millisecond))
	h.setGappedBook(legA, 100)
	h.setGappedBook(legB, 50)

	var mu sync.Mutex
	everImpaired, everHalted := false, false
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				s := h.snap()
				mu.Lock()
				if s.impaired {
					everImpaired = true
				}
				if s.halted {
					everHalted = true
				}
				mu.Unlock()
			}
		}
	}()

	const cycles = 400
	traded, gated := 0, 0
	for i := 0; i < cycles; i++ {
		dir := 1
		if i%2 == 0 {
			dir = -1
		}
		price, buyPrint := 100.0, false
		if dir < 0 {
			price, buyPrint = 102.0, true
		}
		h.setIntent(dir, false, 1)
		if h.waitWorkingSoft(250 * time.Millisecond) {
			h.publicPrint(legA, price, 1, buyPrint)
			traded++
			time.Sleep(6 * time.Millisecond)
			h.setIntent(-dir, true, 1)
			if h.waitWorkingSoft(250 * time.Millisecond) {
				cp, cb := 102.0, true
				if -dir > 0 {
					cp, cb = 100.0, false
				}
				h.publicPrint(legA, cp, 1, cb)
			}
		} else {
			gated++ // опен не открылся — вероятно, зарезервировали под хеджи
		}
		h.hold()
		time.Sleep(4 * time.Millisecond)
	}

	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if everImpaired {
		t.Fatal("IMPAIRED: bootstrap-лимитер НЕ защитил хедж от голодания квотой (фикс не работает)")
	}
	if everHalted {
		t.Fatal("HALTED без оператора")
	}
	if traded == 0 {
		t.Fatal("ни одного фила")
	}
	t.Logf("OK: %d циклов, квота 200 без RefreshQuota, филлов=%d, опенов-загейтчено=%d, ноль impaired — резерв держится сам",
		cycles, traded, gated)
}
