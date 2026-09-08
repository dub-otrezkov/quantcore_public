// Package hedges хранит рыночные заявки и неотправленные хеджи.
package hedges

import (
	"errors"
	"sort"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

type market struct {
	orderID   string
	order     model.OrderRequest
	sentAt    time.Time
	nextCheck time.Time
	checkWait time.Duration
}

// Check — запрос состояния рыночной заявки.
type Check struct {
	OrderID string
	Request model.OrderRequest
}

// CheckResult — итог проверки рыночной заявки.
type CheckResult struct {
	Known   bool
	Done    bool
	Missing model.OrderRequest
}

// Work — копия хеджа, который надо отправить позже.
type Work struct {
	ID      uint64
	Request model.OrderRequest
	LastErr string
}

type workItem struct {
	id      uint64
	order   model.OrderRequest
	lastErr string
}

// List можно использовать с нулевым значением. Его карты не выходят из пакета.
type List struct {
	market  map[string]*market
	work    map[uint64]*workItem
	unknown []Unknown
	nextID  uint64
}

func (m *List) init() {
	if m.market == nil {
		m.market = make(map[string]*market)
	}
	if m.work == nil {
		m.work = make(map[uint64]*workItem)
	}
}

// AddMarket сохраняет рыночную заявку до подтверждения её объёма.
func (m *List) AddMarket(orderID string, req model.OrderRequest, sentAt time.Time) error {
	m.init()
	if orderID == "" {
		return errors.New("empty taker order id")
	}
	if req.Kind != model.OrderMarket || req.Lots <= 0 {
		return errors.New("pending hedge must be a positive taker order")
	}
	if _, found := m.market[orderID]; found {
		return errors.New("taker already pending")
	}
	m.market[orderID] = &market{orderID: orderID, order: req, sentAt: sentAt}
	return nil
}

// SeeFill убирает заявку из проверки, когда пришёл весь объём.
func (m *List) SeeFill(orderID string, filled int) bool {
	p := m.market[orderID]
	if p == nil || filled < p.order.Lots {
		return false
	}
	delete(m.market, orderID)
	return true
}

// Checks returns due market orders and backs off each unresolved order. Pending
// responses and partial fills keep that schedule; a terminal status or full fill
// removes it. A delayed event loop schedules from now, without catch-up bursts.
func (m *List) Checks(now time.Time, wait, firstEvery, maxEvery time.Duration) []Check {
	if firstEvery <= 0 {
		firstEvery = time.Second
	}
	maxEvery = max(firstEvery, maxEvery)
	ids := make([]string, 0, len(m.market))
	for id := range m.market {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	checks := make([]Check, 0, len(ids))
	for _, id := range ids {
		p := m.market[id]
		if now.Sub(p.sentAt) < wait ||
			(!p.nextCheck.IsZero() && now.Before(p.nextCheck)) {
			continue
		}
		if p.checkWait < firstEvery {
			p.checkWait = firstEvery
		} else if p.checkWait > maxEvery/2 {
			// Clamp before multiplying, including very large configured caps.
			p.checkWait = maxEvery
		} else {
			p.checkWait *= 2
		}
		p.nextCheck = now.Add(p.checkWait)
		checks = append(checks, Check{OrderID: id, Request: p.order})
	}
	return checks
}

// SetStatus закрывает проверку или возвращает заявку на недостающий объём.
func (m *List) SetStatus(orderID string, status model.OrderStatus) CheckResult {
	p := m.market[orderID]
	if p == nil {
		return CheckResult{}
	}
	if !status.Done && status.Filled < p.order.Lots {
		return CheckResult{Known: true}
	}
	delete(m.market, orderID)
	result := CheckResult{Known: true, Done: status.Filled >= p.order.Lots}
	if status.Filled < p.order.Lots {
		result.Missing = p.order
		result.Missing.Role = model.RoleFix
		result.Missing.Lots = p.order.Lots - max(status.Filled, 0)
	}
	return result
}

// Add сохраняет хедж, который не удалось отправить.
func (m *List) Add(req model.OrderRequest, err error) uint64 {
	m.init()
	m.nextID++
	d := &workItem{id: m.nextID, order: req}
	if err != nil {
		d.lastErr = err.Error()
	}
	m.work[d.id] = d
	return d.id
}

// All возвращает отсортированную копию отложенных хеджей.
func (m *List) All() []Work {
	ids := make([]uint64, 0, len(m.work))
	for id := range m.work {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]Work, 0, len(ids))
	for _, id := range ids {
		d := m.work[id]
		result = append(result, Work{ID: d.id, Request: d.order, LastErr: d.lastErr})
	}
	return result
}

// Done удаляет успешно отправленный хедж.
func (m *List) Done(id uint64) bool {
	if _, ok := m.work[id]; !ok {
		return false
	}
	delete(m.work, id)
	return true
}

// HasWork проверяет, осталась ли работа.
func (m *List) HasWork() bool {
	return len(m.market) > 0 || len(m.work) > 0 || len(m.unknown) > 0
}

// MarketCount возвращает число рыночных заявок без подтверждения.
func (m *List) MarketCount() int { return len(m.market) }
