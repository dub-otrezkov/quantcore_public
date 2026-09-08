// Package orders хранит все заявки одного движка и защищает от повторных fill.
package orders

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

type order struct {
	id         string
	req        model.OrderRequest
	sentAt     time.Time
	guessPrice float64
	filled     int
	used       int
	done       bool
	endFilled  int
	fills      map[string]struct{}
}

// Info — копия состояния одной заявки.
type Info struct {
	ID         string
	Request    model.OrderRequest
	SentAt     time.Time
	GuessPrice float64
	Filled     int
	Used       int
	Done       bool
	EndFilled  int
}

// Change — изменение после одного события брокера.
// Lots может быть меньше нуля, если рыночная заявка исполнилась не полностью.
type Change struct {
	Known     bool
	Again     bool
	Conflict  bool
	Extra     int
	Lots      int
	PriceLots int
	FillPrice float64
	Order     Info
}

// List можно использовать с нулевым значением. Карты не выходят из пакета.
type List struct {
	orders    map[string]*order
	closeList map[string]struct{}
}

func (r *List) init() {
	if r.orders == nil {
		r.orders = make(map[string]*order)
	}
	if r.closeList == nil {
		r.closeList = make(map[string]struct{})
	}
}

// Add сохраняет отправленную заявку. Для рыночной заявки countNow сразу
// учитывает весь размер, а следующие события подтверждают или правят его.
func (r *List) Add(
	id string,
	req model.OrderRequest,
	sentAt time.Time,
	guessPrice float64,
	countNow bool,
) (Change, error) {
	r.init()
	if id == "" {
		return Change{}, errors.New("empty order id")
	}
	if req.Lots <= 0 {
		return Change{}, errors.New("order lots must be positive")
	}
	if _, exists := r.orders[id]; exists {
		return Change{}, fmt.Errorf("order %q already registered", id)
	}
	o := &order{
		id:         id,
		req:        req,
		sentAt:     sentAt,
		guessPrice: guessPrice,
		fills:      make(map[string]struct{}),
	}
	if countNow {
		o.used = req.Lots
	}
	r.orders[id] = o
	result := Change{Known: true, Order: snapshot(o)}
	if countNow {
		result.Lots = req.Lots
	}
	return result, nil
}

// AddFill учитывает одно исполнение ровно один раз.
func (r *List) AddFill(fill model.Fill) Change {
	o := r.orders[fill.OrderID]
	if o == nil || fill.Lots <= 0 {
		return Change{}
	}
	result := Change{Known: true, FillPrice: fill.Price}
	if fill.FillID != "" {
		if _, again := o.fills[fill.FillID]; again {
			result.Again = true
			result.Order = snapshot(o)
			return result
		}
		o.fills[fill.FillID] = struct{}{}
	}

	take := min(fill.Lots, max(o.req.Lots-o.filled, 0))
	result.Extra = fill.Lots - take
	oldFilled := o.filled
	oldUsed := o.used
	o.filled += take

	// Fill может подтверждать объём, который уже был учтён по ответу брокера.
	// Цену меняем только для общей части.
	result.PriceLots = min(o.filled, oldUsed) - min(oldFilled, oldUsed)
	if result.PriceLots < 0 {
		result.PriceLots = 0
	}
	if o.filled > o.used {
		result.Lots = o.filled - o.used
		o.used = o.filled
	}
	result.Conflict = o.done && o.filled > o.endFilled
	result.Order = snapshot(o)
	return result
}

// Close закрывает заявку и учитывает её итоговый объём.
func (r *List) Close(orderID string, filledNow int) Change {
	o := r.orders[orderID]
	if o == nil {
		return Change{}
	}
	filledNow = min(max(filledNow, 0), o.req.Lots)
	if o.done {
		// The first terminal count is already settled. Replays cannot retract
		// it or mint inventory; subsequent fill events still record real lots.
		return Change{
			Known: true, Conflict: filledNow != o.endFilled || o.filled > filledNow,
			FillPrice: o.guessPrice, Order: snapshot(o),
		}
	}
	o.done = true
	o.endFilled = filledNow
	final := max(filledNow, o.filled)
	result := Change{
		Known:     true,
		Lots:      final - o.used,
		Conflict:  o.filled > filledNow,
		FillPrice: o.guessPrice,
	}
	o.used = final
	delete(r.closeList, orderID)
	result.Order = snapshot(o)
	return result
}

// NeedClose ставит заявку в очередь на повторную отмену или проверку.
func (r *List) NeedClose(orderID string) bool {
	r.init()
	o := r.orders[orderID]
	if o == nil || o.done {
		return false
	}
	r.closeList[orderID] = struct{}{}
	return true
}

// OrdersToClose возвращает отсортированную копию очереди.
func (r *List) OrdersToClose() []string {
	ids := make([]string, 0, len(r.closeList))
	for id := range r.closeList {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Closing reports whether a failed retirement already belongs to the retry loop.
func (r *List) Closing(orderID string) bool {
	_, ok := r.closeList[orderID]
	return ok
}

// HasOrder проверяет, знает ли список эту заявку.
func (r *List) HasOrder(id string) bool {
	_, ok := r.orders[id]
	return ok
}

// Info возвращает копию состояния заявки.
func (r *List) Info(id string) (Info, bool) {
	o := r.orders[id]
	if o == nil {
		return Info{}, false
	}
	return snapshot(o), true
}

func snapshot(o *order) Info {
	return Info{
		ID:         o.id,
		Request:    o.req,
		SentAt:     o.sentAt,
		GuessPrice: o.guessPrice,
		Filled:     o.filled,
		Used:       o.used,
		Done:       o.done,
		EndFilled:  o.endFilled,
	}
}
