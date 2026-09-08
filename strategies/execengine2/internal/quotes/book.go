// Package quotes хранит последние цены пары и время запрета новых сделок.
package quotes

import (
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

// Book хранит цены двух инструментов. Его вызывает одна горутина движка.
type Book struct {
	legA      string
	legB      string
	a         model.Prices
	b         model.Prices
	openAfter time.Time
}

// New создаёт стакан для пары инструментов.
func New(legA, legB string) *Book {
	return &Book{legA: legA, legB: legB}
}

// Update сохраняет новые bid и ask. Возвращает false для чужого инструмента.
func (b *Book) Update(symbol string, at time.Time, bid, ask float64) bool {
	var target *model.Prices
	switch symbol {
	case b.legA:
		target = &b.a
	case b.legB:
		target = &b.b
	default:
		return false
	}
	if at.Before(target.At) {
		return false
	}
	*target = model.Prices{Bid: bid, Ask: ask, At: at}
	return true
}

// Prices возвращает копию последних цен инструмента.
func (b *Book) Prices(leg model.Leg) model.Prices {
	if leg == model.LegA {
		return b.a
	}
	if leg == model.LegB {
		return b.b
	}
	return model.Prices{}
}

// Ready проверяет цены обоих инструментов и запрет новых сделок.
func (b *Book) Ready(now time.Time, maxStaleness time.Duration) bool {
	if now.Before(b.openAfter) || !b.a.Valid() || !b.b.Valid() {
		return false
	}
	staleA, staleB := b.TooOld(now, maxStaleness)
	return !staleA && !staleB
}

// TooOld проверяет возраст цен. Нулевой maxAge выключает только проверку времени.
func (b *Book) TooOld(now time.Time, maxStaleness time.Duration) (bool, bool) {
	stale := func(t model.Prices) bool {
		if !t.Valid() {
			return true
		}
		return maxStaleness > 0 && now.Sub(t.At) > maxStaleness
	}
	return stale(b.a), stale(b.b)
}

// BlockOpen запрещает новые сделки до заданного времени.
func (b *Book) BlockOpen(until time.Time) {
	if until.After(b.openAfter) {
		b.openAfter = until
	}
}

// OpenTime возвращает время, после которого можно открыть сделку.
func (b *Book) OpenTime() time.Time { return b.openAfter }
