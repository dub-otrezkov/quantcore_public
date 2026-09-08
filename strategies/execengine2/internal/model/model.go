// Package model хранит простые типы, общие для движка и его частей.
package model

import "time"

// Side — сторона заявки или сделки.
type Side uint8

const (
	SideBuy Side = iota + 1
	SideSell
)

// Other возвращает другую сторону.
func (s Side) Other() Side {
	if s == SideBuy {
		return SideSell
	}
	return SideBuy
}

// Sign возвращает знак позиции: покупка +1, продажа -1.
func (s Side) Sign() int {
	if s == SideBuy {
		return 1
	}
	return -1
}

// Leg — один инструмент пары.
type Leg uint8

const (
	LegNone Leg = iota
	LegA
	LegB
)

// Other возвращает второй инструмент пары.
func (l Leg) Other() Leg {
	if l == LegA {
		return LegB
	}
	if l == LegB {
		return LegA
	}
	return LegNone
}

// OrderKind задаёт тип заявки: лимитная или рыночная.
type OrderKind uint8

const (
	OrderLimit OrderKind = iota + 1
	OrderMarket
)

// OrderRole объясняет, зачем была создана заявка.
type OrderRole uint8

const (
	RoleTrade OrderRole = iota + 1
	RoleHedge
	RoleFix
	RoleLateFill
)

// Mode задаёт способ исполнения одной парной сделки.
type Mode uint8

const (
	ModeDefault Mode = iota
	ModeTwoLimits
	ModeLimitA
	ModeLimitB
	ModeMarket
)

// Plan — решение стратегии, которое движок ещё не применил.
type Plan struct {
	Action    int
	IsClose   bool
	OpenPrice float64
	Lots      int
	Mode      Mode
}

// Signal — вход стратегии. Движок не читает поле Signal.
type Signal struct {
	Time   time.Time
	Signal any
}

// Lot — одна часть позиции стратегии.
type Lot struct {
	Price float64
	Size  int
	Time  time.Time
}

// Result — итог применения плана.
type Result struct {
	Code      int
	ClosedLot *Lot
}

// Prices — последние цены первого уровня стакана.
type Prices struct {
	Bid float64
	Ask float64
	At  time.Time
}

// Price возвращает цену для лимитной заявки.
func (t Prices) Price(side Side) float64 {
	if side == SideBuy {
		return t.Bid
	}
	return t.Ask
}

// MarketPrice возвращает цену другой стороны стакана.
func (t Prices) MarketPrice(side Side) float64 {
	if side == SideBuy {
		return t.Ask
	}
	return t.Bid
}

// Valid проверяет цены перед отправкой заявки.
func (t Prices) Valid() bool {
	return t.Bid > 0 && t.Ask >= t.Bid
}

// OrderRequest — команда на отправку заявки без типов конкретного брокера.
type OrderRequest struct {
	Symbol  string
	Side    Side
	Kind    OrderKind
	Role    OrderRole
	Leg     Leg
	Lots    int
	Price   float64
	TradeID uint64
}

// Fill — одно исполнение от брокера. FillID нужен для защиты от повторов.
type Fill struct {
	FillID  string
	OrderID string
	Symbol  string
	Side    Side
	Lots    int
	Price   float64
	At      time.Time
}

// CancelResult — итог отмены заявки.
type CancelResult struct {
	Filled int
}

// OrderStatus — текущее состояние заявки у брокера.
// Filled is cumulative executed lots, not a delta. Broker.Status uses Done for
// all terminal outcomes; Engine.OnOrderStatus documents the v1 stream filter.
type OrderStatus struct {
	Filled int
	Done   bool
}

// LimitKind задаёт, можно ли брать запас для хеджа.
type LimitKind uint8

const (
	LimitNormal LimitKind = iota + 1
	LimitMust
)

// PositionChange меняет позицию стратегии.
// Lots: покупка со знаком плюс, продажа со знаком минус.
type PositionChange struct {
	OrderID string
	Symbol  string
	Lots    int
	Price   float64
	Reason  string
}

// PriceChange заменяет примерную цену фактической, не меняя позицию.
type PriceChange struct {
	OrderID string
	Symbol  string
	Lots    int
	From    float64
	To      float64
}
