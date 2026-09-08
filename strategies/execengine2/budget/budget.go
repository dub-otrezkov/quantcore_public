// Package budget provides shared placement budgets. Use Quota for broker metrics
// and automatic windows; Atomic is a fixed counter with explicit local resets.
package budget

import (
	"errors"
	"sync/atomic"

	"QuantCore/strategies/execengine2/internal/model"
)

// Atomic — общий счётчик для всех движков одного счёта.
// reserve не меняется, remaining — единственное изменяемое поле.
type Atomic struct {
	remaining atomic.Int64
	reserve   int64
}

// New создаёт лимит с заданным числом попыток и запасом для хеджа.
func New(limit, reserve int64) (*Atomic, error) {
	if limit < 0 {
		return nil, errors.New("budget limit must not be negative")
	}
	if reserve < 0 {
		return nil, errors.New("budget reserve must not be negative")
	}
	b := &Atomic{reserve: reserve}
	b.remaining.Store(limit)
	return b, nil
}

// Allow checks a discretionary burst without charging attempts. Engines that
// need v1 admission semantics book each actual RPC separately with LimitMust.
func (b *Atomic) Allow(ops int64) bool {
	if ops <= 0 {
		return true
	}
	remaining := b.remaining.Load()
	return remaining >= b.reserve && ops <= remaining-b.reserve
}

// Take одной атомарной операцией проверяет и списывает попытки.
// Обязательный хедж может взять запас и увести счётчик ниже нуля.
func (b *Atomic) Take(ops int64, class model.LimitKind) bool {
	if ops <= 0 {
		return true
	}
	for {
		current := b.remaining.Load()
		if class != model.LimitMust && (current < b.reserve || ops > current-b.reserve) {
			return false
		}
		if b.remaining.CompareAndSwap(current, current-ops) {
			return true
		}
	}
}

// Reset начинает новое окно. Время окна хранится снаружи.
func (b *Atomic) Reset(limit int64) {
	b.remaining.Store(limit)
}

// Remaining возвращает остаток. Минус означает, что обязательный хедж
// вышел за обычный лимит окна.
func (b *Atomic) Remaining() int64 {
	return b.remaining.Load()
}
