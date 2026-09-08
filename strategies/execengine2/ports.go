package execengine2

import (
	"context"
	"time"
)

// Broker — единая точка работы движка с брокером.
// Context приходит от вызывающего кода и не заменяется внутри.
// Place должен поддерживать два одновременных вызова для ног ModeMarket.
// Engine ждёт оба ответа до изменения состояния или других вызовов Broker.
type Broker interface {
	Place(ctx context.Context, req OrderRequest) (orderID string, err error)
	Cancel(ctx context.Context, orderID string) (CancelResult, error)
	Status(ctx context.Context, orderID string) (OrderStatus, error)
}

// SendLimit проверяет и сразу списывает попытки отправки заявки.
// Optional Allow(int64) bool enables v1 admission: check first, then book each
// RPC after its return with Take(..., LimitMust). Such a limit must accept that
// mandatory booking, including a negative remaining balance. RetryAfter(int64)
// time.Duration optionally supplies the quota-reset delay. Quota implements both.
type SendLimit interface {
	Take(ops int64, class LimitKind) bool
	Remaining() int64
}

// Clock даёт движку текущее время.
type Clock interface {
	Now() time.Time
}

// Strategy читает сигнал и хранит позицию стратегии.
type Strategy interface {
	Peek(Signal) Plan
	Commit(Plan, time.Time) Result
	Position() int
}

// Saver сохраняет позицию, если стратегия это умеет.
type Saver interface {
	SaveLots()
}

// Updates принимает изменения позиции и цены сделки.
type Updates interface {
	Apply(PositionChange)
	Amend(PriceChange)
}

// Logger принадлежит одному движку и содержит его метку в сообщениях.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Criticalf(format string, args ...any)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type noLimit struct{}

func (noLimit) Take(int64, LimitKind) bool { return true }
func (noLimit) Remaining() int64           { return int64(^uint64(0) >> 1) }
