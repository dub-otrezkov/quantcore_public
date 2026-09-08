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
// A limit that also implements Admitter uses its check-then-book contract instead.
type SendLimit interface {
	Take(ops int64, class LimitKind) bool
	Remaining() int64
}

// Admitter is the optional SendLimit contract for v1-compatible admission.
// Allow checks without spending; the engine books each actual RPC after its
// return with Take(..., LimitMust). Mandatory booking must succeed even when
// the remaining balance becomes negative. As with all Go interfaces, a matching
// method satisfies this contract structurally; no registration is required.
type Admitter interface {
	Allow(ops int64) bool
}

// RetryDelayer optionally gives a denied SendLimit's delay for the same number
// of attempts it just checked. The engine adds this delay to its event time.
type RetryDelayer interface {
	RetryAfter(ops int64) time.Duration
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
