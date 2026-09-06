package hedges

import (
	"time"

	"QuantCore/strategies/execengine2/internal/model"
)

// Unknown keeps the original request and idempotency key until the broker
// acknowledges it. Counted records an earlier provisional inventory credit.
type Unknown struct {
	ID       uint64
	ClientID string
	Request  model.OrderRequest
	Counted  bool
	NextTry  time.Time
}

func (m *List) AddUnknown(clientID string, req model.OrderRequest, counted bool, nextTry time.Time) {
	m.nextID++
	m.unknown = append(m.unknown, Unknown{
		ID: m.nextID, ClientID: clientID, Request: req, Counted: counted, NextTry: nextTry,
	})
}

// UnknownDue returns copies and paces retries independently of position checks.
func (m *List) UnknownDue(now time.Time, wait time.Duration) []Unknown {
	due := make([]Unknown, 0, len(m.unknown))
	for i := range m.unknown {
		if now.Before(m.unknown[i].NextTry) {
			continue
		}
		m.unknown[i].NextTry = now.Add(wait)
		due = append(due, m.unknown[i])
	}
	return due
}

func (m *List) ResolveUnknown(id uint64) {
	for i, pending := range m.unknown {
		if pending.ID == id {
			m.unknown = append(m.unknown[:i], m.unknown[i+1:]...)
			return
		}
	}
}

func (m *List) UnknownCount() int { return len(m.unknown) }

func (m *List) UnknownOrders() []Unknown {
	return append([]Unknown(nil), m.unknown...)
}

// ConfirmCounted releases only orders whose entire provisional volume has
// been confirmed by an authoritative inventory snapshot. Uncredited opening
// requests cannot be inferred absent from a matching baseline position.
func (m *List) ConfirmCounted() {
	remaining := m.unknown[:0]
	for _, pending := range m.unknown {
		if !pending.Counted {
			remaining = append(remaining, pending)
		}
	}
	m.unknown = remaining
}
