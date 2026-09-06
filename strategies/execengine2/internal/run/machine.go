// Package run хранит общее состояние движка и время следующей попытки.
package run

import "time"

// Code — одно явное состояние движка вместо набора bool.
type Code uint8

const (
	Ready Code = iota
	Fixing
	CheckNeeded
	Stopped
)

func (s Code) String() string {
	switch s {
	case Ready:
		return "ready"
	case Fixing:
		return "fixing"
	case CheckNeeded:
		return "check_needed"
	case Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Info — копия общего состояния движка.
type Info struct {
	Code    Code
	Reason  string
	NextTry time.Time
	Wait    time.Duration
}

// State можно использовать с нулевым значением: оно равно Ready.
type State struct {
	code    Code
	reason  string
	nextTry time.Time
	wait    time.Duration
}

// CanOpen проверяет, можно ли начать новую сделку.
func (m *State) CanOpen() bool { return m.code == Ready }

// StartFix запрещает новые сделки и ставит работу на повтор.
func (m *State) StartFix(now time.Time, firstWait time.Duration, reason string) {
	if m.code == Stopped {
		return
	}
	if firstWait <= 0 {
		firstWait = time.Second
	}
	m.code = Fixing
	m.reason = reason
	if m.wait <= 0 {
		m.wait = firstWait
	}
	if m.nextTry.IsZero() || now.Add(m.wait).Before(m.nextTry) {
		m.nextTry = now.Add(m.wait)
	}
}

// FixDue проверяет время следующей попытки.
func (m *State) FixDue(now time.Time) bool {
	return m.code != Stopped && !m.nextTry.IsZero() && !now.Before(m.nextTry)
}

// FixDone выбирает время следующей попытки или просит проверить позиции.
func (m *State) FixDone(
	now time.Time,
	left int,
	didWork bool,
	firstWait time.Duration,
	maxWait time.Duration,
) {
	if m.code == Stopped || m.nextTry.IsZero() {
		return
	}
	if left == 0 {
		m.code = CheckNeeded
		m.reason = "broker work is done; position check is needed"
		m.nextTry = time.Time{}
		m.wait = 0
		return
	}
	if firstWait <= 0 {
		firstWait = time.Second
	}
	if maxWait < firstWait {
		maxWait = firstWait
	}
	if didWork || m.wait <= 0 {
		m.wait = firstWait
	} else {
		m.wait = min(m.wait*2, maxWait)
	}
	m.nextTry = now.Add(m.wait)
}

// NeedCheck запрещает новые сделки до проверки позиций.
func (m *State) NeedCheck(reason string) {
	if m.code == Stopped {
		return
	}
	m.code = CheckNeeded
	m.reason = reason
}

// CheckPositions возвращает Ready только при равных позициях и без другой работы.
func (m *State) CheckPositions(brokerA, brokerB, strategyA, strategyB int, hasWork bool) bool {
	if m.code == Stopped {
		return false
	}
	if hasWork {
		// A position poll must not discard scheduled cancel/hedge retries.
		return false
	}
	if brokerA != strategyA || brokerB != strategyB {
		m.code = CheckNeeded
		m.reason = "broker and engine positions differ"
		return false
	}
	m.code = Ready
	m.reason = ""
	m.nextTry = time.Time{}
	m.wait = 0
	return true
}

// Stop полностью останавливает движок.
func (m *State) Stop(reason string) bool {
	if m.code == Stopped {
		return false
	}
	m.code = Stopped
	m.reason = reason
	m.nextTry = time.Time{}
	m.wait = 0
	return true
}

// Info возвращает копию состояния.
func (m *State) Info() Info {
	return Info{Code: m.code, Reason: m.reason, NextTry: m.nextTry, Wait: m.wait}
}
