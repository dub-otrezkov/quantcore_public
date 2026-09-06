package run_test

import (
	"testing"
	"time"

	"QuantCore/strategies/execengine2/internal/run"
)

func TestFixNeedsPositionCheck(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	var m run.State
	if !m.CanOpen() {
		t.Fatal("zero-value machine was not healthy")
	}
	m.StartFix(now, time.Second, "cancel unresolved")
	if m.CanOpen() || m.FixDue(now) {
		t.Fatal("recovery transition or pacing is wrong")
	}
	if !m.FixDue(now.Add(time.Second)) {
		t.Fatal("retry did not become due")
	}
	m.FixDone(now.Add(time.Second), 0, true, time.Second, time.Minute)
	if got := m.Info().Code; got != run.CheckNeeded {
		t.Fatalf("state = %s, want awaiting_reconcile", got)
	}
	if m.CheckPositions(1, -1, 0, 0, false) {
		t.Fatal("mismatched position became healthy")
	}
	if !m.CheckPositions(0, 0, 0, 0, false) || !m.CanOpen() {
		t.Fatal("matching reconciliation did not restore healthy")
	}
}

func TestFixWaitAndStop(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var m run.State
	m.StartFix(now, time.Second, "broker down")
	m.FixDone(now.Add(time.Second), 2, false, time.Second, 4*time.Second)
	if got := m.Info().Wait; got != 2*time.Second {
		t.Fatalf("retry gap = %s, want 2s", got)
	}
	m.FixDone(now.Add(3*time.Second), 1, false, time.Second, 4*time.Second)
	if got := m.Info().Wait; got != 4*time.Second {
		t.Fatalf("retry gap = %s, want 4s", got)
	}
	if !m.Stop("manual") || m.Info().Code != run.Stopped {
		t.Fatal("halt transition failed")
	}
	m.StartFix(now, time.Second, "must not resurrect")
	if m.Info().Code != run.Stopped {
		t.Fatal("halted machine resurrected")
	}
}

func TestPositionChecksPreserveOutstandingRetries(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var m run.State
	m.StartFix(now, time.Second, "hedge debt")
	m.NeedCheck("another placement became unknown")
	if m.CheckPositions(0, 0, 0, 0, true) || !m.FixDue(now.Add(time.Second)) {
		t.Fatal("reconciliation discarded pending retry work")
	}
	m.FixDone(now.Add(time.Second), 1, false, time.Second, time.Minute)
	if m.FixDue(now.Add(2*time.Second)) || !m.FixDue(now.Add(3*time.Second)) {
		t.Fatal("reconciliation disabled retry backoff")
	}
	m.Stop("manual")
	if m.FixDue(now.Add(time.Hour)) {
		t.Fatal("stopped state allowed a retry")
	}
}
