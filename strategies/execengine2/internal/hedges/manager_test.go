package hedges_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/internal/hedges"
)

func hedgeRequest(lots int) execengine2.OrderRequest {
	return execengine2.OrderRequest{
		Symbol: "B", Side: execengine2.SideSell, Kind: execengine2.OrderMarket,
		Role: execengine2.RoleHedge, Leg: execengine2.LegB, Lots: lots, TradeID: 7,
	}
}

func TestMarketCheckFindsMissingLots(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	if err := m.AddMarket("t1", hedgeRequest(5), now); err != nil {
		t.Fatal(err)
	}
	if got := m.Checks(now.Add(time.Second), 2*time.Second, time.Second, time.Minute); len(got) != 0 {
		t.Fatalf("early probes = %v", got)
	}
	probes := m.Checks(now.Add(2*time.Second), 2*time.Second, time.Second, time.Minute)
	if len(probes) != 1 || probes[0].OrderID != "t1" {
		t.Fatalf("probes = %+v", probes)
	}
	result := m.SetStatus("t1", execengine2.OrderStatus{Filled: 3, Done: true})
	if !result.Known || result.Done || result.Missing.Lots != 2 ||
		result.Missing.Role != execengine2.RoleFix {
		t.Fatalf("status result = %+v", result)
	}
}

func TestAllReturnsCopy(t *testing.T) {
	t.Parallel()
	var m hedges.List
	id := m.Add(hedgeRequest(2), errors.New("transport"))
	debts := m.All()
	if len(debts) != 1 || debts[0].ID != id || debts[0].LastErr != "transport" {
		t.Fatalf("debts = %+v", debts)
	}
	debts[0].Request.Lots = 99
	if got := m.All()[0].Request.Lots; got != 2 {
		t.Fatalf("caller mutated debt lots to %d", got)
	}
	if !m.Done(id) || m.HasWork() {
		t.Fatal("resolved debt remained outstanding")
	}
}

func TestFillEndsMarketCheck(t *testing.T) {
	t.Parallel()
	var m hedges.List
	if err := m.AddMarket("t1", hedgeRequest(2), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if m.SeeFill("t1", 1) {
		t.Fatal("partial report confirmed taker")
	}
	if !m.SeeFill("t1", 2) || m.MarketCount() != 0 {
		t.Fatal("full report did not confirm taker")
	}
}

func TestMarketCheckBackoffIsPerOrderAndUsesConfiguredCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 8, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	for id, delay := range map[string]time.Duration{"a": 0, "b": 4 * time.Second} {
		if err := m.AddMarket(id, hedgeRequest(2), now.Add(delay)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tick := range []struct {
		second int
		ids    []string
	}{
		{0, nil},
		{1, nil},
		{2, []string{"a"}}, // Initial grace, then the configured first interval.
		{3, []string{"a"}},
		{4, nil},
		{5, []string{"a"}},
		{6, []string{"b"}},
		{7, []string{"b"}},
		{8, nil},
		{9, []string{"a", "b"}},
		{12, nil},
		{13, []string{"a", "b"}},
		{100, []string{"a", "b"}}, // A delayed tick does not cause catch-up bursts.
		{100, nil},
		{103, nil},
		{104, []string{"a", "b"}},
	} {
		checks := m.Checks(now.Add(time.Duration(tick.second)*time.Second), 2*time.Second, time.Second, 4*time.Second)
		ids := make([]string, 0, len(checks))
		for _, check := range checks {
			ids = append(ids, check.OrderID)
			// Healthy pending responses and partial fills must not restart the
			// polling rate while the remaining volume is still unresolved.
			if result := m.SetStatus(check.OrderID, execengine2.OrderStatus{Filled: 1}); !result.Known || result.Done {
				t.Fatalf("pending status = %+v", result)
			}
			if m.SeeFill(check.OrderID, 1) {
				t.Fatal("partial fill removed unresolved market")
			}
		}
		if !slices.Equal(ids, tick.ids) {
			t.Fatalf("at %ds checks=%v, want %v", tick.second, ids, tick.ids)
		}
	}
	if !m.SeeFill("a", 2) {
		t.Fatal("full fill did not clear market check")
	}
	if result := m.SetStatus("b", execengine2.OrderStatus{Filled: 1, Done: true}); !result.Known || result.Missing.Lots != 1 {
		t.Fatalf("terminal partial status = %+v", result)
	}
	if checks := m.Checks(now.Add(time.Hour), 2*time.Second, time.Second, 4*time.Second); len(checks) != 0 || m.MarketCount() != 0 {
		t.Fatalf("terminal orders retained polling schedules: %+v", checks)
	}
}

func TestMarketCheckCapCannotShortenFirstInterval(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 8, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	if err := m.AddMarket("a", hedgeRequest(2), now); err != nil {
		t.Fatal(err)
	}
	for _, second := range []int{1, 5, 9} {
		at := now.Add(time.Duration(second) * time.Second)
		if checks := m.Checks(at.Add(-time.Nanosecond), time.Second, 4*time.Second, time.Second); len(checks) != 0 {
			t.Fatalf("polled early at %s", at.Add(-time.Nanosecond))
		}
		if checks := m.Checks(at, time.Second, 4*time.Second, time.Second); len(checks) != 1 {
			t.Fatalf("missed check at %s", at)
		}
	}
}

func TestMarketCheckBackoffCannotOverflow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 8, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	if err := m.AddMarket("a", hedgeRequest(2), now); err != nil {
		t.Fatal(err)
	}
	firstEvery := time.Duration(1 << 62)
	maxEvery := time.Duration(1<<63 - 1)
	next := now.Add(time.Second)
	for _, interval := range []time.Duration{firstEvery, maxEvery, maxEvery} {
		if checks := m.Checks(next, time.Second, firstEvery, maxEvery); len(checks) != 1 {
			t.Fatalf("missed check at %s", next)
		}
		next = next.Add(interval)
		if checks := m.Checks(next.Add(-time.Nanosecond), time.Second, firstEvery, maxEvery); len(checks) != 0 {
			t.Fatal("large retry cap overflowed into an early poll")
		}
	}
}
