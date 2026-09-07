package hedges_test

import (
	"slices"
	"testing"
	"time"

	"QuantCore/strategies/execengine2/internal/hedges"
)

func TestUnknownBackoffIsPerOrderAndUsesConfiguredCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 7, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	m.AddUnknown("first", hedgeRequest(2), false, now.Add(time.Second))
	m.AddUnknown("second", hedgeRequest(1), true, now.Add(4*time.Second))
	for _, tick := range []struct {
		second int
		ids    []string
	}{
		{0, nil},
		{1, []string{"first"}},
		{2, nil},
		{3, []string{"first"}},
		{4, []string{"second"}},
		{5, nil},
		{6, []string{"second"}},
		{7, []string{"first"}},
		{10, []string{"second"}},
		{11, []string{"first"}},
		// A delayed event loop performs one probe, without a catch-up burst.
		{100, []string{"first", "second"}},
		{100, nil},
		{103, nil},
		{104, []string{"first", "second"}},
	} {
		due := m.UnknownDue(now.Add(time.Duration(tick.second)*time.Second), time.Second, 4*time.Second)
		ids := make([]string, 0, len(due))
		for _, pending := range due {
			ids = append(ids, pending.ClientID)
		}
		if !slices.Equal(ids, tick.ids) {
			t.Fatalf("at %ds due=%v, want %v", tick.second, ids, tick.ids)
		}
	}
}
