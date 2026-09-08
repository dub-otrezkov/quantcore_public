package execengine2_test

import (
	"reflect"
	"testing"
)

// Terminal-dead status is a deliberate wire-level difference: v1's dead flag has
// no executed count, so it retires the already-terminal maker to obtain it.
// v2 receives Done/Filled and cancels only a still-live sibling. These exact
// extra v1 cancels are asserted here; the general parity test filters nothing.
func TestV1V2MakerStatusParity(t *testing.T) {
	for _, tc := range []struct {
		name           string
		events         int
		oldCancels     []string
		currentCancels []string
	}{
		{"maker_terminal_partial", 7, []string{"cancel 1"}, nil},
		{"maker_terminal_unfilled", 6, []string{"cancel 1"}, nil},
		{"maker_terminal_dual", 6, []string{"cancel 1", "cancel 2"}, []string{"cancel 2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := runParityCase(t, true, tc.name)
			current := runParityCase(t, false, tc.name)
			if len(old) != tc.events || len(current) != tc.events {
				t.Fatalf("event count: v1=%d v2=%d, want %d", len(old), len(current), tc.events)
			}
			terminal := tc.events - 2
			prefix := old[terminal-1].Calls
			oldCalls := append(append([]string(nil), prefix...), tc.oldCancels...)
			currentCalls := append(append([]string(nil), prefix...), tc.currentCancels...)
			for event := range old {
				before, after := old[event], current[event]
				if event >= terminal {
					if !reflect.DeepEqual(before.Calls, oldCalls) || !reflect.DeepEqual(after.Calls, currentCalls) {
						t.Fatalf("unexpected terminal RPCs after event %d\nv1: %v, want %v\nv2: %v, want %v", event+1, before.Calls, oldCalls, after.Calls, currentCalls)
					}
					// Both full RPC traces have already matched the exact case-specific
					// expectations. Every other observable must remain identical.
					before.Calls = after.Calls
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("behavior differs after event %d\nv1: %+v\nv2: %+v", event+1, before, after)
				}
			}
			for version, trace := range map[string][]parityOutcome{"v1": old, "v2": current} {
				if !reflect.DeepEqual(trace[terminal-2], trace[terminal-1]) {
					t.Fatalf("%s nonterminal status changed the working trade", version)
				}
				if !reflect.DeepEqual(trace[terminal], trace[terminal+1]) {
					t.Fatalf("%s repeated terminal status was not a no-op", version)
				}
				if state := trace[terminal]; state.Working || state.LiveMakers != 0 || state.Halted {
					t.Fatalf("%s terminal maker did not leave an idle, unhalted trade: %+v", version, state)
				}
			}
		})
	}
}

func TestV1V2FilledMakerStatusWaitsForFill(t *testing.T) {
	old := runParityCase(t, true, "maker_filled_status_before_fill")
	current := runParityCase(t, false, "maker_filled_status_before_fill")
	if len(old) != 5 || len(current) != 5 {
		t.Fatalf("event count: v1=%d v2=%d, want 5", len(old), len(current))
	}
	for event := range old {
		if !reflect.DeepEqual(old[event], current[event]) {
			t.Fatalf("behavior differs after event %d\nv1: %+v\nv2: %+v", event+1, old[event], current[event])
		}
	}
	// IsDeadStatus(FILLED/EXECUTED) is false even though Broker.Status is done.
	// Passing generic terminality to the maker stream would drop this clip and
	// turn its real fill into an uncommitted late fill instead of a normal commit.
	status := current[3]
	if !status.Working || len(status.Accounting) != 0 || len(status.Commits) != 0 || status.Saves != 0 || !reflect.DeepEqual(status.Calls, current[2].Calls) {
		t.Fatalf("FILLED stream status acted before its fill: %+v", status)
	}
	filled := current[4]
	if filled.Working || filled.Position != 6 || filled.Saves != 1 || !reflect.DeepEqual(filled.Commits, []int{6}) {
		t.Fatalf("fill after FILLED status failed to commit normally: %+v", filled)
	}
}

// A richer status event saves a v1 confirmation RPC; it must not reorder
// accounting callbacks or postpone the hedge owed by that very event.
func TestV1V2DeadStatusAccountingParity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		events      int
		terminal    int
		oldRPCs     []string
		currentRPCs []string
	}{
		{
			"maker_dead_b_before_stream", 7, 3,
			[]string{"cancel 1", "cancel 2", "place A market buy=true lots=2 price=0"},
			[]string{"cancel 1", "place A market buy=true lots=2 price=0"},
		},
		{
			"taker_dead_short_same_event", 8, 4,
			[]string{"status 2", "place B market buy=false lots=4 price=0"},
			[]string{"place B market buy=false lots=4 price=0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := runParityCase(t, true, tc.name)
			current := runParityCase(t, false, tc.name)
			if len(old) != tc.events || len(current) != tc.events {
				t.Fatalf("event count: v1=%d v2=%d, want %d", len(old), len(current), tc.events)
			}
			prefix := old[tc.terminal-1].Calls
			oldCalls := append(append([]string(nil), prefix...), tc.oldRPCs...)
			currentCalls := append(append([]string(nil), prefix...), tc.currentRPCs...)
			for event := range old {
				before, after := old[event], current[event]
				if event >= tc.terminal {
					if !reflect.DeepEqual(before.Calls, oldCalls) || !reflect.DeepEqual(after.Calls, currentCalls) {
						t.Fatalf("status did not issue the exact immediate RPCs after event %d\nv1: %v, want %v\nv2: %v, want %v", event+1, before.Calls, oldCalls, after.Calls, currentCalls)
					}
					before.Calls = after.Calls // only the explicitly asserted confirmation RPC differs
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("status accounting differs after event %d\nv1: %+v\nv2: %+v", event+1, before, after)
				}
			}
			for version, trace := range map[string][]parityOutcome{"v1": old, "v2": current} {
				if !reflect.DeepEqual(trace[tc.terminal], trace[tc.terminal+1]) {
					t.Fatalf("%s duplicate dead status repeated an effect", version)
				}
			}
		})
	}
}

func TestV1V2DeadTakerStreakParity(t *testing.T) {
	old := runParityCase(t, true, "taker_dead_streak")
	current := runParityCase(t, false, "taker_dead_streak")
	// The first two terminal shortfalls retry immediately; the third becomes
	// paced debt. Only v1 needs a Status RPC to obtain each terminal fill count.
	rpcEvents := []struct{ old, current []string }{
		{}, {},
		{[]string{"place A limit buy=true lots=6 price=100"}, []string{"place A limit buy=true lots=6 price=100"}},
		{[]string{"place B market buy=false lots=6 price=0", "cancel 1"}, []string{"place B market buy=false lots=6 price=0", "cancel 1"}},
		{[]string{"status 2", "place B market buy=false lots=6 price=0"}, []string{"place B market buy=false lots=6 price=0"}},
		{[]string{"status 3", "place B market buy=false lots=6 price=0"}, []string{"place B market buy=false lots=6 price=0"}},
		{[]string{"status 4"}, nil},
		{}, // RetryWait has not elapsed at one second.
		{[]string{"place B market buy=false lots=6 price=0"}, []string{"place B market buy=false lots=6 price=0"}},
	}
	if len(old) != len(rpcEvents) || len(current) != len(rpcEvents) {
		t.Fatalf("event count: v1=%d v2=%d, want %d", len(old), len(current), len(rpcEvents))
	}
	var oldCalls, currentCalls []string
	for event, rpc := range rpcEvents {
		oldCalls = append(oldCalls, rpc.old...)
		currentCalls = append(currentCalls, rpc.current...)
		before, after := old[event], current[event]
		if !reflect.DeepEqual(before.Calls, oldCalls) || !reflect.DeepEqual(after.Calls, currentCalls) {
			t.Fatalf("unexpected retry RPCs after event %d\nv1: %v, want %v\nv2: %v, want %v", event+1, before.Calls, oldCalls, after.Calls, currentCalls)
		}
		before.Calls = after.Calls // the exact three v1-only Status RPCs were asserted above
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("retry accounting differs after event %d\nv1: %+v\nv2: %+v", event+1, before, after)
		}
	}
}
