package finambroker

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/strategies/execengine2"
)

func TestGatewayResumeKeepsOriginalDayWindow(t *testing.T) {
	boundaries := []struct {
		name string
		at   time.Time
	}{
		{"Moscow midnight", time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)},
		{"UTC midnight", time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)},
	}
	for _, boundary := range boundaries {
		for _, when := range []string{"before", "at", "next day", "found after expiry"} {
			t.Run(boundary.name+"/"+when, func(t *testing.T) {
				now := boundary.at.Add(-time.Hour)
				req := limitBuy()
				firstLimit := &testLimit{}
				placeCalls, findCalls := 0, 0
				var sentID string
				api := &fakeAPI{
					place: func(_ context.Context, _ execengine2.OrderRequest, clientID string) (apiOrder, error) {
						placeCalls++
						sentID = clientID
						return apiOrder{}, status.Error(codes.Unavailable, "original reply lost")
					},
				}
				g := &Gateway{api: api, ids: testIDs(t), limit: firstLimit, waiter: waitFunc(fastWait), now: func() time.Time { return now }}
				_, err := g.Place(context.Background(), req)
				requireUnknownClientID(t, err, sentID)
				if placeCalls != 1 {
					t.Fatalf("initial sends = %d, want 1", placeCalls)
				}
				originalID := sentID
				recoveryBudget := &recoveryLimit{t: t, allowedCalls: sendTries}
				g.limit = recoveryBudget
				placeCalls = 0
				api.place = func(_ context.Context, gotReq execengine2.OrderRequest, clientID string) (apiOrder, error) {
					placeCalls++
					if clientID != originalID || gotReq != req {
						t.Fatal("resumption changed the original placement")
					}
					return recoveryOrder(req), nil
				}
				api.find = func(_ context.Context, clientID string) (apiOrder, bool, error) {
					findCalls++
					if clientID != originalID {
						t.Fatal("resumption lookup changed the original ID")
					}
					return recoveryOrder(req), when == "found after expiry", nil
				}
				now = boundary.at
				if when == "before" {
					now = now.Add(-time.Nanosecond)
				}
				if when == "next day" || when == "found after expiry" {
					now = now.Add(24 * time.Hour)
				}
				id, err := g.ResumePlacement(context.Background(), req, originalID)
				wantSends := 0
				if when == "before" {
					wantSends = 1
				}
				if when == "before" || when == "found after expiry" {
					if err != nil || id != "recovered-order" {
						t.Fatalf("id = %q, error = %v", id, err)
					}
				} else {
					requireUnknownClientID(t, err, originalID)
				}
				if placeCalls != wantSends || recoveryBudget.calls != wantSends || findCalls != 1 {
					t.Fatalf("sends = %d, budget calls = %d, finds = %d", placeCalls, recoveryBudget.calls, findCalls)
				}
				if _, known := g.pendingPlacement(originalID); known {
					t.Fatal("resolved or expired resend window was retained")
				}
			})
		}
	}
}

func TestGatewayUnknownResendWindowOnlyLooksUp(t *testing.T) {
	limit := &recoveryLimit{t: t, allowedCalls: sendTries}
	findCalls := 0
	g := &Gateway{limit: limit, api: &fakeAPI{
		find: func(context.Context, string) (apiOrder, bool, error) {
			findCalls++
			return apiOrder{}, false, nil
		},
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			t.Fatal("resent an ID whose original day is unknown")
			return apiOrder{}, nil
		},
	}}
	_, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
	requireUnknownClientID(t, err, recoveryClientID)
	if findCalls != 1 || limit.calls != 0 {
		t.Fatalf("finds = %d, budget calls = %d", findCalls, limit.calls)
	}
}

func TestGatewayNewPlacementPrunesExpiredRecoveryWindows(t *testing.T) {
	boundary := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
	now := boundary.Add(-time.Hour)
	api := &fakeAPI{place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
		return apiOrder{}, status.Error(codes.Unavailable, "reply lost")
	}}
	g := &Gateway{api: api, ids: testIDs(t), limit: noRetryLimit{}, waiter: waitFunc(fastWait), now: func() time.Time { return now }}
	placeUnknown := func(req execengine2.OrderRequest) string {
		t.Helper()
		_, err := g.Place(context.Background(), req)
		clientID, ok := execengine2.ErrorClientID(err)
		if !ok || !execengine2.OrderMayExist(err) {
			t.Fatalf("unknown placement lost its client ID: %v", err)
		}
		return clientID
	}
	market := limitBuy()
	market.Kind, market.Price, market.Role = execengine2.OrderMarket, 0, execengine2.RoleHedge
	expired := map[string]execengine2.OrderRequest{
		placeUnknown(limitBuy()): limitBuy(),
		placeUnknown(market):     market,
	}
	// A new insertion just before expiry must retain the original windows.
	now = boundary.Add(-time.Nanosecond)
	expired[placeUnknown(limitBuy())] = limitBuy()
	for clientID, req := range expired {
		pending, known := g.pendingPlacement(clientID)
		if !known || pending.request != req || !pending.deadline.Equal(boundary) {
			t.Fatalf("unexpired placement %s changed: %+v, known = %t", clientID, pending, known)
		}
	}
	// At the deadline, the next unknown placement clears both order kinds.
	now = boundary
	currentLimitID := placeUnknown(limitBuy())
	for clientID := range expired {
		if _, known := g.pendingPlacement(clientID); known {
			t.Fatalf("expired placement %s survived the next insertion", clientID)
		}
	}
	now = now.Add(time.Minute)
	current := map[string]execengine2.OrderRequest{
		currentLimitID:       limitBuy(),
		placeUnknown(market): market,
	}
	if len(g.placements) != len(current) {
		t.Fatalf("recovery windows = %d, want %d", len(g.placements), len(current))
	}
	for clientID, req := range current {
		pending, known := g.pendingPlacement(clientID)
		if !known || pending.request != req || !pending.deadline.Equal(boundary.Add(3*time.Hour)) {
			t.Fatalf("current placement %s changed: %+v, known = %t", clientID, pending, known)
		}
	}
	// Pruning never grants an expired ID a new resend window.
	limit := &recoveryLimit{t: t, allowedCalls: sendTries}
	g.limit = limit
	findCalls := 0
	api.find = func(_ context.Context, clientID string) (apiOrder, bool, error) {
		findCalls++
		if _, known := expired[clientID]; !known {
			t.Fatalf("lookup changed the expired client ID to %s", clientID)
		}
		return apiOrder{}, false, nil
	}
	api.place = func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
		t.Fatal("resent a pruned placement")
		return apiOrder{}, nil
	}
	for clientID, req := range expired {
		_, err := g.ResumePlacement(context.Background(), req, clientID)
		requireUnknownClientID(t, err, clientID)
	}
	if findCalls != len(expired) || limit.calls != 0 {
		t.Fatalf("finds = %d, budget calls = %d", findCalls, limit.calls)
	}
}

func TestGatewayResumeRefusesChangedOriginalRequest(t *testing.T) {
	for _, field := range []string{"symbol", "side", "lots", "price", "role"} {
		t.Run(field, func(t *testing.T) {
			req := limitBuy()
			limit := &recoveryLimit{t: t, allowedCalls: sendTries}
			g := &Gateway{limit: limit, api: &fakeAPI{
				place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
					t.Fatal("resent a changed request with the original ID")
					return apiOrder{}, nil
				},
			}}
			rememberRecoveryPlacement(g, req)
			switch field {
			case "symbol":
				req.Symbol = "GAZP"
			case "side":
				req.Side = execengine2.SideSell
			case "lots":
				req.Lots++
			case "price":
				req.Price++
			case "role":
				req.Role = execengine2.RoleHedge
			}
			_, err := g.ResumePlacement(context.Background(), req, recoveryClientID)
			requireUnknownClientID(t, err, recoveryClientID)
			if limit.calls != 0 {
				t.Fatalf("mismatched request consumed %d send reservations", limit.calls)
			}
		})
	}
}

type noRetryLimit struct{}

func (noRetryLimit) Take(int64, execengine2.LimitKind) bool { return false }
func (noRetryLimit) Remaining() int64                       { return 0 }

func TestGatewayConcurrentPlacementsKeepIndependentRecoveryWindows(t *testing.T) {
	const count = 64
	req := limitBuy()
	api := &fakeAPI{place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
		return apiOrder{}, status.Error(codes.Unavailable, "reply lost")
	}}
	g := &Gateway{api: api, ids: testIDs(t), limit: noRetryLimit{}, waiter: waitFunc(fastWait)}
	results := make(chan error, count)
	for range count {
		go func() {
			_, err := g.Place(context.Background(), req)
			results <- err
		}()
	}
	clientIDs := make([]string, 0, count)
	for range count {
		err := <-results
		clientID, ok := execengine2.ErrorClientID(err)
		if !ok || !execengine2.OrderMayExist(err) {
			t.Fatalf("unknown placement lost its client ID: %v", err)
		}
		clientIDs = append(clientIDs, clientID)
	}
	if len(g.placements) != count {
		t.Fatalf("recovery windows = %d, want %d", len(g.placements), count)
	}
	api.find = func(context.Context, string) (apiOrder, bool, error) {
		return recoveryOrder(req), true, nil
	}
	for _, clientID := range clientIDs {
		go func() {
			_, err := g.ResumePlacement(context.Background(), req, clientID)
			results <- err
		}()
	}
	for range count {
		if err := <-results; err != nil {
			t.Fatalf("concurrent recovery failed: %v", err)
		}
	}
	if len(g.placements) != 0 {
		t.Fatalf("retained %d resolved windows", len(g.placements))
	}
}

func TestGatewayDoesNotRetryAcrossDayBoundary(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary placement", true: "resumed placement"}[resume], func(t *testing.T) {
			boundary := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
			now := boundary.Add(-time.Second)
			limit := &testLimit{allow: true}
			placeCalls, findCalls := 0, 0
			clientID := recoveryClientID
			g := &Gateway{ids: testIDs(t), limit: limit, waiter: waitFunc(fastWait), now: func() time.Time { return now }, api: &fakeAPI{
				place: func(_ context.Context, _ execengine2.OrderRequest, id string) (apiOrder, error) {
					placeCalls++
					clientID = id
					now = boundary
					return apiOrder{}, status.Error(codes.Unavailable, "reply lost at midnight")
				},
				find: func(context.Context, string) (apiOrder, bool, error) {
					findCalls++
					return apiOrder{}, false, nil
				},
			}}
			var err error
			wantLimitCalls := 0
			wantFindCalls := findTries
			if resume {
				g.rememberPlacement(clientID, limitBuy(), placementRetryDeadline(now))
				_, err = g.ResumePlacement(context.Background(), limitBuy(), clientID)
				wantLimitCalls = 1
				wantFindCalls = 1
			} else {
				_, err = g.Place(context.Background(), limitBuy())
			}
			requireUnknownClientID(t, err, clientID)
			if placeCalls != 1 || limit.calls != wantLimitCalls || findCalls != wantFindCalls {
				t.Fatalf("sends = %d, budget calls = %d, finds = %d", placeCalls, limit.calls, findCalls)
			}
		})
	}
}

func TestGatewayResumeCannotExtendOriginalWindow(t *testing.T) {
	boundary := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
	now := boundary.Add(-time.Hour)
	limit := &recoveryLimit{t: t, allowedCalls: sendTries}
	placeCalls := 0
	g := &Gateway{limit: limit, waiter: waitFunc(fastWait), now: func() time.Time { return now }, api: &fakeAPI{
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			placeCalls++
			return apiOrder{}, status.Error(codes.Unavailable, "reply lost")
		},
	}}
	g.rememberPlacement(recoveryClientID, limitBuy(), placementRetryDeadline(now))
	for call := 1; call <= 3; call++ {
		_, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
		requireUnknownClientID(t, err, recoveryClientID)
		if placeCalls != call {
			t.Fatalf("resends before expiry = %d, want %d", placeCalls, call)
		}
	}
	now = boundary
	placeCalls = 0
	limit.calls = 0
	_, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
	requireUnknownClientID(t, err, recoveryClientID)
	if placeCalls != 0 || limit.calls != 0 {
		t.Fatalf("sends after expiry = %d, budget calls = %d", placeCalls, limit.calls)
	}
}
