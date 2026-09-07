package finambroker

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/strategies/execengine2"
)

func TestGatewayAlreadyExistsMakesPlacementLookupOnly(t *testing.T) {
	cases := []struct {
		name            string
		duplicateAt     int
		resume          bool
		cancelDuplicate bool
	}{
		{name: "initial send", duplicateAt: 1},
		{name: "initial retry", duplicateAt: 2},
		{name: "resumed send", duplicateAt: 2, resume: true},
		{name: "initial canceled reply", duplicateAt: 1, cancelDuplicate: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := limitBuy()
			req.Kind, req.Price, req.Role = execengine2.OrderMarket, 0, execengine2.RoleHedge
			boundary := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
			now := boundary.Add(-time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := &recoveryLimit{t: t, allowedCalls: sendTries}
			placeCalls, findCalls := 0, 0
			var originalID string
			var lookupErr error
			visible := false
			duplicate := status.Error(codes.AlreadyExists, "duplicate client_order_id")
			api := &fakeAPI{
				place: func(_ context.Context, gotReq execengine2.OrderRequest, clientID string) (apiOrder, error) {
					placeCalls++
					if originalID == "" {
						originalID = clientID
					}
					if gotReq != req || clientID != originalID {
						t.Fatal("resend changed the original request or client ID")
					}
					if placeCalls >= tc.duplicateAt {
						if tc.cancelDuplicate {
							cancel()
						}
						return apiOrder{}, duplicate
					}
					return apiOrder{}, status.Error(codes.Unavailable, "initial reply lost")
				},
				find: func(_ context.Context, clientID string) (apiOrder, bool, error) {
					findCalls++
					if clientID != originalID {
						t.Fatal("lookup changed the original client ID")
					}
					return recoveryOrder(req), visible, lookupErr
				},
			}
			g := &Gateway{api: api, ids: testIDs(t), limit: budget, waiter: waitFunc(fastWait), now: func() time.Time { return now }}
			if tc.resume {
				g.limit = noRetryLimit{}
			}
			_, err := g.Place(ctx, req)
			requireUnknownClientID(t, err, originalID)
			if tc.resume {
				g.limit = budget
				_, err = g.ResumePlacement(context.Background(), req, originalID)
				requireUnknownClientID(t, err, originalID)
			}
			if !errors.Is(err, duplicate) || placeCalls != tc.duplicateAt || budget.calls != tc.duplicateAt-1 {
				t.Fatalf("after duplicate: error = %v, sends = %d, budget calls = %d", err, placeCalls, budget.calls)
			}
			if tc.cancelDuplicate && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation after duplicate: %v", err)
			}
			// A duplicate is evidence the ID is occupied, even when subsequent
			// lookups fail. It never permits another send or a definitive rejection.
			lookupFailure := status.Error(codes.Unavailable, "lookup unavailable")
			for _, failure := range []error{nil, lookupFailure, nil, lookupFailure} {
				lookupErr = failure
				beforeFinds := findCalls
				id, err := g.ResumePlacement(context.Background(), req, originalID)
				requireUnknownClientID(t, err, originalID)
				if id != "" || findCalls != beforeFinds+1 || placeCalls != tc.duplicateAt || budget.calls != tc.duplicateAt-1 {
					t.Fatalf("lookup-only call: id = %q, finds = %d, sends = %d, budget = %d", id, findCalls-beforeFinds, placeCalls, budget.calls)
				}
				if failure != nil && !errors.Is(err, failure) {
					t.Fatalf("lost lookup error: %v", err)
				}
			}
			// The request guard must survive the transition to lookup-only.
			changed := req
			changed.Lots++
			beforeFinds := findCalls
			_, err = g.ResumePlacement(context.Background(), changed, originalID)
			requireUnknownClientID(t, err, originalID)
			if findCalls != beforeFinds {
				t.Fatal("looked up a changed request")
			}
			// Expiry cannot restore resend permission. An eventual matching order
			// is still adoptable through Find without charging the send budget.
			now, lookupErr = boundary, nil
			_, err = g.ResumePlacement(context.Background(), req, originalID)
			requireUnknownClientID(t, err, originalID)
			visible = true
			id, err := g.ResumePlacement(context.Background(), req, originalID)
			if err != nil || id != "recovered-order" || placeCalls != tc.duplicateAt || budget.calls != tc.duplicateAt-1 {
				t.Fatalf("late lookup: id = %q, error = %v, sends = %d, budget = %d", id, err, placeCalls, budget.calls)
			}
			if _, known := g.pendingPlacement(originalID); known {
				t.Fatal("resolved placement was retained")
			}
		})
	}
}

func TestGatewayAlreadyExistsStillProbesDuringPlace(t *testing.T) {
	req := limitBuy()
	budget := &testLimit{allow: true}
	placeCalls, findCalls := 0, 0
	g := &Gateway{ids: testIDs(t), limit: budget, waiter: waitFunc(fastWait), api: &fakeAPI{
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			placeCalls++
			return apiOrder{}, status.Error(codes.AlreadyExists, "duplicate client_order_id")
		},
		find: func(context.Context, string) (apiOrder, bool, error) {
			findCalls++
			return recoveryOrder(req), findCalls == findTries, nil
		},
	}}
	id, err := g.Place(context.Background(), req)
	if err != nil || id != "recovered-order" || placeCalls != 1 || findCalls != findTries || budget.calls != 0 {
		t.Fatalf("id = %q, error = %v, sends = %d, finds = %d, budget = %d", id, err, placeCalls, findCalls, budget.calls)
	}
	if len(g.placements) != 0 {
		t.Fatal("resolved duplicate was retained")
	}
}
