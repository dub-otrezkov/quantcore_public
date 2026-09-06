package finambroker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/strategies/execengine2"
)

const recoveryClientID = "qaaaaaaaaaaaa0000001"

type recoveryContextKey struct{}

func recoveryOrder(req execengine2.OrderRequest) apiOrder {
	return apiOrder{
		id: "recovered-order", symbol: req.Symbol, side: req.Side, kind: req.Kind,
		lots: req.Lots, price: req.Price,
	}
}

func rememberRecoveryPlacement(g *Gateway, req execengine2.OrderRequest) {
	g.now = func() time.Time { return time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC) }
	g.rememberPlacement(recoveryClientID, req, placementRetryDeadline(g.timeNow()))
}

func requireUnknownClientID(t *testing.T, err error, clientID string) {
	t.Helper()
	if err == nil || !execengine2.OrderMayExist(err) {
		t.Fatalf("error = %v, want unresolved placement", err)
	}
	if got, ok := execengine2.ErrorClientID(err); !ok || got != clientID {
		t.Fatalf("error client ID = %q, present = %v, want %q", got, ok, clientID)
	}
}

type recoveryLimit struct {
	t            *testing.T
	allowedCalls int
	calls        int
}

func (l *recoveryLimit) Take(ops int64, kind execengine2.LimitKind) bool {
	l.t.Helper()
	if ops != 1 || kind != execengine2.LimitMust {
		l.t.Fatalf("recovery limit Take(%d, %d), want (1, LimitMust)", ops, kind)
	}
	l.calls++
	return l.calls <= l.allowedCalls
}

func (*recoveryLimit) Remaining() int64 { return 0 }

func TestGatewayResumeFindsWithoutSpendingSendBudget(t *testing.T) {
	req := limitBuy()
	ctx := context.WithValue(context.Background(), recoveryContextKey{}, "caller")
	limit := &recoveryLimit{t: t}
	findCalls := 0
	api := &fakeAPI{
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			t.Fatal("an already resolved order must not be resent")
			return apiOrder{}, nil
		},
		find: func(got context.Context, clientID string) (apiOrder, bool, error) {
			findCalls++
			if got != ctx || clientID != recoveryClientID {
				t.Fatalf("lookup changed context or client ID: %q", clientID)
			}
			return recoveryOrder(req), true, nil
		},
	}
	// Recovery must work without an ID generator: it owns an existing ID.
	g := &Gateway{api: api, limit: limit}
	id, err := g.ResumePlacement(ctx, req, recoveryClientID)
	if err != nil || id != "recovered-order" || findCalls != 1 || limit.calls != 0 {
		t.Fatalf("id = %q, error = %v, finds = %d, budget calls = %d", id, err, findCalls, limit.calls)
	}
}

func TestGatewayResumeResendsOriginalIDAndChargesEverySend(t *testing.T) {
	req := limitBuy()
	req.Role = execengine2.RoleTrade
	ctx := context.WithValue(context.Background(), recoveryContextKey{}, "caller")
	limit := &recoveryLimit{t: t, allowedCalls: sendTries}
	findCalls, placeCalls := 0, 0
	api := &fakeAPI{
		find: func(got context.Context, clientID string) (apiOrder, bool, error) {
			findCalls++
			if got != ctx || clientID != recoveryClientID {
				t.Fatalf("lookup changed context or client ID: %q", clientID)
			}
			return apiOrder{}, false, nil
		},
		place: func(got context.Context, gotReq execengine2.OrderRequest, clientID string) (apiOrder, error) {
			placeCalls++
			if findCalls == 0 || got != ctx || gotReq != req || clientID != recoveryClientID {
				t.Fatalf("resend before lookup or changed input: req = %+v, ID = %q", gotReq, clientID)
			}
			if limit.calls != placeCalls {
				t.Fatalf("send %d has no budget reservation: %d", placeCalls, limit.calls)
			}
			if placeCalls == 1 {
				return apiOrder{}, status.Error(codes.Unavailable, "lost resend reply")
			}
			return recoveryOrder(req), nil
		},
	}
	g := &Gateway{api: api, limit: limit, waiter: waitFunc(func(context.Context, time.Duration) error {
		t.Fatal("recovery must return to the engine without waiting")
		return nil
	})}
	rememberRecoveryPlacement(g, req)
	id, err := g.ResumePlacement(ctx, req, recoveryClientID)
	requireUnknownClientID(t, err, recoveryClientID)
	if id != "" || placeCalls != 1 || limit.calls != 1 || findCalls != 1 {
		t.Fatalf("first call: id = %q, sends = %d, budget = %d, finds = %d", id, placeCalls, limit.calls, findCalls)
	}
	id, err = g.ResumePlacement(ctx, req, recoveryClientID)
	if err != nil || id != "recovered-order" || placeCalls != 2 || limit.calls != 2 || findCalls != 2 {
		t.Fatalf("id = %q, error = %v, sends = %d, budget calls = %d", id, err, placeCalls, limit.calls)
	}
}

func TestGatewayResumeAbsentActiveOrderStaysUnknown(t *testing.T) {
	for _, lookupFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "lookup unavailable"}[lookupFails], func(t *testing.T) {
			limit := &recoveryLimit{t: t, allowedCalls: sendTries}
			placeCalls, findCalls := 0, 0
			lostReply := status.Error(codes.Unavailable, "lost reply")
			api := &fakeAPI{
				place: func(_ context.Context, _ execengine2.OrderRequest, clientID string) (apiOrder, error) {
					placeCalls++
					if clientID != recoveryClientID {
						t.Fatalf("resend changed ID to %q", clientID)
					}
					return apiOrder{}, lostReply
				},
				find: func(context.Context, string) (apiOrder, bool, error) {
					findCalls++
					if lookupFails {
						return apiOrder{}, false, status.Error(codes.Unavailable, "lookup unavailable")
					}
					return apiOrder{}, false, nil
				},
			}
			g := &Gateway{api: api, limit: limit, waiter: waitFunc(fastWait)}
			rememberRecoveryPlacement(g, limitBuy())
			for call := 1; call <= 3; call++ {
				id, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
				requireUnknownClientID(t, err, recoveryClientID)
				if id != "" || !errors.Is(err, lostReply) || placeCalls != call || limit.calls != call || findCalls != call {
					t.Fatalf("call %d: id = %q, error = %v, sends = %d, budget = %d, finds = %d", call, id, err, placeCalls, limit.calls, findCalls)
				}
			}
		})
	}
}

func TestGatewayResumeStopsAtSendBudget(t *testing.T) {
	for _, allowed := range []int{0, 1} {
		t.Run(map[int]string{0: "first resend blocked", 1: "retry blocked"}[allowed], func(t *testing.T) {
			limit := &recoveryLimit{t: t, allowedCalls: allowed}
			placeCalls := 0
			g := &Gateway{limit: limit, waiter: waitFunc(fastWait), api: &fakeAPI{
				place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
					placeCalls++
					return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
				},
			}}
			rememberRecoveryPlacement(g, limitBuy())
			for call := 0; call <= allowed; call++ {
				_, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
				requireUnknownClientID(t, err, recoveryClientID)
			}
			if placeCalls != allowed || limit.calls != allowed+1 {
				t.Fatalf("sends = %d, budget calls = %d, allowed = %d", placeCalls, limit.calls, allowed)
			}
		})
	}
}

func TestGatewayResumeWithoutSendLimitStillLooksUp(t *testing.T) {
	findCalls := 0
	g := &Gateway{api: &fakeAPI{
		find: func(context.Context, string) (apiOrder, bool, error) {
			findCalls++
			return apiOrder{}, false, nil
		},
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			t.Fatal("resend without a send limit")
			return apiOrder{}, nil
		},
	}}
	rememberRecoveryPlacement(g, limitBuy())
	_, err := g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
	requireUnknownClientID(t, err, recoveryClientID)
	if findCalls != 1 {
		t.Fatalf("finds = %d, want 1", findCalls)
	}
}

func TestGatewayResumeStopsOnCancellation(t *testing.T) {
	for _, when := range []string{"before lookup", "during lookup", "during resend"} {
		t.Run(when, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if when == "before lookup" {
				cancel()
			}
			limit := &recoveryLimit{t: t, allowedCalls: sendTries}
			placeCalls, findCalls := 0, 0
			api := &fakeAPI{
				find: func(got context.Context, _ string) (apiOrder, bool, error) {
					findCalls++
					if got != ctx {
						t.Fatal("lookup replaced caller context")
					}
					if when == "during lookup" {
						cancel()
					}
					return apiOrder{}, false, nil
				},
				place: func(got context.Context, _ execengine2.OrderRequest, _ string) (apiOrder, error) {
					placeCalls++
					if got != ctx {
						t.Fatal("resend replaced caller context")
					}
					if when == "during resend" {
						cancel()
					}
					return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
				},
			}
			g := &Gateway{api: api, limit: limit}
			rememberRecoveryPlacement(g, limitBuy())
			_, err := g.ResumePlacement(ctx, limitBuy(), recoveryClientID)
			requireUnknownClientID(t, err, recoveryClientID)
			wantFinds, wantSends := 1, 0
			if when == "before lookup" {
				wantFinds = 0
			}
			if when == "during resend" {
				wantSends = 1
			}
			if !errors.Is(err, context.Canceled) || findCalls != wantFinds || placeCalls != wantSends || limit.calls != wantSends {
				t.Fatalf("error = %v, finds = %d, sends = %d, budget calls = %d", err, findCalls, placeCalls, limit.calls)
			}
		})
	}
}

func TestGatewayResumeRefusesMismatchedOrders(t *testing.T) {
	cases := []struct {
		name   string
		change func(*apiOrder)
	}{
		{"empty id", func(o *apiOrder) { o.id = "" }},
		{"symbol", func(o *apiOrder) { o.symbol = "GAZP" }},
		{"side", func(o *apiOrder) { o.side = execengine2.SideSell }},
		{"kind", func(o *apiOrder) { o.kind = execengine2.OrderMarket }},
		{"lots", func(o *apiOrder) { o.lots++ }},
		{"price", func(o *apiOrder) { o.price++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := limitBuy()
			mismatch := recoveryOrder(req)
			tc.change(&mismatch)
			g := &Gateway{api: &fakeAPI{
				find: func(context.Context, string) (apiOrder, bool, error) { return mismatch, true, nil },
				place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
					t.Fatal("must not resend an ID known to belong to a different order")
					return apiOrder{}, nil
				},
			}}
			rememberRecoveryPlacement(g, req)
			id, err := g.ResumePlacement(context.Background(), req, recoveryClientID)
			requireUnknownClientID(t, err, recoveryClientID)
			if id != "" {
				t.Fatalf("adopted mismatched order %q", id)
			}
		})
	}
}

func TestGatewayResumeDoesNotAdoptMismatchAfterResend(t *testing.T) {
	for _, source := range []string{"placement response", "lookup response"} {
		t.Run(source, func(t *testing.T) {
			req := limitBuy()
			mismatch := recoveryOrder(req)
			mismatch.lots++
			findCalls := 0
			g := &Gateway{limit: &recoveryLimit{t: t, allowedCalls: sendTries}, waiter: waitFunc(fastWait), api: &fakeAPI{
				find: func(context.Context, string) (apiOrder, bool, error) {
					findCalls++
					return mismatch, findCalls > 1, nil
				},
				place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
					if source == "placement response" {
						return mismatch, nil
					}
					return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
				},
			}}
			rememberRecoveryPlacement(g, req)
			id, err := g.ResumePlacement(context.Background(), req, recoveryClientID)
			requireUnknownClientID(t, err, recoveryClientID)
			if source == "lookup response" {
				id, err = g.ResumePlacement(context.Background(), req, recoveryClientID)
				requireUnknownClientID(t, err, recoveryClientID)
				if findCalls != 2 {
					t.Fatalf("finds = %d, want one per call", findCalls)
				}
			}
			if id != "" {
				t.Fatalf("adopted mismatched order %q", id)
			}
		})
	}
}

func TestGatewayLateRejectDoesNotDisproveEarlierPlacement(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary place", true: "resumed place"}[resume], func(t *testing.T) {
			lostReply := status.Error(codes.Unavailable, "original reply lost")
			reject := status.Error(codes.FailedPrecondition, "order can no longer be placed")
			placeCalls := 0
			clientID := recoveryClientID
			api := &fakeAPI{place: func(_ context.Context, _ execengine2.OrderRequest, id string) (apiOrder, error) {
				placeCalls++
				clientID = id
				if !resume && placeCalls == 1 {
					return apiOrder{}, lostReply
				}
				return apiOrder{}, reject
			}}
			g := &Gateway{api: api, ids: testIDs(t), limit: &testLimit{allow: true}, waiter: waitFunc(fastWait)}
			var err error
			if resume {
				rememberRecoveryPlacement(g, limitBuy())
				_, err = g.ResumePlacement(context.Background(), limitBuy(), clientID)
			} else {
				_, err = g.Place(context.Background(), limitBuy())
			}
			requireUnknownClientID(t, err, clientID)
			if !errors.Is(err, reject) || (!resume && !errors.Is(err, lostReply)) {
				t.Fatalf("lost original or final placement error: %v", err)
			}
		})
	}
}

func TestGatewayConcurrentPlacementsUseUniqueClientIDs(t *testing.T) {
	const count = 128
	var mu sync.Mutex
	seen := make(map[string]bool, count)
	g := &Gateway{ids: testIDs(t), api: &fakeAPI{
		place: func(_ context.Context, req execengine2.OrderRequest, clientID string) (apiOrder, error) {
			mu.Lock()
			defer mu.Unlock()
			if len(clientID) != 20 || seen[clientID] {
				return apiOrder{}, status.Error(codes.InvalidArgument, "invalid or repeated client ID")
			}
			seen[clientID] = true
			return recoveryOrder(req), nil
		},
	}}
	results := make(chan error, count)
	for range count {
		go func() {
			_, err := g.Place(context.Background(), limitBuy())
			results <- err
		}()
	}
	for range count {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != count {
		t.Fatalf("unique IDs = %d, want %d", len(seen), count)
	}
}
