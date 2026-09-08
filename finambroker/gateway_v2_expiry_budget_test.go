package finambroker

import (
	"context"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type expiryLimit struct{ advance func() }

func (l expiryLimit) Take(int64, execengine2.LimitKind) bool { l.advance(); return true }
func (expiryLimit) Remaining() int64                         { return 100 }

func TestGatewayRechecksResendDeadlineAfterBudgetAdmission(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "resume"}[resume], func(t *testing.T) {
			deadline := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
			now := deadline.Add(-time.Nanosecond)
			calls := 0
			g := &Gateway{
				ids: testIDs(t), waiter: waitFunc(fastWait), now: func() time.Time { return now },
				limit: expiryLimit{advance: func() { now = deadline }},
				api: &fakeAPI{place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
					calls++
					return apiOrder{}, status.Error(codes.Unavailable, "lost response")
				}},
			}
			var err error
			wantCalls := 1
			if resume {
				g.rememberPlacement(recoveryClientID, limitBuy(), deadline)
				_, err = g.ResumePlacement(context.Background(), limitBuy(), recoveryClientID)
				wantCalls = 0
			} else {
				_, err = g.Place(context.Background(), limitBuy())
			}
			if !execengine2.OrderMayExist(err) || calls != wantCalls {
				t.Fatalf("expired placement resent or ambiguity lost: calls=%d want=%d err=%v", calls, wantCalls, err)
			}
		})
	}
}
