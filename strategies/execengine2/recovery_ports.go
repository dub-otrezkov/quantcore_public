package execengine2

import "context"

// PlacementResumer continues an ambiguous placement using its original client ID.
// Implementations look up that ID before resending and charge every resend to
// LimitMust. Absence or a later rejection does not prove it was never placed;
// unresolved errors must preserve the original ID with OrderUnknown.
// Once safe idempotent resends expire, recovery may only perform lookups.
type PlacementResumer interface {
	ResumePlacement(ctx context.Context, req OrderRequest, clientID string) (string, error)
}
