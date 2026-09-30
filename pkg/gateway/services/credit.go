package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/types"
)

// WorkspaceCredit is the credit gate's view of a workspace; nil without a gate.
func (gws *GatewayService) WorkspaceCredit(ctx context.Context, workspace *types.Workspace) *types.CreditStatus {
	return gws.scheduler.CreditGate().Status(ctx, workspace.ExternalId)
}
