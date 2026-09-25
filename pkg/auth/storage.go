package auth

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/types"
)

// EnsureWorkspaceStorage gives a workspace that still has no storage row its
// bucket and row, and attaches them to the workspace in place. Workspaces
// created before per-workspace storage existed reach the gateway without one;
// new workspaces are created with it. The gateway installs the implementation
// at startup; until then, and in tests, workspaces pass through unchanged.
var EnsureWorkspaceStorage func(ctx context.Context, workspace *types.Workspace) error

func ensureWorkspaceStorage(ctx context.Context, workspace *types.Workspace) error {
	if EnsureWorkspaceStorage == nil || workspace == nil || workspace.StorageAvailable() {
		return nil
	}
	return EnsureWorkspaceStorage(ctx, workspace)
}
