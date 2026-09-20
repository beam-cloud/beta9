package abstractions

import (
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func parseWorkspaceSecretKey(workspace *types.Workspace) ([]byte, error) {
	if workspace == nil {
		return nil, common.ErrWorkspaceSigningKeyUnavailable
	}
	return common.ParseSecretKeyPointer(workspace.SigningKey)
}
