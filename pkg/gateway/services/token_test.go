package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestPublicTokenCreationRejectsPlatformDeployerType(t *testing.T) {
	for _, callerType := range []string{types.TokenTypeWorkspace, types.TokenTypeClusterAdmin, types.TokenTypePlatformDeployer} {
		t.Run(callerType, func(t *testing.T) {
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{TokenType: callerType}, Workspace: &types.Workspace{Id: 1}})
			response, err := (&GatewayService{}).CreateToken(ctx, &pb.CreateTokenRequest{TokenType: types.TokenTypePlatformDeployer})
			require.NoError(t, err)
			require.False(t, response.Ok)
			require.Contains(t, response.ErrMsg, "Invalid token type")
		})
	}
	require.False(t, auth.HasInteractivePermission(&auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypePlatformDeployer}}))
}
