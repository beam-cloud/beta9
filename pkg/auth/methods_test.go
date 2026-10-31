package auth

import (
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMethodPermissions(t *testing.T) {
	for _, kind := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate, types.TokenTypeWorkspaceRestricted, "unknown"} {
		info := &AuthInfo{Token: &types.Token{TokenType: kind}}
		for _, method := range []string{pb.GatewayService_ListTokens_FullMethodName, pb.GatewayService_CreateToken_FullMethodName, pb.SecretService_GetSecret_FullMethodName, pb.GatewayService_GetProviderJoinCommand_FullMethodName, pb.GatewayService_ExportWorkspaceConfig_FullMethodName, "/unknown.Service/Method"} {
			require.Error(t, authorizeMethod(info, method), "%s %s", kind, method)
		}
		require.False(t, HasPermission(info))
	}
	require.NoError(t, authorizeMethod(&AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorker}}, pb.WorkerRepositoryService_ClaimContainer_FullMethodName))
	require.NoError(t, authorizeMethod(&AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkspaceRestricted}}, pb.FunctionService_FunctionGetArgs_FullMethodName))
	require.Error(t, authorizeMethod(&AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkspace}}, pb.WorkerRepositoryService_ClaimContainer_FullMethodName))
}

func TestCredentialValidity(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Second)
	after := now.Add(time.Second)
	for _, token := range []*types.Token{nil, {Active: false}, {Active: true, DisabledByClusterAdmin: true}, {Active: true, ExpiresAt: &before}, {Active: true, ExpiresAt: &now}} {
		require.False(t, token.ValidAt(now))
	}
	require.True(t, (&types.Token{Active: true, ExpiresAt: &after}).ValidAt(now))
}
