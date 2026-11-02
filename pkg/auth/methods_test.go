package auth

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestAuthorizeMethod(t *testing.T) {
	workerMethod := pb.WorkerRepositoryService_GetWorkerById_FullMethodName
	for _, tc := range []struct {
		name      string
		token     types.Token
		method    string
		permitted bool
	}{
		{"bound worker calls worker service", types.Token{TokenType: types.TokenTypeWorker, WorkerID: "w"}, workerMethod, true},
		{"bound private worker calls thunder", types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "w"}, pb.ThunderService_CreateClientEnrollment_FullMethodName, true},
		{"unbound worker", types.Token{TokenType: types.TokenTypeWorker}, workerMethod, false},
		{"worker lists tokens", types.Token{TokenType: types.TokenTypeWorker, WorkerID: "w"}, pb.GatewayService_ListTokens_FullMethodName, false},
		{"worker runs container", types.Token{TokenType: types.TokenTypeWorker, WorkerID: "w"}, pb.Scheduler_RunContainer_FullMethodName, false},
		{"workspace calls worker service", types.Token{TokenType: types.TokenTypeWorkspacePrimary}, workerMethod, false},
		{"cluster admin calls worker service", types.Token{TokenType: types.TokenTypeClusterAdmin}, workerMethod, false},
		{"workspace runs container", types.Token{TokenType: types.TokenTypeWorkspacePrimary}, pb.Scheduler_RunContainer_FullMethodName, false},
		{"cluster admin runs container", types.Token{TokenType: types.TokenTypeClusterAdmin}, pb.Scheduler_RunContainer_FullMethodName, true},
		{"workspace lists tokens", types.Token{TokenType: types.TokenTypeWorkspace}, pb.GatewayService_ListTokens_FullMethodName, true},
		{"deployer lists tokens", types.Token{TokenType: types.TokenTypePlatformDeployer}, pb.GatewayService_ListTokens_FullMethodName, false},
		{"deployer gets a pool join command", types.Token{TokenType: types.TokenTypePlatformDeployer}, pb.GatewayService_GetPoolJoinCommand_FullMethodName, false},
		{"restricted token gets a pool join command", types.Token{TokenType: types.TokenTypeWorkspaceRestricted}, pb.GatewayService_GetPoolJoinCommand_FullMethodName, false},
		{"restricted token reads secret", types.Token{TokenType: types.TokenTypeWorkspaceRestricted}, pb.SecretService_GetSecret_FullMethodName, false},
		{"restricted token deploys", types.Token{TokenType: types.TokenTypeWorkspaceRestricted}, pb.GatewayService_DeployStub_FullMethodName, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := authorizeMethod(&AuthInfo{Token: &tc.token}, tc.method)
			if tc.permitted {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
