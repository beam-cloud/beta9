package auth

import (
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Worker tokens may call only the services workers use, and nothing else may
// call those services with a workspace token.
var workerServicePrefixes = []string{
	"/WorkerRepositoryService/",
	"/ContainerRepositoryService/",
	"/BackendRepositoryService/",
	"/thunder.ThunderService/",
}

var clusterAdminMethods = map[string]bool{
	pb.Scheduler_RunContainer_FullMethodName: true,
}

// Methods that mint, reveal, or manage credentials, which deploy and runtime
// tokens never need.
var credentialMethods = map[string]bool{
	pb.GatewayService_ListTokens_FullMethodName:             true,
	pb.GatewayService_CreateToken_FullMethodName:            true,
	pb.GatewayService_ToggleToken_FullMethodName:            true,
	pb.GatewayService_DeleteToken_FullMethodName:            true,
	pb.GatewayService_CreatePoolJoinToken_FullMethodName:    true,
	pb.GatewayService_GetPoolJoinCommand_FullMethodName:     true,
	pb.GatewayService_RevokePoolJoinToken_FullMethodName:    true,
	pb.GatewayService_GetProviderJoinCommand_FullMethodName: true,
	pb.SecretService_CreateSecret_FullMethodName:            true,
	pb.SecretService_DeleteSecret_FullMethodName:            true,
	pb.SecretService_UpdateSecret_FullMethodName:            true,
	pb.SecretService_GetSecret_FullMethodName:               true,
	pb.SecretService_ListSecrets_FullMethodName:             true,
}

func isWorkerMethod(method string) bool {
	for _, prefix := range workerServicePrefixes {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}

func authorizeMethod(authInfo *AuthInfo, method string) error {
	tokenType := authInfo.Token.TokenType
	denied := false
	switch {
	case types.IsWorkerTokenType(tokenType):
		denied = !isWorkerMethod(method) || authInfo.Token.WorkerID == ""
	case isWorkerMethod(method):
		denied = true
	case clusterAdminMethods[method]:
		denied = tokenType != types.TokenTypeClusterAdmin
	case credentialMethods[method]:
		denied = tokenType == types.TokenTypePlatformDeployer || tokenType == types.TokenTypeWorkspaceRestricted
	}
	if denied {
		return status.Error(codes.PermissionDenied, "token is not permitted to call this method")
	}
	return nil
}
