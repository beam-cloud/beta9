package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) GetSecrets(ctx context.Context, req *pb.GetSecretsRequest) (*pb.GetSecretsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo.Token.TokenType != types.TokenTypeWorker {
		return &pb.GetSecretsResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
	}

	workspace, err := gws.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx, req.WorkspaceId)
	if err != nil {
		return nil, err
	}

	secrets, err := gws.backendRepo.GetSecretsByNamesDecrypted(ctx, &workspace, req.Names)
	if err != nil {
		return nil, err
	}

	resp := pb.GetSecretsResponse{
		Ok:      true,
		ErrMsg:  "",
		Secrets: make(map[string]string, len(secrets)),
	}
	for _, s := range secrets {
		resp.Secrets[s.Name] = s.Value
	}

	return &resp, nil
}
