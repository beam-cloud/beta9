package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) Authorize(ctx context.Context, in *pb.AuthorizeRequest) (*pb.AuthorizeResponse, error) {
	authInfo, authFound := auth.AuthInfoFromContext(ctx)
	if authFound {
		return &pb.AuthorizeResponse{
			WorkspaceId: authInfo.Workspace.ExternalId,
			Ok:          true,
		}, nil
	}

	// See if this gateway has been configured previously
	workspaces, err := gws.backendRepo.ListWorkspaces(ctx)
	if err != nil || len(workspaces) >= 1 {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Invalid token",
		}, nil
	}

	// If no workspaces are found, we can create a new one for the user
	// and generate a new token
	workspace, err := gws.backendRepo.CreateWorkspace(ctx)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new workspace",
		}, nil
	}

	// Now that we have a workspace, create a new token
	token, err := gws.backendRepo.CreateToken(ctx, workspace.Id, types.TokenTypeClusterAdmin, true)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new token",
		}, nil
	}

	return &pb.AuthorizeResponse{
		Ok:          true,
		NewToken:    token.Key,
		WorkspaceId: workspace.ExternalId,
	}, nil
}

func (gws *GatewayService) SignPayload(ctx context.Context, in *pb.SignPayloadRequest) (*pb.SignPayloadResponse, error) {
	authInfo, authFound := auth.AuthInfoFromContext(ctx)
	if !authFound {
		return &pb.SignPayloadResponse{
			Ok:       false,
			ErrorMsg: "Invalid token",
		}, nil
	}

	if authInfo.Workspace.SigningKey == nil || *authInfo.Workspace.SigningKey == "" {
		return &pb.SignPayloadResponse{
			Ok:       false,
			ErrorMsg: "Invalid signing key",
		}, nil
	}

	sig := auth.SignPayload(in.Payload, *authInfo.Workspace.SigningKey)
	return &pb.SignPayloadResponse{
		Ok:        true,
		Signature: sig.Key,
		Timestamp: sig.Timestamp,
	}, nil
}
