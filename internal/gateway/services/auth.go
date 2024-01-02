package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beam/internal/auth"
	pb "github.com/beam-cloud/beam/proto"
)

func (gws *GatewayService) Authorize(ctx context.Context, in *pb.AuthorizeRequest) (*pb.AuthorizeResponse, error) {
	authInfo, authFound := auth.AuthInfoFromContext(ctx)
	if authFound {
		return &pb.AuthorizeResponse{
			ContextId: authInfo.Workspace.ExternalId,
			Ok:        true,
		}, nil
	}

	// See if the this gateway has been configured previously
	existingContexts, err := gws.backendRepo.ListContexts(ctx)
	if err != nil || len(existingContexts) >= 1 {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Invalid token",
		}, nil
	}

	// If no contexts are found, we can create a new one for the user
	// and generate a new token
	context, err := gws.backendRepo.CreateContext(ctx)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new context",
		}, nil
	}

	// Now that we have a context, create a new token
	token, err := gws.backendRepo.CreateToken(ctx, context.Id)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new token",
		}, nil
	}

	return &pb.AuthorizeResponse{
		Ok:        true,
		NewToken:  token.Key,
		ContextId: context.ExternalId,
	}, nil
}
