package gatewayservices

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) ListTokens(ctx context.Context, req *pb.ListTokensRequest) (*pb.ListTokensResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceId := authInfo.Workspace.Id
	tokens, err := gws.backendRepo.ListTokens(ctx, workspaceId)
	if err != nil {
		return &pb.ListTokensResponse{
			Tokens: []*pb.Token{},
			Ok:     false,
			ErrMsg: "Unable to list tokens.",
		}, nil
	}

	var t []*pb.Token
	for _, token := range tokens {
		var workspaceId uint32
		if token.WorkspaceId != nil {
			workspaceId = uint32(*token.WorkspaceId)
		}

		updatedAt := *timestamppb.New(token.UpdatedAt.Time)

		t = append(t, &pb.Token{
			TokenId:     token.ExternalId,
			Key:         token.Key,
			Active:      token.Active,
			Reusable:    token.Reusable,
			WorkspaceId: &workspaceId,
			TokenType:   token.TokenType,
			CreatedAt:   timestamppb.New(token.CreatedAt.Time),
			UpdatedAt:   &updatedAt,
		})
	}

	return &pb.ListTokensResponse{
		Tokens: t,
		Ok:     true,
	}, nil
}

func (gws *GatewayService) CreateToken(ctx context.Context, req *pb.CreateTokenRequest) (*pb.CreateTokenResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	tokenType := req.TokenType
	if tokenType == "" {
		tokenType = types.TokenTypeWorkspace
	}

	// Validate token type - only allow workspace and workspace_restricted
	if tokenType != types.TokenTypeWorkspace && tokenType != types.TokenTypeWorkspaceRestricted {
		return &pb.CreateTokenResponse{
			Token:  &pb.Token{},
			Ok:     false,
			ErrMsg: "Invalid token type. Allowed types: workspace, workspace_restricted",
		}, nil
	}

	token, err := gws.backendRepo.CreateToken(ctx, authInfo.Workspace.Id, tokenType, true)
	if err != nil {
		return &pb.CreateTokenResponse{
			Token:  &pb.Token{},
			Ok:     false,
			ErrMsg: "Unable to create token.",
		}, nil
	}

	updatedAt := *timestamppb.New(token.UpdatedAt.Time)
	workspaceId := uint32(authInfo.Workspace.Id)

	return &pb.CreateTokenResponse{
		Token: &pb.Token{
			TokenId:     token.ExternalId,
			Key:         token.Key,
			Active:      token.Active,
			Reusable:    token.Reusable,
			WorkspaceId: &workspaceId,
			TokenType:   token.TokenType,
			CreatedAt:   timestamppb.New(token.CreatedAt.Time),
			UpdatedAt:   &updatedAt,
		},
		Ok: true,
	}, nil
}

func (gws *GatewayService) ToggleToken(ctx context.Context, req *pb.ToggleTokenRequest) (*pb.ToggleTokenResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	token, err := gws.backendRepo.ToggleToken(ctx, authInfo.Workspace.Id, req.TokenId)
	if err != nil {
		return &pb.ToggleTokenResponse{
			Token:  &pb.Token{},
			Ok:     false,
			ErrMsg: "Unable to toggle token.",
		}, nil
	}

	updatedAt := *timestamppb.New(token.UpdatedAt.Time)
	workspaceId := uint32(authInfo.Workspace.Id)

	return &pb.ToggleTokenResponse{
		Token: &pb.Token{
			TokenId:     token.ExternalId,
			Key:         token.Key,
			Active:      token.Active,
			Reusable:    token.Reusable,
			WorkspaceId: &workspaceId,
			TokenType:   token.TokenType,
			CreatedAt:   timestamppb.New(token.CreatedAt.Time),
			UpdatedAt:   &updatedAt,
		},
		Ok: true,
	}, nil
}

func (gws *GatewayService) DeleteToken(ctx context.Context, req *pb.DeleteTokenRequest) (*pb.DeleteTokenResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	err := gws.backendRepo.DeleteToken(ctx, authInfo.Workspace.Id, req.TokenId)
	if err != nil {
		return &pb.DeleteTokenResponse{
			Ok:     false,
			ErrMsg: "Unable to delete token.",
		}, nil
	}

	return &pb.DeleteTokenResponse{
		Ok: true,
	}, nil
}
