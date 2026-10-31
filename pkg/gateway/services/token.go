package gatewayservices

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func tokenProto(token types.Token, reveal bool) *pb.Token {
	var workspaceID *uint32
	if token.WorkspaceId != nil {
		id := uint32(*token.WorkspaceId)
		workspaceID = &id
	}
	out := &pb.Token{TokenId: token.ExternalId, Active: token.Active, Reusable: token.Reusable, WorkspaceId: workspaceID, TokenType: token.TokenType, CreatedAt: timestamppb.New(token.CreatedAt.Time), UpdatedAt: timestamppb.New(token.UpdatedAt.Time)}
	if token.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*token.ExpiresAt)
	}
	if reveal {
		out.Key = token.Key
	}
	return out
}

func (gws *GatewayService) ListTokens(ctx context.Context, req *pb.ListTokensRequest) (*pb.ListTokensResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(info) {
		return &pb.ListTokensResponse{ErrMsg: "Unauthorized Access"}, nil
	}
	tokens, err := gws.backendRepo.ListTokens(ctx, info.Workspace.Id)
	if err != nil {
		return &pb.ListTokensResponse{ErrMsg: "Unable to list tokens."}, nil
	}
	out := &pb.ListTokensResponse{Ok: true}
	for _, token := range tokens {
		out.Tokens = append(out.Tokens, tokenProto(token, false))
	}
	return out, nil
}

func (gws *GatewayService) CreateToken(ctx context.Context, req *pb.CreateTokenRequest) (*pb.CreateTokenResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(info) {
		return &pb.CreateTokenResponse{ErrMsg: "Unauthorized Access"}, nil
	}
	kind := req.TokenType
	if kind == "" {
		kind = types.TokenTypeWorkspace
	}
	if kind != types.TokenTypeWorkspace && kind != types.TokenTypeWorkspaceRestricted {
		return &pb.CreateTokenResponse{ErrMsg: "Invalid token type."}, nil
	}
	if _, err := repository.TokenExpiry(req.ExpiresInSeconds); err != nil {
		return &pb.CreateTokenResponse{ErrMsg: err.Error()}, nil
	}
	token, err := repository.CreateExpiringToken(ctx, gws.backendRepo, info.Workspace.Id, kind, req.ExpiresInSeconds)
	if err != nil {
		return &pb.CreateTokenResponse{ErrMsg: "Unable to create token."}, nil
	}
	return &pb.CreateTokenResponse{Ok: true, Token: tokenProto(token, true)}, nil
}

func (gws *GatewayService) mutableToken(ctx context.Context, info *auth.AuthInfo, tokenID string) (*types.Token, error) {
	token, err := gws.backendRepo.GetTokenByExternalId(ctx, info.Workspace.Id, tokenID)
	if err != nil {
		return nil, err
	}
	if token.TokenType == types.TokenTypeWorkspacePrimary && info.Token.TokenType != types.TokenTypeClusterAdmin {
		return nil, fmt.Errorf("token cannot be changed")
	}
	return token, nil
}

func (gws *GatewayService) evictToken(token *types.Token) error {
	if gws.workspaceRepo == nil {
		return nil
	}
	return gws.workspaceRepo.RevokeToken(token.CacheKey())
}

func (gws *GatewayService) ToggleToken(ctx context.Context, req *pb.ToggleTokenRequest) (*pb.ToggleTokenResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(info) {
		return &pb.ToggleTokenResponse{ErrMsg: "Unauthorized Access"}, nil
	}
	previous, err := gws.mutableToken(ctx, info, req.TokenId)
	if err != nil {
		return &pb.ToggleTokenResponse{ErrMsg: "Unable to find token."}, nil
	}
	token, err := gws.backendRepo.ToggleToken(ctx, info.Workspace.Id, req.TokenId)
	if err != nil {
		return &pb.ToggleTokenResponse{ErrMsg: "Unable to toggle token."}, nil
	}
	if err = gws.evictToken(previous); err != nil {
		return &pb.ToggleTokenResponse{ErrMsg: "Unable to refresh token."}, nil
	}
	return &pb.ToggleTokenResponse{Ok: true, Token: tokenProto(token, false)}, nil
}

func (gws *GatewayService) DeleteToken(ctx context.Context, req *pb.DeleteTokenRequest) (*pb.DeleteTokenResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(info) {
		return &pb.DeleteTokenResponse{ErrMsg: "Unauthorized Access"}, nil
	}
	token, err := gws.mutableToken(ctx, info, req.TokenId)
	if err != nil {
		return &pb.DeleteTokenResponse{ErrMsg: "Unable to find token."}, nil
	}
	if err = gws.backendRepo.DeleteToken(ctx, info.Workspace.Id, req.TokenId); err != nil {
		return &pb.DeleteTokenResponse{ErrMsg: "Unable to delete token."}, nil
	}
	if err = gws.evictToken(token); err != nil {
		return &pb.DeleteTokenResponse{ErrMsg: "Unable to refresh token."}, nil
	}
	return &pb.DeleteTokenResponse{Ok: true}, nil
}

func (gws *GatewayService) RenewToken(ctx context.Context, req *pb.RenewTokenRequest) (*pb.RenewTokenResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(info) {
		return &pb.RenewTokenResponse{ErrMsg: "Unauthorized Access"}, nil
	}
	previous, err := gws.mutableToken(ctx, info, req.TokenId)
	if err != nil {
		return &pb.RenewTokenResponse{ErrMsg: "Unable to find token."}, nil
	}
	token, err := repository.RenewToken(ctx, gws.backendRepo, info.Workspace.Id, req.TokenId, req.ExpiresInSeconds)
	if err != nil {
		return &pb.RenewTokenResponse{ErrMsg: "Unable to renew token."}, nil
	}
	if err = gws.evictToken(previous); err != nil {
		return &pb.RenewTokenResponse{ErrMsg: "Unable to refresh token."}, nil
	}
	return &pb.RenewTokenResponse{Ok: true, Token: tokenProto(token, false)}, nil
}
