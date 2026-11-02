package apiv1

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
)

type TokenGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
	tokens      auth.Tokens
}

func NewTokenGroup(g *echo.Group, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, config types.AppConfig) *TokenGroup {
	group := &TokenGroup{routerGroup: g,
		backendRepo: backendRepo,
		tokens:      auth.NewTokens(backendRepo, workspaceRepo),
		config:      config,
	}

	g.PATCH("/admin/:workspaceId", auth.WithClusterAdminAuth(group.ClusterAdminUpdateAllWorkspaceTokens))
	g.POST("/:workspaceId", auth.WithCredentialAuth(group.CreateWorkspaceToken))
	g.GET("/:workspaceId", auth.WithCredentialAuth(group.ListWorkspaceTokens))
	g.GET("/:workspaceId/signing-key", auth.WithCredentialAuth(group.GetSigningKey))
	g.POST("/:workspaceId/:tokenId/toggle", auth.WithCredentialAuth(group.ToggleWorkspaceToken))
	g.DELETE("/:workspaceId/:tokenId", auth.WithCredentialAuth(group.DeleteWorkspaceToken))

	return group
}

func (g *TokenGroup) CreateWorkspaceToken(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	tokenType := ctx.QueryParam("token_type")
	if tokenType == "" {
		tokenType = types.TokenTypeWorkspace
	}

	clusterAdmin := cc.AuthInfo.Token.TokenType == types.TokenTypeClusterAdmin
	if tokenType != types.TokenTypeWorkspace && !(clusterAdmin && tokenType == types.TokenTypeWorkspacePrimary) {
		return HTTPBadRequest("Invalid token type")
	}

	token, err := g.backendRepo.CreateToken(ctx.Request().Context(), workspace.Id, tokenType, true)
	if err != nil {
		return HTTPInternalServerError("Unable to create token")
	}

	serializedToken, err := serializer.Serialize(token)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serializedToken)
}

type ClusterAdminTokensRequestSerializer struct {
	Disabled bool `json:"disabled"`
}

func (g *TokenGroup) ClusterAdminUpdateAllWorkspaceTokens(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	var req ClusterAdminTokensRequestSerializer
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid request")
	}

	if err := g.tokens.SetDisabledByClusterAdmin(ctx.Request().Context(), workspace.Id, req.Disabled); err != nil {
		return HTTPInternalServerError("Unable to update tokens")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"ok": true,
	})
}

func (g *TokenGroup) ListWorkspaceTokens(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	tokens, err := g.backendRepo.ListTokens(ctx.Request().Context(), workspace.Id)
	if err != nil {
		return HTTPInternalServerError("Failed to list tokens")
	}

	serializedTokens, err := serializer.Serialize(tokens)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serializedTokens)
}

func (g *TokenGroup) ToggleWorkspaceToken(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	toggledToken, err := g.tokens.Toggle(ctx.Request().Context(), cc.AuthInfo.Token, workspace.Id, ctx.Param("tokenId"))
	if err != nil {
		return tokenChangeError(err, "Failed to toggle token")
	}

	serializedToken, err := serializer.Serialize(toggledToken)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serializedToken)
}

func (g *TokenGroup) DeleteWorkspaceToken(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	if err := g.tokens.Delete(ctx.Request().Context(), cc.AuthInfo.Token, workspace.Id, ctx.Param("tokenId")); err != nil {
		return tokenChangeError(err, "Failed to delete token")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"ok": true,
	})
}

func tokenChangeError(err error, message string) error {
	var rejected auth.TokenChangeError
	if errors.As(err, &rejected) {
		return HTTPBadRequest(rejected.Error())
	}
	return HTTPInternalServerError(message)
}

func (g *TokenGroup) GetSigningKey(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	return ctx.JSON(http.StatusOK, map[string]any{
		"signing_key": workspace.SigningKey,
	})
}
