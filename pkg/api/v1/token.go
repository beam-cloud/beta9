package apiv1

import (
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
}

func NewTokenGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *TokenGroup {
	group := &TokenGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.PATCH("/admin/:workspaceId", auth.WithClusterAdminAuth(group.ClusterAdminUpdateAllWorkspaceTokens))
	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.CreateWorkspaceToken))
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListWorkspaceTokens))
	g.GET("/:workspaceId/signing-key", auth.WithWorkspaceAuth(group.GetSigningKey))
	g.POST("/:workspaceId/:tokenId/toggle", auth.WithWorkspaceAuth(group.ToggleWorkspaceToken))
	g.DELETE("/:workspaceId/:tokenId", auth.WithWorkspaceAuth(group.DeleteWorkspaceToken))

	return group
}

func (g *TokenGroup) CreateWorkspaceToken(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	token, err := g.backendRepo.CreateToken(ctx.Request().Context(), workspace.Id, types.TokenTypeWorkspace, true)
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
	Disabled bool `json:"enabled"`
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

	tokens, err := g.backendRepo.ListTokens(ctx.Request().Context(), workspace.Id)
	if err != nil {
		return HTTPInternalServerError("Failed to list tokens")
	}

	for _, token := range tokens {
		err := g.backendRepo.UpdateTokenAsClusterAdmin(ctx.Request().Context(), token.ExternalId, req.Disabled)
		if err != nil {
			return HTTPInternalServerError("Unable to update token")
		}
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
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	extTokenId := ctx.Param("tokenId")
	token, err := g.backendRepo.ToggleToken(ctx.Request().Context(), workspace.Id, extTokenId)
	if err != nil {
		return HTTPInternalServerError("Failed to toggle token")
	}

	serializedToken, err := serializer.Serialize(token)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serializedToken)
}

func (g *TokenGroup) DeleteWorkspaceToken(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	extTokenId := ctx.Param("tokenId")
	err = g.backendRepo.DeleteToken(ctx.Request().Context(), workspace.Id, extTokenId)
	if err != nil {
		return HTTPInternalServerError("Failed to delete token")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"ok": true,
	})
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
