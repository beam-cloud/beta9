package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
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

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": token.Key,
	})
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

	return ctx.JSON(http.StatusOK, tokens)
}
