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

	g.POST("/:workspaceId", group.CreateWorkspaceToken)
	g.DELETE("/:workspaceId", group.RevokeWorkspaceToken)
	g.GET("/:workspaceId", group.ListWorkspaceTokens)

	return group
}

func (g *TokenGroup) CreateWorkspaceToken(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized, "Invalid token")
	}

	workspaceId := ctx.Param("workspaceId")

	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace")
	}

	token, err := g.backendRepo.CreateToken(ctx.Request().Context(), workspace.Id, types.TokenTypeWorkspace, true)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Unable to create token")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": token.Key,
	})
}

type RevokeTokenRequest struct {
	Token string `json:"token"`
}

func (g *TokenGroup) RevokeWorkspaceToken(ctx echo.Context) error {
	_, _ = ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"ok": true,
	})
}

func (g *TokenGroup) ListWorkspaceTokens(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized, "Unauthorized access")
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	tokens, err := g.backendRepo.ListTokens(ctx.Request().Context(), workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tokens")
	}

	return ctx.JSON(http.StatusOK, tokens)
}
