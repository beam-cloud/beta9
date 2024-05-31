package secret

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
)

type secretGroup struct {
	routeGroup *echo.Group
	ss         *WorkspaceSecretService
}

func registerSecretRoutes(g *echo.Group, ss *WorkspaceSecretService) *secretGroup {
	group := &secretGroup{
		routeGroup: g,
		ss:         ss,
	}

	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.CreateSecret))
	g.GET("/:workspaceId/:secretName", auth.WithWorkspaceAuth(group.GetSecret))
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListSecrets))
	g.PATCH("/:workspaceId/:secretName", auth.WithWorkspaceAuth(group.UpdateSecret))
	g.DELETE("/:workspaceId/:secretId", auth.WithWorkspaceAuth(group.DeleteSecret))

	return group
}

func (g *secretGroup) CreateSecret(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	secretInput := types.WorkspaceSecret{}
	if err := ctx.Bind(&secretInput); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid secret")
	}

	// Create the secret
	secret, err := g.ss.backendRepo.CreateSecret(ctx.Request().Context(), &workspace, cc.AuthInfo.Token.Id, secretInput.Name, secretInput.Value)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" { // Unique violation
				return echo.NewHTTPError(http.StatusConflict, "Secret already exists")
			}
		}

		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create secret")
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) GetSecret(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	secretName := ctx.Param("secretName")
	secret, err := g.ss.backendRepo.GetSecretByName(ctx.Request().Context(), &workspace, secretName)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "Secret not found")
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) ListSecrets(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	secrets, err := g.ss.backendRepo.ListSecrets(ctx.Request().Context(), &workspace)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list secret")
	}

	return ctx.JSON(http.StatusOK, secrets)
}

func (g *secretGroup) UpdateSecret(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	secretName := ctx.Param("secretName")

	secretInput := types.WorkspaceSecret{}
	if err := ctx.Bind(&secretInput); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid secret")
	}

	// Update the secret
	secret, err := g.ss.backendRepo.UpdateSecret(ctx.Request().Context(), &workspace, cc.AuthInfo.Token.Id, secretName, secretInput.Value)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to update secret")
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) DeleteSecret(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	secretId := ctx.Param("secretId")
	if err := g.ss.backendRepo.DeleteSecret(ctx.Request().Context(), &workspace, secretId); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete secret")
	}

	return ctx.NoContent(http.StatusOK)
}
