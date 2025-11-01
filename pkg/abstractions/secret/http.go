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

	g.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(group.CreateSecret))
	g.GET("/:workspaceId/:secretName", auth.WithStrictWorkspaceAuth(group.GetSecret))
	g.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(group.ListSecrets))
	g.PATCH("/:workspaceId/:secretName", auth.WithStrictWorkspaceAuth(group.UpdateSecret))
	g.DELETE("/:workspaceId/:secretName", auth.WithStrictWorkspaceAuth(group.DeleteSecret))

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
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	secretInput := types.Secret{}
	if err := ctx.Bind(&secretInput); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid secret",
		})
	}

	// Create the secret
	secret, err := g.ss.backendRepo.CreateSecret(ctx.Request().Context(), &workspace, cc.AuthInfo.Token.Id, secretInput.Name, secretInput.Value, true)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code.Name() == "unique_violation" {
				return ctx.JSON(http.StatusConflict,
					map[string]interface{}{
						"error": "secret already exists",
					})
			}
		}

		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to create secret",
		})
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) GetSecret(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	secretName := ctx.Param("secretName")
	secret, err := g.ss.backendRepo.GetSecretByNameDecrypted(ctx.Request().Context(), &workspace, secretName)
	if err != nil {
		return ctx.JSON(http.StatusNotFound, map[string]interface{}{
			"error": "secret not found",
		})
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) ListSecrets(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	secrets, err := g.ss.backendRepo.ListSecrets(ctx.Request().Context(), &workspace)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to list secret",
		})
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
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	secretName := ctx.Param("secretName")

	secretInput := types.Secret{}
	if err := ctx.Bind(&secretInput); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid secret",
		})
	}

	// Update the secret
	secret, err := g.ss.backendRepo.UpdateSecret(ctx.Request().Context(), &workspace, cc.AuthInfo.Token.Id, secretName, secretInput.Value)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to update secret",
		})
	}

	return ctx.JSON(http.StatusOK, secret)
}

func (g *secretGroup) DeleteSecret(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.ss.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	secretName := ctx.Param("secretName")
	if err := g.ss.backendRepo.DeleteSecret(ctx.Request().Context(), &workspace, secretName); err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to delete secret",
		})
	}

	return ctx.NoContent(http.StatusOK)
}
