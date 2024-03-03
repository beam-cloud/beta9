package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gorilla/schema"
	"github.com/labstack/echo/v4"
)

type DeploymentGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
	decoder     *schema.Decoder
}

func NewDeploymentGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *DeploymentGroup {
	group := &DeploymentGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
		decoder:     schema.NewDecoder(),
	}

	g.GET("/:workspaceId", group.ListDeployments)
	g.GET("/:workspaceId/", group.ListDeployments)

	g.GET("/:workspaceId/:deploymentId", group.RetrieveDeployment)
	g.GET("/:workspaceId/:deploymentId/", group.RetrieveDeployment)

	return group
}

func (g *DeploymentGroup) ListDeployments(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	var filters types.DeploymentFilter
	if err := g.decoder.Decode(&filters, ctx.QueryParams()); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	if deployments, err := g.backendRepo.ListDeployments(ctx.Request().Context(), filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list deployments")
	} else {
		return ctx.JSON(http.StatusOK, deployments)
	}
}

func (g *DeploymentGroup) RetrieveDeployment(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	deploymentId := ctx.Param("deploymentId")
	if deployment, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), workspace.Id, deploymentId); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve deployment")
	} else {
		return ctx.JSON(http.StatusOK, deployment)
	}
}
