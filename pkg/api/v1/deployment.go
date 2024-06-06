package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"k8s.io/utils/ptr"
)

type DeploymentGroup struct {
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	redisClient   *common.RedisClient
	scheduler     scheduler.Scheduler
}

func NewDeploymentGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	scheduler scheduler.Scheduler,
	redisClient *common.RedisClient,
	config types.AppConfig,
) *DeploymentGroup {
	group := &DeploymentGroup{routerGroup: g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		scheduler:     scheduler,
		redisClient:   redisClient,
		config:        config,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListDeployments))
	g.GET("/:workspaceId/:deploymentId", auth.WithWorkspaceAuth(group.RetrieveDeployment))
	g.POST("/:workspaceId/stop/:deploymentId/", auth.WithWorkspaceAuth(group.StopDeployment))
	g.POST("/:workspaceId/stop-all-active-deployments", auth.WithClusterAdminAuth(group.StopAllActiveDeployments))

	return group
}

func (g *DeploymentGroup) ListDeployments(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	var filters types.DeploymentFilter
	if err := ctx.Bind(&filters); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	if filters.Pagination {
		if deployments, err := g.backendRepo.ListDeploymentsPaginated(ctx.Request().Context(), filters); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list deployments")
		} else {
			return ctx.JSON(http.StatusOK, deployments)
		}
	} else {
		if deployments, err := g.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), filters); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list deployments")
		} else {
			return ctx.JSON(http.StatusOK, deployments)
		}

	}
}

func (g *DeploymentGroup) RetrieveDeployment(ctx echo.Context) error {
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

func (g *DeploymentGroup) StopDeployment(ctx echo.Context) error {
	cc, ok := ctx.(auth.HttpAuthContext)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get auth info")
	}

	deploymentId := ctx.Param("deploymentId")

	// Get deployment
	deploymentWithRelated, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "failed to get deployment",
		})
	}

	return g.stopDeployments([]types.DeploymentWithRelated{*deploymentWithRelated}, ctx)
}

func (g *DeploymentGroup) StopAllActiveDeployments(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to get auth info",
		})
	}

	filters := types.DeploymentFilter{
		WorkspaceID: cc.AuthInfo.Workspace.Id,
		Active:      ptr.To(true),
	}

	deployments, err := g.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), filters)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "failed to get deployments",
		})
	}

	return g.stopDeployments(deployments, ctx)
}

func (g *DeploymentGroup) stopDeployments(deployments []types.DeploymentWithRelated, ctx echo.Context) error {
	for _, deployment := range deployments {
		// Stop active containers
		containers, err := g.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId)
		if err == nil {
			for _, container := range containers {
				g.scheduler.Stop(container.ContainerId)
			}
		}

		// Disable deployment
		deployment.Active = false
		_, err = g.backendRepo.UpdateDeployment(ctx.Request().Context(), deployment.Deployment)
		if err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"error": "failed to disable deployment",
			})
		}

		// Publish reload instance event
		eventBus := common.NewEventBus(g.redisClient)
		eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
			"stub_id":   deployment.Stub.ExternalId,
			"stub_type": deployment.StubType,
		}})
	}

	return ctx.JSON(http.StatusOK, "")
}
