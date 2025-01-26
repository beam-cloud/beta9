package apiv1

import (
	"net/http"
	"path"
	"time"

	"github.com/labstack/echo/v4"
	"k8s.io/utils/ptr"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
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
	g.GET("/:workspaceId/latest", auth.WithWorkspaceAuth(group.ListLatestDeployments))
	g.GET("/:workspaceId/:deploymentId", auth.WithWorkspaceAuth(group.RetrieveDeployment))
	g.GET("/:workspaceId/download/:stubId", auth.WithWorkspaceAuth(group.DownloadDeploymentPackage))
	g.POST("/:workspaceId/stop/:deploymentId", auth.WithWorkspaceAuth(group.StopDeployment))
	g.POST("/:workspaceId/start/:deploymentId", auth.WithWorkspaceAuth(group.StartDeployment))
	g.POST("/:workspaceId/stop-all-active-deployments", auth.WithClusterAdminAuth(group.StopAllActiveDeployments))
	g.DELETE("/:workspaceId/:deploymentId", auth.WithWorkspaceAuth(group.DeleteDeployment))

	return group
}

func (g *DeploymentGroup) ListDeployments(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	var filters types.DeploymentFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	if filters.Pagination {
		if deployments, err := g.backendRepo.ListDeploymentsPaginated(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list deployments")
		} else {
			return ctx.JSON(http.StatusOK, deployments)
		}
	} else {
		if deployments, err := g.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list deployments")
		} else {
			sanitizeDeployments(deployments)
			return ctx.JSON(http.StatusOK, deployments)
		}

	}
}

func (g *DeploymentGroup) RetrieveDeployment(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	deploymentId := ctx.Param("deploymentId")
	if deployment, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), workspace.Id, deploymentId); err != nil {
		return HTTPInternalServerError("Failed to get deployment")
	} else if deployment == nil {
		return HTTPNotFound()
	} else {
		deployment.Stub.SanitizeConfig()
		return ctx.JSON(http.StatusOK, deployment)
	}
}

func (g *DeploymentGroup) StopDeployment(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	deploymentId := ctx.Param("deploymentId")

	// Get deployment
	deploymentWithRelated, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentId)
	if err != nil {
		return HTTPBadRequest("Failed to get deployment")
	}

	if deploymentWithRelated == nil {
		return HTTPBadRequest("Deployment not found")
	}

	return g.stopDeployments([]types.DeploymentWithRelated{*deploymentWithRelated}, ctx)
}

func (g *DeploymentGroup) StartDeployment(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	deploymentId := ctx.Param("deploymentId")

	// Get deployment
	deploymentWithRelated, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentId)
	if err != nil {
		return HTTPBadRequest("Failed to get deployment")
	}

	if deploymentWithRelated == nil {
		return HTTPBadRequest("Deployment not found")
	}

	// Start deployment
	deploymentWithRelated.Deployment.Active = true
	if _, err := g.backendRepo.UpdateDeployment(ctx.Request().Context(), deploymentWithRelated.Deployment); err != nil {
		return HTTPInternalServerError("Failed to start deployment")
	}

	// Publish reload instance event
	eventBus := common.NewEventBus(g.redisClient)
	eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
		"stub_id":   deploymentWithRelated.Stub.ExternalId,
		"stub_type": deploymentWithRelated.StubType,
		"timestamp": time.Now().Unix(),
	}})

	return ctx.NoContent(http.StatusOK)
}

func (g *DeploymentGroup) StopAllActiveDeployments(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	filters := types.DeploymentFilter{
		WorkspaceID: workspace.Id,
		Active:      ptr.To(true),
	}

	deployments, err := g.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), filters)
	if err != nil {
		return HTTPBadRequest("Failed to get deployments")
	}

	return g.stopDeployments(deployments, ctx)
}

func (g *DeploymentGroup) DeleteDeployment(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	deploymentId := ctx.Param("deploymentId")

	// Get deployment
	deploymentWithRelated, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentId)
	if err != nil {
		return HTTPBadRequest("Failed to get deployment")
	}

	if deploymentWithRelated == nil {
		return HTTPBadRequest("Deployment not found")
	}

	// Delete deployment
	if err := g.backendRepo.DeleteDeployment(ctx.Request().Context(), deploymentWithRelated.Deployment); err != nil {
		return HTTPInternalServerError("Failed to delete deployment")
	}

	// Stop deployment
	if err := g.stopDeployments([]types.DeploymentWithRelated{*deploymentWithRelated}, ctx); err != nil {
		return HTTPInternalServerError("Failed to stop deployment")
	}

	return ctx.NoContent(http.StatusOK)
}

func (g *DeploymentGroup) ListLatestDeployments(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	var filters types.DeploymentFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	if deployments, err := g.backendRepo.ListLatestDeploymentsWithRelatedPaginated(ctx.Request().Context(), filters); err != nil {
		return HTTPInternalServerError("Failed to list deployments")
	} else {
		sanitizeDeployments(deployments.Data)
		return ctx.JSON(http.StatusOK, deployments)
	}
}

func (g *DeploymentGroup) DownloadDeploymentPackage(ctx echo.Context) error {
	stubId := ctx.Param("stubId")

	extWorkspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), extWorkspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	object, err := g.backendRepo.GetObjectByExternalStubId(ctx.Request().Context(), stubId, workspace.Id)
	if err != nil {
		return HTTPInternalServerError("Failed to get object")
	}
	path := getPackagePath(workspace.Name, object.ExternalId)

	return ctx.File(path)
}

func (g *DeploymentGroup) stopDeployments(deployments []types.DeploymentWithRelated, ctx echo.Context) error {
	for _, deployment := range deployments {
		// Stop scheduled job
		if deployment.StubType == types.StubTypeScheduledJobDeployment {
			if scheduledJob, err := g.backendRepo.GetScheduledJob(ctx.Request().Context(), deployment.Id); err == nil {
				g.backendRepo.DeleteScheduledJob(ctx.Request().Context(), scheduledJob)
			}
		}

		// Stop active containers
		containers, err := g.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId)
		if err == nil {
			for _, container := range containers {
				g.scheduler.Stop(&types.StopContainerArgs{ContainerId: container.ContainerId})
			}
		}

		// Disable deployment
		deployment.Active = false
		_, err = g.backendRepo.UpdateDeployment(ctx.Request().Context(), deployment.Deployment)
		if err != nil {
			return HTTPInternalServerError("Failed to disable deployment")
		}

		// Publish reload instance event
		eventBus := common.NewEventBus(g.redisClient)
		eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
			"stub_id":   deployment.Stub.ExternalId,
			"stub_type": deployment.StubType,
			"timestamp": time.Now().Unix(),
		}})
	}

	return ctx.NoContent(http.StatusOK)
}

func getPackagePath(workspaceName, objectId string) string {
	return path.Join("/data/objects/", workspaceName, objectId)
}

func sanitizeDeployments(deployments []types.DeploymentWithRelated) {
	for i := range deployments {
		deployments[i].Stub.SanitizeConfig()
	}
}
