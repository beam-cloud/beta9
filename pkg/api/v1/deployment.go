package apiv1

import (
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/labstack/echo/v4"
	"k8s.io/utils/ptr"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	repoCommon "github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
)

const presignedURLExpirationS = 10 * 60

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
			paginatedSerializedDeployments := repoCommon.CursorPaginationInfo[types.DeploymentWithRelated]{
				Data: deployments.Data,
				Next: deployments.Next,
			}

			serializedPaginatedDeployments, err := serializer.Serialize(paginatedSerializedDeployments)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize response")
			}

			return ctx.JSON(http.StatusOK, serializedPaginatedDeployments)
		}
	} else {
		if deployments, err := g.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list deployments")
		} else {
			serializedDeployments, err := serializer.Serialize(deployments)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize response")
			}

			return ctx.JSON(http.StatusOK, serializedDeployments)
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
		serializedDeployment, err := serializer.Serialize(deployment)
		if err != nil {
			return HTTPInternalServerError("Failed to serialize response")
		}

		return ctx.JSON(http.StatusOK, serializedDeployment)
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

	if err = stopDeployments(ctx.Request().Context(), []types.DeploymentWithRelated{*deploymentWithRelated}, CommonClients{
		containerRepo: g.containerRepo,
		backendRepo:   g.backendRepo,
		scheduler:     g.scheduler,
		redisClient:   g.redisClient,
	}); err != nil {
		return HTTPInternalServerError(err.Error())
	}

	return ctx.NoContent(http.StatusOK)
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

	if err = stopDeployments(ctx.Request().Context(), deployments, CommonClients{
		containerRepo: g.containerRepo,
		backendRepo:   g.backendRepo,
		scheduler:     g.scheduler,
		redisClient:   g.redisClient,
	}); err != nil {
		return HTTPInternalServerError(err.Error())
	}

	return ctx.NoContent(http.StatusOK)
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
	if err := stopDeployments(ctx.Request().Context(), []types.DeploymentWithRelated{*deploymentWithRelated}, CommonClients{
		containerRepo: g.containerRepo,
		backendRepo:   g.backendRepo,
		scheduler:     g.scheduler,
		redisClient:   g.redisClient,
	}); err != nil {
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
		paginatedSerializedDeployments := repoCommon.CursorPaginationInfo[types.DeploymentWithRelated]{
			Data: deployments.Data,
			Next: deployments.Next,
		}

		serializedPaginatedDeployments, err := serializer.Serialize(paginatedSerializedDeployments)
		if err != nil {
			return HTTPInternalServerError("Failed to serialize response")
		}

		return ctx.JSON(http.StatusOK, serializedPaginatedDeployments)
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

	if workspace.StorageId != nil {
		workspaceStorage, err := g.backendRepo.GetWorkspaceStorage(ctx.Request().Context(), *workspace.StorageId)
		if err != nil {
			return HTTPInternalServerError("Failed to get workspace storage")
		}
		storageClient, err := clients.NewWorkspaceStorageClient(ctx.Request().Context(), workspace.Name, workspaceStorage)
		if err != nil {
			return HTTPInternalServerError("Failed to get object")
		}
		presignedURL, err := storageClient.GeneratePresignedGetURL(ctx.Request().Context(), fmt.Sprintf("%s/%s", types.DefaultObjectPrefix, object.ExternalId), presignedURLExpirationS)
		if err != nil {
			return HTTPInternalServerError("Failed to generate presigned URL")
		}

		return ctx.Redirect(http.StatusTemporaryRedirect, presignedURL)
	}

	path := getPackagePath(workspace.Name, object.ExternalId)
	return ctx.File(path)
}

func getPackagePath(workspaceName, objectId string) string {
	return path.Join("/data/objects/", workspaceName, objectId)
}
