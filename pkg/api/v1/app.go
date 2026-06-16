package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	repoCommon "github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type AppGroup struct {
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	scheduler     scheduler.Scheduler
	redisClient   *common.RedisClient
}

func NewAppGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig, containerRepo repository.ContainerRepository, scheduler scheduler.Scheduler, redisClient *common.RedisClient) *AppGroup {
	group := &AppGroup{
		routerGroup:   g,
		backendRepo:   backendRepo,
		config:        config,
		containerRepo: containerRepo,
		scheduler:     scheduler,
		redisClient:   redisClient,
	}

	g.GET("/:workspaceId/latest", auth.WithWorkspaceAuth(group.ListAppWithLatestActivity))
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListApps))
	g.GET("/:workspaceId/:appId", auth.WithWorkspaceAuth(group.RetrieveApp))
	g.DELETE("/:workspaceId/:appId", auth.WithStrictWorkspaceAuth(group.DeleteApp))

	return group
}

type AppWithLatestStubOrDeployment struct {
	types.App
	Stub       *types.StubWithRelated       `json:"stub,omitempty" serializer:"stub"`
	Deployment *types.DeploymentWithRelated `json:"deployment,omitempty" serializer:"deployment"`
}

func (a *AppGroup) ListAppWithLatestActivity(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	var filters types.AppFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	apps, err := a.backendRepo.ListAppsPaginated(ctx.Request().Context(), workspace.Id, filters)
	if err != nil {
		return err
	}

	appsWithLatest := repoCommon.CursorPaginationInfo[AppWithLatestStubOrDeployment]{
		Data: make([]AppWithLatestStubOrDeployment, len(apps.Data)),
		Next: apps.Next,
	}

	appIDs := make([]string, 0, len(apps.Data))
	for i := range apps.Data {
		appIDs = append(appIDs, apps.Data[i].ExternalId)
	}

	deploymentsByApp, err := a.backendRepo.ListLatestDeploymentsByAppIDs(ctx.Request().Context(), workspace.Id, appIDs)
	if err != nil {
		return HTTPBadRequest("Failed to get apps")
	}

	appIDsWithoutDeployment := make([]string, 0, len(apps.Data))
	for i := range apps.Data {
		if _, ok := deploymentsByApp[apps.Data[i].ExternalId]; !ok {
			appIDsWithoutDeployment = append(appIDsWithoutDeployment, apps.Data[i].ExternalId)
		}
	}

	stubsByApp, err := a.backendRepo.ListLatestStubsByAppIDs(ctx.Request().Context(), workspace.Id, appIDsWithoutDeployment)
	if err != nil {
		return HTTPBadRequest("Failed to get apps")
	}

	for i := range apps.Data {
		appsWithLatest.Data[i].App = apps.Data[i]

		if deployment, ok := deploymentsByApp[apps.Data[i].ExternalId]; ok {
			deploymentCopy := deployment
			appsWithLatest.Data[i].Deployment = &deploymentCopy
			continue
		}

		stub, ok := stubsByApp[apps.Data[i].ExternalId]
		if !ok {
			continue
		}

		stubCopy := stub
		appsWithLatest.Data[i].Stub = &stubCopy
		appsWithLatest.Data[i].Stub.SanitizeConfig()
	}

	serializedAppsWithLatest, err := serializer.Serialize(appsWithLatest)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(
		http.StatusOK,
		serializedAppsWithLatest,
	)
}

func (a *AppGroup) ListApps(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	var filters types.AppFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	apps, err := a.backendRepo.ListAppsPaginated(ctx.Request().Context(), workspace.Id, filters)
	if err != nil {
		return err
	}

	serializedApps, err := serializer.Serialize(apps)
	if err != nil {
		return HTTPBadRequest("Failed to serialize response")
	}

	return ctx.JSON(
		http.StatusOK,
		serializedApps,
	)
}

func (a *AppGroup) RetrieveApp(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")
	appId := ctx.Param("appId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	app, err := a.backendRepo.RetrieveApp(
		ctx.Request().Context(),
		workspace.Id,
		appId,
	)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve app")
	}

	if app == nil {
		return HTTPNotFound()
	}

	serializedApp, err := serializer.Serialize(app)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serializedApp)
}

func (a *AppGroup) DeleteApp(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")
	appId := ctx.Param("appId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	app, err := a.backendRepo.RetrieveApp(
		ctx.Request().Context(),
		workspace.Id,
		appId,
	)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve app")
	}

	if app == nil {
		return HTTPNotFound()
	}

	deploymentFilters := types.DeploymentFilter{
		WorkspaceID: workspace.Id,
		AppId:       app.ExternalId,
	}

	deployments, err := a.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), deploymentFilters)
	if err != nil {
		return HTTPBadRequest("Failed to get deployments")
	}

	if err = stopDeployments(ctx.Request().Context(), deployments, CommonClients{
		containerRepo: a.containerRepo,
		backendRepo:   a.backendRepo,
		scheduler:     a.scheduler,
		redisClient:   a.redisClient,
	}); err != nil {
		return HTTPInternalServerError(err.Error())
	}

	for _, deployment := range deployments {
		err := a.backendRepo.DeleteDeployment(ctx.Request().Context(), deployment.Deployment)
		if err != nil {
			log.Error().Str("deployment_id", deployment.ExternalId).Err(err).Msg("failed to delete deployment")
		}
	}

	stubFilters := types.StubFilter{
		WorkspaceID: workspace.ExternalId,
		AppId:       app.ExternalId,
	}

	stubs, err := a.backendRepo.ListStubs(ctx.Request().Context(), stubFilters)
	if err != nil {
		return HTTPBadRequest("Failed to get stubs")
	}

	stubMap := make(map[string]uint)
	for _, val := range stubs {
		stubMap[val.ExternalId] = 1
	}

	containerStates, err := a.containerRepo.GetActiveContainersByWorkspaceId(workspaceID)
	if err != nil {
		return HTTPInternalServerError("Failed to get containers")
	}

	for _, state := range containerStates {
		if _, ok := stubMap[state.StubId]; !ok {
			continue
		}

		err := a.scheduler.Stop(&types.StopContainerArgs{ContainerId: state.ContainerId})
		if err != nil {
			log.Error().Str("container_id", state.ContainerId).Err(err).Msg("failed to stop container")
		}
	}

	return a.backendRepo.DeleteApp(ctx.Request().Context(), appId)
}
