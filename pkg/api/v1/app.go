package apiv1

import (
	"context"
	"net/http"
	"strings"

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
	Stub              *types.StubWithRelated       `json:"stub,omitempty" serializer:"stub"`
	Deployment        *types.DeploymentWithRelated `json:"deployment,omitempty" serializer:"deployment"`
	URL               string                       `json:"url,omitempty" serializer:"url,omitempty"`
	InvokeURL         string                       `json:"invoke_url,omitempty" serializer:"invoke_url,omitempty"`
	ConnectionURL     string                       `json:"connection_url,omitempty" serializer:"connection_url,omitempty"`
	PoolName          string                       `json:"pool_name" serializer:"pool_name"`
	RunningContainers int                          `json:"running_containers" serializer:"running_containers"`
	IsService         bool                         `json:"is_service" serializer:"is_service"`
	Serving           *types.ServingConfig         `json:"serving,omitempty" serializer:"serving"`
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

	latestStubIndexes := map[string][]int{}
	for i := range apps.Data {
		appsWithLatest.Data[i].App = apps.Data[i]

		if deployment, ok := deploymentsByApp[apps.Data[i].ExternalId]; ok {
			deploymentCopy := deployment
			a.enrichAppWithStubConfig(ctx.Request().Context(), &workspace, &appsWithLatest.Data[i], &deploymentCopy.Stub, &deploymentCopy.Deployment)
			if deploymentCopy.Stub.ExternalId != "" {
				latestStubIndexes[deploymentCopy.Stub.ExternalId] = append(latestStubIndexes[deploymentCopy.Stub.ExternalId], i)
			}
			deploymentCopy.URL = appsWithLatest.Data[i].URL
			deploymentCopy.InvokeURL = appsWithLatest.Data[i].InvokeURL
			if err := deploymentCopy.Stub.SanitizeConfig(); err != nil {
				return HTTPInternalServerError("Failed to sanitize stub config")
			}
			appsWithLatest.Data[i].Deployment = &deploymentCopy
			continue
		}

		stub, ok := stubsByApp[apps.Data[i].ExternalId]
		if !ok {
			continue
		}

		stubCopy := stub
		a.enrichAppWithStubConfig(ctx.Request().Context(), &workspace, &appsWithLatest.Data[i], &stubCopy.Stub, nil)
		if stubCopy.Stub.ExternalId != "" {
			latestStubIndexes[stubCopy.Stub.ExternalId] = append(latestStubIndexes[stubCopy.Stub.ExternalId], i)
		}
		appsWithLatest.Data[i].Stub = &stubCopy
		if err := appsWithLatest.Data[i].Stub.SanitizeConfig(); err != nil {
			return HTTPInternalServerError("Failed to sanitize stub config")
		}
	}

	if a.containerRepo != nil && len(latestStubIndexes) > 0 {
		runningByStubID, err := countRunningContainersByStubID(a.containerRepo, latestStubIndexes)
		if err != nil {
			return HTTPInternalServerError("Failed to get running containers")
		}

		for stubID, indexes := range latestStubIndexes {
			for _, index := range indexes {
				appsWithLatest.Data[index].RunningContainers = runningByStubID[stubID]
			}
		}
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

func countRunningContainersByStubID(containerRepo repository.ContainerRepository, latestStubIndexes map[string][]int) (map[string]int, error) {
	runningByStubID := make(map[string]int, len(latestStubIndexes))
	for stubID := range latestStubIndexes {
		containers, err := containerRepo.GetActiveContainersByStubId(stubID)
		if err != nil {
			return nil, err
		}

		for _, container := range containers {
			if container.Status == types.ContainerStatusRunning {
				runningByStubID[stubID]++
			}
		}
	}
	return runningByStubID, nil
}

func (a *AppGroup) enrichAppWithStubConfig(ctx context.Context, workspace *types.Workspace, app *AppWithLatestStubOrDeployment, stub *types.Stub, deployment *types.Deployment) {
	if stub == nil {
		return
	}

	config, err := stub.UnmarshalConfig()
	if err != nil || config == nil {
		return
	}

	if config.Pool != nil {
		poolName := strings.TrimSpace(config.Pool.Name)
		if poolName != "" && poolName != types.DefaultCPUWorkerPoolName {
			app.PoolName = poolName
		}
	}
	app.IsService = config.IsService
	app.Serving = cloneServingConfig(config.EffectiveServingConfig())
	app.URL = a.appURL(stub, config, deployment)
	app.InvokeURL = app.URL
	if connectionURL := a.databaseConnectionURL(ctx, workspace, app.Serving); connectionURL != "" {
		app.ConnectionURL = connectionURL
		app.Serving.Database.ConnectionURL = connectionURL
	}
}

func cloneServingConfig(serving *types.ServingConfig) *types.ServingConfig {
	if serving == nil {
		return nil
	}
	clone := *serving
	if serving.Database != nil {
		database := *serving.Database
		clone.Database = &database
	}
	if serving.LLM != nil {
		llm := *serving.LLM
		clone.LLM = &llm
	}
	return &clone
}

func (a *AppGroup) appURL(stub *types.Stub, config *types.StubConfigV1, deployment *types.Deployment) string {
	stubWithRelated := &types.StubWithRelated{Stub: *stub}
	if stub.Type.Kind() == types.StubTypePod || stub.Type.Kind() == types.StubTypeSandbox {
		externalURL := a.config.GatewayService.HTTP.GetExternalURL()
		urlType := a.config.GatewayService.InvokeURLType
		if config.TCP {
			externalURL = a.config.Abstractions.Pod.TCP.GetExternalURL()
			urlType = common.InvokeUrlTypeHost
		}
		if deployment != nil {
			return common.BuildPodDeploymentURL(externalURL, urlType, deployment, config)
		}
		return common.BuildPodURL(externalURL, urlType, stubWithRelated, config)
	}
	if deployment != nil {
		return common.BuildDeploymentURL(a.config.GatewayService.HTTP.GetExternalURL(), a.config.GatewayService.InvokeURLType, stubWithRelated, deployment)
	}
	return common.BuildStubURL(a.config.GatewayService.HTTP.GetExternalURL(), a.config.GatewayService.InvokeURLType, stubWithRelated)
}

func (a *AppGroup) databaseConnectionURL(ctx context.Context, workspace *types.Workspace, serving *types.ServingConfig) string {
	if workspace == nil || serving == nil || serving.Database == nil || serving.Database.ConnectionURLSecretName == "" {
		return ""
	}
	secret, err := a.backendRepo.GetSecretByNameDecrypted(ctx, workspace, serving.Database.ConnectionURLSecretName)
	if err != nil {
		log.Warn().Err(err).Str("secret_name", serving.Database.ConnectionURLSecretName).Msg("failed to load database connection url")
		return ""
	}
	return secret.Value
}

func (a *AppGroup) appWithLatestStubOrDeployment(ctx context.Context, workspace *types.Workspace, app types.App) (AppWithLatestStubOrDeployment, error) {
	appWithLatest := AppWithLatestStubOrDeployment{App: app}

	deploymentsByApp, err := a.backendRepo.ListLatestDeploymentsByAppIDs(ctx, workspace.Id, []string{app.ExternalId})
	if err != nil {
		return appWithLatest, err
	}
	if deployment, ok := deploymentsByApp[app.ExternalId]; ok {
		deploymentCopy := deployment
		a.enrichAppWithStubConfig(ctx, workspace, &appWithLatest, &deploymentCopy.Stub, &deploymentCopy.Deployment)
		deploymentCopy.URL = appWithLatest.URL
		deploymentCopy.InvokeURL = appWithLatest.InvokeURL
		appWithLatest.Deployment = &deploymentCopy
		return appWithLatest, nil
	}

	stubsByApp, err := a.backendRepo.ListLatestStubsByAppIDs(ctx, workspace.Id, []string{app.ExternalId})
	if err != nil {
		return appWithLatest, err
	}
	if stub, ok := stubsByApp[app.ExternalId]; ok {
		stubCopy := stub
		a.enrichAppWithStubConfig(ctx, workspace, &appWithLatest, &stubCopy.Stub, nil)
		appWithLatest.Stub = &stubCopy
	}

	return appWithLatest, nil
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

	appWithLatest, err := a.appWithLatestStubOrDeployment(ctx.Request().Context(), &workspace, *app)
	if err != nil {
		return HTTPInternalServerError("Failed to get app metadata")
	}

	serializedApp, err := serializer.Serialize(appWithLatest)
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
