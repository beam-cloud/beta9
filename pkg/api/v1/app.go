package apiv1

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	repoCommon "github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
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
	g.GET("/:workspaceId/activity", auth.WithWorkspaceAuth(group.GetAppActivity))
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListApps))
	g.GET("/:workspaceId/:appId", auth.WithWorkspaceAuth(group.RetrieveApp))
	g.DELETE("/:workspaceId/:appId", auth.WithStrictWorkspaceAuth(group.DeleteApp))

	return group
}

type AppWithLatestStubOrDeployment struct {
	types.App
	Stub          *types.StubWithRelated       `json:"stub,omitempty" serializer:"stub"`
	Deployment    *types.DeploymentWithRelated `json:"deployment,omitempty" serializer:"deployment"`
	URL           string                       `json:"url,omitempty" serializer:"url,omitempty"`
	InvokeURL     string                       `json:"invoke_url,omitempty" serializer:"invoke_url,omitempty"`
	ConnectionURL string                       `json:"connection_url,omitempty" serializer:"connection_url,omitempty"`
	PoolName      string                       `json:"pool_name" serializer:"pool_name"`
	// RunningContainers counts running containers across every stub in the
	// app, not just the latest one, so multi-function and multi-config apps
	// read as live whenever any of them is doing work.
	RunningContainers int `json:"running_containers" serializer:"running_containers"`
	// ActiveDeployments counts deployments in the app that are currently
	// active (deployed and not stopped), across all of its functions.
	ActiveDeployments int `json:"active_deployments" serializer:"active_deployments"`
	// State is derived from the two counts above: running > idle > stopped.
	// Only the list endpoint knows running containers, so only it sets this.
	State     types.AppState       `json:"state,omitempty" serializer:"state,omitempty"`
	IsService bool                 `json:"is_service" serializer:"is_service"`
	Serving   *types.ServingConfig `json:"serving,omitempty" serializer:"serving"`
}

// AppListResponse is a page of apps plus workspace-wide state counts, so the
// dashboard's Running / Idle / Stopped tabs describe the whole workspace and
// not just the page that happens to be loaded.
type AppListResponse struct {
	repoCommon.CursorPaginationInfo[AppWithLatestStubOrDeployment]
	StateCounts types.AppStateCounts `json:"state_counts" serializer:"state_counts"`
}

func appStateFor(runningContainers, activeDeployments int) types.AppState {
	switch {
	case runningContainers > 0:
		return types.AppStateRunning
	case activeDeployments > 0:
		return types.AppStateIdle
	default:
		return types.AppStateStopped
	}
}

// workspaceAppState is everything needed to classify every app in a workspace:
// running containers per app (Redis) and active deployments per app (Postgres).
// Apps in neither map are stopped.
type workspaceAppState struct {
	runningByApp map[string]int
	activeByApp  map[string]int
	totalApps    int
}

func (w workspaceAppState) counts() types.AppStateCounts {
	counts := types.AppStateCounts{All: w.totalApps}
	for appID := range w.runningByApp {
		if w.runningByApp[appID] > 0 {
			counts.Running++
		}
	}
	for appID, active := range w.activeByApp {
		if active > 0 && w.runningByApp[appID] == 0 {
			counts.Idle++
		}
	}
	counts.Stopped = counts.All - counts.Running - counts.Idle
	if counts.Stopped < 0 {
		counts.Stopped = 0
	}
	return counts
}

// scope narrows an AppFilter to the apps in the requested state. Running and
// idle are finite id sets; stopped is everything else.
func (w workspaceAppState) scope(filters *types.AppFilter, state types.AppState) {
	running := make([]string, 0, len(w.runningByApp))
	for appID, n := range w.runningByApp {
		if n > 0 {
			running = append(running, appID)
		}
	}
	idle := make([]string, 0, len(w.activeByApp))
	for appID, n := range w.activeByApp {
		if n > 0 && w.runningByApp[appID] == 0 {
			idle = append(idle, appID)
		}
	}
	switch state {
	case types.AppStateRunning:
		filters.IncludeExternalIds = running
	case types.AppStateIdle:
		filters.IncludeExternalIds = idle
	case types.AppStateStopped:
		filters.ExcludeExternalIds = append(running, idle...)
	}
}

// loadWorkspaceAppState fans out the three independent reads in parallel.
func (a *AppGroup) loadWorkspaceAppState(ctx context.Context, workspace *types.Workspace) (workspaceAppState, error) {
	state := workspaceAppState{runningByApp: map[string]int{}, activeByApp: map[string]int{}}
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if a.containerRepo == nil {
			return nil
		}
		running, err := countRunningContainersForApps(gctx, a.containerRepo, a.backendRepo, workspace.ExternalId)
		if err != nil {
			return err
		}
		state.runningByApp = running
		return nil
	})
	g.Go(func() error {
		active, err := a.backendRepo.CountActiveDeploymentsByApp(gctx, workspace.Id, nil)
		if err != nil {
			return err
		}
		state.activeByApp = active
		return nil
	})
	g.Go(func() error {
		total, err := a.backendRepo.CountApps(gctx, workspace.Id)
		if err != nil {
			return err
		}
		state.totalApps = total
		return nil
	})

	if err := g.Wait(); err != nil {
		return workspaceAppState{}, err
	}
	return state, nil
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
	stateFilter, ok := types.ParseAppState(ctx.QueryParam("state"))
	if !ok {
		return HTTPBadRequest("Invalid state filter; expected running, idle or stopped")
	}

	reqCtx := ctx.Request().Context()

	// Live state for the whole workspace comes first: it drives both the tab
	// counts and, when a state filter is set, which apps the page may contain.
	appState, err := a.loadWorkspaceAppState(reqCtx, &workspace)
	if err != nil {
		return HTTPInternalServerError("Failed to get app state")
	}
	if stateFilter != "" {
		appState.scope(&filters, stateFilter)
	}

	apps, err := a.backendRepo.ListAppsPaginated(reqCtx, workspace.Id, filters)
	if err != nil {
		return err
	}

	response := AppListResponse{
		CursorPaginationInfo: repoCommon.CursorPaginationInfo[AppWithLatestStubOrDeployment]{
			Data: make([]AppWithLatestStubOrDeployment, len(apps.Data)),
			Next: apps.Next,
		},
		StateCounts: appState.counts(),
	}

	appIDs := make([]string, 0, len(apps.Data))
	for i := range apps.Data {
		appIDs = append(appIDs, apps.Data[i].ExternalId)
	}

	deploymentsByApp, err := a.backendRepo.ListLatestDeploymentsByAppIDs(reqCtx, workspace.Id, appIDs)
	if err != nil {
		return HTTPBadRequest("Failed to get apps")
	}

	appIDsWithoutDeployment := make([]string, 0, len(apps.Data))
	for i := range apps.Data {
		if _, ok := deploymentsByApp[apps.Data[i].ExternalId]; !ok {
			appIDsWithoutDeployment = append(appIDsWithoutDeployment, apps.Data[i].ExternalId)
		}
	}

	stubsByApp, err := a.backendRepo.ListLatestStubsByAppIDs(reqCtx, workspace.Id, appIDsWithoutDeployment)
	if err != nil {
		return HTTPBadRequest("Failed to get apps")
	}

	for i := range apps.Data {
		app := &response.Data[i]
		app.App = apps.Data[i]
		app.RunningContainers = appState.runningByApp[apps.Data[i].ExternalId]
		app.ActiveDeployments = appState.activeByApp[apps.Data[i].ExternalId]
		app.State = appStateFor(app.RunningContainers, app.ActiveDeployments)

		if deployment, ok := deploymentsByApp[apps.Data[i].ExternalId]; ok {
			deploymentCopy := deployment
			a.enrichAppWithStubConfig(app, &deploymentCopy.Stub, &deploymentCopy.Deployment, false)
			deploymentCopy.URL = app.URL
			deploymentCopy.InvokeURL = app.InvokeURL
			if err := sanitizeDeploymentWithRelated(&deploymentCopy); err != nil {
				return HTTPInternalServerError("Failed to sanitize stub config")
			}
			app.Deployment = &deploymentCopy
			continue
		}

		stub, ok := stubsByApp[apps.Data[i].ExternalId]
		if !ok {
			continue
		}

		stubCopy := stub
		a.enrichAppWithStubConfig(app, &stubCopy.Stub, nil, false)
		app.Stub = &stubCopy
		if err := sanitizeStubWithRelated(app.Stub); err != nil {
			return HTTPInternalServerError("Failed to sanitize stub config")
		}
	}

	serialized, err := serializer.Serialize(response)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serialized)
}

const (
	appActivityWindow  = 24 * time.Hour
	maxActivityAppIDs  = 100
	activityBucketSize = time.Hour
)

// GetAppActivity returns the last 24h of hourly activity for a set of apps in
// one call: tasks created (and failed) from Postgres, sandboxes created from
// the Redis counters bumped at creation. Every app gets a full, zero-filled
// series so the client never has to align buckets itself.
func (a *AppGroup) GetAppActivity(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	appIDs := uniqueNonEmpty(strings.Split(ctx.QueryParam("app_ids"), ","))
	if len(appIDs) == 0 {
		return HTTPBadRequest("app_ids is required")
	}
	if len(appIDs) > maxActivityAppIDs {
		return HTTPBadRequest("Too many app_ids")
	}

	now := time.Now().UTC()
	since := now.Add(-appActivityWindow).Truncate(activityBucketSize)

	var tasks, sandboxes map[string][]types.AppActivityBucket
	g, gctx := errgroup.WithContext(ctx.Request().Context())
	g.Go(func() error {
		var err error
		tasks, err = a.backendRepo.AggregateTaskActivityByApp(gctx, workspace.Id, appIDs, since)
		return err
	})
	g.Go(func() error {
		if a.containerRepo == nil {
			return nil
		}
		var err error
		sandboxes, err = a.containerRepo.GetSandboxActivity(workspaceID, appIDs, since)
		return err
	})
	if err := g.Wait(); err != nil {
		return HTTPInternalServerError("Failed to get app activity")
	}

	bucketCount := int(appActivityWindow/activityBucketSize) + 1
	response := make(map[string][]types.AppActivityBucket, len(appIDs))
	for _, appID := range appIDs {
		byHour := make(map[int64]*types.AppActivityBucket, bucketCount)
		series := make([]types.AppActivityBucket, 0, bucketCount)
		for t := since; !t.After(now); t = t.Add(activityBucketSize) {
			series = append(series, types.AppActivityBucket{Time: t})
		}
		for i := range series {
			byHour[series[i].Time.Unix()] = &series[i]
		}
		for _, source := range [][]types.AppActivityBucket{tasks[appID], sandboxes[appID]} {
			for _, b := range source {
				if target, ok := byHour[b.Time.UTC().Truncate(activityBucketSize).Unix()]; ok {
					target.Total += b.Total
					target.Failed += b.Failed
				}
			}
		}
		response[appID] = series
	}

	return ctx.JSON(http.StatusOK, response)
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// countRunningContainersForApps counts running containers per app across all
// of the workspace's active containers, resolving each container's stub to its
// app so containers from older stubs and other functions are included.
func countRunningContainersForApps(ctx context.Context, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, workspaceID string) (map[string]int, error) {
	containers, err := containerRepo.GetActiveContainersByWorkspaceId(workspaceID)
	if err != nil {
		return nil, err
	}

	runningByStubID := map[string]int{}
	for _, container := range containers {
		if container.Status != types.ContainerStatusRunning || container.StubId == "" {
			continue
		}
		runningByStubID[container.StubId]++
	}

	runningByAppID := make(map[string]int, len(runningByStubID))
	if len(runningByStubID) == 0 {
		return runningByAppID, nil
	}

	stubIDs := make([]string, 0, len(runningByStubID))
	for stubID := range runningByStubID {
		stubIDs = append(stubIDs, stubID)
	}

	appIDsByStub, err := backendRepo.ListAppIDsByStubExternalIDs(ctx, workspaceID, stubIDs)
	if err != nil {
		return nil, err
	}

	for stubID, count := range runningByStubID {
		if appID, ok := appIDsByStub[stubID]; ok && appID != "" {
			runningByAppID[appID] += count
		}
	}

	return runningByAppID, nil
}

func (a *AppGroup) enrichAppWithStubConfig(app *AppWithLatestStubOrDeployment, stub *types.Stub, deployment *types.Deployment, includeDatabaseSecretNames bool) {
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
	if !includeDatabaseSecretNames {
		clearDatabaseSecretNames(app.Serving)
	}
	app.URL = a.appURL(stub, config, deployment)
	app.InvokeURL = app.URL
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

func clearDatabaseSecretNames(serving *types.ServingConfig) {
	if serving == nil || serving.Database == nil {
		return
	}
	serving.Database.ClearSecretNames()
}

func sanitizeDeploymentWithRelated(deployment *types.DeploymentWithRelated) error {
	deployment.Workspace = deployment.Workspace.WithoutPrivateCredentials()
	return deployment.Stub.SanitizeConfig()
}

func sanitizeStubWithRelated(stub *types.StubWithRelated) error {
	stub.Workspace = stub.Workspace.WithoutPrivateCredentials()
	return stub.SanitizeConfig()
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

func (a *AppGroup) hydrateDatabaseConnectionURL(ctx context.Context, workspace *types.Workspace, app *AppWithLatestStubOrDeployment) {
	if app == nil {
		return
	}
	defer clearDatabaseSecretNames(app.Serving)

	if workspace == nil || workspace.SigningKey == nil || app.Serving == nil || app.Serving.Database == nil {
		return
	}

	name := app.Serving.Database.ConnectionURLSecretName
	if name == "" {
		return
	}

	secrets, err := a.backendRepo.GetSecretsByNameDecrypted(ctx, workspace, []string{name})
	if err != nil {
		log.Warn().Err(err).Str("secret_name", name).Msg("failed to load database connection url")
		return
	}
	if len(secrets) == 0 {
		return
	}

	app.ConnectionURL = secrets[0].Value
	app.Serving.Database.ConnectionURL = secrets[0].Value
}

func (a *AppGroup) appWithLatestStubOrDeployment(ctx context.Context, workspace *types.Workspace, app types.App) (AppWithLatestStubOrDeployment, error) {
	appWithLatest := AppWithLatestStubOrDeployment{App: app}

	activeDeploymentsByApp, err := a.backendRepo.CountActiveDeploymentsByApp(ctx, workspace.Id, []string{app.ExternalId})
	if err != nil {
		return appWithLatest, err
	}
	appWithLatest.ActiveDeployments = activeDeploymentsByApp[app.ExternalId]

	deploymentsByApp, err := a.backendRepo.ListLatestDeploymentsByAppIDs(ctx, workspace.Id, []string{app.ExternalId})
	if err != nil {
		return appWithLatest, err
	}
	if deployment, ok := deploymentsByApp[app.ExternalId]; ok {
		deploymentCopy := deployment
		a.enrichAppWithStubConfig(&appWithLatest, &deploymentCopy.Stub, &deploymentCopy.Deployment, true)
		deploymentCopy.URL = appWithLatest.URL
		deploymentCopy.InvokeURL = appWithLatest.InvokeURL
		if err := sanitizeDeploymentWithRelated(&deploymentCopy); err != nil {
			return appWithLatest, err
		}
		appWithLatest.Deployment = &deploymentCopy
		return appWithLatest, nil
	}

	stubsByApp, err := a.backendRepo.ListLatestStubsByAppIDs(ctx, workspace.Id, []string{app.ExternalId})
	if err != nil {
		return appWithLatest, err
	}
	if stub, ok := stubsByApp[app.ExternalId]; ok {
		stubCopy := stub
		a.enrichAppWithStubConfig(&appWithLatest, &stubCopy.Stub, nil, true)
		if err := sanitizeStubWithRelated(&stubCopy); err != nil {
			return appWithLatest, err
		}
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

	workspace, err := a.backendRepo.GetWorkspaceByExternalIdWithSigningKey(ctx.Request().Context(), workspaceID)
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
	a.hydrateDatabaseConnectionURL(ctx.Request().Context(), &workspace, &appWithLatest)

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
