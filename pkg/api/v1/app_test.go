package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	repoCommon "github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type appBackendRepo struct {
	repository.BackendRepository
	workspace        *types.Workspace
	apps             repoCommon.CursorPaginationInfo[types.App]
	deploymentsByApp map[string][]types.DeploymentWithRelated
	stubsByApp       map[string][]types.StubWithRelated
	secrets          map[string]string
	secretLookups    int
	// extraStubApps maps stub external IDs that aren't any app's latest stub
	// to the app they belong to.
	extraStubApps map[string]string
	taskActivity  map[string][]types.AppActivityBucket
	lastFilters   types.AppFilter
}

type appContainerRepo struct {
	repository.ContainerRepository
	states          []types.ContainerState
	sandboxActivity map[string][]types.AppActivityBucket
}

func (r *appContainerRepo) GetActiveContainersByWorkspaceId(_ string) ([]types.ContainerState, error) {
	return r.states, nil
}

func (r *appContainerRepo) GetSandboxActivity(_ string, appIds []string, _ time.Time) (map[string][]types.AppActivityBucket, error) {
	out := map[string][]types.AppActivityBucket{}
	for _, id := range appIds {
		if b, ok := r.sandboxActivity[id]; ok {
			out[id] = b
		}
	}
	return out, nil
}

func (r *appBackendRepo) GetWorkspaceByExternalId(_ context.Context, externalId string) (types.Workspace, error) {
	if r.workspace == nil || r.workspace.ExternalId != externalId {
		return types.Workspace{}, errors.New("workspace not found")
	}

	return *r.workspace, nil
}

func (r *appBackendRepo) GetWorkspaceByExternalIdWithSigningKey(_ context.Context, externalId string) (types.Workspace, error) {
	return r.GetWorkspaceByExternalId(context.Background(), externalId)
}

// ListAppsPaginated honours the include / exclude scoping the handler derives
// from live state, the way the real query does.
func (r *appBackendRepo) ListAppsPaginated(_ context.Context, _ uint, filters types.AppFilter) (repoCommon.CursorPaginationInfo[types.App], error) {
	r.lastFilters = filters
	page := repoCommon.CursorPaginationInfo[types.App]{Next: r.apps.Next}
	for _, app := range r.apps.Data {
		if filters.IncludeExternalIds != nil {
			found := false
			for _, id := range filters.IncludeExternalIds {
				if id == app.ExternalId {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		excluded := false
		for _, id := range filters.ExcludeExternalIds {
			if id == app.ExternalId {
				excluded = true
			}
		}
		if excluded {
			continue
		}
		page.Data = append(page.Data, app)
	}
	return page, nil
}

func (r *appBackendRepo) CountApps(_ context.Context, _ uint) (int, error) {
	return len(r.apps.Data), nil
}

func (r *appBackendRepo) AggregateTaskActivityByApp(_ context.Context, _ uint, appIDs []string, _ time.Time) (map[string][]types.AppActivityBucket, error) {
	out := map[string][]types.AppActivityBucket{}
	for _, id := range appIDs {
		if b, ok := r.taskActivity[id]; ok {
			out[id] = b
		}
	}
	return out, nil
}

func (r *appBackendRepo) RetrieveApp(_ context.Context, workspaceID uint, appID string) (*types.App, error) {
	for i := range r.apps.Data {
		app := r.apps.Data[i]
		if app.WorkspaceId == workspaceID && app.ExternalId == appID {
			return &app, nil
		}
	}
	return nil, nil
}

func (r *appBackendRepo) ListDeploymentsWithRelated(_ context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	return r.deploymentsByApp[filters.AppId], nil
}

func (r *appBackendRepo) ListLatestDeploymentsByAppIDs(_ context.Context, _ uint, appExternalIDs []string) (map[string]types.DeploymentWithRelated, error) {
	deployments := map[string]types.DeploymentWithRelated{}
	for _, appID := range appExternalIDs {
		if len(r.deploymentsByApp[appID]) > 0 {
			deployments[appID] = r.deploymentsByApp[appID][0]
		}
	}
	return deployments, nil
}

func (r *appBackendRepo) CountActiveDeploymentsByApp(_ context.Context, _ uint, appExternalIDs []string) (map[string]int, error) {
	if appExternalIDs == nil {
		for appID := range r.deploymentsByApp {
			appExternalIDs = append(appExternalIDs, appID)
		}
	}
	counts := map[string]int{}
	for _, appID := range appExternalIDs {
		for _, deployment := range r.deploymentsByApp[appID] {
			if deployment.Active {
				counts[appID]++
			}
		}
	}
	return counts, nil
}

// ListAppIDsByStubExternalIDs resolves stubs through both the deployment and
// stub fixtures, plus any explicit extra mappings (e.g. older stubs that are
// no longer the app's latest).
func (r *appBackendRepo) ListAppIDsByStubExternalIDs(_ context.Context, _ string, stubExternalIDs []string) (map[string]string, error) {
	known := map[string]string{}
	for appID, deployments := range r.deploymentsByApp {
		for _, deployment := range deployments {
			known[deployment.Stub.ExternalId] = appID
		}
	}
	for appID, stubs := range r.stubsByApp {
		for _, stub := range stubs {
			known[stub.ExternalId] = appID
		}
	}
	for stubID, appID := range r.extraStubApps {
		known[stubID] = appID
	}
	result := map[string]string{}
	for _, stubID := range stubExternalIDs {
		if appID, ok := known[stubID]; ok {
			result[stubID] = appID
		}
	}
	return result, nil
}

func (r *appBackendRepo) ListStubs(_ context.Context, filters types.StubFilter) ([]types.StubWithRelated, error) {
	return r.stubsByApp[filters.AppId], nil
}

func (r *appBackendRepo) ListLatestStubsByAppIDs(_ context.Context, _ uint, appExternalIDs []string) (map[string]types.StubWithRelated, error) {
	stubs := map[string]types.StubWithRelated{}
	for _, appID := range appExternalIDs {
		if len(r.stubsByApp[appID]) > 0 {
			stubs[appID] = r.stubsByApp[appID][0]
		}
	}
	return stubs, nil
}

func (r *appBackendRepo) GetSecretsByNameDecrypted(_ context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error) {
	r.secretLookups++
	if workspace == nil || r.workspace == nil || workspace.Id != r.workspace.Id {
		return nil, errors.New("workspace not found")
	}
	secrets := make([]types.Secret, 0, len(names))
	for _, name := range names {
		if value, ok := r.secrets[name]; ok {
			secrets = append(secrets, types.Secret{Name: name, Value: value, WorkspaceId: workspace.Id})
		}
	}
	return secrets, nil
}

func TestListAppWithLatestActivityAllowsAppsWithoutActivity(t *testing.T) {
	workspace := &types.Workspace{Id: 1, ExternalId: "workspace-1", Name: "Workspace 1"}
	emptyApp := types.App{Id: 1, ExternalId: "app-empty", Name: "Empty App", WorkspaceId: workspace.Id}
	appWithStub := types.App{Id: 2, ExternalId: "app-with-stub", Name: "App With Stub", WorkspaceId: workspace.Id}

	appGroup := &AppGroup{
		backendRepo: &appBackendRepo{
			workspace: workspace,
			apps: repoCommon.CursorPaginationInfo[types.App]{
				Data: []types.App{emptyApp, appWithStub},
				Next: "",
			},
			deploymentsByApp: map[string][]types.DeploymentWithRelated{},
			stubsByApp: map[string][]types.StubWithRelated{
				appWithStub.ExternalId: {
					{
						Stub: types.Stub{
							Id:          1,
							ExternalId:  "stub-1",
							Name:        "Stub 1",
							Type:        types.StubType(types.StubTypeFunction),
							Config:      "{}",
							WorkspaceId: workspace.Id,
							AppId:       appWithStub.Id,
						},
						Workspace: *workspace,
						App:       &appWithStub,
					},
				},
			},
		},
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspace-1/latest", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(workspace.ExternalId)

	err := appGroup.ListAppWithLatestActivity(&auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, emptyApp.ExternalId) {
		t.Fatalf("expected response to include empty app %q: %s", emptyApp.ExternalId, body)
	}

	if !strings.Contains(body, appWithStub.ExternalId) {
		t.Fatalf("expected response to include app with stub %q: %s", appWithStub.ExternalId, body)
	}
}

func TestListAppWithLatestActivityIncludesCardEnrichment(t *testing.T) {
	workspace := &types.Workspace{Id: 1, ExternalId: "workspace-1", Name: "Workspace 1"}
	appWithDeployment := types.App{Id: 1, ExternalId: "app-deploy", Name: "App Deploy", WorkspaceId: workspace.Id}
	appWithStub := types.App{Id: 2, ExternalId: "app-stub", Name: "App Stub", WorkspaceId: workspace.Id}

	appGroup := &AppGroup{
		backendRepo: &appBackendRepo{
			workspace: workspace,
			apps: repoCommon.CursorPaginationInfo[types.App]{
				Data: []types.App{appWithDeployment, appWithStub},
				Next: "",
			},
			extraStubApps: map[string]string{"old-stub": appWithDeployment.ExternalId},
			deploymentsByApp: map[string][]types.DeploymentWithRelated{
				appWithDeployment.ExternalId: {
					{
						// Latest deployment is stopped; an older function in
						// the same app is still deployed (below).
						Deployment: types.Deployment{
							ExternalId:  "deployment-1",
							Name:        "Deployment 1",
							Active:      false,
							WorkspaceId: workspace.Id,
							AppId:       appWithDeployment.Id,
						},
						Stub: types.Stub{
							Id:          1,
							ExternalId:  "stub-deploy",
							Name:        "Stub Deploy",
							Type:        types.StubType(types.StubTypePodDeployment),
							Config:      `{"pool":{"name":"gpu-pool"},"is_service":true,"serving":{"app_kind":"database","serving_protocol":"postgres"}}`,
							WorkspaceId: workspace.Id,
							AppId:       appWithDeployment.Id,
						},
						Workspace: *workspace,
						App:       appWithDeployment,
					},
					{
						Deployment: types.Deployment{
							ExternalId:  "deployment-0",
							Name:        "Deployment 0",
							Active:      true,
							WorkspaceId: workspace.Id,
							AppId:       appWithDeployment.Id,
						},
						Stub: types.Stub{
							Id:          3,
							ExternalId:  "stub-older-fn",
							Name:        "Older Function",
							Type:        types.StubType(types.StubTypeEndpointDeployment),
							Config:      "{}",
							WorkspaceId: workspace.Id,
							AppId:       appWithDeployment.Id,
						},
						Workspace: *workspace,
						App:       appWithDeployment,
					},
				},
			},
			stubsByApp: map[string][]types.StubWithRelated{
				appWithStub.ExternalId: {
					{
						Stub: types.Stub{
							Id:          2,
							ExternalId:  "stub-latest",
							Name:        "Stub Latest",
							Type:        types.StubType(types.StubTypePodDeployment),
							Config:      `{"pool":{"name":"cpu-pool"},"is_service":true,"ports":[8080]}`,
							WorkspaceId: workspace.Id,
							AppId:       appWithStub.Id,
						},
						Workspace: *workspace,
						App:       &appWithStub,
					},
				},
			},
		},
		containerRepo: &appContainerRepo{
			states: []types.ContainerState{
				{StubId: "stub-deploy", Status: types.ContainerStatusRunning},
				{StubId: "stub-deploy", Status: types.ContainerStatusRunning},
				{StubId: "stub-deploy", Status: types.ContainerStatusPending},
				{StubId: "stub-latest", Status: types.ContainerStatusRunning},
				// An older stub of the deployment app: still counts for the app.
				{StubId: "old-stub", Status: types.ContainerStatusRunning},
				// Not in any listed app: ignored.
				{StubId: "unknown-stub", Status: types.ContainerStatusRunning},
			},
		},
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspace-1/latest", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(workspace.ExternalId)

	err := appGroup.ListAppWithLatestActivity(&auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var response struct {
		StateCounts types.AppStateCounts `json:"state_counts"`
		Data        []struct {
			ID                string `json:"id"`
			PoolName          string `json:"pool_name"`
			RunningContainers int    `json:"running_containers"`
			ActiveDeployments int    `json:"active_deployments"`
			State             string `json:"state"`
			IsService         bool   `json:"is_service"`
			Serving           struct {
				AppKind         string `json:"app_kind"`
				ServingProtocol string `json:"serving_protocol"`
			} `json:"serving"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	var rawResponse struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rawResponse); err != nil {
		t.Fatalf("failed to parse raw response: %v", err)
	}
	for _, app := range rawResponse.Data {
		for _, legacyField := range []string{"app_kind", "serving_protocol", "llm"} {
			if _, ok := app[legacyField]; ok {
				t.Fatalf("response contains legacy top-level field %q: %+v", legacyField, app)
			}
		}
	}

	byID := map[string]struct {
		PoolName          string
		RunningContainers int
		ActiveDeployments int
		IsService         bool
		Serving           struct {
			AppKind         string `json:"app_kind"`
			ServingProtocol string `json:"serving_protocol"`
		}
	}{}
	for _, app := range response.Data {
		byID[app.ID] = struct {
			PoolName          string
			RunningContainers int
			ActiveDeployments int
			IsService         bool
			Serving           struct {
				AppKind         string `json:"app_kind"`
				ServingProtocol string `json:"serving_protocol"`
			}
		}{
			PoolName:          app.PoolName,
			RunningContainers: app.RunningContainers,
			ActiveDeployments: app.ActiveDeployments,
			IsService:         app.IsService,
			Serving:           app.Serving,
		}
	}

	deploymentApp := byID[appWithDeployment.ExternalId]
	if deploymentApp.PoolName != "gpu-pool" || deploymentApp.RunningContainers != 3 || !deploymentApp.IsService {
		t.Fatalf("unexpected deployment app enrichment: %+v", deploymentApp)
	}
	if deploymentApp.ActiveDeployments != 1 {
		t.Fatalf("expected the older active deployment to count, got %+v", deploymentApp)
	}
	// Both apps have running containers, so both are running and nothing is idle
	// or stopped, workspace-wide.
	if response.StateCounts != (types.AppStateCounts{All: 2, Running: 2}) {
		t.Fatalf("unexpected state counts: %+v", response.StateCounts)
	}
	for _, app := range response.Data {
		if app.State != string(types.AppStateRunning) {
			t.Fatalf("expected %s to be running, got %q", app.ID, app.State)
		}
	}
	if deploymentApp.Serving.AppKind != "database" || deploymentApp.Serving.ServingProtocol != "postgres" {
		t.Fatalf("unexpected deployment app serving enrichment: %+v", deploymentApp)
	}

	stubApp := byID[appWithStub.ExternalId]
	if stubApp.PoolName != "cpu-pool" || stubApp.RunningContainers != 1 || stubApp.ActiveDeployments != 0 || !stubApp.IsService {
		t.Fatalf("unexpected stub app enrichment: %+v", stubApp)
	}
}

func TestDatabaseAppListOmitsConnectionURLAndRetrieveIncludesIt(t *testing.T) {
	signingKey := "test-signing-key"
	workspace := &types.Workspace{Id: 1, ExternalId: "workspace-1", Name: "Workspace 1", SigningKey: &signingKey}
	appWithDatabase := types.App{Id: 1, ExternalId: "app-db", Name: "App DB", WorkspaceId: workspace.Id}
	connectionURL := "rediss://default:password@redis-a1b2c3d-latest-6379.svc.stage.beam.cloud:443/0"

	backendRepo := &appBackendRepo{
		workspace: workspace,
		apps: repoCommon.CursorPaginationInfo[types.App]{
			Data: []types.App{appWithDatabase},
			Next: "",
		},
		deploymentsByApp: map[string][]types.DeploymentWithRelated{
			appWithDatabase.ExternalId: {
				{
					Deployment: types.Deployment{
						ExternalId:  "deployment-db",
						Name:        "luke-staging-redis",
						Subdomain:   "redis-a1b2c3d",
						Version:     1,
						WorkspaceId: workspace.Id,
						AppId:       appWithDatabase.Id,
					},
					Stub: types.Stub{
						Id:          1,
						ExternalId:  "stub-db",
						Name:        "Stub DB",
						Type:        types.StubType(types.StubTypePodDeployment),
						Config:      `{"is_service":true,"tcp":true,"ports":[6379],"serving":{"app_kind":"database","serving_protocol":"redis","database":{"kind":"redis","port":6379,"connection_env_name":"REDIS_URL","connection_url_secret_name":"BETA9_REDIS_URL"}}}`,
						WorkspaceId: workspace.Id,
						AppId:       appWithDatabase.Id,
					},
					Workspace: *workspace,
					App:       appWithDatabase,
				},
			},
		},
		secrets: map[string]string{
			"BETA9_REDIS_URL": connectionURL,
		},
	}
	appGroup := &AppGroup{
		config: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				InvokeURLType: common.InvokeUrlTypeHost,
				HTTP: types.HTTPConfig{
					TLS:          true,
					ExternalHost: "app.stage.beam.cloud",
					ExternalPort: 443,
				},
			},
			Abstractions: types.AbstractionConfig{
				Pod: types.PodConfig{
					TCP: types.PodTCPConfig{
						Enabled:      true,
						ExternalHost: "svc.stage.beam.cloud",
						ExternalPort: 443,
					},
				},
			},
		},
		backendRepo: backendRepo,
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspace-1/latest", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(workspace.ExternalId)

	err := appGroup.ListAppWithLatestActivity(&auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	listBody := rec.Body.String()

	var listResponse struct {
		Data []struct {
			ID         string `json:"id"`
			URL        string `json:"url"`
			InvokeURL  string `json:"invoke_url"`
			Deployment struct {
				URL       string `json:"url"`
				InvokeURL string `json:"invoke_url"`
			} `json:"deployment"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if len(listResponse.Data) != 1 {
		t.Fatalf("expected one app, got %d", len(listResponse.Data))
	}

	app := listResponse.Data[0]
	expectedURL := "https://redis-a1b2c3d-latest-6379.svc.stage.beam.cloud"
	if app.URL != expectedURL || app.InvokeURL != expectedURL || app.Deployment.URL != expectedURL || app.Deployment.InvokeURL != expectedURL {
		t.Fatalf("unexpected urls: %+v", app)
	}
	if backendRepo.secretLookups != 0 {
		t.Fatalf("list endpoint should not load secrets, got %d lookups", backendRepo.secretLookups)
	}
	var rawList struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rawList); err != nil {
		t.Fatalf("failed to parse raw response: %v", err)
	}
	if _, ok := rawList.Data[0]["connection_url"]; ok {
		t.Fatalf("list response should not include connection_url: %s", listBody)
	}
	if strings.Contains(listBody, "connection_url_secret_name") || strings.Contains(listBody, "password_secret_name") {
		t.Fatalf("list response should not include database secret names: %s", listBody)
	}
	if servingRaw, ok := rawList.Data[0]["serving"]; ok {
		var serving map[string]json.RawMessage
		if err := json.Unmarshal(servingRaw, &serving); err != nil {
			t.Fatalf("failed to parse serving response: %v", err)
		}
		if databaseRaw, ok := serving["database"]; ok {
			var database map[string]json.RawMessage
			if err := json.Unmarshal(databaseRaw, &database); err != nil {
				t.Fatalf("failed to parse database response: %v", err)
			}
			if _, ok := database["connection_url"]; ok {
				t.Fatalf("list serving response should not include connection_url: %s", listBody)
			}
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/workspace-1/app-db", nil)
	rec = httptest.NewRecorder()
	ctx = e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId", "appId")
	ctx.SetParamValues(workspace.ExternalId, appWithDatabase.ExternalId)

	err = appGroup.RetrieveApp(&auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	detailBody := rec.Body.String()
	if backendRepo.secretLookups != 1 {
		t.Fatalf("retrieve endpoint should load one secret batch, got %d lookups", backendRepo.secretLookups)
	}
	if strings.Contains(detailBody, "connection_url_secret_name") || strings.Contains(detailBody, "password_secret_name") {
		t.Fatalf("retrieve response should not include database secret names: %s", detailBody)
	}

	var detailResponse struct {
		URL           string `json:"url"`
		InvokeURL     string `json:"invoke_url"`
		ConnectionURL string `json:"connection_url"`
		Serving       struct {
			Database struct {
				ConnectionURL string `json:"connection_url"`
			} `json:"database"`
		} `json:"serving"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detailResponse); err != nil {
		t.Fatalf("failed to parse retrieve response: %v", err)
	}
	if detailResponse.URL != expectedURL || detailResponse.InvokeURL != expectedURL {
		t.Fatalf("unexpected retrieve urls: %+v", detailResponse)
	}
	if detailResponse.ConnectionURL != connectionURL || detailResponse.Serving.Database.ConnectionURL != connectionURL {
		t.Fatalf("unexpected retrieve connection url: %+v", detailResponse)
	}
}

func TestEnrichAppWithStubConfigHidesDefaultPool(t *testing.T) {
	app := AppWithLatestStubOrDeployment{}
	stub := &types.Stub{
		Config: `{"pool":{"name":"default"},"is_service":true}`,
	}

	appGroup := &AppGroup{}
	appGroup.enrichAppWithStubConfig(&app, stub, nil, false)

	if app.PoolName != "" {
		t.Fatalf("expected default pool to be hidden, got %q", app.PoolName)
	}
	if !app.IsService {
		t.Fatal("expected service metadata to remain populated")
	}
}

// Three apps in three states. Counts must describe the whole workspace and the
// state filter must scope the page server-side.
func TestListAppWithLatestActivityStateFilterAndCounts(t *testing.T) {
	workspace := &types.Workspace{Id: 1, ExternalId: "workspace-1", Name: "Workspace 1"}
	running := types.App{Id: 1, ExternalId: "app-running", Name: "Running", WorkspaceId: workspace.Id}
	idle := types.App{Id: 2, ExternalId: "app-idle", Name: "Idle", WorkspaceId: workspace.Id}
	stopped := types.App{Id: 3, ExternalId: "app-stopped", Name: "Stopped", WorkspaceId: workspace.Id}

	deployment := func(app types.App, stubID string, active bool) types.DeploymentWithRelated {
		return types.DeploymentWithRelated{
			Deployment: types.Deployment{ExternalId: "dep-" + app.ExternalId, Name: app.Name, Active: active, WorkspaceId: workspace.Id, AppId: app.Id},
			Stub:       types.Stub{ExternalId: stubID, Name: app.Name, Type: types.StubType(types.StubTypeEndpointDeployment), Config: "{}", WorkspaceId: workspace.Id, AppId: app.Id},
			Workspace:  *workspace,
			App:        app,
		}
	}

	repo := &appBackendRepo{
		workspace: workspace,
		apps:      repoCommon.CursorPaginationInfo[types.App]{Data: []types.App{running, idle, stopped}},
		deploymentsByApp: map[string][]types.DeploymentWithRelated{
			running.ExternalId: {deployment(running, "stub-running", true)},
			idle.ExternalId:    {deployment(idle, "stub-idle", true)},
			stopped.ExternalId: {deployment(stopped, "stub-stopped", false)},
		},
		stubsByApp: map[string][]types.StubWithRelated{},
	}
	appGroup := &AppGroup{
		backendRepo: repo,
		containerRepo: &appContainerRepo{states: []types.ContainerState{
			{StubId: "stub-running", Status: types.ContainerStatusRunning},
		}},
	}

	call := func(query string) (struct {
		StateCounts types.AppStateCounts `json:"state_counts"`
		Data        []struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"data"`
	}, types.AppFilter) {
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/workspace-1/latest"+query, nil)
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames("workspaceId")
		ctx.SetParamValues(workspace.ExternalId)
		if err := appGroup.ListAppWithLatestActivity(&auth.HttpAuthContext{
			Context:  ctx,
			AuthInfo: &auth.AuthInfo{Workspace: workspace, Token: &types.Token{TokenType: types.TokenTypeWorkspacePrimary}},
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}
		var out struct {
			StateCounts types.AppStateCounts `json:"state_counts"`
			Data        []struct {
				ID    string `json:"id"`
				State string `json:"state"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("parse: %v", err)
		}
		return out, repo.lastFilters
	}

	all, _ := call("")
	if all.StateCounts != (types.AppStateCounts{All: 3, Running: 1, Idle: 1, Stopped: 1}) {
		t.Fatalf("unexpected counts: %+v", all.StateCounts)
	}
	states := map[string]string{}
	for _, app := range all.Data {
		states[app.ID] = app.State
	}
	if states[running.ExternalId] != "running" || states[idle.ExternalId] != "idle" || states[stopped.ExternalId] != "stopped" {
		t.Fatalf("unexpected per-app states: %+v", states)
	}

	for _, tc := range []struct {
		state string
		want  string
	}{{"running", running.ExternalId}, {"idle", idle.ExternalId}, {"stopped", stopped.ExternalId}} {
		page, filters := call("?state=" + tc.state)
		if len(page.Data) != 1 || page.Data[0].ID != tc.want {
			t.Fatalf("state=%s: expected only %s, got %+v", tc.state, tc.want, page.Data)
		}
		// Counts stay workspace-wide regardless of the filter.
		if page.StateCounts.All != 3 {
			t.Fatalf("state=%s: counts should cover the workspace, got %+v", tc.state, page.StateCounts)
		}
		if tc.state == "stopped" && len(filters.ExcludeExternalIds) != 2 {
			t.Fatalf("stopped should exclude running+idle, got %+v", filters)
		}
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspace-1/latest?state=bogus", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(workspace.ExternalId)
	err := appGroup.ListAppWithLatestActivity(&auth.HttpAuthContext{
		Context:  ctx,
		AuthInfo: &auth.AuthInfo{Workspace: workspace, Token: &types.Token{TokenType: types.TokenTypeWorkspacePrimary}},
	})
	if err == nil {
		t.Fatalf("expected an error for an invalid state filter")
	}
}

func TestGetAppActivityMergesTasksAndSandboxesIntoFullSeries(t *testing.T) {
	workspace := &types.Workspace{Id: 1, ExternalId: "workspace-1", Name: "Workspace 1"}
	now := time.Now().UTC().Truncate(time.Hour)
	appGroup := &AppGroup{
		backendRepo: &appBackendRepo{
			workspace: workspace,
			taskActivity: map[string][]types.AppActivityBucket{
				"app-a": {{Time: now.Add(-2 * time.Hour), Total: 5, Failed: 1}, {Time: now, Total: 2}},
			},
		},
		containerRepo: &appContainerRepo{sandboxActivity: map[string][]types.AppActivityBucket{
			"app-a": {{Time: now, Total: 3}},
			"app-b": {{Time: now.Add(-time.Hour), Total: 7}},
		}},
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspace-1/activity?app_ids=app-a,app-b,app-a,", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(workspace.ExternalId)
	if err := appGroup.GetAppActivity(&auth.HttpAuthContext{
		Context:  ctx,
		AuthInfo: &auth.AuthInfo{Workspace: workspace, Token: &types.Token{TokenType: types.TokenTypeWorkspacePrimary}},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}

	var out map[string][]types.AppActivityBucket
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected two apps, got %d", len(out))
	}
	// 24h window + the current partial hour = 25 buckets, zero-filled.
	if len(out["app-a"]) != 25 || len(out["app-b"]) != 25 {
		t.Fatalf("expected 25 buckets per app, got %d / %d", len(out["app-a"]), len(out["app-b"]))
	}
	byTime := func(series []types.AppActivityBucket, at time.Time) types.AppActivityBucket {
		for _, b := range series {
			if b.Time.Equal(at) {
				return b
			}
		}
		t.Fatalf("missing bucket at %s", at)
		return types.AppActivityBucket{}
	}
	if b := byTime(out["app-a"], now); b.Total != 5 || b.Failed != 0 {
		t.Fatalf("expected tasks+sandboxes merged into the current hour, got %+v", b)
	}
	if b := byTime(out["app-a"], now.Add(-2*time.Hour)); b.Total != 5 || b.Failed != 1 {
		t.Fatalf("unexpected older bucket: %+v", b)
	}
	if b := byTime(out["app-b"], now.Add(-time.Hour)); b.Total != 7 {
		t.Fatalf("unexpected sandbox-only bucket: %+v", b)
	}
	if b := byTime(out["app-b"], now); b.Total != 0 {
		t.Fatalf("expected zero-filled bucket, got %+v", b)
	}
}
