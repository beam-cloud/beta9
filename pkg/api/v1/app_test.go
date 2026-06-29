package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
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
}

type appContainerRepo struct {
	repository.ContainerRepository
	states []types.ContainerState
}

func (r *appContainerRepo) GetActiveContainersByWorkspaceId(_ string) ([]types.ContainerState, error) {
	return r.states, nil
}

func (r *appContainerRepo) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	states := make([]types.ContainerState, 0)
	for _, state := range r.states {
		if state.StubId == stubId {
			states = append(states, state)
		}
	}
	return states, nil
}

func (r *appBackendRepo) GetWorkspaceByExternalId(_ context.Context, externalId string) (types.Workspace, error) {
	if r.workspace == nil || r.workspace.ExternalId != externalId {
		return types.Workspace{}, errors.New("workspace not found")
	}

	return *r.workspace, nil
}

func (r *appBackendRepo) ListAppsPaginated(_ context.Context, _ uint, _ types.AppFilter) (repoCommon.CursorPaginationInfo[types.App], error) {
	return r.apps, nil
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
			deploymentsByApp: map[string][]types.DeploymentWithRelated{
				appWithDeployment.ExternalId: {
					{
						Deployment: types.Deployment{
							ExternalId:  "deployment-1",
							Name:        "Deployment 1",
							WorkspaceId: workspace.Id,
							AppId:       appWithDeployment.Id,
						},
						Stub: types.Stub{
							Id:          1,
							ExternalId:  "stub-deploy",
							Name:        "Stub Deploy",
							Type:        types.StubType(types.StubTypePodDeployment),
							Config:      `{"pool":{"name":"gpu-pool"},"is_service":true,"serving":{"app_kind":"llm_model","serving_protocol":"openai","llm":{"model_id":"Qwen/Qwen2.5-0.5B-Instruct","engine":"vllm","served_model_name":"qwen-test","context_length":4096,"metrics_path":"/metrics","slo_tier":"standard"}}}`,
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
				{StubId: "old-stub", Status: types.ContainerStatusRunning},
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
		Data []struct {
			ID                string `json:"id"`
			PoolName          string `json:"pool_name"`
			RunningContainers int    `json:"running_containers"`
			IsService         bool   `json:"is_service"`
			Serving           struct {
				AppKind         string `json:"app_kind"`
				ServingProtocol string `json:"serving_protocol"`
				LLM             struct {
					ModelID         string `json:"model_id"`
					Engine          string `json:"engine"`
					ServedModelName string `json:"served_model_name"`
					ContextLength   int    `json:"context_length"`
					MetricsPath     string `json:"metrics_path"`
					SLOTier         string `json:"slo_tier"`
				} `json:"llm"`
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
		IsService         bool
		Serving           struct {
			AppKind         string `json:"app_kind"`
			ServingProtocol string `json:"serving_protocol"`
			LLM             struct {
				ModelID         string `json:"model_id"`
				Engine          string `json:"engine"`
				ServedModelName string `json:"served_model_name"`
				ContextLength   int    `json:"context_length"`
				MetricsPath     string `json:"metrics_path"`
				SLOTier         string `json:"slo_tier"`
			} `json:"llm"`
		}
	}{}
	for _, app := range response.Data {
		byID[app.ID] = struct {
			PoolName          string
			RunningContainers int
			IsService         bool
			Serving           struct {
				AppKind         string `json:"app_kind"`
				ServingProtocol string `json:"serving_protocol"`
				LLM             struct {
					ModelID         string `json:"model_id"`
					Engine          string `json:"engine"`
					ServedModelName string `json:"served_model_name"`
					ContextLength   int    `json:"context_length"`
					MetricsPath     string `json:"metrics_path"`
					SLOTier         string `json:"slo_tier"`
				} `json:"llm"`
			}
		}{
			PoolName:          app.PoolName,
			RunningContainers: app.RunningContainers,
			IsService:         app.IsService,
			Serving:           app.Serving,
		}
	}

	deploymentApp := byID[appWithDeployment.ExternalId]
	if deploymentApp.PoolName != "gpu-pool" || deploymentApp.RunningContainers != 2 || !deploymentApp.IsService {
		t.Fatalf("unexpected deployment app enrichment: %+v", deploymentApp)
	}
	if deploymentApp.Serving.AppKind != "llm_model" || deploymentApp.Serving.ServingProtocol != "openai" || deploymentApp.Serving.LLM.ModelID != "Qwen/Qwen2.5-0.5B-Instruct" || deploymentApp.Serving.LLM.ContextLength != 4096 {
		t.Fatalf("unexpected deployment app llm enrichment: %+v", deploymentApp)
	}

	stubApp := byID[appWithStub.ExternalId]
	if stubApp.PoolName != "cpu-pool" || stubApp.RunningContainers != 1 || !stubApp.IsService {
		t.Fatalf("unexpected stub app enrichment: %+v", stubApp)
	}
}

func TestEnrichAppWithStubConfigHidesDefaultPool(t *testing.T) {
	app := AppWithLatestStubOrDeployment{}
	stub := &types.Stub{
		Config: `{"pool":{"name":"default"},"is_service":true}`,
	}

	enrichAppWithStubConfig(&app, stub)

	if app.PoolName != "" {
		t.Fatalf("expected default pool to be hidden, got %q", app.PoolName)
	}
	if !app.IsService {
		t.Fatal("expected service metadata to remain populated")
	}
}
