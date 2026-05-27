package apiv1

import (
	"context"
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

func (r *appBackendRepo) ListStubs(_ context.Context, filters types.StubFilter) ([]types.StubWithRelated, error) {
	return r.stubsByApp[filters.AppId], nil
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
