package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type securityBackendRepo struct {
	repository.BackendRepository
	stub           *types.StubWithRelated
	requestedTasks []string
	tasks          map[string]*types.TaskWithRelated
}

func (r *securityBackendRepo) GetStubByExternalId(context.Context, string, ...types.QueryFilter) (*types.StubWithRelated, error) {
	return r.stub, nil
}

func (r *securityBackendRepo) GetTaskWithRelated(_ context.Context, id string) (*types.TaskWithRelated, error) {
	r.requestedTasks = append(r.requestedTasks, id)
	return r.tasks[id], nil
}

func TestStopTasksChecksTaskOwnership(t *testing.T) {
	repo := &securityBackendRepo{tasks: map[string]*types.TaskWithRelated{
		"own-task":     {Task: types.Task{WorkspaceId: 1, Status: types.TaskStatusComplete}},
		"foreign-task": {Task: types.Task{WorkspaceId: 2, Status: types.TaskStatusRunning}},
	}}
	e := echo.New()
	rec := httptest.NewRecorder()
	ctx := e.NewContext(httptest.NewRequest(http.MethodDelete, "/task/own?task_ids=foreign-task,own-task", nil), rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues("own")
	cc := &auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{Id: 1, ExternalId: "own"}}}
	// No dispatcher or container scheduler: a foreign task must never reach them.
	require.NoError(t, (&TaskGroup{backendRepo: repo}).StopTasks(cc))
	require.Equal(t, []string{"foreign-task", "own-task"}, repo.requestedTasks)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestStopTasksScopesAdminToRequestedWorkspace(t *testing.T) {
	for _, tokenType := range []string{types.TokenTypeWorkspace, types.TokenTypeClusterAdmin} {
		t.Run(tokenType, func(t *testing.T) {
			repo := &securityBackendRepo{tasks: map[string]*types.TaskWithRelated{
				"foreign-task": {Task: types.Task{WorkspaceId: 2, Status: types.TaskStatusComplete}, Workspace: types.Workspace{ExternalId: "other"}},
				"outside-task": {Task: types.Task{WorkspaceId: 3, Status: types.TaskStatusRunning}, Workspace: types.Workspace{ExternalId: "outside"}},
			}}
			ctx := echo.New().NewContext(httptest.NewRequest(http.MethodDelete, "/task/other?task_ids=foreign-task,outside-task", nil), httptest.NewRecorder())
			ctx.SetParamNames("workspaceId")
			ctx.SetParamValues("other")
			cc := &auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{Id: 1, ExternalId: "own"}, Token: &types.Token{TokenType: tokenType},
			}}
			err := auth.WithWorkspaceAuth((&TaskGroup{backendRepo: repo}).StopTasks)(cc)
			if tokenType == types.TokenTypeClusterAdmin {
				require.NoError(t, err)
				require.Equal(t, []string{"foreign-task", "outside-task"}, repo.requestedTasks)
			} else {
				require.Equal(t, http.StatusUnauthorized, err.(*echo.HTTPError).Code)
				require.Empty(t, repo.requestedTasks)
			}
		})
	}
}

func TestStubConfigRequiresOwnerAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name      string
		workspace uint
		status    int
	}{
		{name: "anonymous", status: http.StatusUnauthorized},
		{name: "other workspace", workspace: 2, status: http.StatusNotFound},
		{name: "owner", workspace: 1, status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := echo.New()
			repo := &securityBackendRepo{stub: &types.StubWithRelated{Stub: types.Stub{WorkspaceId: 1, Config: `{}`}}}
			group := NewStubGroup(e.Group("/stub"), repo, nil, nil, nil, types.AppConfig{})
			if tc.workspace != 0 {
				e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
					return func(c echo.Context) error {
						return next(&auth.HttpAuthContext{Context: c, AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{Id: tc.workspace}}})
					}
				})
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stub/target/config", nil))
			require.Equal(t, tc.status, rec.Code)
			if tc.workspace == 0 {
				// The handler also fails closed when called without its route wrapper.
				err := group.GetConfig(e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder()))
				require.Equal(t, http.StatusUnauthorized, err.(*echo.HTTPError).Code)
			}
		})
	}
}

func TestWebhookURLRejectsInternalDestinations(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1/hook", "http://localhost/hook", "http://169.254.169.254/latest", "http://10.0.0.1/hook", "http://[::1]/hook", "http://100.64.0.1/hook", "file:///tmp/hook", "http://:80/"} {
		require.Error(t, ValidateWebhookURL(raw), raw)
	}
	require.NoError(t, ValidateWebhookURL("https://8.8.8.8/hook"))
}
