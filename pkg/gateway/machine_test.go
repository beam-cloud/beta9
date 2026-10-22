package gateway

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type gpuInventoryWorkerRepo struct {
	repository.WorkerRepository
	err   error
	calls int
}

func (r *gpuInventoryWorkerRepo) GetGpuCounts() (map[string]int, error) {
	r.calls++
	return map[string]int{"H100": 4, "T4": 0}, r.err
}

type gpuInventoryWorkspaceRepo struct {
	repository.WorkspaceRepository
}

func (gpuInventoryWorkspaceRepo) AuthorizeToken(string) (*types.Token, *types.Workspace, error) {
	storageID := uint(1)
	return &types.Token{Active: true, TokenType: types.TokenTypeWorkspacePrimary}, &types.Workspace{
		ExternalId: "workspace-1",
		Storage:    &types.WorkspaceStorage{Id: &storageID},
	}, nil
}

func TestMachineGPUInventoryRoute(t *testing.T) {
	for _, tc := range []struct {
		name          string
		method        string
		path          string
		authenticated bool
		repoError     error
		status        int
		calls         int
	}{
		{name: "inventory", method: http.MethodGet, path: "/workspace-1/gpus", authenticated: true, status: http.StatusOK, calls: 1},
		{name: "unauthenticated", method: http.MethodGet, path: "/workspace-1/gpus", status: http.StatusUnauthorized},
		{name: "other workspace", method: http.MethodGet, path: "/workspace-2/gpus", authenticated: true, status: http.StatusUnauthorized},
		{name: "repository failure", method: http.MethodGet, path: "/workspace-1/gpus", authenticated: true, repoError: errors.New("unavailable"), status: http.StatusInternalServerError, calls: 1},
		{name: "register removed", method: http.MethodPost, path: "/register", authenticated: true, status: http.StatusNotFound},
		{name: "config removed", method: http.MethodGet, path: "/config", authenticated: true, status: http.StatusNotFound},
		{name: "list removed", method: http.MethodGet, path: "/list", authenticated: true, status: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := &gpuInventoryWorkerRepo{err: tc.repoError}
			gateway := &Gateway{
				Scheduler:     &scheduler.Scheduler{},
				WorkspaceRepo: gpuInventoryWorkspaceRepo{},
				workerRepo:    workers,
			}
			gateway.Config.GatewayService.HTTP.ExternalHost = "gateway.example.com"
			gateway.Config.GatewayService.HTTP.ExternalPort = 80
			require.NoError(t, gateway.initHttp())

			request := httptest.NewRequest(tc.method, "http://gateway.example.com/api/v1/machine"+tc.path, nil)
			if tc.authenticated {
				request.Header.Set("Authorization", "Bearer workspace-token")
			}
			recorder := httptest.NewRecorder()
			gateway.echo.ServeHTTP(recorder, request)

			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Equal(t, tc.calls, workers.calls)
			if tc.status == http.StatusOK {
				require.JSONEq(t, `{"H100":4,"T4":0}`, recorder.Body.String())
			}
		})
	}
}
