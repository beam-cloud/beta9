package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestMetricEventTypesIncludeContainerEventsForRealtimeCounts(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?event_types=container.metrics,container.event,task.created,task.updated,container.log", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	got := metricEventTypesFromContext(ctx)
	want := []string{
		types.EventContainerMetrics,
		types.EventContainerEvent,
		types.EventTaskCreated,
		types.EventTaskUpdated,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected metric event types: got %#v want %#v", got, want)
	}
}

type poolMetricsBackend struct {
	repository.BackendRepository
	admin *types.Workspace
}

func (r *poolMetricsBackend) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	return r.admin, nil
}

type poolMetricsEvents struct {
	repository.EventRepository
	workspaces []string
}

func (r *poolMetricsEvents) GetPoolMetricsTimeseries(_ context.Context, query types.EventQuery, _, _ time.Time, _ string) (*types.PoolMetricsTimeseriesResponse, error) {
	r.workspaces = append(r.workspaces, query.WorkspaceID)
	return &types.PoolMetricsTimeseriesResponse{}, nil
}

func TestPoolMetricsScopeIsDerivedFromAuthenticatedRole(t *testing.T) {
	tests := []struct {
		name string
		auth *auth.AuthInfo
		want []string
	}{
		{
			name: "workspace user ignores forged route workspace",
			auth: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "user-workspace"}, Token: &types.Token{TokenType: types.TokenTypeWorkspacePrimary, Active: true}},
			want: []string{"user-workspace"},
		},
		{
			name: "platform operator includes platform inventory",
			auth: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "operator-workspace"}, Token: &types.Token{TokenType: types.TokenTypeClusterAdmin, Active: true}},
			want: []string{"route-workspace", "platform-workspace"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			request := httptest.NewRequest(http.MethodGet, "/?start=2026-07-13T10:00:00Z&end=2026-07-13T10:30:00Z&interval=15s", nil)
			ctx := e.NewContext(request, httptest.NewRecorder())
			ctx.SetParamNames("workspaceId")
			ctx.SetParamValues("route-workspace")
			events := &poolMetricsEvents{}
			group := &MetricsGroup{
				backendRepo: &poolMetricsBackend{admin: &types.Workspace{ExternalId: "platform-workspace"}},
				eventRepo:   events,
			}

			if err := group.GetPoolMetricTimeseries(&auth.HttpAuthContext{Context: ctx, AuthInfo: tt.auth}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(events.workspaces, tt.want) {
				t.Fatalf("workspaces = %#v, want %#v", events.workspaces, tt.want)
			}
		})
	}
}
