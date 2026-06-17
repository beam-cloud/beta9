package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bytes"
	"io"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"k8s.io/utils/ptr"
)

type sandboxRowsEventRepo struct {
	repository.EventRepository
	history          *types.EventHistoryResponse
	historiesByStub  map[string]*types.EventHistoryResponse
	containers       map[string]*types.ContainerEventsResponse
	queries          []types.EventQuery
	containerQueries []types.EventQuery
}

func (r *sandboxRowsEventRepo) GetEventHistory(_ context.Context, query types.EventQuery) (*types.EventHistoryResponse, error) {
	r.queries = append(r.queries, query)
	if r.historiesByStub != nil && query.StubID != "" {
		return r.historiesByStub[query.StubID], nil
	}
	return r.history, nil
}

func (r *sandboxRowsEventRepo) GetContainerEvents(_ context.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	query.ContainerID = containerID
	r.containerQueries = append(r.containerQueries, query)
	return r.containers[containerID], nil
}

func NewStubGroupForTest() *StubGroup {
	backendRepo, _ := repository.NewBackendPostgresRepositoryForTest()

	config := types.AppConfig{
		GatewayService: types.GatewayServiceConfig{
			StubLimits: types.StubLimits{
				Cpu:         128000,
				Memory:      40000,
				MaxGpuCount: 2,
			},
		},
	}

	e := echo.New()

	return NewStubGroup(
		e.Group("/stubs"),
		backendRepo,
		nil,
		repository.NewEventClientRepo(config),
		config,
	)
}

// NewStubGroupWithMockForTest creates a StubGroup with a mock repository for testing
func NewStubGroupWithMockForTest() (*StubGroup, sqlmock.Sqlmock, *echo.Echo) {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()

	config := types.AppConfig{
		GatewayService: types.GatewayServiceConfig{
			StubLimits: types.StubLimits{
				Cpu:         128000,
				Memory:      40000,
				MaxGpuCount: 2,
			},
		},
	}

	e := echo.New()

	stubGroup := NewStubGroup(
		e.Group("/stubs"),
		backendRepo,
		nil,
		repository.NewEventClientRepo(config),
		config,
	)

	return stubGroup, mock, e
}

func generateDefaultStubWithConfig() *types.Stub {
	return &types.Stub{
		Name:   "Test Stub",
		Config: `{"runtime":{"cpu":1000,"gpu":"","gpu_count":0,"memory":1000,"image_id":"","gpus":[]},"handler":"","on_start":"","on_deploy":"","on_deploy_stub_id":"","python_version":"python3","keep_warm_seconds":600,"max_pending_tasks":100,"callback_url":"","task_policy":{"max_retries":3,"timeout":3600,"expires":"0001-01-01T00:00:00Z","ttl":0},"workers":1,"concurrent_requests":1,"authorized":false,"volumes":null,"autoscaler":{"type":"queue_depth","max_containers":1,"tasks_per_container":1,"min_containers":0},"extra":{},"checkpoint_enabled":false,"work_dir":"","entry_point":["sleep 100"],"ports":[]}`,
	}
}

func TestBuildSandboxRowsReturnsOneRowPerRecentContainer(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stub := &types.StubWithRelated{Stub: types.Stub{
		ExternalId: "sandbox-stub",
		Name:       "sandbox",
		CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
	}}

	group := &StubGroup{eventRepo: &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: []types.ContainerEventRecord{
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(1 * time.Second)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(1 * time.Second), EndTime: base.Add(1044 * time.Millisecond)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(1250 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(2 * time.Second)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(2 * time.Second), EndTime: base.Add(2044 * time.Millisecond)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(2250 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(3 * time.Second)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(3 * time.Second), EndTime: base.Add(3044 * time.Millisecond)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(3250 * time.Millisecond)},
		}},
	}}

	rows := group.buildSandboxRows(context.Background(), "workspace", stub, nil, 0)
	if got, want := len(rows), 3; got != want {
		t.Fatalf("expected %d sandbox rows, got %d", want, got)
	}

	expectedContainerIDs := []string{"sandbox-stub-33333333", "sandbox-stub-22222222", "sandbox-stub-11111111"}
	for i, expectedContainerID := range expectedContainerIDs {
		row := rows[i]
		if row.Id != expectedContainerID {
			t.Fatalf("row %d id = %q, want container id %q", i, row.Id, expectedContainerID)
		}
		if row.StubId != "sandbox-stub" {
			t.Fatalf("row %d stub_id = %q, want stub id", i, row.StubId)
		}
		if row.ContainerId != expectedContainerID {
			t.Fatalf("row %d container_id = %q, want %q", i, row.ContainerId, expectedContainerID)
		}
		if row.Status != SandboxStatusStopped {
			t.Fatalf("row %d status = %q, want %q", i, row.Status, SandboxStatusStopped)
		}
		if row.TimeToStartedMs == nil || *row.TimeToStartedMs != 44 {
			t.Fatalf("row %d time_to_started_ms = %v, want 44", i, row.TimeToStartedMs)
		}
		if row.TimeToInteractiveMs == nil || *row.TimeToInteractiveMs != 44 {
			t.Fatalf("row %d time_to_interactive_ms = %v, want 44", i, row.TimeToInteractiveMs)
		}
	}
}

func TestBuildSandboxRowsAppliesMaxRows(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stub := &types.StubWithRelated{Stub: types.Stub{
		ExternalId: "sandbox-stub",
		Name:       "sandbox",
		CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
	}}

	group := &StubGroup{eventRepo: &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: []types.ContainerEventRecord{
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(1 * time.Second)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(2 * time.Second)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(3 * time.Second)},
		}},
	}}

	rows := group.buildSandboxRows(context.Background(), "workspace", stub, nil, 2)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("expected %d sandbox rows, got %d", want, got)
	}
	if rows[0].ContainerId != "sandbox-stub-33333333" || rows[1].ContainerId != "sandbox-stub-22222222" {
		t.Fatalf("unexpected limited rows: %#v", rows)
	}
}

func TestBuildSandboxRowsReturnsEveryActiveContainer(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stub := &types.StubWithRelated{Stub: types.Stub{
		ExternalId: "sandbox-stub",
		Name:       "sandbox",
		CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
	}}

	group := &StubGroup{}
	rows := group.buildSandboxRows(context.Background(), "workspace", stub, []types.ContainerState{
		{
			ContainerId: "sandbox-stub-11111111",
			StubId:      "sandbox-stub",
			Status:      types.ContainerStatusRunning,
			ScheduledAt: base.Add(1 * time.Second).Unix(),
			StartedAt:   base.Add(2 * time.Second).Unix(),
		},
		{
			ContainerId: "sandbox-stub-22222222",
			StubId:      "sandbox-stub",
			Status:      types.ContainerStatusPending,
			ScheduledAt: base.Add(3 * time.Second).Unix(),
		},
	}, 0)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("expected %d sandbox rows, got %d", want, got)
	}
	if rows[0].ContainerId != "sandbox-stub-22222222" || rows[1].ContainerId != "sandbox-stub-11111111" {
		t.Fatalf("unexpected container ordering: %#v", rows)
	}
	if rows[0].Id != rows[0].ContainerId || rows[1].Id != rows[1].ContainerId {
		t.Fatalf("expected active sandbox ids to match container ids: %#v", rows)
	}
	if rows[0].StubId != "sandbox-stub" || rows[1].StubId != "sandbox-stub" {
		t.Fatalf("expected active sandbox stub ids to be preserved: %#v", rows)
	}
	if rows[0].Status != SandboxStatusPending {
		t.Fatalf("pending row status = %q, want %q", rows[0].Status, SandboxStatusPending)
	}
	if rows[1].Status != SandboxStatusRunning {
		t.Fatalf("running row status = %q, want %q", rows[1].Status, SandboxStatusRunning)
	}
}

func TestBuildSandboxRowsEnrichesActiveTimingFromHistory(t *testing.T) {
	base := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Millisecond)
	stub := &types.StubWithRelated{Stub: types.Stub{
		ExternalId: "sandbox-stub",
		Name:       "sandbox",
		CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
	}}

	eventRepo := &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: []types.ContainerEventRecord{
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(100 * time.Millisecond), EndTime: base.Add(1500 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(1500 * time.Millisecond), EndTime: base.Add(2500 * time.Millisecond)},
		}},
	}
	group := &StubGroup{eventRepo: eventRepo}

	rows := group.buildSandboxRows(context.Background(), "workspace", stub, []types.ContainerState{
		{
			ContainerId: "sandbox-stub-11111111",
			StubId:      "sandbox-stub",
			Status:      types.ContainerStatusRunning,
		},
	}, 1)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("expected %d sandbox rows, got %d", want, got)
	}
	if got := len(eventRepo.queries); got != 1 {
		t.Fatalf("expected active row timing to query history despite full page, got %d queries", got)
	}

	row := rows[0]
	if !row.CreatedAt.Equal(base) {
		t.Fatalf("created_at = %s, want lifecycle created_at %s", row.CreatedAt, base)
	}
	if row.TimeToStartedMs == nil || *row.TimeToStartedMs != 1500 {
		t.Fatalf("time_to_started_ms = %v, want 1500", row.TimeToStartedMs)
	}
	if row.TimeToInteractiveMs == nil || *row.TimeToInteractiveMs != 2500 {
		t.Fatalf("time_to_interactive_ms = %v, want 2500", row.TimeToInteractiveMs)
	}
	wantStartedAtMs := base.Add(1500 * time.Millisecond).UnixMilli()
	if row.StartedAtMs == nil || *row.StartedAtMs != wantStartedAtMs {
		t.Fatalf("started_at_ms = %v, want %d", row.StartedAtMs, wantStartedAtMs)
	}
	wantInteractiveAtMs := base.Add(2500 * time.Millisecond).UnixMilli()
	if row.InteractiveAtMs == nil || *row.InteractiveAtMs != wantInteractiveAtMs {
		t.Fatalf("interactive_at_ms = %v, want %d", row.InteractiveAtMs, wantInteractiveAtMs)
	}
	if row.LifetimeMs == nil || *row.LifetimeMs <= 0 {
		t.Fatalf("lifetime_ms = %v, want a positive running lifetime", row.LifetimeMs)
	}
}

func TestBuildSandboxStatsRowsUsesAppHistoryForAppNamespaceStats(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stubs := []types.StubWithRelated{{
		Stub: types.Stub{
			ExternalId: "sandbox-stub",
			Name:       "sandbox",
			CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
		},
	}}

	eventRepo := &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: []types.ContainerEventRecord{
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(1 * time.Second)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(1250 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(2 * time.Second)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(2250 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(3 * time.Second)},
			{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-33333333", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(3250 * time.Millisecond)},
		}},
	}
	group := &StubGroup{eventRepo: eventRepo}

	rows := group.buildSandboxStatsRows(context.Background(), "workspace", "app-1", stubs, nil)
	if got, want := len(rows), 3; got != want {
		t.Fatalf("expected %d sandbox stats rows, got %d", want, got)
	}
	if got := len(eventRepo.queries); got != 3 {
		t.Fatalf("expected app, legacy, and bounded stub fallback history queries, got %d", got)
	}
	query := eventRepo.queries[0]
	if query.AppID != "app-1" {
		t.Fatalf("history query app_id = %q, want app-1", query.AppID)
	}
	if query.StubID != "" {
		t.Fatalf("history query stub_id = %q, want empty", query.StubID)
	}
	query = eventRepo.queries[1]
	if query.AppID != "" || query.StubID != "" {
		t.Fatalf("legacy query app_id/stub_id = %q/%q, want empty", query.AppID, query.StubID)
	}
	query = eventRepo.queries[2]
	if query.AppID != "" || query.StubID != "sandbox-stub" {
		t.Fatalf("stub fallback query app_id/stub_id = %q/%q, want empty/sandbox-stub", query.AppID, query.StubID)
	}
}

func TestBuildSandboxStatsRowsMergesStubFallbackForPartialAppHistory(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stubs := []types.StubWithRelated{{
		Stub: types.Stub{
			ExternalId: "sandbox-stub",
			Name:       "sandbox",
			CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
		},
	}}

	appHistory := []types.ContainerEventRecord{
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(100 * time.Millisecond), EndTime: base.Add(900 * time.Millisecond)},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(900 * time.Millisecond), EndTime: base.Add(1200 * time.Millisecond)},
		{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(2 * time.Second)},
	}
	stubHistory := append([]types.ContainerEventRecord{}, appHistory...)
	stubHistory = append(stubHistory,
		types.ContainerEventRecord{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(10 * time.Second)},
		types.ContainerEventRecord{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(10*time.Second + 100*time.Millisecond), EndTime: base.Add(11 * time.Second)},
		types.ContainerEventRecord{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", StartTime: base.Add(11 * time.Second), EndTime: base.Add(12 * time.Second)},
		types.ContainerEventRecord{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-22222222", WorkspaceID: "workspace", StubID: "sandbox-stub", Timestamp: base.Add(13 * time.Second)},
	)

	eventRepo := &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: appHistory},
		historiesByStub: map[string]*types.EventHistoryResponse{
			"sandbox-stub": {Events: stubHistory},
		},
	}
	group := &StubGroup{eventRepo: eventRepo}

	rows := group.buildSandboxStatsRows(context.Background(), "workspace", "app-1", stubs, nil)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("expected %d sandbox stats rows, got %d", want, got)
	}

	rowsByContainer := map[string]SandboxRow{}
	for _, row := range rows {
		rowsByContainer[row.ContainerId] = row
	}
	if _, ok := rowsByContainer["sandbox-stub-11111111"]; !ok {
		t.Fatalf("missing app-history row: %#v", rows)
	}
	fallback := rowsByContainer["sandbox-stub-22222222"]
	if fallback.TimeToInteractiveMs == nil || *fallback.TimeToInteractiveMs != 2000 {
		t.Fatalf("fallback time_to_interactive_ms = %v, want 2000", fallback.TimeToInteractiveMs)
	}
	if fallback.LifetimeMs == nil || *fallback.LifetimeMs != 2000 {
		t.Fatalf("fallback lifetime_ms = %v, want 2000", fallback.LifetimeMs)
	}
}

func TestBuildSandboxStatsRowsEnrichesActiveTimingFromHistory(t *testing.T) {
	base := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Millisecond)
	stubs := []types.StubWithRelated{{
		Stub: types.Stub{
			ExternalId: "sandbox-stub",
			Name:       "sandbox",
			CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
		},
	}}
	eventRepo := &sandboxRowsEventRepo{
		history: &types.EventHistoryResponse{Events: []types.ContainerEventRecord{
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(100 * time.Millisecond), EndTime: base.Add(1500 * time.Millisecond)},
			{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(1500 * time.Millisecond), EndTime: base.Add(2500 * time.Millisecond)},
		}},
	}
	group := &StubGroup{eventRepo: eventRepo}

	rows := group.buildSandboxStatsRows(context.Background(), "workspace", "app-1", stubs, map[string][]types.ContainerState{
		"sandbox-stub": {
			{
				ContainerId: "sandbox-stub-11111111",
				StubId:      "sandbox-stub",
				Status:      types.ContainerStatusRunning,
			},
		},
	})
	if got, want := len(rows), 1; got != want {
		t.Fatalf("expected %d sandbox stats rows, got %d", want, got)
	}

	row := rows[0]
	if row.TimeToStartedMs == nil || *row.TimeToStartedMs != 1500 {
		t.Fatalf("time_to_started_ms = %v, want 1500", row.TimeToStartedMs)
	}
	if row.TimeToInteractiveMs == nil || *row.TimeToInteractiveMs != 2500 {
		t.Fatalf("time_to_interactive_ms = %v, want 2500", row.TimeToInteractiveMs)
	}
	wantStartedAtMs := base.Add(1500 * time.Millisecond).UnixMilli()
	if row.StartedAtMs == nil || *row.StartedAtMs != wantStartedAtMs {
		t.Fatalf("started_at_ms = %v, want %d", row.StartedAtMs, wantStartedAtMs)
	}
	wantInteractiveAtMs := base.Add(2500 * time.Millisecond).UnixMilli()
	if row.InteractiveAtMs == nil || *row.InteractiveAtMs != wantInteractiveAtMs {
		t.Fatalf("interactive_at_ms = %v, want %d", row.InteractiveAtMs, wantInteractiveAtMs)
	}
	if row.LifetimeMs == nil || *row.LifetimeMs <= 0 {
		t.Fatalf("lifetime_ms = %v, want a positive running lifetime", row.LifetimeMs)
	}
}

func TestBuildSandboxRowsHydratesMissingTimingFromContainerStream(t *testing.T) {
	base := time.Date(2026, 6, 16, 20, 1, 0, 0, time.UTC)
	stub := types.StubWithRelated{
		Stub: types.Stub{
			ExternalId: "sandbox-stub",
			Name:       "sandbox",
			CreatedAt:  types.Time{Time: base.Add(-time.Hour)},
		},
	}

	containerID := "sandbox-stub-22222222"
	historyEvents := []types.ContainerEventRecord{
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(100 * time.Millisecond), EndTime: base.Add(1500 * time.Millisecond)},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(1500 * time.Millisecond), EndTime: base.Add(2500 * time.Millisecond)},
		{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: "sandbox-stub-11111111", WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(3 * time.Second)},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(10 * time.Second)},
		{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(13 * time.Second)},
	}
	canonicalEvents := []types.ContainerEventRecord{
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSchedulerQueuePush), ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(10 * time.Second)},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleStartup), ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(10*time.Second + 100*time.Millisecond), EndTime: base.Add(11 * time.Second)},
		{Type: types.EventContainerLifecycle, EventID: string(types.ContainerLifecycleSandboxProcessManagerReady), ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", StartTime: base.Add(11 * time.Second), EndTime: base.Add(12 * time.Second)},
		{Type: types.EventContainerEvent, EventID: "runtime.exited", ContainerID: containerID, WorkspaceID: "workspace", AppID: "app-1", StubID: "sandbox-stub", Timestamp: base.Add(13 * time.Second)},
	}

	eventRepo := &sandboxRowsEventRepo{
		containers: map[string]*types.ContainerEventsResponse{
			containerID: {ContainerID: containerID, Events: canonicalEvents},
		},
	}
	group := &StubGroup{eventRepo: eventRepo}

	rows := group.buildSandboxRowsWithPreloadedSummaries(
		context.Background(),
		"workspace",
		&stub,
		nil,
		50,
		sandboxContainerSummariesFromHistory(historyEvents, 0),
	)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("expected %d sandbox rows, got %d", want, got)
	}

	var hydrated *SandboxRow
	for i := range rows {
		if rows[i].ContainerId == containerID {
			hydrated = &rows[i]
			break
		}
	}
	if hydrated == nil {
		t.Fatalf("expected row for %s, got %#v", containerID, rows)
	}
	if hydrated.TimeToStartedMs == nil || *hydrated.TimeToStartedMs != 1000 {
		t.Fatalf("time_to_started_ms = %v, want 1000", hydrated.TimeToStartedMs)
	}
	if hydrated.TimeToInteractiveMs == nil || *hydrated.TimeToInteractiveMs != 2000 {
		t.Fatalf("time_to_interactive_ms = %v, want 2000", hydrated.TimeToInteractiveMs)
	}
	if hydrated.LifetimeMs == nil || *hydrated.LifetimeMs != 2000 {
		t.Fatalf("lifetime_ms = %v, want 2000", hydrated.LifetimeMs)
	}
	if got := len(eventRepo.containerQueries); got != 1 {
		t.Fatalf("expected one canonical container lookup, got %d", got)
	}
	query := eventRepo.containerQueries[0]
	if query.ContainerID != containerID || query.StubID != "sandbox-stub" || query.WorkspaceID != "workspace" {
		t.Fatalf("unexpected container lookup query: %#v", query)
	}
	if query.Limit != sandboxContainerHistoryLimit {
		t.Fatalf("container lookup limit = %d, want %d", query.Limit, sandboxContainerHistoryLimit)
	}
}

// generateMockStubRows creates mock database rows for stub queries
func generateMockStubRows(id uint, externalID, name, config string, workspaceID uint) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "external_id", "name", "type", "config", "config_version", "object_id", "workspace_id", "created_at", "updated_at", "public", "app_id", "workspace.id", "workspace.external_id", "workspace.name", "workspace.created_at", "workspace.updated_at", "workspace.signing_key", "workspace.volume_cache_enabled", "workspace.multi_gpu_enabled", "object.id", "object.external_id", "object.hash", "object.size", "object.workspace_id", "object.created_at", "app.id", "app.external_id", "app.name", "workspace.storage.id", "workspace.storage.external_id", "workspace.storage.bucket_name", "workspace.storage.access_key", "workspace.storage.secret_key", "workspace.storage.endpoint_url", "workspace.storage.region", "workspace.storage.created_at", "workspace.storage.updated_at"}).
		AddRow(id, externalID, name, "deployment", config, 1, 1, workspaceID, now, now, false, 1, workspaceID, fmt.Sprintf("workspace-%d", workspaceID), "Test Workspace", now, now, nil, false, false, 1, fmt.Sprintf("obj-%d", id), fmt.Sprintf("hash%d", id), 1000, workspaceID, now, 1, fmt.Sprintf("app-%d", id), "Test App", nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestProcessStubOverrides(t *testing.T) {
	stubGroup := NewStubGroupForTest()

	tests := []struct {
		name     string
		cpu      *int64
		memory   *int64
		gpu      *string
		gpuCount *uint32
		error    bool
	}{
		{
			name: "Test with CPU override",
			cpu:  ptr.To(int64(2000)),
		},
		{
			name:   "Test with Memory override",
			memory: ptr.To(int64(4096)),
		},
		{
			name: "Test with GPU override",
			gpu:  ptr.To(string(types.GPU_A10G)),
		},
		{
			name:     "Test with GPU Count override",
			gpuCount: ptr.To(uint32(2)),
		},
		{
			name:     "Test with all overrides",
			cpu:      ptr.To(int64(2000)),
			memory:   ptr.To(int64(4096)),
			gpu:      ptr.To(string(types.GPU_A10G)),
			gpuCount: ptr.To(uint32(2)),
		},
		{
			name: "Test with no overrides",
		},
		{
			name:  "Test with invalid GPU",
			gpu:   ptr.To("invalid-gpu"),
			error: true,
		},
		{
			name:     "Test with invalid GPU Count",
			gpuCount: ptr.To(uint32(3)),
			error:    true,
		},
		{
			name:   "Test with invalid Memory",
			memory: ptr.To(int64(50000)),
			error:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &types.StubWithRelated{Stub: *generateDefaultStubWithConfig()}

			err := stubGroup.processStubOverrides(OverrideStubConfig{
				Cpu:      tt.cpu,
				Memory:   tt.memory,
				Gpu:      tt.gpu,
				GpuCount: tt.gpuCount,
			}, stub)
			if err != nil {
				if !tt.error {
					t.Errorf("Unexpected error: %v", err)
				}
				return
			} else {
				if tt.error {
					t.Errorf("Expected error but got none")
					return
				}
			}

			var stubConfig types.StubConfigV1
			err = json.Unmarshal([]byte(stub.Config), &stubConfig)
			if err != nil {
				t.Errorf("Failed to unmarshal stub config: %v", err)
			}
			if tt.cpu != nil && *tt.cpu != stubConfig.Runtime.Cpu {
				t.Errorf("Expected CPU %d, got %d", *tt.cpu, stubConfig.Runtime.Cpu)
			}
			if tt.memory != nil && *tt.memory != stubConfig.Runtime.Memory {
				t.Errorf("Expected Memory %d, got %d", *tt.memory, stubConfig.Runtime.Memory)
			}
			if tt.gpu != nil && (len(stubConfig.Runtime.Gpus) == 0 || *tt.gpu != string(stubConfig.Runtime.Gpus[0])) {
				t.Errorf("Expected GPU %s, got %s", *tt.gpu, stubConfig.Runtime.Gpu)
			}
			if tt.gpuCount != nil && *tt.gpuCount != stubConfig.Runtime.GpuCount {
				t.Errorf("Expected GPU Count %d, got %d", *tt.gpuCount, stubConfig.Runtime.GpuCount)
			}
		})
	}

}

func TestUpdateConfig(t *testing.T) {
	tests := []struct {
		name           string
		stubID         string
		workspaceID    uint
		requestBody    UpdateConfigRequest
		setupMock      func(*sqlmock.Sqlmock)
		expectedStatus int
		expectedError  bool
		expectedFields []string
	}{
		{
			name:        "Test successful config update with multiple fields",
			stubID:      "test-stub-123",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu":    2000,
					"runtime.memory": 4096,
					"python_version": "python3.9",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(1, "test-stub-123", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000},"python_version":"python3"}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-123").WillReturnRows(rows)

				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"runtime.cpu", "runtime.memory", "python_version"},
		},
		{
			name:        "Test successful config update with single field",
			stubID:      "test-stub-456",
			workspaceID: 2,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(2, "test-stub-456", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 2)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-456").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 2).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"runtime.cpu"},
		},
		{
			name:        "Test successful config update with gpu field",
			stubID:      "test-stub-456",
			workspaceID: 2,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
					"runtime.gpu": "H100",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(2, "test-stub-456", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000, "gpu": "T4"}}`, 2)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-456").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 2).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"runtime.cpu", "runtime.gpu"},
		},
		{
			name:        "Test stub not found",
			stubID:      "non-existent-stub",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				(*mock).ExpectQuery("SELECT").WithArgs("non-existent-stub").WillReturnError(sql.ErrNoRows)
			},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
		{
			name:        "Test workspace mismatch",
			stubID:      "test-stub-789",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(3, "test-stub-789", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 999) // Different workspace
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-789").WillReturnRows(rows)
			},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
		{
			name:        "Test empty fields",
			stubID:      "test-stub-empty",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(4, "test-stub-empty", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-empty").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test invalid field path",
			stubID:      "test-stub-invalid",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"invalid.field": "value",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(5, "test-stub-invalid", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-invalid").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test invalid config JSON",
			stubID:      "test-stub-bad-config",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(6, "test-stub-bad-config", "Test Stub", `invalid json`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-bad-config").WillReturnRows(rows)
			},
			expectedStatus: http.StatusInternalServerError,
			expectedError:  true,
		},
		{
			name:        "Test CPU and memory validation failure",
			stubID:      "test-stub-limits",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu":    200000, // Exceeds limit
					"runtime.memory": 50000,  // Exceeds limit
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(7, "test-stub-limits", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-limits").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test database update failure",
			stubID:      "test-stub-db-error",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(8, "test-stub-db-error", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-db-error").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 8).WillReturnError(sql.ErrConnDone)
			},
			expectedStatus: http.StatusInternalServerError,
			expectedError:  true,
		},
		{
			name:        "Test empty field path",
			stubID:      "test-stub-empty-path",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"": "value",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(9, "test-stub-empty-path", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-empty-path").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test nested field update",
			stubID:      "test-stub-nested",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"task_policy.max_retries": 5,
					"task_policy.timeout":     7200,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(10, "test-stub-nested", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000},"task_policy":{"max_retries":3,"timeout":3600}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-nested").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 10).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"task_policy.max_retries", "task_policy.timeout"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubGroup, mock, e := NewStubGroupWithMockForTest()

			if tt.setupMock != nil {
				tt.setupMock(&mock)
			}

			jsonBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPatch, "/", nil)
			req.Body = io.NopCloser(bytes.NewReader(jsonBody))
			req.ContentLength = int64(len(jsonBody))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			c.SetParamNames("stubId")
			c.SetParamValues(tt.stubID)

			authCtx := &auth.HttpAuthContext{
				Context: c,
				AuthInfo: &auth.AuthInfo{
					Workspace: &types.Workspace{Id: tt.workspaceID},
				},
			}

			err := stubGroup.UpdateConfig(authCtx)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Mock expectations were not met: %v", err)
			}

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if httpErr, ok := err.(*echo.HTTPError); ok {
					if httpErr.Code != tt.expectedStatus {
						t.Errorf("Expected status %d, got %d", tt.expectedStatus, httpErr.Code)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if rec.Code != tt.expectedStatus {
					t.Errorf("Expected status %d, got %d", tt.expectedStatus, rec.Code)
				}

				if tt.expectedStatus == http.StatusOK {
					var response map[string]interface{}
					if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
						t.Errorf("Failed to unmarshal response: %v", err)
					}

					if message, ok := response["message"].(string); !ok {
						t.Error("Response missing message field")
					} else if !strings.Contains(message, "Stub config updated successfully") {
						t.Errorf("Unexpected message: %s", message)
					}

					if updatedFields, ok := response["updated_fields"].([]interface{}); ok {
						if len(updatedFields) != len(tt.expectedFields) {
							t.Errorf("Expected %d updated fields, got %d", len(tt.expectedFields), len(updatedFields))
						}

						for _, expectedField := range tt.expectedFields {
							found := false
							for _, field := range updatedFields {
								if field == expectedField {
									found = true
									break
								}
							}
							if !found {
								t.Errorf("Expected field %s not found in response", expectedField)
							}
						}
					} else {
						t.Error("Response missing updated_fields")
					}
				}
			}
		})
	}
}

func TestUpdateConfigField(t *testing.T) {
	stubGroup := NewStubGroupForTest()

	tests := []struct {
		name        string
		config      *types.StubConfigV1
		fieldPath   string
		value       interface{}
		expectedErr bool
		checkResult func(*types.StubConfigV1) bool
	}{
		{
			name: "Test updating runtime.cpu",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000, Memory: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     2000,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 2000
			},
		},
		{
			name: "Test updating runtime.memory",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000, Memory: 1000},
			},
			fieldPath: "runtime.memory",
			value:     4096,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Memory == 4096
			},
		},
		{
			name: "Test updating python_version",
			config: &types.StubConfigV1{
				PythonVersion: "python3",
			},
			fieldPath: "python_version",
			value:     "python3.9",
			checkResult: func(config *types.StubConfigV1) bool {
				return config.PythonVersion == "python3.9"
			},
		},
		{
			name: "Test updating nested field",
			config: &types.StubConfigV1{
				TaskPolicy: types.TaskPolicy{MaxRetries: 3, Timeout: 3600},
			},
			fieldPath: "task_policy.max_retries",
			value:     5,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.TaskPolicy.MaxRetries == 5
			},
		},
		{
			name: "Test updating boolean field",
			config: &types.StubConfigV1{
				Authorized: false,
			},
			fieldPath: "authorized",
			value:     true,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Authorized == true
			},
		},
		{
			name: "Test updating with string value for int field",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     "1500",
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 1500
			},
		},
		{
			name: "Test updating with float value for int field",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     2500.0,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 2500
			},
		},
		{
			name: "Test invalid field path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "invalid.field",
			value:       "value",
			expectedErr: true,
		},
		{
			name: "Test empty field path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "",
			value:       "value",
			expectedErr: true,
		},
		{
			name: "Test empty field name in path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "runtime..cpu",
			value:       2000,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configCopy := *tt.config

			err := stubGroup.updateConfigField(&configCopy, tt.fieldPath, tt.value)

			if tt.expectedErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if tt.checkResult != nil && !tt.checkResult(&configCopy) {
					t.Errorf("Field update did not produce expected result")
				}
			}
		})
	}
}
