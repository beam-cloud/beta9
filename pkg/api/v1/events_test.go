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
)

type cacheEventRepo struct {
	repository.EventRepository

	called bool
	query  types.EventQuery
}

func (r *cacheEventRepo) GetEventHistory(ctx context.Context, query types.EventQuery) (*types.EventHistoryResponse, error) {
	r.called = true
	r.query = query
	return &types.EventHistoryResponse{Streams: []string{"events/platform/cache"}}, nil
}

func TestSummarizeContainerEventsBatchReturnsCoverageAndBottleneck(t *testing.T) {
	items := []ContainerEventSummary{
		{
			ContainerID: "container-1",
			EventCount:  4,
			Summary: map[string]int64{
				"container_startup_ms":         200,
				"image_ms":                     10,
				"network_setup_ms":             150,
				"network_ms":                   150,
				"runtime_ms":                   200,
				"runtime_start_to_pid_ms":      20,
				"scheduler_backlog_ms":         5,
				"clip_read_total_us":           500000,
				"worker_queue_ms":              2,
				"worker_receive_to_running_ms": 190,
				"running_to_first_log_ms":      30,
			},
		},
		{
			ContainerID: "container-2",
			EventCount:  4,
			Missing:     []string{"image.load"},
			Summary: map[string]int64{
				"container_startup_ms":         100,
				"network_setup_ms":             90,
				"network_ms":                   90,
				"runtime_ms":                   100,
				"runtime_start_to_pid_ms":      10,
				"scheduler_backlog_ms":         4,
				"worker_queue_ms":              2,
				"worker_receive_to_running_ms": 95,
				"running_to_first_log_ms":      25,
			},
		},
	}

	response := summarizeContainerEventsBatch(items, 5, []string{"container.startup", "image.load"}, []string{"image_ms", "network_ms"})

	if got, want := response.Coverage.ContainersWithEvents, 2; got != want {
		t.Fatalf("unexpected event coverage: got %d want %d", got, want)
	}
	if got, want := response.Coverage.RequiredLifecycleMissing["image.load"], 1; got != want {
		t.Fatalf("unexpected image lifecycle misses: got %d want %d", got, want)
	}
	if got, want := response.Coverage.RequiredMetricMissing["image_ms"], 1; got != want {
		t.Fatalf("unexpected image metric misses: got %d want %d", got, want)
	}
	if response.PrimaryBottleneck == nil {
		t.Fatal("expected primary bottleneck")
	}
	if got, want := response.PrimaryBottleneck.EventID, "network.setup"; got != want {
		t.Fatalf("unexpected primary bottleneck: got %q want %q", got, want)
	}
	if len(response.Stages) == 0 {
		t.Fatal("expected user-facing stage summaries")
	}
	if got, want := response.Stages[0].EventID, "worker_queue"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}
	for _, phase := range response.Phases {
		if phase.MetricKey == "clip_read_total_us" {
			t.Fatal("non-millisecond metrics should not be exposed as phases")
		}
	}
}

func TestNormalizeContainerEventsBatchTargets(t *testing.T) {
	targets := normalizeContainerEventsBatchTargets(ContainerEventsBatchRequest{
		Targets: []ContainerEventsBatchTarget{
			{ContainerID: " container-1 ", StubID: " stub-1 "},
			{ContainerID: "container-1", StubID: "stub-1"},
			{TaskID: " task-1 "},
		},
		ContainerIDs: []string{"container-2", ""},
		TaskIDs:      []string{"task-1", "task-2"},
	})

	if got, want := len(targets), 4; got != want {
		t.Fatalf("unexpected targets: got %d want %d: %#v", got, want, targets)
	}
	if got, want := targets[0].ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected normalized container id: got %q want %q", got, want)
	}
	if got, want := targets[0].StubID, "stub-1"; got != want {
		t.Fatalf("unexpected normalized stub id: got %q want %q", got, want)
	}
	if got, want := targets[1].TaskID, "task-1"; got != want {
		t.Fatalf("unexpected normalized task id: got %q want %q", got, want)
	}
}

func TestEventQueryTypesNormalizesValues(t *testing.T) {
	eventTypes := eventQueryTypes([]string{" container.event ", "", "container.lifecycle", "container.event"})

	if got, want := len(eventTypes), 2; got != want {
		t.Fatalf("unexpected event type count: got %d want %d", got, want)
	}
	if got, want := eventTypes[0], "container.event"; got != want {
		t.Fatalf("unexpected first event type: got %q want %q", got, want)
	}
	if got, want := eventTypes[1], "container.lifecycle"; got != want {
		t.Fatalf("unexpected second event type: got %q want %q", got, want)
	}
}

func TestEventStreamQueryFromContextParsesS2ReadOptions(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?event_types=container.event,container.lifecycle&seq_num=7&wait=30&limit=10", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())

	query, err := eventStreamQueryFromContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if query.SeqNum == nil || *query.SeqNum != 7 {
		t.Fatalf("unexpected seq_num: %#v", query.SeqNum)
	}
	if query.WaitSeconds == nil || *query.WaitSeconds != 30 {
		t.Fatalf("unexpected wait: %#v", query.WaitSeconds)
	}
	if query.Clamp == nil || !*query.Clamp {
		t.Fatalf("expected seq_num streams to clamp by default")
	}
	if got, want := query.Limit, uint64(10); got != want {
		t.Fatalf("unexpected limit: got %d want %d", got, want)
	}
	if got, want := len(query.EventTypes), 2; got != want {
		t.Fatalf("unexpected event type count: got %d want %d", got, want)
	}
}

func TestEventStreamQueryFromContextUsesLastEventID(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Last-Event-ID", "41")
	ctx := e.NewContext(req, httptest.NewRecorder())

	query, err := eventStreamQueryFromContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if query.SeqNum == nil || *query.SeqNum != 42 {
		t.Fatalf("unexpected resumed seq_num: %#v", query.SeqNum)
	}
}

func TestEventHistoryQueryFromContextParsesFilters(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?event_types=stub.state.warning,stub.state.degraded&stub_id=stub-1&container_id=container-1&start_time=2026-05-28T10:00:00Z&end_time=2026-05-28T10:05:00Z&limit=25", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues("workspace-1")

	query, err := eventHistoryQueryFromContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := query.WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace id: got %q want %q", got, want)
	}
	if got, want := query.StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub id: got %q want %q", got, want)
	}
	if got, want := query.ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected container id: got %q want %q", got, want)
	}
	if got, want := query.Limit, uint64(25); got != want {
		t.Fatalf("unexpected limit: got %d want %d", got, want)
	}
	if query.StartTime == nil || query.EndTime == nil {
		t.Fatal("expected start and end times")
	}
	if got, want := len(query.EventTypes), 2; got != want {
		t.Fatalf("unexpected event type count: got %d want %d", got, want)
	}
}

func TestCacheEventQueryUsesStubCacheStream(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?workspace_id=workspace-1&stub_id=stub-1&container_id=container-1&start_time=2026-05-28T10:00:00Z&end_time=2026-05-28T10:05:00Z&limit=25", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())

	query, err := cacheEventQueryFromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if query.Platform != "" {
		t.Fatalf("cache route should read the stub cache stream, got platform %q", query.Platform)
	}
	if got, want := query.EventTypes, []string{types.EventStubCacheRequiredContent}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected event types: got %#v want %#v", got, want)
	}
	if got, want := query.WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace id: got %q want %q", got, want)
	}
	if got, want := query.StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub id: got %q want %q", got, want)
	}
	if got, want := query.ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected container id: got %q want %q", got, want)
	}
	if query.StartTime == nil || query.EndTime == nil {
		t.Fatal("expected start and end times")
	}
	if query.TailOffset != nil {
		t.Fatalf("explicit start time should not also set tail offset: %#v", query.TailOffset)
	}
}

func TestCacheEventQueryDefaultsToBoundedTailRead(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?workspace_id=workspace-1&stub_id=stub-1&limit=50000", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())

	query, err := cacheEventQueryFromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := query.Limit, uint64(1000); got != want {
		t.Fatalf("unexpected capped limit: got %d want %d", got, want)
	}
	if query.TailOffset == nil {
		t.Fatal("expected default tail offset")
	}
	if got, want := *query.TailOffset, int64(1000); got != want {
		t.Fatalf("unexpected tail offset: got %d want %d", got, want)
	}
}

func TestCacheEventQueryRejectsMultipleStartSelectors(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?workspace_id=workspace-1&stub_id=stub-1&start_time=2026-05-28T10:00:00Z&tail_offset=100", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())

	if _, err := cacheEventQueryFromContext(ctx); err == nil {
		t.Fatal("expected multiple start selectors to be rejected")
	}
}

func TestCacheEventQueryRequiresStubTarget(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?workspace_id=workspace-1", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())

	if _, err := cacheEventQueryFromContext(ctx); err == nil {
		t.Fatal("expected missing stub id to be rejected")
	}
}

func TestCacheEventsRequiresClusterAdminToken(t *testing.T) {
	e := echo.New()
	repo := &cacheEventRepo{}
	group := &EventGroup{eventRepo: repo}
	handler := auth.WithClusterAdminAuth(group.GetCacheEvents)

	req := httptest.NewRequest(http.MethodGet, "/?limit=25", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())
	workspaceCtx := &auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Token: &types.Token{TokenType: types.TokenTypeWorkspace},
		},
	}

	err := handler(workspaceCtx)
	if err == nil {
		t.Fatal("expected workspace token to be rejected")
	}
	httpErr, ok := err.(*echo.HTTPError)
	if !ok || httpErr.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected workspace token error: %#v", err)
	}
	if repo.called {
		t.Fatal("event repo should not be called for non-admin token")
	}

	req = httptest.NewRequest(http.MethodGet, "/?limit=25&workspace_id=workspace-1&stub_id=stub-1", nil)
	rec := httptest.NewRecorder()
	ctx = e.NewContext(req, rec)
	adminCtx := &auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Token: &types.Token{TokenType: types.TokenTypeClusterAdmin},
		},
	}

	if err := handler(adminCtx); err != nil {
		t.Fatal(err)
	}
	if !repo.called {
		t.Fatal("expected event repo to be called for admin token")
	}
	if got, want := rec.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status: got %d want %d", got, want)
	}
	if repo.query.Platform != "" {
		t.Fatalf("unexpected platform stream: got %q", repo.query.Platform)
	}
	if got, want := repo.query.WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace id: got %q want %q", got, want)
	}
	if got, want := repo.query.StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub id: got %q want %q", got, want)
	}
	if got, want := repo.query.EventTypes[0], types.EventStubCacheRequiredContent; got != want {
		t.Fatalf("unexpected event type: got %q want %q", got, want)
	}
}
