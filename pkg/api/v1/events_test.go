package apiv1

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

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

func TestGetEventHistoryReturnsServiceUnavailableWhenReadsUnsupported(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/workspace-1/history", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues("workspace-1")

	group := &EventGroup{eventRepo: repository.NewEventClientRepo(types.AppConfig{})}
	if err := group.GetEventHistory(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestGetEventHistoryDoesNotReturn500ForCanceledReads(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/workspace-1/history", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues("workspace-1")

	group := &EventGroup{eventRepo: canceledEventHistoryRepo{}}
	if err := group.GetEventHistory(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status = %d, want %d", rec.Code, statusClientClosedRequest)
	}
}

type canceledEventHistoryRepo struct {
	repository.EventRepository
}

func (canceledEventHistoryRepo) GetEventHistory(context.Context, types.EventQuery) (*types.EventHistoryResponse, error) {
	return nil, fmt.Errorf("read event history from s2 stream: %w", context.Canceled)
}
