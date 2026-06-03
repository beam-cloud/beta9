package repository

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type captureEventSink struct {
	events []cloudevents.Event
}

func (s *captureEventSink) PushEvent(event cloudevents.Event) error {
	s.events = append(s.events, event)
	return nil
}

func TestEventHTTPSinkDeliversCloudEvent(t *testing.T) {
	type callbackRequest struct {
		body []byte
		err  string
	}
	requests := make(chan callbackRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("X-Test"), "yes"; got != want {
			requests <- callbackRequest{err: "unexpected header: got " + got + " want " + want}
			return
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			requests <- callbackRequest{err: err.Error()}
			return
		}
		raw, err := json.Marshal(body)
		if err != nil {
			requests <- callbackRequest{err: err.Error()}
			return
		}
		requests <- callbackRequest{body: raw}
	}))
	defer server.Close()

	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventContainerEvent, types.EventContainerEventSchemaVersion, types.EventContainerEventSchema{
		ID:          types.ContainerEventRuntimeExited,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Message:     "runtime exited",
	})
	if err != nil {
		t.Fatal(err)
	}

	sink := newEventHTTPSink(types.EventCallbackConfig{
		URL:        server.URL,
		EventTypes: []string{"container.*"},
		Headers:    map[string]string{"X-Test": "yes"},
	})
	defer close(sink.queue)

	if err := sink.PushEvent(event); err != nil {
		t.Fatal(err)
	}

	select {
	case req := <-requests:
		if req.err != "" {
			t.Fatal(req.err)
		}
		var delivered struct {
			Type string `json:"type"`
			Data struct {
				ContainerID string `json:"container_id"`
				Message     string `json:"message"`
			} `json:"data"`
		}
		if err := json.Unmarshal(req.body, &delivered); err != nil {
			t.Fatal(err)
		}
		if got, want := delivered.Type, types.EventContainerEvent; got != want {
			t.Fatalf("unexpected event type: got %q want %q", got, want)
		}
		if got, want := delivered.Data.ContainerID, "container-1"; got != want {
			t.Fatalf("unexpected container id: got %q want %q", got, want)
		}
		if got, want := delivered.Data.Message, "runtime exited"; got != want {
			t.Fatalf("unexpected message: got %q want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event callback")
	}
}

func TestTaskCloudEventUsesTaskUpdatedAt(t *testing.T) {
	repo := &EventClientRepo{}
	updatedAt := time.Date(2026, 5, 28, 12, 0, 0, 123000000, time.UTC)

	event, err := repo.createEventObject(types.EventTaskUpdated, types.EventTaskSchemaVersion, types.EventTaskSchema{
		ID:        "task-1",
		Status:    types.TaskStatusComplete,
		CreatedAt: updatedAt.Add(-time.Minute),
		UpdatedAt: updatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !event.Time().Equal(updatedAt) {
		t.Fatalf("unexpected event time: got %s want %s", event.Time(), updatedAt)
	}
}

func TestEventHTTPSinkSkipsUnmatchedEvents(t *testing.T) {
	if !eventHTTPMatches([]string{"container.*"}, types.EventContainerEvent) {
		t.Fatal("expected wildcard callback filter to match container event")
	}
	if eventHTTPMatches([]string{"task.*"}, types.EventContainerEvent) {
		t.Fatal("expected task wildcard callback filter to skip container event")
	}
}

func TestPushContainerTaskEventBuildsCanonicalEvent(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}
	task := &types.TaskWithRelated{
		Task: types.Task{
			ExternalId:  "task-1",
			ContainerId: "container-1",
		},
		Stub: types.Stub{
			ExternalId: "stub-1",
			Type:       types.StubType(types.StubTypeFunction),
		},
		Workspace: types.Workspace{
			ExternalId: "workspace-1",
		},
	}

	repo.PushContainerTaskEvent(task, types.ContainerEventRunnerStartTask, types.ContainerEventOptions{
		Source:  types.EventSourceGatewayStartTask,
		Message: types.EventMessageRunnerCalledStartTask,
		Attrs:   map[string]string{types.EventAttrContainerID: "container-1"},
	})

	if got, want := len(sink.events), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}
	if got, want := sink.events[0].Type(), types.EventContainerEvent; got != want {
		t.Fatalf("unexpected cloud event type: got %q want %q", got, want)
	}

	var event types.EventContainerEventSchema
	if err := json.Unmarshal(sink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if got, want := event.ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected container id: got %q want %q", got, want)
	}
	if got, want := event.Source, types.EventSourceGatewayStartTask.String(); got != want {
		t.Fatalf("unexpected source: got %q want %q", got, want)
	}
	if got, want := event.Message, types.EventMessageRunnerCalledStartTask.String(); got != want {
		t.Fatalf("unexpected message: got %q want %q", got, want)
	}
	if got, want := event.Attrs[types.EventAttrContainerID], "container-1"; got != want {
		t.Fatalf("unexpected attr: got %q want %q", got, want)
	}
}

func TestPushContainerRunnerEventFallbacksToRunnerDomain(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		WorkspaceId: "workspace-1",
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeFunction)},
		},
	}

	repo.PushContainerRunnerEvent("worker-1", request, &types.ContainerRunnerEvent{
		Type:      types.RunnerEventTypeEvent,
		ID:        "runner.custom_event",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		TaskID:    "task-1",
		Message:   "custom runner event",
	})

	if got, want := len(sink.events), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}

	var event types.EventContainerEventSchema
	if err := json.Unmarshal(sink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if got, want := event.Domain, types.EventDomainRunner; got != want {
		t.Fatalf("unexpected domain: got %q want %q", got, want)
	}
	if got, want := event.Source, types.EventSourceRunnerStdout.String(); got != want {
		t.Fatalf("unexpected source: got %q want %q", got, want)
	}
	if got, want := event.WorkerID, "worker-1"; got != want {
		t.Fatalf("unexpected worker id: got %q want %q", got, want)
	}
}

func TestPushContainerResourceMetricsEventIncludesReservedResources(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		WorkspaceId: "workspace-1",
		Cpu:         2500,
		GpuCount:    2,
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeFunction)},
		},
	}

	repo.PushContainerResourceMetricsEvent("worker-1", request, types.EventContainerMetricsData{
		CPUTotal: uint64(request.Cpu),
	})

	if got, want := len(sink.events), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}

	var event types.EventContainerMetricsSchema
	if err := json.Unmarshal(sink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if got, want := event.CPU, request.Cpu; got != want {
		t.Fatalf("unexpected cpu: got %d want %d", got, want)
	}
	if got, want := event.GPUCount, request.GpuCount; got != want {
		t.Fatalf("unexpected gpu count: got %d want %d", got, want)
	}
}

func TestPushContainerTaskLifecycleSinceSkipsMissingTimestamp(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}

	repo.PushContainerTaskLifecycleSince(context.Background(), nil, &types.TaskWithRelated{
		Task: types.Task{
			ExternalId:  "task-1",
			ContainerId: "container-1",
		},
		Stub: types.Stub{
			ExternalId: "stub-1",
			Type:       types.StubType(types.StubTypeFunction),
		},
		Workspace: types.Workspace{
			ExternalId: "workspace-1",
			Name:       "workspace-name",
		},
	}, types.ContainerLifecycleRunnerStartToGetArgs, types.FunctionLifecycleCheckpointStartTask, time.Now(), true, types.ContainerLifecycleOptions{
		Source: types.EventSourceGatewayFunctionGetArgs,
	})

	if len(sink.events) != 0 {
		t.Fatalf("expected missing lifecycle timestamp to skip event, got %d events", len(sink.events))
	}
}

func TestPushContainerLogSkipsCallbackSinks(t *testing.T) {
	storageSink := &captureEventSink{}
	callbackSink := &captureEventSink{}
	repo := &EventClientRepo{
		storageSinks:  []eventSink{storageSink},
		callbackSinks: []eventSink{callbackSink},
	}

	repo.PushContainerLogEvent(types.EventContainerLogSchema{
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Line:        "hello",
	})

	if got, want := len(storageSink.events), 1; got != want {
		t.Fatalf("unexpected storage event count: got %d want %d", got, want)
	}
	if got := len(callbackSink.events); got != 0 {
		t.Fatalf("expected log event to skip callbacks, got %d callback events", got)
	}
}

func TestContainerLogCloudEventUsesLogTimestamp(t *testing.T) {
	logAt := time.Date(2026, 5, 28, 12, 30, 0, 123456789, time.UTC)
	repo := &EventClientRepo{}

	event, err := repo.createEventObject(types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		Timestamp:   logAt,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Line:        "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := event.Time(); !got.Equal(logAt) {
		t.Fatalf("unexpected event time: got %s want %s", got, logAt)
	}
}

func TestPushContainerRequestLogLineIncludesRequestContext(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		WorkspaceId: "workspace-1",
		AppId:       "app-1",
		Env:         []string{"TASK_ID=task-from-env"},
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeFunction)},
		},
	}

	repo.PushContainerRequestLogLine("worker-1", request, "", types.EventLogStreamStdout, "hello")

	if got, want := len(sink.events), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}

	var event types.EventContainerLogSchema
	if err := json.Unmarshal(sink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}

	if got, want := event.ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected container id: got %q want %q", got, want)
	}
	if got, want := event.TaskID, "task-from-env"; got != want {
		t.Fatalf("unexpected task id: got %q want %q", got, want)
	}
	if got, want := event.AppID, "app-1"; got != want {
		t.Fatalf("unexpected app id: got %q want %q", got, want)
	}
	if got, want := event.WorkerID, "worker-1"; got != want {
		t.Fatalf("unexpected worker id: got %q want %q", got, want)
	}
	if got, want := event.Stream, types.EventLogStreamStdout; got != want {
		t.Fatalf("unexpected stream: got %q want %q", got, want)
	}

	metadata := eventMetadataFromCloudEvent(sink.events[0])
	if got, want := metadata.ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected metadata container id: got %q want %q", got, want)
	}
	if got, want := metadata.TaskID, "task-from-env"; got != want {
		t.Fatalf("unexpected metadata task id: got %q want %q", got, want)
	}
}

func TestPushPlatformLogSkipsCallbackSinks(t *testing.T) {
	storageSink := &captureEventSink{}
	callbackSink := &captureEventSink{}
	repo := &EventClientRepo{
		storageSinks:  []eventSink{storageSink},
		callbackSinks: []eventSink{callbackSink},
	}

	repo.PushPlatformLogEvent(types.EventPlatformLogSchema{
		WorkerID: "worker-1",
		Line:     "worker ready",
	})

	if got, want := len(storageSink.events), 1; got != want {
		t.Fatalf("unexpected storage event count: got %d want %d", got, want)
	}
	if got := len(callbackSink.events); got != 0 {
		t.Fatalf("expected platform log event to skip callbacks, got %d callback events", got)
	}
}

func TestPushPlatformCacheSkipsCallbackSinks(t *testing.T) {
	storageSink := &captureEventSink{}
	callbackSink := &captureEventSink{}
	repo := &EventClientRepo{
		storageSinks:  []eventSink{storageSink},
		callbackSinks: []eventSink{callbackSink},
	}

	repo.PushPlatformCacheEvent(types.EventPlatformCacheSchema{
		Action:      "clip_cache_read_aggregate",
		Result:      "miss",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
	})

	if got, want := len(storageSink.events), 1; got != want {
		t.Fatalf("unexpected storage event count: got %d want %d", got, want)
	}
	if got := len(callbackSink.events); got != 0 {
		t.Fatalf("expected platform cache event to skip callbacks, got %d callback events", got)
	}
	var event types.EventPlatformCacheSchema
	if err := json.Unmarshal(storageSink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if got, want := event.Action, "clip_cache_read_aggregate"; got != want {
		t.Fatalf("unexpected platform cache action: got %q want %q", got, want)
	}
}
