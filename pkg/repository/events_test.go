package repository

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func (s *captureEventSink) PushEventSync(event cloudevents.Event) error {
	return s.PushEvent(event)
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

func TestBeta9WebhookPayloadMatchesInternalAPIShape(t *testing.T) {
	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventStubDeploy, types.EventStubSchemaVersion, types.EventStubSchema{
		ID:          "stub-1",
		StubType:    types.StubType(types.StubTypeFunction),
		WorkspaceID: "workspace-1",
		StubConfig:  "{}",
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := beta9WebhookEventPayload(event)
	if err != nil {
		t.Fatal(err)
	}

	var delivered []struct {
		Type        string          `json:"type"`
		Data        json.RawMessage `json:"data"`
		WorkspaceID string          `json:"workspaceid"`
		StubID      string          `json:"stubid"`
		Date        float64         `json:"date"`
	}
	if err := json.Unmarshal(body, &delivered); err != nil {
		t.Fatal(err)
	}

	if got, want := len(delivered), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}
	if got, want := delivered[0].Type, types.EventStubDeploy; got != want {
		t.Fatalf("unexpected event type: got %q want %q", got, want)
	}
	if got, want := delivered[0].WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace extension: got %q want %q", got, want)
	}
	if got, want := delivered[0].StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub extension: got %q want %q", got, want)
	}
	if delivered[0].Date == 0 {
		t.Fatal("expected beta9 webhook callback to include date")
	}
}

func TestInternalAPIEventCallbackFilter(t *testing.T) {
	eventTypes := internalAPICallbackEventTypes()
	for _, eventType := range eventTypes {
		if !eventHTTPMatches(eventTypes, eventType) {
			t.Fatalf("expected callback filter to match %q", eventType)
		}
	}
	if eventHTTPMatches(eventTypes, types.EventWorkerLifecycle) {
		t.Fatal("expected internal API callback filter to skip worker lifecycle events")
	}
}

func internalAPICallbackEventTypes() []string {
	return []string{
		types.EventStubDeploy,
		types.EventStubServe,
		types.EventStubRun,
		types.EventStubClone,
		types.EventWorkerPoolDegraded,
		types.EventWorkerPoolHealthy,
		types.EventGatewayEndpointCalled,
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
		AppId:       "app-1",
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
	if got, want := event.AppID, request.AppId; got != want {
		t.Fatalf("unexpected app id: got %q want %q", got, want)
	}
	if got, want := sink.events[0].Extensions()["appid"], request.AppId; got != want {
		t.Fatalf("unexpected app extension: got %q want %q", got, want)
	}
}

func TestPushComputeEventBuildsWorkspaceScopedCloudEvent(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{sink}}
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)

	repo.PushComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   now,
		WorkspaceID: "workspace-1",
		PoolName:    "private-gpu",
		MachineID:   "machine-1",
		Action:      types.EventComputeActionMachineJoined,
		Status:      "schedulable",
		Transport:   types.BackendRouteTransportTSNet,
	})

	if got, want := len(sink.events), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}
	if got, want := sink.events[0].Type(), types.EventComputeMachine; got != want {
		t.Fatalf("unexpected cloud event type: got %q want %q", got, want)
	}
	if !sink.events[0].Time().Equal(now) {
		t.Fatalf("unexpected event time: got %s want %s", sink.events[0].Time(), now)
	}
	if got, want := sink.events[0].Extensions()["workspaceid"], "workspace-1"; got != want {
		t.Fatalf("unexpected workspace extension: got %q want %q", got, want)
	}
	if got, want := sink.events[0].Extensions()["machineid"], "machine-1"; got != want {
		t.Fatalf("unexpected machine extension: got %q want %q", got, want)
	}

	var event types.EventComputeSchema
	if err := json.Unmarshal(sink.events[0].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if got, want := event.Action, types.EventComputeActionMachineJoined; got != want {
		t.Fatalf("unexpected action: got %q want %q", got, want)
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

func TestPushContainerLogEventQueuedOptionalProcessFields(t *testing.T) {
	storageSink := &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{storageSink}}

	err := repo.PushContainerLogEventQueued(types.EventContainerLogSchema{
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Stream:      types.EventLogStreamStdout,
		Line:        "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(storageSink.events), 1; got != want {
		t.Fatalf("unexpected storage event count: got %d want %d", got, want)
	}

	raw := storageSink.events[0].Data()
	if json.Valid(raw) {
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if _, ok := payload["pid"]; ok {
			t.Fatal("expected pid to be omitted for ordinary log")
		}
		if _, ok := payload["process_args"]; ok {
			t.Fatal("expected process_args to be omitted for ordinary log")
		}
	}

	err = repo.PushContainerLogEventQueued(types.EventContainerLogSchema{
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Stream:      types.EventLogStreamStderr,
		Line:        "boom",
		PID:         123,
		ProcessArgs: []string{"python3", "-c", "print('hi')"},
		ProcessCwd:  "/workspace",
		ProcessSeq:  7,
	})
	if err != nil {
		t.Fatal(err)
	}

	var event types.EventContainerLogSchema
	if err := json.Unmarshal(storageSink.events[1].Data(), &event); err != nil {
		t.Fatal(err)
	}
	if event.PID != 123 || event.ProcessSeq != 7 || event.ProcessCwd != "/workspace" {
		t.Fatalf("unexpected process fields: %+v", event)
	}
	if got, want := event.ProcessArgs, []string{"python3", "-c", "print('hi')"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected process args: got %#v want %#v", got, want)
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
