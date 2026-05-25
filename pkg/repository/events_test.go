package repository

import (
	"context"
	"encoding/json"
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

func TestPushContainerTaskEventBuildsCanonicalEvent(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{sinks: []eventSink{sink}}
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
	repo := &EventClientRepo{sinks: []eventSink{sink}}
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

func TestPushContainerTaskLifecycleSinceSkipsMissingTimestamp(t *testing.T) {
	sink := &captureEventSink{}
	repo := &EventClientRepo{sinks: []eventSink{sink}}

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
