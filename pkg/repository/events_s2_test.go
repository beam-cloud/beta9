package repository

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

func TestS2ContainerStreamNameUsesWorkspaceStubContainer(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	stream := repo.streamNameForEvent(types.EventContainerLifecycle, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
	})

	if got, want := string(stream), "events/workspaces/workspace-123/stubs/stub-456/containers/container-789"; got != want {
		t.Fatalf("unexpected stream name: got %q want %q", got, want)
	}
}

func TestS2ContainerScopedEventsDoNotFallbackToNonCanonicalStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	stream := repo.streamNameForEvent(types.EventContainerLifecycle, eventMetadata{
		ContainerID: "container-789",
		TaskID:      "task-123",
		WorkerID:    "worker-123",
	})

	if stream != "" {
		t.Fatalf("container-scoped event should not fall back to %q without workspace/stub metadata", stream)
	}
}

func TestEventMetadataExtensionsRoundTrip(t *testing.T) {
	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventContainerEvent, types.EventContainerEventSchemaVersion, types.EventContainerEventSchema{
		ID:          types.ContainerEventRuntimeExited,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		TaskID:      "task-1",
		WorkerID:    "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	if metadata.ContainerID != "container-1" ||
		metadata.WorkspaceID != "workspace-1" ||
		metadata.StubID != "stub-1" ||
		metadata.TaskID != "task-1" ||
		metadata.WorkerID != "worker-1" {
		t.Fatalf("metadata did not round trip: %#v", metadata)
	}
}

func TestS2StreamDeletionPendingErrorIsTransient(t *testing.T) {
	err := fmt.Errorf("ensure stream: %w", &s2.S2Error{
		Code:   "stream_deletion_pending",
		Status: 409,
		Origin: "server",
	})

	if !isS2EventStreamDeletionPending(err) {
		t.Fatal("expected stream_deletion_pending to be recognized through wrapping")
	}

	otherErr := &s2.S2Error{
		Code:   "resource_already_exists",
		Status: 409,
		Origin: "server",
	}
	if isS2EventStreamDeletionPending(otherErr) {
		t.Fatal("unexpectedly treated non-deletion-pending S2 error as transient")
	}
}

func TestAugmentContainerEventResponseBuildsLifecycleSummary(t *testing.T) {
	now := time.Now().UTC()
	response := &types.ContainerEventsResponse{
		ContainerID: "container-1",
		Summary:     map[string]int64{},
	}

	lifecycleData, err := json.Marshal(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		Domain:      types.EventDomainImage,
		StartTime:   now,
		EndTime:     now.Add(1200 * time.Millisecond),
		DurationMs:  1200,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		WorkerID:    "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	record := types.ContainerEventRecord{
		Type: types.EventContainerLifecycle,
		Data: lifecycleData,
	}
	augmentContainerEventResponse(response, &record)
	response.Events = append(response.Events, record)
	response.Summary = summarizeContainerLifecycleDurations(response.Events)

	if got, want := response.WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace: got %q want %q", got, want)
	}
	if got, want := response.StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub: got %q want %q", got, want)
	}
	if got, want := response.Summary["image_ms"], int64(1200); got != want {
		t.Fatalf("unexpected image summary: got %d want %d", got, want)
	}
	if got, want := record.EventID, string(types.ContainerLifecycleImageLoad); got != want {
		t.Fatalf("unexpected event id: got %q want %q", got, want)
	}
}

func TestNestedImageLifecycleDoesNotDoubleCountImageSummary(t *testing.T) {
	now := time.Now().UTC()
	response := &types.ContainerEventsResponse{
		ContainerID: "container-1",
		Summary:     map[string]int64{},
	}

	lifecycleData, err := json.Marshal(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleID("image.embedded_cache_restore"),
		Domain:      types.EventDomainImage,
		ParentID:    types.ContainerLifecycleImageLoad,
		StartTime:   now,
		EndTime:     now.Add(300 * time.Millisecond),
		DurationMs:  300,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	record := types.ContainerEventRecord{
		Type: types.EventContainerLifecycle,
		Data: lifecycleData,
	}
	augmentContainerEventResponse(response, &record)
	response.Events = append(response.Events, record)
	response.Summary = summarizeContainerLifecycleDurations(response.Events)

	if got := response.Summary["image_ms"]; got != 0 {
		t.Fatalf("nested image lifecycle should not inflate image_ms, got %d", got)
	}
	if got, want := response.Summary["image_embedded_cache_restore_ms"], int64(300); got != want {
		t.Fatalf("unexpected nested image summary: got %d want %d", got, want)
	}
}

func TestRepeatedCumulativeLifecycleUsesMaxDuration(t *testing.T) {
	response := &types.ContainerEventsResponse{
		ContainerID: "container-1",
		Events: []types.ContainerEventRecord{
			{
				Type:        types.EventContainerLifecycle,
				EventID:     string(types.ContainerLifecycleSchedulerBacklogWait),
				Domain:      string(types.EventDomainScheduler),
				DurationMs:  1000,
				ContainerID: "container-1",
			},
			{
				Type:        types.EventContainerLifecycle,
				EventID:     string(types.ContainerLifecycleSchedulerBacklogWait),
				Domain:      string(types.EventDomainScheduler),
				DurationMs:  4000,
				ContainerID: "container-1",
			},
		},
	}

	response.Summary = summarizeContainerLifecycleDurations(response.Events)

	if got, want := response.Summary["scheduler_backlog_ms"], int64(4000); got != want {
		t.Fatalf("unexpected backlog summary: got %d want %d", got, want)
	}
	if got, want := response.Summary["scheduler_ms"], int64(4000); got != want {
		t.Fatalf("unexpected scheduler summary: got %d want %d", got, want)
	}
}

func TestSummaryIncludesLogTimingCheckpoints(t *testing.T) {
	now := time.Now().UTC()
	events := []types.ContainerEventRecord{
		{
			Type:       types.EventContainerLifecycle,
			EventID:    string(types.ContainerLifecycleSchedulerQueuePush),
			StartTime:  now.Add(-500 * time.Millisecond),
			EndTime:    now.Add(-400 * time.Millisecond),
			DurationMs: 100,
		},
		{
			Type:       types.EventContainerLifecycle,
			EventID:    string(types.ContainerLifecycleStartup),
			StartTime:  now,
			EndTime:    now.Add(time.Second),
			DurationMs: 1000,
		},
		{
			Type:      types.EventContainerEvent,
			EventID:   string(types.ContainerEventRunnerProcessStarted),
			Timestamp: now.Add(2 * time.Second),
		},
		{
			Type:      types.EventContainerEvent,
			EventID:   string(types.ContainerEventRunnerModuleLoaded),
			Timestamp: now.Add(3 * time.Second),
		},
		{
			Type:      types.EventContainerEvent,
			EventID:   string(types.ContainerEventRunnerMainEntered),
			Timestamp: now.Add(3500 * time.Millisecond),
		},
		{
			Type:      types.EventContainerEvent,
			EventID:   string(types.ContainerEventRunnerStartTask),
			Timestamp: now.Add(4 * time.Second),
		},
		{
			Type:      types.EventContainerLog,
			Timestamp: now.Add(6 * time.Second),
			Line:      "user log",
		},
	}

	summary := summarizeContainerLifecycleDurations(events)

	if got, want := summary["running_to_first_log_ms"], int64(5000); got != want {
		t.Fatalf("unexpected running to first log summary: got %d want %d", got, want)
	}
	if got, want := summary["start_task_to_first_log_ms"], int64(2000); got != want {
		t.Fatalf("unexpected start task to first log summary: got %d want %d", got, want)
	}
	if got, want := summary["scheduler_queue_to_running_ms"], int64(1500); got != want {
		t.Fatalf("unexpected scheduler queue to running summary: got %d want %d", got, want)
	}
	if got, want := summary["running_to_runner_process_started_ms"], int64(1000); got != want {
		t.Fatalf("unexpected running to process summary: got %d want %d", got, want)
	}
	if got, want := summary["runner_process_to_module_loaded_ms"], int64(1000); got != want {
		t.Fatalf("unexpected process to module summary: got %d want %d", got, want)
	}
	if got, want := summary["runner_module_loaded_to_main_ms"], int64(500); got != want {
		t.Fatalf("unexpected module to main summary: got %d want %d", got, want)
	}
	if got, want := summary["runner_main_to_start_task_ms"], int64(500); got != want {
		t.Fatalf("unexpected main to start task summary: got %d want %d", got, want)
	}
}
