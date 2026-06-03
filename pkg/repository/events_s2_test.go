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

func TestS2ContainerEventsAlsoUseStubAggregateStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerLifecycle, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
	})

	if got, want := len(streams), 3; got != want {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", got, want, streams)
	}
	if got, want := string(streams[0]), "events/workspaces/workspace-123/stubs/stub-456/containers/container-789"; got != want {
		t.Fatalf("unexpected container stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[1]), "events/workspaces/workspace-123/stubs/stub-456"; got != want {
		t.Fatalf("unexpected stub stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[2]), "events/workspaces/workspace-123"; got != want {
		t.Fatalf("unexpected workspace stream name: got %q want %q", got, want)
	}
}

func TestS2StubCacheRequiredContentUsesDedicatedStreamOnly(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventStubCacheRequiredContent, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
	})

	if got, want := len(streams), 1; got != want {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", got, want, streams)
	}
	if got, want := string(streams[0]), "events/workspaces/workspace-123/stubs/stub-456/cache"; got != want {
		t.Fatalf("unexpected stub cache stream name: got %q want %q", got, want)
	}
}

func TestS2StubCacheRequiredContentRequiresWorkspaceAndStub(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	cases := []struct {
		name     string
		metadata eventMetadata
	}{
		{name: "missing stub", metadata: eventMetadata{WorkspaceID: "workspace-123"}},
		{name: "missing workspace", metadata: eventMetadata{StubID: "stub-456"}},
		{name: "missing both", metadata: eventMetadata{}},
	}
	for _, tc := range cases {
		if streams := repo.streamNamesForEvent(types.EventStubCacheRequiredContent, tc.metadata); len(streams) != 0 {
			t.Fatalf("%s: expected no streams, got %#v", tc.name, streams)
		}
	}
}

func TestS2PlatformCacheUsesPlatformStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventPlatformCache, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
	})

	if got, want := len(streams), 1; got != want {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", got, want, streams)
	}
	if got, want := string(streams[0]), "events/platform/cache"; got != want {
		t.Fatalf("unexpected platform cache stream name: got %q want %q", got, want)
	}
}

func TestS2ContainerMetricsAlsoUseWorkspaceAggregateStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerMetrics, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/stub-456/containers/container-789",
		"events/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected metric stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected metric stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestS2StubEventsAlsoUseWorkspaceAggregateStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent("stub.state.degraded", eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stub stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stub stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestS2ContainerLogsUseDifferentiatedLogStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerLog, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/logs/workspaces/workspace-123/stubs/stub-456/containers/container-789",
		"events/logs/workspaces/workspace-123/stubs/stub-456",
		"events/logs/workspaces/workspace-123/tasks/task-123",
		"events/logs/workspaces/workspace-123/apps/app-123",
		"events/logs/workspaces/workspace-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected log stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected log stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestS2ContainerLogRecordUsesLogTimestamp(t *testing.T) {
	logAt := time.Date(2026, 5, 28, 12, 30, 0, 123000000, time.UTC)
	eventRepo := &EventClientRepo{}
	event, err := eventRepo.createEventObject(types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		Timestamp:   logAt,
		ContainerID: "container-789",
		StubID:      "stub-456",
		TaskID:      "task-123",
		WorkspaceID: "workspace-123",
		Line:        "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	repo := &S2EventRepository{streamPrefix: "events"}
	record, streams, err := repo.appendRecordForEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) == 0 {
		t.Fatal("expected log streams")
	}
	if record.Timestamp == nil {
		t.Fatal("expected s2 timestamp")
	}
	if got, want := *record.Timestamp, uint64(logAt.UnixMilli()); got != want {
		t.Fatalf("unexpected s2 timestamp: got %d want %d", got, want)
	}
}

func TestTaskLogQueryRequiresTaskTaggedLogs(t *testing.T) {
	query := types.LogQuery{
		TaskID:      "task-123",
		ContainerID: "container-789",
	}

	if logRecordMatchesQuery(types.LogRecord{ContainerID: "container-789", Message: "untagged"}, query) {
		t.Fatal("expected untagged log from the task container to be filtered")
	}
	if !logRecordMatchesQuery(types.LogRecord{TaskID: "task-123", ContainerID: "container-789", Message: "tagged"}, query) {
		t.Fatal("expected tagged task log to match")
	}
	if logRecordMatchesQuery(types.LogRecord{TaskID: "other-task", ContainerID: "container-789", Message: "other task"}, query) {
		t.Fatal("expected other task log from the same container to be filtered")
	}
	if logRecordMatchesQuery(types.LogRecord{ContainerID: "other-container", Message: "other container"}, query) {
		t.Fatal("expected untagged log from a different container to be filtered")
	}
}

func TestS2TaskEventsUseWorkspaceAndAppAggregateStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventTaskCreated, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/tasks/task-123",
		"events/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123",
		"events/workspaces/workspace-123/apps/app-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected task stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected task stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestS2TaskUpdateEventsUseTaskStreamWhenContainerScoped(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventTaskUpdated, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/tasks/task-123",
		"events/workspaces/workspace-123/stubs/stub-456/containers/container-789",
		"events/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123",
		"events/workspaces/workspace-123/apps/app-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected task stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected task stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestTaskEventSchemaIncludesStubTypeAndDeploymentContext(t *testing.T) {
	version := uint(7)
	deploymentID := "deployment-123"
	deploymentName := "api"
	task := &types.TaskWithRelated{
		Task: types.Task{
			ExternalId:  "task-123",
			Status:      types.TaskStatusRunning,
			ContainerId: "container-123",
			CreatedAt:   types.Time{Time: time.Unix(0, 0).UTC()},
			UpdatedAt:   types.Time{Time: time.Unix(10, 0).UTC()},
		},
	}
	task.Workspace.ExternalId = "workspace-123"
	task.Stub.ExternalId = "stub-123"
	task.Stub.Type = types.StubType(types.StubTypeASGIDeployment)
	task.App.ExternalId = "app-123"
	task.Deployment.ExternalId = &deploymentID
	task.Deployment.Name = &deploymentName
	task.Deployment.Version = &version

	event := eventTaskSchemaFromTask(task)

	if event.StubType != types.StubType(types.StubTypeASGIDeployment) {
		t.Fatalf("unexpected stub type: got %q", event.StubType)
	}
	if event.DeploymentID != deploymentID {
		t.Fatalf("unexpected deployment id: got %q want %q", event.DeploymentID, deploymentID)
	}
	if event.DeploymentName != deploymentName {
		t.Fatalf("unexpected deployment name: got %q want %q", event.DeploymentName, deploymentName)
	}
	if event.DeploymentVersion != "7" {
		t.Fatalf("unexpected deployment version: got %q want %q", event.DeploymentVersion, "7")
	}
	if !event.UpdatedAt.Equal(time.Unix(10, 0).UTC()) {
		t.Fatalf("unexpected updated at: got %s", event.UpdatedAt)
	}
}

func TestS2PlatformLogsUseInternalPlatformStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventPlatformLog, eventMetadata{
		WorkerID: "worker-123",
	})

	if got, want := len(streams), 1; got != want {
		t.Fatalf("unexpected platform log stream count: got %d want %d", got, want)
	}
	if got, want := streams[0], s2.StreamName("events/logs/platform/workers/worker-123"); got != want {
		t.Fatalf("unexpected platform log stream: got %q want %q", got, want)
	}

	serviceStream := repo.streamNamesForEvent(types.EventPlatformLog, eventMetadata{
		ServiceName: "gateway",
		InstanceID:  "pod/1",
	})
	if got, want := serviceStream[0], s2.StreamName("events/logs/platform/services/gateway/pod_1"); got != want {
		t.Fatalf("unexpected platform service log stream: got %q want %q", got, want)
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

func TestS2PlatformEventStreamsUseEntityMetadata(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	tests := []struct {
		name      string
		eventType string
		metadata  eventMetadata
		want      string
	}{
		{
			name:      "worker lifecycle",
			eventType: types.EventWorkerLifecycle,
			metadata:  eventMetadata{WorkerID: "worker-1", PoolName: "default"},
			want:      "events/workers/worker-1",
		},
		{
			name:      "worker pool state",
			eventType: types.EventWorkerPoolDegraded,
			metadata:  eventMetadata{PoolName: "gpu/default"},
			want:      "events/worker-pools/gpu_default",
		},
		{
			name:      "gateway endpoint",
			eventType: types.EventGatewayEndpointCalled,
			metadata:  eventMetadata{WorkspaceID: "workspace-1"},
			want:      "events/workspaces/workspace-1",
		},
		{
			name:      "stub state",
			eventType: "stub.state.degraded",
			metadata:  eventMetadata{WorkspaceID: "workspace-1", StubID: "stub-1"},
			want:      "events/workspaces/workspace-1/stubs/stub-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := repo.streamNameForEvent(tt.eventType, tt.metadata)
			if got := string(stream); got != tt.want {
				t.Fatalf("unexpected stream name: got %q want %q", got, tt.want)
			}
		})
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

func TestEventQueryAllowsType(t *testing.T) {
	query := types.EventQuery{EventTypes: []string{types.EventContainerEvent, types.EventTaskUpdated}}

	if !eventQueryAllowsType(query, types.EventTaskUpdated) {
		t.Fatal("expected task.updated to be allowed")
	}
	if eventQueryAllowsType(query, types.EventContainerLog) {
		t.Fatal("expected container.log to be filtered")
	}
	if !eventQueryAllowsType(types.EventQuery{}, types.EventContainerLog) {
		t.Fatal("empty event type filter should allow all events")
	}
	if !eventQueryAllowsType(types.EventQuery{EventTypes: []string{"stub.state.*"}}, "stub.state.degraded") {
		t.Fatal("expected wildcard event type to be allowed")
	}
}

func TestEventRecordMatchesQueryFiltersByScopeAndTime(t *testing.T) {
	start := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	query := types.EventQuery{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		TaskID:      "task-1",
		StartTime:   &start,
		EndTime:     &end,
	}

	record := types.ContainerEventRecord{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		TaskID:      "task-1",
		Timestamp:   start.Add(time.Minute),
	}
	if !eventRecordMatchesQuery(record, query) {
		t.Fatal("expected scoped record inside time range to match")
	}

	record.WorkspaceID = ""
	if eventRecordMatchesQuery(record, query) {
		t.Fatal("expected missing workspace metadata to be rejected")
	}

	record.WorkspaceID = "workspace-1"
	record.Timestamp = end
	if eventRecordMatchesQuery(record, query) {
		t.Fatal("expected end time to be exclusive")
	}
}

func TestEventMetadataPoolNameRoundTrip(t *testing.T) {
	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventWorkerPoolDegraded, types.EventWorkerPoolStateSchemaVersion, types.EventWorkerPoolStateSchema{
		PoolName: "default",
		Status:   string(types.WorkerPoolStatusDegraded),
	})
	if err != nil {
		t.Fatal(err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	if got, want := metadata.PoolName, "default"; got != want {
		t.Fatalf("unexpected pool metadata: got %q want %q", got, want)
	}
}

func TestMetricsBucketCalculatesIORatesFromSampleInterval(t *testing.T) {
	acc := &metricsBucketAccumulator{key: time.Unix(0, 0).UnixMilli()}
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-1",
		ContainerMetrics: types.EventContainerMetricsData{
			SampleIntervalMs: 5000,
			DiskReadBytes:    10 * 1024 * 1024,
			DiskWriteBytes:   5 * 1024 * 1024,
			NetworkBytesRecv: 100 * 1024,
			NetworkBytesSent: 50 * 1024,
		},
	})

	bucket := acc.bucket()
	if got, want := bucket.DiskReadBytesRateAvg.Value, float64(2*1024*1024); got != want {
		t.Fatalf("unexpected disk read rate: got %f want %f", got, want)
	}
	if got, want := bucket.DiskWriteBytesRateAvg.Value, float64(1024*1024); got != want {
		t.Fatalf("unexpected disk write rate: got %f want %f", got, want)
	}
	if got, want := bucket.NetworkRecvBytesRateAvg.Value, float64(20*1024); got != want {
		t.Fatalf("unexpected network recv rate: got %f want %f", got, want)
	}
	if got, want := bucket.NetworkSentBytesRateAvg.Value, float64(10*1024); got != want {
		t.Fatalf("unexpected network sent rate: got %f want %f", got, want)
	}
}

func TestMetricsBucketSumsContainerIORates(t *testing.T) {
	acc := &metricsBucketAccumulator{key: time.Unix(0, 0).UnixMilli()}
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-1",
		ContainerMetrics: types.EventContainerMetricsData{
			SampleIntervalMs: 1000,
			NetworkBytesRecv: 10 * 1024,
		},
	})
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-2",
		ContainerMetrics: types.EventContainerMetricsData{
			SampleIntervalMs: 1000,
			NetworkBytesRecv: 20 * 1024,
		},
	})

	bucket := acc.bucket()
	if got, want := bucket.NetworkRecvBytesRateAvg.Value, float64(30*1024); got != want {
		t.Fatalf("unexpected total network recv rate: got %f want %f", got, want)
	}
}

func TestMetricsBucketCountsUniqueContainers(t *testing.T) {
	acc := &metricsBucketAccumulator{key: time.Unix(0, 0).UnixMilli()}
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-1",
		ContainerMetrics: types.EventContainerMetricsData{
			CPUTotal: 1000,
		},
	})
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-1",
		ContainerMetrics: types.EventContainerMetricsData{
			CPUTotal: 1000,
		},
	})
	acc.add(types.EventContainerMetricsSchema{
		ContainerID: "container-2",
		ContainerMetrics: types.EventContainerMetricsData{
			CPUTotal: 1000,
		},
	})

	bucket := acc.bucket()
	if got, want := bucket.ContainerCount.Value, float64(2); got != want {
		t.Fatalf("unexpected container count: got %f want %f", got, want)
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
			Type:       types.EventContainerLifecycle,
			EventID:    string(types.ContainerLifecycleWorkerQueueReceive),
			StartTime:  now.Add(-100 * time.Millisecond),
			EndTime:    now.Add(-50 * time.Millisecond),
			DurationMs: 50,
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
	if got, want := summary["scheduler_queue_to_worker_receive_ms"], int64(400); got != want {
		t.Fatalf("unexpected scheduler queue to worker receive summary: got %d want %d", got, want)
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
