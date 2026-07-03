package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
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

	if got, want := len(streams), 4; got != want {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", got, want, streams)
	}
	if got, want := string(streams[0]), "events/workspaces/workspace-123/stubs/stub-456/containers/container-789"; got != want {
		t.Fatalf("unexpected container stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[1]), "events/workspaces/workspace-123/containers/container-789"; got != want {
		t.Fatalf("unexpected container alias stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[2]), "events/workspaces/workspace-123/stubs/stub-456"; got != want {
		t.Fatalf("unexpected stub stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[3]), "events/workspaces/workspace-123"; got != want {
		t.Fatalf("unexpected workspace stream name: got %q want %q", got, want)
	}
}

func TestS2AppNamespaceContainerEventsUseAppNamespaceStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerLifecycle, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		AppID:       "app-789",
		ContainerID: "container-abc",
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/stub-456/containers/container-abc",
		"events/workspaces/workspace-123/containers/container-abc",
		"events/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123",
		"events/workspaces/workspace-123/apps/app-789",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestResolveContainerStreamsUsesExactStreamWithoutExistenceList(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveContainerStreams(context.Background(), "container-789", types.EventQuery{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/workspaces/workspace-123/stubs/stub-456/containers/container-789"}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestResolveContainerStreamsParsesStubScopedContainerIDWithoutPrefixList(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	containerID := "endpoint-" + stubID + "-1717f4fc"

	streams, err := repo.resolveContainerStreams(context.Background(), containerID, types.EventQuery{
		WorkspaceID: "workspace-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/containers/endpoint-5e3e31ff-aef4-40b6-a98d-439268a9832e-1717f4fc"}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestS2StubScopedContainerEventsSkipAliasStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	containerID := "sandbox-" + stubID + "-1717f4fc"

	streams := repo.streamNamesForEvent(types.EventContainerLifecycle, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      stubID,
		ContainerID: containerID,
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/containers/sandbox-5e3e31ff-aef4-40b6-a98d-439268a9832e-1717f4fc",
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e",
		"events/workspaces/workspace-123",
	}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestResolveContainerStreamsUsesAliasForUnscopedContainerIDWithoutPrefixList(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveContainerStreams(context.Background(), "container-789", types.EventQuery{
		WorkspaceID: "workspace-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/workspaces/workspace-123/containers/container-789"}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
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

func TestMergeStubCacheRequiredContentRecordKeepsLatestItem(t *testing.T) {
	merged := map[string]types.CacheRequiredContentItem{}
	writeRecord := func(kind types.CacheContentKind, item types.CacheRequiredContentItem) {
		body, err := json.Marshal(struct {
			Type string                                    `json:"type"`
			Data types.EventStubCacheRequiredContentSchema `json:"data"`
		}{
			Type: types.EventStubCacheRequiredContent,
			Data: types.EventStubCacheRequiredContentSchema{
				Kind:  kind,
				Items: []types.CacheRequiredContentItem{item},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		mergeStubCacheRequiredContentRecord(merged, body)
	}

	writeRecord(types.CacheContentKindClipV2, types.CacheRequiredContentItem{
		Hash:       "sha256:abc",
		RoutingKey: "layer",
		SizeBytes:  100,
	})
	writeRecord(types.CacheContentKindClipV2, types.CacheRequiredContentItem{
		Hash:       "sha256:abc",
		RoutingKey: "layer",
		SizeBytes:  200,
	})

	items := stubCacheRequiredContentItems(merged)
	if got, want := len(items), 1; got != want {
		t.Fatalf("unexpected item count: got %d want %d", got, want)
	}
	if got, want := items[0].Kind, types.CacheContentKindClipV2; got != want {
		t.Fatalf("unexpected kind: got %q want %q", got, want)
	}
	if got, want := items[0].SizeBytes, int64(200); got != want {
		t.Fatalf("unexpected size: got %d want %d", got, want)
	}
}

func TestMergeStubCacheRequiredContentRecordKeepsLatestDiskSnapshotGeneration(t *testing.T) {
	merged := map[string]types.CacheRequiredContentItem{}
	writeRecord := func(items ...types.CacheRequiredContentItem) {
		body, err := json.Marshal(struct {
			Type string                                    `json:"type"`
			Data types.EventStubCacheRequiredContentSchema `json:"data"`
		}{
			Type: types.EventStubCacheRequiredContent,
			Data: types.EventStubCacheRequiredContentSchema{
				Kind:  types.CacheContentKindDiskSnapshot,
				Items: items,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		mergeStubCacheRequiredContentRecord(merged, body)
	}

	writeRecord(
		types.CacheRequiredContentItem{
			Hash:               "old-manifest",
			RoutingKey:         "old-manifest",
			Source:             "durable-disks/redis-data/snapshots/1/manifest.json",
			DiskName:           "redis-data",
			SnapshotGeneration: 1,
		},
		types.CacheRequiredContentItem{
			Hash:               "old-only-chunk",
			RoutingKey:         "old-only-chunk",
			Source:             "durable-disks/redis-data/chunks/old-only-chunk",
			DiskName:           "redis-data",
			SnapshotGeneration: 1,
		},
		types.CacheRequiredContentItem{
			Hash:       "old-unversioned-chunk",
			RoutingKey: "old-unversioned-chunk",
			Source:     "durable-disks/redis-data/chunks/old-unversioned-chunk",
		},
	)
	writeRecord(types.CacheRequiredContentItem{
		Hash:               "new-manifest",
		RoutingKey:         "new-manifest",
		Source:             "durable-disks/redis-data/snapshots/2/manifest.json",
		DiskName:           "redis-data",
		SnapshotGeneration: 2,
	})
	writeRecord(types.CacheRequiredContentItem{
		Hash:               "new-chunk",
		RoutingKey:         "new-chunk",
		Source:             "durable-disks/redis-data/chunks/new-chunk",
		DiskName:           "redis-data",
		SnapshotGeneration: 2,
	})
	writeRecord(types.CacheRequiredContentItem{
		Hash:       "ignored-old-late",
		RoutingKey: "ignored-old-late",
		Source:     "durable-disks/redis-data/chunks/ignored-old-late",
	})

	items := stubCacheRequiredContentItems(merged)
	if got, want := len(items), 2; got != want {
		t.Fatalf("unexpected item count: got %d want %d: %#v", got, want, items)
	}
	for _, item := range items {
		if got, want := item.SnapshotGeneration, int64(2); got != want {
			t.Fatalf("unexpected snapshot generation: got %d want %d", got, want)
		}
		if strings.HasPrefix(item.Hash, "old") || strings.HasPrefix(item.Hash, "ignored") {
			t.Fatalf("stale snapshot item kept: %#v", item)
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
		"events/workspaces/workspace-123/containers/container-789",
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

func TestS2ContainerMetricsFanOutToWorkspaceMachineStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerMetrics, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
		MachineID:   "machine-abc",
	})

	want := s2.StreamName("events/workspaces/workspace-123/machines/machine-abc")
	if !slices.Contains(streams, want) {
		t.Fatalf("machine stream %q missing from %#v", want, streams)
	}
	for _, stream := range streams {
		if strings.Contains(string(stream), "machines/") && !strings.HasPrefix(string(stream), "events/workspaces/workspace-123/") {
			t.Fatalf("machine stream escaped workspace prefix: %q", stream)
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

func TestS2ContainerLogsUseContainerStubLookupBeforeAppNamespaceIndex(t *testing.T) {
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
		"events/logs/workspaces/workspace-123/containers/container-789",
		"events/logs/workspaces/workspace-123/stubs/stub-456",
		"events/workspaces/workspace-123/stubs/stub-456/tasks",
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

func TestS2ContainerLogsFanOutToWorkspaceMachineLogStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventContainerLog, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
		MachineID:   "machine-abc",
	})

	want := s2.StreamName("events/logs/workspaces/workspace-123/machines/machine-abc")
	if !slices.Contains(streams, want) {
		t.Fatalf("machine log stream %q missing from %#v", want, streams)
	}
	for _, stream := range streams {
		if strings.Contains(string(stream), "machines/") && !strings.HasPrefix(string(stream), "events/logs/workspaces/workspace-123/") {
			t.Fatalf("machine log stream escaped workspace log prefix: %q", stream)
		}
	}
}

func TestS2StubScopedContainerLogsSkipAliasStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	containerID := "endpoint-" + stubID + "-1717f4fc"

	streams := repo.streamNamesForEvent(types.EventContainerLog, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      stubID,
		ContainerID: containerID,
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/logs/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/containers/endpoint-5e3e31ff-aef4-40b6-a98d-439268a9832e-1717f4fc",
		"events/logs/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e",
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/tasks",
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

func TestResolveLogStreamsUsesMultiplexedStubTaskStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/workspaces/workspace-123/stubs/stub-456/tasks"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected task log streams: got %q want %q", streams, want)
	}
}

func TestResolveLogStreamsFallsBackToLegacyTaskStreamWithoutStub(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		TaskID:      "task-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/logs/workspaces/workspace-123/tasks/task-123"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected task log streams: got %q want %q", streams, want)
	}
}

func TestResolveMachineLogStreamsUsesAgentAndWorkerStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveLogStreams(types.LogQuery{
		MachineID: "machine-123",
		WorkerID:  "agent-worker-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{
		"events/logs/platform/services/agent/machine-123",
		"events/logs/platform/workers/agent-worker-123",
	}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected machine log streams: got %q want %q", streams, want)
	}
}

func TestResolveWorkspaceMachineLogHistoryUsesMachineStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		MachineID:   "machine-123",
		WorkerID:    "agent-worker-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/logs/workspaces/workspace-123/machines/machine-123"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected machine history stream target: got %q want %q", streams, want)
	}
}

func TestStreamMachineLogsUsesMachineStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.machineLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		MachineID:   "machine-123",
		WorkerID:    "agent-worker-123",
	})

	want := []s2.StreamName{"events/logs/workspaces/workspace-123/machines/machine-123"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected machine stream target: got %q want %q", streams, want)
	}
}

func TestResolveLogStreamsParsesStubScopedContainerIDWithoutPrefixList(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	containerID := "sandbox-" + stubID + "-1717f4fc"

	streams, err := repo.resolveLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		ContainerID: containerID,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/logs/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/containers/sandbox-5e3e31ff-aef4-40b6-a98d-439268a9832e-1717f4fc"}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestResolveLogStreamsUsesAliasForUnscopedContainerIDWithoutPrefixList(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveLogStreams(types.LogQuery{
		WorkspaceID: "workspace-123",
		ContainerID: "container-789",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []s2.StreamName{"events/logs/workspaces/workspace-123/containers/container-789"}
	if len(streams) != len(want) {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", len(streams), len(want), streams)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("unexpected stream at %d: got %q want %q", i, streams[i], want[i])
		}
	}
}

func TestNextTailReadWindow(t *testing.T) {
	// Streams shorter than the chunk size must still be read in full.
	if offset, count := nextTailReadWindow(0, 2, 100); offset != 2 || count != 2 {
		t.Fatalf("unexpected short stream window: got offset=%d count=%d want offset=2 count=2", offset, count)
	}
	if offset, count := nextTailReadWindow(0, 250, 100); offset != 100 || count != 100 {
		t.Fatalf("unexpected first window: got offset=%d count=%d want offset=100 count=100", offset, count)
	}
	// The final (oldest) window must clamp count so already scanned records
	// are not re-read and returned as duplicates.
	if offset, count := nextTailReadWindow(200, 250, 100); offset != 250 || count != 50 {
		t.Fatalf("unexpected final window: got offset=%d count=%d want offset=250 count=50", offset, count)
	}
	if offset, count := nextTailReadWindow(0, 250, 0); offset != 250 || count != 250 {
		t.Fatalf("unexpected zero chunk window: got offset=%d count=%d want offset=250 count=250", offset, count)
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

func TestLogRecordFromS2ExtractsLineFromEventData(t *testing.T) {
	logAt := time.Date(2026, 6, 12, 21, 38, 7, 0, time.UTC)
	eventRepo := &EventClientRepo{}
	event, err := eventRepo.createEventObject(types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		Timestamp:   logAt,
		ContainerID: "container-789",
		StubID:      "stub-456",
		StubType:    "asgi",
		TaskID:      "task-123",
		WorkspaceID: "workspace-123",
		AppID:       "app-123",
		WorkerID:    "worker-1",
		Stream:      "stdout",
		Line:        "Starting gunicorn 22.0.0",
		PID:         123,
		ProcessArgs: []string{"python3", "-c", "print('hi')"},
		ProcessCwd:  "/workspace",
		ProcessSeq:  7,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	logRecord, ok := logRecordFromS2(s2.SequencedRecord{SeqNum: 7, Timestamp: uint64(logAt.UnixMilli()), Body: body})
	if !ok {
		t.Fatal("expected container log record to be accepted")
	}
	if got, want := logRecord.Message, "Starting gunicorn 22.0.0"; got != want {
		t.Fatalf("unexpected log message: got %q want %q", got, want)
	}
	if got, want := logRecord.Stream, "stdout"; got != want {
		t.Fatalf("unexpected log stream: got %q want %q", got, want)
	}
	if got, want := logRecord.ContainerID, "container-789"; got != want {
		t.Fatalf("unexpected container id: got %q want %q", got, want)
	}
	if got, want := logRecord.TaskID, "task-123"; got != want {
		t.Fatalf("unexpected task id: got %q want %q", got, want)
	}
	if got, want := logRecord.PID, int32(123); got != want {
		t.Fatalf("unexpected pid: got %d want %d", got, want)
	}
	if got, want := logRecord.ProcessArgs, []string{"python3", "-c", "print('hi')"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected process args: got %#v want %#v", got, want)
	}
	if got, want := logRecord.ProcessCwd, "/workspace"; got != want {
		t.Fatalf("unexpected process cwd: got %q want %q", got, want)
	}
	if got, want := logRecord.ProcessSeq, uint64(7); got != want {
		t.Fatalf("unexpected process seq: got %d want %d", got, want)
	}
	if !logRecord.Timestamp.Equal(logAt) {
		t.Fatalf("unexpected timestamp: got %s want %s", logRecord.Timestamp, logAt)
	}

	// Records without a log line (e.g. malformed or non-log events) are skipped.
	if _, ok := logRecordFromS2(s2.SequencedRecord{Body: []byte(`{"type":"container.log","data":{}}`)}); ok {
		t.Fatal("expected record without a line to be rejected")
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

func TestS2TaskEventsUseWorkspaceAndAppNamespaceStreams(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventTaskCreated, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/stub-456/tasks",
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
		"events/workspaces/workspace-123/stubs/stub-456/tasks",
		"events/workspaces/workspace-123/containers/container-789",
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

func TestS2TaskUpdateEventsSkipAliasForStubScopedContainer(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	containerID := "pod-" + stubID + "-1717f4fc"

	streams := repo.streamNamesForEvent(types.EventTaskUpdated, eventMetadata{
		WorkspaceID: "workspace-123",
		StubID:      stubID,
		ContainerID: containerID,
		TaskID:      "task-123",
		AppID:       "app-123",
	})

	want := []s2.StreamName{
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/tasks",
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e/containers/pod-5e3e31ff-aef4-40b6-a98d-439268a9832e-1717f4fc",
		"events/workspaces/workspace-123/stubs/5e3e31ff-aef4-40b6-a98d-439268a9832e",
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

func TestS2TaskEventsWithoutStubFallBackToWorkspaceStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventTaskCreated, eventMetadata{
		WorkspaceID: "workspace-123",
		TaskID:      "task-123",
	})

	want := []s2.StreamName{"events/workspaces/workspace-123"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected task stream fallback: got %q want %q", streams, want)
	}
}

func TestResolveEventHistoryStreamsUsesStubTaskStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams, err := repo.resolveEventHistoryStreams(context.Background(), types.EventQuery{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-123",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []s2.StreamName{"events/workspaces/workspace-123/stubs/stub-456/tasks"}
	if !reflect.DeepEqual(streams, want) {
		t.Fatalf("unexpected task history streams: got %q want %q", streams, want)
	}

	legacyStreams, err := repo.resolveEventHistoryStreams(context.Background(), types.EventQuery{
		TaskID: "task-123",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantLegacy := []s2.StreamName{"events/tasks/task-123"}
	if !reflect.DeepEqual(legacyStreams, wantLegacy) {
		t.Fatalf("unexpected legacy task history streams: got %q want %q", legacyStreams, wantLegacy)
	}
}

func sequencedRecordForEvent(t *testing.T, eventType string, schemaVersion string, schema interface{}) s2.SequencedRecord {
	t.Helper()
	eventRepo := &EventClientRepo{}
	event, err := eventRepo.createEventObject(eventType, schemaVersion, schema)
	if err != nil {
		t.Fatal(err)
	}
	repo := &S2EventRepository{streamPrefix: "events"}
	record, _, err := repo.appendRecordForEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	return s2.SequencedRecord{
		Headers: record.Headers,
		Body:    record.Body,
	}
}

func TestEventRecordHeadersSkipDemultiplexesByTaskHeader(t *testing.T) {
	recordA := sequencedRecordForEvent(t, types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-789",
		TaskID:      "task-a",
		Line:        "from task a",
	})
	recordB := sequencedRecordForEvent(t, types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		ContainerID: "container-790",
		TaskID:      "task-b",
		Line:        "from task b",
	})

	query := types.EventQuery{TaskID: "task-a"}
	if eventRecordHeadersSkip(recordA, query) {
		t.Fatal("expected task-a record to pass the header pre-filter")
	}
	if !eventRecordHeadersSkip(recordB, query) {
		t.Fatal("expected task-b record to be skipped on header inspection alone")
	}

	// Legacy records without headers must fall through to body filtering.
	if eventRecordHeadersSkip(s2.SequencedRecord{Body: recordB.Body}, query) {
		t.Fatal("expected headerless record to fall through to body filtering")
	}
}

func TestEventRecordHeadersSkipFiltersByTypeHeader(t *testing.T) {
	logRecord := sequencedRecordForEvent(t, types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-a",
		Line:        "noise",
	})

	excludeLogs := types.EventQuery{TaskID: "task-a", ExcludeEventTypes: []string{types.EventContainerLog}}
	if !eventRecordHeadersSkip(logRecord, excludeLogs) {
		t.Fatal("expected excluded log record to be skipped via type header")
	}

	onlyTaskEvents := types.EventQuery{TaskID: "task-a", EventTypes: []string{"task.*"}}
	if !eventRecordHeadersSkip(logRecord, onlyTaskEvents) {
		t.Fatal("expected log record to be skipped when only task.* types are requested")
	}

	allowLogs := types.EventQuery{TaskID: "task-a", EventTypes: []string{types.EventContainerLog}}
	if eventRecordHeadersSkip(logRecord, allowLogs) {
		t.Fatal("expected log record to pass when container.log is explicitly requested")
	}
}

func TestLogRecordHeadersSkipDemultiplexesByTaskHeader(t *testing.T) {
	logRecord := sequencedRecordForEvent(t, types.EventContainerLog, types.EventContainerLogSchemaVersion, types.EventContainerLogSchema{
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
		TaskID:      "task-a",
		Line:        "hello",
	})
	taskEventRecord := sequencedRecordForEvent(t, types.EventTaskUpdated, types.EventTaskSchemaVersion, types.EventTaskSchema{
		ID:          "task-a",
		WorkspaceID: "workspace-123",
		StubID:      "stub-456",
	})

	query := types.LogQuery{TaskID: "task-a"}
	if logRecordHeadersSkip(logRecord, query) {
		t.Fatal("expected matching task log record to pass the header pre-filter")
	}
	if !logRecordHeadersSkip(taskEventRecord, query) {
		t.Fatal("expected non-log record in the multiplexed stream to be skipped via type header")
	}
	if !logRecordHeadersSkip(logRecord, types.LogQuery{TaskID: "task-b"}) {
		t.Fatal("expected other task's log record to be skipped via task_id header")
	}
	if logRecordHeadersSkip(s2.SequencedRecord{Body: logRecord.Body}, query) {
		t.Fatal("expected headerless record to fall through to body filtering")
	}
}

func TestMetricsRecordMatchesAppScopedQuery(t *testing.T) {
	query := types.EventQuery{AppID: "app-1"}
	matchingPayload := types.EventContainerMetricsSchema{AppID: "app-1"}
	otherPayload := types.EventContainerMetricsSchema{AppID: "app-2"}

	if !metricsRecordMatchesQuery(s2.SequencedRecord{}, matchingPayload, query) {
		t.Fatal("expected matching payload app id to pass")
	}
	if metricsRecordMatchesQuery(s2.SequencedRecord{}, types.EventContainerMetricsSchema{}, query) {
		t.Fatal("expected app-scoped query to reject records without app id")
	}
	if metricsRecordMatchesQuery(s2.SequencedRecord{}, otherPayload, query) {
		t.Fatal("expected mismatched payload app id to be rejected")
	}

	matchingHeader := s2.SequencedRecord{Headers: []s2.Header{s2.NewHeader("app_id", "app-1")}}
	if !metricsRecordMatchesQuery(matchingHeader, types.EventContainerMetricsSchema{}, query) {
		t.Fatal("expected matching app header to pass legacy payload")
	}
	if metricsRecordMatchesQuery(matchingHeader, otherPayload, query) {
		t.Fatal("expected mismatched payload to reject even with a matching header")
	}

	otherHeader := s2.SequencedRecord{Headers: []s2.Header{s2.NewHeader("app_id", "app-2")}}
	if metricsRecordMatchesQuery(otherHeader, matchingPayload, query) {
		t.Fatal("expected mismatched app header to be rejected")
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

	workspaceStreams := repo.streamNamesForEvent(types.EventPlatformLog, eventMetadata{
		WorkspaceID: "workspace-123",
		ServiceName: "agent",
		InstanceID:  "machine-123",
	})
	if got, want := workspaceStreams, []s2.StreamName{
		"events/logs/platform/services/agent/machine-123",
		"events/logs/workspaces/workspace-123",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected workspace platform log streams: got %q want %q", got, want)
	}
}

func TestScopedS2EventRepositoryRoutesOnlyWorkspacePrefixes(t *testing.T) {
	repo := &ScopedS2EventRepository{
		streamPrefix: "events",
		targets: []scopedS2EventTarget{
			{name: "logs", prefix: "events/logs/workspaces/workspace-123"},
			{name: "events", prefix: "events/workspaces/workspace-123"},
		},
	}

	if target := repo.targetForStream("events/logs/platform/workers/worker-123"); target != nil {
		t.Fatalf("expected platform log stream to be outside scoped targets, got %q", target.name)
	}
	if target := repo.targetForStream("events/platform/cache"); target != nil {
		t.Fatalf("expected platform event stream to be outside scoped targets, got %q", target.name)
	}
	if target := repo.targetForStream("events/logs/workspaces/workspace-123/stubs/stub-123"); target == nil || target.name != "logs" {
		t.Fatalf("expected workspace log stream to use logs target, got %#v", target)
	}
	if target := repo.targetForStream("events/logs/workspaces/workspace-123/machines/machine-123"); target == nil || target.name != "logs" {
		t.Fatalf("expected workspace machine log stream to use logs target, got %#v", target)
	}
	if target := repo.targetForStream("events/workspaces/workspace-123/stubs/stub-123"); target == nil || target.name != "events" {
		t.Fatalf("expected workspace event stream to use events target, got %#v", target)
	}
	if target := repo.targetForStream("events/workspaces/workspace-123/machines/machine-123"); target == nil || target.name != "events" {
		t.Fatalf("expected workspace machine event stream to use events target, got %#v", target)
	}
	if target := repo.targetForStream("events/workspaces/workspace-1234/stubs/stub-123"); target != nil {
		t.Fatalf("expected adjacent workspace prefix to be outside scoped targets, got %q", target.name)
	}
}

func TestS2PlatformLogRecordsDecodeForWorkspaceLogs(t *testing.T) {
	eventRepo := &EventClientRepo{}
	logAt := time.Date(2026, 6, 6, 15, 0, 0, 0, time.UTC)
	event, err := eventRepo.createEventObject(types.EventPlatformLog, types.EventPlatformLogSchemaVersion, types.EventPlatformLogSchema{
		Timestamp:   logAt,
		WorkspaceID: "workspace-123",
		PoolName:    "private-dev",
		MachineID:   "machine-123",
		Service:     types.AgentTelemetrySourceWorker,
		InstanceID:  "worker-123",
		WorkerID:    "worker-123",
		Stream:      types.EventLogStreamStderr,
		Line:        "worker ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	record, ok := logRecordFromS2(s2.SequencedRecord{
		SeqNum:    7,
		Timestamp: uint64(logAt.UnixMilli()),
		Body:      body,
	})
	if !ok {
		t.Fatal("expected platform log record to decode")
	}
	if record.Message != "worker ready" || record.Stream != types.EventLogStreamStderr || record.WorkspaceID != "workspace-123" || record.MachineID != "machine-123" || record.WorkerID != "worker-123" {
		t.Fatalf("unexpected log record: %#v", record)
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
			name:      "compute route",
			eventType: types.EventComputeRoute,
			metadata:  eventMetadata{WorkspaceID: "workspace-1", WorkerID: "worker-1", RouteID: "route-1"},
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
		AppID:       "app-1",
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
		metadata.AppID != "app-1" ||
		metadata.TaskID != "task-1" ||
		metadata.WorkerID != "worker-1" {
		t.Fatalf("metadata did not round trip: %#v", metadata)
	}
}

func TestComputeEventMetadataExtensionsRoundTrip(t *testing.T) {
	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventComputeRoute, types.EventComputeSchemaVersion, types.EventComputeSchema{
		WorkspaceID: "workspace-1",
		PoolName:    "private-gpu",
		MachineID:   "machine-1",
		WorkerID:    "worker-1",
		ContainerID: "container-1",
		RouteID:     "route-1",
		Action:      types.EventComputeActionRouteStatusUpdated,
		Status:      types.BackendRouteStateReady,
	})
	if err != nil {
		t.Fatal(err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	if metadata.WorkspaceID != "workspace-1" ||
		metadata.PoolName != "private-gpu" ||
		metadata.MachineID != "machine-1" ||
		metadata.WorkerID != "worker-1" ||
		metadata.ContainerID != "container-1" ||
		metadata.RouteID != "route-1" {
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
	err := fmt.Errorf("append stream: %w", &s2.S2Error{
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

func TestS2ReadEmptyRecognizesStreamNotFoundByCodeAndMessage(t *testing.T) {
	codeErr := fmt.Errorf("read stream: %w", &s2.S2Error{
		Code:   "stream_not_found",
		Status: 400,
		Origin: "server",
	})
	if !isS2ReadEmpty(codeErr) {
		t.Fatal("expected stream_not_found code to be treated as empty")
	}

	messageErr := fmt.Errorf("read stream: stream does not exist")
	if !isS2ReadEmpty(messageErr) {
		t.Fatal("expected stream missing message to be treated as empty")
	}

	genericNotFound := fmt.Errorf("read stream: %w", &s2.S2Error{
		Code:   "not_found",
		Status: 404,
		Origin: "server",
	})
	if isS2ReadEmpty(genericNotFound) {
		t.Fatal("generic not_found should not be treated as an empty stream read")
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
