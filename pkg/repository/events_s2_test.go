package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
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

func mergeScopedRequiredContentForTest(t *testing.T, state *stubCacheRequiredContentState, schema types.EventStubCacheRequiredContentSchema) {
	t.Helper()
	body, err := json.Marshal(struct {
		Type string                                    `json:"type"`
		Data types.EventStubCacheRequiredContentSchema `json:"data"`
	}{Type: types.EventStubCacheRequiredContent, Data: schema})
	if err != nil {
		t.Fatal(err)
	}
	mergeStubCacheRequiredContentRecordIntoState(state, body)
}

func scopedRequiredContentSchemaForTest(t *testing.T, scope, revisionID string, generation int64, parts [][]types.CacheRequiredContentItem) []types.EventStubCacheRequiredContentSchema {
	t.Helper()
	all := make([]types.CacheRequiredContentItem, 0)
	for _, part := range parts {
		all = append(all, part...)
	}
	canonical, digest, totalBytes, err := types.CanonicalCacheRequiredContentSet(all)
	if err != nil {
		t.Fatal(err)
	}
	base := types.EventStubCacheRequiredContentSchema{
		Scope: scope, RevisionGeneration: generation, RevisionID: revisionID, Replace: true,
		PartCount: len(parts), SetDigest: digest, ItemCount: len(canonical), TotalBytes: totalBytes,
	}
	records := make([]types.EventStubCacheRequiredContentSchema, 0, len(parts)+1)
	for index, part := range parts {
		record := base
		record.PartIndex, record.Items = index, part
		records = append(records, record)
	}
	commit := base
	commit.Commit = true
	records = append(records, commit)
	return records
}

func TestScopedStateRequiredContentCommitsCompleteMultipartRevisionAtomically(t *testing.T) {
	state := &stubCacheRequiredContentState{items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{}}
	scope := "bd9a783d-9857-4d1d-ae42-62629f7ecf89"
	manifest := types.CacheRequiredContentItem{
		Kind: types.CacheContentKindStateManifest, Hash: "parent-manifest", RoutingKey: "parent-manifest",
		Source: "state-volumes/generation-parent/manifest.json", VolumeID: scope, GenerationID: "4f96d83a-0eb8-4a9a-afd4-fcae79069302", SizeBytes: 64,
	}
	chunk := types.CacheRequiredContentItem{
		Kind: types.CacheContentKindStateChunk, Hash: "parent-chunk", RoutingKey: "parent-chunk",
		Source: "state-volumes/chunks/parent-chunk", VolumeID: scope, GenerationID: "4f96d83a-0eb8-4a9a-afd4-fcae79069302", SizeBytes: 4096,
	}
	revision1 := scopedRequiredContentSchemaForTest(t, scope, "86dd770a-1adc-4e2e-9677-4acbc7601ef9", 1,
		[][]types.CacheRequiredContentItem{{manifest}, {chunk}})
	// Commit-before-parts and out-of-order parts retain no partial set.
	mergeScopedRequiredContentForTest(t, state, revision1[2])
	mergeScopedRequiredContentForTest(t, state, revision1[1])
	if got := state.requiredContentItems(); len(got) != 0 {
		t.Fatalf("incomplete revision became visible: %#v", got)
	}
	mergeScopedRequiredContentForTest(t, state, revision1[0])
	if got := state.requiredContentItems(); len(got) != 2 {
		t.Fatalf("complete revision item count = %d, want 2: %#v", len(got), got)
	}

	newManifest := manifest
	newManifest.Hash, newManifest.RoutingKey, newManifest.GenerationID = "new-manifest", "new-manifest", "4d4740d2-6af4-4d1b-8357-5981d42e5886"
	revision2 := scopedRequiredContentSchemaForTest(t, scope, "09737b39-ae66-4fa5-93da-d6ca4b6d60c9", 2,
		[][]types.CacheRequiredContentItem{{newManifest}, {chunk}})
	mergeScopedRequiredContentForTest(t, state, revision2[0])
	mergeScopedRequiredContentForTest(t, state, revision2[2])
	// Missing part 1 leaves the prior committed revision intact.
	byHash := map[string]bool{}
	for _, item := range state.requiredContentItems() {
		byHash[item.Hash] = true
	}
	if !byHash["parent-manifest"] || byHash["new-manifest"] {
		t.Fatalf("incomplete replacement displaced prior revision: %#v", byHash)
	}
	mergeScopedRequiredContentForTest(t, state, revision2[1])
	byHash = map[string]bool{}
	for _, item := range state.requiredContentItems() {
		byHash[item.Hash] = true
	}
	if byHash["parent-manifest"] || !byHash["new-manifest"] || !byHash["parent-chunk"] {
		t.Fatalf("complete replacement did not atomically swap the scope: %#v", byHash)
	}
}

func TestScopedStateRequiredContentRejectsMixedAndConflictingPartsAndCommitsTombstone(t *testing.T) {
	state := &stubCacheRequiredContentState{items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{}}
	scope := "bd9a783d-9857-4d1d-ae42-62629f7ecf89"
	otherScope := "e7fb96a2-c8d7-484d-9673-d0eecdfca146"
	item := func(hash string, kind types.CacheContentKind, volume string) types.CacheRequiredContentItem {
		return types.CacheRequiredContentItem{Kind: kind, Hash: hash, RoutingKey: hash, Source: "objects/" + hash,
			VolumeID: volume, GenerationID: "4f96d83a-0eb8-4a9a-afd4-fcae79069302", SizeBytes: 1}
	}
	base := scopedRequiredContentSchemaForTest(t, scope, "86dd770a-1adc-4e2e-9677-4acbc7601ef9", 1,
		[][]types.CacheRequiredContentItem{{item("base-manifest", types.CacheContentKindStateManifest, scope)}})
	for _, record := range base {
		mergeScopedRequiredContentForTest(t, state, record)
	}
	fork := scopedRequiredContentSchemaForTest(t, otherScope, "4d4740d2-6af4-4d1b-8357-5981d42e5886", 1,
		[][]types.CacheRequiredContentItem{{item("fork-manifest", types.CacheContentKindStateManifest, otherScope)}})
	for _, record := range fork {
		mergeScopedRequiredContentForTest(t, state, record)
	}

	revision2 := scopedRequiredContentSchemaForTest(t, scope, "09737b39-ae66-4fa5-93da-d6ca4b6d60c9", 2,
		[][]types.CacheRequiredContentItem{{item("new-manifest", types.CacheContentKindStateManifest, scope)}, {item("new-chunk", types.CacheContentKindStateChunk, scope)}})
	revision3 := scopedRequiredContentSchemaForTest(t, scope, "da1e4117-7e39-43cf-89cc-9185bd24a46f", 3,
		[][]types.CacheRequiredContentItem{{item("other-manifest", types.CacheContentKindStateManifest, scope)}, {item("other-chunk", types.CacheContentKindStateChunk, scope)}})
	mergeScopedRequiredContentForTest(t, state, revision2[0])
	mergeScopedRequiredContentForTest(t, state, revision3[1])
	mergeScopedRequiredContentForTest(t, state, revision2[2])
	mergeScopedRequiredContentForTest(t, state, revision3[2])
	if got := state.requiredContentItems(); len(got) != 2 {
		t.Fatalf("mixed incomplete revisions changed committed scopes: %#v", got)
	}
	// A conflicting duplicate part poisons only its staged revision.
	conflict := revision2[0]
	conflict.Items = []types.CacheRequiredContentItem{item("forged-manifest", types.CacheContentKindStateManifest, scope)}
	mergeScopedRequiredContentForTest(t, state, conflict)
	mergeScopedRequiredContentForTest(t, state, revision2[1])
	if got := state.requiredContentItems(); len(got) != 2 {
		t.Fatalf("conflicting duplicate part changed committed scopes: %#v", got)
	}

	_, emptyDigest, _, err := types.CanonicalCacheRequiredContentSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	mergeScopedRequiredContentForTest(t, state, types.EventStubCacheRequiredContentSchema{
		Scope: scope, RevisionGeneration: 4, RevisionID: "b948fa30-76f9-45ef-98c7-04f2986f8fc1",
		Replace: true, Commit: true, Tombstone: true, SetDigest: emptyDigest,
	})
	items := state.requiredContentItems()
	if len(items) != 1 || items[0].Hash != "fork-manifest" {
		t.Fatalf("tombstone removed an independent fork scope or retained its own: %#v", items)
	}
}

func TestReadStubCacheRequiredContentRefusesPartialScanAndResumesToTail(t *testing.T) {
	repo := &S2EventRepository{
		streamPrefix:     "events",
		stubCacheContent: map[s2.StreamName]*stubCacheRequiredContentState{},
	}
	repo.readRecords = func(_ context.Context, _ s2.StreamName, seqNum, count uint64) (*s2.ReadBatch, error) {
		if seqNum == 0 {
			if count != 2 {
				t.Fatalf("read count = %d, want bounded count 2", count)
			}
			return &s2.ReadBatch{Records: []s2.SequencedRecord{
				{SeqNum: 0, Body: []byte(`{"type":"ignored"}`)},
				{SeqNum: 1, Body: []byte(`{"type":"ignored"}`)},
			}}, nil
		}
		if seqNum != 2 {
			t.Fatalf("resume sequence = %d, want 2", seqNum)
		}
		return &s2.ReadBatch{}, nil
	}

	items, err := repo.readStubCacheRequiredContent(context.Background(), "workspace", "stub", 2)
	if items != nil {
		t.Fatalf("partial read returned items: %#v", items)
	}
	var incomplete *ErrStubCacheRequiredContentIncomplete
	if !errors.As(err, &incomplete) {
		t.Fatalf("partial read error = %v, want ErrStubCacheRequiredContentIncomplete", err)
	}
	if incomplete.RecordsRead != 2 || incomplete.NextSeqNum != 2 {
		t.Fatalf("incomplete metadata = %#v, want records=2 next=2", incomplete)
	}

	items, err = repo.readStubCacheRequiredContent(context.Background(), "workspace", "stub", 2)
	if err != nil {
		t.Fatalf("resumed read: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("resumed complete set = %#v, want empty", items)
	}
}

func TestEventClientRepoRoundTripsLargeScopedStateRevisionThroughS2WriterAndReader(t *testing.T) {
	const (
		scope      = "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc"
		revisionID = "12614665-148e-405b-9cc3-6e1b06f659d9"
	)
	items := make([]types.CacheRequiredContentItem, 0, types.CacheRequiredContentMaxItemsPerPart+1)
	for index := 0; index < types.CacheRequiredContentMaxItemsPerPart+1; index++ {
		kind := types.CacheContentKindStateChunk
		if index == 0 {
			kind = types.CacheContentKindStateManifest
		}
		hash := fmt.Sprintf("sha256:%064x", index+1)
		items = append(items, types.CacheRequiredContentItem{
			Hash: hash, RoutingKey: hash, ExpectedHash: hash, SizeBytes: int64(index + 1),
			Kind: kind, VolumeID: scope, GenerationID: revisionID,
		})
	}
	records, err := types.BuildScopedCacheRequiredContentRevision(
		"workspace", "stub", "node", scope, 9, revisionID, items, false,
	)
	if err != nil {
		t.Fatal(err)
	}

	state := &stubCacheRequiredContentState{
		items:  map[string]types.CacheRequiredContentItem{},
		scopes: map[string]*stubCacheRequiredContentScope{},
	}
	var written []types.EventStubCacheRequiredContentSchema
	s2Sink := &S2EventRepository{
		streamPrefix: "events",
		appendRecords: func(stream s2.StreamName, appendRecords []s2.AppendRecord) error {
			if got, want := string(stream), "events/workspaces/workspace/stubs/stub/cache"; got != want {
				t.Fatalf("unexpected S2 stream: got %q want %q", got, want)
			}
			for _, record := range appendRecords {
				var envelope struct {
					Data types.EventStubCacheRequiredContentSchema `json:"data"`
				}
				if err := json.Unmarshal(record.Body, &envelope); err != nil {
					t.Fatal(err)
				}
				written = append(written, envelope.Data)
				mergeStubCacheRequiredContentRecordIntoState(state, record.Body)
			}
			return nil
		},
	}
	client := &EventClientRepo{storageSinks: []eventSink{s2Sink}}
	for _, record := range records {
		if err := client.PushStubCacheRequiredContent(record); err != nil {
			t.Fatal(err)
		}
	}

	if got, want := len(written), len(records); got != want {
		t.Fatalf("unexpected S2 record count: got %d want %d", got, want)
	}
	for index, record := range written {
		if record.ItemCount != len(items) || record.PartCount != 2 || record.TotalBytes != records[0].TotalBytes || record.SetDigest != records[0].SetDigest {
			t.Fatalf("S2 record %d changed aggregate multipart metadata: %+v", index, record)
		}
	}
	if got := state.requiredContentItems(); len(got) != len(items) {
		t.Fatalf("committed S2 reader item count = %d, want %d", len(got), len(items))
	}
}

func TestEventClientRepoReturnsDeletionPendingMidScopedRevision(t *testing.T) {
	const (
		scope      = "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc"
		revisionID = "12614665-148e-405b-9cc3-6e1b06f659d9"
	)
	items := make([]types.CacheRequiredContentItem, 0, types.CacheRequiredContentMaxItemsPerPart+1)
	for index := 0; index < types.CacheRequiredContentMaxItemsPerPart+1; index++ {
		kind := types.CacheContentKindStateChunk
		if index == 0 {
			kind = types.CacheContentKindStateManifest
		}
		hash := fmt.Sprintf("sha256:%064x", index+1)
		items = append(items, types.CacheRequiredContentItem{
			Hash: hash, RoutingKey: hash, SizeBytes: 1, Kind: kind,
			VolumeID: scope, GenerationID: revisionID,
		})
	}
	records, err := types.BuildScopedCacheRequiredContentRevision(
		"workspace", "stub", "node", scope, 1, revisionID, items, false,
	)
	if err != nil {
		t.Fatal(err)
	}

	state := &stubCacheRequiredContentState{
		items:  map[string]types.CacheRequiredContentItem{},
		scopes: map[string]*stubCacheRequiredContentScope{},
	}
	appendCalls := 0
	s2Sink := &S2EventRepository{
		streamPrefix: "events",
		appendRecords: func(_ s2.StreamName, appendRecords []s2.AppendRecord) error {
			appendCalls++
			if appendCalls == 2 {
				return &s2.S2Error{Code: "stream_deletion_pending", Status: 409, Origin: "server"}
			}
			for _, record := range appendRecords {
				mergeStubCacheRequiredContentRecordIntoState(state, record.Body)
			}
			return nil
		},
	}
	client := &EventClientRepo{storageSinks: []eventSink{s2Sink}}
	if err := client.PushStubCacheRequiredContent(records[0]); err != nil {
		t.Fatal(err)
	}
	err = client.PushStubCacheRequiredContent(records[1])
	if err == nil || !isS2EventStreamDeletionPending(err) {
		t.Fatalf("expected retryable stream_deletion_pending error, got %v", err)
	}
	if got := state.requiredContentItems(); len(got) != 0 {
		t.Fatalf("incomplete multipart revision became visible after failed S2 append: %#v", got)
	}
	if appendCalls != 2 {
		t.Fatalf("unexpected append calls after synchronous failure: %d", appendCalls)
	}
}

func TestEventClientRepoRejectsScopedStateRevisionWithoutDurableSyncSink(t *testing.T) {
	records, err := types.BuildScopedCacheRequiredContentRevision(
		"workspace", "stub", "node", "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc", 1,
		"12614665-148e-405b-9cc3-6e1b06f659d9", nil, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &EventClientRepo{}
	if client.HasDurableScopedStateSink() {
		t.Fatal("empty event client unexpectedly advertised a durable state sink")
	}
	if err := client.PushStubCacheRequiredContent(records[0]); !errors.Is(err, ErrEventWriteUnsupported) {
		t.Fatalf("scoped tombstone without durable sink error = %v, want %v", err, ErrEventWriteUnsupported)
	}
}

func TestUnscopedStateRequiredContentIsRejected(t *testing.T) {
	merged := map[string]types.CacheRequiredContentItem{}
	writeRecord := func(kind types.CacheContentKind, items ...types.CacheRequiredContentItem) {
		body, err := json.Marshal(struct {
			Type string                                    `json:"type"`
			Data types.EventStubCacheRequiredContentSchema `json:"data"`
		}{
			Type: types.EventStubCacheRequiredContent,
			Data: types.EventStubCacheRequiredContentSchema{
				Kind:  kind,
				Items: items,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		mergeStubCacheRequiredContentRecord(merged, body)
	}

	writeRecord(types.CacheContentKindStateManifest,
		types.CacheRequiredContentItem{
			Hash:         "parent-manifest",
			RoutingKey:   "parent-manifest",
			Source:       "state-volumes/generation-parent/manifest.json",
			VolumeID:     "root",
			GenerationID: "generation-parent",
		},
	)
	writeRecord(types.CacheContentKindStateChunk,
		types.CacheRequiredContentItem{
			Hash:         "parent-chunk",
			RoutingKey:   "parent-chunk",
			Source:       "state-volumes/chunks/parent-chunk",
			VolumeID:     "root",
			GenerationID: "generation-parent",
		},
	)
	writeRecord(types.CacheContentKindStateManifest,
		types.CacheRequiredContentItem{
			Hash:         "child-manifest",
			RoutingKey:   "child-manifest",
			Source:       "state-volumes/generation-child/manifest.json",
			VolumeID:     "root",
			GenerationID: "generation-child",
		},
	)
	writeRecord(types.CacheContentKindStateChunk, types.CacheRequiredContentItem{
		Hash:         "child-chunk",
		RoutingKey:   "child-chunk",
		Source:       "state-volumes/chunks/child-chunk",
		VolumeID:     "root",
		GenerationID: "generation-child",
	})

	if items := stubCacheRequiredContentItems(merged); len(items) != 0 {
		t.Fatalf("unscoped additive block state was accepted: %#v", items)
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

func TestS2PlatformCacheMachineEventsFanOutToWorkspaceMachineStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}

	streams := repo.streamNamesForEvent(types.EventPlatformCache, eventMetadata{
		WorkspaceID: "workspace-123",
		MachineID:   "machine-456",
	})

	if got, want := len(streams), 2; got != want {
		t.Fatalf("unexpected stream count: got %d want %d: %#v", got, want, streams)
	}
	if got, want := string(streams[0]), "events/platform/cache"; got != want {
		t.Fatalf("unexpected platform cache stream name: got %q want %q", got, want)
	}
	if got, want := string(streams[1]), "events/workspaces/workspace-123/machines/machine-456"; got != want {
		t.Fatalf("unexpected machine stream name: got %q want %q", got, want)
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

func TestResolveLogStreamsHonorsExplicitTaskAndContainerScopes(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	tests := []struct {
		name  string
		query types.LogQuery
		want  s2.StreamName
	}{
		{
			name:  "task prefers complete app aggregate",
			query: types.LogQuery{ObjectType: types.GatewayObjectTypeTask, WorkspaceID: "workspace-123", StubID: "stub-456", AppID: "app-789", TaskID: "task-123", ContainerID: "container-abc"},
			want:  "events/logs/workspaces/workspace-123/apps/app-789",
		},
		{
			name:  "container keeps container stream with task filter",
			query: types.LogQuery{ObjectType: types.GatewayObjectTypeContainer, WorkspaceID: "workspace-123", StubID: "stub-456", TaskID: "task-123", ContainerID: "container-abc"},
			want:  "events/logs/workspaces/workspace-123/stubs/stub-456/containers/container-abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streams, err := repo.resolveLogStreams(tt.query)
			if err != nil {
				t.Fatal(err)
			}
			if len(streams) != 1 || streams[0] != tt.want {
				t.Fatalf("streams = %q, want [%q]", streams, tt.want)
			}
		})
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
	// A historical end position skips records appended later instead of
	// spending the 50k scan budget walking backward from the live tail.
	start := logTailOffsetBeforeSeq(185443, 125443)
	if offset, count := nextTailReadWindow(start, 185443, 100); start != 60000 || offset != 60100 || count != 100 {
		t.Fatalf("unexpected positioned window: start=%d offset=%d count=%d", start, offset, count)
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
			name:      "pool scoped event",
			eventType: types.EventWorkerLifecycle,
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

func TestComputeHeartbeatUsesDedicatedPoolMetricsStream(t *testing.T) {
	repo := &S2EventRepository{streamPrefix: "events"}
	tests := []struct {
		eventType, action string
	}{
		{types.EventComputeMachine, types.EventComputeActionMachineHeartbeat},
		{types.EventComputePool, types.EventComputeActionPoolHeartbeat},
	}
	for _, test := range tests {
		streams := repo.streamNamesForEvent(test.eventType, eventMetadata{
			WorkspaceID: "workspace-1",
			MachineID:   "machine-1",
			Action:      test.action,
		})

		if !slices.Contains(streams, s2.StreamName("events/workspaces/workspace-1/compute/metrics")) {
			t.Fatalf("pool metrics stream missing for %q from %#v", test.action, streams)
		}
		if slices.Contains(streams, s2.StreamName("events/workspaces/workspace-1/compute")) {
			t.Fatalf("heartbeat %q leaked into lifecycle history: %#v", test.action, streams)
		}
	}
}

func TestS2MetricsScanBudgetReportsExhaustion(t *testing.T) {
	budget := s2MetricsScanBudget(s2ReadScanLimit).consume(123)
	if scanned, truncated := budget.state(); scanned != 123 || truncated {
		t.Fatalf("partial budget state = (%d, %v), want (123, false)", scanned, truncated)
	}

	budget = budget.consume(s2ReadScanLimit)
	if scanned, truncated := budget.state(); scanned != s2ReadScanLimit || !truncated {
		t.Fatalf("exhausted budget state = (%d, %v), want (%d, true)", scanned, truncated, s2ReadScanLimit)
	}
}

func TestReserveS2MetricsReadIsAtomic(t *testing.T) {
	remaining := atomic.Int64{}
	remaining.Store(s2ReadScanLimit)
	reservations := make(chan uint64, 100)

	for range 100 {
		go func() {
			reservations <- reserveS2MetricsRead(&remaining)
		}()
	}

	var reserved uint64
	for range 100 {
		reservation := <-reservations
		if reservation > s2MetricsReadLimit {
			t.Fatalf("reservation = %d, want at most %d", reservation, s2MetricsReadLimit)
		}
		reserved += reservation
	}
	if reserved != s2ReadScanLimit || remaining.Load() != 0 {
		t.Fatalf("reserved = %d, remaining = %d; want %d, 0", reserved, remaining.Load(), s2ReadScanLimit)
	}
}

func TestPoolMetricsBucketKeyUsesContainingInterval(t *testing.T) {
	interval := 5 * time.Minute
	alignedEnd := time.Date(2026, 7, 14, 10, 5, 0, 0, time.UTC)
	partialEnd := alignedEnd.Add(2 * time.Minute)

	if got, want := poolMetricsBucketKey(alignedEnd, interval), alignedEnd.Add(-interval).UnixMilli(); got != want {
		t.Fatalf("aligned bucket key = %d, want %d", got, want)
	}
	if got, want := poolMetricsBucketKey(partialEnd, interval), alignedEnd.UnixMilli(); got != want {
		t.Fatalf("partial bucket key = %d, want %d", got, want)
	}
}

func TestComputePoolSnapshotFromS2(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"type": types.EventComputePool,
		"time": time.Unix(1, 0).UTC(),
		"data": types.EventComputeSchema{PoolName: "pool-a", Action: types.EventComputeActionPoolHeartbeat, MachineCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	sample, eventTime, ok := computePoolMetricFromS2(s2.SequencedRecord{Body: body})
	if !ok || sample.PoolName != "pool-a" || sample.MachineCount != 2 || !eventTime.Equal(time.Unix(1, 0).UTC()) {
		t.Fatalf("unexpected pool sample: ok=%v time=%v sample=%+v", ok, eventTime, sample)
	}
}

func TestPoolMetricsBucketAggregatesLatestMachineState(t *testing.T) {
	bucket := &poolMetricsBucket{key: 1_000, samples: map[string]types.EventComputeSchema{}}
	bucket.samples["pool-a\x00"] = types.EventComputeSchema{
		WorkspaceID: "workspace-1", PoolName: "pool-a", Action: types.EventComputeActionPoolHeartbeat, MachineCount: 20, CPUCount: 100, MemoryMB: 100_000, GPUCount: 20,
		Attrs: map[string]string{"container_count": "30", "free_gpu_count": "10", "cpu_utilization_pct": "99", "memory_used_mb": "99000", "hourly_cost_cents": "2000"},
	}
	bucket.samples["pool-a\x00machine-1"] = types.EventComputeSchema{
		WorkspaceID: "workspace-1", PoolName: "pool-a", MachineID: "machine-1", CPUCount: 4, MemoryMB: 8_000, GPUCount: 2,
		Attrs: map[string]string{"container_count": "3", "free_gpu_count": "1", "cpu_utilization_pct": "60", "memory_used_mb": "4000", "disk_used_mb": "100", "disk_total_mb": "200", "hourly_cost_cents": "200"},
	}
	bucket.samples["pool-a\x00machine-2"] = types.EventComputeSchema{
		WorkspaceID: "workspace-1", PoolName: "pool-a", MachineID: "machine-2", CPUCount: 8, MemoryMB: 16_000, GPUCount: 2,
		Attrs: map[string]string{"container_count": "5", "free_gpu_count": "0", "cpu_utilization_pct": "30", "memory_used_mb": "12000", "disk_used_mb": "300", "disk_total_mb": "600", "hourly_cost_cents": "300"},
	}
	bucket.samples["pool-b\x00"] = types.EventComputeSchema{
		WorkspaceID: "workspace-1", PoolName: "pool-b", Action: types.EventComputeActionPoolHeartbeat, MachineCount: 3, CPUCount: 12, MemoryMB: 24_000, GPUCount: 4,
		Attrs: map[string]string{"container_count": "7", "free_gpu_count": "2", "cpu_utilization_pct": "25", "memory_used_mb": "6000", "hourly_cost_cents": "600"},
	}

	metrics := bucket.point(time.Minute).Pools
	metric := metrics[0]
	if metric.MachineCount != 2 || metric.ContainerCount != 8 || metric.GPUCount != 4 || metric.FreeGPUCount != 1 {
		t.Fatalf("unexpected capacity metrics: %+v", metric)
	}
	checks := []struct {
		name      string
		got, want float64
	}{
		{"cpu utilization", metric.CPUUtilizationPct, 40},
		{"memory utilization", metric.MemoryUtilizationPct, 200.0 / 3},
		{"gpu utilization", metric.GPUUtilizationPct, 75},
		{"disk utilization", metric.DiskUsagePct, 50},
		{"hourly cost", metric.HourlyCost, 5},
		{"estimated cost", metric.EstimatedCost, 5.0 / 60},
	}
	for _, check := range checks {
		if math.Abs(check.got-check.want) > 0.001 {
			t.Fatalf("%s = %v, want %v: %+v", check.name, check.got, check.want, metric)
		}
	}
	pool := metrics[1]
	if pool.PoolName != "pool-b" || pool.MachineCount != 3 || pool.GPUCount != 4 || pool.FreeGPUCount != 2 || pool.ContainerCount != 7 {
		t.Fatalf("unexpected pool snapshot: %+v", pool)
	}
}

func TestPoolMetricsBucketFallsBackToPoolCost(t *testing.T) {
	bucket := &poolMetricsBucket{key: 1_000, samples: map[string]types.EventComputeSchema{
		"pool-a\x00": {
			WorkspaceID:  "workspace-1",
			PoolName:     "pool-a",
			Action:       types.EventComputeActionPoolHeartbeat,
			MachineCount: 1,
			Attrs: map[string]string{
				types.EventComputeAttrHourlyCostCents: "80",
			},
		},
		"pool-a\x00machine-1": {
			WorkspaceID: "workspace-1",
			PoolName:    "pool-a",
			MachineID:   "machine-1",
			GPUCount:    1,
			Attrs: map[string]string{
				types.EventComputeAttrFreeGPUCount: "1",
			},
		},
	}}

	metrics := bucket.point(time.Hour).Pools
	if len(metrics) != 1 {
		t.Fatalf("pool count = %d, want 1", len(metrics))
	}
	if got, want := metrics[0].HourlyCost, 0.8; got != want {
		t.Fatalf("hourly cost = %v, want %v: %+v", got, want, metrics[0])
	}
	if got, want := metrics[0].EstimatedCost, 0.8; got != want {
		t.Fatalf("estimated cost = %v, want %v: %+v", got, want, metrics[0])
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
	event, err := repo.createEventObject(types.EventWorkerLifecycle, types.EventWorkerLifecycleSchemaVersion, types.EventWorkerLifecycleSchema{
		PoolName: "default",
		Status:   types.EventWorkerLifecycleDeleted,
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

func TestS2EndPositionPastTailFallsBackToLiveTail(t *testing.T) {
	err := &s2.RangeNotSatisfiableError{S2Error: &s2.S2Error{
		Status: httpStatusRangeNotSatisfiable,
		Origin: "server",
	}}
	offset, positionErr := logEndPositionTailOffset(100, nil, err)
	if positionErr != nil || offset != 0 {
		t.Fatalf("expected live-tail fallback, got offset=%d err=%v", offset, positionErr)
	}

	batch := &s2.ReadBatch{Records: []s2.SequencedRecord{{SeqNum: 40}}}
	offset, positionErr = logEndPositionTailOffset(100, batch, nil)
	if positionErr != nil || offset != 60 {
		t.Fatalf("expected historical offset 60, got offset=%d err=%v", offset, positionErr)
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
