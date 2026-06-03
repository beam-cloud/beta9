package worker

import (
	"context"
	"sync"
	"testing"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// fakeEventRepo captures required-content events. It embeds the EventRepository
// interface so only the methods exercised by the reporter need implementations.
type fakeEventRepo struct {
	repo.EventRepository
	mu     sync.Mutex
	pushed []types.EventStubCacheRequiredContentSchema
}

func (f *fakeEventRepo) PushStubCacheRequiredContent(schema types.EventStubCacheRequiredContentSchema) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushed = append(f.pushed, schema)
}

func newTestReporter(eventRepo repo.EventRepository) *cacheContentReporter {
	return &cacheContentReporter{
		ctx:       context.Background(),
		eventRepo: eventRepo,
		locality:  "default",
		pending:   make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		reported:  make(map[string]struct{}),
	}
}

func TestReporterGeneratesOncePerStub(t *testing.T) {
	r := newTestReporter(&fakeEventRepo{})

	// With no Redis metadata, the in-memory guard ensures one-time generation.
	require.True(t, r.shouldGenerateRequiredContent("stub-a"))
	require.False(t, r.shouldGenerateRequiredContent("stub-a"))
	require.True(t, r.shouldGenerateRequiredContent("stub-b"))
}

func TestReporterCoalescesItemsPerStubKind(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)

	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{
		{Hash: "h1"},
		{Hash: "h2"},
	})
	// Overlapping report updates h2 and adds h3; should coalesce to 3 unique items.
	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{
		{Hash: "h2", SizeBytes: 5},
		{Hash: "h3"},
	})

	r.flush()

	require.Len(t, fake.pushed, 1)
	event := fake.pushed[0]
	require.Equal(t, "ws", event.WorkspaceID)
	require.Equal(t, "stub", event.StubID)
	require.Equal(t, types.CacheContentKindClipV1, event.Kind)
	require.Len(t, event.Items, 3)
	for _, item := range event.Items {
		require.Equal(t, item.Hash, item.RoutingKey, "routing key should default to hash")
	}
}

func TestReporterSeparatesByKind(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)

	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{{Hash: "h1"}})
	r.reportItems("ws", "stub", types.CacheContentKindClipV2, []types.CacheRequiredContentItem{{Hash: "h2"}})

	r.flush()
	require.Len(t, fake.pushed, 2)
}

func TestReporterVolumeRespectsSizeThreshold(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)
	r.volumeMinBytes = 1024
	r.activeStubs = func(string) []string { return []string{"stub"} }

	r.ReportVolumeContent("ws", "small", "/p/small", 512)  // below threshold -> dropped
	r.ReportVolumeContent("ws", "big", "/p/big", 4096)     // above threshold -> kept

	r.flush()
	require.Len(t, fake.pushed, 1)
	event := fake.pushed[0]
	require.Equal(t, types.CacheContentKindVolume, event.Kind)
	require.Len(t, event.Items, 1)
	require.Equal(t, "big", event.Items[0].Hash)
}

func TestReporterVolumeNoActiveStubsIsNoop(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)
	r.activeStubs = func(string) []string { return nil }

	r.ReportVolumeContent("ws", "big", "/p/big", 4096)
	r.flush()
	require.Empty(t, fake.pushed)
}
