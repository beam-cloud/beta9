package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clip "github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/stretchr/testify/require"
)

// fakeEventRepo captures required-content events. It embeds the EventRepository
// interface so only the methods exercised by the reporter need implementations.
type fakeEventRepo struct {
	repo.EventRepository
	mu          sync.Mutex
	pushed      []types.EventStubCacheRequiredContentSchema
	cacheEvents []types.EventPlatformCacheSchema
	items       []types.CacheRequiredContentItem
	itemsByStub map[string]map[string]types.CacheRequiredContentItem
	readKeys    []string
	err         error
	readErr     error
}

func (f *fakeEventRepo) PushStubCacheRequiredContent(schema types.EventStubCacheRequiredContentSchema) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.itemsByStub == nil {
		f.itemsByStub = map[string]map[string]types.CacheRequiredContentItem{}
	}
	stubKey := schema.WorkspaceID + "|" + schema.StubID
	bucket := f.itemsByStub[stubKey]
	if bucket == nil {
		bucket = map[string]types.CacheRequiredContentItem{}
		f.itemsByStub[stubKey] = bucket
	}
	for _, item := range schema.Items {
		if item.Hash == "" {
			continue
		}
		if item.Kind == "" {
			item.Kind = schema.Kind
		}
		key := item.Hash + "\x00" + item.RoutingKey
		if item.Kind == types.CacheContentKindCheckpoint {
			key = string(types.CacheContentKindCheckpoint)
		}
		bucket[key] = item
	}
	f.pushed = append(f.pushed, schema)
	return nil
}

func (f *fakeEventRepo) PushPlatformCacheEvent(schema types.EventPlatformCacheSchema) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cacheEvents = append(f.cacheEvents, schema)
}

func (f *fakeEventRepo) ReadStubCacheRequiredContent(ctx context.Context, workspaceID, stubID string) ([]types.CacheRequiredContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	f.readKeys = append(f.readKeys, workspaceID+"|"+stubID)
	if f.itemsByStub != nil {
		if bucket := f.itemsByStub[workspaceID+"|"+stubID]; len(bucket) > 0 {
			items := make([]types.CacheRequiredContentItem, 0, len(bucket))
			for _, item := range bucket {
				items = append(items, item)
			}
			return items, nil
		}
	}
	return append([]types.CacheRequiredContentItem(nil), f.items...), nil
}

type testHostDirectoryFunc func(context.Context, string) ([]*cache.Host, error)

func (f testHostDirectoryFunc) GetAvailableHosts(ctx context.Context, locality string) ([]*cache.Host, error) {
	return f(ctx, locality)
}

func newTestReporter(eventRepo repo.EventRepository) *cacheContentReporter {
	return &cacheContentReporter{
		ctx:       context.Background(),
		eventRepo: eventRepo,
		locality:  "default",
		pending:   make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		recent:    make(map[reporterStubKey]struct{}),
		reported:  make(map[string]struct{}),
	}
}

func TestAuditCacheChurnEventIncludesMachineFields(t *testing.T) {
	t.Setenv(types.WorkerMachineEnv, "machine-a")
	events := &fakeEventRepo{}
	manager := &WorkerCacheManager{
		config: types.AppConfig{
			ManagedCompute: types.ManagedComputeConfig{SellerWorkspaceID: "workspace-a"},
		},
		eventRepo: events,
		workerID:  "worker-a",
		poolName:  "pool-a",
		nodeID:    "node-a",
		locality:  "default",
	}

	manager.auditCacheChurnEvent("host-a", cache.CacheChurnEvent{
		Operation:           cache.CacheChurnOperationDiskEviction,
		Status:              cache.CacheChurnStatusProtectedEvicted,
		Path:                "/var/lib/beta9/cache/default/node-a",
		EvictedObjects:      3,
		ProtectedObjects:    1,
		FreedBytes:          1024,
		ProtectedFreedBytes: 512,
		UsagePct:            0.88,
		WatermarkPct:        0.80,
		AvailableBytes:      32,
		ReserveBytes:        40,
		TargetFreeBytes:     8,
	})

	require.Len(t, events.cacheEvents, 1)
	event := events.cacheEvents[0]
	require.Equal(t, "workspace-a", event.WorkspaceID)
	require.Equal(t, "machine-a", event.MachineID)
	require.Equal(t, "worker-a", event.WorkerID)
	require.Equal(t, "pool-a", event.PoolName)
	require.Equal(t, "node-a", event.NodeID)
	require.Equal(t, cache.CacheChurnStatusProtectedEvicted, event.Status)
	require.Equal(t, cache.CacheChurnOperationDiskEviction, event.Operation)
	require.Equal(t, int64(1024), event.FreedBytes)
	require.Equal(t, 1, event.ProtectedObjects)
}

func TestAuditCacheChurnEventUsesScopedS2WorkspacePrefix(t *testing.T) {
	t.Setenv(types.WorkerMachineEnv, "machine-a")
	events := &fakeEventRepo{}
	manager := &WorkerCacheManager{
		config: types.AppConfig{
			Database: types.DatabaseConfig{
				S2: types.S2Config{EventStreamPrefix: "events/workspaces/workspace-private"},
			},
		},
		eventRepo: events,
		workerID:  "worker-a",
		poolName:  "pool-a",
		locality:  "workspace-private/pool-a",
	}

	manager.auditCacheChurnEvent("host-a", cache.CacheChurnEvent{
		Operation: cache.CacheChurnOperationDiskEviction,
		Status:    cache.CacheChurnStatusEvicted,
	})

	require.Len(t, events.cacheEvents, 1)
	require.Equal(t, "workspace-private", events.cacheEvents[0].WorkspaceID)
	require.Equal(t, "machine-a", events.cacheEvents[0].MachineID)
}

func TestPressureProtectedContentPrioritizesNewestStubWorkingSet(t *testing.T) {
	now := time.Now()
	stubs := []recentStubContent{
		{
			stub: cache.RecentStub{WorkspaceID: "ws", StubID: "new", LastSeen: now},
			items: []types.CacheRequiredContentItem{
				{Hash: "new-volume", Kind: types.CacheContentKindVolume, SizeBytes: 20},
				{Hash: "new-checkpoint", Kind: types.CacheContentKindCheckpoint, SizeBytes: 40},
			},
		},
		{
			stub: cache.RecentStub{WorkspaceID: "ws", StubID: "old", LastSeen: now.Add(-time.Hour)},
			items: []types.CacheRequiredContentItem{
				{Hash: "old-checkpoint", Kind: types.CacheContentKindCheckpoint, SizeBytes: 40},
				{Hash: "old-volume", Kind: types.CacheContentKindVolume, SizeBytes: 20},
			},
		},
	}

	protected := pressureProtectedContentFromRecentStubs(stubs, "", cache.DiskUsage{TotalBytes: 100}, 0.75, 0)

	require.Contains(t, protected, "new-checkpoint")
	require.Contains(t, protected, "new-volume")
	require.NotContains(t, protected, "old-checkpoint")
	require.NotContains(t, protected, "old-volume")
}

func TestPressureProtectedContentPrioritizesCheckpointWithinStub(t *testing.T) {
	now := time.Now()
	stubs := []recentStubContent{
		{
			stub: cache.RecentStub{WorkspaceID: "ws", StubID: "stub", LastSeen: now},
			items: []types.CacheRequiredContentItem{
				{Hash: "volume", Kind: types.CacheContentKindVolume, SizeBytes: 50},
				{Hash: "checkpoint", Kind: types.CacheContentKindCheckpoint, SizeBytes: 50},
			},
		},
	}

	protected := pressureProtectedContentFromRecentStubs(stubs, "", cache.DiskUsage{TotalBytes: 90}, 0.75, 0)

	require.Contains(t, protected, "checkpoint")
	require.NotContains(t, protected, "volume")
}

func newCheckpointCacheForTest(t *testing.T, ctx context.Context) (*cache.Server, *cache.Client) {
	t.Helper()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Server.PageSizeBytes = 1024 * 1024
	cfg.Server.DiskCacheEvictWatermarkPct = 1
	metadataStore := cache.NewMockCacheMetadataStore()
	server, err := cache.NewServerWithOptions(ctx, cfg, "test", cache.WithServerMetadataStore(metadataStore), cache.WithServerHostID("local-host"))
	require.NoError(t, err)

	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	host := server.Host()
	require.NotNil(t, host)
	host.Addr = addr
	host.PrivateAddr = addr

	client, err := cache.NewClientWithHostDirectory(ctx, cfg, metadataStore, testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{host}, nil
	}), "test")
	require.NoError(t, err)
	client.AttachLocalServer(server)

	require.Eventually(t, func() bool {
		_, err := client.PrimaryReadHost("checkpoint-test-probe")
		return err == nil
	}, 3*time.Second, 20*time.Millisecond)

	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	return server, client
}

func createMaterializedCheckpointForTest(t *testing.T, checkpointPath string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFsDir, "state.txt"), []byte("filesystem payload"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime payload"), 0644))
}

func createCheckpointArchiveForTest(t *testing.T, checkpointID string) (archivePath, hash string, size int64) {
	t.Helper()

	root := t.TempDir()
	checkpointPath := filepath.Join(root, checkpointID)
	createMaterializedCheckpointForTest(t, checkpointPath)

	archivePath = filepath.Join(t.TempDir(), checkpointID+checkpointArchiveExtension)
	require.NoError(t, createTar(checkpointPath, archivePath))

	hash, size, err := fileSHA256(archivePath)
	require.NoError(t, err)
	return archivePath, hash, size
}

type claimedMetadataStore struct {
	cache.CacheMetadataStore
	recent           int
	recentLocalities []string
}

type localityRecentMetadataStore struct {
	*cache.MockCacheMetadataStore
	mu     sync.Mutex
	stubs  map[string][]cache.RecentStub
	listed []string
}

func (m *localityRecentMetadataStore) AddRecentStub(_ context.Context, locality, workspaceID, stubID string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stubs == nil {
		m.stubs = map[string][]cache.RecentStub{}
	}
	stubs := m.stubs[locality]
	now := time.Now()
	for i := range stubs {
		if stubs[i].WorkspaceID == workspaceID && stubs[i].StubID == stubID {
			stubs[i].LastSeen = now
			m.stubs[locality] = stubs
			return nil
		}
	}
	m.stubs[locality] = append(stubs, cache.RecentStub{WorkspaceID: workspaceID, StubID: stubID, LastSeen: now})
	return nil
}

func (m *localityRecentMetadataStore) ListRecentStubs(_ context.Context, locality string, _ time.Duration, limit int) ([]cache.RecentStub, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listed = append(m.listed, locality)
	stubs := append([]cache.RecentStub(nil), m.stubs[locality]...)
	if limit > 0 && len(stubs) > limit {
		stubs = stubs[:limit]
	}
	return stubs, nil
}

func (m *claimedMetadataStore) AddRecentStub(_ context.Context, locality, _, _ string, _ time.Duration) error {
	m.recent++
	m.recentLocalities = append(m.recentLocalities, locality)
	return nil
}

func TestReporterGeneratesOncePerStub(t *testing.T) {
	r := newTestReporter(&fakeEventRepo{})

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

func TestReporterIndexesRecentStubOnceAfterRequiredContentWrite(t *testing.T) {
	fake := &fakeEventRepo{}
	metadata := &claimedMetadataStore{}
	r := newTestReporter(fake)
	r.metadata = metadata
	r.locality = "locality-a"

	for range 100 {
		r.touchRecentStub("ws", "stub")
	}
	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{{Hash: "h1"}})

	r.flush()
	require.Len(t, fake.pushed, 1)
	require.Equal(t, []string{"locality-a"}, metadata.recentLocalities)
	require.Equal(t, "locality-a", fake.pushed[0].Locality)
}

func TestReporterRetriesRequiredContentWhenEventWriteFails(t *testing.T) {
	fake := &fakeEventRepo{err: errors.New("s2 unavailable")}
	metadata := &claimedMetadataStore{}
	r := newTestReporter(fake)
	r.metadata = metadata

	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{{Hash: "h1"}})
	r.flush()
	require.Empty(t, fake.pushed)

	fake.err = nil
	r.flush()
	require.Len(t, fake.pushed, 1)
}

func TestReporterVolumeRespectsSizeThreshold(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)
	r.volumeMinBytes = 1024
	r.activeStubs = func(string) []string { return []string{"stub"} }

	r.ReportVolumeContent("ws", "small", "/p/small", 512) // below threshold -> dropped
	r.ReportVolumeContent("ws", "big", "/p/big", 4096)    // above threshold -> kept

	r.flush()
	require.Len(t, fake.pushed, 1)
	event := fake.pushed[0]
	require.Equal(t, types.CacheContentKindVolume, event.Kind)
	require.Len(t, event.Items, 1)
	require.Equal(t, "big", event.Items[0].Hash)
	require.Equal(t, "/p/big", event.Items[0].Source)
	require.Equal(t, int64(4096), event.Items[0].SizeBytes)
}

func TestReporterVolumeDefaultThresholdKeepsOnlyLargeObjects(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)
	r.volumeMinBytes = cacheDefaultVolumeReportMinBytes
	r.activeStubs = func(string) []string { return []string{"stub"} }

	r.ReportVolumeContent("ws", "small", "/p/32mb", 32<<20)
	r.ReportVolumeContent("ws", "large", "/p/128mb", 128<<20)

	r.flush()
	require.Len(t, fake.pushed, 1)
	require.Len(t, fake.pushed[0].Items, 1)
	require.Equal(t, "large", fake.pushed[0].Items[0].Hash)
}

func TestReporterVolumeNoActiveStubsIsNoop(t *testing.T) {
	fake := &fakeEventRepo{}
	r := newTestReporter(fake)
	r.activeStubs = func(string) []string { return nil }

	r.ReportVolumeContent("ws", "big", "/p/big", 4096)
	r.flush()
	require.Empty(t, fake.pushed)
}

func TestCacheVolumeReportMinBytesDefaultAndOverride(t *testing.T) {
	manager := &WorkerCacheManager{}
	require.Equal(t, int64(128<<20), manager.cacheVolumeReportMinBytes())

	manager.config.Cache.Reconciliation.VolumeMinBytes = 64 << 20
	require.Equal(t, int64(64<<20), manager.cacheVolumeReportMinBytes())
}

func TestCacheContentAppliesToAccelerator(t *testing.T) {
	require.True(t, cacheContentAppliesToAccelerator(types.CacheRequiredContentItem{Kind: types.CacheContentKindCheckpoint, Accelerator: "a10g"}, "A10G"))
	require.True(t, cacheContentAppliesToAccelerator(types.CacheRequiredContentItem{Kind: types.CacheContentKindCheckpoint}, "A10G"))
	require.False(t, cacheContentAppliesToAccelerator(types.CacheRequiredContentItem{Kind: types.CacheContentKindCheckpoint, Accelerator: "T4"}, "A10G"))
	require.True(t, cacheContentAppliesToAccelerator(types.CacheRequiredContentItem{Kind: types.CacheContentKindVolume, Accelerator: "T4"}, "A10G"))
}

func TestProtectedContentSkipsCheckpointForOtherAccelerator(t *testing.T) {
	protected, checkpointIDs := protectedContentFromRecentStubs([]recentStubContent{{
		items: []types.CacheRequiredContentItem{
			{Hash: "checkpoint", Kind: types.CacheContentKindCheckpoint, CheckpointID: "checkpoint-a", Accelerator: "T4"},
			{Hash: "volume", Kind: types.CacheContentKindVolume},
		},
	}}, "A10G")

	require.NotContains(t, protected, "checkpoint")
	require.Contains(t, protected, "volume")
	require.Empty(t, checkpointIDs)
}

func TestReconcileBackoffIsBypassedByNewStubSighting(t *testing.T) {
	failedAt := time.Now().Add(-time.Minute)
	manager := &WorkerCacheManager{
		reconcileFailures: map[string]time.Time{
			"hash\x00route": failedAt,
		},
	}

	require.True(t, manager.reconcileBackingOff("hash", "route", failedAt.Add(-time.Second)))
	require.False(t, manager.reconcileBackingOff("hash", "route", failedAt.Add(time.Second)))
	require.Empty(t, manager.reconcileFailures)
}

func TestReconcileBackoffIsBypassedBySecondPrecisionStubSighting(t *testing.T) {
	failedAt := time.Unix(1_700_000_000, int64(750*time.Millisecond))
	manager := &WorkerCacheManager{
		reconcileFailures: map[string]time.Time{
			"hash\x00route": failedAt,
		},
	}

	require.False(t, manager.reconcileBackingOff("hash", "route", time.Unix(failedAt.Unix(), 0)))
	require.Empty(t, manager.reconcileFailures)
}

func TestReconcileSuccessBackoffSkipsRecentMaterialization(t *testing.T) {
	manager := &WorkerCacheManager{}

	manager.recordReconcileSuccess("hash", "route")
	require.True(t, manager.reconcileRecentlySucceeded("hash", "route"))

	manager.reconcileSuccesses[reconcileItemKey("hash", "route")] = time.Now().Add(-cacheReconcileSuccessBackoff - time.Second)
	require.False(t, manager.reconcileRecentlySucceeded("hash", "route"))
	require.Empty(t, manager.reconcileSuccesses)
}

func TestReconcileBudgetCapsMaterializationAttempts(t *testing.T) {
	budget := newReconcileBudget(1, 0)

	require.True(t, budget.take(0))
	require.False(t, budget.exhausted())
	require.False(t, budget.take(0))
	require.True(t, budget.exhausted())

	var unlimited *reconcileBudget
	require.True(t, unlimited.take(0))
	require.False(t, unlimited.exhausted())
}

func TestReconcileBudgetCapsBytesPerCycle(t *testing.T) {
	budget := newReconcileBudget(100, 10)

	// An item crossing the byte boundary is admitted; the budget exhausts after.
	require.True(t, budget.take(4))
	require.True(t, budget.take(8))
	require.False(t, budget.exhausted())
	require.False(t, budget.take(1))
	require.True(t, budget.exhausted())

	// A single item larger than the whole byte budget is still admitted.
	huge := newReconcileBudget(100, 10)
	require.True(t, huge.take(1<<30))
	require.False(t, huge.take(1))

	// Items with unknown size only consume the item budget.
	unknown := newReconcileBudget(2, 10)
	require.True(t, unknown.take(0))
	require.True(t, unknown.take(0))
	require.False(t, unknown.take(0))
	require.True(t, unknown.exhausted())
}

func TestLoadRecentRequiredContentCachesByLastSeen(t *testing.T) {
	fake := &fakeEventRepo{
		itemsByStub: map[string]map[string]types.CacheRequiredContentItem{
			"ws|stub-a": {"h1": {Hash: "h1", Kind: types.CacheContentKindDiskSnapshot}},
			"ws|stub-b": {"h2": {Hash: "h2", Kind: types.CacheContentKindDiskSnapshot}},
		},
	}
	manager := &WorkerCacheManager{ctx: context.Background(), eventRepo: fake}

	seenA := time.Unix(100, 0)
	seenB := time.Unix(200, 0)
	stubs := []cache.RecentStub{
		{WorkspaceID: "ws", StubID: "stub-a", LastSeen: seenA},
		{WorkspaceID: "ws", StubID: "stub-b", LastSeen: seenB},
	}

	content, complete := manager.loadRecentRequiredContent(stubs)
	require.True(t, complete)
	require.Len(t, content, 2)
	require.Equal(t, []string{"ws|stub-a", "ws|stub-b"}, fake.readKeys)

	// Unchanged scores are served from cache without new reads.
	content, complete = manager.loadRecentRequiredContent(stubs)
	require.True(t, complete)
	require.Len(t, content, 2)
	require.Equal(t, "h1", content[0].items[0].Hash)
	require.Equal(t, []string{"ws|stub-a", "ws|stub-b"}, fake.readKeys)

	// A bumped score (new activity/content) forces a re-read of that stub only.
	stubs[1].LastSeen = seenB.Add(time.Second)
	_, _ = manager.loadRecentRequiredContent(stubs)
	require.Equal(t, []string{"ws|stub-a", "ws|stub-b", "ws|stub-b"}, fake.readKeys)

	// Stubs that age out of the recent index are evicted from the cache.
	_, _ = manager.loadRecentRequiredContent(stubs[1:])
	require.NotContains(t, manager.requiredContentCache, "ws|stub-a")

	// Read errors are not cached; the stub is retried next cycle.
	fake.readErr = errors.New("s2 unavailable")
	_, complete = manager.loadRecentRequiredContent([]cache.RecentStub{{WorkspaceID: "ws", StubID: "stub-c", LastSeen: seenA}})
	require.False(t, complete)
	require.NotContains(t, manager.requiredContentCache, "ws|stub-c")
}

// windowedRecentMetadataStore honors the lookback the way the coordinator does.
type windowedRecentMetadataStore struct {
	*cache.MockCacheMetadataStore
	stubs     []cache.RecentStub
	lookbacks []time.Duration
}

func (m *windowedRecentMetadataStore) ListRecentStubs(_ context.Context, _ string, ttl time.Duration, _ int) ([]cache.RecentStub, error) {
	m.lookbacks = append(m.lookbacks, ttl)
	cutoff := time.Now().Add(-ttl)
	var out []cache.RecentStub
	for _, stub := range m.stubs {
		if !stub.LastSeen.Before(cutoff) {
			out = append(out, stub)
		}
	}
	return out, nil
}

func TestListRecentStubsSyncsIncrementallyAndAgesOutLocally(t *testing.T) {
	now := time.Now()
	store := &windowedRecentMetadataStore{stubs: []cache.RecentStub{
		{WorkspaceID: "ws", StubID: "old", LastSeen: now.Add(-6 * 24 * time.Hour)},
		{WorkspaceID: "ws", StubID: "recent", LastSeen: now.Add(-time.Hour)},
	}}
	manager := &WorkerCacheManager{ctx: context.Background(), metadataStore: store}
	ids := func(stubs []cache.RecentStub) []string {
		out := make([]string, 0, len(stubs))
		for _, stub := range stubs {
			out = append(out, stub.StubID)
		}
		return out
	}

	// The first listing covers the whole window, MRU-first.
	stubs, err := manager.listRecentStubs(false)
	require.NoError(t, err)
	require.Equal(t, []string{"recent", "old"}, ids(stubs))
	require.Equal(t, manager.recentStubTTL(), store.lookbacks[0])

	// A sync asks only for what was touched since, and merges it in.
	manager.recentStubs.listedAt = now.Add(-10 * time.Second)
	store.stubs = append(store.stubs, cache.RecentStub{WorkspaceID: "ws", StubID: "new", LastSeen: now})
	stubs, err = manager.listRecentStubs(false)
	require.NoError(t, err)
	require.Less(t, store.lookbacks[1], time.Minute)
	require.Equal(t, []string{"new", "recent", "old"}, ids(stubs))

	// Stubs leave the window without a round trip.
	manager.recentStubs.stubs["ws|old"] = cache.RecentStub{WorkspaceID: "ws", StubID: "old", LastSeen: now.Add(-8 * 24 * time.Hour)}
	stubs, err = manager.listRecentStubs(false)
	require.NoError(t, err)
	require.Equal(t, []string{"new", "recent"}, ids(stubs))

	// A maintenance pass relists the whole window.
	_, err = manager.listRecentStubs(true)
	require.NoError(t, err)
	require.Equal(t, manager.recentStubTTL(), store.lookbacks[3])
}

func TestReconcilePoolDedupesAndBoundsConcurrency(t *testing.T) {
	var nilPool *reconcilePool
	require.True(t, nilPool.claim("a"))
	require.True(t, nilPool.claim("a")) // nil pool never dedupes
	require.False(t, nilPool.didWork())
	nilPool.markProgress() // must not panic
	nilPool.wait()

	// concurrency <= 1 yields the inline (nil) pool
	require.Nil(t, newReconcilePool(1))

	pool := newReconcilePool(4)
	require.True(t, pool.claim("a"))
	require.False(t, pool.claim("a"))
	require.True(t, pool.claim("b"))

	var inFlight, maxInFlight, total atomic.Int64
	for i := 0; i < 32; i++ {
		pool.submit(func() {
			cur := inFlight.Add(1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			inFlight.Add(-1)
			total.Add(1)
		})
	}
	pool.wait()

	require.EqualValues(t, 32, total.Load())
	require.LessOrEqual(t, maxInFlight.Load(), int64(4))
	require.False(t, pool.didWork())
	pool.markProgress()
	require.True(t, pool.didWork())
}

func TestReconcileOnceUsesOnlyCurrentLocalityRecentStubs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	localServer, err := cache.NewServerWithOptions(ctx, cfg, "locality-b", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	client, err := cache.NewClientWithHostDirectory(ctx, clientCfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{localHost}, nil
	}), "locality-b")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })

	metadata := &localityRecentMetadataStore{
		MockCacheMetadataStore: cache.NewMockCacheMetadataStore(),
		stubs: map[string][]cache.RecentStub{
			"locality-a": {{WorkspaceID: "workspace-a", StubID: "stub-a", LastSeen: time.Now()}},
			"locality-b": {{WorkspaceID: "workspace-b", StubID: "stub-b", LastSeen: time.Now()}},
		},
	}
	fake := &fakeEventRepo{}
	manager := &WorkerCacheManager{
		ctx:               ctx,
		locality:          "locality-b",
		metadataStore:     metadata,
		eventRepo:         fake,
		client:            client,
		server:            localServer,
		checkpointRoot:    t.TempDir(),
		reconcileFailures: make(map[string]time.Time),
	}

	manager.reconcileOnce(false)

	require.Equal(t, []string{"locality-b"}, metadata.listed)
	require.Equal(t, []string{"workspace-b|stub-b"}, fake.readKeys)
}

func TestLocalHostOwnsForReconcileKeepsOwnershipThroughBriefEndpointLoss(t *testing.T) {
	require.Equal(t, cacheDefaultRegistrationTTL, cacheReconcileOwnerGracePeriod)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 2

	localServer, err := cache.NewServerWithOptions(ctx, cfg, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr

	// A peer that is registered in the ring but currently has no live endpoint
	// (e.g. its pod is mid-restart during a rolling deploy).
	peerHost := &cache.Host{
		HostId:      "peer-host",
		PoolName:    localHost.PoolName,
		Locality:    "test",
		NodeID:      "node-peer",
		CachePathID: "path-peer",
	}
	require.False(t, peerHost.HasEndpoint())

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	client, err := cache.NewClientWithHostDirectory(ctx, clientCfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{localHost, peerHost}, nil
	}), "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })

	require.Eventually(t, func() bool {
		return len(client.RankedReadHosts("probe")) == 2
	}, 3*time.Second, 20*time.Millisecond)

	manager := &WorkerCacheManager{
		ctx:           ctx,
		locality:      "test",
		client:        client,
		ownerLastLive: make(map[string]time.Time),
	}

	// Find one key owned by the down peer and one owned by the live local host
	peerKey, localKey := "", ""
	for i := 0; i < 256 && (peerKey == "" || localKey == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		ranked := client.RankedReadHosts(key)
		require.Len(t, ranked, 2)
		if ranked[0].HostId == peerHost.HostId && peerKey == "" {
			peerKey = key
		}
		if ranked[0].HostId == localHost.HostId && localKey == "" {
			localKey = key
		}
	}
	require.NotEmpty(t, peerKey)
	require.NotEmpty(t, localKey)

	// Keys owned by the live local host always reconcile locally
	require.True(t, manager.localHostOwnsForReconcile(localHost.HostId, localKey))

	// Keys owned by the briefly endpoint-less peer must NOT fail over within
	// the grace window: its on-disk content survives the restart, and taking
	// over would duplicate its entire key range.
	require.False(t, manager.localHostOwnsForReconcile(localHost.HostId, peerKey))

	// Once the peer has been endpoint-less past the grace period, its keys
	// fail over to the next-ranked live host so the system still self-heals.
	manager.ownerLastLive[peerHost.HostId] = time.Now().Add(-cacheReconcileOwnerGracePeriod - time.Minute)
	require.True(t, manager.localHostOwnsForReconcile(localHost.HostId, peerKey))
}

func TestReconcileStubSkipsRecentlyMaterializedMissingContent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 1

	localServer, err := cache.NewServerWithOptions(ctx, cfg, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	client, err := cache.NewClientWithHostDirectory(ctx, clientCfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{localHost}, nil
	}), "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })

	routingKey := "route"
	require.Eventually(t, func() bool {
		primary, err := client.PrimaryReadHost(routingKey)
		return err == nil && primary != nil && primary.HostId == localHost.HostId
	}, 3*time.Second, 20*time.Millisecond)

	hash := strings.Repeat("a", 64)
	fake := &fakeEventRepo{items: []types.CacheRequiredContentItem{{
		Kind:       types.CacheContentKindClipV1,
		Hash:       hash,
		RoutingKey: routingKey,
		SizeBytes:  1,
		Source:     "image.clip",
	}}}
	manager := &WorkerCacheManager{
		ctx:                ctx,
		locality:           "test",
		metadataStore:      cache.NewMockCacheMetadataStore(),
		eventRepo:          fake,
		client:             client,
		checkpointRoot:     t.TempDir(),
		reconcileFailures:  make(map[string]time.Time),
		reconcileSuccesses: map[string]time.Time{reconcileItemKey(hash, routingKey): time.Now()},
	}

	checkpointIDs := manager.reconcileStub(localServer, localHost.HostId, cache.RecentStub{WorkspaceID: "workspace", StubID: "stub"}, nil)

	require.Empty(t, checkpointIDs)
	require.Empty(t, fake.cacheEvents)
	require.False(t, localServer.HasCompleteContent(hash, 1))
}

// The gateway no longer vends S3 archive credentials; private-pool workers
// must materialize CLIP v1 archives through the gateway-presigned data URL.
func TestMaterializeArchiveObjectUsesBrokeredDataURL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	content := []byte("clip v1 archive data")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer origin.Close()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	server, err := cache.NewServerWithOptions(ctx, cfg, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("local-host"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	workerRepo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{
			Ok:                  true,
			ImageArchiveDataUrl: origin.URL + "/image-a.clip",
		},
	}
	checkpointRoot := filepath.Join(t.TempDir(), "checkpoints")
	require.NoError(t, os.MkdirAll(checkpointRoot, 0o755))
	manager := &WorkerCacheManager{
		ctx: ctx,
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{RegistryStore: registry.S3ImageRegistryStore},
		},
		workerRepo:       workerRepo,
		originCredsCache: make(map[string]*originCredentials),
		checkpointRoot:   checkpointRoot,
	}

	stub := cache.RecentStub{WorkspaceID: "workspace-a", StubID: "stub-a"}
	item := types.CacheRequiredContentItem{
		Hash:      hash,
		SizeBytes: int64(len(content)),
		ImageID:   "image-a",
		Source:    "image-a." + registry.LocalImageFileExtension,
		Kind:      types.CacheContentKindClipV1,
	}

	status := manager.materializeArchiveObject(ctx, server, stub, item, "/images/image-a.clip")

	require.Equal(t, types.CacheAuditStatusMaterialized, status)
	require.True(t, server.HasCompleteContent(hash, int64(len(content))))
	require.NotEmpty(t, workerRepo.requests)
	require.Equal(t, "image-a", workerRepo.requests[0].ImageId)
	require.Equal(t, "workspace-a", workerRepo.requests[0].WorkspaceId)
}

func TestMaterializeOCILayerPrivateWorkerRequiresGatewayRegistryCredentials(t *testing.T) {
	workerRepo := &fakeImageCredentialWorkerRepo{
		resp: &pb.GetCacheOriginCredentialsResponse{Ok: true},
	}
	manager := &WorkerCacheManager{
		ctx:              context.Background(),
		poolConfig:       types.WorkerPoolConfig{Mode: types.PoolModePrivate},
		workerRepo:       workerRepo,
		originCredsCache: make(map[string]*originCredentials),
	}
	stub := cache.RecentStub{WorkspaceID: "workspace-a", StubID: "stub-a"}
	item := types.CacheRequiredContentItem{
		Hash:    strings.Repeat("b", 64),
		ImageID: "image-a",
		Source:  "registry.example.com/team/image@sha256:" + strings.Repeat("a", 64),
		Kind:    types.CacheContentKindClipV2,
	}

	status := manager.materializeOCILayer(context.Background(), nil, stub, item)

	require.Equal(t, types.CacheAuditStatusOriginFailure, status)
	require.Len(t, workerRepo.requests, 1)
	require.Equal(t, "workspace-a", workerRepo.requests[0].WorkspaceId)
	require.Equal(t, "stub-a", workerRepo.requests[0].StubId)
	require.Equal(t, "image-a", workerRepo.requests[0].ImageId)
	require.Equal(t, "registry.example.com", workerRepo.requests[0].Registry)
}

func TestEnsureCheckpointMaterializedReportsMissingCheckpointDemand(t *testing.T) {
	fake := &fakeEventRepo{}
	metadata := &claimedMetadataStore{}
	reporter := newTestReporter(fake)
	reporter.metadata = metadata
	worker := &Worker{
		cacheManager: &WorkerCacheManager{
			checkpointRoot: t.TempDir(),
			reporter:       reporter,
			reconcileNow:   make(chan struct{}, 1),
		},
	}
	checkpoint := &types.Checkpoint{
		CheckpointId:   "checkpoint-a",
		CacheHash:      strings.Repeat("a", 64),
		CacheSizeBytes: 1,
		OriginKey:      "checkpoints/checkpoint-a.tar",
		Accelerator:    "A10G",
	}

	_, err := worker.ensureCheckpointMaterialized(context.Background(), &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
	}, checkpoint)
	require.Error(t, err)
	require.Len(t, worker.cacheManager.reconcileNow, 1)
	reporter.flush()

	require.Equal(t, 1, metadata.recent)
	require.Equal(t, []string{"default"}, metadata.recentLocalities)
	require.Len(t, fake.pushed, 1)
	require.Equal(t, "default", fake.pushed[0].Locality)
	require.Equal(t, types.CacheContentKindCheckpoint, fake.pushed[0].Kind)
	require.Equal(t, "workspace", fake.pushed[0].WorkspaceID)
	require.Equal(t, "stub", fake.pushed[0].StubID)
	require.Equal(t, checkpoint.CacheHash, fake.pushed[0].Items[0].Hash)
	require.Equal(t, checkpoint.CheckpointId, fake.pushed[0].Items[0].CheckpointID)
	require.Equal(t, checkpoint.Accelerator, fake.pushed[0].Items[0].Accelerator)
}

func TestCheckpointMaterializationLockSerializesSameCheckpoint(t *testing.T) {
	manager := &WorkerCacheManager{}
	release, err := manager.acquireCheckpointMaterialization(context.Background(), "checkpoint-a")
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = manager.acquireCheckpointMaterialization(waitCtx, "checkpoint-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	releaseOther, err := manager.acquireCheckpointMaterialization(context.Background(), "checkpoint-b")
	require.NoError(t, err)
	releaseOther()

	release()
	releaseAgain, err := manager.acquireCheckpointMaterialization(context.Background(), "checkpoint-a")
	require.NoError(t, err)
	releaseAgain()
	require.Empty(t, manager.checkpointLocks)
}

func TestCheckpointMaterializationLockSerializesAcrossManagersOnSameHost(t *testing.T) {
	checkpointRoot := t.TempDir()
	firstManager := &WorkerCacheManager{checkpointRoot: checkpointRoot}
	secondManager := &WorkerCacheManager{checkpointRoot: checkpointRoot}

	release, err := firstManager.acquireCheckpointMaterialization(context.Background(), "checkpoint-a")
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = secondManager.acquireCheckpointMaterialization(waitCtx, "checkpoint-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	releaseOther, err := secondManager.acquireCheckpointMaterialization(context.Background(), "checkpoint-b")
	require.NoError(t, err)
	releaseOther()

	release()
	releaseAgain, err := secondManager.acquireCheckpointMaterialization(context.Background(), "checkpoint-a")
	require.NoError(t, err)
	releaseAgain()
}

func TestCheckpointMaterializationLockCancellationIsSkipped(t *testing.T) {
	manager := &WorkerCacheManager{}
	release, err := manager.acquireCheckpointMaterialization(context.Background(), "checkpoint-a")
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status := manager.materializeCheckpoint(ctx, nil, cache.RecentStub{}, types.CacheRequiredContentItem{
		CheckpointID: "checkpoint-a",
		Hash:         "hash",
		SizeBytes:    1,
	}, "hash")

	require.Equal(t, types.CacheAuditStatusSkipped, status)
	require.False(t, reconcileStatusIsFailure(status))
}

func TestEnsureCheckpointMaterializedDoesNotReportAlreadyLocalCheckpoint(t *testing.T) {
	fake := &fakeEventRepo{}
	metadata := &claimedMetadataStore{}
	reporter := newTestReporter(fake)
	reporter.metadata = metadata

	checkpointID := "checkpoint-local"
	checkpointRoot := t.TempDir()
	createMaterializedCheckpointForTest(t, filepath.Join(checkpointRoot, checkpointID))
	worker := &Worker{
		cacheManager: &WorkerCacheManager{
			checkpointRoot: checkpointRoot,
			reporter:       reporter,
			reconcileNow:   make(chan struct{}, 1),
		},
	}
	checkpoint := &types.Checkpoint{
		CheckpointId:   checkpointID,
		CacheHash:      strings.Repeat("c", 64),
		CacheSizeBytes: 128,
		OriginKey:      "checkpoints/checkpoint-local.tar",
		Accelerator:    "CPU",
	}

	path, err := worker.ensureCheckpointMaterialized(context.Background(), &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
	}, checkpoint)

	require.NoError(t, err)
	require.Equal(t, filepath.Join(checkpointRoot, checkpointID), path)
	require.Empty(t, worker.cacheManager.reconcileNow)

	require.Zero(t, metadata.recent)
	require.Empty(t, metadata.recentLocalities)
	require.Empty(t, fake.pushed)
}

func TestEnsureCheckpointMaterializedRestoresFromEmbeddedCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, client := newCheckpointCacheForTest(t, ctx)
	checkpointID := "checkpoint-cache-hit"
	archivePath, hash, size := createCheckpointArchiveForTest(t, checkpointID)
	_, err := client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      archivePath,
		CachePath: "checkpoints/" + checkpointID + checkpointArchiveExtension,
	}, cache.StoreContentOptions{RoutingKey: hash, Lock: true})
	require.NoError(t, err)
	require.True(t, server.HasCompleteContent(hash, size))

	fake := &fakeEventRepo{}
	reporter := newTestReporter(fake)
	reporter.metadata = &claimedMetadataStore{}
	checkpointRoot := filepath.Join(t.TempDir(), "checkpoints")
	worker := &Worker{
		cacheManager: &WorkerCacheManager{
			client:         client,
			checkpointRoot: checkpointRoot,
			reporter:       reporter,
			reconcileNow:   make(chan struct{}, 1),
		},
	}

	path, err := worker.ensureCheckpointMaterialized(context.Background(), &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
	}, &types.Checkpoint{
		CheckpointId:   checkpointID,
		CacheHash:      hash,
		CacheSizeBytes: size,
		OriginKey:      "checkpoints/" + checkpointID + checkpointArchiveExtension,
	})

	require.NoError(t, err)
	require.True(t, checkpointMaterialized(path))
	require.NoFileExists(t, filepath.Join(checkpointRoot, checkpointID+checkpointArchiveExtension))
	require.Len(t, worker.cacheManager.reconcileNow, 1)
	reporter.flush()

	require.Len(t, fake.pushed, 1)
	require.Equal(t, types.CacheContentKindCheckpoint, fake.pushed[0].Kind)
	require.Equal(t, hash, fake.pushed[0].Items[0].Hash)
	require.Equal(t, hash, fake.pushed[0].Items[0].RoutingKey)
	require.Equal(t, checkpointID, fake.pushed[0].Items[0].CheckpointID)
}

func TestReconcileCheckpointExtractsEmbeddedCacheArchive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, client := newCheckpointCacheForTest(t, ctx)
	checkpointID := "checkpoint-reconcile"
	archivePath, hash, size := createCheckpointArchiveForTest(t, checkpointID)
	_, err := client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      archivePath,
		CachePath: "checkpoints/" + checkpointID + checkpointArchiveExtension,
	}, cache.StoreContentOptions{RoutingKey: hash, Lock: true})
	require.NoError(t, err)

	checkpointRoot := filepath.Join(t.TempDir(), "checkpoints")
	manager := &WorkerCacheManager{
		ctx:            ctx,
		client:         client,
		checkpointRoot: checkpointRoot,
	}
	status := manager.materializeCheckpoint(ctx, server, cache.RecentStub{
		WorkspaceID: "workspace",
		StubID:      "stub",
	}, types.CacheRequiredContentItem{
		Kind:         types.CacheContentKindCheckpoint,
		Hash:         hash,
		RoutingKey:   hash,
		SizeBytes:    size,
		CheckpointID: checkpointID,
	}, hash)

	require.Equal(t, types.CacheAuditStatusMaterialized, status)
	require.True(t, checkpointMaterialized(filepath.Join(checkpointRoot, checkpointID)))
	require.NoFileExists(t, filepath.Join(checkpointRoot, checkpointID+checkpointArchiveExtension))
}

func TestReconcileCheckpointIgnoresSuccessBackoffWhenExtractedPayloadMissing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, client := newCheckpointCacheForTest(t, ctx)
	checkpointID := "checkpoint-success-backoff"
	archivePath, hash, size := createCheckpointArchiveForTest(t, checkpointID)
	_, err := client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      archivePath,
		CachePath: "checkpoints/" + checkpointID + checkpointArchiveExtension,
	}, cache.StoreContentOptions{RoutingKey: hash, Lock: true})
	require.NoError(t, err)

	fake := &fakeEventRepo{items: []types.CacheRequiredContentItem{{
		Kind:         types.CacheContentKindCheckpoint,
		Hash:         hash,
		RoutingKey:   hash,
		SizeBytes:    size,
		CheckpointID: checkpointID,
		Accelerator:  "CPU",
	}}}
	checkpointRoot := filepath.Join(t.TempDir(), "checkpoints")
	manager := &WorkerCacheManager{
		ctx:            ctx,
		locality:       "test",
		accelerator:    "CPU",
		metadataStore:  cache.NewMockCacheMetadataStore(),
		eventRepo:      fake,
		client:         client,
		checkpointRoot: checkpointRoot,
	}
	manager.recordReconcileSuccess(hash, hash)

	manager.reconcileStub(server, "local-host", cache.RecentStub{
		WorkspaceID: "workspace",
		StubID:      "stub",
	}, newReconcileBudget(10, 0))

	require.True(t, checkpointMaterialized(filepath.Join(checkpointRoot, checkpointID)))
	require.Len(t, fake.cacheEvents, 1)
	require.Equal(t, types.CacheAuditStatusMaterialized, fake.cacheEvents[0].Status)
	require.False(t, manager.reconcileRecentlySucceeded(hash, hash))
}

func TestRequiredContentReportedByOneWorkerReconcilesCheckpointOnPeerInLocality(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 2
	cfg.Server.DiskCacheEvictWatermarkPct = 1
	metadata := &localityRecentMetadataStore{
		MockCacheMetadataStore: cache.NewMockCacheMetadataStore(),
		stubs:                  map[string][]cache.RecentStub{},
	}
	newServer := func(hostID string) (*cache.Server, *cache.Host) {
		t.Helper()
		serverCfg := cfg
		serverCfg.Server.DiskCacheDir = t.TempDir()
		server, err := cache.NewServerWithOptions(ctx, serverCfg, "locality-a", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID(hostID))
		require.NoError(t, err)
		addr, err := server.Serve("127.0.0.1:0", "")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, server.Close()) })

		host := server.Host()
		require.NotNil(t, host)
		host.Addr = addr
		host.PrivateAddr = addr
		return server, host
	}

	sourceServer, sourceHost := newServer("source-host")
	destinationServer, destinationHost := newServer("destination-host")

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	destinationClient, err := cache.NewClientWithHostDirectory(ctx, clientCfg, metadata, testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{sourceHost, destinationHost}, nil
	}), "locality-a")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destinationClient.Cleanup()) })
	destinationClient.AttachLocalServer(destinationServer)
	require.Eventually(t, func() bool {
		return len(destinationClient.RankedReadHosts("probe")) == 2
	}, 3*time.Second, 20*time.Millisecond)

	checkpointID := ""
	archivePath := ""
	hash := ""
	size := int64(0)
	for i := 0; i < 32; i++ {
		candidateID := fmt.Sprintf("checkpoint-peer-reconcile-%d", i)
		candidateArchive, candidateHash, candidateSize := createCheckpointArchiveForTest(t, candidateID)
		ranked := destinationClient.RankedReadHosts(candidateHash)
		if len(ranked) > 0 && ranked[0].HostId == sourceHost.HostId {
			checkpointID = candidateID
			archivePath = candidateArchive
			hash = candidateHash
			size = candidateSize
			break
		}
	}
	require.NotEmpty(t, checkpointID, "expected a checkpoint hash that reads from the source host first")

	archive, err := os.Open(archivePath)
	require.NoError(t, err)
	_, _, err = sourceServer.StoreReader(ctx, archive, hash)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.True(t, sourceServer.HasCompleteContent(hash, size))
	require.False(t, destinationServer.HasCompleteContent(hash, size))

	events := &fakeEventRepo{}
	reporter := newTestReporter(events)
	reporter.metadata = metadata
	reporter.locality = "locality-a"
	reporter.recentStubTTL = time.Hour
	workerA := &Worker{cacheManager: &WorkerCacheManager{reporter: reporter}}
	workerA.reportCheckpointRequiredContent(&types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
	}, checkpointID, &checkpointCacheMetadata{
		hash:        hash,
		sizeBytes:   size,
		originKey:   "checkpoints/" + checkpointID + checkpointArchiveExtension,
		locality:    "locality-a",
		accelerator: "CPU",
	})
	reporter.flush()

	checkpointRoot := filepath.Join(t.TempDir(), "checkpoints")
	managerB := &WorkerCacheManager{
		ctx:                ctx,
		config:             types.AppConfig{Cache: cfg},
		locality:           "locality-a",
		accelerator:        "CPU",
		metadataStore:      metadata,
		eventRepo:          events,
		client:             destinationClient,
		server:             destinationServer,
		checkpointRoot:     checkpointRoot,
		reconcileFailures:  make(map[string]time.Time),
		reconcileSuccesses: make(map[string]time.Time),
	}

	managerB.reconcileOnce(false)

	require.True(t, destinationServer.HasCompleteContent(hash, size))
	require.True(t, checkpointMaterialized(filepath.Join(checkpointRoot, checkpointID)))
	require.Equal(t, []string{"locality-a"}, metadata.listed)

	events.mu.Lock()
	pushed := append([]types.EventStubCacheRequiredContentSchema(nil), events.pushed...)
	cacheEvents := append([]types.EventPlatformCacheSchema(nil), events.cacheEvents...)
	readKeys := append([]string(nil), events.readKeys...)
	events.mu.Unlock()

	require.Len(t, pushed, 1)
	require.Equal(t, "locality-a", pushed[0].Locality)
	require.Equal(t, []string{"workspace|stub"}, readKeys)
	require.Len(t, cacheEvents, 1)
	require.Equal(t, "locality-a", cacheEvents[0].Locality)
	require.Equal(t, destinationHost.HostId, cacheEvents[0].LogicalHost)
	require.Equal(t, types.CacheAuditStatusMaterialized, cacheEvents[0].Status)
}

// Disk snapshot chunks replicate to every host in the locality (like
// checkpoints) so a restore reads them locally, while other kinds stay
// sharded by ring owner.
func TestReconcileDiskSnapshotChunksMaterializeOnNonOwnerHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 2

	newServer := func(hostID string) (*cache.Server, *cache.Host) {
		t.Helper()
		serverCfg := cfg
		serverCfg.Server.DiskCacheDir = t.TempDir()
		server, err := cache.NewServerWithOptions(ctx, serverCfg, "locality-a", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID(hostID))
		require.NoError(t, err)
		addr, err := server.Serve("127.0.0.1:0", "")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, server.Close()) })

		host := server.Host()
		require.NotNil(t, host)
		host.Addr = addr
		host.PrivateAddr = addr
		return server, host
	}
	sourceServer, sourceHost := newServer("source-host")
	destinationServer, destinationHost := newServer("destination-host")

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	destinationClient, err := cache.NewClientWithHostDirectory(ctx, clientCfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{sourceHost, destinationHost}, nil
	}), "locality-a")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destinationClient.Cleanup()) })
	destinationClient.AttachLocalServer(destinationServer)
	require.Eventually(t, func() bool {
		return len(destinationClient.RankedReadHosts("probe")) == 2
	}, 3*time.Second, 20*time.Millisecond)

	// Two source-host-owned blobs: one disk snapshot chunk (must replicate
	// to the non-owner destination) and one owner-sharded volume blob
	// (must stay gated).
	sourceOwnedContent := func(seed string) (string, []byte) {
		t.Helper()
		for i := 0; i < 256; i++ {
			data := []byte(fmt.Sprintf("%s-%d-content", seed, i))
			digest := sha256.Sum256(data)
			hash := hex.EncodeToString(digest[:])
			ranked := destinationClient.RankedReadHosts(hash)
			require.Len(t, ranked, 2)
			if ranked[0].HostId == sourceHost.HostId {
				return hash, data
			}
		}
		t.Fatal("expected content owned by the source host")
		return "", nil
	}
	chunkHash, chunkData := sourceOwnedContent("chunk")
	volumeHash, volumeData := sourceOwnedContent("volume")
	for hash, data := range map[string][]byte{chunkHash: chunkData, volumeHash: volumeData} {
		_, _, err = sourceServer.StoreReader(ctx, strings.NewReader(string(data)), hash)
		require.NoError(t, err)
		require.True(t, sourceServer.HasCompleteContent(hash, int64(len(data))))
		require.False(t, destinationServer.HasCompleteContent(hash, int64(len(data))))
	}

	events := &fakeEventRepo{items: []types.CacheRequiredContentItem{
		{
			Kind:       types.CacheContentKindDiskSnapshot,
			Hash:       chunkHash,
			RoutingKey: chunkHash,
			SizeBytes:  int64(len(chunkData)),
			DiskName:   "machine-root",
		},
		{
			Kind:       types.CacheContentKindVolume,
			Hash:       volumeHash,
			RoutingKey: volumeHash,
			SizeBytes:  int64(len(volumeData)),
		},
	}}
	manager := &WorkerCacheManager{
		ctx:               ctx,
		config:            types.AppConfig{Cache: cfg},
		locality:          "locality-a",
		metadataStore:     cache.NewMockCacheMetadataStore(),
		eventRepo:         events,
		client:            destinationClient,
		server:            destinationServer,
		reconcileFailures: make(map[string]time.Time),
	}

	manager.reconcileStub(destinationServer, destinationHost.HostId, cache.RecentStub{
		WorkspaceID: "workspace",
		StubID:      "stub",
	}, newReconcileBudget(10, 0))

	require.True(t, destinationServer.HasCompleteContent(chunkHash, int64(len(chunkData))),
		"disk snapshot chunk must materialize on the non-owner host")
	require.False(t, destinationServer.HasCompleteContent(volumeHash, int64(len(volumeData))),
		"owner-sharded content must not materialize on the non-owner host")
}

func TestReconcileStubFansOutCheckpointsAcrossMatchingHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 1

	localServer, err := cache.NewServerWithOptions(ctx, cfg, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := cache.NewServerWithOptions(ctx, remoteCfg, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("remote-host"))
	require.NoError(t, err)
	remoteAddr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr
	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = remoteAddr
	remoteHost.PrivateAddr = remoteAddr

	clientCfg := cfg
	clientCfg.Server.DiskCacheDir = t.TempDir()
	client, err := cache.NewClientWithHostDirectory(ctx, clientCfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{localHost, remoteHost}, nil
	}), "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })

	routingKey := ""
	require.Eventually(t, func() bool {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("route-%d", i)
			primary, err := client.PrimaryReadHost(key)
			if err == nil && primary.HostId == remoteHost.HostId {
				routingKey = key
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond)

	fake := &fakeEventRepo{items: []types.CacheRequiredContentItem{
		{
			Kind:         types.CacheContentKindCheckpoint,
			Hash:         strings.Repeat("a", 64),
			RoutingKey:   routingKey,
			SizeBytes:    1,
			CheckpointID: "checkpoint-a",
			Accelerator:  "A10G",
		},
		{
			Kind:       types.CacheContentKindClipV2,
			Hash:       strings.Repeat("b", 64),
			RoutingKey: routingKey,
			SizeBytes:  1,
		},
	}}
	manager := &WorkerCacheManager{
		ctx:               ctx,
		locality:          "test",
		accelerator:       "A10G",
		metadataStore:     cache.NewMockCacheMetadataStore(),
		eventRepo:         fake,
		client:            client,
		checkpointRoot:    filepath.Join(t.TempDir(), "checkpoints"),
		reconcileFailures: make(map[string]time.Time),
	}

	checkpointIDs := manager.reconcileStub(localServer, localHost.HostId, cache.RecentStub{WorkspaceID: "workspace", StubID: "stub"}, nil)

	require.Equal(t, []string{"checkpoint-a"}, checkpointIDs)
	require.Len(t, fake.cacheEvents, 1)
	require.Equal(t, types.CacheContentKindCheckpoint, fake.cacheEvents[0].Kind)
	require.Equal(t, localHost.HostId, fake.cacheEvents[0].LogicalHost)
	require.Equal(t, types.CacheAuditStatusMiss, fake.cacheEvents[0].Status)

	fake.mu.Lock()
	fake.cacheEvents = nil
	fake.items[0].Accelerator = "T4"
	fake.mu.Unlock()

	checkpointIDs = manager.reconcileStub(localServer, localHost.HostId, cache.RecentStub{WorkspaceID: "workspace", StubID: "stub"}, nil)
	require.Empty(t, checkpointIDs)
	require.Empty(t, fake.cacheEvents)
}

func TestPruneLocalCheckpointsKeepsActiveAndFresh(t *testing.T) {
	root := t.TempDir()
	manager := &WorkerCacheManager{
		checkpointRoot: root,
		config: types.AppConfig{
			Cache: cache.Config{
				Reconciliation: cache.ReconciliationConfig{RecentStubTTLSeconds: 60},
			},
		},
	}

	require.NoError(t, os.MkdirAll(filepath.Join(root, "keep", checkpointFsDir), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "drop", checkpointFsDir), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "fresh", checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "keep"+checkpointArchiveExtension), []byte("archive"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "drop"+checkpointArchiveExtension), []byte("archive"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "fresh"+checkpointArchiveExtension), []byte("archive"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".extracting"), 0755))

	old := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(root, "drop"), old, old))
	require.NoError(t, os.Chtimes(filepath.Join(root, "drop"+checkpointArchiveExtension), old, old))

	manager.pruneLocalCheckpoints(map[string]struct{}{"keep": struct{}{}})

	require.DirExists(t, filepath.Join(root, "keep"))
	require.FileExists(t, filepath.Join(root, "keep"+checkpointArchiveExtension))
	require.NoDirExists(t, filepath.Join(root, "drop"))
	require.NoFileExists(t, filepath.Join(root, "drop"+checkpointArchiveExtension))
	require.DirExists(t, filepath.Join(root, "fresh"))
	require.FileExists(t, filepath.Join(root, "fresh"+checkpointArchiveExtension))
	require.DirExists(t, filepath.Join(root, ".extracting"))
}

func testClipV1Metadata(t *testing.T) *clipCommon.ClipArchiveMetadata {
	t.Helper()

	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "usr", "bin"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "usr", "bin", "tool"), []byte("tool-data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "config.json"), []byte(`{"ok":true}`), 0644))

	archivePath := filepath.Join(t.TempDir(), "legacy.clip")
	archiver := clip.NewClipArchiver()
	require.NoError(t, archiver.Create(clip.ClipArchiverOptions{
		SourcePath:  src,
		OutputFile:  archivePath,
		ArchivePath: archivePath,
	}))
	meta, err := archiver.ExtractMetadata(archivePath)
	require.NoError(t, err)
	return meta
}

func newTestV1ImageClient(reporter *cacheContentReporter, metadata *cache.FSMetadata) *ImageClient {
	return &ImageClient{
		contentReporter: reporter,
		registry:        &registry.ImageRegistry{ImageFileExtension: registry.LocalImageFileExtension},
		archiveContentMetadata: func(_ context.Context, _ string) (*cache.FSMetadata, error) {
			return metadata, nil
		},
	}
}

func TestClipV1ArchiveRequiredContentIsSingleArchiveObject(t *testing.T) {
	client := newTestV1ImageClient(nil, &cache.FSMetadata{Hash: "archive-hash", Size: 4096})
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub", ImageId: "image"}

	item, ok := client.clipV1ArchiveRequiredContent(context.Background(), request)
	require.True(t, ok)
	require.Equal(t, "archive-hash", item.Hash)
	require.Equal(t, "archive-hash", item.ExpectedHash)
	require.Equal(t, "/images/image.clip", item.RoutingKey)
	require.Equal(t, int64(4096), item.SizeBytes)
	// Origin source is the registry object key so a host that lost the archive
	// can re-fetch it from the same place the image-load path pulls it.
	require.Equal(t, "image.clip", item.Source)
	require.Equal(t, types.CacheContentKindClipV1, item.Kind)
}

func TestClipV1ArchiveRequiredContentUsesDataArchiveForRemoteRegistry(t *testing.T) {
	var requestedPath string
	client := &ImageClient{
		registry: &registry.ImageRegistry{ImageFileExtension: registry.RemoteImageFileExtension},
		archiveContentMetadata: func(_ context.Context, path string) (*cache.FSMetadata, error) {
			requestedPath = path
			return &cache.FSMetadata{Hash: "archive-hash", Size: 4096}, nil
		},
	}
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub", ImageId: "image"}

	item, ok := client.clipV1ArchiveRequiredContent(context.Background(), request)
	require.True(t, ok)
	require.Equal(t, "/images/image.clip", requestedPath)
	require.Equal(t, "/images/image.clip", item.RoutingKey)
	require.Equal(t, "image.clip", item.Source)
	require.Equal(t, "archive-hash", item.Hash)
	require.Equal(t, types.CacheContentKindClipV1, item.Kind)
}

func TestClipV1ArchiveRequiredContentSkipsWhenUncached(t *testing.T) {
	client := newTestV1ImageClient(nil, nil)
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub", ImageId: "image"}

	_, ok := client.clipV1ArchiveRequiredContent(context.Background(), request)
	require.False(t, ok)
}

func TestOCIRequiredContentItemsFromLayers(t *testing.T) {
	meta := testClipV2Metadata()
	ociInfo, ok := ociStorageInfo(meta)
	require.True(t, ok)

	items := ociRequiredContentItems("image-a", ociInfo)
	require.Len(t, items, 2)

	byHash := map[string]types.CacheRequiredContentItem{}
	for _, item := range items {
		byHash[item.Hash] = item
		require.Equal(t, item.Hash, item.RoutingKey)
		require.Equal(t, item.Hash, item.ExpectedHash)
		require.Equal(t, "image-a", item.ImageID)
		require.Equal(t, types.CacheContentKindClipV2, item.Kind)
		require.NotEmpty(t, item.Source)
	}
	require.Equal(t, "registry.example.com/team/image@sha256:layer-a", byHash[strings.Repeat("a", 64)].Source)
	require.Equal(t, "registry.example.com/team/image@sha256:layer-b", byHash[strings.Repeat("b", 64)].Source)
}

func TestOCIRequiredContentItemsSkipsInvalidLayerMetadata(t *testing.T) {
	ociInfo := &clipCommon.OCIStorageInfo{
		RegistryURL: "https://registry.example.com",
		Repository:  "team/image",
		DecompressedHashByLayer: map[string]string{
			"sha256:valid":   strings.Repeat("a", 64),
			"sha256:invalid": "not-a-sha256",
		},
	}

	items := ociRequiredContentItems("image-a", ociInfo)
	require.Len(t, items, 1)
	require.Equal(t, strings.Repeat("a", 64), items[0].Hash)
	require.Equal(t, "image-a", items[0].ImageID)

	ociInfo.Repository = ""
	require.Empty(t, ociRequiredContentItems("image-a", ociInfo))
}

func testClipV2Metadata() *clipCommon.ClipArchiveMetadata {
	return &clipCommon.ClipArchiveMetadata{
		StorageInfo: clipCommon.OCIStorageInfo{
			RegistryURL: "https://registry.example.com",
			Repository:  "team/image",
			DecompressedHashByLayer: map[string]string{
				"sha256:layer-a": strings.Repeat("a", 64),
				"sha256:layer-b": strings.Repeat("b", 64),
			},
		},
	}
}

func reportRequiredContentForTest(client *ImageClient, request *types.ContainerRequest, metadata *clipCommon.ClipArchiveMetadata) {
	if report, ok := client.imageRequiredContent(context.Background(), request, metadata); ok {
		client.publishRequiredContent(request, report)
	}
}

func TestReportRequiredContentClipV1EmitsSingleArchiveObject(t *testing.T) {
	fake := &fakeEventRepo{}
	client := newTestV1ImageClient(newTestReporter(fake), &cache.FSMetadata{Hash: "archive-hash", Size: 4096})
	request := &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
		ImageId:     "image",
	}

	reportRequiredContentForTest(client, request, testClipV1Metadata(t))
	client.contentReporter.flush()

	require.Len(t, fake.pushed, 1)
	event := fake.pushed[0]
	require.Equal(t, types.CacheContentKindClipV1, event.Kind)
	require.Equal(t, "workspace", event.WorkspaceID)
	require.Equal(t, "stub", event.StubID)

	// The whole v1 archive is reconciled as a single content object routed by
	// its cachefs path, not as thousands of per-file index entries.
	require.Len(t, event.Items, 1)
	item := event.Items[0]
	require.Equal(t, types.CacheContentKindClipV1, item.Kind)
	require.Equal(t, "archive-hash", item.Hash)
	require.Equal(t, "archive-hash", item.ExpectedHash)
	require.Equal(t, "/images/image.clip", item.RoutingKey)
	require.Equal(t, int64(4096), item.SizeBytes)
	require.Equal(t, "image.clip", item.Source)
}

func TestReportRequiredContentClipV1SkipsWhenArchiveUncached(t *testing.T) {
	fake := &fakeEventRepo{}
	client := newTestV1ImageClient(newTestReporter(fake), nil)
	request := &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
		ImageId:     "image",
	}

	reportRequiredContentForTest(client, request, testClipV1Metadata(t))
	client.contentReporter.flush()

	require.Empty(t, fake.pushed)
}

func TestReportRequiredContentUsesNestedRequestIDs(t *testing.T) {
	fake := &fakeEventRepo{}
	client := &ImageClient{contentReporter: newTestReporter(fake)}
	request := &types.ContainerRequest{
		ImageId: "image",
		Workspace: types.Workspace{
			ExternalId: "workspace-nested",
		},
		Stub: types.StubWithRelated{
			Stub: types.Stub{ExternalId: "stub-nested"},
		},
	}

	reportRequiredContentForTest(client, request, testClipV2Metadata())
	client.contentReporter.flush()

	require.Len(t, fake.pushed, 1)
	require.Equal(t, "workspace-nested", fake.pushed[0].WorkspaceID)
	require.Equal(t, "stub-nested", fake.pushed[0].StubID)
	require.Equal(t, types.CacheContentKindClipV2, fake.pushed[0].Kind)
	require.Len(t, fake.pushed[0].Items, 2)
}

func TestReportRequiredContentQueuesReconciliation(t *testing.T) {
	fake := &fakeEventRepo{}
	requested := false
	reporter := newTestReporter(fake)
	reporter.reconcileNow = func() { requested = true }
	client := &ImageClient{contentReporter: reporter}
	request := &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
		ImageId:     "image",
	}

	reportRequiredContentForTest(client, request, testClipV2Metadata())
	reporter.flush()

	require.True(t, requested)
	require.Len(t, fake.pushed, 1)
	require.Equal(t, types.CacheContentKindClipV2, fake.pushed[0].Kind)
	require.Len(t, fake.pushed[0].Items, 2)
}

func TestActiveStubsForWorkspaceUsesNestedRequestIDs(t *testing.T) {
	manager := &WorkerCacheManager{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	manager.containerInstances.Set("container", &ContainerInstance{
		Request: &types.ContainerRequest{
			Workspace: types.Workspace{ExternalId: "workspace-nested"},
			Stub: types.StubWithRelated{
				Stub: types.Stub{ExternalId: "stub-nested"},
			},
		},
	})

	require.Equal(t, []string{"stub-nested"}, manager.activeStubsForWorkspace("workspace-nested"))
	require.Empty(t, manager.activeStubsForWorkspace("other-workspace"))
}

func TestContentCachePathUsesFullClipArchiveForS3V1(t *testing.T) {
	client := &ImageClient{
		imageCachePath: "/images/cache",
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{RegistryStore: registry.S3ImageRegistryStore},
		},
	}
	request := &types.ContainerRequest{ImageId: "image", ContainerId: "endpoint-container"}

	require.Equal(t, "/images/cache/image.clip", client.contentCachePath(request, lazyImageArchive{}))

	client.config.ImageService.RegistryStore = registry.LocalImageRegistryStore
	client.config.ImageService.LocalCacheEnabled = true
	require.Equal(t, "/images/cache/image.cache", client.contentCachePath(request, lazyImageArchive{}))
}

func TestEnsureV1ArchiveDataCacheUsesExistingLocalArchive(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("hello"), 0644))

	cacheDir := t.TempDir()
	archivePath := filepath.Join(cacheDir, "image.clip")
	archiver := clip.NewClipArchiver()
	require.NoError(t, archiver.Create(clip.ClipArchiverOptions{
		SourcePath:  src,
		OutputFile:  archivePath,
		ArchivePath: archivePath,
	}))

	client := &ImageClient{
		imageCachePath: cacheDir,
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{RegistryStore: registry.S3ImageRegistryStore},
		},
	}
	path, ok := client.restoreV1ArchiveDataCache(context.Background(), &types.ContainerRequest{ImageId: "image"}, nil)
	require.True(t, ok)
	require.Equal(t, archivePath, path)
}

func TestValidateRestoredImageArchiveAcceptsClipV1WhenDefaultIsV2(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("hello"), 0644))

	archivePath := filepath.Join(t.TempDir(), "legacy.rclip")
	archiver := clip.NewClipArchiver()
	require.NoError(t, archiver.Create(clip.ClipArchiverOptions{
		SourcePath:  src,
		OutputFile:  archivePath,
		ArchivePath: archivePath,
	}))
	info, err := os.Stat(archivePath)
	require.NoError(t, err)

	client := &ImageClient{
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{ClipVersion: uint32(types.ClipVersion2)},
		},
	}
	require.NoError(t, client.validateRestoredImageArchive(archivePath, "legacy-image", info.Size()))
}

func TestPruneStubCodeCacheRemovesExpiredAndTempEntries(t *testing.T) {
	root := t.TempDir()
	oldReady := writeStubCodeCacheEntry(t, root, "old", time.Now().Add(-8*24*time.Hour))
	recentReady := writeStubCodeCacheEntry(t, root, "recent", time.Now())
	tmpDir := filepath.Join(root, "old.tmp.container")
	require.NoError(t, os.MkdirAll(tmpDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "file.py"), []byte("tmp"), 0644))
	oldTempTime := time.Now().Add(-stubCodeTempDirGraceTime - time.Minute)
	require.NoError(t, os.Chtimes(tmpDir, oldTempTime, oldTempTime))
	activeTmpDir := filepath.Join(root, "active.tmp.container")
	require.NoError(t, os.MkdirAll(activeTmpDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(activeTmpDir, "file.py"), []byte("tmp"), 0644))

	pruned, freed := pruneStubCodeCache(root, 7*24*time.Hour)

	require.Equal(t, 2, pruned)
	require.Positive(t, freed)
	require.NoFileExists(t, oldReady)
	require.NoDirExists(t, filepath.Dir(oldReady))
	require.NoDirExists(t, tmpDir)
	require.DirExists(t, activeTmpDir)
	require.FileExists(t, recentReady)
}

func TestPressureEvictStubCodeCacheSkipsActiveTempEntries(t *testing.T) {
	root := t.TempDir()
	oldReady := writeStubCodeCacheEntry(t, root, "old", time.Now().Add(-8*24*time.Hour))
	activeTmpDir := filepath.Join(root, "active.tmp.container")
	require.NoError(t, os.MkdirAll(activeTmpDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(activeTmpDir, "file.py"), []byte("tmp"), 0644))

	evicted, freed := pressureEvictStubCodeCache(root, 1<<30)

	require.Equal(t, 1, evicted)
	require.Positive(t, freed)
	require.NoFileExists(t, oldReady)
	require.NoDirExists(t, filepath.Dir(oldReady))
	require.DirExists(t, activeTmpDir)
}

func TestEvictImageCacheProtectsRecentAndMountedImages(t *testing.T) {
	cacheRoot := t.TempDir()
	mountRoot := filepath.Join(t.TempDir(), "mnt")
	activeHash := strings.Repeat("a", 64)
	recentHash := strings.Repeat("b", 64)
	staleHash := strings.Repeat("c", 64)

	require.NoError(t, os.MkdirAll(filepath.Join(mountRoot, "worker", "active"), 0o755))
	oci := &clipCommon.OCIStorageInfo{DecompressedHashByLayer: map[string]string{"sha256:active": activeHash}}
	meta := testClipV1Metadata(t)
	meta.StorageInfo = oci
	require.NoError(t, clip.NewClipArchiver().CreateRemoteArchive(oci, meta, filepath.Join(cacheRoot, "active.rclip")))
	for _, name := range []string{activeHash, recentHash, staleHash, "recent.rclip", "stale.rclip"} {
		require.NoError(t, os.WriteFile(filepath.Join(cacheRoot, name), []byte("cached"), 0o600))
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, entry := range listImageCacheEntries(cacheRoot) {
		require.NoError(t, os.Chtimes(entry.path, old, old))
	}

	protected := protectedImageCache([]recentStubContent{{items: []types.CacheRequiredContentItem{{
		ImageID: "recent", Hash: recentHash, Kind: types.CacheContentKindClipV2,
	}}}}, nil, cacheRoot, mountRoot)
	evicted, _ := evictImageCache(cacheRoot, protected, time.Now().Add(-time.Hour), 0)

	require.True(t, protected.complete)
	require.Equal(t, 2, evicted)
	require.FileExists(t, filepath.Join(cacheRoot, activeHash))
	require.FileExists(t, filepath.Join(cacheRoot, recentHash))
	require.FileExists(t, filepath.Join(cacheRoot, "active.rclip"))
	require.FileExists(t, filepath.Join(cacheRoot, "recent.rclip"))
	require.NoFileExists(t, filepath.Join(cacheRoot, staleHash))
	require.NoFileExists(t, filepath.Join(cacheRoot, "stale.rclip"))
}

func TestEvictImageCacheProtectsLayersWithoutMountedMetadata(t *testing.T) {
	cacheRoot := t.TempDir()
	mountRoot := filepath.Join(t.TempDir(), "mnt")
	layerHash := strings.Repeat("d", 64)
	require.NoError(t, os.MkdirAll(filepath.Join(mountRoot, "worker", "active-without-metadata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cacheRoot, layerHash), []byte("layer"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheRoot, "stale.clip"), []byte("archive"), 0o600))
	old := time.Now().Add(-2 * time.Hour)
	for _, entry := range listImageCacheEntries(cacheRoot) {
		require.NoError(t, os.Chtimes(entry.path, old, old))
	}

	protected := protectedImageCache(nil, nil, cacheRoot, mountRoot)
	evicted, _ := evictImageCache(cacheRoot, protected, time.Now().Add(-time.Hour), 0)

	require.False(t, protected.complete)
	require.Equal(t, 1, evicted)
	require.FileExists(t, filepath.Join(cacheRoot, layerHash))
	require.NoFileExists(t, filepath.Join(cacheRoot, "stale.clip"))
}

func TestPruneStaleImageMountPaths(t *testing.T) {
	mountRoot := t.TempDir()
	for _, workerID := range []string{"live", "stale"} {
		require.NoError(t, os.Mkdir(filepath.Join(mountRoot, workerID), 0o755))
	}

	pruned, complete := pruneStaleImageMountPathsWithLookup(mountRoot, func(workerID string) (bool, error) {
		return workerID == "live", nil
	})

	require.True(t, complete)
	require.Equal(t, 1, pruned)
	require.DirExists(t, filepath.Join(mountRoot, "live"))
	require.NoDirExists(t, filepath.Join(mountRoot, "stale"))
}

func TestPruneStaleImageMountPathsPreservesWorkersWithUnknownLiveness(t *testing.T) {
	mountRoot := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(mountRoot, "unknown"), 0o755))

	pruned, complete := pruneStaleImageMountPathsWithLookup(mountRoot, func(string) (bool, error) {
		return false, errors.New("gateway unavailable")
	})

	require.False(t, complete)
	require.Zero(t, pruned)
	require.DirExists(t, filepath.Join(mountRoot, "unknown"))
}

func writeStubCodeCacheEntry(t *testing.T, root, name string, readyTime time.Time) string {
	t.Helper()
	dir := filepath.Join(root, name)
	readyPath := filepath.Join(dir, stubCodeReadyMarker)
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.py"), []byte("print('ok')"), 0644))
	require.NoError(t, os.WriteFile(readyPath, []byte("ok"), 0644))
	require.NoError(t, os.Chtimes(readyPath, readyTime, readyTime))
	return readyPath
}
