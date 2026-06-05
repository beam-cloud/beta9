package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
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
		reported:  make(map[string]struct{}),
	}
}

type claimedMetadataStore struct {
	cache.CacheMetadataStore
	claimed bool
	marked  int
	recent  int
}

type localityRecentMetadataStore struct {
	*cache.MockCacheMetadataStore
	stubs  map[string][]cache.RecentStub
	listed []string
}

func (m *localityRecentMetadataStore) ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]cache.RecentStub, error) {
	m.listed = append(m.listed, locality)
	return append([]cache.RecentStub(nil), m.stubs[locality]...), nil
}

func (m *claimedMetadataStore) MarkStubReported(context.Context, string, string, time.Duration) (bool, error) {
	m.marked++
	return m.claimed, nil
}

func (m *claimedMetadataStore) AddRecentStub(context.Context, string, string, string, time.Duration) error {
	m.recent++
	return nil
}

func TestReporterGeneratesOncePerStub(t *testing.T) {
	r := newTestReporter(&fakeEventRepo{})

	// With no Redis metadata, the in-memory guard ensures one-time generation.
	require.True(t, r.shouldGenerateRequiredContent("stub-a"))
	require.False(t, r.shouldGenerateRequiredContent("stub-a"))
	require.True(t, r.shouldGenerateRequiredContent("stub-b"))
}

func TestReporterRedisMarkerIsAdvisory(t *testing.T) {
	r := newTestReporter(&fakeEventRepo{})
	r.metadata = &claimedMetadataStore{claimed: false}

	require.True(t, r.shouldGenerateRequiredContent("stub-a"))
	require.False(t, r.shouldGenerateRequiredContent("stub-a"))
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

func TestReporterMarksRedisAfterSuccessfulRequiredContentWrite(t *testing.T) {
	fake := &fakeEventRepo{}
	metadata := &claimedMetadataStore{claimed: true}
	r := newTestReporter(fake)
	r.metadata = metadata

	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{{Hash: "h1"}})
	require.Zero(t, metadata.marked)

	r.flush()
	require.Len(t, fake.pushed, 1)
	require.Equal(t, 1, metadata.marked)
}

func TestReporterRetriesRequiredContentWhenEventWriteFails(t *testing.T) {
	fake := &fakeEventRepo{err: errors.New("s2 unavailable")}
	metadata := &claimedMetadataStore{claimed: true}
	r := newTestReporter(fake)
	r.metadata = metadata

	r.reportItems("ws", "stub", types.CacheContentKindClipV1, []types.CacheRequiredContentItem{{Hash: "h1"}})
	r.flush()
	require.Empty(t, fake.pushed)
	require.Zero(t, metadata.marked)

	fake.err = nil
	r.flush()
	require.Len(t, fake.pushed, 1)
	require.Equal(t, 1, metadata.marked)
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

func TestCheckpointAcceleratorMatch(t *testing.T) {
	manager := &WorkerCacheManager{accelerator: "A10G"}

	require.True(t, manager.checkpointAcceleratorMatches(types.CacheRequiredContentItem{Accelerator: "a10g"}))
	require.True(t, manager.checkpointAcceleratorMatches(types.CacheRequiredContentItem{}))
	require.False(t, manager.checkpointAcceleratorMatches(types.CacheRequiredContentItem{Accelerator: "T4"}))
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

	manager.reconcileOnce()

	require.Equal(t, []string{"locality-b"}, metadata.listed)
	require.Equal(t, []string{"workspace-b|stub-b"}, fake.readKeys)
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
	require.Len(t, fake.pushed, 1)
	require.Equal(t, types.CacheContentKindCheckpoint, fake.pushed[0].Kind)
	require.Equal(t, "workspace", fake.pushed[0].WorkspaceID)
	require.Equal(t, "stub", fake.pushed[0].StubID)
	require.Equal(t, checkpoint.CacheHash, fake.pushed[0].Items[0].Hash)
	require.Equal(t, checkpoint.CheckpointId, fake.pushed[0].Items[0].CheckpointID)
	require.Equal(t, checkpoint.Accelerator, fake.pushed[0].Items[0].Accelerator)
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

	checkpointIDs := manager.reconcileStub(localServer, localHost.HostId, cache.RecentStub{WorkspaceID: "workspace", StubID: "stub"})

	require.Equal(t, []string{"checkpoint-a"}, checkpointIDs)
	require.Len(t, fake.cacheEvents, 1)
	require.Equal(t, types.CacheContentKindCheckpoint, fake.cacheEvents[0].Kind)
	require.Equal(t, localHost.HostId, fake.cacheEvents[0].LogicalHost)
	require.Equal(t, types.CacheAuditStatusMiss, fake.cacheEvents[0].Status)

	fake.mu.Lock()
	fake.cacheEvents = nil
	fake.items[0].Accelerator = "T4"
	fake.mu.Unlock()

	checkpointIDs = manager.reconcileStub(localServer, localHost.HostId, cache.RecentStub{WorkspaceID: "workspace", StubID: "stub"})
	require.Empty(t, checkpointIDs)
	require.Empty(t, fake.cacheEvents)
}

func TestPruneLocalCheckpointsKeepsActive(t *testing.T) {
	root := t.TempDir()
	manager := &WorkerCacheManager{checkpointRoot: root}

	require.NoError(t, os.MkdirAll(filepath.Join(root, "keep", checkpointFsDir), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "drop", checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "keep"+checkpointArchiveExtension), []byte("archive"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "drop"+checkpointArchiveExtension), []byte("archive"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".extracting"), 0755))

	manager.pruneLocalCheckpoints(map[string]struct{}{"keep": struct{}{}})

	require.DirExists(t, filepath.Join(root, "keep"))
	require.FileExists(t, filepath.Join(root, "keep"+checkpointArchiveExtension))
	require.NoDirExists(t, filepath.Join(root, "drop"))
	require.NoFileExists(t, filepath.Join(root, "drop"+checkpointArchiveExtension))
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

	items := ociRequiredContentItems(ociInfo)
	require.Len(t, items, 2)

	byHash := map[string]types.CacheRequiredContentItem{}
	for _, item := range items {
		byHash[item.Hash] = item
		require.Equal(t, item.Hash, item.RoutingKey)
		require.Equal(t, item.Hash, item.ExpectedHash)
		require.Equal(t, types.CacheContentKindClipV2, item.Kind)
		require.NotEmpty(t, item.Source)
	}
	require.Equal(t, "registry.example.com/team/image@sha256:layer-a", byHash["hash-a"].Source)
	require.Equal(t, "registry.example.com/team/image@sha256:layer-b", byHash["hash-b"].Source)
}

func testClipV2Metadata() *clipCommon.ClipArchiveMetadata {
	return &clipCommon.ClipArchiveMetadata{
		StorageInfo: clipCommon.OCIStorageInfo{
			RegistryURL: "https://registry.example.com",
			Repository:  "team/image",
			DecompressedHashByLayer: map[string]string{
				"sha256:layer-a": "hash-a",
				"sha256:layer-b": "hash-b",
			},
		},
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

	client.reportRequiredContent(context.Background(), request, testClipV1Metadata(t))
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

	client.reportRequiredContent(context.Background(), request, testClipV1Metadata(t))
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

	client.reportRequiredContent(context.Background(), request, testClipV2Metadata())
	client.contentReporter.flush()

	require.Len(t, fake.pushed, 1)
	require.Equal(t, "workspace-nested", fake.pushed[0].WorkspaceID)
	require.Equal(t, "stub-nested", fake.pushed[0].StubID)
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
