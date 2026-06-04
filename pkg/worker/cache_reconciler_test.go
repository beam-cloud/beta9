package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	mu     sync.Mutex
	pushed []types.EventStubCacheRequiredContentSchema
	err    error
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
}

func (m *claimedMetadataStore) MarkStubReported(context.Context, string, string, time.Duration) (bool, error) {
	m.marked++
	return m.claimed, nil
}

func (m *claimedMetadataStore) AddRecentStub(context.Context, string, string, string, time.Duration) error {
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

	r.reportItems("ws", "stub", types.CacheContentKindClipV1Archive, []types.CacheRequiredContentItem{{Hash: "archive", RoutingKey: "/images/archive.clip"}})
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

	r.reportItems("ws", "stub", types.CacheContentKindClipV1Archive, []types.CacheRequiredContentItem{{Hash: "archive", RoutingKey: "/images/archive.clip"}})
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

func TestRequiredContentItemsClipV1FromArchive(t *testing.T) {
	meta := testClipV1Metadata(t)
	items, kind := requiredContentItems(meta)
	require.Equal(t, types.CacheContentKindClipV1, kind)
	require.Len(t, items, 2)
	for _, item := range items {
		require.NotEmpty(t, item.Hash)
		require.Equal(t, item.Hash, item.RoutingKey)
		require.Equal(t, item.Hash, item.ExpectedHash)
		require.Equal(t, types.CacheContentKindClipV1, item.Kind)
		require.NotEmpty(t, item.Source)
	}
}

func TestRequiredContentItemsClipV2FromOCILayers(t *testing.T) {
	meta := testClipV2Metadata()

	items, kind := requiredContentItems(meta)
	require.Equal(t, types.CacheContentKindClipV2, kind)
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

func TestReportRequiredContentClipV1IncludesArchiveItem(t *testing.T) {
	fake := &fakeEventRepo{}
	client := &ImageClient{contentReporter: newTestReporter(fake)}
	request := &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
		ImageId:     "image",
	}

	client.reportRequiredContent(request, testClipV1Metadata(t))
	client.contentReporter.flush()

	require.Len(t, fake.pushed, 1)
	event := fake.pushed[0]
	require.Equal(t, types.CacheContentKindClipV1Archive, event.Kind)
	require.Equal(t, "workspace", event.WorkspaceID)
	require.Equal(t, "stub", event.StubID)
	require.Len(t, event.Items, 1)

	archiveItem := event.Items[0]
	require.Equal(t, types.CacheContentKindClipV1Archive, archiveItem.Kind)
	require.Equal(t, "clip-v1-archive:image", archiveItem.Hash)
	require.Equal(t, "/images/image.clip", archiveItem.RoutingKey)
	require.Equal(t, "image.clip", archiveItem.Source)
}

func TestReportRequiredContentClipV1DoesNotEmitIndexEntries(t *testing.T) {
	fake := &fakeEventRepo{}
	client := &ImageClient{contentReporter: newTestReporter(fake)}
	request := &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "stub",
		ImageId:     "image",
	}

	client.reportRequiredContent(request, testClipV1Metadata(t))
	client.contentReporter.flush()

	require.Len(t, fake.pushed, 1)
	require.Equal(t, types.CacheContentKindClipV1Archive, fake.pushed[0].Kind)
	require.Len(t, fake.pushed[0].Items, 1)
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

	client.reportRequiredContent(request, testClipV2Metadata())
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
