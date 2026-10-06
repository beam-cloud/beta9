package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLinkBlobInfoCacheAdoptsThenProtectsTheSharedCopy(t *testing.T) {
	root := t.TempDir()
	local := filepath.Join(root, "containers", "cache")
	target := filepath.Join(root, "persistent", "blob-info-cache")
	require.NoError(t, os.MkdirAll(filepath.Join(local, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(local, "blob-info-cache-v1.sqlite"), []byte("first pod"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(local, "nested", "entry"), []byte("nested"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Dir(target), 0o700))

	// No shared copy yet: the pod's directory becomes it, private to root.
	require.NoError(t, linkBlobInfoCache(local, target))
	linked, err := os.Readlink(local)
	require.NoError(t, err)
	require.Equal(t, target, linked)
	require.FileExists(t, filepath.Join(target, "nested", "entry"))
	info, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())

	// Already linked: nothing to do.
	require.NoError(t, linkBlobInfoCache(local, target))

	// A later pod that cached locally before linking must not overwrite the
	// shared index; its directory is set aside instead.
	require.NoError(t, os.Remove(local))
	require.NoError(t, os.MkdirAll(local, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(local, "blob-info-cache-v1.sqlite"), []byte("second pod"), 0o600))
	require.NoError(t, linkBlobInfoCache(local, target))
	shared, err := os.ReadFile(filepath.Join(target, "blob-info-cache-v1.sqlite"))
	require.NoError(t, err)
	require.Equal(t, "first pod", string(shared))
	asides, err := filepath.Glob(filepath.Join(filepath.Dir(local), "cache.pod-*"))
	require.NoError(t, err)
	require.Len(t, asides, 1)
	require.FileExists(t, filepath.Join(asides[0], "blob-info-cache-v1.sqlite"))
}

func TestImageIndexProgressReporterEmitsMonotonicAggregateUpdates(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	reporter := newImageIndexProgressReporter(logger)
	reporter.lastReported = time.Now().Add(-imageIndexProgressInterval)

	reporter.report(clip.OCIIndexProgress{
		LayerIndex:      3,
		LayerDigest:     "layer-3",
		Stage:           "completed",
		CompletedLayers: 3,
		TotalLayers:     10,
		BytesProcessed:  2 << 30,
		Source:          clip.LayerSourceLocalLayout,
	})
	reporter.report(clip.OCIIndexProgress{
		LayerIndex:      2,
		LayerDigest:     "layer-2",
		Stage:           "completed",
		CompletedLayers: 2,
		TotalLayers:     10,
		BytesProcessed:  1 << 30,
		Source:          clip.LayerSourceIndexCache,
	})
	reporter.finish()

	logs := output.String()
	require.Contains(t, logs, "Image indexing: 3/10 layers complete")
	require.NotContains(t, logs, "Image indexing: 2/10")
	require.Contains(t, logs, "Image indexed in")
	require.Contains(t, logs, "1 cached")
}

func TestImageIndexProgressReporterCoalescesRapidUpdates(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	reporter := newImageIndexProgressReporter(logger)

	for layer := 1; layer <= 9; layer++ {
		reporter.report(clip.OCIIndexProgress{
			LayerIndex:      layer,
			LayerDigest:     fmt.Sprintf("layer-%d", layer),
			Stage:           "completed",
			CompletedLayers: layer,
			TotalLayers:     10,
			BytesProcessed:  1 << 30,
			Source:          clip.LayerSourceIndexCache,
		})
	}
	require.NotContains(t, output.String(), "Image indexing:")

	reporter.lastReported = time.Now().Add(-imageIndexProgressInterval)
	reporter.report(clip.OCIIndexProgress{
		LayerIndex:      9,
		LayerDigest:     "layer-9",
		Stage:           "completed",
		CompletedLayers: 9,
		TotalLayers:     10,
		BytesProcessed:  1 << 30,
		Source:          clip.LayerSourceIndexCache,
	})
	reporter.finish()

	logs := output.String()
	require.Contains(t, logs, "Image indexing: 9/10 layers complete")
	require.Equal(t, 1, bytes.Count(output.Bytes(), []byte("Image indexing:")))
	require.Contains(t, logs, "Image indexed in")
	require.Contains(t, logs, "9 cached")
}

func TestImageRegistryPullFailureLogLevel(t *testing.T) {
	var buf bytes.Buffer
	previous := zerologlog.Logger
	zerologlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() {
		zerologlog.Logger = previous
	})

	dockerfile := "FROM ubuntu:22.04"
	logImageRegistryPullFailure(errors.New("missing"), "build-image", &types.ContainerRequest{
		BuildOptions: types.BuildOptions{Dockerfile: &dockerfile},
	})
	require.Contains(t, buf.String(), `"level":"debug"`)
	require.Contains(t, buf.String(), "continuing with build request path")
	require.NotContains(t, buf.String(), `"level":"error"`)

	buf.Reset()
	logImageRegistryPullFailure(errors.New("missing"), "runtime-image", &types.ContainerRequest{})
	require.Contains(t, buf.String(), `"level":"error"`)
	require.Contains(t, buf.String(), "failed to pull image from registry")
}

func TestEmbeddedImageCacheFallbackLogLevel(t *testing.T) {
	var buf bytes.Buffer
	previous := zerologlog.Logger
	zerologlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() {
		zerologlog.Logger = previous
	})

	dockerfile := "FROM ubuntu:22.04"
	logEmbeddedImageCacheFallback(errors.New("cache miss"), "build-image", &types.ContainerRequest{
		BuildOptions: types.BuildOptions{Dockerfile: &dockerfile},
	})
	require.Contains(t, buf.String(), `"level":"debug"`)
	require.Contains(t, buf.String(), "continuing with build request path")
	require.NotContains(t, buf.String(), `"level":"warn"`)

	buf.Reset()
	logEmbeddedImageCacheFallback(errors.New("cache unavailable"), "runtime-image", &types.ContainerRequest{})
	require.Contains(t, buf.String(), `"level":"warn"`)
	require.Contains(t, buf.String(), "falling back to registry")
}

func TestLazyMountOptionsUsesWholeArchiveCacheOnlyForV1(t *testing.T) {
	client := &ImageClient{
		cacheClient:    &cache.Client{},
		imageCachePath: "/images/cache",
		config: types.AppConfig{ImageService: types.ImageServiceConfig{
			RegistryStore: registry.S3ImageRegistryStore,
		}},
	}
	request := &types.ContainerRequest{ImageId: "image-v1"}

	options := client.lazyMountOptions(context.Background(), request, lazyImageArchive{})

	require.Equal(t, "/images/cache/image-v1.clip", options.CachePath)
	require.Nil(t, options.ContentCache)
	require.False(t, options.ContentCacheAvailable)
}

func TestLazyMountOptionsRetainsPerLayerCacheForOCI(t *testing.T) {
	client := &ImageClient{
		cacheClient:    &cache.Client{},
		imageCachePath: "/images/cache",
		v2ImageRefs:    common.NewSafeMap[string](),
	}
	request := &types.ContainerRequest{ImageId: "image-v2"}

	options := client.lazyMountOptions(context.Background(), request, lazyImageArchive{storageMode: "oci"})

	require.NotNil(t, options.ContentCache)
	require.True(t, options.ContentCacheAvailable)
}

func TestSuccessfulImageLoadActivatesExecutingLocality(t *testing.T) {
	reporter := &cacheContentReporter{
		metadata: cache.NewMockCacheMetadataStore(),
		recent:   make(map[reporterStubKey]struct{}),
	}
	client := &ImageClient{contentReporter: reporter}
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub"}

	client.recordSuccessfulImageLoad(context.Background(), request, nil)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	require.Contains(t, reporter.recent, reporterStubKey{workspaceID: "workspace", stubID: "stub"})
}

func TestMountedImageFirstActivationChecksAllCachedContentOwners(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner, _ := newCheckpointCacheForTest(t, ctx)
	hashes := make(map[string]string)
	for _, name := range []string{"layer-a", "layer-b", "canonical", "compact", "legacy"} {
		hash, _, err := owner.StoreReader(ctx, strings.NewReader(name), "")
		require.NoError(t, err)
		hashes[name] = hash
	}
	var mu sync.Mutex
	checked := make(map[string]bool)
	rpc := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		if request, ok := req.(*pb.CacheHasContentRequest); ok && err == nil {
			mu.Lock()
			checked[request.Hash] = response.(*pb.CacheHasContentResponse).Exists
			mu.Unlock()
		}
		return response, err
	}))
	pb.RegisterCacheServer(rpc, owner)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go rpc.Serve(listener)
	t.Cleanup(rpc.Stop)
	host := *owner.Host()
	host.Addr, host.PrivateAddr = listener.Addr().String(), listener.Addr().String()
	cfg := testCacheManagerConfig(t.TempDir()).Cache
	contentCache, err := cache.NewClientWithHostDirectory(ctx, cfg, cache.NewMockCacheMetadataStore(), testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		return []*cache.Host{&host}, nil
	}), "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, contentCache.Cleanup()) })
	require.Eventually(t, func() bool { return len(contentCache.RankedReadHosts("probe")) > 0 }, time.Second, time.Millisecond)

	for _, format := range []string{"oci", "legacy"} {
		t.Run(format, func(t *testing.T) {
			client := &ImageClient{
				cacheClient: contentCache, contentReporter: newTestReporter(&fakeEventRepo{}), imageCachePath: t.TempDir(),
				registry:           &registry.ImageRegistry{ImageFileExtension: registry.RemoteImageFileExtension},
				mountedFuseServers: common.NewSafeMap[*fuse.Server](),
				archiveContentMetadata: func(_ context.Context, path string) (*cache.FSMetadata, error) {
					name := "canonical"
					if strings.HasSuffix(path, ".batch") {
						name = "compact"
					} else if strings.HasSuffix(path, ".clip") {
						name = "legacy"
					}
					return &cache.FSMetadata{Hash: hashes[name]}, nil
				},
			}
			expected := []string{"canonical", "legacy"}
			if format == "oci" {
				meta := testClipV2Metadata()
				info := meta.StorageInfo.(clipCommon.OCIStorageInfo)
				info.DecompressedHashByLayer = map[string]string{"a": hashes["layer-a"], "b": hashes["layer-b"]}
				meta.StorageInfo, meta.OriginalArchiveHash = info, hashes["canonical"]
				client.archiveMetadata.Store("image", &imageRecord{metadata: meta})
				expected = []string{"layer-a", "layer-b", "canonical", "compact"}
			}
			client.mountedFuseServers.Set("image", nil)
			mu.Lock()
			clear(checked)
			mu.Unlock()
			startCtx, stop := context.WithCancel(ctx)
			stop() // A canceled first caller must not poison the shared activation.
			_, err := client.PullLazy(startCtx, &types.ContainerRequest{ImageId: "image", StubId: format, WorkspaceId: "workspace"})
			require.NoError(t, err)
			mu.Lock()
			for _, name := range expected {
				require.True(t, checked[hashes[name]], "%s must reach its owning cache before the mounted hit returns", name)
			}
			mu.Unlock()
			if format == "oci" {
				require.Eventually(t, func() bool {
					client.contentReporter.mu.Lock()
					defer client.contentReporter.mu.Unlock()
					return len(client.contentReporter.reported) == 0
				}, time.Second, time.Millisecond)
			}
		})
	}
}

func TestOCIRequiredContentIncludesMetadataBeforeCachePublication(t *testing.T) {
	fake := &fakeEventRepo{}
	client := &ImageClient{
		contentReporter: newTestReporter(fake), imageCachePath: t.TempDir(),
		registry: &registry.ImageRegistry{ImageFileExtension: registry.RemoteImageFileExtension},
	}
	client.contentReporter.metadata = cache.NewMockCacheMetadataStore()
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub", ImageId: "image"}
	metadata := testClipV2Metadata()
	client.recordSuccessfulImageLoad(context.Background(), request, metadata)
	require.Eventually(t, func() bool {
		client.contentReporter.mu.Lock()
		defer client.contentReporter.mu.Unlock()
		return len(client.contentReporter.reported) == 0
	}, time.Second, time.Millisecond)
	client.contentReporter.mu.Lock()
	_, recent := client.contentReporter.recent[reporterStubKey{workspaceID: "workspace", stubID: "stub"}]
	client.contentReporter.mu.Unlock()
	require.True(t, recent, "successful starts refresh recency even when required-content generation fails")
	data := []byte("verified image metadata")
	require.NoError(t, os.WriteFile(client.localArchivePath("image"), data, 0600))

	client.recordSuccessfulImageLoad(context.Background(), request, metadata)
	require.Eventually(t, func() bool {
		client.contentReporter.mu.Lock()
		defer client.contentReporter.mu.Unlock()
		return len(client.contentReporter.pending) == 1
	}, time.Second, time.Millisecond)
	client.contentReporter.flush()
	require.Len(t, fake.pushed, 1)
	require.Len(t, fake.pushed[0].Items, 3)
	for _, item := range fake.pushed[0].Items {
		if item.Kind == types.CacheContentKindClipV1 {
			require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(data)), item.Hash)
			require.Equal(t, item.Hash, item.ExpectedHash)
			require.Equal(t, int64(len(data)), item.SizeBytes)
			require.Equal(t, "/images/image.rclip", item.RoutingKey)
			require.Equal(t, "image.rclip", item.Source)
			return
		}
	}
	t.Fatal("required metadata archive is missing")
}

func TestFastMetadataCacheRestoreAndLegacyFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, contentCache := newCheckpointCacheForTest(t, ctx)
	archiver := clip.NewClipArchiver()
	originalPath := filepath.Join(t.TempDir(), "image.rclip")
	metadata := testClipV1Metadata(t)
	oci := testClipV2Metadata().StorageInfo.(clipCommon.OCIStorageInfo)
	oci.Layers = []string{"sha256:layer-a", "sha256:layer-b"}
	oci.ImageMetadata = &clipCommon.ImageMetadata{Architecture: "amd64", Os: "linux"}
	require.NoError(t, archiver.CreateRemoteArchive(oci, metadata, originalPath))
	fastPath := originalPath + ".batch"
	require.NoError(t, archiver.TranscodeMetadata(originalPath, fastPath, nil))
	client := &ImageClient{
		cacheClient: contentCache, imageCachePath: t.TempDir(),
		registry: &registry.ImageRegistry{ImageFileExtension: registry.RemoteImageFileExtension},
	}
	client.publishImageArchiveToEmbeddedCache(fastPath, "image")
	request := &types.ContainerRequest{ImageId: "image"}
	archive, err := client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Equal(t, client.localArchivePath("image")+".batch", archive.path)
	fastMetadata := archive.metadata
	require.NoFileExists(t, client.localArchivePath("image"))
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Same(t, fastMetadata, archive.metadata, "unchanged verified archive reuses its decoded metadata")
	report, ok := client.imageRequiredContent(ctx, request, archive.metadata)
	require.True(t, ok)
	require.Len(t, report.items, 4)
	for _, item := range report.items[2:] {
		require.Equal(t, "image.rclip", item.Source)
		require.Equal(t, types.CacheContentKindClipV1, item.Kind)
		require.True(t, server.HasCompleteContent(item.Hash, item.SizeBytes) || item.RoutingKey == "/images/image.rclip")
	}
	require.Equal(t, "/images/image.rclip", report.items[2].RoutingKey)
	require.Equal(t, "/images/image.rclip.batch", report.items[3].RoutingKey)
	original, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(original)), report.items[2].Hash)
	require.Equal(t, int64(len(original)), report.items[2].SizeBytes)

	// Corrupt derived metadata falls back to the canonical archive.
	client.imageCachePath = t.TempDir()
	require.NoError(t, os.WriteFile(client.localArchivePath("image"), original, 0600))
	require.NoError(t, os.WriteFile(client.localArchivePath("image")+".batch", []byte("partial"), 0600))
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Equal(t, client.localArchivePath("image"), archive.path)
	require.NoFileExists(t, archive.path+".batch")
	_, ok = client.imageRequiredContent(ctx, request, fastMetadata)
	require.True(t, ok, "rebuild missing derived file with previously cached fast metadata")
	require.FileExists(t, archive.path+".batch")
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Equal(t, client.localArchivePath("image")+".batch", archive.path)
	info, err := os.Stat(archive.path)
	require.NoError(t, err)
	replacement := archive.path + ".replacement"
	require.NoError(t, os.WriteFile(replacement, bytes.Repeat([]byte{'x'}, int(info.Size())), 0600))
	require.NoError(t, os.Chtimes(replacement, info.ModTime(), info.ModTime()))
	require.NoError(t, os.Rename(replacement, archive.path))
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Equal(t, client.localArchivePath("image"), archive.path, "same-size replacement invalidates the verified-file memo")
	// A directory at the derived path deterministically prevents publication.
	require.NoError(t, os.Mkdir(archive.path+".batch", 0700))
	report, ok = client.imageRequiredContent(ctx, request, testClipV2Metadata())
	require.False(t, ok, "retain canonical content and retry failed derived metadata")
	require.Len(t, report.items, 3)
	fake := &fakeEventRepo{}
	client.contentReporter = newTestReporter(fake)
	request.WorkspaceId, request.StubId = "workspace", "stub"
	client.recordSuccessfulImageLoad(ctx, request, archive.metadata)
	require.Eventually(t, func() bool {
		client.contentReporter.mu.Lock()
		defer client.contentReporter.mu.Unlock()
		return len(client.contentReporter.pending) == 1 && len(client.contentReporter.reported) == 0
	}, time.Second, time.Millisecond)
	client.contentReporter.flush()
	require.Len(t, fake.pushed, 1)
	require.Len(t, fake.pushed[0].Items, 3, "canonical metadata and layers remain available")
	require.NoError(t, os.Remove(archive.path+".batch"))
	client.recordSuccessfulImageLoad(ctx, request, archive.metadata)
	require.Eventually(t, func() bool {
		client.contentReporter.mu.Lock()
		defer client.contentReporter.mu.Unlock()
		for _, items := range client.contentReporter.pending {
			return len(items) == 4 && len(client.contentReporter.reported) == 1
		}
		return false
	}, time.Second, time.Millisecond)
	client.contentReporter.flush()
	require.Len(t, fake.pushed, 2)
	require.Len(t, fake.pushed[1].Items, 4, "a later successful start retries compact metadata")
	memoCount := 0
	client.archiveMetadata.Range(func(_, _ any) bool { memoCount++; return true })
	require.Equal(t, 1, memoCount, "canonical and derived metadata share one memo per image")
	var parses sync.WaitGroup
	for _, meta := range []*clipCommon.ClipArchiveMetadata{fastMetadata, archive.metadata} {
		parses.Go(func() {
			for range 20 {
				client.cacheOCIMetadata("image", meta, &imageRecord{metadata: meta})
			}
		})
	}
	parses.Wait()
	value, ok := client.archiveMetadata.Load("image")
	require.True(t, ok)
	cached := client.cachedImageMetadata("image")
	require.Same(t, cached, value.(*imageRecord).metadata, "concurrent source swaps retain one metadata tree")
}

func TestLocalImageArchiveReadyPreservesInProgressPlaceholder(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "image.clip")
	require.NoError(t, os.WriteFile(archivePath, nil, 0o600))

	client := &ImageClient{}
	require.False(t, client.localImageArchiveReady(archivePath, "image"))
	require.FileExists(t, archivePath)
}

func TestValidateRestoredOCIArchiveSizeLimit(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "image.rclip")
	layer := "sha256:" + strings.Repeat("a", 64)
	oci := &clipCommon.OCIStorageInfo{
		Layers:                  []string{layer},
		DecompressedHashByLayer: map[string]string{layer: strings.Repeat("b", 64)},
		ImageMetadata:           &clipCommon.ImageMetadata{Architecture: "amd64", Os: "linux"},
	}
	metadata := testClipV1Metadata(t)
	archiver := clip.NewClipArchiver()
	require.NoError(t, archiver.CreateRemoteArchive(oci, metadata, archivePath))
	client := &ImageClient{}
	require.NoError(t, client.validateRestoredImageArchive(archivePath, "image", 512<<20))
	require.ErrorContains(t, client.validateRestoredImageArchive(archivePath, "image", (512<<20)+1), "unexpectedly large")

	oci.ImageMetadata = nil
	require.NoError(t, archiver.CreateRemoteArchive(oci, metadata, archivePath))
	require.ErrorContains(t, client.validateRestoredImageArchive(archivePath, "image", 512<<20), "missing embedded image metadata")
}

func createTestLegacyImageArchive(t *testing.T, archivePath string) ([]byte, *cache.FSMetadata) {
	t.Helper()
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("image data"), 0o600))
	require.NoError(t, clip.NewClipArchiver().Create(clip.ClipArchiverOptions{
		SourcePath: source, OutputFile: archivePath, ArchivePath: archivePath,
	}))
	data, err := os.ReadFile(archivePath)
	require.NoError(t, err)
	return data, &cache.FSMetadata{Hash: fmt.Sprintf("%x", sha256.Sum256(data)), Size: uint64(len(data))}
}

func TestWaitForV1ArchiveCacheRetriesUnavailableReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, contentCache := newCheckpointCacheForTest(t, ctx)
	cacheDir := t.TempDir()
	data, metadata := createTestLegacyImageArchive(t, filepath.Join(t.TempDir(), "image.clip"))
	localPath := filepath.Join(cacheDir, "image.clip")
	lookups := 0
	client := &ImageClient{
		imageCachePath: cacheDir,
		cacheClient:    contentCache,
		archiveContentMetadata: func(context.Context, string) (*cache.FSMetadata, error) {
			lookups++
			if lookups == 2 {
				_, _, err := server.StoreReader(ctx, bytes.NewReader(data), metadata.Hash)
				require.NoError(t, err)
			} else if lookups == 3 {
				// Bound a broken replica retry without waiting for its 30-minute timeout.
				require.NoError(t, os.WriteFile(localPath, data, 0o600))
			}
			return metadata, nil
		},
	}

	item, err := client.waitForV1ArchiveCache("image")
	require.NoError(t, err)
	require.Equal(t, metadata.Hash, item.Hash)
	require.Equal(t, 2, lookups, "retry the replica without requiring a local archive")
	require.NoFileExists(t, localPath)
}

func TestWaitForV1ArchiveCacheSeedsExistingMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, contentCache := newCheckpointCacheForTest(t, ctx)
	cacheDir := t.TempDir()
	archivePath := filepath.Join(cacheDir, "image.clip")
	data, metadata := createTestLegacyImageArchive(t, archivePath)
	client := &ImageClient{
		imageCachePath: cacheDir,
		cacheClient:    contentCache,
		archiveContentMetadata: func(context.Context, string) (*cache.FSMetadata, error) {
			return metadata, nil
		},
	}

	require.False(t, server.HasCompleteContent(metadata.Hash, int64(metadata.Size)))
	item, err := client.waitForV1ArchiveCache("image")
	require.NoError(t, err)
	require.Equal(t, metadata.Hash, item.Hash)
	require.True(t, server.HasCompleteContent(item.Hash, item.SizeBytes))

	peerCache := newTestPeerClient(t, ctx, server.Host())
	peer := &ImageClient{cacheClient: peerCache}
	restoredPath := filepath.Join(t.TempDir(), "image.clip")
	require.NoError(t, peer.writeImageArchiveFromContentCache(ctx, restoredPath, "image", item.Hash, item.SizeBytes, item.RoutingKey))
	restored, err := os.ReadFile(restoredPath)
	require.NoError(t, err)
	require.Equal(t, data, restored)

	require.NoError(t, os.Remove(archivePath))
	cachedItem, err := client.waitForV1ArchiveCache("image")
	require.NoError(t, err)
	require.Equal(t, item, cachedItem)

	// Remote legacy metadata is a separate required object; retry a failed report.
	client.registry = &registry.ImageRegistry{ImageFileExtension: registry.RemoteImageFileExtension}
	fake := &fakeEventRepo{}
	client.contentReporter = newTestReporter(fake)
	request := &types.ContainerRequest{ImageId: "image", WorkspaceId: "workspace", StubId: "stub"}
	lookups := 0
	client.archiveContentMetadata = func(context.Context, string) (*cache.FSMetadata, error) {
		lookups++
		if lookups > 1 {
			return nil, errors.New("metadata unavailable")
		}
		return metadata, nil
	}
	client.completeV1ArchiveCache(request)
	client.contentReporter.flush()
	require.Len(t, fake.pushed, 1)
	require.Len(t, fake.pushed[0].Items, 1)
	require.Equal(t, 1, lookups, "retain the known data item without a second metadata lookup")
	require.Empty(t, client.contentReporter.reported)
	parsed, err := clip.NewClipArchiver().ExtractMetadata(restoredPath)
	require.NoError(t, err)
	require.NoError(t, clip.NewClipArchiver().CreateRemoteArchive(clipCommon.S3StorageInfo{Bucket: "images", Key: "image.clip"}, parsed, client.localArchivePath("image")))
	metadataData, err := os.ReadFile(client.localArchivePath("image"))
	require.NoError(t, err)
	lookups = 0
	client.completeV1ArchiveCache(request)
	client.contentReporter.flush()
	require.Len(t, fake.pushed, 2)
	require.Len(t, fake.pushed[1].Items, 2)
	metadataHash := fmt.Sprintf("%x", sha256.Sum256(metadataData))
	require.Contains(t, fake.pushed[1].Items, types.CacheRequiredContentItem{
		Hash: metadataHash, ExpectedHash: metadataHash, SizeBytes: int64(len(metadataData)),
		RoutingKey: "/images/image.rclip", Source: "image.rclip", ImageID: "image", Kind: types.CacheContentKindClipV1,
	})

	// A complete local archive does not ask the gateway for origin credentials.
	workerRepo := &fakeImageCredentialWorkerRepo{err: errFakeGatewayUnavailable}
	client.workerRepoClient, client.workerPoolName = workerRepo, "private"
	client.config.ImageService.RegistryStore = registry.S3ImageRegistryStore
	client.config.Worker.Pools = map[string]types.WorkerPoolConfig{"private": {Mode: types.PoolModePrivate}}
	require.NoError(t, os.WriteFile(client.clipV1ArchiveDataCachePath("image"), data, 0600))
	archive, err := client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Equal(t, client.clipV1ArchiveDataCachePath("image"), archive.path)
	require.Empty(t, workerRepo.requests)

	// A local-store worker can still receive cached S3 metadata from the gateway.
	client.config.ImageService.RegistryStore = registry.LocalImageRegistryStore
	workerRepo.err = nil
	workerRepo.resp = &pb.GetCacheOriginCredentialsResponse{Ok: true, ImageArchiveStorage: &pb.CacheWorkspaceStorageCredentials{
		BucketName: "brokered-images", AccessKey: "access", SecretKey: "secret",
	}}
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Len(t, workerRepo.requests, 1)
	options := client.lazyMountOptions(ctx, request, archive)
	require.NotNil(t, options.StorageInfo)
	require.NotNil(t, options.Credentials.S3)
	require.Equal(t, "brokered-images", options.StorageInfo.(*clipCommon.S3StorageInfo).Bucket)
	require.Equal(t, "access", options.Credentials.S3.AccessKey)

	// A full local archive needs no S3 override or gateway request.
	workerRepo.requests = nil
	require.NoError(t, os.WriteFile(client.localArchivePath("image"), data, 0600))
	archive, err = client.prepareLazyImageArchive(ctx, request)
	require.NoError(t, err)
	require.Nil(t, client.lazyMountOptions(ctx, request, archive).StorageInfo)
	require.Empty(t, workerRepo.requests)
}

func TestRestoreV1ArchiveDataCacheRemovesDirectoryTarget(t *testing.T) {
	cacheDir := t.TempDir()
	targetPath := filepath.Join(cacheDir, "image.clip")
	require.NoError(t, os.Mkdir(targetPath, 0o700))

	client := &ImageClient{
		imageCachePath: cacheDir,
		config: types.AppConfig{ImageService: types.ImageServiceConfig{
			RegistryStore: registry.S3ImageRegistryStore,
		}},
	}
	_, ok := client.restoreV1ArchiveDataCache(context.Background(), &types.ContainerRequest{ImageId: "image"}, &lazyImageArchive{})

	require.False(t, ok)
	require.NoDirExists(t, targetPath)
}

func TestRestoreV1ArchiveDataCacheDefersLargeRemoteArchive(t *testing.T) {
	client := &ImageClient{
		cacheClient:    &cache.Client{},
		imageCachePath: t.TempDir(),
		config: types.AppConfig{ImageService: types.ImageServiceConfig{
			RegistryStore: registry.S3ImageRegistryStore,
			Registries:    types.ImageRegistriesConfig{S3: types.S3ImageRegistryConfig{BucketName: "images"}},
		}},
		archiveContentMetadata: func(context.Context, string) (*cache.FSMetadata, error) {
			return &cache.FSMetadata{Hash: "archive", Size: maxSyncV1ArchiveDataRestoreBytes + 1}, nil
		},
	}

	archive := lazyImageArchive{}
	path, ok := client.restoreV1ArchiveDataCache(
		context.Background(),
		&types.ContainerRequest{ImageId: "image"},
		&archive,
	)

	require.False(t, ok)
	require.Empty(t, path)
	require.Equal(t, "images", archive.sourceRegistry.BucketName)
}

func TestGetBuildContextDoesNotFallBackToWorkspaceFuseMount(t *testing.T) {
	baseMountPath := t.TempDir()
	buildPath := t.TempDir()
	workspaceName := "workspace"
	objectID := "build-context"
	storageID := uint(1)
	bucket := "bucket"

	fuseFallbackPath := filepath.Join(baseMountPath, workspaceName, types.DefaultObjectPrefix, objectID)
	require.NoError(t, writeZipObject(fuseFallbackPath, map[string]string{
		"main.py": "print('do not read through fuse')\n",
	}))

	client := &ImageClient{
		config: types.AppConfig{
			Storage: types.StorageConfig{
				WorkspaceStorage: types.WorkspaceStorageConfig{
					BaseMountPath: baseMountPath,
				},
			},
		},
	}
	request := &types.ContainerRequest{
		Workspace: types.Workspace{
			Name: workspaceName,
			Storage: &types.WorkspaceStorage{
				Id:         &storageID,
				BucketName: &bucket,
			},
		},
		BuildOptions: types.BuildOptions{
			BuildCtxObject: &objectID,
		},
	}

	_, err := client.getBuildContext(context.Background(), buildPath, request)

	require.ErrorContains(t, err, "workspace storage credentials are required")
}

// Credentials the gateway vends arrive as username:password or as JSON, and
// JSON may carry surrounding whitespace; all of them have to end up as a
// username:password --creds value buildah can use.
func TestGetBuildahAuthArgsParsesEveryCredentialForm(t *testing.T) {
	client := &ImageClient{}
	for name, creds := range map[string]string{
		"plain":           "user:pa:ss",
		"json":            `{"USERNAME":"user","PASSWORD":"pa:ss"}`,
		"json-whitespace": "  \n" + `{"USERNAME":"user","PASSWORD":"pa:ss"}` + "\n",
	} {
		require.Equal(t, []string{"--creds", "user:pa:ss"}, client.getBuildahAuthArgs(context.Background(), "registry.example.com/img", creds), name)
	}
	require.Nil(t, client.getBuildahAuthArgs(context.Background(), "registry.example.com/img", ""))
	require.Nil(t, client.getBuildahAuthArgs(context.Background(), "registry.example.com/img", `{"AWS_ACCESS_KEY_ID":"a","AWS_SECRET_ACCESS_KEY":"b"}`))
}

// RUN steps get a writable working directory holding the build context, and
// what they write there does not reach the shared extracted context.
func TestWritableBuildContextIsPrivateToTheBuild(t *testing.T) {
	ctxDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(ctxDir, "pkg"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(ctxDir, "pkg", "main.py"), []byte("print('hi')\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(ctxDir, "setup.py"), []byte("setup()\n"), 0o755))

	client := &ImageClient{imageCachePath: t.TempDir()}
	request := &types.ContainerRequest{ContainerId: "build-1", ImageId: "img-1"}
	// A context extracted into the build's own directory is already private
	// and is handed back as is.
	buildPath := t.TempDir()
	privateCtx := filepath.Join(buildPath, "build-ctx")
	require.NoError(t, os.MkdirAll(privateCtx, 0o755))
	dir, cleanup, err := client.writableBuildContext(context.Background(), request, buildPath, privateCtx)
	require.NoError(t, err)
	require.Equal(t, privateCtx, dir)
	cleanup()

	dir, cleanup, err = client.writableBuildContext(context.Background(), request, buildPath, ctxDir)
	require.NoError(t, err)
	require.NotEqual(t, ctxDir, dir)

	body, err := os.ReadFile(filepath.Join(dir, "pkg", "main.py"))
	require.NoError(t, err)
	require.Equal(t, "print('hi')\n", string(body))
	info, err := os.Stat(filepath.Join(dir, "setup.py"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())

	require.NoError(t, os.WriteFile(filepath.Join(dir, "pkg", "generated.py"), []byte("x"), 0o644))
	require.NoError(t, os.Remove(filepath.Join(dir, "setup.py")))
	_, err = os.Stat(filepath.Join(ctxDir, "pkg", "generated.py"))
	require.True(t, os.IsNotExist(err), "a build step's writes must not land in the shared context")
	_, err = os.Stat(filepath.Join(ctxDir, "setup.py"))
	require.NoError(t, err, "a build step's removals must not land in the shared context")

	cleanup()
	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err), "the build's copy is removed with the build")
	entries, err := os.ReadDir(filepath.Join(client.imageCachePath, "spool"))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNewBuildahCommandUsesCancelableProcessGroup(t *testing.T) {
	cmd := newBuildahCommand(
		context.Background(),
		[]string{"--version"},
		[]string{"TMPDIR=/tmp"},
		io.Discard,
		io.Discard,
	)

	require.Equal(t, []string{"buildah", "--version"}, cmd.Args)
	require.Equal(t, []string{"TMPDIR=/tmp"}, cmd.Env)
	require.NotNil(t, cmd.Cancel)
	require.NotNil(t, cmd.SysProcAttr)
	require.True(t, cmd.SysProcAttr.Setpgid)
	require.Equal(t, imageCommandCancelGracePeriod, cmd.WaitDelay)
}

func TestTerminateImageProcessGroupKillsDescendants(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process-group signal semantics are validated on Linux workers")
	}

	cmd := exec.Command("sh", "-c", "trap '' TERM; sleep 30 & wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())

	require.NoError(t, terminateImageProcessGroup(cmd.Process.Pid))

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process group did not exit after termination")
	}
}

func TestVerifyImageSourceFailsOnlyWhenRegistryRefusesRemoteLayers(t *testing.T) {
	status, requests := http.StatusOK, []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.WriteHeader(status)
	}))
	defer server.Close()

	cachePath := t.TempDir()
	layer := "sha256:" + strings.Repeat("1", 64)
	c := &ImageClient{}
	options := clip.MountOptions{CachePath: cachePath, Metadata: &clipCommon.ClipArchiveMetadata{StorageInfo: &clipCommon.OCIStorageInfo{
		RegistryURL: server.Listener.Addr().String(),
		Repository:  "org/app",
		// Indexed from a local copy whose converted manifest the registry never had.
		Reference:               "sha256:" + strings.Repeat("2", 64),
		Layers:                  []string{layer},
		DecompressedHashByLayer: map[string]string{layer: "hash-1"},
	}}}

	for _, status = range []int{http.StatusOK, http.StatusInternalServerError} {
		require.NoError(t, c.verifyImageSource(context.Background(), options), status)
	}
	require.Contains(t, requests, "HEAD /v2/org/app/blobs/"+layer)
	for _, status = range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		require.ErrorContains(t, c.verifyImageSource(context.Background(), options), "image source "+server.Listener.Addr().String()+"/org/app@"+layer, status)
	}

	// A layer already in the layer cache needs no registry at all.
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "hash-1"), nil, 0o644))
	requests = nil
	require.NoError(t, c.verifyImageSource(context.Background(), options))
	require.Empty(t, requests)

	// A peer's complete layer also needs no origin credentials or HEAD request.
	require.NoError(t, os.Remove(filepath.Join(cachePath, "hash-1")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cacheServer, _ := newCheckpointCacheForTest(t, ctx)
	hash, _, err := cacheServer.StoreReader(ctx, strings.NewReader("cached layer"), "")
	require.NoError(t, err)
	peer := newTestPeerClient(t, ctx, cacheServer.Host())
	c.cacheClient = peer
	info, _ := ociStorageInfo(options.Metadata)
	info.DecompressedHashByLayer[layer] = hash
	require.NoError(t, c.verifyImageSource(ctx, options))
	require.Empty(t, requests)
	bridge := newImageContentCache(peer, "image", "layer", nil)
	cached, err := bridge.ContentExists(hash, struct{ RoutingKey string }{})
	require.NoError(t, err)
	require.True(t, cached)
	cached, err = bridge.ContentExists(strings.Repeat("0", 64), struct{ RoutingKey string }{})
	require.NoError(t, err)
	require.False(t, cached)
}

func newTestPeerClient(t *testing.T, ctx context.Context, host *cache.Host) *cache.Client {
	t.Helper()
	hostSnapshot := *host
	client, err := cache.NewClientWithHostDirectory(ctx, testCacheManagerConfig(t.TempDir()).Cache, nil,
		testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
			host := hostSnapshot
			return []*cache.Host{&host}, nil
		}), "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })
	require.NoError(t, client.WaitForHosts(3*time.Second))
	return client
}

func TestHasOCILayersRejectsDockerFormatImages(t *testing.T) {
	image := func(mediaTypes ...string) common.ImageMetadata {
		var meta common.ImageMetadata
		for _, mediaType := range mediaTypes {
			meta.LayersData = append(meta.LayersData, struct {
				MIMEType    string `json:"MIMEType"`
				Digest      string `json:"Digest"`
				Size        int    `json:"Size"`
				Annotations any    `json:"Annotations"`
			}{MIMEType: mediaType})
		}
		return meta
	}

	require.True(t, hasOCILayers(image("application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.v1.tar")))
	require.False(t, hasOCILayers(image("application/vnd.docker.image.rootfs.diff.tar.gzip")))
	require.False(t, hasOCILayers(image("application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.docker.image.rootfs.diff.tar.gzip")))
	require.False(t, hasOCILayers(image()))
}

func TestRemoteLayersSkipsLayersHeldLocally(t *testing.T) {
	info := &clipCommon.OCIStorageInfo{
		Layers: []string{"sha256:a", "sha256:b", "sha256:c"},
		DecompressedHashByLayer: map[string]string{
			"sha256:a": "hash-a",
			"sha256:b": "hash-b",
		},
	}
	cachePath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "hash-a"), nil, 0o644))

	// b is not on disk; c has no decompressed hash so it can never be local.
	require.Equal(t, []string{"sha256:b", "sha256:c"}, (&ImageClient{}).remoteLayers(info, cachePath))
}
