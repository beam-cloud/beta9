package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	clipStorage "github.com/beam-cloud/clip/pkg/storage"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestPrepareLazyArchiveContentSkipsSandboxOCIArchive(t *testing.T) {
	mountOptions := clip.MountOptions{}
	err := prepareLazyArchiveContent(
		context.Background(),
		&types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}},
		lazyImageArchive{storageMode: string(clipCommon.StorageModeOCI)},
		&mountOptions,
		nil,
	)

	require.NoError(t, err)
	require.Nil(t, mountOptions.Context)
	require.Zero(t, mountOptions.PrepareConcurrency)
	require.Nil(t, mountOptions.PrepareProgress)
}

func TestPrepareLazyArchiveContentStillEagerlyPreparesNonSandboxOCIArchive(t *testing.T) {
	mountOptions := clip.MountOptions{}
	err := prepareLazyArchiveContent(
		context.Background(),
		&types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeEndpoint)}}},
		lazyImageArchive{storageMode: string(clipCommon.StorageModeOCI)},
		&mountOptions,
		nil,
	)

	require.Error(t, err, "invalid mount options must reach clip.PrepareArchiveContent")
	require.NotNil(t, mountOptions.Context)
	require.Equal(t, imageLayerPrepareConcurrency, mountOptions.PrepareConcurrency)
}

func TestSandboxEmbeddedImageArchiveCacheHitStillRestoresArchive(t *testing.T) {
	cacheClient := newImageArchiveCacheTestClient(t)
	imageID := "sandbox-cache-hit"
	cachePath := fmt.Sprintf("%s/%s.%s", types.AgentImagesPath, imageID, registry.LocalImageFileExtension)
	sourcePath := createImageArchiveForCacheTest(t)

	_, err := cacheClient.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, cache.StoreContentOptions{RoutingKey: cachePath, Lock: true})
	require.NoError(t, err)

	client := &ImageClient{
		cacheClient: cacheClient,
		registry:    &registry.ImageRegistry{ImageFileExtension: registry.LocalImageFileExtension},
	}
	destinationPath := filepath.Join(t.TempDir(), imageID+"."+registry.LocalImageFileExtension)
	hit, _, err := client.pullImageArchiveFromEmbeddedCache(context.Background(), destinationPath, sandboxImageRequest(imageID))

	require.NoError(t, err)
	require.True(t, hit)
	require.FileExists(t, destinationPath)
	source, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	restored, err := os.ReadFile(destinationPath)
	require.NoError(t, err)
	require.Equal(t, source, restored)
}

func TestSandboxEmbeddedImageArchiveCacheMissFallsThroughWithoutSynchronousFill(t *testing.T) {
	cacheClient := newImageArchiveCacheTestClient(t)
	imageID := "sandbox-cache-miss"
	client := &ImageClient{
		cacheClient: cacheClient,
		registry:    &registry.ImageRegistry{ImageFileExtension: registry.LocalImageFileExtension},
		config: types.AppConfig{
			ImageService: types.ImageServiceConfig{
				RegistryStore: registry.S3ImageRegistryStore,
				Registries: types.ImageRegistriesConfig{
					S3: types.S3ImageRegistryConfig{
						BucketName:     "sandbox-origin",
						Region:         "us-east-1",
						Endpoint:       "http://127.0.0.1:1",
						AccessKey:      "access",
						SecretKey:      "secret",
						ForcePathStyle: true,
					},
				},
			},
		},
	}
	destinationPath := filepath.Join(t.TempDir(), imageID+"."+registry.LocalImageFileExtension)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hit, sourceRegistry, err := client.pullImageArchiveFromEmbeddedCache(
		ctx,
		destinationPath,
		sandboxImageRequest(imageID),
	)

	require.NoError(t, err)
	require.False(t, hit)
	require.Nil(t, sourceRegistry)
	require.NoFileExists(t, destinationPath)
}

func sandboxImageRequest(imageID string) *types.ContainerRequest {
	return &types.ContainerRequest{
		ImageId: imageID,
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)},
		},
	}
}

func createImageArchiveForCacheTest(t *testing.T) string {
	t.Helper()

	sourceDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "hello.txt"), []byte("hello from cache"), 0644))

	archivePath := filepath.Join(t.TempDir(), "image.clip")
	require.NoError(t, clip.NewClipArchiver().Create(clip.ClipArchiverOptions{
		SourcePath:  sourceDir,
		OutputFile:  archivePath,
		ArchivePath: archivePath,
	}))
	return archivePath
}

func newImageArchiveCacheTestClient(t *testing.T) *cache.Client {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := testCacheManagerConfig(t.TempDir()).Cache
	metadataStore := cache.NewMockCacheMetadataStore()
	server, err := cache.NewServerWithOptions(
		ctx,
		cfg,
		"image-test",
		cache.WithServerMetadataStore(metadataStore),
		cache.WithServerHostID("image-test-host"),
	)
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)

	host := server.Host()
	require.NotNil(t, host)
	host.Addr = addr
	host.PrivateAddr = addr

	client, err := cache.NewClientWithHostDirectory(
		ctx,
		cfg,
		metadataStore,
		testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
			return []*cache.Host{host}, nil
		}),
		"image-test",
	)
	require.NoError(t, err)
	client.AttachLocalServer(server)
	require.Eventually(t, func() bool {
		primary, err := client.PrimaryReadHost("image-test-probe")
		return err == nil && primary != nil && primary.HostId == host.HostId
	}, 3*time.Second, 20*time.Millisecond)

	t.Cleanup(func() {
		require.NoError(t, client.Cleanup())
		require.NoError(t, server.Close())
		cancel()
	})
	return client
}

func TestImageLayerPrepareProgressLoggerEmitsAggregateUpdates(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	report := imageLayerPrepareProgressLogger(logger)
	require.NotNil(t, report)

	report(clipStorage.PrepareProgress{Total: 4})
	report(clipStorage.PrepareProgress{Completed: 1, Total: 4, Bytes: 1024})
	report(clipStorage.PrepareProgress{Completed: 4, Total: 4, Bytes: 4 * 1024 * 1024})

	logs := output.String()
	require.Contains(t, logs, "Preparing 4 image layers (8 concurrent)")
	require.Contains(t, logs, "Prepared 4 image layers (4.0 MiB)")
	require.NotContains(t, logs, "1/4 ready", "rapid per-layer updates should be coalesced")
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

func TestOCILayoutPushArgsUseEightWayDigestPreservingCopy(t *testing.T) {
	args := ociLayoutPushArgs("/tmp/layout", "registry.example.com/beam/image:test", "user:token", true)

	require.Equal(t, []string{
		"copy",
		"--image-parallel-copies", "8",
		"--preserve-digests",
		"--retry-times", "5",
		"--retry-delay", "1s",
		"--dest-tls-verify=false",
		"--dest-creds", "user:token",
		"oci:/tmp/layout:latest",
		"docker://registry.example.com/beam/image:test",
	}, args)
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
