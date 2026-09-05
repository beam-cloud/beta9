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
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
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

func TestLocalImageArchiveReadyPreservesInProgressPlaceholder(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "image.clip")
	require.NoError(t, os.WriteFile(archivePath, nil, 0o600))

	client := &ImageClient{}
	require.False(t, client.localImageArchiveReady(archivePath, "image"))
	require.FileExists(t, archivePath)
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
	_, ok := client.restoreV1ArchiveDataCache(context.Background(), &types.ContainerRequest{ImageId: "image"}, nil)

	require.False(t, ok)
	require.NoDirExists(t, targetPath)
}

func TestRestoreV1ArchiveDataCacheDefersLargeRemoteArchive(t *testing.T) {
	client := &ImageClient{
		cacheClient:    &cache.Client{},
		imageCachePath: t.TempDir(),
		config: types.AppConfig{ImageService: types.ImageServiceConfig{
			RegistryStore: registry.S3ImageRegistryStore,
		}},
		archiveContentMetadata: func(context.Context, string) (*cache.FSMetadata, error) {
			return &cache.FSMetadata{Hash: "archive", Size: maxSyncV1ArchiveDataRestoreBytes + 1}, nil
		},
	}

	path, ok := client.restoreV1ArchiveDataCache(
		context.Background(),
		&types.ContainerRequest{ImageId: "image"},
		&types.S3ImageRegistryConfig{BucketName: "images"},
	)

	require.False(t, ok)
	require.Empty(t, path)
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

func TestLayersToPrepareSkipsLocallyCompleteLayers(t *testing.T) {
	info := &clipCommon.OCIStorageInfo{
		Layers: []string{"sha256:a", "sha256:b", "sha256:c"},
		DecompressedHashByLayer: map[string]string{
			"sha256:a": "hash-a",
			"sha256:b": "hash-b",
		},
	}
	local := map[string]bool{"hash-a": true}

	remaining := layersToPrepare(info, func(hash string) bool { return local[hash] })

	// b is not local; c has no decompressed hash so it can never be local.
	require.Equal(t, []string{"sha256:b", "sha256:c"}, remaining)
}
