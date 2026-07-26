package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/clip/pkg/clip"
	clipStorage "github.com/beam-cloud/clip/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

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

func TestWaitForImageMountWaitsForMountInfo(t *testing.T) {
	var mounted atomic.Bool
	go func() {
		time.Sleep(10 * time.Millisecond)
		mounted.Store(true)
	}()

	err := waitForImageMount(
		context.Background(),
		"/images/test",
		make(chan error),
		func(string) bool { return mounted.Load() },
		func(string) error { return nil },
		100*time.Millisecond,
	)

	require.NoError(t, err)
}

func TestWaitForImageMountReturnsServerError(t *testing.T) {
	serverErrors := make(chan error, 1)
	serverErrors <- errors.New("fuse failed")

	err := waitForImageMount(
		context.Background(),
		"/images/test",
		serverErrors,
		func(string) bool { return false },
		func(string) error { return nil },
		100*time.Millisecond,
	)

	require.ErrorContains(t, err, "fuse failed")
}

func TestWaitForImageMountTimesOut(t *testing.T) {
	err := waitForImageMount(
		context.Background(),
		"/images/test",
		make(chan error),
		func(string) bool { return false },
		func(string) error { return nil },
		15*time.Millisecond,
	)

	require.ErrorContains(t, err, "was not ready")
}

func TestWatchImageMountRemovesStoppedServer(t *testing.T) {
	client := &ImageClient{mountedFuseServers: common.NewSafeMap[*fuse.Server]()}
	serverErrors := make(chan error)
	client.mountedFuseServers.Set("image", nil)
	close(serverErrors)

	client.watchImageMount("image", nil, serverErrors)

	require.False(t, client.mountedImageReady("image"))
}

func TestDirectSandboxArchivePullOnlyUsesRemoteMetadataArchive(t *testing.T) {
	request := &types.ContainerRequest{
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
	}
	client := &ImageClient{registry: &reg.ImageRegistry{ImageFileExtension: reg.RemoteImageFileExtension}}
	require.True(t, client.directSandboxArchivePull(request))

	client.registry.ImageFileExtension = reg.LocalImageFileExtension
	require.False(t, client.directSandboxArchivePull(request))

	request.Stub.Type = types.StubType(types.StubTypeFunction)
	client.registry.ImageFileExtension = reg.RemoteImageFileExtension
	require.False(t, client.directSandboxArchivePull(request))
}

func TestPullLazyMountedImageReportsCachedContentForNewStub(t *testing.T) {
	v2Info, ok := ociStorageInfo(testClipV2Metadata())
	require.True(t, ok)
	tests := []struct {
		name   string
		kind   types.CacheContentKind
		report requiredContentReport
	}{
		{
			name: "v2",
			kind: types.CacheContentKindClipV2,
			report: requiredContentReport{
				kind:  types.CacheContentKindClipV2,
				items: ociRequiredContentItems("warm-image", v2Info),
			},
		},
		{
			name: "v1",
			kind: types.CacheContentKindClipV1,
			report: requiredContentReport{
				kind: types.CacheContentKindClipV1,
				items: []types.CacheRequiredContentItem{{
					Hash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					RoutingKey: "/images/warm-image.clip",
					ImageID:    "warm-image",
					Kind:       types.CacheContentKindClipV1,
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := &fakeEventRepo{}
			reporter := newTestReporter(events)
			client := &ImageClient{
				contentReporter:    reporter,
				mountedFuseServers: common.NewSafeMap[*fuse.Server](),
				requiredContent:    common.NewSafeMap[requiredContentReport](),
			}
			client.mountedFuseServers.Set("warm-image", nil)
			client.requiredContent.Set("warm-image", test.report)

			_, err := client.PullLazy(context.Background(), &types.ContainerRequest{
				ImageId:     "warm-image",
				WorkspaceId: "workspace",
				StubId:      "new-stub",
			}, nil)
			require.NoError(t, err)

			reporter.flush()
			require.Len(t, events.pushed, 1)
			require.Equal(t, test.kind, events.pushed[0].Kind)
			require.Len(t, events.pushed[0].Items, len(test.report.items))
		})
	}
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
