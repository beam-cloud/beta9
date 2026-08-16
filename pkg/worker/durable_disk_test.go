package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestPublicDiskSnapshotStoreDownloadsPresignedObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("chunk"))
	}))
	defer server.Close()

	store := &durableDiskSnapshotURLStore{
		workspaceID: "workspace-1",
		snapshotID:  "snapshot-1",
		resolveURL: func(_ context.Context, req *pb.GetDiskSnapshotDownloadURLRequest) (*pb.GetDiskSnapshotDownloadURLResponse, error) {
			require.Equal(t, "chunk-key", req.ObjectKey)
			return &pb.GetDiskSnapshotDownloadURLResponse{Ok: true, Url: server.URL}, nil
		},
	}
	reader, err := store.DownloadWithReader(context.Background(), "chunk-key")
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "chunk", string(data))
}

func TestDurableDiskSnapshotProtoKeepsPublic(t *testing.T) {
	snapshot := durableDiskSnapshotFromProto(durableDiskSnapshotToProto(&types.DiskSnapshot{Public: true}))
	require.True(t, snapshot.Public)
}

func TestDurableDiskCleansStalePostgresPid(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "pg-data")
	mount := durableDiskTestMount(primary)
	mount.MountPath = types.PostgresDataMountPath

	incomplete := filepath.Join(primary, "pgdata")
	require.NoError(t, os.MkdirAll(incomplete, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "PG_VERSION"), []byte("16"), 0o600))
	require.False(t, durableDiskHasRestorablePayload(mount, primary))
	require.ErrorContains(t, prepareSnapshotDurableDiskMount(mount), "active or incomplete local payload")

	require.NoError(t, os.MkdirAll(filepath.Join(incomplete, "global"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "global", "pg_control"), []byte("control"), 0o600))
	require.True(t, durableDiskHasRestorablePayload(mount, primary))

	pidFile := filepath.Join(incomplete, "postmaster.pid")
	require.NoError(t, os.WriteFile(pidFile, []byte("123"), 0o600))
	require.NoError(t, prepareSnapshotDurableDiskMount(mount))
	require.NoFileExists(t, pidFile)
}

func TestSeedDurableDiskSnapshotResolvesAnotherDisksSnapshot(t *testing.T) {
	backendRepo := &fakeBackendRepoClient{
		sourceSnapshot: &pb.DiskSnapshot{
			ExternalId:  "snapshot-source",
			DiskName:    "the-source-disk",
			ManifestKey: "durable-disks/the-source-disk/snapshots/1/manifest.json",
		},
	}
	worker := &Worker{ctx: context.Background(), backendRepoClient: backendRepo}
	request := &types.ContainerRequest{ContainerId: "container-1"}
	mount := &types.Mount{
		DurableDisk: &types.DurableDiskMountConfig{Name: "the-fork-disk", SourceSnapshotId: "snapshot-source"},
	}

	seed, err := worker.seedDurableDiskSnapshot(context.Background(), request, mount)

	require.NoError(t, err)
	require.Equal(t, "snapshot-source", backendRepo.requestedSnapshotId)
	require.Equal(t, "the-source-disk", seed.DiskName, "a fork restores the disk it came from, not its own")
}

func TestDurableDiskSeedFallsBackToTheStubConfig(t *testing.T) {
	config, err := json.Marshal(types.StubConfigV1{Disks: []*pb.DurableDisk{{
		Name:             "fork-disk",
		SourceSnapshotId: "snapshot-source",
	}}})
	require.NoError(t, err)
	request := &types.ContainerRequest{Stub: types.StubWithRelated{
		Stub: types.Stub{Config: string(config)},
	}}
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "fork-disk"}}

	require.Equal(t, "snapshot-source", durableDiskSourceSnapshotFromStub(request, mount.DurableDisk.Name))
}

func TestSeedDurableDiskSnapshotRefusesAnEmptyDiskWhenTheSourceIsGone(t *testing.T) {
	worker := &Worker{ctx: context.Background(), backendRepoClient: &fakeBackendRepoClient{}}
	mount := &types.Mount{
		DurableDisk: &types.DurableDiskMountConfig{Name: "the-fork-disk", SourceSnapshotId: "snapshot-vanished"},
	}

	seed, err := worker.seedDurableDiskSnapshot(context.Background(), &types.ContainerRequest{}, mount)

	require.ErrorContains(t, err, "source snapshot")
	require.Nil(t, seed)
}

func TestSeedDurableDiskSnapshotFailsOpenWhenLookupIsUnavailable(t *testing.T) {
	backendRepo := &fakeBackendRepoClient{getDiskSnapshotErr: fmt.Errorf("backend unavailable")}
	worker := &Worker{ctx: context.Background(), backendRepoClient: backendRepo}
	localPath := filepath.Join(t.TempDir(), "fork")
	require.NoError(t, os.MkdirAll(localPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(localPath, "already-restored"), []byte("payload"), 0o600))
	mount := &types.Mount{
		LocalPath: localPath,
		DurableDisk: &types.DurableDiskMountConfig{
			Name:             "the-fork-disk",
			SourceSnapshotId: "snapshot-temporarily-unavailable",
		},
	}

	seed, err := worker.seedDurableDiskSnapshot(context.Background(), &types.ContainerRequest{}, mount)

	require.NoError(t, err)
	require.Nil(t, seed)
	require.NoError(t, worker.restoreDurableDiskSnapshot(&types.ContainerRequest{}, mount))
	require.FileExists(t, filepath.Join(localPath, "already-restored"))
}

func TestSeedDurableDiskSnapshotStillHonorsCancellation(t *testing.T) {
	backendRepo := &fakeBackendRepoClient{getDiskSnapshotErr: fmt.Errorf("backend unavailable")}
	worker := &Worker{backendRepoClient: backendRepo}
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{
		Name:             "fork",
		SourceSnapshotId: "source",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	seed, err := worker.seedDurableDiskSnapshot(ctx, &types.ContainerRequest{}, mount)

	require.Nil(t, seed)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLatestDurableDiskSnapshotManifestPreservesEmptyTree(t *testing.T) {
	manifest := &types.DiskSnapshotManifest{Version: 1, Format: types.DiskSnapshotFormatDirV1}
	manifestData, err := json.Marshal(manifest)
	require.NoError(t, err)
	store := &fakeDurableDiskSnapshotStore{objects: map[string][]byte{"empty/manifest.json": manifestData}}
	backendRepo := &fakeBackendRepoClient{latestSnapshot: &pb.DiskSnapshot{
		ExternalId:  "empty-snapshot",
		DiskName:    "empty-disk",
		Format:      types.DiskSnapshotFormatDirV1,
		ManifestKey: "empty/manifest.json",
	}}
	worker := &Worker{backendRepoClient: backendRepo}
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "empty-disk"}}

	parent, previous, err := worker.latestDurableDiskSnapshotManifest(context.Background(), &types.ContainerRequest{}, mount, store)

	require.NoError(t, err)
	require.Equal(t, "empty-snapshot", parent.ExternalId)
	require.NotNil(t, previous)
	require.Empty(t, previous.Files)

	emptySource := t.TempDir()
	snapshot, _, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, emptySource, "empty/snapshots/2",
		types.DiskSnapshot{DiskName: "empty-disk", Format: types.DiskSnapshotFormatDirV1},
		4, previous, true,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot)
}

func TestNextDurableDiskSnapshotGenerationIsMonotonicAcrossWorkerClocks(t *testing.T) {
	generation, err := nextDurableDiskSnapshotGeneration(200, &types.DiskSnapshot{Generation: 100})
	require.NoError(t, err)
	require.Equal(t, int64(200), generation)

	generation, err = nextDurableDiskSnapshotGeneration(100, &types.DiskSnapshot{Generation: 200})
	require.NoError(t, err)
	require.Equal(t, int64(201), generation)

	generation, err = nextDurableDiskSnapshotGeneration(math.MaxInt64-1, &types.DiskSnapshot{Generation: math.MaxInt64})
	require.ErrorContains(t, err, "generation is exhausted")
	require.Zero(t, generation)
}

func TestConcurrentDurableDiskGenerationAttemptsKeepIndependentManifests(t *testing.T) {
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "snapshots"}}
	store := &fakeDurableDiskSnapshotStore{}
	generation := int64(42)

	snapshot := func(payload string) *types.DiskSnapshot {
		source := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(source, "weights"), []byte(payload), 0o600))
		created, _, err := createDurableDiskDirectorySnapshot(
			context.Background(), store, source, durableDiskSnapshotObjectPrefix(mount, generation),
			types.DiskSnapshot{DiskName: "snapshots", Generation: generation}, 4, nil, false,
		)
		require.NoError(t, err)
		return created
	}

	first := snapshot("first model")
	second := snapshot("second model")
	require.Equal(t, generation, first.Generation)
	require.Equal(t, generation, second.Generation)
	require.NotEqual(t, first.ObjectPrefix, second.ObjectPrefix)
	require.NotEqual(t, first.ManifestKey, second.ManifestKey)
	require.Equal(t, "durable-disks/snapshots/chunks", durableDiskSnapshotChunkPrefix(first.ObjectPrefix))

	for _, test := range []struct {
		name     string
		snapshot *types.DiskSnapshot
		want     string
	}{
		{name: "first", snapshot: first, want: "first model"},
		{name: "second", snapshot: second, want: "second model"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "restored")
			_, err := restoreDurableDiskDirectorySnapshotWithCache(
				context.Background(), store, nil, test.snapshot.ManifestKey,
				test.snapshot.ManifestDigest, test.snapshot.ManifestSizeBytes, target,
			)
			require.NoError(t, err)
			data, err := os.ReadFile(filepath.Join(target, "weights"))
			require.NoError(t, err)
			require.Equal(t, test.want, string(data))
		})
	}
}

func TestLatestDurableDiskSnapshotManifestDistinguishesMissingHistoryFromFailure(t *testing.T) {
	mount := &types.Mount{DurableDisk: &types.DurableDiskMountConfig{Name: "model"}}
	request := &types.ContainerRequest{}

	t.Run("first snapshot", func(t *testing.T) {
		backend := &latestDiskSnapshotBackend{
			fakeBackendRepoClient: &fakeBackendRepoClient{},
			response: &pb.GetLatestDiskSnapshotResponse{
				Ok:       false,
				ErrorMsg: (&types.ErrDiskSnapshotNotFound{SnapshotId: "latest:model"}).Error(),
			},
		}
		parent, manifest, err := (&Worker{backendRepoClient: backend}).latestDurableDiskSnapshotManifest(
			context.Background(), request, mount, &fakeDurableDiskSnapshotStore{},
		)
		require.NoError(t, err)
		require.Nil(t, parent)
		require.Nil(t, manifest)
	})

	t.Run("repository unavailable", func(t *testing.T) {
		backend := &latestDiskSnapshotBackend{
			fakeBackendRepoClient: &fakeBackendRepoClient{},
			err:                   errors.New("repository unavailable"),
		}
		parent, manifest, err := (&Worker{backendRepoClient: backend}).latestDurableDiskSnapshotManifest(
			context.Background(), request, mount, &fakeDurableDiskSnapshotStore{},
		)
		require.ErrorContains(t, err, "repository unavailable")
		require.Nil(t, parent)
		require.Nil(t, manifest)
	})

	t.Run("manifest unavailable", func(t *testing.T) {
		backend := &latestDiskSnapshotBackend{
			fakeBackendRepoClient: &fakeBackendRepoClient{},
			response: &pb.GetLatestDiskSnapshotResponse{Ok: true, Snapshot: &pb.DiskSnapshot{
				ExternalId:  "snapshot-1",
				DiskName:    "model",
				ManifestKey: "missing/manifest.json",
			}},
		}
		parent, manifest, err := (&Worker{backendRepoClient: backend}).latestDurableDiskSnapshotManifest(
			context.Background(), request, mount, &fakeDurableDiskSnapshotStore{},
		)
		require.ErrorContains(t, err, "load latest durable disk snapshot manifest")
		require.Nil(t, parent)
		require.Nil(t, manifest)
	})

	t.Run("manifest metadata missing", func(t *testing.T) {
		backend := &latestDiskSnapshotBackend{
			fakeBackendRepoClient: &fakeBackendRepoClient{},
			response: &pb.GetLatestDiskSnapshotResponse{Ok: true, Snapshot: &pb.DiskSnapshot{
				ExternalId: "snapshot-without-manifest",
				DiskName:   "model",
			}},
		}
		parent, manifest, err := (&Worker{backendRepoClient: backend}).latestDurableDiskSnapshotManifest(
			context.Background(), request, mount, &fakeDurableDiskSnapshotStore{},
		)
		require.ErrorContains(t, err, "has no manifest")
		require.Nil(t, parent)
		require.Nil(t, manifest)
	})
}

func TestSyncDurableDiskMountsHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := &types.ContainerRequest{Mounts: []types.Mount{{
		DurableDisk: &types.DurableDiskMountConfig{Name: "disk"},
	}}}

	snapshots, err := (&Worker{}).syncDurableDiskMounts(ctx, request, durableDiskSyncExplicit)

	require.Empty(t, snapshots)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDurableDiskInactivityWatchdogResetsOnlyOnProgress(t *testing.T) {
	ctx, stop := withDurableDiskInactivityWatchdog(context.Background(), 80*time.Millisecond)
	defer stop()

	for range 3 {
		time.Sleep(40 * time.Millisecond)
		reportDurableDiskProgress(ctx, durableDiskProgressEvent{logicalBytes: 1})
		select {
		case <-ctx.Done():
			t.Fatal("watchdog expired while snapshot progress continued")
		default:
		}
	}

	select {
	case <-ctx.Done():
		require.ErrorIs(t, context.Cause(ctx), errDurableDiskSnapshotInactive)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("watchdog did not cancel an inactive snapshot")
	}
}

func TestDurableDiskLockContentionKeepsInactivityWatchdogAlive(t *testing.T) {
	diskPath := filepath.Join(t.TempDir(), "disk")
	mount := &types.Mount{
		LocalPath: diskPath,
		DurableDisk: &types.DurableDiskMountConfig{
			Name: "disk",
		},
	}
	lockDir := filepath.Join(filepath.Dir(diskPath), durableDiskLockDir)
	require.NoError(t, os.MkdirAll(lockDir, 0o755))
	lock := NewFileLock(filepath.Join(lockDir, filepath.Base(diskPath)+".lock"))
	require.NoError(t, lock.Acquire())

	ctx, stop := withDurableDiskInactivityWatchdog(context.Background(), 650*time.Millisecond)
	defer stop()
	releaseErr := make(chan error, 1)
	go func() {
		time.Sleep(900 * time.Millisecond)
		releaseErr <- lock.Release()
	}()
	started := time.Now()
	acquired := false
	err := withDurableDiskLock(ctx, mount, func() error {
		acquired = true
		return nil
	})

	require.NoError(t, <-releaseErr)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Greater(t, time.Since(started), 650*time.Millisecond)
	require.NoError(t, ctx.Err())
}

func TestAppendOnlyPrefixVerificationReportsProgressAndHonorsCancellation(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "appendonly.aof")
	require.NoError(t, os.WriteFile(filename, make([]byte, 4<<20), 0o600))

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logicalBytes, uploadChunks int64
	baseCtx = withDurableDiskProgressReporter(baseCtx, func(progress durableDiskProgressEvent) {
		logicalBytes += progress.logicalBytes
		uploadChunks += progress.chunks
		cancel()
	})
	ctx, stopWatchdog := withDurableDiskInactivityWatchdog(baseCtx, time.Hour)
	defer stopWatchdog()

	reusable, err := durableDiskSnapshotFileChunksReusable(ctx, filename, types.DiskSnapshotFile{
		Type:      "file",
		SizeBytes: 4 << 20,
		Chunks: []types.DiskSnapshotChunk{{
			OffsetBytes: 0,
			SizeBytes:   4 << 20,
			Digest:      "sha256:not-reached",
		}},
	})

	require.False(t, reusable)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(durableDiskSnapshotReadBufferSize), logicalBytes)
	require.Zero(t, uploadChunks, "rehashing an existing prefix is read progress, not a completed upload")
}

func TestChunkSpoolingAndUploadExposeStreamingProgress(t *testing.T) {
	t.Run("source read", func(t *testing.T) {
		baseCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var logicalBytes int64
		ctx := withDurableDiskProgressReporter(baseCtx, func(progress durableDiskProgressEvent) {
			logicalBytes += progress.logicalBytes
			if logicalBytes > 0 {
				cancel()
			}
		})
		_, _, _, err := spoolDurableDiskSnapshotChunk(
			ctx,
			bytes.NewReader(make([]byte, 4<<20)),
			"model.safetensors",
			0,
			4<<20,
			make([]byte, durableDiskSnapshotReadBufferSize),
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int64(durableDiskSnapshotReadBufferSize), logicalBytes)
	})

	t.Run("upload read", func(t *testing.T) {
		chunk := &durableDiskSnapshotChunkBody{}
		defer chunk.release()
		_, err := chunk.Write(make([]byte, 4<<20))
		require.NoError(t, err)

		baseCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		progressEvents := 0
		ctx := withDurableDiskProgressReporter(baseCtx, func(progress durableDiskProgressEvent) {
			progressEvents++
			require.Zero(t, progress.logicalBytes)
			require.Zero(t, progress.chunks)
			cancel()
		})
		err = uploadDurableDiskSnapshotChunk(ctx, &fakeDurableDiskSnapshotStore{}, "chunk", chunk, 4<<20)
		require.ErrorIs(t, err, context.Canceled)
		require.Positive(t, progressEvents)
	})
}

func TestDurabilityReportingNeverBlocksCheckpointOrDiskPublication(t *testing.T) {
	events := &blockingRequiredContentEventRepo{
		fakeEventRepo: &fakeEventRepo{},
		started:       make(chan struct{}),
	}
	reporter := newTestReporter(events)
	worker := &Worker{cacheManager: &WorkerCacheManager{reporter: reporter}}
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub"}

	done := make(chan struct{})
	go func() {
		worker.reportCheckpointRequiredContent(request, "checkpoint", &checkpointCacheMetadata{
			hash: "checkpoint-hash", sizeBytes: 42, originKey: "checkpoints/checkpoint.tar",
		})
		worker.reportDurableDiskSnapshotContent(request, &types.DiskSnapshot{
			BucketName: "bucket", ManifestKey: "disk/manifest.json", ManifestDigest: "sha256:manifest", ManifestSizeBytes: 10,
		}, &types.DiskSnapshotManifest{Files: []types.DiskSnapshotFile{{
			Type: "file", Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:chunk", ObjectKey: "disk/chunks/chunk", SizeBytes: 4}},
		}}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("durability reporting blocked on the external event sink")
	}
	select {
	case <-events.started:
		t.Fatal("durability path synchronously flushed its best-effort report")
	default:
	}
	reporter.mu.Lock()
	pending := len(reporter.pending)
	reporter.mu.Unlock()
	require.Equal(t, 2, pending, "both reports should remain queued for the reporter's periodic flush")
}

func TestAddRequestMountsPreparesDurableDisk(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "durable")
	spec := getTestBaseSpec()
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{{
			LocalPath: localPath,
			MountPath: types.PostgresDataMountPath,
			MountType: types.StorageModeDurableDisk,
			DurableDisk: &types.DurableDiskMountConfig{
				Name:       "pg-data",
				Size:       "10Gi",
				Filesystem: "ext4",
				Driver:     "snapshot",
			},
		}},
	}

	volumeCacheMap, err := (&Worker{}).addRequestMounts(request, &spec)

	require.NoError(t, err)
	require.Empty(t, volumeCacheMap)
	require.DirExists(t, localPath)
	require.FileExists(t, filepath.Join(localPath, ".beta9-durable-disk"))
	require.Len(t, spec.Mounts, 1)
	require.Equal(t, localPath, spec.Mounts[0].Source)
	require.Equal(t, request.Mounts[0].MountPath, spec.Mounts[0].Destination)
	require.Equal(t, []string{"rbind", "rw"}, spec.Mounts[0].Options)
}

func TestDurableDiskShouldKeepLocalPayload(t *testing.T) {
	snapshot := &types.DiskSnapshot{Generation: 2, ManifestDigest: "sha256:new"}

	require.False(t, durableDiskShouldKeepLocalPayload(durableDiskMarker{
		State:      durableDiskStateDirty,
		Generation: 1,
	}, snapshot))
	require.True(t, durableDiskShouldKeepLocalPayload(durableDiskMarker{
		State:      durableDiskStateDirty,
		Generation: 2,
	}, snapshot))
	require.True(t, durableDiskShouldKeepLocalPayload(durableDiskMarker{
		State:          durableDiskStateClean,
		Generation:     2,
		ManifestDigest: "sha256:new",
	}, snapshot))
	require.False(t, durableDiskShouldKeepLocalPayload(durableDiskMarker{
		State:          durableDiskStateClean,
		Generation:     2,
		ManifestDigest: "sha256:old",
	}, snapshot))
}

func TestPublishedDurableDiskGenerationSurvivesLocalMarkerFailure(t *testing.T) {
	blockedPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockedPath, []byte("blocked"), 0o600))
	mount := durableDiskTestMount(blockedPath)
	snapshot := &types.DiskSnapshot{
		ExternalId:     "snapshot-published",
		Generation:     42,
		ManifestDigest: "sha256:published",
	}

	recordPublishedDurableDiskMarker(mount, snapshot)

	require.Equal(t, "snapshot-published", snapshot.ExternalId)
	require.NoFileExists(t, filepath.Join(blockedPath, durableDiskMarkerFile))
}

func TestCreateDurableDiskDirectorySnapshotDedupesChunks(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "pg-data")
	require.NoError(t, os.MkdirAll(filepath.Join(source, "pgdata", "base"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(source, "pgdata", "pg_wal"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "1"), []byte("base-data"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "pg_wal", "0001"), []byte("wal1"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	first, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/pg-data/snapshots/1", types.DiskSnapshot{
		ExternalId: "snap-1",
		DiskName:   "pg-data",
		Format:     types.DiskSnapshotFormatPostgresWalV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotFormatPostgresWalV1, first.Format)
	require.NotEmpty(t, firstManifest.Files)

	baseFile := snapshotTestFile(firstManifest, "pgdata/base/1")
	require.NotEmpty(t, baseFile.Chunks)

	store.uploadCalls = 0
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "1"), []byte("base-data-mutated"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "2"), []byte("new-base-file"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "pg_wal", "0002"), []byte("wal2"), 0o600))
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/pg-data/snapshots/2", types.DiskSnapshot{
		DiskName:         "pg-data",
		Format:           types.DiskSnapshotFormatPostgresWalV1,
		ParentSnapshotId: first.ExternalId,
		Filesystem:       "ext4",
		Generation:       2,
	}, 4, firstManifest, false)
	require.NoError(t, err)
	require.Equal(t, first.ExternalId, second.ParentSnapshotId)
	require.NotEqual(t, baseFile.Chunks, snapshotTestFile(secondManifest, "pgdata/base/1").Chunks)
	require.NotEmpty(t, snapshotTestFile(secondManifest, "pgdata/base/2").Chunks)
	walFile := snapshotTestFile(secondManifest, "pgdata/pg_wal/0002")
	require.NotEmpty(t, walFile.Chunks)
	chunk := walFile.Chunks[0]
	chunkHash := strings.TrimPrefix(chunk.Digest, "sha256:")
	cacheReader := &fakeDurableDiskSnapshotCacheReader{objects: map[string][]byte{chunkHash: []byte("wal2")}}
	delete(store.objects, chunk.ObjectKey)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, cacheReader, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	require.Equal(t, 1, cacheReader.hits)
	data, err := os.ReadFile(filepath.Join(restored, "pgdata", "base", "1"))
	require.NoError(t, err)
	require.Equal(t, "base-data-mutated", string(data))
	data, err = os.ReadFile(filepath.Join(restored, "pgdata", "base", "2"))
	require.NoError(t, err)
	require.Equal(t, "new-base-file", string(data))
	data, err = os.ReadFile(filepath.Join(restored, "pgdata", "pg_wal", "0002"))
	require.NoError(t, err)
	require.Equal(t, "wal2", string(data))
}

func TestIncrementalSnapshotUploadsOnlyTheChangedChunk(t *testing.T) {
	source := t.TempDir()
	model := filepath.Join(source, "model.safetensors")
	require.NoError(t, os.WriteFile(model, []byte("aaaabbbbccccdddd"), 0o600))
	store := &fakeDurableDiskSnapshotStore{}

	_, first, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/model/snapshots/1",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1}, 4, nil, false,
	)
	require.NoError(t, err)
	before := snapshotTestFile(first, "model.safetensors")
	require.Len(t, before.Chunks, 4)

	store.uploadCalls = 0
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, os.WriteFile(model, []byte("aaaaxxxxccccdddd"), 0o600))
	_, second, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/model/snapshots/2",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1}, 4, first, false,
	)
	require.NoError(t, err)
	after := snapshotTestFile(second, "model.safetensors")
	require.Len(t, after.Chunks, 4)
	require.Equal(t, before.Chunks[0], after.Chunks[0])
	require.NotEqual(t, before.Chunks[1], after.Chunks[1])
	require.Equal(t, before.Chunks[2:], after.Chunks[2:])
	require.Equal(t, 2, store.uploadCalls, "only one new chunk and the manifest should be uploaded")
}

func TestCreateDurableDiskDirectorySnapshotUploadsChunksInParallel(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(filepath.Join(source, "model"), data, 0o600))

	store := &parallelUploadDurableDiskSnapshotStore{fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{}}
	_, _, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/model/snapshots/1", types.DiskSnapshot{
		DiskName: "model", Format: types.DiskSnapshotFormatDirV1, Filesystem: "ext4", Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	require.Greater(t, store.maxActive, 1)
	require.LessOrEqual(t, store.maxActive, durableDiskSnapshotUploadConcurrency)
}

// A real tree is mostly small files of one chunk, so the chunk pool is one deep unless
// separate files are in flight. This is what a package install's snapshot time turns on.
func TestCreateDurableDiskDirectorySnapshotSnapshotsSeparateFilesInParallel(t *testing.T) {
	source := t.TempDir()
	for i := range 16 {
		require.NoError(t, os.WriteFile(filepath.Join(source, fmt.Sprintf("file-%d", i)), []byte(fmt.Sprintf("contents-%d", i)), 0o600))
	}

	store := &parallelUploadDurableDiskSnapshotStore{fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{}}
	_, manifest, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/workspace/snapshots/1",
		types.DiskSnapshot{DiskName: "workspace", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)
	require.NoError(t, err)
	require.Len(t, manifest.Files, 16)
	require.Greater(t, store.maxActive, 1)
	require.LessOrEqual(t, store.maxActive, durableDiskSnapshotFileConcurrency)
}

// Identical files must cost one upload even when read at the same moment, or concurrency
// would undo the deduplication it was added to speed up.
func TestCreateDurableDiskDirectorySnapshotUploadsIdenticalFilesOnce(t *testing.T) {
	source := t.TempDir()
	for i := range 16 {
		require.NoError(t, os.WriteFile(filepath.Join(source, fmt.Sprintf("copy-%d", i)), []byte("identical contents"), 0o600))
	}

	store := &fakeDurableDiskSnapshotStore{}
	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/workspace/snapshots/1",
		types.DiskSnapshot{DiskName: "workspace", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)
	require.NoError(t, err)
	require.Len(t, manifest.Files, 16)

	_, chunkUploads := store.uploadCounts()
	require.Equal(t, 1, chunkUploads)
	require.Equal(t, int64(16), snapshot.ChunkCount, "every file still records the chunk it shares")
	for _, file := range manifest.Files {
		require.Len(t, file.Chunks, 1)
		require.Equal(t, manifest.Files[0].Chunks[0].ObjectKey, file.Chunks[0].ObjectKey)
	}
}

func TestDurableDiskSnapshotChunkBodyKeepsSmallChunksOutOfTheFilesystem(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		body := &durableDiskSnapshotChunkBody{}
		defer body.release()
		payload := bytes.Repeat([]byte{0x5a}, durableDiskSnapshotInlineChunkSize)
		written, err := body.Write(payload)

		require.NoError(t, err)
		require.Equal(t, len(payload), written)
		require.Nil(t, body.file, "a chunk that fits in hand should not cost a temp file")
		read, err := io.ReadAll(body.reader(int64(len(payload))))
		require.NoError(t, err)
		require.Equal(t, payload, read)
	})

	t.Run("large", func(t *testing.T) {
		body := &durableDiskSnapshotChunkBody{}
		payload := bytes.Repeat([]byte{0x5a}, durableDiskSnapshotInlineChunkSize+1)
		for _, part := range [][]byte{payload[:len(payload)-1], payload[len(payload)-1:]} {
			_, err := body.Write(part)
			require.NoError(t, err)
		}

		require.NotNil(t, body.file, "a chunk too large to hold should spill rather than be kept")
		name := body.file.Name()
		read, err := io.ReadAll(body.reader(int64(len(payload))))
		require.NoError(t, err)
		require.Equal(t, payload, read, "spilling keeps the bytes written before it in order")

		body.release()
		_, err = os.Stat(name)
		require.True(t, os.IsNotExist(err), "a released chunk should leave nothing behind")
	})

	// In memory or spilled, a chunk must arrive seekable, or every one pays for the
	// per-upload buffer the S3 manager allocates for a plain reader.
	t.Run("seekable either way", func(t *testing.T) {
		for _, size := range []int{durableDiskSnapshotInlineChunkSize, durableDiskSnapshotInlineChunkSize + 1} {
			body := &durableDiskSnapshotChunkBody{}
			_, err := body.Write(bytes.Repeat([]byte{0x5a}, size))
			require.NoError(t, err)

			var uploaded io.Reader = &durableDiskSnapshotProgressReader{
				ctx:    context.Background(),
				reader: body.reader(int64(size)),
			}
			_, seekable := uploaded.(io.Seeker)
			at, readableAt := uploaded.(io.ReaderAt)
			require.True(t, seekable, "the uploader was offered a chunk it cannot seek")
			require.True(t, readableAt, "the uploader was offered a chunk it cannot read at an offset")

			tail := make([]byte, 1)
			_, err = at.ReadAt(tail, int64(size)-1)
			require.NoError(t, err)
			require.Equal(t, []byte{0x5a}, tail)
			body.release()
		}
	})
}

func TestSnapshotDurableDiskReaderReturnsMidStreamReadError(t *testing.T) {
	readErr := fmt.Errorf("injected read failure")
	source := &failingDurableDiskSnapshotReaderAt{
		data:   []byte("first-chunk-and-more"),
		failAt: 10,
		err:    readErr,
	}
	store := &fakeDurableDiskSnapshotStore{}
	file := &types.DiskSnapshotFile{Path: "model", Type: "file"}

	err := snapshotDurableDiskReader(context.Background(), store, source, "model", "chunks", 8, newDurableDiskSnapshotChunkSet(), file)

	require.ErrorIs(t, err, readErr)
	require.Len(t, file.Chunks, 1)
	require.Equal(t, int64(8), file.Chunks[0].SizeBytes)
}

func TestSnapshotDurableDiskReaderCancelsSiblingUploadsOnReadError(t *testing.T) {
	readErr := syscall.EIO
	store := &cancellationBlockingDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{},
		started:                      make(chan struct{}),
		canceled:                     make(chan struct{}),
	}
	source := &failAfterUploadStartsReaderAt{
		data:          []byte("12345678"),
		uploadStarted: store.started,
		err:           readErr,
	}
	file := &types.DiskSnapshotFile{Path: "model", Type: "file"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- snapshotDurableDiskReader(ctx, store, source, "model", "chunks", 8, newDurableDiskSnapshotChunkSet(), file)
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, readErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("source read failure did not cancel the in-flight upload promptly")
	}
	select {
	case <-store.canceled:
	default:
		t.Fatal("in-flight upload did not observe cancellation")
	}
}

func TestSnapshotDurableDiskReaderSpoolsChunksWithFixedMemoryBuffer(t *testing.T) {
	data := make([]byte, 2*durableDiskSnapshotReadBufferSize+17)
	source := &trackingDurableDiskSnapshotReaderAt{reader: bytes.NewReader(data)}
	store := &fakeDurableDiskSnapshotStore{}
	file := &types.DiskSnapshotFile{Path: "model", Type: "file"}

	err := snapshotDurableDiskReader(
		context.Background(),
		store,
		source,
		"model",
		"chunks",
		4*durableDiskSnapshotReadBufferSize,
		newDurableDiskSnapshotChunkSet(),
		file,
	)

	require.NoError(t, err)
	require.Len(t, file.Chunks, 1)
	require.Equal(t, int64(len(data)), file.Chunks[0].SizeBytes)
	require.LessOrEqual(t, source.maxReadSize(), durableDiskSnapshotReadBufferSize)
}

func TestSnapshotDurableDiskReaderRetriesFromTheStartOfTheChunk(t *testing.T) {
	payload := []byte("complete model chunk")
	store := &retryingDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{},
		failures:                     1,
	}
	file := &types.DiskSnapshotFile{Path: "model", Type: "file"}

	err := snapshotDurableDiskReader(
		context.Background(), store, bytes.NewReader(payload), "model", "chunks", int64(len(payload)),
		newDurableDiskSnapshotChunkSet(), file,
	)

	require.NoError(t, err)
	require.Equal(t, 2, store.attempts)
	require.Equal(t, payload, store.lastBody, "a retry must rewind rather than upload an empty tail")
	require.Equal(t, 1, store.uploadCalls)
}

func TestCreateDurableDiskDirectorySnapshotRejectsFileMutationBeforePublishing(t *testing.T) {
	source := t.TempDir()
	filename := filepath.Join(source, "model.safetensors")
	require.NoError(t, os.WriteFile(filename, []byte("original"), 0o600))
	store := &mutatingDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{},
		mutate: func() error {
			return os.WriteFile(filename, []byte("modified-and-longer"), 0o600)
		},
	}

	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/model/snapshots/1",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)

	require.ErrorContains(t, err, "changed while snapshotting")
	require.Nil(t, snapshot)
	require.Nil(t, manifest)
}

func TestCreateDurableDiskDirectorySnapshotDoesNotPublishAfterUploadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "model"), []byte("model"), 0o600))
	store := &cancelingDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{},
		cancel:                       cancel,
	}

	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		ctx,
		store,
		source,
		"durable-disks/model/snapshots/1",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
		5,
		nil,
		false,
	)

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snapshot)
	require.Nil(t, manifest)
	require.Zero(t, store.uploadCalls)
}

func TestCreateDurableDiskDirectorySnapshotHonorsCanceledTreeWalk(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "model.safetensors"), []byte("model"), 0o600))
	store := &fakeDurableDiskSnapshotStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		ctx, store, source, "durable-disks/model/snapshots/1",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snapshot)
	require.Nil(t, manifest)
	require.Zero(t, store.uploadCalls)
}

func TestValidateDurableDiskSnapshotTreeHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := validateDurableDiskSnapshotTree(ctx, t.TempDir(), map[string]types.DiskSnapshotFile{})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCreateDurableDiskDirectorySnapshotRejectsNewFileBeforePublishing(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "model-00001.safetensors"), []byte("first shard"), 0o600))
	store := &mutatingDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{},
		mutate: func() error {
			return os.WriteFile(filepath.Join(source, "model-00002.safetensors"), []byte("second shard"), 0o600)
		},
	}

	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/model/snapshots/1",
		types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)

	require.ErrorContains(t, err, "changed while snapshotting")
	require.Nil(t, snapshot)
	require.Nil(t, manifest)
	for key := range store.objects {
		require.NotContains(t, key, "manifest.json")
	}
}

func TestCreateDurableDiskDirectorySnapshotReusesAppendOnlyTail(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "redis-data")
	aof := filepath.Join(source, "appendonlydir", "appendonly.aof.1.incr.aof")
	require.NoError(t, os.MkdirAll(filepath.Dir(aof), 0o700))
	require.NoError(t, os.WriteFile(aof, []byte("aaaabbbb"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	_, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/1", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	firstFile := snapshotTestFile(firstManifest, "appendonlydir/appendonly.aof.1.incr.aof")
	require.Len(t, firstFile.Chunks, 2)

	store.uploadCalls = 0
	require.NoError(t, os.WriteFile(aof, []byte("aaaabbbbcccc"), 0o600))
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/2", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest, false)
	require.NoError(t, err)
	secondFile := snapshotTestFile(secondManifest, "appendonlydir/appendonly.aof.1.incr.aof")
	require.Len(t, secondFile.Chunks, 3)
	require.Equal(t, firstFile.Chunks, secondFile.Chunks[:2])
	require.Equal(t, int64(8), secondFile.Chunks[2].OffsetBytes)
	require.Equal(t, 2, store.uploadCalls)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, nil, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(restored, "appendonlydir", "appendonly.aof.1.incr.aof"))
	require.NoError(t, err)
	require.Equal(t, "aaaabbbbcccc", string(data))
}

func TestCreateDurableDiskDirectorySnapshotRejectsRecreatedAppendOnlyPrefix(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "redis-data")
	aof := filepath.Join(source, "appendonlydir", "appendonly.aof.1.incr.aof")
	require.NoError(t, os.MkdirAll(filepath.Dir(aof), 0o700))
	require.NoError(t, os.WriteFile(aof, []byte("aaaabbbb"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	_, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/1", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)

	require.NoError(t, os.Remove(aof))
	require.NoError(t, os.WriteFile(aof, []byte("xxxxbbbbcccc"), 0o600))
	second, _, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/2", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest, false)
	require.NoError(t, err)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, nil, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(restored, "appendonlydir", "appendonly.aof.1.incr.aof"))
	require.NoError(t, err)
	require.Equal(t, "xxxxbbbbcccc", string(data))
}

func TestUnchangedCleanupDetectsSameSizeWriteWithRestoredMtime(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "data")
	file := filepath.Join(source, "dbfile")
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(file, []byte("aaaabbbb"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	_, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/1", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	firstFile := snapshotTestFile(firstManifest, "dbfile")
	require.NotEmpty(t, firstFile.Chunks)

	modTime := time.Unix(0, firstFile.ModTimeUnixNano)
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, os.WriteFile(file, []byte("xxxxbbbb"), 0o600))
	require.NoError(t, os.Chtimes(file, modTime, modTime))

	second, _, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/2", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest, true)
	require.NoError(t, err)
	require.NotNil(t, second, "same-size writes after the explicit snapshot must not be skipped at cleanup")

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, nil, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(restored, "dbfile"))
	require.NoError(t, err)
	require.Equal(t, "xxxxbbbb", string(data))
}

func TestCreateDurableDiskDirectorySnapshotPreservesEmptyDirectory(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "value"), []byte("present"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	_, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/1", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, firstManifest.Files)

	require.NoError(t, os.Remove(filepath.Join(source, "value")))
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/2", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest, false)
	require.NoError(t, err)
	require.Empty(t, secondManifest.Files)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, nil, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	entries, err := os.ReadDir(restored)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestExplicitDurableDiskSnapshotReusesUnchangedGeneration(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "value"), []byte("present"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	first, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/1", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil, true)
	require.NoError(t, err)
	require.NotNil(t, first)

	uploadsAfterFirst := store.uploadCalls
	store.resetUploadCounts()
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/2", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest, true)
	require.NoError(t, err)
	require.Nil(t, second, "an untouched disk should report no new generation")
	require.Nil(t, secondManifest)
	require.Positive(t, uploadsAfterFirst)
	uploadCalls, chunkUploadCalls := store.uploadCounts()
	require.Zero(t, uploadCalls, "not even the manifest should have been written")
	require.Zero(t, chunkUploadCalls)

	require.NoError(t, os.WriteFile(filepath.Join(source, "value"), []byte("changed"), 0o600))
	third, _, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/3", types.DiskSnapshot{
		DiskName:   "data",
		Format:     types.DiskSnapshotFormatDirV1,
		Filesystem: "ext4",
		Generation: 3,
	}, 4, firstManifest, true)
	require.NoError(t, err)
	require.NotNil(t, third)
}

func TestFinalDurableDiskSnapshotPublishesFreshUnchangedGeneration(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "value"), []byte("present"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	first, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/1", types.DiskSnapshot{
		DiskName: "data", Format: types.DiskSnapshotFormatDirV1, Filesystem: "ext4", Generation: 1,
	}, 4, nil, false)
	require.NoError(t, err)
	require.NotNil(t, first)
	store.resetUploadCounts()

	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/data/snapshots/2", types.DiskSnapshot{
		DiskName: "data", Format: types.DiskSnapshotFormatDirV1, Filesystem: "ext4", Generation: 2,
	}, 4, firstManifest, false)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, int64(2), second.Generation)
	require.NotEqual(t, first.ManifestKey, second.ManifestKey)
	require.Equal(t, firstManifest.Files, secondManifest.Files)
	uploadCalls, chunkUploadCalls := store.uploadCounts()
	require.Equal(t, 1, uploadCalls, "terminal finalization should upload only its fresh manifest")
	require.Zero(t, chunkUploadCalls, "terminal finalization should reuse every unchanged CAS chunk")
}

func TestDurableDiskSnapshotContentsMatchIgnoresFilesystemIdentity(t *testing.T) {
	before := &types.DiskSnapshotManifest{
		Format: types.DiskSnapshotFormatDirV1,
		Files: []types.DiskSnapshotFile{{
			Path: "value", Type: "file", Mode: 0o600, SizeBytes: 7, ModTimeUnixNano: 1,
			DeviceId: 10, Inode: 20, ChangeUnixNano: 99,
			Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:aaa", SizeBytes: 7}},
		}},
	}
	after := &types.DiskSnapshotManifest{
		Format: types.DiskSnapshotFormatDirV1,
		Files: []types.DiskSnapshotFile{{
			Path: "value", Type: "file", Mode: 0o600, SizeBytes: 7, ModTimeUnixNano: 1,
			DeviceId: 33, Inode: 44, ChangeUnixNano: 1234,
			Chunks: []types.DiskSnapshotChunk{{Digest: "sha256:aaa", SizeBytes: 7, ObjectKey: "elsewhere"}},
		}},
	}

	require.True(t, durableDiskSnapshotContentsMatch(before, after))

	after.Files[0].Chunks[0].Digest = "sha256:bbb"
	require.False(t, durableDiskSnapshotContentsMatch(before, after), "different bytes are a different disk")
}

func TestDurableDiskSnapshotRequiredContentItems(t *testing.T) {
	items := durableDiskSnapshotRequiredContentItems(&types.DiskSnapshot{
		BucketName:        "disk-bucket",
		DiskName:          "pg-data",
		Generation:        7,
		ManifestKey:       "durable-disks/pg-data/snapshots/7/manifest.json",
		ManifestDigest:    "sha256:" + strings.Repeat("a", 64),
		ManifestSizeBytes: 512,
	}, &types.DiskSnapshotManifest{
		Files: []types.DiskSnapshotFile{{
			Path: "pgdata/base/1",
			Type: "file",
			Chunks: []types.DiskSnapshotChunk{{
				ObjectKey: "durable-disks/pg-data/chunks/" + strings.Repeat("b", 64),
				Digest:    "sha256:" + strings.Repeat("b", 64),
				SizeBytes: 4096,
			}},
		}},
	})

	require.Len(t, items, 2)
	require.Equal(t, types.CacheContentKindDiskSnapshot, items[0].Kind)
	require.Equal(t, strings.Repeat("a", 64), items[0].Hash)
	require.Equal(t, "disk-bucket", items[0].SourceBucket)
	require.Equal(t, "durable-disks/pg-data/snapshots/7/manifest.json", items[0].Source)
	require.Equal(t, int64(512), items[0].SizeBytes)
	require.Equal(t, "pg-data", items[0].DiskName)
	require.Equal(t, int64(7), items[0].SnapshotGeneration)
	require.Equal(t, strings.Repeat("b", 64), items[1].Hash)
	require.Equal(t, "disk-bucket", items[1].SourceBucket)
	require.Equal(t, "pg-data", items[1].DiskName)
	require.Equal(t, int64(7), items[1].SnapshotGeneration)
}

func TestRestoreDurableDiskDirectorySnapshotDownloadsChunksInParallel(t *testing.T) {
	source := t.TempDir()
	payload := []byte(strings.Repeat("parallel restore ", durableDiskRestoreConcurrency))
	require.NoError(t, os.WriteFile(filepath.Join(source, "model"), payload, 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	snapshot, _, err := createDurableDiskDirectorySnapshot(
		context.Background(),
		store,
		source,
		"durable-disks/data/snapshots/1",
		types.DiskSnapshot{DiskName: "data"},
		4,
		nil,
		false,
	)
	require.NoError(t, err)

	parallelStore := &parallelDownloadDurableDiskSnapshotStore{
		fakeDurableDiskSnapshotStore: store,
	}
	target := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(
		context.Background(),
		parallelStore,
		nil,
		snapshot.ManifestKey,
		snapshot.ManifestDigest,
		snapshot.ManifestSizeBytes,
		target,
	)
	require.NoError(t, err)
	require.Greater(t, parallelStore.maxActive, 1)
	restored, err := os.ReadFile(filepath.Join(target, "model"))
	require.NoError(t, err)
	require.Equal(t, payload, restored)
}

func TestRestoreDurableDiskDirectorySnapshotDownloadsSeparateFilesInParallel(t *testing.T) {
	source := t.TempDir()
	for i := range 16 {
		require.NoError(t, os.WriteFile(filepath.Join(source, fmt.Sprintf("file-%d", i)), []byte(fmt.Sprintf("contents-%d", i)), 0o600))
	}

	store := &fakeDurableDiskSnapshotStore{}
	snapshot, _, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/workspace/snapshots/1",
		types.DiskSnapshot{DiskName: "workspace", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)
	require.NoError(t, err)

	parallelStore := &parallelDownloadDurableDiskSnapshotStore{fakeDurableDiskSnapshotStore: store}
	target := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(
		context.Background(), parallelStore, nil,
		snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes, target,
	)
	require.NoError(t, err)
	require.Greater(t, parallelStore.maxActive, 1)
	require.LessOrEqual(t, parallelStore.maxActive, durableDiskRestoreConcurrency)
	for i := range 16 {
		restored, err := os.ReadFile(filepath.Join(target, fmt.Sprintf("file-%d", i)))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("contents-%d", i), string(restored))
	}
}

func TestRestoreDurableDiskDirectorySnapshotPreservesTargetOnFailure(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "new"), []byte("new payload"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		context.Background(),
		store,
		source,
		"durable-disks/data/snapshots/1",
		types.DiskSnapshot{DiskName: "data"},
		4,
		nil,
		false,
	)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Files[0].Chunks)
	delete(store.objects, manifest.Files[0].Chunks[0].ObjectKey)

	parent := t.TempDir()
	target := filepath.Join(parent, "restored")
	require.NoError(t, os.MkdirAll(target, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(target, "old"), []byte("old payload"), 0o600))

	_, err = restoreDurableDiskDirectorySnapshotWithCache(
		context.Background(),
		store,
		nil,
		snapshot.ManifestKey,
		snapshot.ManifestDigest,
		snapshot.ManifestSizeBytes,
		target,
	)
	require.Error(t, err)
	data, readErr := os.ReadFile(filepath.Join(target, "old"))
	require.NoError(t, readErr)
	require.Equal(t, "old payload", string(data))
	staging, globErr := filepath.Glob(filepath.Join(parent, ".restored.restore-*"))
	require.NoError(t, globErr)
	require.Empty(t, staging)
}

func TestDurableDiskTransferTimeoutScalesWithSnapshotSize(t *testing.T) {
	require.Equal(t, 5*time.Minute, durableDiskTransferTimeout(0))
	require.Greater(t, durableDiskTransferTimeout(16<<30), 5*time.Minute)
	require.Equal(t, time.Hour, durableDiskTransferTimeout(1<<40))
}

func BenchmarkDurableDiskModelSnapshot(b *testing.B) {
	const modelSize = int64(256 << 20)

	for _, fixture := range []struct {
		name   string
		sparse bool
	}{
		{name: "dense-256MiB"},
		{name: "sparse-256MiB", sparse: true},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			source := b.TempDir()
			model := filepath.Join(source, "model.safetensors")
			if fixture.sparse {
				file, err := os.Create(model)
				require.NoError(b, err)
				require.NoError(b, file.Truncate(modelSize))
				_, err = file.WriteAt(bytes.Repeat([]byte{0x5a}, 1<<20), modelSize-(1<<20))
				require.NoError(b, err)
				require.NoError(b, file.Close())
			} else {
				file, err := os.Create(model)
				require.NoError(b, err)
				block := bytes.Repeat([]byte{0x5a}, 1<<20)
				for written := int64(0); written < modelSize; written += int64(len(block)) {
					_, err = file.Write(block)
					require.NoError(b, err)
				}
				require.NoError(b, file.Close())
			}

			store := &discardDurableDiskSnapshotStore{}
			_, previous, err := createDurableDiskDirectorySnapshot(
				context.Background(), store, source, "durable-disks/model/snapshots/baseline",
				types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
				defaultDurableDiskSnapshotChunkSize, nil, false,
			)
			require.NoError(b, err)

			b.Run("cold", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, _, err := createDurableDiskDirectorySnapshot(
						context.Background(), store, source, fmt.Sprintf("durable-disks/model/snapshots/cold-%d", i),
						types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
						defaultDurableDiskSnapshotChunkSize, nil, false,
					)
					require.NoError(b, err)
				}
			})
			b.Run("unchanged", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, _, err := createDurableDiskDirectorySnapshot(
						context.Background(), store, source, fmt.Sprintf("durable-disks/model/snapshots/unchanged-%d", i),
						types.DiskSnapshot{DiskName: "model", Format: types.DiskSnapshotFormatDirV1},
						defaultDurableDiskSnapshotChunkSize, previous, true,
					)
					require.NoError(b, err)
				}
			})
		})
	}
}

// A real workspace is thousands of small files, bounded by round trips rather than by the
// bytes the large-file benchmark above measures.
func BenchmarkDurableDiskWorkspaceSnapshot(b *testing.B) {
	source := b.TempDir()
	writeDurableDiskWorkspaceFixture(b, source, 2000, 4<<10)

	for b.Loop() {
		store := &latentDurableDiskSnapshotStore{fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{}, latency: 2 * time.Millisecond}
		_, _, err := createDurableDiskDirectorySnapshot(
			context.Background(), store, source, "durable-disks/workspace/snapshots/1",
			types.DiskSnapshot{DiskName: "workspace", Format: types.DiskSnapshotFormatDirV1},
			defaultDurableDiskSnapshotChunkSize, nil, false,
		)
		require.NoError(b, err)
	}
}

func BenchmarkDurableDiskWorkspaceRestore(b *testing.B) {
	source := b.TempDir()
	writeDurableDiskWorkspaceFixture(b, source, 2000, 4<<10)

	store := &latentDurableDiskSnapshotStore{fakeDurableDiskSnapshotStore: &fakeDurableDiskSnapshotStore{}, latency: 2 * time.Millisecond}
	snapshot, _, err := createDurableDiskDirectorySnapshot(
		context.Background(), store, source, "durable-disks/workspace/snapshots/1",
		types.DiskSnapshot{DiskName: "workspace", Format: types.DiskSnapshotFormatDirV1},
		defaultDurableDiskSnapshotChunkSize, nil, false,
	)
	require.NoError(b, err)

	for b.Loop() {
		target := filepath.Join(b.TempDir(), "restored")
		_, err := restoreDurableDiskDirectorySnapshotWithCache(
			context.Background(), store, nil,
			snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes, target,
		)
		require.NoError(b, err)
	}
}

// A package install, with contents unique per file so nothing dedupes away and the count is real.
func writeDurableDiskWorkspaceFixture(tb testing.TB, root string, files, size int) {
	tb.Helper()
	for i := range files {
		dir := filepath.Join(root, "node_modules", fmt.Sprintf("package-%02d", i%100))
		require.NoError(tb, os.MkdirAll(dir, 0o755))
		content := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, size/2)
		require.NoError(tb, os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%d.js", i)), content, 0o600))
	}
}

// An object store with a round trip, which is what a snapshot of many small files spends on.
type latentDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	latency time.Duration
}

func (s *latentDurableDiskSnapshotStore) Upload(ctx context.Context, key string, data []byte) error {
	time.Sleep(s.latency)
	return s.fakeDurableDiskSnapshotStore.Upload(ctx, key, data)
}

func (s *latentDurableDiskSnapshotStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	time.Sleep(s.latency)
	return s.fakeDurableDiskSnapshotStore.UploadWithReader(ctx, key, data)
}

func (s *latentDurableDiskSnapshotStore) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	time.Sleep(s.latency)
	return s.fakeDurableDiskSnapshotStore.DownloadWithReader(ctx, key)
}

func snapshotTestFile(manifest *types.DiskSnapshotManifest, name string) types.DiskSnapshotFile {
	for _, file := range manifest.Files {
		if file.Path == name {
			return file
		}
	}
	return types.DiskSnapshotFile{}
}

func durableDiskTestMount(primary string) *types.Mount {
	return &types.Mount{
		LocalPath: primary,
		MountPath: "/data",
		DurableDisk: &types.DurableDiskMountConfig{
			Name:   filepath.Base(primary),
			Driver: types.DurableDiskDriverSnapshot,
		},
	}
}

type fakeDurableDiskSnapshotStore struct {
	mu               sync.Mutex
	objects          map[string][]byte
	uploadCalls      int
	chunkUploadCalls int
}

type latestDiskSnapshotBackend struct {
	*fakeBackendRepoClient
	response *pb.GetLatestDiskSnapshotResponse
	err      error
}

type blockingRequiredContentEventRepo struct {
	*fakeEventRepo
	started chan struct{}
	once    sync.Once
}

func (r *blockingRequiredContentEventRepo) PushStubCacheRequiredContent(types.EventStubCacheRequiredContentSchema) error {
	r.once.Do(func() { close(r.started) })
	select {}
}

func (b *latestDiskSnapshotBackend) GetLatestDiskSnapshot(
	_ context.Context,
	_ *pb.GetLatestDiskSnapshotRequest,
	_ ...grpc.CallOption,
) (*pb.GetLatestDiskSnapshotResponse, error) {
	if b.response == nil && b.err == nil {
		return &pb.GetLatestDiskSnapshotResponse{Ok: true}, nil
	}
	return b.response, b.err
}

type discardDurableDiskSnapshotStore struct{}

func (*discardDurableDiskSnapshotStore) Upload(context.Context, string, []byte) error {
	return nil
}

func (*discardDurableDiskSnapshotStore) UploadWithReader(_ context.Context, _ string, data io.Reader) error {
	_, err := io.Copy(io.Discard, data)
	return err
}

func (*discardDurableDiskSnapshotStore) DownloadWithReader(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

type parallelUploadDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	activeMu  sync.Mutex
	active    int
	maxActive int
}

type cancelingDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	cancel context.CancelFunc
}

type cancellationBlockingDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	started      chan struct{}
	canceled     chan struct{}
	startedOnce  sync.Once
	canceledOnce sync.Once
}

type retryingDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	mu       sync.Mutex
	failures int
	attempts int
	lastBody []byte
}

func (s *retryingDurableDiskSnapshotStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.attempts++
	s.lastBody = append([]byte(nil), body...)
	attempt := s.attempts
	s.mu.Unlock()
	if attempt <= s.failures {
		return fmt.Errorf("transient upload failure")
	}
	return s.fakeDurableDiskSnapshotStore.Upload(ctx, key, body)
}

type mutatingDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	mutate func() error
	once   sync.Once
}

func (s *mutatingDurableDiskSnapshotStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.once.Do(func() {
		err = s.mutate()
	})
	if err != nil {
		return err
	}
	return s.fakeDurableDiskSnapshotStore.Upload(ctx, key, body)
}

func (s *cancelingDurableDiskSnapshotStore) UploadWithReader(context.Context, string, io.Reader) error {
	s.cancel()
	return context.Canceled
}

type failingDurableDiskSnapshotReaderAt struct {
	data   []byte
	failAt int64
	err    error
}

type failAfterUploadStartsReaderAt struct {
	data          []byte
	uploadStarted <-chan struct{}
	err           error
}

func (r *failAfterUploadStartsReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= int64(len(r.data)) {
		<-r.uploadStarted
		return 0, r.err
	}
	n := copy(p, r.data[offset:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *failingDurableDiskSnapshotReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= r.failAt {
		return 0, r.err
	}
	available := int64(len(r.data)) - offset
	if available <= 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), available, r.failAt-offset)
	copied := copy(p, r.data[offset:offset+n])
	if offset+int64(copied) >= r.failAt {
		return copied, r.err
	}
	if copied < len(p) {
		return copied, io.EOF
	}
	return copied, nil
}

type trackingDurableDiskSnapshotReaderAt struct {
	reader  io.ReaderAt
	mu      sync.Mutex
	maxRead int
}

func (r *trackingDurableDiskSnapshotReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.Lock()
	r.maxRead = max(r.maxRead, len(p))
	r.mu.Unlock()
	return r.reader.ReadAt(p, offset)
}

func (r *trackingDurableDiskSnapshotReaderAt) maxReadSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxRead
}

func (s *parallelUploadDurableDiskSnapshotStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.activeMu.Lock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	s.activeMu.Unlock()
	time.Sleep(20 * time.Millisecond)
	err = s.fakeDurableDiskSnapshotStore.Upload(ctx, key, body)
	s.activeMu.Lock()
	s.active--
	s.activeMu.Unlock()
	return err
}

func (s *cancellationBlockingDurableDiskSnapshotStore) UploadWithReader(ctx context.Context, _ string, _ io.Reader) error {
	s.startedOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	s.canceledOnce.Do(func() { close(s.canceled) })
	return ctx.Err()
}

type parallelDownloadDurableDiskSnapshotStore struct {
	*fakeDurableDiskSnapshotStore
	mu        sync.Mutex
	active    int
	maxActive int
}

func (s *parallelDownloadDurableDiskSnapshotStore) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if strings.HasSuffix(key, "/manifest.json") {
		return s.fakeDurableDiskSnapshotStore.DownloadWithReader(ctx, key)
	}

	s.mu.Lock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	s.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return s.fakeDurableDiskSnapshotStore.DownloadWithReader(ctx, key)
}

func (s *fakeDurableDiskSnapshotStore) Upload(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploadCalls++
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *fakeDurableDiskSnapshotStore) UploadWithReader(_ context.Context, key string, data io.Reader) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.chunkUploadCalls++
	s.mu.Unlock()
	return s.Upload(context.Background(), key, body)
}

func (s *fakeDurableDiskSnapshotStore) resetUploadCounts() {
	s.mu.Lock()
	s.uploadCalls = 0
	s.chunkUploadCalls = 0
	s.mu.Unlock()
}

func (s *fakeDurableDiskSnapshotStore) uploadCounts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploadCalls, s.chunkUploadCalls
}

func (s *fakeDurableDiskSnapshotStore) DownloadWithReader(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	data := append([]byte(nil), s.objects[key]...)
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(data)), nil
}

type fakeDurableDiskSnapshotCacheReader struct {
	objects map[string][]byte
	mu      sync.Mutex
	calls   int
	hits    int
}

func (s *fakeDurableDiskSnapshotCacheReader) ReadContentInto(_ context.Context, hash string, offset int64, dest []byte, _ cache.ClientOptions) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	data, ok := s.objects[hash]
	if !ok {
		return 0, fmt.Errorf("cache miss")
	}
	s.hits++
	end := offset + int64(len(dest))
	if offset < 0 || end > int64(len(data)) {
		return 0, fmt.Errorf("invalid cache range")
	}
	return int64(copy(dest, data[offset:end])), nil
}
