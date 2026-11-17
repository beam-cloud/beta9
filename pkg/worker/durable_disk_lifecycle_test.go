package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTerminalMicroVMCheckpointKeepsItsSealedDiskGeneration(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		request := &types.ContainerRequest{
			ContainerId: "paused-vm",
			Mounts:      []types.Mount{{LocalPath: t.TempDir(), DurableDisk: &types.DurableDiskMountConfig{Name: "root", Size: "1Gi", Driver: types.DurableDiskDriverQcow}}},
		}
		require.NoError(t, os.WriteFile(filepath.Join(request.Mounts[0].LocalPath, "guest-state"), []byte("paired with memory"), 0600))
		worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, nil)
		worker.backendRepoClient = &fakeBackendRepoClient{}
		worker.diskManager = disk.NewManager(disk.Config{Root: t.TempDir()})
		instance := &ContainerInstance{Runtime: NewMockRuntime("microvm", runtime.Capabilities{BlockRoot: true})}
		instance.terminalCheckpointCreated.Store(terminal)
		worker.containerInstances.Set(request.ContainerId, instance)
		// The checkpoint's disk is already offline. A regular stop must refuse
		// an absent disk; a successful memory checkpoint must only detach it.
		code, _ := worker.finalizeDurableDiskMountsWithContext(context.Background(), request.ContainerId, request, int(types.ContainerExitCodeUser), true)
		if terminal {
			require.Equal(t, int(types.ContainerExitCodeUser), code)
		} else {
			require.Equal(t, int(types.ContainerExitCodeUnknownError), code)
		}
	}
}

func TestFailedBlockRootRestoreKeepsPairedDisks(t *testing.T) {
	for _, started := range []bool{false, true} {
		request := &types.ContainerRequest{
			ContainerId: "failed-memory-restore",
			Checkpoint:  &types.Checkpoint{CheckpointId: "saved-ram"},
			Mounts: []types.Mount{{LocalPath: t.TempDir(), DurableDisk: &types.DurableDiskMountConfig{
				Name: "paired-root", Size: "1Gi", Driver: types.DurableDiskDriverSnapshot,
			}}},
		}
		worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, nil)
		worker.backendRepoClient = &fakeBackendRepoClient{}
		instance, _ := worker.containerInstances.Get(request.ContainerId)
		instance.Runtime = NewMockRuntime("microvm", runtime.Capabilities{BlockRoot: true})
		if started {
			instance.markRuntimeStarted(1)
		}
		code, _ := worker.finalizeDurableDiskMountsWithContext(context.Background(), request.ContainerId, request, int(types.ContainerExitCodeStartupFailed), true)
		if started {
			require.Equal(t, int(types.ContainerExitCodeUnknownError), code, "a guest that ran must still publish its writes")
		} else {
			require.Equal(t, int(types.ContainerExitCodeStartupFailed), code, "unused paired disks must not require another snapshot")
		}
	}
}

// fakeVMDiskHost stands in for the tools that attach a VM's disk: qemu-img
// leaves an image behind, the daemon its pidfile and export socket, and
// formatting succeeds.
func fakeVMDiskHost(_ context.Context, name string, args ...string) ([]byte, error) {
	switch name {
	case "qemu-img":
		if args[0] == "create" {
			return nil, os.WriteFile(args[len(args)-2], []byte("qcow2-stub"), 0o600)
		}
		if args[0] == "convert" {
			return nil, os.WriteFile(args[len(args)-1], []byte("qcow2-stub"), 0o600)
		}
	case "qemu-storage-daemon":
		for i, arg := range args {
			if arg == "--pidfile" {
				if err := os.WriteFile(args[i+1], []byte("4242\n"), 0o600); err != nil {
					return nil, err
				}
			}
			if _, socket, ok := strings.Cut(arg, "addr.path="); ok && strings.HasPrefix(arg, "type=vhost-user-blk,") {
				socket, _, _ = strings.Cut(socket, ",")
				if err := os.WriteFile(socket, nil, 0o600); err != nil {
					return nil, err
				}
			}
		}
	}
	return nil, nil
}

// diskPublications counts what publishing a disk asks of the object store
// and of the backend.
type diskPublications struct {
	*fakeBackendRepoClient
	storeRequests, snapshotLookups atomic.Int32
}

func (p *diskPublications) GetLatestDiskSnapshot(ctx context.Context, in *pb.GetLatestDiskSnapshotRequest, opts ...grpc.CallOption) (*pb.GetLatestDiskSnapshotResponse, error) {
	p.snapshotLookups.Add(1)
	return p.fakeBackendRepoClient.GetLatestDiskSnapshot(ctx, in, opts...)
}

// attachedVMRoot is a running VM whose root disk is attached on its worker.
func attachedVMRoot(t *testing.T, skipExitSnapshot bool) (*Worker, *types.ContainerRequest, *diskPublications) {
	t.Helper()
	publications := &diskPublications{fakeBackendRepoClient: &fakeBackendRepoClient{}}
	store := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { publications.storeRequests.Add(1) }))
	t.Cleanup(store.Close)
	id, endpoint, region, credential, bucket := uint(1), store.URL, "us-east-1", "test", "test"
	request := &types.ContainerRequest{
		ContainerId: "vm-stub-12345678",
		WorkspaceId: "workspace",
		Workspace: types.Workspace{Name: "test", Storage: &types.WorkspaceStorage{
			Id: &id, EndpointUrl: &endpoint, Region: &region, AccessKey: &credential, SecretKey: &credential, BucketName: &bucket,
		}},
		Mounts: []types.Mount{{
			LocalPath: filepath.Join(t.TempDir(), "vm-root"),
			MountPath: types.DurableDiskRootMountPath,
			MountType: types.StorageModeDurableDisk,
			DurableDisk: &types.DurableDiskMountConfig{
				Name: "vm-root", Size: "1Gi", Driver: types.DurableDiskDriverQcow, SkipExitSnapshot: skipExitSnapshot,
			},
		}},
	}
	worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, NewMockRuntime("microvm", runtime.Capabilities{BlockRoot: true}))
	worker.backendRepoClient = publications
	root := t.TempDir()
	worker.diskManager = disk.NewManager(disk.Config{
		Root: filepath.Join(root, "disks"), SysBlockPath: filepath.Join(root, "sys"), DevPath: filepath.Join(root, "dev"), Runner: fakeVMDiskHost,
	})
	_, err := worker.diskManager.Attach(context.Background(), disk.AttachSpec{
		Key: worker.qcowVolumeKey(request, &request.Mounts[0]), VirtualSizeBytes: 1 << 30, Export: disk.ExportVhostUser, Owner: request.ContainerId,
	}, nil)
	require.NoError(t, err)
	return worker, request, publications
}

func localQcowLayers(t *testing.T, worker *Worker) []string {
	t.Helper()
	var layers []string
	require.NoError(t, filepath.WalkDir(worker.diskManager.Root(), func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".qcow2") {
			layers = append(layers, path)
		}
		return err
	}))
	return layers
}

// A root that is not captured goes with its VM: nothing is asked of the
// object store or the backend, and the local volume, which pruning never
// reclaims because nothing in it was published, is removed. A captured root
// still has to publish, and keeps its local layers for the next attachment.
func TestExitDiscardsARootThatIsNotCaptured(t *testing.T) {
	for _, skipExitSnapshot := range []bool{true, false} {
		t.Run(fmt.Sprintf("skip=%v", skipExitSnapshot), func(t *testing.T) {
			worker, request, publications := attachedVMRoot(t, skipExitSnapshot)
			require.Len(t, localQcowLayers(t, worker), 1)

			code, reported := worker.finalizeDurableDiskMountsWithContext(context.Background(), request.ContainerId, request, int(types.ContainerExitCodeUser), true)

			_, attached := worker.diskManager.Volume(worker.qcowVolumeKey(request, &request.Mounts[0]))
			require.False(t, attached, "the disk is detached either way")
			if !skipExitSnapshot {
				require.Equal(t, int(types.ContainerExitCodeUnknownError), code, "a captured root must publish, and this daemon cannot seal it")
				require.False(t, reported)
				require.Positive(t, publications.storeRequests.Load())
				require.EqualValues(t, 1, publications.snapshotLookups.Load())
				require.Len(t, localQcowLayers(t, worker), 1)
				return
			}
			require.Equal(t, int(types.ContainerExitCodeUser), code)
			require.True(t, reported)
			require.Zero(t, publications.storeRequests.Load())
			require.Zero(t, publications.snapshotLookups.Load())
			require.Empty(t, localQcowLayers(t, worker))
		})
	}
}

// Nothing else publishes such a root either. A snapshot leaves it attached
// and as it was, and the memory of its VM is not saved at all: there would
// be no disk to resume it on.
func TestNoBoundaryPublishesARootThatIsNotCaptured(t *testing.T) {
	worker, request, publications := attachedVMRoot(t, true)
	for _, mode := range []durableDiskSyncMode{durableDiskSyncExplicit, durableDiskSyncMemoryCheckpoint} {
		snapshots, err := worker.syncDurableDiskMounts(context.Background(), request, mode)
		require.NoError(t, err)
		require.Empty(t, snapshots)
	}
	err := worker.createCheckpoint(context.Background(), &CreateCheckpointOpts{Request: request, CheckpointId: "memory", TerminateAfterCheckpoint: true})
	require.ErrorContains(t, err, "vm-root is not captured")

	_, attached := worker.diskManager.Volume(worker.qcowVolumeKey(request, &request.Mounts[0]))
	require.True(t, attached)
	require.Len(t, localQcowLayers(t, worker), 1)
	require.Zero(t, publications.storeRequests.Load())
	require.Zero(t, publications.snapshotLookups.Load())
	require.Zero(t, publications.createCalls, "a refused checkpoint leaves no record behind")
}

func TestDurableDiskCleanupContextIgnoresWorkerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, (&Worker{ctx: ctx}).durableDiskCleanupContext().Err())
}

func TestFailedStartupFinalizesOnlyAcquiredDisks(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprint(started), func(t *testing.T) {
			request := &types.ContainerRequest{ContainerId: "failed-attach", Mounts: []types.Mount{{
				LocalPath:   t.TempDir(),
				DurableDisk: &types.DurableDiskMountConfig{Name: "root", Size: "1Gi", Driver: types.DurableDiskDriverQcow},
			}}}
			worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, nil)
			worker.diskManager = disk.NewManager(disk.Config{Root: t.TempDir()})
			worker.backendRepoClient = &fakeBackendRepoClient{}
			instance, _ := worker.containerInstances.Get(request.ContainerId)
			if started {
				instance.markRuntimeStarted(1)
			}
			code, reported := worker.finalizeDurableDiskMountsWithContext(context.Background(), request.ContainerId, request, int(types.ContainerExitCodeStartupFailed), true)
			if started {
				require.Equal(t, int(types.ContainerExitCodeUnknownError), code, "a running guest's missing disk must fail closed")
				require.False(t, reported)
			} else {
				require.Equal(t, int(types.ContainerExitCodeStartupFailed), code)
				require.True(t, reported)
			}
			require.Len(t, request.Mounts, 1, "finalization must not mutate the original request")
		})
	}
}

func TestDurableDiskCleanupBudgetCoversEveryDiskAllowance(t *testing.T) {
	request := &types.ContainerRequest{Mounts: []types.Mount{
		{DurableDisk: &types.DurableDiskMountConfig{Size: "1Ti"}},
		{DurableDisk: &types.DurableDiskMountConfig{Size: "16Gi"}},
	}}
	oneTiB, err := durableDiskSizeBytes("1Ti")
	require.NoError(t, err)
	sixteenGiB, err := durableDiskSizeBytes("16Gi")
	require.NoError(t, err)
	perDiskAllowances := 2*durableDiskLockWait + durableDiskTransferTimeout(oneTiB) + durableDiskTransferTimeout(sixteenGiB)

	budget := durableDiskCleanupBudget(request)
	require.GreaterOrEqual(t, budget, perDiskAllowances+2*durableDiskCleanupGrace)
	// A stalled sync must reach its inactivity deadline before its STOPPING lease expires.
	require.Greater(t, time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second, durableDiskSnapshotInactivityTimeout)
	require.Equal(
		t,
		time.Duration(1<<63-1),
		addDurableDiskCleanupBudget(time.Duration(1<<63-1)-time.Second, 2*time.Second),
	)
}

func TestDurableDiskSyncFailureExitCode(t *testing.T) {
	for _, test := range []struct {
		name string
		got  int
		vm   bool
		want int
	}{
		{name: "success", got: int(types.ContainerExitCodeSuccess), want: int(types.ContainerExitCodeUnknownError)},
		{name: "scheduler stop", got: int(types.ContainerExitCodeScheduler), want: int(types.ContainerExitCodeUnknownError)},
		{name: "ttl stop", got: int(types.ContainerExitCodeTtl), want: int(types.ContainerExitCodeUnknownError)},
		{name: "user stop", got: int(types.ContainerExitCodeUser), want: int(types.ContainerExitCodeUnknownError)},
		{name: "admin stop", got: int(types.ContainerExitCodeAdmin), want: int(types.ContainerExitCodeUnknownError)},
		{name: "eviction", got: int(types.ContainerExitCodeEvicted), want: int(types.ContainerExitCodeEvicted)},
		{name: "VM eviction", got: int(types.ContainerExitCodeEvicted), vm: true, want: int(types.ContainerExitCodeUnknownError)},
		{name: "oom", got: int(types.ContainerExitCodeOomKill), want: int(types.ContainerExitCodeOomKill)},
		{name: "existing failure", got: 42, want: 42},
		{name: "startup failure with failed final sync", got: int(types.ContainerExitCodeStartupFailed), want: int(types.ContainerExitCodeUnknownError)},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, durableDiskSyncFailureExitCode(test.got, test.vm))
		})
	}
}

// Only a container that failed on its own leaves a database disk's writes in
// its journal; clean exits and platform stops still publish a generation.
func TestDurableDiskFinalSyncMode(t *testing.T) {
	for _, code := range []types.ContainerExitCode{
		types.ContainerExitCodeSuccess,
		types.ContainerExitCodeScheduler,
		types.ContainerExitCodeTtl,
		types.ContainerExitCodeUser,
		types.ContainerExitCodeAdmin,
		types.ContainerExitCodeEvicted,
	} {
		require.Equal(t, durableDiskSyncFinal, durableDiskFinalSyncMode(int(code)), "exit code %d", code)
	}
	for _, code := range []types.ContainerExitCode{
		types.ContainerExitCodeUnknownError,
		types.ContainerExitCodeOomKill,
		types.ContainerExitCodeInvalidCustomImage,
		42,
	} {
		require.Equal(t, durableDiskSyncFailed, durableDiskFinalSyncMode(int(code)), "exit code %d", code)
	}
}

func TestFinalizeDurableDiskMountsWithCanceledContextReportsFailure(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container-canceled-durable-finalization",
		Mounts: []types.Mount{{
			LocalPath: t.TempDir(),
			DurableDisk: &types.DurableDiskMountConfig{
				Name: "disk-canceled-finalization",
				Size: "1Gi",
			},
		}},
	}
	worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	exitCode, exitReported := worker.finalizeDurableDiskMountsWithContext(
		ctx,
		request.ContainerId,
		request,
		int(types.ContainerExitCodeSuccess),
		true,
	)

	require.Equal(t, int(types.ContainerExitCodeUnknownError), exitCode)
	require.False(t, exitReported)
	instance, exists := worker.containerInstances.Get(request.ContainerId)
	require.True(t, exists)
	localExitCode, _ := instance.lifecycleState()
	require.Equal(t, int(types.ContainerExitCodeUnknownError), localExitCode)
}

func TestDurableDiskProgressRefreshIsAsynchronousAndCoalesced(t *testing.T) {
	repoClient := &fakeContainerRepoClient{updateStatusStarted: make(chan struct{}, 1)}
	worker := &Worker{containerRepoClient: repoClient}
	ctx, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-progress", snapshotLeaseEvery(time.Hour))

	started := time.Now()
	for range 100 {
		reportDurableDiskProgress(ctx, durableDiskProgressEvent{logicalBytes: 16 << 20, files: 1, chunks: 1})
	}
	require.Less(t, time.Since(started), 100*time.Millisecond, "progress reporting must not block the snapshot hot path")

	select {
	case <-repoClient.updateStatusStarted:
	case <-time.After(time.Second):
		t.Fatal("first real progress did not refresh the STOPPING lease")
	}
	stop()
	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 1)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
}

func TestDurableDiskProgressRetriesLeaseRefreshAfterTransientFailure(t *testing.T) {
	repoClient := &fakeContainerRepoClient{updateStatusErrors: []error{errors.New("transient repository failure")}}
	worker := &Worker{containerRepoClient: repoClient}
	ctx, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-progress-retry", snapshotLeaseEvery(10*time.Millisecond))
	defer stop()

	reportDurableDiskProgress(ctx, durableDiskProgressEvent{logicalBytes: 1})
	require.Eventually(t, func() bool {
		return len(repoClient.containerStatusUpdates()) == 1
	}, time.Second, time.Millisecond)

	// A failed best-effort refresh must not disable later progress leases.
	reportDurableDiskProgress(ctx, durableDiskProgressEvent{logicalBytes: 1})
	require.Eventually(t, func() bool {
		return len(repoClient.containerStatusUpdates()) >= 2
	}, time.Second, time.Millisecond)

	updates := repoClient.containerStatusUpdates()
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[1].ExpirySeconds)
}

func TestDurableDiskProgressDoesNotRefreshAStaticStoppingState(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{containerRepoClient: repoClient}
	_, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-static", snapshotLeaseEvery(10*time.Millisecond))

	time.Sleep(30 * time.Millisecond)
	stop()

	require.Empty(t, repoClient.containerStatusUpdates())
}

// A dead worker cannot renew a journaled disk's lease, so its replacement
// waits about one journal lease. A live worker renews it while finalization
// is quiet, so the gateway never starts a replacement the journal would fence.
func TestJournaledDiskStoppingLeaseRenewsWithoutProgress(t *testing.T) {
	lease := journaledDiskStoppingLease
	require.Less(t, 2*lease.refresh, time.Duration(lease.expirySeconds)*time.Second, "one missed renewal must not lapse the lease")
	require.Less(t, lease.expirySeconds, types.ContainerStateTtlSWhileStopping)

	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{containerRepoClient: repoClient}
	lease.refresh = 10 * time.Millisecond
	_, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-journaled", lease)
	require.Eventually(t, func() bool {
		return len(repoClient.containerStatusUpdates()) >= 2
	}, time.Second, time.Millisecond)
	stop()

	for _, update := range repoClient.containerStatusUpdates() {
		require.Equal(t, string(types.ContainerStatusStopping), update.Status)
		require.Equal(t, journaledDiskStoppingLease.expirySeconds, update.ExpirySeconds)
	}
}

func TestDurableDiskStoppingLeaseIsShortOnlyWhenEveryWritableDiskIsJournaled(t *testing.T) {
	journaled := types.Mount{MountPath: "/data", DurableDisk: &types.DurableDiskMountConfig{Name: "db", Size: "1Gi", Driver: types.DurableDiskDriverQcow}}
	snapshot := types.Mount{MountPath: "/files", DurableDisk: &types.DurableDiskMountConfig{Name: "files", Size: "1Gi", Driver: types.DurableDiskDriverSnapshot}}
	readOnly := types.Mount{MountPath: "/seed", ReadOnly: true, DurableDisk: &types.DurableDiskMountConfig{Name: "seed", Size: "1Gi", Driver: types.DurableDiskDriverSnapshot}}

	for _, test := range []struct {
		name    string
		request *types.ContainerRequest
		want    stoppingLease
	}{
		{"journaled database disk", databaseRequest(t, "object-store-flush", journaled), journaledDiskStoppingLease},
		{"journaled disk beside a read-only disk", databaseRequest(t, "object-store-flush", journaled, readOnly), journaledDiskStoppingLease},
		{"journaled disk beside a snapshot disk", databaseRequest(t, "object-store-flush", journaled, snapshot), snapshotDiskStoppingLease},
		{"database without flush durability", databaseRequest(t, "", journaled), snapshotDiskStoppingLease},
		{"snapshot database disk", databaseRequest(t, "object-store-flush", snapshot), snapshotDiskStoppingLease},
		{"application disk", &types.ContainerRequest{Mounts: []types.Mount{journaled}}, snapshotDiskStoppingLease},
		{"no request", nil, snapshotDiskStoppingLease},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, durableDiskStoppingLease(test.request))
		})
	}
}

func TestClearContainerPublishesTheStoppingLease(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason types.StopContainerReason
		want   stoppingLease
	}{
		{"journaled disk", "", journaledDiskStoppingLease},
		// The scheduler counts an eviction victim's resources only while its
		// state exists, which must outlast its finalization.
		{"eviction victim with a journaled disk", types.StopContainerReasonEvicted, snapshotDiskStoppingLease},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := databaseRequest(t, "object-store-flush", types.Mount{
				MountPath:   "/data",
				DurableDisk: &types.DurableDiskMountConfig{Name: "db", Size: "1Gi", Driver: types.DurableDiskDriverQcow},
			})
			repoClient := &fakeContainerRepoClient{}
			worker := newContainerFinalizationTestWorker(request, repoClient, nil)
			worker.diskManager = disk.NewManager(disk.Config{Root: t.TempDir()})
			instance, exists := worker.containerInstances.Get(request.ContainerId)
			require.True(t, exists)
			instance.setStopReason(test.reason)

			require.Equal(t, test.want, worker.containerStoppingLease(request.ContainerId, request))
			worker.clearContainer(request.ContainerId, request, 0, false)

			updates := repoClient.containerStatusUpdates()
			require.NotEmpty(t, updates)
			require.Equal(t, string(types.ContainerStatusStopping), updates[0].Status)
			require.Equal(t, test.want.expirySeconds, updates[0].ExpirySeconds)
		})
	}
}

func TestEvictedContainerReleasesResourcesBeforeItsFinalSync(t *testing.T) {
	fixture := newLockedDurableDiskFinalizationFixture(t, "container-evicted-sync")
	request := fixture.request
	worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, nil)
	instance, _ := worker.containerInstances.Get(request.ContainerId)
	instance.CPUSet = "2-3"
	instance.setStopReason(types.StopContainerReasonEvicted)
	clearDone := clearContainerAsync(worker, request, int(types.ContainerExitCodeEvicted))

	// The request that evicted it may start while the sync waits on the disk lock.
	require.Empty(t, worker.waitForContainersReleased(context.Background(), []string{request.ContainerId}, time.Second))
	require.Empty(t, instance.CPUSet)
	_, syncing := worker.containerInstances.Get(request.ContainerId)
	require.True(t, syncing)
	select {
	case <-clearDone:
		t.Fatal("finalization finished while the disk lock was held")
	default:
	}
	fixture.releaseAndWaitForClear(t, clearDone)
}

func TestEvictedContainerIsReleasedOnceItsRuntimeIsSeenStopped(t *testing.T) {
	fixture := newLockedDurableDiskFinalizationFixture(t, "container-evicted-stale-state")
	request := fixture.request
	// The runtime still reports the container running when clearContainer first checks.
	stoppedAt := time.Now().Add(300 * time.Millisecond)
	rt := &mockRuntime{state: func(context.Context, string) (runtime.State, error) {
		if time.Now().Before(stoppedAt) {
			return runtime.State{Status: types.RuncContainerStatusRunning}, nil
		}
		return runtime.State{Status: types.RuncContainerStatusStopped}, nil
	}}
	worker := newContainerFinalizationTestWorker(request, &fakeContainerRepoClient{}, rt)
	instance, _ := worker.containerInstances.Get(request.ContainerId)
	instance.setStopReason(types.StopContainerReasonEvicted)
	clearDone := clearContainerAsync(worker, request, int(types.ContainerExitCodeEvicted))

	require.Empty(t, worker.waitForContainersReleased(context.Background(), []string{request.ContainerId}, 5*time.Second))
	_, syncing := worker.containerInstances.Get(request.ContainerId)
	require.True(t, syncing)
	fixture.releaseAndWaitForClear(t, clearDone)
}

func snapshotLeaseEvery(refresh time.Duration) stoppingLease {
	lease := snapshotDiskStoppingLease
	lease.refresh = refresh
	return lease
}

func databaseRequest(t *testing.T, durability string, mounts ...types.Mount) *types.ContainerRequest {
	t.Helper()
	config, err := json.Marshal(types.StubConfigV1{Serving: &types.ServingConfig{
		Database: &types.DatabaseServingConfig{DurabilityMode: durability},
	}})
	require.NoError(t, err)
	request := &types.ContainerRequest{ContainerId: "container-database", Mounts: mounts}
	request.Stub.Config = string(config)
	return request
}
