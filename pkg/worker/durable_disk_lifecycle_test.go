package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDurableDiskCleanupContextIgnoresWorkerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, (&Worker{ctx: ctx}).durableDiskCleanupContext().Err())
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
		want int
	}{
		{name: "success", got: int(types.ContainerExitCodeSuccess), want: int(types.ContainerExitCodeUnknownError)},
		{name: "scheduler stop", got: int(types.ContainerExitCodeScheduler), want: int(types.ContainerExitCodeUnknownError)},
		{name: "ttl stop", got: int(types.ContainerExitCodeTtl), want: int(types.ContainerExitCodeUnknownError)},
		{name: "user stop", got: int(types.ContainerExitCodeUser), want: int(types.ContainerExitCodeUnknownError)},
		{name: "admin stop", got: int(types.ContainerExitCodeAdmin), want: int(types.ContainerExitCodeUnknownError)},
		{name: "eviction", got: int(types.ContainerExitCodeEvicted), want: int(types.ContainerExitCodeEvicted)},
		{name: "oom", got: int(types.ContainerExitCodeOomKill), want: int(types.ContainerExitCodeOomKill)},
		{name: "existing failure", got: 42, want: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, durableDiskSyncFailureExitCode(test.got))
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
		// state exists, and the victim holds them until finalization ends.
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
