package worker

import (
	"context"
	"errors"
	"testing"
	"time"

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
		{name: "oom", got: int(types.ContainerExitCodeOomKill), want: int(types.ContainerExitCodeOomKill)},
		{name: "existing failure", got: 42, want: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, durableDiskSyncFailureExitCode(test.got))
		})
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
	ctx, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-progress", time.Hour)

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
	ctx, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-progress-retry", 10*time.Millisecond)
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
	_, stop := worker.durableDiskStoppingProgressContext(context.Background(), "container-static", 10*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	stop()

	require.Empty(t, repoClient.containerStatusUpdates())
}
