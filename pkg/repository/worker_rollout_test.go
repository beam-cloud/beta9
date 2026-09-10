package repository

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCancelBusyWorkerRolloutRestoresCapacityAndCordon(t *testing.T) {
	for _, cordoned := range []bool{false, true} {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb)
		worker := &types.Worker{Id: "rollback-worker", Status: types.WorkerStatusAvailable,
			TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16000, FreeMemory: 16000,
			Gpu: "RTX5090", TotalGpuCount: 1, FreeGpuCount: 1}
		require.NoError(t, repo.AddWorker(worker))
		seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "hosted", Cpu: 2000, Memory: 4000, Gpu: "RTX5090", GpuCount: 1, Evictable: true})
		ready, err := repo.PrepareWorkerRollout(worker.Id, "B")
		require.NoError(t, err)
		require.False(t, ready)
		if cordoned {
			require.NoError(t, repo.SetWorkerCordon(worker.Id, true))
		}
		// A stale B snapshot cannot cancel a newer target C.
		_, err = repo.PrepareWorkerRollout(worker.Id, "C")
		require.NoError(t, err)
		require.NoError(t, repo.CancelWorkerRollout(worker.Id, "B"))
		state, err := repo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.Equal(t, "C", state.RolloutGeneration)
		require.Equal(t, types.WorkerStatusDisabled, state.Status)
		require.NoError(t, repo.CancelWorkerRollout(worker.Id, "C"))
		state, err = repo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.Empty(t, state.RolloutGeneration)
		require.Equal(t, cordoned, state.CordonRequested)
		if cordoned {
			require.Equal(t, types.WorkerStatusDisabled, state.Status)
		} else {
			require.Equal(t, types.WorkerStatusAvailable, state.Status)
			require.Zero(t, state.FreeGpuCount)
			require.EqualValues(t, 1, state.EvictableGpuCount)
			require.EqualValues(t, 6000, state.FreeCpu)
		}
	}
}

func TestCancelRolloutDoesNotPublishUnreadyWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	require.NoError(t, repo.AddWorker(&types.Worker{Id: "unready", Status: types.WorkerStatusPending}))
	_, err = repo.PrepareWorkerRollout("unready", "B")
	require.NoError(t, err)
	require.NoError(t, repo.CancelWorkerRollout("unready", "B"))
	state, err := repo.GetWorkerById("unready")
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusPending, state.Status)
	require.NoError(t, repo.ToggleWorkerAvailable("unready", "A"))
	state, err = repo.GetWorkerById("unready")
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusAvailable, state.Status)
}
