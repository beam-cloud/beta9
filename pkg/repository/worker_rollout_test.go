package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
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

func TestWorkerRolloutStopsOnlyIdleServerlessContainers(t *testing.T) {
	for _, kind := range []string{types.StubTypeTaskQueue, types.StubTypeEndpoint, types.StubTypeASGI, types.StubTypeVM, types.StubTypePod, types.StubTypeFunction} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			workerRepo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker", Status: types.WorkerStatusAvailable}
			require.NoError(t, workerRepo.AddWorker(worker))
			container := &types.ContainerState{ContainerId: "container", WorkerId: worker.Id, StubId: "stub", Status: types.ContainerStatusRunning}
			seedRunningContainer(t, rdb, worker.Id, container)
			stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(kind), Config: `{"workers":2,"concurrent_requests":4}`}, Workspace: types.Workspace{Name: "workspace"}}
			_, err = workerRepo.PrepareWorkerRollout(worker.Id, "next")
			require.NoError(t, err)
			endpointKey := common.RedisKeys.EndpointRequestTokens("workspace", "stub", container.ContainerId)
			processingKey := common.RedisKeys.TaskQueueProcessingLock("workspace", "stub", container.ContainerId)
			indexKey := common.RedisKeys.TaskQueueTaskRunningLockIndex("workspace", "stub", container.ContainerId)
			taskKey := common.RedisKeys.TaskQueueTaskRunningLock("workspace", "stub", container.ContainerId, "active")
			require.NoError(t, rdb.Set(ctx, endpointKey, 1, time.Minute).Err())
			require.NoError(t, rdb.Set(ctx, processingKey, 1, time.Minute).Err())
			require.NoError(t, rdb.SAdd(ctx, indexKey, "active").Err())
			require.NoError(t, rdb.Set(ctx, taskKey, 1, time.Minute).Err())
			ready, err := PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
			require.NoError(t, err)
			require.False(t, ready, "active requests must finish before stopping")
			require.NoError(t, rdb.Del(ctx, processingKey).Err())
			ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
			require.NoError(t, err)
			require.False(t, ready, "running tasks and requests still own the container")
			require.NoError(t, rdb.Del(ctx, taskKey, endpointKey).Err())
			ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "obsolete", *container, stub)
			require.NoError(t, err)
			require.False(t, ready, "cancelled rollout cannot stop a container")
			ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
			require.NoError(t, err)
			require.Equal(t, kind == types.StubTypeTaskQueue || kind == types.StubTypeEndpoint || kind == types.StubTypeASGI, ready)
			if ready {
				state, err := NewContainerRedisRepositoryForTest(rdb).GetContainerState(container.ContainerId)
				require.NoError(t, err)
				require.Equal(t, types.ContainerStatusStopping, state.Status)
				ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
				require.NoError(t, err)
				require.True(t, ready, "retry finalization if publishing the stop fails")
			}
		})
	}
}

func TestEndpointWorkerRolloutFinishesInflightRequestsAndFencesAdmission(t *testing.T) {
	ctx := context.Background()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	workers := NewWorkerRedisRepositoryForTest(rdb)
	containers := NewContainerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker", Status: types.WorkerStatusAvailable}
	require.NoError(t, workers.AddWorker(worker))
	container := &types.ContainerState{ContainerId: "container", WorkerId: worker.Id, StubId: "stub", Status: types.ContainerStatusRunning}
	seedRunningContainer(t, rdb, worker.Id, container)
	stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeEndpoint), Config: `{"workers":2}`}, Workspace: types.Workspace{Name: "workspace"}}
	acquired, err := containers.AcquireEndpointRequestToken(ctx, "workspace", "stub", container.ContainerId, 2, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	_, err = workers.PrepareWorkerRollout(worker.Id, "next")
	require.NoError(t, err)
	acquired, err = containers.AcquireEndpointRequestToken(ctx, "workspace", "stub", container.ContainerId, 2, time.Minute)
	require.NoError(t, err)
	require.False(t, acquired, "a stale gateway's replica list cannot admit another request")
	ready, err := PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
	require.NoError(t, err)
	require.False(t, ready)
	require.NoError(t, containers.ReleaseEndpointRequestToken(ctx, "workspace", "stub", container.ContainerId, "active", 2, time.Minute))
	ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, workers.CancelWorkerRollout(worker.Id, "next"))
	acquired, err = containers.AcquireEndpointRequestToken(ctx, "workspace", "stub", container.ContainerId, 2, time.Minute)
	require.NoError(t, err)
	require.False(t, acquired, "STOPPING remains fenced after worker rollout cancellation")
}

func TestWorkerRolloutWaitsForClaimAfterGatewayLeaseExpires(t *testing.T) {
	ctx := context.Background()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	workers := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker", Status: types.WorkerStatusAvailable}
	require.NoError(t, workers.AddWorker(worker))
	container := &types.ContainerState{ContainerId: "container", WorkerId: worker.Id, StubId: "stub", Status: types.ContainerStatusRunning}
	seedRunningContainer(t, rdb, worker.Id, container)
	stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeTaskQueue), Config: `{"workers":1}`}, Workspace: types.Workspace{Name: "workspace"}}
	_, err = workers.PrepareWorkerRollout(worker.Id, "next")
	require.NoError(t, err)
	tasks := NewTaskRedisRepository(rdb)
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", container.ContainerId))
	index := common.RedisKeys.TaskQueueTaskRunningLockIndex("workspace", "stub", container.ContainerId)
	require.NoError(t, rdb.SAdd(ctx, index, "task").Err())
	// The ephemeral monitor lease is absent after a gateway restart.
	ready, err := PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
	require.NoError(t, err)
	require.False(t, ready)
	require.True(t, rdb.SIsMember(ctx, index, "task").Val())
	// A claim owned by a different container must not block draining this one.
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "replacement"))
	ready, err = PrepareIdleWorkerRolloutContainer(ctx, rdb, worker.Id, "next", *container, stub)
	require.NoError(t, err)
	require.True(t, ready)
}
