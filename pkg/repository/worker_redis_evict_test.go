package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

// seedRunningContainer indexes a RUNNING container on a worker and returns its
// state key.
func seedRunningContainer(t *testing.T, rdb *common.RedisClient, workerID string, state *types.ContainerState) string {
	t.Helper()
	state.Status = types.ContainerStatusRunning
	state.WorkerId = workerID
	key := common.RedisKeys.SchedulerContainerState(state.ContainerId)
	assert.NoError(t, rdb.HSet(context.TODO(), key, common.ToSlice(state)).Err())
	assert.NoError(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(workerID), key).Err())
	return key
}

func queuedRequest(t *testing.T, rdb *common.RedisClient, workerID string) *types.ContainerRequest {
	t.Helper()
	payloads, err := rdb.LRange(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(workerID), 0, -1)
	assert.NoError(t, err)
	assert.Len(t, payloads, 1)
	var request types.ContainerRequest
	assert.NoError(t, json.Unmarshal([]byte(payloads[0]), &request))
	return &request
}

func TestScheduleEvictableRequestTracksEvictableCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	worker := &types.Worker{Id: "worker-evictable", Status: types.WorkerStatusAvailable, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 2, FreeCpu: 4000, FreeMemory: 8000, FreeGpuCount: 2}
	assert.NoError(t, repo.AddWorker(worker))

	request := &types.ContainerRequest{ContainerId: "replica-1", Cpu: 1000, Memory: 1000, Gpu: "A10G", GpuCount: 1,
		Evictable: true, OpportunisticOnly: true, DrainSeconds: 30, EvictOrder: 1}
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))

	assert.Equal(t, int64(3000), worker.FreeCpu)
	assert.Equal(t, uint32(1), worker.FreeGpuCount)
	assert.Equal(t, int64(1000), worker.EvictableCpu)
	assert.Equal(t, int64(1250), worker.EvictableMemory)
	assert.Equal(t, uint32(1), worker.EvictableGpuCount)

	stored, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), stored.EvictableCpu)
	assert.Equal(t, int64(1250), stored.EvictableMemory)
	assert.Equal(t, uint32(1), stored.EvictableGpuCount)
	assert.Empty(t, queuedRequest(t, rdb, worker.Id).EvictContainerIds)
}

func TestScheduleEvictsToCoverShortfall(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	// Two GPUs, both held by evictable replicas; nothing free. AddWorker
	// reconciles capacity from the indexed containers.
	worker := &types.Worker{Id: "worker-full", Status: types.WorkerStatusAvailable, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 2}
	// The older decode replica should survive; the newer prefill one goes first
	// on EvictOrder, and among equals the newest is evicted.
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "decode-old", Cpu: 1000, Memory: 1000,
		Gpu: "A10G", GpuCount: 1, Evictable: true, EvictOrder: 2, DrainSeconds: 45, StartedAt: 100})
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "prefill-new", Cpu: 1000, Memory: 1000,
		Gpu: "A10G", GpuCount: 1, Evictable: true, EvictOrder: 0, DrainSeconds: 20, StartedAt: 200})
	assert.NoError(t, repo.AddWorker(worker))
	assert.Equal(t, uint32(0), worker.FreeGpuCount)
	assert.Equal(t, uint32(2), worker.EvictableGpuCount)
	assert.Equal(t, int64(2000), worker.FreeCpu)
	assert.Equal(t, int64(5500), worker.FreeMemory)

	request := &types.ContainerRequest{ContainerId: "serverless-1", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 1}
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))

	// Exactly one victim, the prefill replica, carried on the request.
	assert.Equal(t, []string{"prefill-new"}, request.EvictContainerIds)
	assert.Equal(t, uint32(20), request.EvictDrainSeconds)
	delivered := queuedRequest(t, rdb, worker.Id)
	assert.Equal(t, []string{"prefill-new"}, delivered.EvictContainerIds)
	assert.Equal(t, uint32(20), delivered.EvictDrainSeconds)

	// The victim is marked so the worker and controller both see it.
	victim, err := rdb.HGetAll(context.TODO(), common.RedisKeys.SchedulerContainerState("prefill-new")).Result()
	assert.NoError(t, err)
	assert.Equal(t, string(types.ContainerStatusStopping), victim["status"])
	assert.Equal(t, "true", victim["evicting"])
	assert.Equal(t, string(types.StopContainerReasonEvicted), victim["stop_reason"])
	survivor, err := rdb.HGetAll(context.TODO(), common.RedisKeys.SchedulerContainerState("decode-old")).Result()
	assert.NoError(t, err)
	assert.Equal(t, string(types.ContainerStatusRunning), survivor["status"])
	assert.NotEqual(t, "true", survivor["evicting"])

	// Capacity moved from the victim to the new request: free = 0 + 1 - 1 GPU.
	stored, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), stored.FreeGpuCount)
	assert.Equal(t, int64(2500), stored.FreeCpu)
	assert.Equal(t, int64(5500+1250-625), stored.FreeMemory)
	assert.Equal(t, uint32(1), stored.EvictableGpuCount)
	assert.Equal(t, int64(1000), stored.EvictableCpu)
	assert.Equal(t, int64(1250), stored.EvictableMemory)
	assert.Equal(t, stored.FreeGpuCount, worker.FreeGpuCount)
	assert.Equal(t, stored.EvictableGpuCount, worker.EvictableGpuCount)
}

func TestScheduleDoesNotEvictForOpportunisticOrEvictableRequests(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	worker := &types.Worker{Id: "worker-no-evict", Status: types.WorkerStatusAvailable, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 1}
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "replica-a", Cpu: 1000, Memory: 1000,
		Gpu: "A10G", GpuCount: 1, Evictable: true})
	assert.NoError(t, repo.AddWorker(worker))
	assert.Equal(t, uint32(0), worker.FreeGpuCount)
	assert.Equal(t, uint32(1), worker.EvictableGpuCount)

	for _, request := range []*types.ContainerRequest{
		{ContainerId: "replica-b", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 1, Evictable: true, OpportunisticOnly: true},
		{ContainerId: "protected-b", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 1, OpportunisticOnly: true},
	} {
		setPendingContainerRequests(t, rdb, request)
		err := repo.ScheduleContainerRequest(worker, request)
		assert.Error(t, err, request.ContainerId)
		assert.Empty(t, request.EvictContainerIds)
	}
	state, err := rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState("replica-a"), "status").Result()
	assert.NoError(t, err)
	assert.Equal(t, string(types.ContainerStatusRunning), state)
}

func TestScheduleEvictionFailsWhenVictimsCannotCover(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	worker := &types.Worker{Id: "worker-short", Status: types.WorkerStatusAvailable, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 2}
	// One evictable GPU and one protected GPU: a two-GPU request cannot fit.
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "replica-a", Cpu: 500, Memory: 500,
		Gpu: "A10G", GpuCount: 1, Evictable: true})
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "protected-a", Cpu: 500, Memory: 500,
		Gpu: "A10G", GpuCount: 1})
	assert.NoError(t, repo.AddWorker(worker))
	assert.Equal(t, uint32(0), worker.FreeGpuCount)
	assert.Equal(t, uint32(1), worker.EvictableGpuCount)

	request := &types.ContainerRequest{ContainerId: "big", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 2}
	setPendingContainerRequests(t, rdb, request)
	err = repo.ScheduleContainerRequest(worker, request)
	assert.True(t, errors.Is(err, ErrInsufficientEvictableCapacity), err)
	for _, id := range []string{"replica-a", "protected-a"} {
		status, err := rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(id), "status").Result()
		assert.NoError(t, err)
		assert.Equal(t, string(types.ContainerStatusRunning), status, id)
	}
}

func TestScheduleEvictionRejectsStaleVictims(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	worker := &types.Worker{Id: "worker-stale-victim", Status: types.WorkerStatusAvailable, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 1}
	victimKey := seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "replica-a", Cpu: 1000, Memory: 1000,
		Gpu: "A10G", GpuCount: 1, Evictable: true})
	assert.NoError(t, repo.AddWorker(worker))
	assert.Equal(t, uint32(0), worker.FreeGpuCount)

	request := &types.ContainerRequest{ContainerId: "serverless", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 1}
	setPendingContainerRequests(t, rdb, request)

	// Drive the script directly with a victim whose state flipped underneath
	// the Go selection: the placement must be rejected, not double-booked.
	assert.NoError(t, rdb.HSet(context.TODO(), victimKey, "status", string(types.ContainerStatusStopping)).Err())
	payload, err := json.Marshal(request)
	assert.NoError(t, err)
	result, err := scheduleContainerRequestsScript.Run(context.TODO(), rdb, []string{
		common.RedisKeys.SchedulerWorkerState(worker.Id),
		common.RedisKeys.SchedulerWorkerRequests(worker.Id),
		common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id),
	}, request.Cpu, capacityMemoryForRequest(request), 1, 1, worker.Id,
		schedulerAssignmentIDField, schedulerDeliveryTokenField, schedulerDeliveryAttemptField, "batch-stale",
		0, 0, 0, 1,
		victimKey, 1000, 1250, 1,
		common.RedisKeys.SchedulerContainerState(request.ContainerId), payload, "A10G", "assignment-stale").Result()
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, result)
	assert.True(t, errors.Is(err, ErrEvictionVictimsChanged), err)
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	assert.Equal(t, "0", rdb.HGet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "gpu_count").Val())
}

func TestReconcileCapacityCountsEvictableAndSkipsEvicting(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)

	worker := &types.Worker{Id: "worker-reconcile-evict", Status: types.WorkerStatusPending, Gpu: "A10G",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 3, FreeCpu: 4000, FreeMemory: 8000, FreeGpuCount: 3}
	assert.NoError(t, repo.AddWorker(worker))
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "protected", Cpu: 1000, Memory: 1000, Gpu: "A10G", GpuCount: 1})
	seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "replica", Cpu: 1000, Memory: 1000, Gpu: "A10G", GpuCount: 1, Evictable: true})
	// An evicting victim's capacity already belongs to the request that
	// displaced it; the request is queued and counted separately.
	evicting := seedRunningContainer(t, rdb, worker.Id, &types.ContainerState{ContainerId: "victim", Cpu: 1000, Memory: 1000, Gpu: "A10G", GpuCount: 1, Evictable: true})
	assert.NoError(t, rdb.HSet(context.TODO(), evicting, "status", string(types.ContainerStatusStopping), "evicting", "true").Err())
	replacement, err := json.Marshal(&types.ContainerRequest{ContainerId: "replacement", Cpu: 500, Memory: 500, Gpu: "A10G", GpuCount: 1})
	assert.NoError(t, err)
	assert.NoError(t, rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), replacement).Err())

	assert.NoError(t, repo.ToggleWorkerAvailable(worker.Id, ""))
	stored, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(4000-1000-1000-500), stored.FreeCpu)
	assert.Equal(t, int64(8000-1250-1250-625), stored.FreeMemory)
	assert.Equal(t, uint32(0), stored.FreeGpuCount)
	assert.Equal(t, int64(1000), stored.EvictableCpu)
	assert.Equal(t, int64(1250), stored.EvictableMemory)
	assert.Equal(t, uint32(1), stored.EvictableGpuCount)
}
