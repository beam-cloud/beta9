package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/tj/assert"
)

type lostRedisScriptReplyHook struct {
	hash  string
	lost  atomic.Bool
	after func()
}

func (h *lostRedisScriptReplyHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *lostRedisScriptReplyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "evalsha" && len(cmd.Args()) > 1 && cmd.Args()[1] == h.hash && h.lost.CompareAndSwap(false, true) {
			if h.after != nil {
				h.after()
			}
			return errors.New("simulated lost Redis reply")
		}
		return err
	}
}

func (h *lostRedisScriptReplyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func setPendingContainerRequests(t *testing.T, rdb *common.RedisClient, requests ...*types.ContainerRequest) {
	t.Helper()
	for _, request := range requests {
		assert.NoError(t, rdb.HSet(
			context.TODO(),
			common.RedisKeys.SchedulerContainerState(request.ContainerId),
			"container_id", request.ContainerId,
			"status", string(types.ContainerStatusPending),
			"worker_id", "",
			"machine_id", "",
		).Err())
	}
}

func TestNewWorkerRedisRepository(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.NotNil(t, repo)
}

func TestAddAndRemoveWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	err = repo.RemoveWorker(newWorker.Id)
	assert.Nil(t, err)

	err = repo.RemoveWorker(newWorker.Id)
	assert.Error(t, err)

	var notFound *types.ErrWorkerNotFound
	assert.True(t, errors.As(err, &notFound))
}

func TestRemoveWorkerRequeuesPendingWorkerRequests(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-with-queued-requests",
		Status:      types.WorkerStatusAvailable,
		PoolName:    "default",
		FreeCpu:     10_000,
		FreeMemory:  10_000,
		TotalCpu:    10_000,
		TotalMemory: 10_000,
	}
	assert.Nil(t, repo.AddWorker(worker))

	requests := []*types.ContainerRequest{
		{
			ContainerId: "container-1", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 2,
			Env: []string{}, Ports: []uint32{}, Checkpoint: &types.Checkpoint{CacheSizeBytes: 9_007_199_254_740_993},
		},
		{ContainerId: "container-2", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 4},
	}
	setPendingContainerRequests(t, rdb, requests...)
	for _, request := range requests {
		currentWorker, err := repo.GetWorkerById(worker.Id)
		assert.Nil(t, err)
		assert.Nil(t, repo.ScheduleContainerRequest(currentWorker, request))
	}
	staleRequest, err := json.Marshal(&types.ContainerRequest{ContainerId: "expired-container"})
	assert.NoError(t, err)
	staleDelivery, err := json.Marshal(map[string]string{"request": string(staleRequest), "delivery_token": "expired-assignment:1"})
	assert.NoError(t, err)
	assert.NoError(t, rdb.HSet(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id), "expired-assignment", staleDelivery).Err())

	assert.Nil(t, repo.RemoveWorker(worker.Id))

	_, err = repo.GetWorkerById(worker.Id)
	_, workerNotFound := err.(*types.ErrWorkerNotFound)
	assert.True(t, workerNotFound)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)

	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(backlog))

	retriesByContainer := map[string]int{}
	requeuedRequests := make([]*types.ContainerRequest, 0, len(backlog))
	for _, raw := range backlog {
		var request types.ContainerRequest
		assert.Nil(t, json.Unmarshal([]byte(raw), &request))
		retriesByContainer[request.ContainerId] = request.RetryCount
		requeuedRequests = append(requeuedRequests, &request)
	}
	assert.Equal(t, 3, retriesByContainer["container-1"])
	assert.Equal(t, 5, retriesByContainer["container-2"])
	for _, request := range requeuedRequests {
		if request.ContainerId == "container-1" {
			assert.Equal(t, []string{}, request.Env)
			assert.Equal(t, []uint32{}, request.Ports)
			assert.NotNil(t, request.Checkpoint)
			assert.Equal(t, int64(9_007_199_254_740_993), request.Checkpoint.CacheSizeBytes)
		}
	}

	replacement := &types.Worker{
		Id:          "replacement-worker",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    10_000,
		TotalMemory: 10_000,
		FreeCpu:     10_000,
		FreeMemory:  10_000,
	}
	assert.NoError(t, repo.AddWorker(replacement))
	for _, request := range requeuedRequests {
		assert.NoError(t, repo.ScheduleContainerRequest(replacement, request))
		assert.Equal(t, replacement.Id, rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "worker_id").Val())
	}
}

func TestGetWorkerById(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
}

func TestToggleWorkerAvailable(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	// Create a pending worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	// Set it to be available
	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	// Retrieve it again and check fields
	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, types.WorkerStatusAvailable, worker.Status)
}

func TestToggleWorkerAvailablePreservesDisabledWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "disabled-worker", Status: types.WorkerStatusDisabled}
	assert.Nil(t, repo.AddWorker(worker))
	assert.Nil(t, repo.ToggleWorkerAvailable(worker.Id, ""))

	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, worker.Status)
}

func TestToggleWorkerAvailableReconcilesCapacityFromQueueAndContainerIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id:            "worker-reconcile-capacity",
		Status:        types.WorkerStatusPending,
		TotalCpu:      4000,
		TotalMemory:   1024,
		TotalGpuCount: 2,
		FreeCpu:       0,
		FreeMemory:    0,
		FreeGpuCount:  0,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	queuedRequest := &types.ContainerRequest{
		ContainerId: "container-reconcile-queued",
		Cpu:         1000,
		Memory:      100,
	}
	queuedJSON, err := json.Marshal(queuedRequest)
	assert.Nil(t, err)
	err = rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), queuedJSON).Err()
	assert.Nil(t, err)
	queuedState := &types.ContainerState{
		ContainerId: queuedRequest.ContainerId,
		Status:      types.ContainerStatusPending,
		WorkerId:    worker.Id,
		Cpu:         queuedRequest.Cpu,
		Memory:      queuedRequest.Memory,
	}
	queuedStateKey := common.RedisKeys.SchedulerContainerState(queuedState.ContainerId)
	err = rdb.HSet(context.TODO(), queuedStateKey, common.ToSlice(queuedState)).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), queuedStateKey).Err()
	assert.Nil(t, err)

	runningState := &types.ContainerState{
		ContainerId: "container-reconcile-running",
		Status:      types.ContainerStatusRunning,
		WorkerId:    worker.Id,
		Cpu:         1000,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	runningStateKey := common.RedisKeys.SchedulerContainerState(runningState.ContainerId)
	err = rdb.HSet(context.TODO(), runningStateKey, common.ToSlice(runningState)).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), runningStateKey).Err()
	assert.Nil(t, err)
	staleStateKey := common.RedisKeys.SchedulerContainerState("container-reconcile-missing")
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), staleStateKey).Err()
	assert.Nil(t, err)
	emptyOwnerStateKey := common.RedisKeys.SchedulerContainerState("container-reconcile-empty-owner")
	assert.NoError(t, rdb.HSet(context.TODO(), emptyOwnerStateKey, "status", string(types.ContainerStatusRunning), "worker_id", "", "cpu", 1000).Err())
	assert.NoError(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), emptyOwnerStateKey).Err())

	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
	assert.Equal(t, int64(2000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(774), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(1), updatedWorker.FreeGpuCount)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	staleIndexExists, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), staleStateKey).Result()
	assert.Nil(t, err)
	assert.False(t, staleIndexExists)
	assert.False(t, rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), emptyOwnerStateKey).Val())
}

func TestToggleWorkerAvailableCapsReconciledCapacityAtZero(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id:            "worker-reconcile-over-reserved",
		Status:        types.WorkerStatusPending,
		TotalCpu:      1000,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       1000,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	queuedRequest := &types.ContainerRequest{
		ContainerId: "container-reconcile-over-reserved",
		Cpu:         2000,
		Memory:      200,
		Gpu:         "A10G",
		GpuCount:    2,
	}
	queuedJSON, err := json.Marshal(queuedRequest)
	assert.Nil(t, err)
	err = rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), queuedJSON).Err()
	assert.Nil(t, err)

	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
}

func TestStoppingContainerRemainsReservedUntilStateDeletion(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-stopping-capacity",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      100,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       100,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	assert.NoError(t, repo.AddWorker(worker))
	request := &types.ContainerRequest{
		ContainerId: "container-stopping-capacity",
		Cpu:         100,
		Memory:      100,
		Gpu:         worker.Gpu,
		GpuCount:    1,
	}
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	assert.NoError(t, rdb.HSet(context.TODO(), stateKey, common.ToSlice(&types.ContainerState{
		ContainerId: request.ContainerId,
		Status:      types.ContainerStatusStopping,
		WorkerId:    worker.Id,
		Cpu:         request.Cpu,
		Memory:      request.Memory,
		Gpu:         request.Gpu,
		GpuCount:    request.GpuCount,
	})).Err())
	assert.NoError(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())

	heartbeat := *worker
	assert.NoError(t, repo.AddWorker(&heartbeat))
	assert.Equal(t, int64(0), heartbeat.FreeCpu)
	assert.Equal(t, int64(0), heartbeat.FreeMemory)
	assert.Equal(t, uint32(0), heartbeat.FreeGpuCount)
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, uint32(0), heartbeat.FreeGpuCount)

	assert.NoError(t, rdb.Del(context.TODO(), stateKey).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, int64(100), heartbeat.FreeCpu)
	assert.Equal(t, int64(125), heartbeat.FreeMemory)
	assert.Equal(t, uint32(1), heartbeat.FreeGpuCount)
	releasedVersion := heartbeat.ResourceVersion
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, releasedVersion, heartbeat.ResourceVersion)
}

func TestStaleWorkerHeartbeatCannotRestoreScheduledCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-heartbeat-schedule-race",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      100,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       100,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	assert.NoError(t, repo.AddWorker(worker))
	request := &types.ContainerRequest{ContainerId: "container-heartbeat-schedule-race", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	setPendingContainerRequests(t, rdb, request)

	heartbeat := *worker
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, repo.AddWorker(&heartbeat))

	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updated.FreeCpu)
	assert.Equal(t, int64(0), updated.FreeMemory)
	assert.Equal(t, uint32(0), updated.FreeGpuCount)
	assert.Equal(t, int64(1), updated.ResourceVersion)
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestUpdateWorkerCapacityReconcilesCompletedGPURequestExactlyOnce(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, err)

	worker := &types.Worker{
		Id:            "worker-capacity-release",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      500,
		TotalMemory:   1250,
		TotalGpuCount: 1,
		FreeCpu:       500,
		FreeMemory:    1250,
		Gpu:           "A10G",
		FreeGpuCount:  1,
	}
	request := &types.ContainerRequest{
		ContainerId: "container-capacity-release",
		Cpu:         500,
		Memory:      1000,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Err())
	assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Err())

	assert.NoError(t, repo.UpdateWorkerCapacity(worker, request, types.AddCapacity))
	version := worker.ResourceVersion
	assert.Equal(t, int64(500), worker.FreeCpu)
	assert.Equal(t, int64(1250), worker.FreeMemory)
	assert.Equal(t, uint32(1), worker.FreeGpuCount)
	assert.NoError(t, repo.UpdateWorkerCapacity(worker, request, types.AddCapacity))
	assert.Equal(t, version, worker.ResourceVersion)
}

func TestUpdateWorkerCapacityForCPUWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	// Create a new worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	// Retrieve the worker
	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
	assert.Equal(t, int64(0), newWorker.ResourceVersion)

	// Remove some capacity from the worker
	firstRequest := &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         500,
		Memory:      100,
		Gpu:         "",
	}
	err = repo.UpdateWorkerCapacity(newWorker, firstRequest, types.RemoveCapacity)
	assert.Nil(t, err)

	// Retrieve the updated worker
	updatedWorker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	freeMemoryAfterFirstRequest := worker.FreeMemory - capacityMemoryForRequest(firstRequest)
	assert.Equal(t, worker.FreeCpu-firstRequest.Cpu, updatedWorker.FreeCpu)
	assert.Equal(t, freeMemoryAfterFirstRequest, updatedWorker.FreeMemory)
	assert.Equal(t, firstRequest.Gpu, updatedWorker.Gpu)
	assert.Equal(t, worker.Status, updatedWorker.Status)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	// Remove some more capacity
	secondRequest := &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         100,
		Memory:      200,
		Gpu:         "",
	}
	err = repo.UpdateWorkerCapacity(updatedWorker, secondRequest, types.RemoveCapacity)
	assert.Nil(t, err)

	updatedWorker, err = repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)

	thirdRequest := &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         100,
		Memory:      100,
		Gpu:         "",
	}
	err = repo.UpdateWorkerCapacity(updatedWorker, thirdRequest, types.RemoveCapacity)
	assert.Nil(t, err)

	// Retrieve the worker again
	updatedWorker, err = repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	freeMemoryAfterThirdRequest := freeMemoryAfterFirstRequest -
		capacityMemoryForRequest(secondRequest) -
		capacityMemoryForRequest(thirdRequest)

	assert.Equal(t, worker.FreeCpu-firstRequest.Cpu-secondRequest.Cpu-thirdRequest.Cpu, updatedWorker.FreeCpu)
	assert.Equal(t, freeMemoryAfterThirdRequest, updatedWorker.FreeMemory)
	assert.Equal(t, worker.Gpu, updatedWorker.Gpu)
	assert.Equal(t, int64(3), updatedWorker.ResourceVersion)
}

func TestCapacityMemoryForRequest(t *testing.T) {
	assert.Equal(t, int64(0), capacityMemoryForRequest(&types.ContainerRequest{}))
	assert.Equal(t, int64(-1), capacityMemoryForRequest(&types.ContainerRequest{Memory: -1}))
	assert.Equal(t, int64(125), capacityMemoryForRequest(&types.ContainerRequest{Memory: 100}))
	assert.Equal(t, int64(2), capacityMemoryForRequest(&types.ContainerRequest{Memory: 1}))
}

func TestUpdateWorkerCapacityAddDoesNotExceedTotalCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-capacity-cap",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      1000,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       900,
		FreeMemory:    100,
		FreeGpuCount:  0,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-capacity-cap",
		Cpu:         500,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    2,
	}
	err = repo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(125), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(1), updatedWorker.FreeGpuCount)
}

func TestGetAllWorkers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	// Create a bunch of available workers
	nWorkers := 100
	for i := 0; i < nWorkers; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:           fmt.Sprintf("worker-available-%d", i),
			Status:       types.WorkerStatusAvailable,
			FreeCpu:      1000,
			FreeMemory:   1000,
			Gpu:          "A10G",
			FreeGpuCount: 1,
		})
		assert.Nil(t, err)
	}

	// Create a bunch of pending workers
	for i := 0; i < nWorkers; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:           fmt.Sprintf("worker-pending-%d", i),
			Status:       types.WorkerStatusPending,
			FreeCpu:      1000,
			FreeMemory:   1000,
			Gpu:          "A10G",
			FreeGpuCount: 1,
		})
		assert.Nil(t, err)
	}

	workers, err := repo.GetAllWorkers()
	assert.Nil(t, err)

	// Ensure we got back the correct total number of workers
	assert.Equal(t, nWorkers*2, len(workers))

	// Ensure we got back the correct number of each status type
	availableCount := 0
	pendingCount := 0
	for _, worker := range workers {
		switch worker.Status {
		case types.WorkerStatusAvailable:
			availableCount++
		case types.WorkerStatusPending:
			pendingCount++
		}
	}
	assert.Equal(t, nWorkers, availableCount)
	assert.Equal(t, nWorkers, pendingCount)
}

func TestGetAllWorkersInPoolUsesPoolIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-pool-a", PoolName: "pool-a", MachineId: "machine-a"}))
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-pool-b", PoolName: "pool-b", MachineId: "machine-a"}))

	workers, err := repo.GetAllWorkersInPool("pool-a")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))
	assert.Equal(t, "worker-pool-a", workers[0].Id)
}

func TestGetAllWorkersOnMachineUsesMachineIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-machine-a", PoolName: "pool-a", MachineId: "machine-a"}))
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-machine-b", PoolName: "pool-a", MachineId: "machine-b"}))

	workers, err := repo.GetAllWorkersOnMachine("machine-a")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))
	assert.Equal(t, "worker-machine-a", workers[0].Id)
}

func TestSetWorkerKeepAliveUpdatesMachineIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-moving", Status: types.WorkerStatusAvailable, PoolName: "pool-a", MachineId: "machine-a"}
	assert.Nil(t, repo.AddWorker(worker))

	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{MachineId: "machine-b"}))

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, "machine-b", updatedWorker.MachineId)

	oldWorkers, err := repo.GetAllWorkersOnMachine("machine-a")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(oldWorkers))

	newWorkers, err := repo.GetAllWorkersOnMachine("machine-b")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(newWorkers))
	assert.Equal(t, worker.Id, newWorkers[0].Id)
}

func TestWorkerSecondaryIndexesRemoveStaleMembers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	staleKey := common.RedisKeys.SchedulerWorkerState("worker-stale")
	poolIndexKey := common.RedisKeys.SchedulerWorkerPoolIndex("pool-a")

	assert.Nil(t, rdb.SAdd(context.TODO(), poolIndexKey, staleKey).Err())

	workers, err := repo.GetAllWorkersInPool("pool-a")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(workers))

	members, err := rdb.SMembers(context.TODO(), poolIndexKey).Result()
	assert.Nil(t, err)
	assert.NotContains(t, members, staleKey)
}

func TestWorkerSecondaryIndexesIgnoreWrongPoolMembers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-wrong-pool", PoolName: "pool-a"}
	assert.Nil(t, repo.AddWorker(worker))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	wrongPoolIndex := common.RedisKeys.SchedulerWorkerPoolIndex("pool-b")
	assert.Nil(t, rdb.SAdd(context.TODO(), wrongPoolIndex, stateKey).Err())

	workers, err := repo.GetAllWorkersInPool("pool-b")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(workers))

	inWrongPool, err := rdb.SIsMember(context.TODO(), wrongPoolIndex, stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inWrongPool)
}

func TestWorkerSecondaryIndexesIgnoreWrongMachineMembers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-wrong-machine", MachineId: "machine-a"}
	assert.Nil(t, repo.AddWorker(worker))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	wrongMachineIndex := common.RedisKeys.SchedulerWorkerMachineIndex("machine-b")
	assert.Nil(t, rdb.SAdd(context.TODO(), wrongMachineIndex, stateKey).Err())

	workers, err := repo.GetAllWorkersOnMachine("machine-b")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(workers))

	inWrongMachine, err := rdb.SIsMember(context.TODO(), wrongMachineIndex, stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inWrongMachine)
}

func TestWorkerSecondaryIndexesUpdateAndRemove(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-moving", PoolName: "pool-a", MachineId: "machine-a"}
	assert.Nil(t, repo.AddWorker(worker))

	worker.PoolName = "pool-b"
	worker.MachineId = "machine-b"
	assert.Nil(t, repo.AddWorker(worker))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	inOldPool, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerPoolIndex("pool-a"), stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inOldPool)
	inNewPool, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerPoolIndex("pool-b"), stateKey).Result()
	assert.Nil(t, err)
	assert.True(t, inNewPool)
	inOldMachine, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerMachineIndex("machine-a"), stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inOldMachine)
	inNewMachine, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerMachineIndex("machine-b"), stateKey).Result()
	assert.Nil(t, err)
	assert.True(t, inNewMachine)

	assert.Nil(t, repo.RemoveWorker(worker.Id))
	for _, indexKey := range []string{
		common.RedisKeys.SchedulerWorkerIndex(),
		common.RedisKeys.SchedulerWorkerPoolIndex("pool-b"),
		common.RedisKeys.SchedulerWorkerMachineIndex("machine-b"),
	} {
		exists, err := rdb.SIsMember(context.TODO(), indexKey, stateKey).Result()
		assert.Nil(t, err)
		assert.False(t, exists)
	}
}

func TestScheduleContainerRequestRestoresCapacityWhenQueuePushFails(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-queue-error",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	err = rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), "wrong-type", 0).Err()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-queue-error",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, request)

	err = repo.ScheduleContainerRequest(worker, request)
	assert.Error(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)

	containerStateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	stateFields, err := rdb.HGetAll(context.TODO(), containerStateKey).Result()
	assert.Nil(t, err)
	assert.Equal(t, "", stateFields["worker_id"])
	assert.Equal(t, "", stateFields["machine_id"])
	indexed, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), containerStateKey).Result()
	assert.Nil(t, err)
	assert.False(t, indexed)
}

func TestScheduleContainerRequestRemovesQueuedRequestWhenMetadataWriteFails(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-metadata-error",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-metadata-error",
		Cpu:         100,
		Memory:      100,
	}
	err = rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "wrong-type", 0).Err()
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(worker, request)
	assert.Error(t, err)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)
	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)
	indexed, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Result()
	assert.Nil(t, err)
	assert.False(t, indexed)
}

func TestScheduleContainerRequestRejectsDisabledWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-disabled-schedule",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	selectedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	err = repo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-disabled-schedule",
		Cpu:         100,
		Memory:      100,
	}
	err = repo.ScheduleContainerRequest(selectedWorker, request)
	assert.Error(t, err)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, updatedWorker.Status)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)
}

func TestScheduleContainerRequestRejectsNegativeResources(t *testing.T) {
	for name, request := range map[string]*types.ContainerRequest{
		"cpu":    {ContainerId: "container-negative-cpu", Cpu: -1, Memory: 100},
		"memory": {ContainerId: "container-negative-memory", Cpu: 100, Memory: -1},
	} {
		t.Run(name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker-negative-" + name, Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
			assert.NoError(t, repo.AddWorker(worker))
			setPendingContainerRequests(t, rdb, request)

			assert.Error(t, repo.ScheduleContainerRequest(worker, request))
			updated, err := repo.GetWorkerById(worker.Id)
			assert.NoError(t, err)
			assert.Equal(t, int64(100), updated.FreeCpu)
			assert.Equal(t, int64(125), updated.FreeMemory)
			assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
		})
	}
}

func TestScheduleContainerRequestRejectsNonPendingState(t *testing.T) {
	for _, status := range []types.ContainerStatus{types.ContainerStatusStopping, ""} {
		t.Run(firstNonEmpty(string(status), "deleted"), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{
				Id:         "worker-cancelled-before-commit",
				Status:     types.WorkerStatusAvailable,
				FreeCpu:    100,
				FreeMemory: 125,
			}
			request := &types.ContainerRequest{
				ContainerId: "container-cancelled-before-commit",
				Cpu:         100,
				Memory:      100,
			}
			assert.NoError(t, repo.AddWorker(worker))
			if status != "" {
				assert.NoError(t, rdb.HSet(
					context.TODO(),
					common.RedisKeys.SchedulerContainerState(request.ContainerId),
					"status", string(status),
				).Err())
			}

			assert.Error(t, repo.ScheduleContainerRequest(worker, request))
			updated, err := repo.GetWorkerById(worker.Id)
			assert.NoError(t, err)
			assert.Equal(t, int64(100), updated.FreeCpu)
			assert.Equal(t, int64(125), updated.FreeMemory)
			assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
			assert.Equal(t, int64(0), rdb.SCard(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)).Val())
		})
	}
}

func TestScheduleContainerRequestRejectsStaleWorkerReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-stale-reservation",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	firstWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	secondWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-first",
		Cpu:         100,
		Memory:      100,
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-second",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, firstRequest, secondRequest)

	err = repo.ScheduleContainerRequest(firstWorkerCopy, firstRequest)
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(secondWorkerCopy, secondRequest)
	assert.Error(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), queueDepth)
}

func TestScheduleContainerRequestSharesWorkerMaintenanceLock(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-live-scheduling",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	assert.NoError(t, repo.AddWorker(worker))

	maintenance := common.NewRedisLock(rdb)
	assert.NoError(t, maintenance.Acquire(context.Background(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10}))
	request := &types.ContainerRequest{
		ContainerId: "container-live-scheduling",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, request)
	done := make(chan error, 1)
	go func() { done <- repo.ScheduleContainerRequest(worker, request) }()
	select {
	case err := <-done:
		t.Fatalf("schedule completed before worker lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assert.NoError(t, maintenance.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id)))
	assert.NoError(t, <-done)
}

func TestScheduleContainerRequestUsesCurrentCapacityForStaleWorkerReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-stale-reservation-current-capacity",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    200,
		FreeMemory: 250,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	firstWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	secondWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-current-capacity-first",
		Cpu:         100,
		Memory:      100,
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-current-capacity-second",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, firstRequest, secondRequest)

	err = repo.ScheduleContainerRequest(firstWorkerCopy, firstRequest)
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(secondWorkerCopy, secondRequest)
	assert.Nil(t, err)

	queuedFirstRequest, err := repo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedFirstRequest)
	assert.Equal(t, firstRequest.ContainerId, queuedFirstRequest.ContainerId)

	queuedSecondRequest, err := repo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedSecondRequest)
	assert.Equal(t, secondRequest.ContainerId, queuedSecondRequest.ContainerId)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
}

func TestScheduleContainerRequestsReservesAndQueuesBatch(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-batch",
		MachineId:     "machine-batch",
		Gpu:           "A10G",
		Status:        types.WorkerStatusAvailable,
		FreeCpu:       300,
		FreeMemory:    375,
		FreeGpuCount:  3,
		TotalGpuCount: 3,
	}
	assert.NoError(t, repo.AddWorker(worker))
	messages, _ := rdb.Subscribe(context.Background(), common.RedisKeys.SchedulerWorkerRequestChannel())

	requests := []*types.ContainerRequest{
		{ContainerId: "container-batch-1", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
		{ContainerId: "container-batch-2", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
		{ContainerId: "container-batch-3", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
	}
	setPendingContainerRequests(t, rdb, requests...)
	assert.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	select {
	case message := <-messages:
		assert.Equal(t, worker.Id, message.Payload)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker request notification")
	}

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(len(requests)), updatedWorker.ResourceVersion)

	actual, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.Len(t, actual, len(requests))
	for i, expected := range requests {
		assert.Equal(t, expected.ContainerId, actual[i].ContainerId)
		assert.Equal(t, worker.MachineId, expected.MachineId)
		assert.Equal(t, worker.MachineId, actual[i].MachineId)

		state, err := rdb.HGetAll(context.TODO(), common.RedisKeys.SchedulerContainerState(expected.ContainerId)).Result()
		assert.NoError(t, err)
		assert.Equal(t, worker.Id, state["worker_id"])
		assert.Equal(t, worker.MachineId, state["machine_id"])
		assert.Equal(t, worker.Gpu, state["gpu"])
		assert.NotEmpty(t, state[schedulerAssignmentIDField])
	}
}

func TestContainerRequestRemainsRecoverableUntilWorkerAcknowledgesIt(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-pending-delivery",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     100,
		FreeMemory:  125,
		TotalCpu:    100,
		TotalMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-pending-delivery", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))

	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.Len(t, delivered, 1)
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	assert.Equal(t, int64(1), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())

	assert.NoError(t, repo.RemoveWorker(worker.Id))
	assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
	assert.Equal(t, int64(1), rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	assert.Empty(t, rdb.HGet(context.TODO(), stateKey, "worker_id").Val())
	assert.Empty(t, rdb.HGet(context.TODO(), stateKey, schedulerAssignmentIDField).Val())
	assert.False(t, rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Val())
	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, 0).Result()
	assert.NoError(t, err)
	assert.Len(t, backlog, 1)
	var requeued types.ContainerRequest
	assert.NoError(t, json.Unmarshal([]byte(backlog[0]), &requeued))
	replacement := &types.Worker{Id: "worker-pending-delivery-replacement", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	assert.NoError(t, repo.AddWorker(replacement))
	assert.NoError(t, repo.ScheduleContainerRequest(replacement, &requeued))
}

func TestAcknowledgedContainerRequestIsNotReplayedWhenWorkerIsRemoved(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-acknowledged-delivery",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     100,
		FreeMemory:  125,
		TotalCpu:    100,
		TotalMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-acknowledged-delivery", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)

	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
	assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
	assert.NoError(t, repo.RemoveWorker(worker.Id))
	assert.Equal(t, int64(0), rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
}

func TestContainerRequestAcknowledgementRejectsCancelledContainer(t *testing.T) {
	for _, status := range []string{string(types.ContainerStatusStopping), ""} {
		status := status
		t.Run(firstNonEmpty(status, "deleted"), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NotNil(t, rdb)
			assert.NoError(t, err)

			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{
				Id:          "worker-cancelled-delivery",
				Status:      types.WorkerStatusAvailable,
				FreeCpu:     100,
				FreeMemory:  125,
				TotalCpu:    100,
				TotalMemory: 125,
			}
			request := &types.ContainerRequest{ContainerId: "container-cancelled-delivery", Cpu: 100, Memory: 100}
			assert.NoError(t, repo.AddWorker(worker))
			setPendingContainerRequests(t, rdb, request)
			assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
			delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
			assert.NoError(t, err)
			assert.Len(t, delivered, 1)

			stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
			if status == "" {
				assert.NoError(t, rdb.Del(context.TODO(), stateKey).Err())
			} else {
				assert.NoError(t, rdb.HSet(context.TODO(), stateKey, "status", status).Err())
			}

			assert.Error(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
			assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
			assert.Equal(t, int64(0), rdb.SCard(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)).Val())
		})
	}
}

func TestRecoveredContainerRequestRejectsStaleAcknowledgement(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-reconnect", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-reconnect", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	first, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)

	assert.NoError(t, repo.RecoverPendingContainerRequests(worker.Id))
	second, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.NotEqual(t, first[0].DeliveryToken, second[0].DeliveryToken)
	assert.Error(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, first[0].DeliveryToken))
	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, second[0].DeliveryToken))
}

func TestSendFailureDoesNotRequeueAcknowledgedDelivery(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-ack-race", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-ack-race", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
	assert.NoError(t, repo.RequeueContainerRequests(worker.Id, delivered))
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestRequeueContainerRequestsPreservesOrder(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-requeue", Status: types.WorkerStatusAvailable, FreeCpu: 300, FreeMemory: 375}
	assert.NoError(t, repo.AddWorker(worker))
	requests := []*types.ContainerRequest{
		{ContainerId: "container-requeue-1", Cpu: 100, Memory: 100},
		{ContainerId: "container-requeue-2", Cpu: 100, Memory: 100},
		{ContainerId: "container-requeue-3", Cpu: 100, Memory: 100},
	}
	setPendingContainerRequests(t, rdb, requests...)
	assert.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	delivered, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.NoError(t, repo.RequeueContainerRequests(worker.Id, delivered))

	actual, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.Len(t, actual, len(requests))
	for i := range requests {
		assert.Equal(t, requests[i].ContainerId, actual[i].ContainerId)
	}
}

func TestScheduleContainerRequestsRejectsBatchWithoutPartialReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-batch-capacity",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	assert.NoError(t, repo.AddWorker(worker))
	duplicate := &types.ContainerRequest{ContainerId: "container-batch-duplicate", Cpu: 1, Memory: 1}
	assert.Error(t, repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{duplicate, duplicate}))
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())

	err = repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{
		{ContainerId: "container-batch-capacity-1", Cpu: 100, Memory: 100},
		{ContainerId: "container-batch-capacity-2", Cpu: 100, Memory: 100},
	})
	assert.Error(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), updatedWorker.FreeCpu)
	assert.Equal(t, int64(125), updatedWorker.FreeMemory)
	assert.Equal(t, int64(0), updatedWorker.ResourceVersion)
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestScheduleContainerRequestsValidatesCapacityFieldsBeforeWriting(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-invalid-resource-version", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-invalid-resource-version", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, rdb.HSet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "resource_version", "invalid").Err())

	payload, err := json.Marshal(request)
	assert.NoError(t, err)
	result, err := scheduleContainerRequestsScript.Run(context.TODO(), rdb, []string{
		common.RedisKeys.SchedulerWorkerState(worker.Id),
		common.RedisKeys.SchedulerWorkerRequests(worker.Id),
		common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id),
	}, request.Cpu, capacityMemoryForRequest(request), 0, 1, worker.Id,
		schedulerAssignmentIDField, schedulerDeliveryTokenField, schedulerDeliveryAttemptField, "batch-invalid-resource-version",
		common.RedisKeys.SchedulerContainerState(request.ContainerId), payload, "", "assignment-invalid-resource-version").Result()
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, result)
	assert.Error(t, err)
	assert.Equal(t, "100", rdb.HGet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "free_cpu").Val())
	assert.Equal(t, "125", rdb.HGet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "free_memory").Val())
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	assert.Empty(t, rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "worker_id").Val())
}

func TestScheduleBatchMarkerSurvivesImmediateContainerFinalization(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
	worker := &types.Worker{Id: "worker-immediate-finalize", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-immediate-finalize", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)

	payload, err := json.Marshal(request)
	assert.NoError(t, err)
	batchID := "batch-immediate-finalize"
	assignment := "assignment-immediate-finalize"
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	result, err := scheduleContainerRequestsScript.Run(context.TODO(), rdb, []string{
		common.RedisKeys.SchedulerWorkerState(worker.Id),
		common.RedisKeys.SchedulerWorkerRequests(worker.Id),
		common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id),
	}, request.Cpu, capacityMemoryForRequest(request), 0, 1, worker.Id,
		schedulerAssignmentIDField, schedulerDeliveryTokenField, schedulerDeliveryAttemptField, batchID,
		stateKey, payload, "", assignment).Result()
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, result)
	assert.NoError(t, err)
	retried, err := scheduleContainerRequestsScript.Run(context.TODO(), rdb, []string{
		common.RedisKeys.SchedulerWorkerState(worker.Id),
		common.RedisKeys.SchedulerWorkerRequests(worker.Id),
		common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id),
	}, request.Cpu, capacityMemoryForRequest(request), 0, 1, worker.Id,
		schedulerAssignmentIDField, schedulerDeliveryTokenField, schedulerDeliveryAttemptField, batchID,
		stateKey, payload, "", assignment).Result()
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, retried)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())

	assert.NoError(t, rdb.Del(context.TODO(), stateKey, common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	committed, err := repo.scheduledBatchCommitted(context.TODO(), worker.Id, batchID, []queuedContainerRequest{{
		request: request, payload: payload, stateKey: stateKey, assignment: assignment,
	}})
	assert.NoError(t, err)
	assert.True(t, committed)
}

func TestScheduleContainerRequestTreatsLostCommitReplyAsSuccess(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-lost-schedule-reply", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-lost-schedule-reply", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	_, err = scheduleContainerRequestsScript.Load(context.TODO(), rdb).Result()
	assert.NoError(t, err)
	hook := &lostRedisScriptReplyHook{hash: scheduleContainerRequestsScript.Hash()}
	rdb.AddHook(hook)

	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.True(t, hook.lost.Load())
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updated.FreeCpu)
	assert.Equal(t, int64(0), updated.FreeMemory)
	assert.Error(t, repo.ScheduleContainerRequest(updated, request))
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestScheduleContainerRequestReconcilesWhenLostReplyHasNoCommitEvidence(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id: "worker-lost-schedule-evidence", Status: types.WorkerStatusAvailable,
		TotalCpu: 100, TotalMemory: 125, FreeCpu: 100, FreeMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-lost-schedule-evidence", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	_, err = scheduleContainerRequestsScript.Load(context.TODO(), rdb).Result()
	assert.NoError(t, err)
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	hook := &lostRedisScriptReplyHook{hash: scheduleContainerRequestsScript.Hash()}
	hook.after = func() {
		assert.NoError(t, rdb.HDel(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), schedulerLastBatchIDField).Err())
		assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), stateKey).Err())
		assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	}
	rdb.AddHook(hook)

	assert.Error(t, repo.ScheduleContainerRequest(worker, request))
	assert.True(t, hook.lost.Load())
	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), updated.FreeCpu)
	assert.Equal(t, int64(125), updated.FreeMemory)
}

func TestDelayedCapacityReleaseCannotOvercreditRecreatedWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id: "worker-recreated", Status: types.WorkerStatusAvailable,
		TotalCpu: 100, TotalMemory: 125, TotalGpuCount: 1,
		FreeCpu: 100, FreeMemory: 125, FreeGpuCount: 1, Gpu: "A10G",
	}
	request := &types.ContainerRequest{ContainerId: "container-recreated", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, repo.RemoveWorker(worker.Id))
	replacement := *worker
	replacement.Id = "worker-recreated-replacement"
	assert.NoError(t, repo.AddWorker(&replacement))
	assert.NoError(t, repo.ScheduleContainerRequest(&replacement, request))
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	oldIndex := common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)
	assert.NoError(t, rdb.SAdd(context.TODO(), oldIndex, stateKey).Err())

	recreated := *worker
	recreated.FreeCpu, recreated.FreeMemory, recreated.FreeGpuCount = 100, 125, 1
	assert.NoError(t, repo.AddWorker(&recreated))
	current := &types.ContainerRequest{ContainerId: "container-recreated-current", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	setPendingContainerRequests(t, rdb, current)
	assert.NoError(t, repo.ScheduleContainerRequest(&recreated, current))
	assert.NoError(t, repo.UpdateWorkerCapacity(&recreated, request, types.AddCapacity))
	assert.Equal(t, int64(0), recreated.FreeCpu)
	assert.Equal(t, int64(0), recreated.FreeMemory)
	assert.Equal(t, uint32(0), recreated.FreeGpuCount)
	assert.False(t, rdb.SIsMember(context.TODO(), oldIndex, stateKey).Val())
	version := recreated.ResourceVersion
	assert.NoError(t, repo.UpdateWorkerCapacity(&recreated, request, types.AddCapacity))
	assert.Equal(t, version, recreated.ResourceVersion)
}

func TestUpdateWorkerCapacityRejectsGPUOverReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-gpu-over-reservation",
		Status:        types.WorkerStatusAvailable,
		FreeCpu:       1000,
		FreeMemory:    1250,
		FreeGpuCount:  0,
		TotalGpuCount: 1,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-gpu-over-reservation",
		Cpu:         100,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    2,
	}

	err = repo.UpdateWorkerCapacity(updatedWorker, request, types.RemoveCapacity)
	assert.Error(t, err)

	unchangedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), unchangedWorker.FreeCpu)
	assert.Equal(t, int64(1250), unchangedWorker.FreeMemory)
	assert.Equal(t, uint32(1), unchangedWorker.FreeGpuCount)
	assert.Equal(t, int64(0), unchangedWorker.ResourceVersion)
}

func TestWorkerNetworkIPIndexMovesPreallocatedReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-a"
	ip := "192.168.0.2"

	err = repo.SetContainerIp(networkPrefix, "network-slot:slot-a", ip)
	assert.Nil(t, err)

	err = repo.MoveContainerIp(networkPrefix, "network-slot:slot-a", "container-a", ip)
	assert.Nil(t, err)

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.Contains(t, ips, ip)

	err = repo.RemoveContainerIp(networkPrefix, "network-slot:slot-a")
	assert.Nil(t, err)

	ips, err = repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.Contains(t, ips, ip)

	containerIP, err := repo.GetContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)
	assert.Equal(t, ip, containerIP)

	err = repo.SetContainerIp(networkPrefix, "container-b", ip)
	assert.Error(t, err)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)

	ips, err = repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, ip)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)
}

func TestGetContainerIpAssignments(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-assignments"

	err = repo.SetContainerIp(networkPrefix, "network-slot:slot-a", "192.168.0.2")
	assert.Nil(t, err)
	err = repo.SetContainerIp(networkPrefix, "container-a", "192.168.0.3")
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.WorkerNetworkIpIndex(networkPrefix), "192.168.0.99").Err()
	assert.Nil(t, err)

	assignments, err := repo.GetContainerIpAssignments(networkPrefix)
	assert.Nil(t, err)
	assert.Equal(t, []types.ContainerIpAssignment{
		{ContainerID: "container-a", IPAddress: "192.168.0.3"},
		{ContainerID: "network-slot:slot-a", IPAddress: "192.168.0.2"},
	}, assignments)

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, "192.168.0.99")
}

func TestRemoveContainerIpCleansLegacyIndexWithoutOwnerKey(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-legacy"
	ip := "192.168.0.12"

	err = repo.SetContainerIp(networkPrefix, "container-a", ip)
	assert.Nil(t, err)

	err = rdb.Del(context.TODO(), common.RedisKeys.WorkerNetworkIpOwner(networkPrefix, ip)).Err()
	assert.Nil(t, err)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, ip)

	err = repo.SetContainerIp(networkPrefix, "container-b", ip)
	assert.Nil(t, err)
}

func TestRemoveWorkerNetworkStateDeletesOnlyMachineScopedAllocations(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewWorkerRedisRepositoryForTest(rdb)
	targetPrefix := common.WorkerNetworkPrefix("beta9", "machine-a")
	otherPrefix := common.WorkerNetworkPrefix("beta9", "machine-b")

	for i := 0; i < 24; i++ {
		containerID := fmt.Sprintf("network-slot:slot-%02d", i)
		ip := fmt.Sprintf("192.168.0.%d", i+2)
		assert.NoError(t, repo.SetContainerIp(targetPrefix, containerID, ip))
	}
	assert.NoError(t, repo.SetContainerIp(otherPrefix, "network-slot:other", "192.168.1.2"))

	ctx := context.Background()
	targetKeys, err := rdb.Keys(ctx, "worker:network:"+targetPrefix+"*")
	assert.NoError(t, err)
	assert.Equal(t, 50, len(targetKeys))
	routeRevisionKey := "scheduler:route:machine:{workspace-1}:pool:machine-a:rev"
	assert.NoError(t, rdb.Set(ctx, routeRevisionKey, "7", 0).Err())

	assert.NoError(t, repo.RemoveWorkerNetworkState(ctx, targetPrefix))
	assert.NoError(t, repo.RemoveWorkerNetworkState(ctx, targetPrefix))

	targetKeys, err = rdb.Keys(ctx, "worker:network:"+targetPrefix+"*")
	assert.NoError(t, err)
	assert.Empty(t, targetKeys)
	otherIP, err := repo.GetContainerIp(otherPrefix, "network-slot:other")
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.2", otherIP)
	routeRevision, err := rdb.Get(ctx, routeRevisionKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "7", routeRevision)
}

func BenchmarkGetAllWorkers(b *testing.B) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(b, rdb)
	assert.Nil(b, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	b.ResetTimer()

	maxDuration := time.Second
	for i := 0; i < b.N; i++ {
		start := time.Now()

		_, _ = repo.GetAllWorkers()

		duration := time.Since(start)
		b.Logf("GetAllWorkers, iteration %d took %v\n", i, duration)

		if duration > maxDuration {
			b.Fatalf("GetAllWorkers, iteration %d took more than %v\n", i, maxDuration)
		}
	}
}

func TestGetId(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	id := repo.GetId()
	assert.Len(t, id, 8)
}
