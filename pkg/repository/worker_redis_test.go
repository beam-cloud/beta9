package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

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

	_, ok := err.(*types.ErrWorkerNotFound)
	assert.True(t, ok) // assert that error is of type ErrWorkerNotFound
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
		{ContainerId: "container-1", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 2},
		{ContainerId: "container-2", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 4},
	}
	for _, request := range requests {
		currentWorker, err := repo.GetWorkerById(worker.Id)
		assert.Nil(t, err)
		assert.Nil(t, repo.ScheduleContainerRequest(currentWorker, request))
	}

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
	for _, raw := range backlog {
		var request types.ContainerRequest
		assert.Nil(t, json.Unmarshal([]byte(raw), &request))
		retriesByContainer[request.ContainerId] = request.RetryCount
	}
	assert.Equal(t, 3, retriesByContainer["container-1"])
	assert.Equal(t, 5, retriesByContainer["container-2"])
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
	err = repo.ToggleWorkerAvailable(worker.Id)
	assert.Nil(t, err)

	// Retrieve it again and check fields
	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, types.WorkerStatusAvailable, worker.Status)
}

func TestSetWorkerKeepAlivePromotesPendingWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-keepalive-pending",
		Status:      types.WorkerStatusPending,
		TotalCpu:    1000,
		TotalMemory: 1000,
		FreeCpu:     1000,
		FreeMemory:  1000,
	}
	assert.Nil(t, repo.AddWorker(worker))

	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{}))

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
}

func TestSetWorkerKeepAliveDoesNotPromoteDisabledWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-keepalive-disabled",
		Status:      types.WorkerStatusDisabled,
		TotalCpu:    1000,
		TotalMemory: 1000,
		FreeCpu:     1000,
		FreeMemory:  1000,
	}
	assert.Nil(t, repo.AddWorker(worker))

	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{}))

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, updatedWorker.Status)
}

func TestSetWorkerKeepAliveReconcilesAvailableWorkerCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-keepalive-reconcile",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    3000,
		TotalMemory: 3000,
		FreeCpu:     0,
		FreeMemory:  0,
	}
	assert.Nil(t, repo.AddWorker(worker))

	for _, state := range []*types.ContainerState{
		{ContainerId: "container-keepalive-running", Status: types.ContainerStatusRunning, Cpu: 1000, Memory: 512},
		{ContainerId: "container-keepalive-stopping", Status: types.ContainerStatusStopping, Cpu: 1000, Memory: 512},
	} {
		key := common.RedisKeys.SchedulerContainerState(state.ContainerId)
		assert.Nil(t, rdb.HSet(context.TODO(), key, common.ToSlice(state)).Err())
		assert.Nil(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), key).Err())
	}

	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{}))

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
	assert.Equal(t, int64(2000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(2360), updatedWorker.FreeMemory)
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

	err = repo.ToggleWorkerAvailable(worker.Id)
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

	err = repo.ToggleWorkerAvailable(worker.Id)
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
}

func TestUpdateWorkerCapacityForGPUWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, err)

	newWorker := &types.Worker{
		Id:           "worker1",
		Status:       types.WorkerStatusPending,
		FreeCpu:      0,
		FreeMemory:   0,
		Gpu:          "A10G",
		FreeGpuCount: 0,
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

	// Add some capacity to the worker
	request := &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         500,
		Memory:      1000,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	err = repo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
	assert.Nil(t, err)
	freeMemoryAfterAdd := capacityMemoryForRequest(request)

	// Retrieve the updated worker
	updatedWorker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, request.Cpu, updatedWorker.FreeCpu)
	assert.Equal(t, freeMemoryAfterAdd, updatedWorker.FreeMemory)
	assert.Equal(t, request.Gpu, updatedWorker.Gpu)
	assert.Equal(t, types.WorkerStatusPending, worker.Status)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	// Remove some capacity
	request = &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         100,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	err = repo.UpdateWorkerCapacity(updatedWorker, request, types.RemoveCapacity)
	assert.Nil(t, err)

	// Retrieve the worker again
	updatedWorker, err = repo.GetWorkerById(newWorker.Id)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
	assert.Nil(t, err)

	assert.Equal(t, int64(400), updatedWorker.FreeCpu)
	assert.Equal(t, freeMemoryAfterAdd-capacityMemoryForRequest(request), updatedWorker.FreeMemory)
	assert.Equal(t, request.Gpu, updatedWorker.Gpu)
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

func TestCleanupScheduledContainerRequestPreservesNewerAssignment(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
	containerStateKey := common.RedisKeys.SchedulerContainerState("container-newer-assignment")
	oldWorkerIndexKey := common.RedisKeys.SchedulerContainerWorkerIndex("worker-old")
	newWorkerIndexKey := common.RedisKeys.SchedulerContainerWorkerIndex("worker-new")
	queueKey := common.RedisKeys.SchedulerWorkerRequests("worker-old")
	requestJSON := []byte(`{"container_id":"container-newer-assignment"}`)
	err = rdb.RPush(context.TODO(), queueKey, requestJSON).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), oldWorkerIndexKey, containerStateKey).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), newWorkerIndexKey, containerStateKey).Err()
	assert.Nil(t, err)
	err = rdb.HSet(context.TODO(), containerStateKey,
		"worker_id", "worker-new",
		"machine_id", "machine-new",
		"schedule_assignment_id", "assignment-old",
	).Err()
	assert.Nil(t, err)

	err = repo.cleanupScheduledContainerRequest(context.TODO(), queueKey, requestJSON, true, containerStateKey, oldWorkerIndexKey, "assignment-old", "worker-old")
	assert.Nil(t, err)

	queueDepth, err := rdb.LLen(context.TODO(), queueKey).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)
	indexedOld, err := rdb.SIsMember(context.TODO(), oldWorkerIndexKey, containerStateKey).Result()
	assert.Nil(t, err)
	assert.False(t, indexedOld)
	indexedNew, err := rdb.SIsMember(context.TODO(), newWorkerIndexKey, containerStateKey).Result()
	assert.Nil(t, err)
	assert.True(t, indexedNew)
	stateFields, err := rdb.HGetAll(context.TODO(), containerStateKey).Result()
	assert.Nil(t, err)
	assert.Equal(t, "worker-new", stateFields["worker_id"])
	assert.Equal(t, "machine-new", stateFields["machine_id"])
	assert.Equal(t, "assignment-old", stateFields["schedule_assignment_id"])
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
		GpuCount:    1,
	}

	err = repo.UpdateWorkerCapacity(updatedWorker, request, types.RemoveCapacity)
	assert.Error(t, err)

	unchangedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), unchangedWorker.FreeCpu)
	assert.Equal(t, int64(1250), unchangedWorker.FreeMemory)
	assert.Equal(t, uint32(0), unchangedWorker.FreeGpuCount)
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
