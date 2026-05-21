package repository

import (
	"context"
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
