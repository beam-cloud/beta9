package repository

import (
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beam/pkg/types"
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
		Id:     "worker1",
		Status: types.WorkerStatusPending,
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	err = repo.RemoveWorker(worker)
	assert.Nil(t, err)

	err = repo.RemoveWorker(worker)
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
		Id:     "worker1",
		Status: types.WorkerStatusPending,
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
}

func TestToggleWorkerAvailable(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:     "worker1",
		Status: types.WorkerStatusPending,
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Create a pending worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	// Set it to be available
	err = repo.ToggleWorkerAvailable(worker.Id)
	assert.Nil(t, err)

	// Retrieve it again and check fields
	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
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
		Id:     "worker1",
		Status: types.WorkerStatusPending,
		Cpu:    0,
		Memory: 0,
		Gpu:    "A10G",
	}

	// Create a new worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	// Retrieve the worker
	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
	assert.Equal(t, int64(0), newWorker.ResourceVersion)

	// Add some capacity to the worker
	request := &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         500,
		Memory:      1000,
		Gpu:         "A10G",
	}
	err = repo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
	assert.Nil(t, err)

	// Retrieve the updated worker
	updatedWorker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, request.Cpu, updatedWorker.Cpu)
	assert.Equal(t, request.Memory, updatedWorker.Memory)
	assert.Equal(t, request.Gpu, updatedWorker.Gpu)
	assert.Equal(t, types.WorkerStatusPending, worker.Status)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	// Remove some capacity
	request = &types.ContainerRequest{
		ContainerId: "container1",
		Cpu:         100,
		Memory:      100,
		Gpu:         "A10G",
	}
	err = repo.UpdateWorkerCapacity(updatedWorker, request, types.RemoveCapacity)
	assert.Nil(t, err)

	// Retrieve the worker again
	updatedWorker, err = repo.GetWorkerById(newWorker.Id)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
	assert.Nil(t, err)

	// GPU requests should set CPU and memory to zero since we aren't sharing GPU workers
	assert.Equal(t, int64(0), updatedWorker.Cpu)
	assert.Equal(t, int64(0), updatedWorker.Memory)
	assert.Equal(t, request.Gpu, updatedWorker.Gpu)
}

func TestUpdateWorkerCapacityForCPUWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:     "worker1",
		Status: types.WorkerStatusPending,
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
		Agent:  "gcp-uc1",
	}

	// Create a new worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	// Retrieve the worker
	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Cpu, worker.Cpu)
	assert.Equal(t, newWorker.Memory, worker.Memory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
	assert.Equal(t, newWorker.Agent, worker.Agent)
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
	assert.Equal(t, worker.Cpu-firstRequest.Cpu, updatedWorker.Cpu)
	assert.Equal(t, worker.Memory-firstRequest.Memory, updatedWorker.Memory)
	assert.Equal(t, firstRequest.Gpu, updatedWorker.Gpu)
	assert.Equal(t, worker.Status, updatedWorker.Status)
	assert.Equal(t, worker.Agent, worker.Agent)
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

	assert.Equal(t, worker.Cpu-firstRequest.Cpu-secondRequest.Cpu-thirdRequest.Cpu, updatedWorker.Cpu)
	assert.Equal(t, worker.Memory-firstRequest.Memory-secondRequest.Memory-thirdRequest.Memory, updatedWorker.Memory)
	assert.Equal(t, worker.Gpu, updatedWorker.Gpu)
	assert.Equal(t, int64(3), updatedWorker.ResourceVersion)
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
			Id:     fmt.Sprintf("worker-available-%d", i),
			Status: types.WorkerStatusAvailable,
			Cpu:    1000,
			Memory: 1000,
			Gpu:    "A10G",
			Agent:  fmt.Sprintf("agent%d", i),
		})
		assert.Nil(t, err)
	}

	// Create a bunch of pending workers
	for i := 0; i < nWorkers; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:     fmt.Sprintf("worker-pending-%d", i),
			Status: types.WorkerStatusPending,
			Cpu:    1000,
			Memory: 1000,
			Gpu:    "A10G",
		})
		assert.Nil(t, err)
	}

	workers, err := repo.GetAllWorkers()
	assert.Nil(t, err)

	// Ensure we got back the correct total number of workers
	assert.Equal(t, nWorkers*2, len(workers))

	// Ensure we got back the correct number of each status type
	agentCount := 0
	availableCount := 0
	pendingCount := 0
	for _, worker := range workers {
		switch worker.Status {
		case types.WorkerStatusAvailable:
			availableCount++
		case types.WorkerStatusPending:
			pendingCount++
		}
		if worker.Agent != "" {
			agentCount++
		}
	}
	assert.Equal(t, nWorkers, agentCount)
	assert.Equal(t, nWorkers, availableCount)
	assert.Equal(t, nWorkers, pendingCount)
}

func TestWorkerWithAgent(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	nAgents := 5

	err = repo.AddWorker(&types.Worker{
		Id:     "worker-1",
		Status: types.WorkerStatusAvailable,
		Cpu:    100,
		Memory: 100,
		Gpu:    "A10G",
	})
	assert.Nil(t, err)

	for i := 0; i < nAgents; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:     fmt.Sprintf("worker-with-agent-%d", i),
			Status: types.WorkerStatusAvailable,
			Cpu:    200,
			Memory: 400,
			Gpu:    "A10G",
			Agent:  fmt.Sprintf("aws-ue1-%d", i),
		})
		assert.Nil(t, err)
	}

	worker, err := repo.GetWorkerById("worker-1")
	assert.Nil(t, err)
	assert.Equal(t, "worker-1", worker.Id)

	worker, err = repo.GetWorkerById("worker-with-agent-1")
	assert.Nil(t, err)
	assert.Equal(t, "worker-with-agent-1", worker.Id)

	agentCount := 0
	workers, err := repo.GetAllWorkers()
	assert.Nil(t, err)
	for _, worker := range workers {
		if worker.Agent != "" {
			assert.Equal(t, fmt.Sprintf("aws-ue1-%d", agentCount), worker.Agent)
			assert.Equal(t, int64(200), worker.Cpu)
			assert.Equal(t, int64(400), worker.Memory)
			agentCount++
			continue
		}

		assert.Equal(t, int64(100), worker.Cpu)
		assert.Equal(t, int64(100), worker.Memory)
	}

	assert.Equal(t, nAgents, agentCount)
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
