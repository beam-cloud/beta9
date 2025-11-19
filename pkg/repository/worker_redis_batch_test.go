package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*common.RedisClient, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	return client, mr
}

func TestBatchUpdateWorkerCapacity_SingleWorker(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Create a test worker
	worker := &types.Worker{
		Id:               "test-worker-1",
		FreeCpu:          1000,
		FreeMemory:       2048,
		FreeGpuCount:     0,
		TotalCpu:         1000,
		TotalMemory:      2048,
		TotalGpuCount:    0,
		Status:           types.WorkerStatusAvailable,
		ResourceVersion:  0,
	}
	err := repo.AddWorker(worker)
	require.NoError(t, err)

	// Create reservations for multiple containers on the same worker
	reservations := []CapacityReservation{
		{
			WorkerId: "test-worker-1",
			Reservations: []ResourceReservation{
				{ContainerId: "container-1", CPU: 100, Memory: 256, GPU: 0},
				{ContainerId: "container-2", CPU: 100, Memory: 256, GPU: 0},
				{ContainerId: "container-3", CPU: 100, Memory: 256, GPU: 0},
			},
		},
	}

	// Batch update to remove capacity
	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.RemoveCapacity)
	require.NoError(t, err)
	assert.Nil(t, errors)

	// Verify worker capacity was updated correctly
	updatedWorker, err := repo.GetWorkerById("test-worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(700), updatedWorker.FreeCpu, "CPU should be reduced by 300 (3x100)")
	assert.Equal(t, int64(1280), updatedWorker.FreeMemory, "Memory should be reduced by 768 (3x256)")
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion, "ResourceVersion should be incremented")
}

func TestBatchUpdateWorkerCapacity_MultipleWorkers(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Create multiple test workers
	for i := 1; i <= 3; i++ {
		worker := &types.Worker{
			Id:               "test-worker-" + string(rune('0'+i)),
			FreeCpu:          1000,
			FreeMemory:       2048,
			FreeGpuCount:     0,
			TotalCpu:         1000,
			TotalMemory:      2048,
			TotalGpuCount:    0,
			Status:           types.WorkerStatusAvailable,
			ResourceVersion:  0,
		}
		err := repo.AddWorker(worker)
		require.NoError(t, err)
	}

	// Create reservations across multiple workers
	reservations := []CapacityReservation{
		{
			WorkerId: "test-worker-1",
			Reservations: []ResourceReservation{
				{ContainerId: "container-1", CPU: 100, Memory: 256, GPU: 0},
				{ContainerId: "container-2", CPU: 100, Memory: 256, GPU: 0},
			},
		},
		{
			WorkerId: "test-worker-2",
			Reservations: []ResourceReservation{
				{ContainerId: "container-3", CPU: 150, Memory: 512, GPU: 0},
			},
		},
		{
			WorkerId: "test-worker-3",
			Reservations: []ResourceReservation{
				{ContainerId: "container-4", CPU: 200, Memory: 1024, GPU: 0},
				{ContainerId: "container-5", CPU: 200, Memory: 1024, GPU: 0},
			},
		},
	}

	// Batch update to remove capacity
	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.RemoveCapacity)
	require.NoError(t, err)
	assert.Nil(t, errors)

	// Verify each worker's capacity
	worker1, err := repo.GetWorkerById("test-worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(800), worker1.FreeCpu)
	assert.Equal(t, int64(1536), worker1.FreeMemory)

	worker2, err := repo.GetWorkerById("test-worker-2")
	require.NoError(t, err)
	assert.Equal(t, int64(850), worker2.FreeCpu)
	assert.Equal(t, int64(1536), worker2.FreeMemory)

	worker3, err := repo.GetWorkerById("test-worker-3")
	require.NoError(t, err)
	assert.Equal(t, int64(600), worker3.FreeCpu)
	assert.Equal(t, int64(0), worker3.FreeMemory)
}

func TestBatchUpdateWorkerCapacity_AddCapacity(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Create a test worker with reduced capacity
	worker := &types.Worker{
		Id:               "test-worker-1",
		FreeCpu:          500,
		FreeMemory:       1024,
		FreeGpuCount:     0,
		TotalCpu:         1000,
		TotalMemory:      2048,
		TotalGpuCount:    0,
		Status:           types.WorkerStatusAvailable,
		ResourceVersion:  0,
	}
	err := repo.AddWorker(worker)
	require.NoError(t, err)

	// Add capacity back
	reservations := []CapacityReservation{
		{
			WorkerId: "test-worker-1",
			Reservations: []ResourceReservation{
				{ContainerId: "container-1", CPU: 100, Memory: 256, GPU: 0},
				{ContainerId: "container-2", CPU: 100, Memory: 256, GPU: 0},
			},
		},
	}

	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.AddCapacity)
	require.NoError(t, err)
	assert.Nil(t, errors)

	// Verify capacity was added back
	updatedWorker, err := repo.GetWorkerById("test-worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(700), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1536), updatedWorker.FreeMemory)
}

func TestBatchUpdateWorkerCapacity_InsufficientCapacity(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Create a test worker with limited capacity
	worker := &types.Worker{
		Id:               "test-worker-1",
		FreeCpu:          100,
		FreeMemory:       256,
		FreeGpuCount:     0,
		TotalCpu:         1000,
		TotalMemory:      2048,
		TotalGpuCount:    0,
		Status:           types.WorkerStatusAvailable,
		ResourceVersion:  0,
	}
	err := repo.AddWorker(worker)
	require.NoError(t, err)

	// Try to reserve more than available
	reservations := []CapacityReservation{
		{
			WorkerId: "test-worker-1",
			Reservations: []ResourceReservation{
				{ContainerId: "container-1", CPU: 200, Memory: 512, GPU: 0},
			},
		},
	}

	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.RemoveCapacity)
	require.Error(t, err)
	assert.NotNil(t, errors)
	assert.Contains(t, errors["test-worker-1"].Error(), "worker out of cpu, memory, or gpu")

	// Verify worker capacity was not changed
	unchangedWorker, err := repo.GetWorkerById("test-worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(100), unchangedWorker.FreeCpu)
	assert.Equal(t, int64(256), unchangedWorker.FreeMemory)
}

func TestBatchUpdateWorkerCapacity_GPUResources(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Create a GPU worker
	worker := &types.Worker{
		Id:               "gpu-worker-1",
		FreeCpu:          1000,
		FreeMemory:       2048,
		FreeGpuCount:     2,
		TotalCpu:         1000,
		TotalMemory:      2048,
		TotalGpuCount:    2,
		Gpu:              string(types.GPU_T4),
		Status:           types.WorkerStatusAvailable,
		ResourceVersion:  0,
	}
	err := repo.AddWorker(worker)
	require.NoError(t, err)

	// Reserve GPU resources
	reservations := []CapacityReservation{
		{
			WorkerId: "gpu-worker-1",
			Reservations: []ResourceReservation{
				{ContainerId: "gpu-container-1", CPU: 500, Memory: 1024, GPU: 1, GPUType: string(types.GPU_T4)},
			},
		},
	}

	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.RemoveCapacity)
	require.NoError(t, err)
	assert.Nil(t, errors)

	// Verify GPU capacity was updated
	updatedWorker, err := repo.GetWorkerById("gpu-worker-1")
	require.NoError(t, err)
	assert.Equal(t, int64(500), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1024), updatedWorker.FreeMemory)
	assert.Equal(t, int64(1), updatedWorker.FreeGpuCount, "GPU count should be reduced by 1")
}

func TestBatchUpdateWorkerCapacity_EmptyReservations(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Call with empty reservations
	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), []CapacityReservation{}, types.RemoveCapacity)
	require.NoError(t, err)
	assert.Nil(t, errors)
}

func TestBatchUpdateWorkerCapacity_NonexistentWorker(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	repo := NewWorkerRedisRepository(client, config)

	// Try to update a worker that doesn't exist
	reservations := []CapacityReservation{
		{
			WorkerId: "nonexistent-worker",
			Reservations: []ResourceReservation{
				{ContainerId: "container-1", CPU: 100, Memory: 256, GPU: 0},
			},
		},
	}

	errors, err := repo.BatchUpdateWorkerCapacity(context.Background(), reservations, types.RemoveCapacity)
	require.Error(t, err)
	assert.NotNil(t, errors)
	assert.Contains(t, errors["nonexistent-worker"].Error(), "failed to get worker state")
}
