package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestScheduler(t *testing.T) (*common.RedisClient, *miniredis.Miniredis, repo.WorkerRepository) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	workerRepo := repo.NewWorkerRedisRepository(client, config)

	return client, mr, workerRepo
}

func TestWorkerStateCache_CacheHit(t *testing.T) {
	_, mr, workerRepo := setupTestScheduler(t)
	defer mr.Close()

	cache := NewWorkerStateCache(workerRepo, 500*time.Millisecond)

	// Add test workers
	for i := 1; i <= 3; i++ {
		worker := &types.Worker{
			Id:              "test-worker-" + string(rune('0'+i)),
			FreeCpu:         1000,
			FreeMemory:      2048,
			Status:          types.WorkerStatusAvailable,
			ResourceVersion: 0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(t, err)
	}

	// First call should populate cache
	workers1, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers1, 3)

	// Second call should use cache (fast path)
	workers2, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers2, 3)

	// Verify cache was used by checking last update time is recent
	assert.True(t, time.Since(cache.lastUpdate) < 100*time.Millisecond)
}

func TestWorkerStateCache_CacheExpiry(t *testing.T) {
	_, mr, workerRepo := setupTestScheduler(t)
	defer mr.Close()

	cache := NewWorkerStateCache(workerRepo, 100*time.Millisecond)

	// Add a test worker
	worker := &types.Worker{
		Id:              "test-worker-1",
		FreeCpu:         1000,
		FreeMemory:      2048,
		Status:          types.WorkerStatusAvailable,
		ResourceVersion: 0,
	}
	err := workerRepo.AddWorker(worker)
	require.NoError(t, err)

	// First call populates cache
	workers1, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers1, 1)

	// Wait for cache to expire
	time.Sleep(150 * time.Millisecond)

	// Add another worker
	worker2 := &types.Worker{
		Id:              "test-worker-2",
		FreeCpu:         1000,
		FreeMemory:      2048,
		Status:          types.WorkerStatusAvailable,
		ResourceVersion: 0,
	}
	err = workerRepo.AddWorker(worker2)
	require.NoError(t, err)

	// Second call should refresh cache and pick up new worker
	workers2, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers2, 2, "Cache should have refreshed and picked up new worker")
}

func TestWorkerStateCache_InvalidateWorker(t *testing.T) {
	_, mr, workerRepo := setupTestScheduler(t)
	defer mr.Close()

	cache := NewWorkerStateCache(workerRepo, 500*time.Millisecond)

	// Add test workers
	for i := 1; i <= 3; i++ {
		worker := &types.Worker{
			Id:              "test-worker-" + string(rune('0'+i)),
			FreeCpu:         1000,
			FreeMemory:      2048,
			Status:          types.WorkerStatusAvailable,
			ResourceVersion: 0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(t, err)
	}

	// Populate cache
	workers, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers, 3)

	// Invalidate one worker
	cache.InvalidateWorker("test-worker-1")

	// Cache should have 2 workers now
	cache.mu.RLock()
	assert.Len(t, cache.workers, 2)
	cache.mu.RUnlock()
}

func TestWorkerStateCache_InvalidateAll(t *testing.T) {
	_, mr, workerRepo := setupTestScheduler(t)
	defer mr.Close()

	cache := NewWorkerStateCache(workerRepo, 500*time.Millisecond)

	// Add test workers
	for i := 1; i <= 3; i++ {
		worker := &types.Worker{
			Id:              "test-worker-" + string(rune('0'+i)),
			FreeCpu:         1000,
			FreeMemory:      2048,
			Status:          types.WorkerStatusAvailable,
			ResourceVersion: 0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(t, err)
	}

	// Populate cache
	workers, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers, 3)

	// Invalidate all
	cache.InvalidateAll()

	// Cache should be empty
	cache.mu.RLock()
	assert.Len(t, cache.workers, 0)
	assert.True(t, cache.lastUpdate.IsZero())
	cache.mu.RUnlock()

	// Next call should refresh
	workers, err = cache.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, workers, 3)
}

func TestHashContainerId_Consistency(t *testing.T) {
	// Test that the hash function produces consistent results
	containerId := "test-container-123"

	hash1 := hashContainerId(containerId)
	hash2 := hashContainerId(containerId)

	assert.Equal(t, hash1, hash2, "Hash should be consistent for same input")
}

func TestHashContainerId_Distribution(t *testing.T) {
	// Test that different container IDs produce different hashes
	containerIds := []string{
		"container-1",
		"container-2",
		"container-3",
		"container-4",
		"container-5",
	}

	hashes := make(map[uint64]bool)
	for _, id := range containerIds {
		hash := hashContainerId(id)
		assert.False(t, hashes[hash], "Hash collision detected")
		hashes[hash] = true
	}
}

func TestApplyWorkerAffinity_SingleWorker(t *testing.T) {
	workers := []*types.Worker{
		{Id: "worker-1", FreeCpu: 1000, FreeMemory: 2048},
	}

	result := applyWorkerAffinity("container-1", workers)
	assert.Len(t, result, 1)
	assert.Equal(t, "worker-1", result[0].Id)
}

func TestApplyWorkerAffinity_MultipleWorkers(t *testing.T) {
	workers := []*types.Worker{
		{Id: "worker-1", FreeCpu: 1000, FreeMemory: 2048},
		{Id: "worker-2", FreeCpu: 1000, FreeMemory: 2048},
		{Id: "worker-3", FreeCpu: 1000, FreeMemory: 2048},
		{Id: "worker-4", FreeCpu: 1000, FreeMemory: 2048},
	}

	// Different containers should get different preferred workers
	result1 := applyWorkerAffinity("container-1", workers)
	result2 := applyWorkerAffinity("container-2", workers)
	result3 := applyWorkerAffinity("container-3", workers)

	// All should return all workers (just reordered)
	assert.Len(t, result1, 4)
	assert.Len(t, result2, 4)
	assert.Len(t, result3, 4)

	// Check that the preferred worker (first in list) varies
	// This is probabilistic but should hold in most cases
	preferredWorkers := []string{
		result1[0].Id,
		result2[0].Id,
		result3[0].Id,
	}

	// At least 2 of 3 should be different
	uniquePreferred := make(map[string]bool)
	for _, id := range preferredWorkers {
		uniquePreferred[id] = true
	}
	assert.GreaterOrEqual(t, len(uniquePreferred), 2, "Affinity should distribute across different workers")
}

func TestApplyWorkerAffinity_Stability(t *testing.T) {
	workers := []*types.Worker{
		{Id: "worker-1", FreeCpu: 1000, FreeMemory: 2048},
		{Id: "worker-2", FreeCpu: 1000, FreeMemory: 2048},
		{Id: "worker-3", FreeCpu: 1000, FreeMemory: 2048},
	}

	containerId := "stable-container"

	// Multiple calls with same container ID should produce same ordering
	result1 := applyWorkerAffinity(containerId, workers)
	result2 := applyWorkerAffinity(containerId, workers)

	assert.Equal(t, result1[0].Id, result2[0].Id, "Preferred worker should be stable")
	assert.Equal(t, result1[1].Id, result2[1].Id, "Worker order should be stable")
	assert.Equal(t, result1[2].Id, result2[2].Id, "Worker order should be stable")
}

func TestApplyWorkerAffinity_EmptyList(t *testing.T) {
	workers := []*types.Worker{}
	result := applyWorkerAffinity("container-1", workers)
	assert.Len(t, result, 0)
}

func BenchmarkWorkerStateCache_Get(b *testing.B) {
	mr, err := miniredis.Run()
	require.NoError(b, err)
	defer mr.Close()

	client, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(b, err)

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}
	workerRepo := repo.NewWorkerRedisRepository(client, config)

	// Add 40 workers (typical cluster size)
	for i := 1; i <= 40; i++ {
		worker := &types.Worker{
			Id:              "worker-" + string(rune('0'+i)),
			FreeCpu:         1000,
			FreeMemory:      2048,
			Status:          types.WorkerStatusAvailable,
			ResourceVersion: 0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(b, err)
	}

	cache := NewWorkerStateCache(workerRepo, 500*time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cache.Get(context.Background())
		require.NoError(b, err)
	}
}

func BenchmarkApplyWorkerAffinity(b *testing.B) {
	workers := make([]*types.Worker, 40)
	for i := 0; i < 40; i++ {
		workers[i] = &types.Worker{
			Id:         "worker-" + string(rune('0'+i)),
			FreeCpu:    1000,
			FreeMemory: 2048,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		containerId := "container-" + string(rune('0'+(i%100)))
		applyWorkerAffinity(containerId, workers)
	}
}
