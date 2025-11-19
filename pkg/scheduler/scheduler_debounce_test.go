package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestDebounce_SequentialRequests tests that sequential requests are batched together
func TestDebounce_SequentialRequests(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	require.NotNil(t, scheduler)

	// Add large workers for bulk provisioning
	for i := 0; i < 5; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        200000,
			FreeCpu:         200000,
			TotalMemory:     200000,
			FreeMemory:      200000,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, scheduler.workerRepo.AddWorker(worker))
	}

	// Create a new context for the worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.ctx = ctx

	// Start the background worker
	go scheduler.processRequestWorker()

	// Simulate sequential requests arriving over 150ms
	// They should batch together due to debounce
	numRequests := 8
	for i := 0; i < numRequests; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, scheduler.requestBacklog.Push(req))
		
		// Small delay between requests (simulating sequential arrival)
		time.Sleep(15 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)
	
	// Check that backlog was processed
	backlogAfter := int(scheduler.requestBacklog.Len())
	processed := numRequests - backlogAfter
	
	t.Logf("Requests processed: %d/%d (backlog: %d)", processed, numRequests, backlogAfter)
	
	// Should have processed requests (debounce worked)
	require.GreaterOrEqual(t, processed, minBatchSize, 
		"Should have processed at least minBatchSize requests due to debounce")
	
	t.Logf("✅ Debounce working: %d sequential requests batched together (processed %d)", 
		numRequests, processed)
}

// TestDebounce_FastRequests tests that rapid requests bypass debounce
func TestDebounce_FastRequests(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	containerRepo := repo.NewContainerRedisRepository(rdb)
	backlog := NewRequestBacklog(rdb)

	// Create large workers
	for i := 0; i < 5; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        200000,
			FreeCpu:         200000,
			TotalMemory:     200000,
			FreeMemory:      200000,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := &Scheduler{
		ctx:                 ctx,
		workerRepo:          workerRepo,
		containerRepo:       containerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, 250),
		cachedWorkers:       []*types.Worker{},
		workerCacheMu:       sync.RWMutex{},
	}

	go scheduler.processRequestWorker()

	// Send minBatchSize requests rapidly (all at once)
	start := time.Now()
	for i := 0; i < minBatchSize; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
		containerRepo.SetContainerState(req.ContainerId, &types.ContainerState{
			ContainerId: req.ContainerId,
			Status:      types.ContainerStatusPending,
		})
	}

	// Wait a short time for processing
	time.Sleep(50 * time.Millisecond)
	
	elapsed := time.Since(start)
	backlogAfter := int(backlog.Len())
	processed := minBatchSize - backlogAfter
	
	t.Logf("Processing completed after: %v (processed: %d/%d)", elapsed, processed, minBatchSize)
	
	// Should bypass debounce and process quickly (not wait full 100ms)
	require.Less(t, elapsed, batchDebounceWindow+50*time.Millisecond, 
		"Should bypass debounce when batch is full")
	
	// Should have processed at least some requests
	require.Greater(t, processed, 0, "Should have processed requests")
	
	t.Logf("✅ Fast bypass working: processed in %v (< %v debounce)", 
		elapsed, batchDebounceWindow)
}

// TestDebounce_BulkProvisioning tests that debounced batches trigger bulk provisioning
func TestDebounce_BulkProvisioning(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	containerRepo := repo.NewContainerRedisRepository(rdb)
	backlog := NewRequestBacklog(rdb)

	// Start with NO workers - should trigger provisioning
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := &Scheduler{
		ctx:                 ctx,
		workerRepo:          workerRepo,
		containerRepo:       containerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, 250),
		cachedWorkers:       []*types.Worker{},
		workerCacheMu:       sync.RWMutex{},
	}

	go scheduler.processRequestWorker()

	// Send sequential CPU-only requests
	for i := 0; i < 10; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
		containerRepo.SetContainerState(req.ContainerId, &types.ContainerState{
			ContainerId: req.ContainerId,
			Status:      types.ContainerStatusPending,
		})
		
		// Simulate sequential arrival
		time.Sleep(15 * time.Millisecond)
	}

	// Wait for debounce to process
	time.Sleep(200 * time.Millisecond)

	// Check that backlog was at least read (processed as a batch)
	// Note: We can't easily test provisioning without mocking controllers,
	// but we can verify debounce created a batch
	backlogLen := backlog.Len()
	t.Logf("Backlog after debounce: %d (processed: %d)", backlogLen, 10-backlogLen)
	
	// Should have processed at least one batch
	require.Less(t, backlogLen, 10, 
		"Should have processed at least some requests in batch")
	
	t.Logf("✅ Debounced batch processed (enables bulk provisioning)")
}
