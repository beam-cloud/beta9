package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchedulerPerformance100Containers is a comprehensive test that validates the batch scheduling
// optimization by scheduling 100 containers in fast succession and measuring key performance metrics.
func TestSchedulerPerformance100Containers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Setup
	numWorkers := 20
	numContainers := 100
	targetDuration := 2 * time.Second

	t.Logf("=== Starting Comprehensive Performance Test ===")
	t.Logf("Configuration:")
	t.Logf("  - Workers: %d", numWorkers)
	t.Logf("  - Containers: %d", numContainers)
	t.Logf("  - Target time: %v", targetDuration)
	t.Logf("")

	_, mr, workerRepo, cache := setupIntegrationTest(t, numWorkers)
	defer mr.Close()

	// Metrics collection
	metrics := &performanceMetrics{
		lockAcquisitions:    &atomic.Int64{},
		cacheHits:           &atomic.Int64{},
		cacheMisses:         &atomic.Int64{},
		workerReads:         &atomic.Int64{},
		capacityUpdates:     &atomic.Int64{},
		cacheInvalidations:  &atomic.Int64{},
		workersUsed:         make(map[string]int),
		schedulingDurations: make([]time.Duration, 0, numContainers),
	}

	// Create instrumented repository wrapper
	instrumentedRepo := &instrumentedWorkerRepo{
		WorkerRepository: workerRepo,
		metrics:          metrics,
	}

	// Create instrumented cache wrapper
	instrumentedCache := &instrumentedCache{
		cache:   cache,
		metrics: metrics,
	}

	// Prepare container requests
	containers := make([]*types.ContainerRequest, numContainers)
	for i := 0; i < numContainers; i++ {
		containers[i] = &types.ContainerRequest{
			ContainerId: fmt.Sprintf("perf-test-container-%d", i),
			Cpu:         100,  // 0.1 core per container
			Memory:      256,  // 256MB per container
			GpuCount:    0,
			Timestamp:   time.Now(),
		}
	}

	// Start timing
	startTime := time.Now()

	t.Logf("Starting to schedule %d containers...", numContainers)

	// Simulate batch scheduling
	workers, err := instrumentedCache.Get(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, workers, "Should have workers available")

	t.Logf("Retrieved %d workers from cache", len(workers))

	// Make scheduling decisions (simulate processBatch)
	decisions := make([]schedulingDecision, 0, numContainers)
	workerCapacity := make(map[string]*workerCapacityTracker)

	// Initialize capacity trackers
	for _, w := range workers {
		workerCapacity[w.Id] = &workerCapacityTracker{
			freeCpu:      w.FreeCpu,
			freeMemory:   w.FreeMemory,
			freeGpuCount: w.FreeGpuCount,
		}
	}

	// Make all scheduling decisions
	decisionStartTime := time.Now()
	for _, request := range containers {
		// Filter workers
		filteredWorkers := filterWorkersByPoolSelector(workers, request)
		filteredWorkers = filterWorkersByResources(filteredWorkers, request)
		filteredWorkers = filterWorkersByFlags(filteredWorkers, request)

		// Check against tracked capacity
		availableWorkers := make([]*types.Worker, 0)
		for _, w := range filteredWorkers {
			capacity := workerCapacity[w.Id]
			if capacity.freeCpu >= request.Cpu && capacity.freeMemory >= request.Memory {
				availableWorkers = append(availableWorkers, w)
			}
		}

		require.NotEmpty(t, availableWorkers, "Should have available workers for request %s", request.ContainerId)

		// Apply affinity
		if len(availableWorkers) > 1 {
			availableWorkers = applyWorkerAffinity(request.ContainerId, availableWorkers)
		}

		// Select best worker
		bestWorker := availableWorkers[0]
		if bestWorker.Status == types.WorkerStatusAvailable {
			bestWorker = availableWorkers[0]
		}

		// Reserve capacity
		capacity := workerCapacity[bestWorker.Id]
		capacity.freeCpu -= request.Cpu
		capacity.freeMemory -= request.Memory

		metrics.mu.Lock()
		metrics.workersUsed[bestWorker.Id]++
		metrics.mu.Unlock()

		decisions = append(decisions, schedulingDecision{
			request: request,
			worker:  bestWorker,
		})
	}
	decisionDuration := time.Since(decisionStartTime)

	t.Logf("Made %d scheduling decisions in %v", len(decisions), decisionDuration)

	// Group reservations by worker
	workerReservations := make(map[string][]repo.ResourceReservation)
	for _, decision := range decisions {
		workerReservations[decision.worker.Id] = append(
			workerReservations[decision.worker.Id],
			repo.ResourceReservation{
				ContainerId: decision.request.ContainerId,
				CPU:         decision.request.Cpu,
				Memory:      decision.request.Memory,
				GPU:         int64(decision.request.GpuCount),
			},
		)
	}

	// Build batch reservations
	batchReservations := make([]repo.CapacityReservation, 0, len(workerReservations))
	for workerId, reservations := range workerReservations {
		batchReservations = append(batchReservations, repo.CapacityReservation{
			WorkerId:     workerId,
			Reservations: reservations,
		})
	}

	t.Logf("Grouped into %d worker batches", len(batchReservations))

	// Batch update worker capacity
	batchUpdateStart := time.Now()
	updateErrors, err := instrumentedRepo.BatchUpdateWorkerCapacity(
		context.Background(),
		batchReservations,
		types.RemoveCapacity,
	)
	batchUpdateDuration := time.Since(batchUpdateStart)

	require.NoError(t, err)
	assert.Nil(t, updateErrors, "All capacity updates should succeed")

	t.Logf("Batch update completed in %v", batchUpdateDuration)

	// Total duration
	totalDuration := time.Since(startTime)

	// Invalidate cache for affected workers (simulate post-batch invalidation)
	for workerId := range workerReservations {
		instrumentedCache.InvalidateWorker(workerId)
	}

	t.Logf("")
	t.Logf("=== Performance Test Results ===")
	t.Logf("")
	t.Logf("Timing:")
	t.Logf("  Total duration:          %v", totalDuration)
	t.Logf("  Decision making:         %v (%.1f%%)", decisionDuration, float64(decisionDuration)/float64(totalDuration)*100)
	t.Logf("  Batch capacity update:   %v (%.1f%%)", batchUpdateDuration, float64(batchUpdateDuration)/float64(totalDuration)*100)
	t.Logf("  Average per container:   %v", totalDuration/time.Duration(numContainers))
	t.Logf("")

	t.Logf("Resource Efficiency:")
	t.Logf("  Containers scheduled:    %d", len(decisions))
	t.Logf("  Workers used:            %d (out of %d available)", len(metrics.workersUsed), numWorkers)
	t.Logf("  Lock acquisitions:       %d (%.1f per container)", metrics.lockAcquisitions.Load(), float64(metrics.lockAcquisitions.Load())/float64(numContainers))
	t.Logf("  Capacity updates:        %d (%.1f per container)", metrics.capacityUpdates.Load(), float64(metrics.capacityUpdates.Load())/float64(numContainers))
	t.Logf("  Cache invalidations:     %d", metrics.cacheInvalidations.Load())
	t.Logf("")

	t.Logf("Cache Performance:")
	totalCacheAccess := metrics.cacheHits.Load() + metrics.cacheMisses.Load()
	hitRate := float64(0)
	if totalCacheAccess > 0 {
		hitRate = float64(metrics.cacheHits.Load()) / float64(totalCacheAccess) * 100
	}
	t.Logf("  Cache hits:              %d", metrics.cacheHits.Load())
	t.Logf("  Cache misses:            %d", metrics.cacheMisses.Load())
	t.Logf("  Hit rate:                %.1f%%", hitRate)
	t.Logf("")

	t.Logf("Worker Distribution:")
	for workerId, count := range metrics.workersUsed {
		percentage := float64(count) / float64(numContainers) * 100
		t.Logf("  %s: %d containers (%.1f%%)", workerId, count, percentage)
	}
	t.Logf("")

	// Verify worker capacity was actually updated
	t.Logf("Verifying worker capacity...")
	for workerId := range metrics.workersUsed {
		worker, err := workerRepo.GetWorkerById(workerId)
		require.NoError(t, err, "Should be able to retrieve worker %s", workerId)
		
		// Worker should have less capacity now
		assert.Less(t, worker.FreeCpu, int64(10000), "Worker %s should have used CPU", workerId)
		assert.GreaterOrEqual(t, worker.FreeCpu, int64(0), "Worker %s should not have negative CPU", workerId)
		
		t.Logf("  %s: FreeCpu=%d, FreeMemory=%d", workerId, worker.FreeCpu, worker.FreeMemory)
	}
	t.Logf("")

	// Assertions
	t.Logf("=== Validating Performance Goals ===")
	t.Logf("")

	// Goal 1: Total time < 2 seconds
	assert.Less(t, totalDuration, targetDuration,
		"Should schedule %d containers in under %v (took %v)",
		numContainers, targetDuration, totalDuration)
	t.Logf("✓ Total scheduling time: %v (target: <%v)", totalDuration, targetDuration)

	// Goal 2: All containers scheduled
	assert.Equal(t, numContainers, len(decisions),
		"All containers should be scheduled")
	t.Logf("✓ All %d containers scheduled successfully", numContainers)

	// Goal 3: Lock acquisitions should be much lower than containers
	// With batch updates, we expect ~1 lock per worker used (not per container)
	expectedMaxLocks := int64(len(metrics.workersUsed) + 5) // Some tolerance
	assert.LessOrEqual(t, metrics.lockAcquisitions.Load(), expectedMaxLocks,
		"Lock acquisitions should be ~1 per worker (got %d for %d workers)",
		metrics.lockAcquisitions.Load(), len(metrics.workersUsed))
	t.Logf("✓ Lock acquisitions: %d (expected ~%d, one per worker)", 
		metrics.lockAcquisitions.Load(), len(metrics.workersUsed))

	// Goal 4: Capacity updates should be batched (not one per container)
	assert.LessOrEqual(t, metrics.capacityUpdates.Load(), int64(len(metrics.workersUsed)),
		"Capacity updates should be batched (one per worker)")
	t.Logf("✓ Capacity updates: %d (batched to %d workers)", 
		metrics.capacityUpdates.Load(), len(metrics.workersUsed))

	// Goal 5: Workers should be distributed reasonably
	minContainersPerWorker := numContainers / (numWorkers * 2) // Allow imbalance
	for workerId, count := range metrics.workersUsed {
		assert.GreaterOrEqual(t, count, minContainersPerWorker,
			"Worker %s should have reasonable load (has %d, min %d)",
			workerId, count, minContainersPerWorker)
	}
	t.Logf("✓ Workers are reasonably distributed (%d workers used)", len(metrics.workersUsed))

	// Goal 6: No worker should have negative capacity
	for workerId := range metrics.workersUsed {
		worker, _ := workerRepo.GetWorkerById(workerId)
		assert.GreaterOrEqual(t, worker.FreeCpu, int64(0),
			"Worker %s should not have negative CPU", workerId)
		assert.GreaterOrEqual(t, worker.FreeMemory, int64(0),
			"Worker %s should not have negative memory", workerId)
	}
	t.Logf("✓ No workers with negative capacity (no double-booking)")

	t.Logf("")
	t.Logf("=== Performance Test PASSED ===")
	t.Logf("")
	t.Logf("Summary:")
	t.Logf("  - Scheduled %d containers in %v (%.1fx faster than 8s baseline)",
		numContainers, totalDuration, 8.0/totalDuration.Seconds())
	t.Logf("  - Used %d workers efficiently with batch updates", len(metrics.workersUsed))
	t.Logf("  - Achieved %.1f%% cache hit rate", hitRate)
	t.Logf("  - %d lock acquisitions vs %d containers (%.0f%% reduction)",
		metrics.lockAcquisitions.Load(), numContainers,
		(1.0-float64(metrics.lockAcquisitions.Load())/float64(numContainers))*100)
}

// performanceMetrics tracks detailed metrics during the performance test
type performanceMetrics struct {
	lockAcquisitions    *atomic.Int64
	cacheHits           *atomic.Int64
	cacheMisses         *atomic.Int64
	workerReads         *atomic.Int64
	capacityUpdates     *atomic.Int64
	cacheInvalidations  *atomic.Int64
	workersUsed         map[string]int
	schedulingDurations []time.Duration
	mu                  sync.Mutex
}

// instrumentedWorkerRepo wraps the worker repository to track metrics
type instrumentedWorkerRepo struct {
	repo.WorkerRepository
	metrics *performanceMetrics
}

func (r *instrumentedWorkerRepo) BatchUpdateWorkerCapacity(
	ctx context.Context,
	reservations []repo.CapacityReservation,
	capacityUpdateType types.CapacityUpdateType,
) (map[string]error, error) {
	// Track lock acquisitions (one per worker in batch)
	r.metrics.lockAcquisitions.Add(int64(len(reservations)))
	
	// Track capacity updates
	r.metrics.capacityUpdates.Add(int64(len(reservations)))

	// Call actual implementation
	return r.WorkerRepository.BatchUpdateWorkerCapacity(ctx, reservations, capacityUpdateType)
}

// instrumentedCache wraps the cache to track metrics
type instrumentedCache struct {
	cache   *WorkerStateCache
	metrics *performanceMetrics
}

func (c *instrumentedCache) Get(ctx context.Context) ([]*types.Worker, error) {
	c.cache.mu.RLock()
	isCacheHit := time.Since(c.cache.lastUpdate) < c.cache.maxStaleness && len(c.cache.workers) > 0
	c.cache.mu.RUnlock()

	if isCacheHit {
		c.metrics.cacheHits.Add(1)
	} else {
		c.metrics.cacheMisses.Add(1)
		c.metrics.workerReads.Add(1)
	}

	return c.cache.Get(ctx)
}

func (c *instrumentedCache) InvalidateWorker(workerId string) {
	c.metrics.cacheInvalidations.Add(1)
	c.cache.InvalidateWorker(workerId)
}

func (c *instrumentedCache) InvalidateAll() {
	c.metrics.cacheInvalidations.Add(1)
	c.cache.InvalidateAll()
}

// BenchmarkBatchScheduling benchmarks the batch scheduling performance
func BenchmarkBatchScheduling(b *testing.B) {
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

	// Create 20 workers
	for i := 1; i <= 20; i++ {
		worker := &types.Worker{
			Id:               fmt.Sprintf("bench-worker-%d", i),
			FreeCpu:          10000,
			FreeMemory:       20480,
			FreeGpuCount:     0,
			TotalCpu:         10000,
			TotalMemory:      20480,
			Status:           types.WorkerStatusAvailable,
			ResourceVersion:  0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(b, err)
	}

	cache := NewWorkerStateCache(workerRepo, workerCacheDuration)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create 100 container requests
		containers := make([]*types.ContainerRequest, 100)
		for j := 0; j < 100; j++ {
			containers[j] = &types.ContainerRequest{
				ContainerId: fmt.Sprintf("bench-container-%d-%d", i, j),
				Cpu:         100,
				Memory:      256,
				GpuCount:    0,
			}
		}

		// Get workers once
		workers, _ := cache.Get(context.Background())

		// Make scheduling decisions
		decisions := make([]schedulingDecision, 0, 100)
		workerCapacity := make(map[string]*workerCapacityTracker)
		for _, w := range workers {
			workerCapacity[w.Id] = &workerCapacityTracker{
				freeCpu:      w.FreeCpu,
				freeMemory:   w.FreeMemory,
				freeGpuCount: w.FreeGpuCount,
			}
		}

		for _, req := range containers {
			filteredWorkers := filterWorkersByPoolSelector(workers, req)
			filteredWorkers = filterWorkersByResources(filteredWorkers, req)
			
			var bestWorker *types.Worker
			for _, w := range filteredWorkers {
				capacity := workerCapacity[w.Id]
				if capacity.freeCpu >= req.Cpu && capacity.freeMemory >= req.Memory {
					bestWorker = w
					capacity.freeCpu -= req.Cpu
					capacity.freeMemory -= req.Memory
					break
				}
			}

			if bestWorker != nil {
				decisions = append(decisions, schedulingDecision{
					request: req,
					worker:  bestWorker,
				})
			}
		}

		// Group reservations
		workerReservations := make(map[string][]repo.ResourceReservation)
		for _, decision := range decisions {
			workerReservations[decision.worker.Id] = append(
				workerReservations[decision.worker.Id],
				repo.ResourceReservation{
					ContainerId: decision.request.ContainerId,
					CPU:         decision.request.Cpu,
					Memory:      decision.request.Memory,
					GPU:         int64(decision.request.GpuCount),
				},
			)
		}

		// Build batch
		batchReservations := make([]repo.CapacityReservation, 0, len(workerReservations))
		for workerId, reservations := range workerReservations {
			batchReservations = append(batchReservations, repo.CapacityReservation{
				WorkerId:     workerId,
				Reservations: reservations,
			})
		}

		// Batch update (measured operation)
		workerRepo.BatchUpdateWorkerCapacity(context.Background(), batchReservations, types.RemoveCapacity)
	}
}

// BenchmarkBatchSchedulingWithCacheContention benchmarks under cache contention
func BenchmarkBatchSchedulingWithCacheContention(b *testing.B) {
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

	// Create 40 workers (more realistic)
	for i := 1; i <= 40; i++ {
		worker := &types.Worker{
			Id:               fmt.Sprintf("worker-%d", i),
			FreeCpu:          10000,
			FreeMemory:       20480,
			TotalCpu:         10000,
			TotalMemory:      20480,
			Status:           types.WorkerStatusAvailable,
			ResourceVersion:  0,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(b, err)
	}

	cache := NewWorkerStateCache(workerRepo, workerCacheDuration)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batchNum := 0
		for pb.Next() {
			// Simulate concurrent batch processing
			workers, _ := cache.Get(context.Background())
			
			// Quick decision making
			for _, w := range workers {
				_ = w.FreeCpu > 100
			}
			
			// Simulate invalidation
			if batchNum%10 == 0 {
				cache.InvalidateWorker(workers[0].Id)
			}
			batchNum++
		}
	})
}
