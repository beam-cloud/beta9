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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIntegrationTest(t *testing.T, numWorkers int) (*common.RedisClient, *miniredis.Miniredis, repo.WorkerRepository, *WorkerStateCache) {
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

	// Create workers with CPU capacity
	for i := 1; i <= numWorkers; i++ {
		worker := &types.Worker{
			Id:               fmt.Sprintf("worker-%d", i),
			FreeCpu:          10000, // 10 cores per worker
			FreeMemory:       20480, // 20GB per worker
			FreeGpuCount:     0,
			TotalCpu:         10000,
			TotalMemory:      20480,
			TotalGpuCount:    0,
			Status:           types.WorkerStatusAvailable,
			Priority:         0,
			ResourceVersion:  0,
			PoolName:         "default",
			RequiresPoolSelector: false,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(t, err)
	}

	cache := NewWorkerStateCache(workerRepo, workerCacheDuration)

	return client, mr, workerRepo, cache
}

// TestSchedule100ContainersUnder2Seconds validates the primary performance goal
func TestSchedule100ContainersUnder2Seconds(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	numWorkers := 20
	numContainers := 100

	_, mr, workerRepo, cache := setupIntegrationTest(t, numWorkers)
	defer mr.Close()

	// Prepare container requests
	containers := make([]*types.ContainerRequest, numContainers)
	for i := 0; i < numContainers; i++ {
		containers[i] = &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         100, // 0.1 core per container
			Memory:      256, // 256MB per container
			GpuCount:    0,
			Timestamp:   time.Now(),
		}
	}

	startTime := time.Now()

	// Simulate concurrent scheduling
	var wg sync.WaitGroup
	errors := make(chan error, numContainers)
	reservations := make(chan *schedulingDecision, numContainers)

	for _, container := range containers {
		wg.Add(1)
		go func(req *types.ContainerRequest) {
			defer wg.Done()

			// Get workers from cache
			workers, err := cache.Get(context.Background())
			if err != nil {
				errors <- err
				return
			}

			// Filter workers
			filteredWorkers := filterWorkersByPoolSelector(workers, req)
			filteredWorkers = filterWorkersByResources(filteredWorkers, req)

			if len(filteredWorkers) == 0 {
				errors <- fmt.Errorf("no suitable worker for %s", req.ContainerId)
				return
			}

			// Apply worker affinity
			if len(filteredWorkers) > 1 {
				filteredWorkers = applyWorkerAffinity(req.ContainerId, filteredWorkers)
			}

			// Select best worker
			selectedWorker := filteredWorkers[0]

			reservations <- &schedulingDecision{
				worker:    selectedWorker,
				container: req,
			}
		}(container)
	}

	wg.Wait()
	close(errors)
	close(reservations)

	// Check for errors
	for err := range errors {
		t.Errorf("Scheduling error: %v", err)
	}

	// Group reservations by worker for batch update
	workerReservations := make(map[string][]repo.ResourceReservation)
	for decision := range reservations {
		workerReservations[decision.worker.Id] = append(
			workerReservations[decision.worker.Id],
			repo.ResourceReservation{
				ContainerId: decision.container.ContainerId,
				CPU:         decision.container.Cpu,
				Memory:      decision.container.Memory,
				GPU:         int64(decision.container.GpuCount),
			},
		)
	}

	// Batch update worker capacity
	batchReservations := make([]repo.CapacityReservation, 0, len(workerReservations))
	for workerId, reservations := range workerReservations {
		batchReservations = append(batchReservations, repo.CapacityReservation{
			WorkerId:     workerId,
			Reservations: reservations,
		})
	}

	updateErrors, err := workerRepo.BatchUpdateWorkerCapacity(
		context.Background(),
		batchReservations,
		types.RemoveCapacity,
	)
	require.NoError(t, err)
	assert.Nil(t, updateErrors)

	duration := time.Since(startTime)

	// Primary success criterion: scheduling should complete in under 2 seconds
	assert.Less(t, duration.Seconds(), 2.0,
		"Scheduling 100 containers should complete in under 2 seconds, took %v", duration)

	t.Logf("Successfully scheduled %d containers across %d workers in %v", numContainers, numWorkers, duration)
	t.Logf("Average scheduling time per container: %v", duration/time.Duration(numContainers))
	t.Logf("Number of workers touched: %d", len(batchReservations))
}

type schedulingDecision struct {
	worker    *types.Worker
	container *types.ContainerRequest
}

// TestConcurrentSchedulingCorrectness verifies no double-booking under contention
func TestConcurrentSchedulingCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	numWorkers := 5
	numContainers := 50

	_, mr, workerRepo, cache := setupIntegrationTest(t, numWorkers)
	defer mr.Close()

	// Create containers that will fill up workers
	containers := make([]*types.ContainerRequest, numContainers)
	for i := 0; i < numContainers; i++ {
		containers[i] = &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000, // 1 core per container
			Memory:      2048, // 2GB per container
			GpuCount:    0,
			Timestamp:   time.Now(),
		}
	}

	// Schedule concurrently
	var wg sync.WaitGroup
	reservations := make(chan *schedulingDecision, numContainers)
	var schedulingErrors []error
	var errMu sync.Mutex

	for _, container := range containers {
		wg.Add(1)
		go func(req *types.ContainerRequest) {
			defer wg.Done()

			workers, err := cache.Get(context.Background())
			if err != nil {
				errMu.Lock()
				schedulingErrors = append(schedulingErrors, err)
				errMu.Unlock()
				return
			}

			filteredWorkers := filterWorkersByPoolSelector(workers, req)
			filteredWorkers = filterWorkersByResources(filteredWorkers, req)

			if len(filteredWorkers) == 0 {
				// Expected when workers are full
				return
			}

			if len(filteredWorkers) > 1 {
				filteredWorkers = applyWorkerAffinity(req.ContainerId, filteredWorkers)
			}

			reservations <- &schedulingDecision{
				worker:    filteredWorkers[0],
				container: req,
			}
		}(container)
	}

	wg.Wait()
	close(reservations)

	require.Empty(t, schedulingErrors, "Should not have scheduling errors")

	// Group and batch update
	workerReservations := make(map[string][]repo.ResourceReservation)
	for decision := range reservations {
		workerReservations[decision.worker.Id] = append(
			workerReservations[decision.worker.Id],
			repo.ResourceReservation{
				ContainerId: decision.container.ContainerId,
				CPU:         decision.container.Cpu,
				Memory:      decision.container.Memory,
				GPU:         int64(decision.container.GpuCount),
			},
		)
	}

	batchReservations := make([]repo.CapacityReservation, 0, len(workerReservations))
	for workerId, reservations := range workerReservations {
		batchReservations = append(batchReservations, repo.CapacityReservation{
			WorkerId:     workerId,
			Reservations: reservations,
		})
	}

	updateErrors, err := workerRepo.BatchUpdateWorkerCapacity(
		context.Background(),
		batchReservations,
		types.RemoveCapacity,
	)
	require.NoError(t, err)
	assert.Nil(t, updateErrors, "All capacity updates should succeed")

	// Verify no worker has negative capacity
	for workerId := range workerReservations {
		worker, err := workerRepo.GetWorkerById(workerId)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, worker.FreeCpu, int64(0), "Worker %s should not have negative CPU", workerId)
		assert.GreaterOrEqual(t, worker.FreeMemory, int64(0), "Worker %s should not have negative memory", workerId)
	}

	t.Logf("Successfully scheduled containers across %d workers without double-booking", numWorkers)
}

// TestWorkerAffinityDistribution verifies containers are distributed evenly
func TestWorkerAffinityDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	numWorkers := 10
	numContainers := 100

	_, mr, _, cache := setupIntegrationTest(t, numWorkers)
	defer mr.Close()

	containers := make([]*types.ContainerRequest, numContainers)
	for i := 0; i < numContainers; i++ {
		containers[i] = &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         10,
			Memory:      256,
			GpuCount:    0,
		}
	}

	workerAssignments := make(map[string]int)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, container := range containers {
		wg.Add(1)
		go func(req *types.ContainerRequest) {
			defer wg.Done()

			workers, err := cache.Get(context.Background())
			require.NoError(t, err)

			filteredWorkers := filterWorkersByPoolSelector(workers, req)
			filteredWorkers = filterWorkersByResources(filteredWorkers, req)

			if len(filteredWorkers) > 1 {
				filteredWorkers = applyWorkerAffinity(req.ContainerId, filteredWorkers)
			}

			if len(filteredWorkers) > 0 {
				mu.Lock()
				workerAssignments[filteredWorkers[0].Id]++
				mu.Unlock()
			}
		}(container)
	}

	wg.Wait()

	// Verify distribution is reasonably even
	// With 100 containers and 10 workers, expect ~10 per worker
	// Allow some variance (e.g., 5-15 per worker)
	for workerId, count := range workerAssignments {
		assert.GreaterOrEqual(t, count, 5, "Worker %s should have at least 5 assignments", workerId)
		assert.LessOrEqual(t, count, 15, "Worker %s should have at most 15 assignments", workerId)
	}

	t.Logf("Container distribution across workers: %v", workerAssignments)
	assert.Len(t, workerAssignments, numWorkers, "All workers should receive assignments")
}

// BenchmarkScheduling100Containers measures scheduling performance
func BenchmarkScheduling100Containers(b *testing.B) {
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
			Id:               fmt.Sprintf("worker-%d", i),
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

		// Schedule them
		var wg sync.WaitGroup
		for _, container := range containers {
			wg.Add(1)
			go func(req *types.ContainerRequest) {
				defer wg.Done()
				workers, _ := cache.Get(context.Background())
				filteredWorkers := filterWorkersByPoolSelector(workers, req)
				filteredWorkers = filterWorkersByResources(filteredWorkers, req)
				if len(filteredWorkers) > 1 {
					_ = applyWorkerAffinity(req.ContainerId, filteredWorkers)
				}
			}(container)
		}
		wg.Wait()
	}
}

// TestPriorityAndPoolConstraints ensures optimizations don't break existing logic
func TestPriorityAndPoolConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	_, mr, workerRepo, cache := setupIntegrationTest(t, 5)
	defer mr.Close()

	// Add a high-priority worker
	highPriorityWorker := &types.Worker{
		Id:               "high-priority-worker",
		FreeCpu:          10000,
		FreeMemory:       20480,
		Status:           types.WorkerStatusAvailable,
		Priority:         100, // High priority
		ResourceVersion:  0,
		PoolName:         "default",
		RequiresPoolSelector: false,
	}
	err := workerRepo.AddWorker(highPriorityWorker)
	require.NoError(t, err)

	// Create a container request
	container := &types.ContainerRequest{
		ContainerId: "test-container",
		Cpu:         100,
		Memory:      256,
		GpuCount:    0,
	}

	workers, err := cache.Get(context.Background())
	require.NoError(t, err)

	filteredWorkers := filterWorkersByPoolSelector(workers, container)
	filteredWorkers = filterWorkersByResources(filteredWorkers, container)

	// Apply affinity
	if len(filteredWorkers) > 1 {
		filteredWorkers = applyWorkerAffinity(container.ContainerId, filteredWorkers)
	}

	// Score workers
	scoredWorkers := []scoredWorker{}
	for _, worker := range filteredWorkers {
		score := int32(0)
		if worker.Status == types.WorkerStatusAvailable {
			score += scoreAvailableWorker
		}
		score += worker.Priority
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	// Sort by score
	var bestWorker *types.Worker
	bestScore := int32(-1000000)
	for _, sw := range scoredWorkers {
		if sw.score > bestScore {
			bestScore = sw.score
			bestWorker = sw.worker
		}
	}

	// High priority worker should be selected
	assert.Equal(t, "high-priority-worker", bestWorker.Id, "High priority worker should be preferred")
}
