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
	"github.com/stretchr/testify/require"
)

// TestScheduler_FewLargeWorkers tests scheduling with minimal worker count
// Goal: 100 containers in < 2s with only 10-20 large workers
func TestScheduler_FewLargeWorkers(t *testing.T) {
	configs := []struct {
		name         string
		workers      int
		cpuPerWorker int64
		batchSize    int
		maxConcurrent int
	}{
		{"10 Large Workers", 10, 200000, 15, 250},
		{"15 Large Workers", 15, 150000, 15, 250},
		{"20 Large Workers", 20, 100000, 15, 250},
		{"10 XL Workers + Large Batch", 10, 200000, 30, 250},
		{"15 Large Workers + Large Batch", 15, 150000, 25, 250},
	}

	fmt.Println("\n🎯 SCHEDULING WITH FEW LARGE WORKERS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("%-35s | %7s | %8s | %9s | %7s\n", "Configuration", "Time", "Success", "Conflicts", "Status")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for _, cfg := range configs {
		result := runSchedulingTest(t, cfg.workers, cfg.cpuPerWorker, cfg.batchSize, cfg.maxConcurrent)
		
		status := "❌"
		if result.scheduled >= 95 && result.duration < 2*time.Second {
			status = "✅"
		} else if result.scheduled >= 90 {
			status = "⚠️"
		}
		
		fmt.Printf("%-35s | %6dms | %6d/100 | %9d | %s\n",
			cfg.name,
			result.duration.Milliseconds(),
			result.scheduled,
			result.conflicts,
			status,
		)
	}
	
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

type scheduleResult struct {
	scheduled  int64
	duration   time.Duration
	conflicts  int64
	throughput float64
}

func runSchedulingTest(t *testing.T, workerCount int, cpuPerWorker int64, batchSize int, maxConcurrent int) scheduleResult {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	backlog := NewRequestBacklog(rdb)

	// Create large workers
	for i := 0; i < workerCount; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        cpuPerWorker,
			FreeCpu:         cpuPerWorker,
			TotalMemory:     cpuPerWorker,
			FreeMemory:      cpuPerWorker,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, maxConcurrent),
		cachedWorkers:       []*types.Worker{},
		workerCacheMu:       sync.RWMutex{},
	}

	// Add 100 containers
	for i := 0; i < 100; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}

	var scheduled int64
	var conflicts int64
	var wg sync.WaitGroup
	var requeueMu sync.Mutex
	var requeued []*types.ContainerRequest

	start := time.Now()

	// Process with requeue support (up to 3 passes)
	for pass := 0; pass < 3; pass++ {
		if pass > 0 {
			requeueMu.Lock()
			for _, req := range requeued {
				backlog.Push(req)
			}
			requeued = nil
			requeueMu.Unlock()

			if backlog.Len() == 0 {
				break
			}
		}

		for backlog.Len() > 0 {
			batch, err := backlog.PopBatch(int64(batchSize))
			if err != nil || len(batch) == 0 {
				break
			}

			workers, err := scheduler.getCachedWorkers()
			if err != nil {
				continue
			}

			for _, request := range batch {
				req := request
				wg.Add(1)

				scheduler.schedulingSemaphore <- struct{}{}
				go func(r *types.ContainerRequest) {
					defer func() {
						<-scheduler.schedulingSemaphore
						wg.Done()
					}()

					success := false
					for attempt := 0; attempt < 5; attempt++ {
						shuffled := shuffleWorkers(workers)

						for _, worker := range shuffled {
							if !scheduler.isWorkerSuitable(worker, r) {
								continue
							}

							err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
							if err == nil {
								atomic.AddInt64(&scheduled, 1)
								success = true
								return
							}
							atomic.AddInt64(&conflicts, 1)
						}

						if attempt < 4 {
							time.Sleep(time.Millisecond * time.Duration(attempt+1))
							workers, _ = scheduler.getCachedWorkers()
						}
					}

					if !success {
						requeueMu.Lock()
						requeued = append(requeued, r)
						requeueMu.Unlock()
					}
				}(req)
			}
		}

		wg.Wait()
	}

	duration := time.Since(start)

	return scheduleResult{
		scheduled:  scheduled,
		duration:   duration,
		conflicts:  conflicts,
		throughput: float64(scheduled) / duration.Seconds(),
	}
}

// TestScheduler_BatchCapacityUpdate tests batch updating worker capacity
// This reduces conflicts by updating multiple containers at once
func TestScheduler_BatchCapacityUpdate(t *testing.T) {
	t.Skip("TODO: Implement batch capacity updates to reduce conflicts")
	
	// Idea: Instead of updating worker capacity one container at a time,
	// batch multiple containers and update capacity in a single atomic operation.
	// This would dramatically reduce conflicts with fewer large workers.
}

// TestScheduler_WorkerAffinity tests assigning batches to specific workers
// This reduces contention by partitioning work across workers
func TestScheduler_WorkerAffinity(t *testing.T) {
	t.Skip("TODO: Implement worker affinity to reduce contention")
	
	// Idea: Assign each batch to a specific worker upfront, reducing
	// the number of workers that each goroutine tries to schedule on.
	// This creates "lanes" that reduce conflicts.
}

// TestScheduler_PessimisticLocking tests using pessimistic locks for batches
// This prevents conflicts by locking workers during batch scheduling
func TestScheduler_PessimisticLocking(t *testing.T) {
	t.Skip("TODO: Implement pessimistic locking for batch operations")
	
	// Idea: For batch operations, use a lock/lease on the worker to prevent
	// other goroutines from trying to schedule on it. This ensures one batch
	// completes before another starts, reducing wasted work from conflicts.
}

// TestScheduler_IdentifyBottleneck analyzes where time is spent
func TestScheduler_IdentifyBottleneck(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	backlog := NewRequestBacklog(rdb)

	// Test with 10 large workers
	for i := 0; i < 10; i++ {
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

	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, 250),
		cachedWorkers:       []*types.Worker{},
		workerCacheMu:       sync.RWMutex{},
	}

	// Add 100 containers
	for i := 0; i < 100; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}

	// Instrumentation
	var (
		totalRetries     int64
		totalWaitTime    int64
		conflictsByRetry = make(map[int]int64)
		successByRetry   = make(map[int]int64)
		mu               sync.Mutex
	)

	var wg sync.WaitGroup
	start := time.Now()

	for backlog.Len() > 0 {
		batch, err := backlog.PopBatch(15)
		if err != nil || len(batch) == 0 {
			break
		}

		workers, _ := scheduler.getCachedWorkers()

		for _, request := range batch {
			req := request
			wg.Add(1)

			scheduler.schedulingSemaphore <- struct{}{}
			go func(r *types.ContainerRequest) {
				defer func() {
					<-scheduler.schedulingSemaphore
					wg.Done()
				}()

				for attempt := 0; attempt < 5; attempt++ {
					attemptStart := time.Now()
					
					if attempt > 0 {
						atomic.AddInt64(&totalRetries, 1)
						waitTime := time.Millisecond * time.Duration(attempt)
						time.Sleep(waitTime)
						atomic.AddInt64(&totalWaitTime, waitTime.Milliseconds())
					}

					for _, worker := range workers {
						if !scheduler.isWorkerSuitable(worker, r) {
							continue
						}

						err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
						if err == nil {
							mu.Lock()
							successByRetry[attempt]++
							mu.Unlock()
							
							elapsed := time.Since(attemptStart)
							if elapsed > 10*time.Millisecond {
								t.Logf("Slow success on attempt %d: %v", attempt, elapsed)
							}
							return
						}
						
						mu.Lock()
						conflictsByRetry[attempt]++
						mu.Unlock()
					}

					workers, _ = scheduler.getCachedWorkers()
				}
			}(req)
		}
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n🔍 BOTTLENECK ANALYSIS (10 Large Workers)\n")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("Total Duration:    %v\n", duration)
	fmt.Printf("Total Retries:     %d\n", totalRetries)
	fmt.Printf("Total Wait Time:   %dms (%.1f%% of total)\n", 
		totalWaitTime, float64(totalWaitTime)/float64(duration.Milliseconds())*100)
	fmt.Println("\nSuccess by Retry Attempt:")
	for i := 0; i < 5; i++ {
		fmt.Printf("  Attempt %d: %d successes, %d conflicts\n", 
			i+1, successByRetry[i], conflictsByRetry[i])
	}
	
	totalConflicts := int64(0)
	for _, c := range conflictsByRetry {
		totalConflicts += c
	}
	
	fmt.Printf("\n💡 Key Metrics:\n")
	fmt.Printf("   Conflicts per success: %.1f\n", 
		float64(totalConflicts)/float64(successByRetry[0]+successByRetry[1]+successByRetry[2]+successByRetry[3]+successByRetry[4]))
	fmt.Printf("   First attempt success: %d%% \n", 
		successByRetry[0]*100/(successByRetry[0]+conflictsByRetry[0]))
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	
	fmt.Println("\n🎯 Bottleneck: High conflicts due to concurrent updates on few workers")
	fmt.Println("   Solution Options:")
	fmt.Println("   1. Batch capacity updates (update multiple containers atomically)")
	fmt.Println("   2. Worker affinity (assign batches to specific workers)")
	fmt.Println("   3. Pessimistic locking (lock worker during batch scheduling)")
	fmt.Println("   4. Pre-allocation (reserve capacity before scheduling)")
}
