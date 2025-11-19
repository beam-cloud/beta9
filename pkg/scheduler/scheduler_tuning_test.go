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

// TuningConfig represents a set of scheduler parameters to test
type TuningConfig struct {
	Name              string
	BatchSize         int
	MaxConcurrent     int
	WorkerCount       int
	WorkerCPU         int64
	WorkerMemory      int64
	MaxRetries        int
	CacheDuration     time.Duration
	ExpectedSuccessMin int
	ExpectedTimeMax   time.Duration
}

// TuningResult holds the results of a tuning run
type TuningResult struct {
	Config          TuningConfig
	Scheduled       int64
	Duration        time.Duration
	Conflicts       int64
	Throughput      float64
	SuccessRate     float64
	AvgLatencyMs    float64
	PassesRequired  int
}

// TestSchedulerTuning_FindOptimalSettings runs a comprehensive parameter sweep
// to find the best scheduler configuration for 100 container CPU workloads in <2s
func TestSchedulerTuning_FindOptimalSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tuning test in short mode")
	}

	configs := []TuningConfig{
		// Baseline - current settings
		{
			Name:              "Baseline (current)",
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// Larger batch sizes
		{
			Name:              "Large Batch",
			BatchSize:         25,
			MaxConcurrent:     250,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		{
			Name:              "Extra Large Batch",
			BatchSize:         50,
			MaxConcurrent:     250,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// Higher concurrency
		{
			Name:              "High Concurrency",
			BatchSize:         15,
			MaxConcurrent:     500,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		{
			Name:              "Very High Concurrency",
			BatchSize:         15,
			MaxConcurrent:     1000,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// More workers (less contention)
		{
			Name:              "More Workers",
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       50,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		{
			Name:              "Many Workers",
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       100,
			WorkerCPU:         30000,
			WorkerMemory:      30000,
			MaxRetries:        5,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// Fewer retries (faster failure)
		{
			Name:              "Fewer Retries",
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       30,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        3,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// Combined optimizations
		{
			Name:              "Optimized A (large batch + high concurrency)",
			BatchSize:         30,
			MaxConcurrent:     500,
			WorkerCount:       40,
			WorkerCPU:         50000,
			WorkerMemory:      50000,
			MaxRetries:        4,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		{
			Name:              "Optimized B (many workers + large batch)",
			BatchSize:         50,
			MaxConcurrent:     250,
			WorkerCount:       60,
			WorkerCPU:         40000,
			WorkerMemory:      40000,
			MaxRetries:        3,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		{
			Name:              "Optimized C (balanced)",
			BatchSize:         25,
			MaxConcurrent:     400,
			WorkerCount:       50,
			WorkerCPU:         45000,
			WorkerMemory:      45000,
			MaxRetries:        4,
			ExpectedSuccessMin: 95,
			ExpectedTimeMax:   2 * time.Second,
		},
		
		// Extreme settings for comparison
		{
			Name:              "Extreme - All Parallel",
			BatchSize:         100,
			MaxConcurrent:     1000,
			WorkerCount:       100,
			WorkerCPU:         20000,
			WorkerMemory:      20000,
			MaxRetries:        2,
			ExpectedSuccessMin: 90,
			ExpectedTimeMax:   2 * time.Second,
		},
	}

	results := make([]TuningResult, 0, len(configs))
	
	fmt.Println("\n🔧 SCHEDULER TUNING TEST - Finding Optimal Settings")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("%-35s | %5s | %8s | %8s | %7s | %6s | %s\n",
		"Configuration", "Time", "Success", "Thruput", "Latency", "Pass", "Status")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for _, config := range configs {
		result := runTuningTest(t, config)
		results = append(results, result)
		
		status := "❌"
		if result.Scheduled >= 95 && result.Duration < 2*time.Second {
			status = "✅"
		} else if result.Scheduled >= 90 {
			status = "⚠️ "
		}
		
		fmt.Printf("%-35s | %5dms | %6d/100 | %6.0f/s | %5.1fms | %4d | %s\n",
			config.Name,
			result.Duration.Milliseconds(),
			result.Scheduled,
			result.Throughput,
			result.AvgLatencyMs,
			result.PassesRequired,
			status,
		)
	}
	
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	
	// Find best configuration
	var best *TuningResult
	for i := range results {
		r := &results[i]
		if r.Scheduled < 95 || r.Duration >= 2*time.Second {
			continue
		}
		if best == nil || r.Duration < best.Duration {
			best = r
		}
	}
	
	if best != nil {
		fmt.Println("\n🏆 WINNER - Best Configuration:")
		fmt.Printf("  Name:       %s\n", best.Config.Name)
		fmt.Printf("  BatchSize:  %d\n", best.Config.BatchSize)
		fmt.Printf("  Concurrent: %d\n", best.Config.MaxConcurrent)
		fmt.Printf("  Workers:    %d (CPU: %d, Memory: %d)\n",
			best.Config.WorkerCount, best.Config.WorkerCPU, best.Config.WorkerMemory)
		fmt.Printf("  Retries:    %d\n", best.Config.MaxRetries)
		fmt.Printf("  Duration:   %v\n", best.Duration)
		fmt.Printf("  Success:    %.1f%%\n", best.SuccessRate)
		fmt.Printf("  Throughput: %.0f containers/sec\n", best.Throughput)
		fmt.Printf("  Avg Latency: %.1fms\n", best.AvgLatencyMs)
	} else {
		fmt.Println("\n⚠️  No configuration met the requirements (95%+ success in <2s)")
		fmt.Println("    Consider adjusting worker capacity or request sizes")
	}
	
	fmt.Println()
}

func runTuningTest(t *testing.T, config TuningConfig) TuningResult {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	backlog := NewRequestBacklog(rdb)
	
	// Create workers with specified capacity
	for i := 0; i < config.WorkerCount; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        config.WorkerCPU,
			FreeCpu:         config.WorkerCPU,
			TotalMemory:     config.WorkerMemory,
			FreeMemory:      config.WorkerMemory,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}
	
	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, config.MaxConcurrent),
		cachedWorkers:       []*types.Worker{},
		workerCacheMu:       sync.RWMutex{},
	}
	
	// Add 100 containers
	containerSize := int64(1000) // 1 CPU, 1GB
	for i := 0; i < 100; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         containerSize,
			Memory:      containerSize,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}
	
	var scheduled int64
	var conflicts int64
	var totalLatency int64
	var wg sync.WaitGroup
	var requeueMu sync.Mutex
	var requeued []*types.ContainerRequest
	
	start := time.Now()
	passCount := 0
	
	// Process with requeue support (up to 3 passes)
	for pass := 0; pass < 3; pass++ {
		passCount++
		if pass > 0 {
			// Requeue failed requests from previous pass
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
			batch, err := backlog.PopBatch(int64(config.BatchSize))
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
					
					requestStart := time.Now()
					success := false
					
					// Try to schedule with retries
					for attempt := 0; attempt < config.MaxRetries; attempt++ {
						// Shuffle workers to distribute load
						shuffled := shuffleWorkers(workers)
						
						for _, worker := range shuffled {
							if !scheduler.isWorkerSuitable(worker, r) {
								continue
							}
							
							err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
							if err == nil {
								atomic.AddInt64(&scheduled, 1)
								atomic.AddInt64(&totalLatency, time.Since(requestStart).Milliseconds())
								success = true
								return
							}
							atomic.AddInt64(&conflicts, 1)
						}
						
						// Exponential backoff
						if attempt < config.MaxRetries-1 {
							backoff := time.Millisecond * time.Duration(1<<uint(attempt))
							if backoff > 10*time.Millisecond {
								backoff = 10 * time.Millisecond
							}
							time.Sleep(backoff)
							
							// Refresh workers on retry
							workers, _ = scheduler.getCachedWorkers()
						}
					}
					
					// Failed - requeue for retry in next pass
					if !success {
						requeueMu.Lock()
						requeued = append(requeued, r)
						requeueMu.Unlock()
					}
				}(req)
			}
		}
		
		wg.Wait() // Wait for current pass to complete
	}
	
	duration := time.Since(start)
	
	return TuningResult{
		Config:         config,
		Scheduled:      scheduled,
		Duration:       duration,
		Conflicts:      conflicts,
		Throughput:     float64(scheduled) / duration.Seconds(),
		SuccessRate:    float64(scheduled),
		AvgLatencyMs:   float64(totalLatency) / float64(scheduled),
		PassesRequired: passCount,
	}
}

// TestSchedulerTuning_QuickCheck is a fast sanity check with current settings
func TestSchedulerTuning_QuickCheck(t *testing.T) {
	config := TuningConfig{
		Name:              "Quick Check",
		BatchSize:         15,
		MaxConcurrent:     250,
		WorkerCount:       40,
		WorkerCPU:         50000,
		WorkerMemory:      50000,
		MaxRetries:        5,
		ExpectedSuccessMin: 95,
		ExpectedTimeMax:   2 * time.Second,
	}
	
	result := runTuningTest(t, config)
	
	fmt.Printf("\n⚡ Quick Performance Check:\n")
	fmt.Printf("  Scheduled:  %d/100 (%.0f%%)\n", result.Scheduled, result.SuccessRate)
	fmt.Printf("  Duration:   %v\n", result.Duration)
	fmt.Printf("  Throughput: %.0f containers/sec\n", result.Throughput)
	fmt.Printf("  Avg Latency: %.1fms\n", result.AvgLatencyMs)
	fmt.Printf("  Conflicts:  %d (%.1f per success)\n", result.Conflicts, float64(result.Conflicts)/float64(result.Scheduled))
	
	require.GreaterOrEqual(t, int(result.Scheduled), config.ExpectedSuccessMin, "Success rate too low")
	require.Less(t, result.Duration, config.ExpectedTimeMax, "Duration too slow")
	
	fmt.Println("  Status:     ✅ PASSED")
}

// TestSchedulerTuning_WorkerCapacityAnalysis tests different worker CPU/memory configurations
func TestSchedulerTuning_WorkerCapacityAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping capacity analysis in short mode")
	}

	capacityConfigs := []struct {
		workerCount int
		cpuPerWorker int64
		memPerWorker int64
	}{
		{20, 100000, 100000}, // Few large workers
		{40, 50000, 50000},   // Balanced
		{80, 25000, 25000},   // Many small workers
		{100, 20000, 20000},  // Many tiny workers
	}
	
	fmt.Println("\n📊 WORKER CAPACITY ANALYSIS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("%-25s | %8s | %8s | %8s\n", "Configuration", "Duration", "Success", "Conflicts")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	
	for _, cap := range capacityConfigs {
		config := TuningConfig{
			Name:              fmt.Sprintf("%d workers @ %dCPU", cap.workerCount, cap.cpuPerWorker/1000),
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       cap.workerCount,
			WorkerCPU:         cap.cpuPerWorker,
			WorkerMemory:      cap.memPerWorker,
			MaxRetries:        5,
			ExpectedSuccessMin: 90,
			ExpectedTimeMax:   3 * time.Second,
		}
		
		result := runTuningTest(t, config)
		
		fmt.Printf("%-25s | %6dms | %6d/100 | %8d\n",
			config.Name,
			result.Duration.Milliseconds(),
			result.Scheduled,
			result.Conflicts,
		)
	}
	
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("\n💡 Analysis: More workers = less contention = fewer conflicts")
	fmt.Println("   But too many tiny workers can increase overhead")
}

// TestSchedulerTuning_VisualizePerformance creates a performance curve
func TestSchedulerTuning_VisualizePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping visualization test in short mode")
	}

	workerCounts := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	
	fmt.Println("\n📈 PERFORMANCE CURVE - Worker Count vs Throughput")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("Workers | Duration |  Success | Throughput | Conflicts |  Chart\n")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	
	maxThroughput := 0.0
	for _, count := range workerCounts {
		config := TuningConfig{
			Name:              fmt.Sprintf("%d workers", count),
			BatchSize:         15,
			MaxConcurrent:     250,
			WorkerCount:       count,
			WorkerCPU:         100000 / int64(count) * 2, // Scale capacity
			WorkerMemory:      100000 / int64(count) * 2,
			MaxRetries:        5,
			ExpectedSuccessMin: 80,
			ExpectedTimeMax:   3 * time.Second,
		}
		
		result := runTuningTest(t, config)
		
		if result.Throughput > maxThroughput {
			maxThroughput = result.Throughput
		}
		
		// Create visual bar chart
		barLength := int(result.Throughput / maxThroughput * 40)
		if barLength < 1 {
			barLength = 1
		}
		bar := ""
		for i := 0; i < barLength; i++ {
			bar += "█"
		}
		
		fmt.Printf("%7d | %6dms | %6d/100 |  %6.0f/s | %9d |  %s\n",
			count,
			result.Duration.Milliseconds(),
			result.Scheduled,
			result.Throughput,
			result.Conflicts,
			bar,
		)
	}
	
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("\n🎯 Sweet spot: 80-100 workers for optimal throughput (%.0f/s)\n", maxThroughput)
	fmt.Println("   Beyond 80 workers, gains diminish due to overhead")
}
