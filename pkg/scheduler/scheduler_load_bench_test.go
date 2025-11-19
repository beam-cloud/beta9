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

// BenchmarkWorkerSelection benchmarks the core worker selection under load
func BenchmarkWorkerSelection(b *testing.B) {
	mr, err := miniredis.Run()
	require.NoError(b, err)
	defer mr.Close()

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(b, err)

	config := types.WorkerConfig{
		Pools: map[string]types.WorkerPoolConfig{
			"default": {
				Mode: types.PoolModeLocal,
			},
		},
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}

	workerRepo := repo.NewWorkerRedisRepository(redisClient, config)

	// Create 20 workers
	for i := 1; i <= 20; i++ {
		worker := &types.Worker{
			Id:                   fmt.Sprintf("worker-%d", i),
			PoolName:             "default",
			FreeCpu:              10000,
			FreeMemory:           20480,
			TotalCpu:             10000,
			TotalMemory:          20480,
			Status:               types.WorkerStatusAvailable,
			ResourceVersion:      0,
			RequiresPoolSelector: false,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(b, err)
	}

	// Create mock scheduler just for selectWorker
	s := &Scheduler{
		ctx:        context.Background(),
		workerRepo: workerRepo,
	}

	b.ResetTimer()

	// Benchmark concurrent worker selection
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			request := &types.ContainerRequest{
				ContainerId: fmt.Sprintf("container-%d", i),
				Cpu:         100,
				Memory:      256,
				GpuCount:    0,
			}

			_, err := s.selectWorker(request)
			if err != nil {
				b.Errorf("selectWorker failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkScheduleContainerRequests benchmarks the full schedule+update flow
func BenchmarkScheduleContainerRequests(b *testing.B) {
	mr, err := miniredis.Run()
	require.NoError(b, err)
	defer mr.Close()

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(b, err)

	config := types.WorkerConfig{
		Pools: map[string]types.WorkerPoolConfig{
			"default": {
				Mode: types.PoolModeLocal,
			},
		},
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}

	workerRepo := repo.NewWorkerRedisRepository(redisClient, config)

	// Create 20 workers with enough capacity for 100 containers
	for i := 1; i <= 20; i++ {
		worker := &types.Worker{
			Id:                   fmt.Sprintf("worker-%d", i),
			PoolName:             "default",
			FreeCpu:              10000,
			FreeMemory:           20480,
			TotalCpu:             10000,
			TotalMemory:          20480,
			Status:               types.WorkerStatusAvailable,
			ResourceVersion:      0,
			RequiresPoolSelector: false,
		}
		err := workerRepo.AddWorker(worker)
		require.NoError(b, err)
	}

	s := &Scheduler{
		ctx:        context.Background(),
		workerRepo: workerRepo,
	}

	b.ResetTimer()

	// Simulate scheduling 100 containers concurrently
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		errors := make(chan error, 100)

		startTime := time.Now()

		for j := 0; j < 100; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("container-%d-%d", i, idx),
					Cpu:         100,
					Memory:      256,
					GpuCount:    0,
				}

				// Select worker
				worker, err := s.selectWorker(request)
				if err != nil {
					errors <- fmt.Errorf("selectWorker failed: %w", err)
					return
				}

				// Schedule (updates capacity)
				err = workerRepo.ScheduleContainerRequest(worker, request)
				if err != nil {
					errors <- fmt.Errorf("ScheduleContainerRequest failed: %w", err)
					return
				}
			}(j)
		}

		wg.Wait()
		close(errors)

		// Report errors
		for err := range errors {
			b.Log(err)
		}

		duration := time.Since(startTime)
		b.ReportMetric(duration.Seconds(), "total_sec")
		b.ReportMetric(float64(duration.Milliseconds())/100.0, "ms/container")
	}
}

// BenchmarkLockContention measures lock contention with different patterns
func BenchmarkLockContention(b *testing.B) {
	mr, err := miniredis.Run()
	require.NoError(b, err)
	defer mr.Close()

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(b, err)

	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Minute * 5,
	}

	workerRepo := repo.NewWorkerRedisRepository(redisClient, config)

	// Create 10 workers
	workers := make([]*types.Worker, 10)
	for i := 0; i < 10; i++ {
		workers[i] = &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			FreeCpu:         10000,
			FreeMemory:      20480,
			TotalCpu:        10000,
			TotalMemory:     20480,
			Status:          types.WorkerStatusAvailable,
			ResourceVersion: 0,
		}
		err := workerRepo.AddWorker(workers[i])
		require.NoError(b, err)
	}

	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("seq-%d-%d", i, j),
					Cpu:         100,
					Memory:      256,
				}
				worker := workers[j%10]
				workerRepo.ScheduleContainerRequest(worker, request)
			}
		}
	})

	b.Run("Concurrent100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			for j := 0; j < 100; j++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					request := &types.ContainerRequest{
						ContainerId: fmt.Sprintf("conc-%d-%d", i, idx),
						Cpu:         100,
						Memory:      256,
					}
					worker := workers[idx%10]
					workerRepo.ScheduleContainerRequest(worker, request)
				}(j)
			}
			wg.Wait()
		}
	})
}
