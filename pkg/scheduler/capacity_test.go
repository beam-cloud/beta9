package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/tj/assert"
)

func TestNormalizeManagedAgentPoolDoesNotRequireSelector(t *testing.T) {
	config := normalizeAgentWorkerPoolConfig(&compute.PoolState{
		ManagementSource: types.WorkerPoolManagementSourceAPI,
		WorkerConfig: &types.WorkerPoolConfig{
			Mode:                 types.PoolModeExternal,
			GPUType:              "RTX5090",
			RequiresPoolSelector: true,
		},
	})

	assert.False(t, config.RequiresPoolSelector)
}

func TestHasManagedPoolForGPU(t *testing.T) {
	manager := NewWorkerPoolManager(false)
	manager.SetPool("beta9-t4", types.WorkerPoolConfig{GPUType: "T4"}, &LocalWorkerPoolControllerForTest{name: "beta9-t4"})
	manager.SetPool("private-h100", types.WorkerPoolConfig{GPUType: "H100", Mode: types.PoolModePrivate}, &LocalWorkerPoolControllerForTest{
		name:             "private-h100",
		mode:             types.PoolModePrivate,
		requiresSelector: true,
	})
	manager.SetPool("marketplace-a6000", types.WorkerPoolConfig{GPUType: "A6000", Mode: types.PoolModeMarketplace}, &LocalWorkerPoolControllerForTest{
		name: "marketplace-a6000",
		mode: types.PoolModeMarketplace,
	})
	scheduler := &Scheduler{workerPoolManager: manager}

	// Pool-config-based: a pool with zero live workers still counts.
	assert.True(t, scheduler.HasManagedPoolForGPU("T4", false))

	// Pools requiring a pool selector can't serve selector-less workloads.
	assert.False(t, scheduler.HasManagedPoolForGPU("H100", false))

	// Marketplace pools only count when the workload opted in.
	assert.False(t, scheduler.HasManagedPoolForGPU("A6000", false))
	assert.True(t, scheduler.HasManagedPoolForGPU("A6000", true))

	// GPU_ANY matches any GPU pool usable without a selector.
	assert.True(t, scheduler.HasManagedPoolForGPU(string(types.GPU_ANY), false))

	// Unknown GPU type: guaranteed blackhole.
	assert.False(t, scheduler.HasManagedPoolForGPU("B200", false))
}

func TestCheckCapacityRestoresPaddedMemoryForReplacement(t *testing.T) {
	s, err := NewSchedulerForTest()
	assert.NoError(t, err)

	err = s.workerRepo.AddWorker(&types.Worker{
		Id:          "worker-1",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    2000,
		FreeCpu:     1000,
		TotalMemory: 250,
		FreeMemory:  125,
	})
	assert.NoError(t, err)

	result := s.CheckCapacity(
		&types.ContainerRequest{ContainerId: uuid.New().String(), Cpu: 1000, Memory: 200},
		1,
		"old-stub",
		[]types.ContainerState{{
			StubId:   "old-stub",
			WorkerId: "worker-1",
			Cpu:      1000,
			Memory:   100,
		}},
	)

	assert.True(t, result.CanSchedule)
}

func TestCheckCapacityRestoresDefaultSingleGPUForReplacement(t *testing.T) {
	s, err := NewSchedulerForTest()
	assert.NoError(t, err)

	err = s.workerRepo.AddWorker(&types.Worker{
		Id:            "worker-1",
		PoolName:      "beta9-t4",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      2000,
		FreeCpu:       1000,
		TotalMemory:   4096,
		FreeMemory:    2048,
		Gpu:           "T4",
		TotalGpuCount: 1,
		FreeGpuCount:  0,
	})
	assert.NoError(t, err)

	result := s.CheckCapacity(
		&types.ContainerRequest{ContainerId: uuid.New().String(), Cpu: 1000, Memory: 512, Gpu: "T4"},
		1,
		"old-stub",
		[]types.ContainerState{{
			StubId:   "old-stub",
			WorkerId: "worker-1",
			Cpu:      1000,
			Memory:   512,
			Gpu:      "T4",
		}},
	)

	assert.True(t, result.CanSchedule)
}
