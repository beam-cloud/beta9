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

func TestServerlessGPUAvailabilityExcludesPrivateAndProviderWorkers(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("serverless-t4", types.WorkerPoolConfig{GPUType: "T4"}, &LocalWorkerPoolControllerForTest{name: "serverless-t4"})
	manager.SetPool("managed-rtx5090", types.WorkerPoolConfig{GPUType: "RTX5090", Mode: types.PoolModeExternal}, &LocalWorkerPoolControllerForTest{name: "managed-rtx5090"})
	manager.SetPool("private-h100", types.WorkerPoolConfig{GPUType: "H100", Mode: types.PoolModePrivate}, &LocalWorkerPoolControllerForTest{
		name:             "private-h100",
		requiresSelector: true,
	})
	manager.SetPool("provider-a6000", types.WorkerPoolConfig{GPUType: "A6000", Mode: types.PoolModeProvider}, &LocalWorkerPoolControllerForTest{
		name: "provider-a6000",
	})
	scheduler := &Scheduler{workerPoolManager: manager}

	availability := scheduler.serverlessGPUAvailability([]*types.Worker{
		{Id: "serverless", PoolName: "serverless-t4", Status: types.WorkerStatusAvailable, Gpu: "T4", FreeGpuCount: 1},
		{Id: "managed", PoolName: "managed-rtx5090", WorkspaceId: "admin", ControlPlaneManaged: true, Status: types.WorkerStatusAvailable, Gpu: "RTX5090", FreeGpuCount: 1},
		{Id: "foreign-private", PoolName: "managed-rtx5090", WorkspaceId: "other-workspace", Status: types.WorkerStatusAvailable, Gpu: "L40S", FreeGpuCount: 1},
		{Id: "private", PoolName: "private-h100", WorkspaceId: "owner", RequiresPoolSelector: true, Status: types.WorkerStatusAvailable, Gpu: "H100", FreeGpuCount: 1},
		{Id: "provider", PoolName: "provider-a6000", WorkspaceId: "provider-workspace", Status: types.WorkerStatusAvailable, Gpu: "A6000", FreeGpuCount: 1},
		{Id: "busy", PoolName: "serverless-t4", Status: types.WorkerStatusAvailable, Gpu: "A10", FreeGpuCount: 0},
		{Id: "disabled", PoolName: "serverless-t4", Status: types.WorkerStatusDisabled, Gpu: "L4", FreeGpuCount: 1},
	})

	assert.True(t, availability["T4"])
	assert.True(t, availability["RTX5090"])
	assert.False(t, availability["L40S"])
	assert.False(t, availability["H100"])
	assert.False(t, availability["A6000"])
	assert.False(t, availability["A10"])
	assert.False(t, availability["L4"])
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

// A GPU that an evictable container holds is room for one replica, not for
// every replica after the first.
func TestCheckCapacityCountsReclaimableRoomOnce(t *testing.T) {
	s := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	s.workerPoolManager.SetPool("beta9-a10g", types.WorkerPoolConfig{GPUType: "A10G"}, nil)
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1}

	fits := func(replicas uint) bool {
		check := capacityCheck{
			scheduler: s, request: request, replicas: replicas,
			workers: []*types.Worker{evictableWorker("worker", "beta9-a10g", 0, 1)},
		}
		return check.placeReplicas().CanSchedule
	}
	assert.True(t, fits(1))
	assert.False(t, fits(2))
}

func TestServerlessAvailabilityIncludesOnlyReclaimableHostedGPUs(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("test", types.WorkerPoolConfig{GPUType: "RTX5090"}, &LocalWorkerPoolControllerForTest{name: "test"})
	s := &Scheduler{workerPoolManager: manager}
	worker := &types.Worker{Id: "hosted", PoolName: "test", Status: types.WorkerStatusAvailable,
		Gpu: "RTX5090", TotalGpuCount: 1, FreeGpuCount: 0, EvictableGpuCount: 1}
	assert.True(t, s.serverlessGPUAvailability([]*types.Worker{worker})["RTX5090"])
	worker.EvictableGpuCount = 0
	assert.False(t, s.serverlessGPUAvailability([]*types.Worker{worker})["RTX5090"], "protected models cannot supply serverless capacity")
	worker.EvictableGpuCount = 1
	worker.Status = types.WorkerStatusDisabled
	assert.False(t, s.serverlessGPUAvailability([]*types.Worker{worker})["RTX5090"], "disabled workers stay unavailable")
}
