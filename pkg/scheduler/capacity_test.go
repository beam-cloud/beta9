package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/tj/assert"
)

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
