package scheduler

import (
	"testing"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type countingBatchContainerRepository struct {
	repo.ContainerRepository
	batchReads  int
	singleReads int
}

func (r *countingBatchContainerRepository) GetContainerState(containerID string) (*types.ContainerState, error) {
	r.singleReads++
	return r.ContainerRepository.GetContainerState(containerID)
}

func (r *countingBatchContainerRepository) GetContainerStatuses(containerIDs []string) (map[string]types.ContainerStatus, map[string]error) {
	r.batchReads++
	return r.ContainerRepository.(*repo.ContainerRedisRepository).GetContainerStatuses(containerIDs)
}

func TestSchedulingBatchPreloadsContainerStates(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	if err != nil {
		t.Fatal(err)
	}

	requests := []*types.ContainerRequest{
		{ContainerId: "pending-1", Cpu: 100, Memory: 128, Timestamp: time.Now()},
		{ContainerId: "pending-2", Cpu: 100, Memory: 128, Timestamp: time.Now()},
		{ContainerId: "stopping", Cpu: 100, Memory: 128, Timestamp: time.Now()},
	}
	for i, request := range requests {
		status := types.ContainerStatusPending
		if i == len(requests)-1 {
			status = types.ContainerStatusStopping
		}
		if err := scheduler.containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
			ContainerId: request.ContainerId,
			Status:      status,
		}); err != nil {
			t.Fatal(err)
		}
	}

	counting := &countingBatchContainerRepository{ContainerRepository: scheduler.containerRepo}
	scheduler.containerRepo = counting
	worker := &types.Worker{
		Id:          "worker",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    1000,
		FreeCpu:     1000,
		TotalMemory: 1024,
		FreeMemory:  1024,
	}
	batch := newSchedulingBatch(scheduler, []*types.Worker{worker}, len(requests))
	batch.plan(requests)

	if counting.batchReads != 1 || counting.singleReads != 0 {
		t.Fatalf("batch reads=%d single reads=%d, want 1 and 0", counting.batchReads, counting.singleReads)
	}
	if len(batch.schedules) != 2 {
		t.Fatalf("planned %d requests, want 2", len(batch.schedules))
	}
}
