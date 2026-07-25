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

func (r *countingBatchContainerRepository) GetContainerState(containerId string) (*types.ContainerState, error) {
	r.singleReads++
	return r.ContainerRepository.GetContainerState(containerId)
}

func (r *countingBatchContainerRepository) GetContainerStatuses(containerIds []string) (map[string]types.ContainerStatus, map[string]error) {
	r.batchReads++
	return r.ContainerRepository.(*repo.ContainerRedisRepository).GetContainerStatuses(containerIds)
}

func TestSchedulingBatchPreloadsContainerStates(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	if err != nil {
		t.Fatal(err)
	}

	pendingCanceled := &types.ContainerRequest{
		ContainerId: "pending-canceled-container",
		Cpu:         100,
		Memory:      128,
		Timestamp:   time.Now(),
	}
	pendingHealthy := &types.ContainerRequest{
		ContainerId: "pending-healthy-container",
		Cpu:         100,
		Memory:      128,
		Timestamp:   time.Now(),
	}
	stopping := &types.ContainerRequest{
		ContainerId: "stopping-container",
		Cpu:         100,
		Memory:      128,
		Timestamp:   time.Now(),
	}
	if err := scheduler.containerRepo.SetContainerState(pendingCanceled.ContainerId, &types.ContainerState{
		ContainerId: pendingCanceled.ContainerId,
		Status:      types.ContainerStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.containerRepo.SetContainerState(pendingHealthy.ContainerId, &types.ContainerState{
		ContainerId: pendingHealthy.ContainerId,
		Status:      types.ContainerStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.containerRepo.SetContainerState(stopping.ContainerId, &types.ContainerState{
		ContainerId: stopping.ContainerId,
		Status:      types.ContainerStatusStopping,
	}); err != nil {
		t.Fatal(err)
	}

	containerRepo := scheduler.containerRepo
	countingRepo := &countingBatchContainerRepository{ContainerRepository: containerRepo}
	scheduler.containerRepo = countingRepo
	worker := &types.Worker{
		Id:          "worker-1",
		PoolName:    "default",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    1000,
		FreeCpu:     1000,
		TotalMemory: 1024,
		FreeMemory:  1024,
	}
	if err := scheduler.workerRepo.AddWorker(worker); err != nil {
		t.Fatal(err)
	}
	batch := newSchedulingBatch(scheduler, []*types.Worker{worker}, 3)
	batch.plan([]*types.ContainerRequest{
		pendingCanceled,
		pendingHealthy,
		stopping,
		{ContainerId: "missing-container", Cpu: 100, Memory: 128, Timestamp: time.Now()},
	})

	if countingRepo.batchReads != 1 {
		t.Fatalf("expected one batch state read, got %d", countingRepo.batchReads)
	}
	if countingRepo.singleReads != 0 {
		t.Fatalf("expected no per-request state reads, got %d", countingRepo.singleReads)
	}
	if len(batch.schedules) != 2 {
		t.Fatalf("expected both pending requests to be planned, got %+v", batch.schedules)
	}

	// A stop racing after the preload must still win at the atomic assignment
	// script; the snapshot only avoids redundant planning reads.
	if err := containerRepo.UpdateContainerStatus(
		pendingCanceled.ContainerId,
		types.ContainerStatusStopping,
		types.ContainerStateTtlSWhilePending,
	); err != nil {
		t.Fatal(err)
	}
	batch.dispatch()

	canceledState, err := containerRepo.GetContainerState(pendingCanceled.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if canceledState.WorkerId != "" {
		t.Fatalf("stopped request was assigned to worker %q", canceledState.WorkerId)
	}
	healthyState, err := containerRepo.GetContainerState(pendingHealthy.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if healthyState.WorkerId != worker.Id {
		t.Fatalf("healthy sibling was assigned to worker %q, want %q", healthyState.WorkerId, worker.Id)
	}
	updatedWorker, err := scheduler.workerRepo.GetWorkerById(worker.Id)
	if err != nil {
		t.Fatal(err)
	}
	if updatedWorker.FreeCpu != worker.TotalCpu-pendingHealthy.Cpu ||
		updatedWorker.FreeMemory != worker.TotalMemory-capacityMemoryForScheduling(pendingHealthy) {
		t.Fatalf("healthy assignment used incorrect worker capacity: %+v", updatedWorker)
	}
}
