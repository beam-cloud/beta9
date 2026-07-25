package scheduler

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
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

type recordingBatchWorkerRepository struct {
	repo.WorkerRepository

	mu         sync.Mutex
	batchSizes []int
}

func (r *recordingBatchWorkerRepository) ScheduleContainerRequests(worker *types.Worker, requests []*types.ContainerRequest) error {
	r.mu.Lock()
	r.batchSizes = append(r.batchSizes, len(requests))
	r.mu.Unlock()
	return r.WorkerRepository.ScheduleContainerRequests(worker, requests)
}

func (r *recordingBatchWorkerRepository) batches() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.batchSizes...)
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

func TestSchedulingBatchStatusReadErrorOnlyRetriesAffectedRequest(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	if err != nil {
		t.Fatal(err)
	}

	containerRepo := scheduler.containerRepo
	countingRepo := &countingBatchContainerRepository{ContainerRepository: containerRepo}
	scheduler.containerRepo = countingRepo

	healthy := &types.ContainerRequest{
		ContainerId: "healthy-container",
		Cpu:         100,
		Memory:      128,
		RetryCount:  firstSchedulingAttemptRetryCount,
		Timestamp:   time.Now(),
	}
	corrupt := &types.ContainerRequest{
		ContainerId: "corrupt-container",
		Cpu:         100,
		Memory:      128,
		RetryCount:  firstSchedulingAttemptRetryCount,
		Timestamp:   time.Now(),
	}
	if err := containerRepo.SetContainerState(healthy.ContainerId, &types.ContainerState{
		ContainerId: healthy.ContainerId,
		Status:      types.ContainerStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.requestBacklog.rdb.Set(
		context.Background(),
		common.RedisKeys.SchedulerContainerState(corrupt.ContainerId),
		"not-a-hash",
		0,
	).Err(); err != nil {
		t.Fatal(err)
	}

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

	batch := newSchedulingBatch(scheduler, []*types.Worker{worker}, 2)
	batch.plan([]*types.ContainerRequest{corrupt, healthy})
	batch.dispatch()

	if countingRepo.batchReads != 1 {
		t.Fatalf("expected one batch state read, got %d", countingRepo.batchReads)
	}
	if countingRepo.singleReads != 1 {
		t.Fatalf("expected one fallback state read for corrupt request, got %d", countingRepo.singleReads)
	}
	if len(batch.schedules) != 1 || batch.schedules[0].request.ContainerId != healthy.ContainerId {
		t.Fatalf("healthy request was not planned independently: %+v", batch.schedules)
	}

	healthyState, err := containerRepo.GetContainerState(healthy.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if healthyState.WorkerId != worker.Id {
		t.Fatalf("healthy request assigned to worker %q, want %q", healthyState.WorkerId, worker.Id)
	}

	time.Sleep(requestProcessingInterval + 10*time.Millisecond)
	requeued, err := scheduler.requestBacklog.Pop()
	if err != nil {
		t.Fatal(err)
	}
	if requeued.ContainerId != corrupt.ContainerId {
		t.Fatalf("requeued container = %q, want %q", requeued.ContainerId, corrupt.ContainerId)
	}
	if requeued.RetryCount != firstSchedulingAttemptRetryCount+1 {
		t.Fatalf("requeued retry count = %d, want %d", requeued.RetryCount, firstSchedulingAttemptRetryCount+1)
	}
}

func TestSchedulingBatchRecursivelySplitsNotPendingDispatches(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	if err != nil {
		t.Fatal(err)
	}

	workerRepo := &recordingBatchWorkerRepository{WorkerRepository: scheduler.workerRepo}
	scheduler.workerRepo = workerRepo
	worker := &types.Worker{
		Id:          "worker-1",
		PoolName:    "default",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    10_000,
		FreeCpu:     10_000,
		TotalMemory: 10_000,
		FreeMemory:  10_000,
	}
	if err := scheduler.workerRepo.AddWorker(worker); err != nil {
		t.Fatal(err)
	}

	const requestCount = 8
	requests := make([]*types.ContainerRequest, 0, requestCount)
	for index := 0; index < requestCount; index++ {
		request := &types.ContainerRequest{
			ContainerId: "container-" + string(rune('a'+index)),
			Cpu:         100,
			Memory:      128,
			RetryCount:  firstSchedulingAttemptRetryCount,
			Timestamp:   time.Now(),
		}
		requests = append(requests, request)
		if err := scheduler.containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
			ContainerId: request.ContainerId,
			Status:      types.ContainerStatusPending,
		}); err != nil {
			t.Fatal(err)
		}
	}

	batch := newSchedulingBatch(scheduler, []*types.Worker{worker}, len(requests))
	batch.plan(requests)
	if len(batch.schedules) != len(requests) {
		t.Fatalf("planned %d requests, want %d", len(batch.schedules), len(requests))
	}
	if err := scheduler.containerRepo.UpdateContainerStatus(
		requests[0].ContainerId,
		types.ContainerStatusStopping,
		types.ContainerStateTtlSWhilePending,
	); err != nil {
		t.Fatal(err)
	}

	batch.dispatch()

	wantBatchSizes := []int{8, 4, 2, 1, 1, 2, 4}
	if got := workerRepo.batches(); !reflect.DeepEqual(got, wantBatchSizes) {
		t.Fatalf("dispatch batch sizes = %v, want %v", got, wantBatchSizes)
	}

	queued, err := scheduler.workerRepo.GetNextContainerRequests(worker.Id, requestCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != requestCount-1 {
		t.Fatalf("queued %d healthy requests, want %d", len(queued), requestCount-1)
	}
	for _, request := range queued {
		if request.ContainerId == requests[0].ContainerId {
			t.Fatal("non-pending request was queued")
		}
	}
}
