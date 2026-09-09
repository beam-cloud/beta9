package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func evictableWorker(id, poolName string, free, evictable uint32) *types.Worker {
	return &types.Worker{
		Id:                id,
		PoolName:          poolName,
		Gpu:               "A10G",
		Status:            types.WorkerStatusAvailable,
		TotalCpu:          8000,
		TotalMemory:       16000,
		TotalGpuCount:     free + evictable,
		FreeCpu:           4000,
		FreeMemory:        8000,
		FreeGpuCount:      free,
		EvictableCpu:      int64(evictable) * 1000,
		EvictableMemory:   int64(evictable) * 1000,
		EvictableGpuCount: evictable,
	}
}

func TestSchedulableCapacityIncludesEvictableOnlyForRequestsThatMayEvict(t *testing.T) {
	worker := evictableWorker("w", "beta9-a10g", 0, 2)

	serverless := &types.ContainerRequest{Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1}
	cpu, memory, gpu := schedulableCapacity(worker, serverless)
	assert.Equal(t, int64(6000), cpu)
	assert.Equal(t, int64(10000), memory)
	assert.Equal(t, uint32(2), gpu)
	assert.Equal(t, int32(1), evictionRankForWorker(worker, serverless))

	for _, request := range []*types.ContainerRequest{
		{Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1, Evictable: true, OpportunisticOnly: true},
		{Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1, OpportunisticOnly: true},
	} {
		cpu, memory, gpu := schedulableCapacity(worker, request)
		assert.Equal(t, int64(4000), cpu)
		assert.Equal(t, int64(8000), memory)
		assert.Equal(t, uint32(0), gpu)
		assert.Equal(t, int32(0), evictionRankForWorker(worker, request))
	}

	assert.Equal(t, int32(0), evictionRankForWorker(evictableWorker("idle", "beta9-a10g", 1, 1), serverless))
}

func TestSelectWorkerPrefersIdleCapacityOverEviction(t *testing.T) {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	scheduler.workerPoolManager.SetPool("beta9-a10g", types.WorkerPoolConfig{GPUType: "A10G"}, nil)

	// The full worker would win best-fit on free capacity alone; eviction rank
	// must send the request to the worker that has a GPU actually free.
	full := evictableWorker("full", "beta9-a10g", 0, 2)
	idle := evictableWorker("idle", "beta9-a10g", 1, 1)
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1}

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{full, idle}, request)
	assert.NoError(t, err)
	assert.Equal(t, "idle", worker.Id)

	// With nothing idle anywhere, evictable capacity is still schedulable.
	worker, err = scheduler.selectWorkerFromWorkers([]*types.Worker{full}, request)
	assert.NoError(t, err)
	assert.Equal(t, "full", worker.Id)

	// An opportunistic replica never sees evictable capacity.
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{full}, &types.ContainerRequest{
		Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1, Evictable: true, OpportunisticOnly: true,
	})
	assert.Error(t, err)
}

func TestReserveWorkerCapacityDrawsEvictableForRequestsThatMayEvict(t *testing.T) {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}

	// A CPU worker shaped like a local pool node: 500m idle, 1000m held by an
	// evictable replica. A 1 CPU serverless pod fits only by evicting.
	worker := &types.Worker{
		Id: "cpu", PoolName: "default", Status: types.WorkerStatusAvailable,
		TotalCpu: 2500, TotalMemory: 4096,
		FreeCpu: 500, FreeMemory: 2816,
		EvictableCpu: 1000, EvictableMemory: 640,
	}
	serverless := &types.ContainerRequest{Cpu: 1000, Memory: 256}
	assert.True(t, scheduler.reserveWorkerCapacity(worker, serverless))
	assert.Equal(t, int64(0), worker.FreeCpu)
	assert.Equal(t, int64(500), worker.EvictableCpu)
	assert.Equal(t, int64(2496), worker.FreeMemory)
	assert.Equal(t, int64(640), worker.EvictableMemory)

	// Whatever is left is spoken for: the next request in the batch that
	// needs more than the remaining evictable share must go elsewhere.
	assert.False(t, scheduler.reserveWorkerCapacity(worker, &types.ContainerRequest{Cpu: 1000, Memory: 256}))
	assert.True(t, scheduler.reserveWorkerCapacity(worker, &types.ContainerRequest{Cpu: 500, Memory: 256}))
	assert.Equal(t, int64(0), worker.EvictableCpu)

	// Opportunistic and evictable requests only ever see idle capacity.
	idleOnly := evictableWorker("gpu", "beta9-a10g", 0, 2)
	assert.False(t, scheduler.reserveWorkerCapacity(idleOnly, &types.ContainerRequest{
		Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1, Evictable: true, OpportunisticOnly: true,
	}))
	assert.Equal(t, uint32(2), idleOnly.EvictableGpuCount)
	assert.True(t, scheduler.reserveWorkerCapacity(idleOnly, &types.ContainerRequest{
		Cpu: 1000, Memory: 1000, GpuRequest: []string{"A10G"}, GpuCount: 1,
	}))
	assert.Equal(t, uint32(0), idleOnly.FreeGpuCount)
	assert.Equal(t, uint32(1), idleOnly.EvictableGpuCount)
}

func TestOpportunisticRequestFailsFastWithoutIdleCapacity(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:        scheduler.ctx,
		name:       "beta9-a10g",
		config:     scheduler.config,
		workerRepo: scheduler.workerRepo,
	}
	scheduler.workerPoolManager.SetPool("beta9-a10g", types.WorkerPoolConfig{GPUType: "A10G"}, controller)

	request := &types.ContainerRequest{
		Gpu:               "A10G",
		GpuCount:          1,
		Cpu:               1000,
		Memory:            1000,
		Evictable:         true,
		OpportunisticOnly: true,
		Timestamp:         time.Now(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, nil).run()

	// No worker was provisioned, nothing was queued, and the request is
	// marked failed so the controller can back off.
	assert.Equal(t, 0, controller.AddWorkerCallCount())
	assert.Equal(t, int64(0), scheduler.requestBacklog.Len())
	status, err := scheduler.containerRepo.GetContainerRequestStatus(request.ContainerId)
	assert.NoError(t, err)
	assert.Equal(t, types.ContainerRequestStatusFailed, status)
	_, err = scheduler.containerRepo.GetContainerState(request.ContainerId)
	assert.Error(t, err)
}

func TestFreePoolCapacityCountsEvictableAsFree(t *testing.T) {
	held := evictableWorker("held", "beta9-a10g", 0, 2)
	held.FreeCpu, held.FreeMemory = 0, 0
	held.EvictableCpu, held.EvictableMemory = 8000, 16000
	pending := evictableWorker("pending", "beta9-a10g", 1, 0)
	pending.Status = types.WorkerStatusPending

	capacity := poolCapacityFromWorkers([]*types.Worker{held, pending})
	assert.Equal(t, uint(2), capacity.FreeGpu)
	assert.Equal(t, int64(8000), capacity.FreeCpu)
	assert.Equal(t, int64(16000), capacity.FreeMemory)
	assert.Equal(t, uint(1), capacity.PendingGpu)
}
