package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// WorkerHoldsPoolHeadroom reports whether the pool's free capacity would fall
// below its configured minimum (poolSizing.minFree*) if worker went away.
//
// The sizer adds a worker whenever free capacity is under the minimum, and an
// idle worker exits after its spindown timeout. Together those make the
// headroom worker cycle: it idles out, the sizer notices and starts another,
// and every request that lands in the ~4 s between pays a worker cold boot.
// The keepalive reply carries this so the worker holding the headroom stays.
//
// Only ready workers count as capacity here: a pending replacement is not yet
// able to take a container, so the incumbent must stay until it is.
func WorkerHoldsPoolHeadroom(workerRepo repository.WorkerRepository, config types.AppConfig, worker *types.Worker) bool {
	if worker == nil || worker.PoolName == "" {
		return false
	}
	poolConfig, ok := config.Worker.Pools[worker.PoolName]
	if !ok {
		return false
	}
	sizing, err := parsePoolSizingConfig(poolConfig.PoolSizing)
	if err != nil || sizing == nil {
		return false
	}
	applyBuildPoolSizingMinimums(worker.PoolName, config, sizing)
	if sizing.MinFreeCpu <= 0 && sizing.MinFreeMemory <= 0 && sizing.MinFreeGpu <= 0 {
		return false
	}

	workers, err := workerRepo.GetAllWorkersInPool(worker.PoolName)
	if err != nil {
		return false
	}
	return freeCapacityWithout(workers, worker.Id).belowMinimum(sizing)
}

// freeCapacityWithout sums the free capacity of the ready workers in workers,
// leaving out the one with id excluded (and pending or disabled workers).
func freeCapacityWithout(workers []*types.Worker, excluded string) *WorkerPoolCapacity {
	capacity := &WorkerPoolCapacity{}
	for _, w := range workers {
		if w.Id == excluded || w.Status == types.WorkerStatusDisabled || w.Status == types.WorkerStatusPending {
			continue
		}
		capacity.FreeCpu += w.FreeCpu
		capacity.FreeMemory += w.FreeMemory
		if w.Gpu != "" && w.FreeCpu > 0 && w.FreeMemory > 0 {
			capacity.FreeGpu += uint(w.FreeGpuCount)
		}
	}
	return capacity
}

func (c *WorkerPoolCapacity) belowMinimum(sizing *types.WorkerPoolSizingConfig) bool {
	return c.FreeCpu < sizing.MinFreeCpu ||
		c.FreeMemory < sizing.MinFreeMemory ||
		(sizing.MinFreeGpu > 0 && c.FreeGpu < sizing.MinFreeGpu)
}
