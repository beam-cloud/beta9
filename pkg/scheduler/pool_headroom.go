package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// WorkerHoldsPoolHeadroom reports whether worker is one of the workers that
// together keep the pool's free capacity at its configured minimum
// (poolSizing.minFree*), and so must not idle out.
//
// The sizer adds a worker whenever free capacity is under the minimum, and an
// idle worker exits after its spindown timeout. Together those make the
// headroom worker cycle: it idles out, the sizer notices and starts another,
// and every request that lands in the ~4 s between pays a worker cold boot.
// The keepalive reply carries this so the workers holding the headroom stay.
//
// Which workers hold it is decided without coordination: ready workers are
// taken in id order and a worker holds headroom while the workers ahead of it
// do not cover the minimum on their own. Every worker evaluating the same
// pool state thus lands on the same set, and that set always covers the
// minimum when the pool does. Asking instead "is the pool still fine without
// me?" lets two idle workers each count the other and both leave at once.
//
// Only ready workers count as capacity here: a pending replacement is not yet
// able to take a container, so the incumbent must stay until it is.
//
// A failed worker lookup answers true: an idle worker that stays one keepalive
// longer costs nothing, while one that leaves on a bad read leaves the pool
// cold.
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
		return true
	}
	return freeCapacityAhead(workers, worker.Id).belowMinimum(sizing)
}

// freeCapacityAhead sums the free capacity of the ready workers in workers
// whose id orders before id (pending and disabled workers are left out).
func freeCapacityAhead(workers []*types.Worker, id string) *WorkerPoolCapacity {
	capacity := &WorkerPoolCapacity{}
	for _, w := range workers {
		if w.Id >= id || w.Status == types.WorkerStatusDisabled || w.Status == types.WorkerStatusPending {
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
