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
//
// A worker that is not available (cordoned, draining, or still pending) is
// never headroom: the scheduler will not place on it, so keeping it changes
// nothing for the pool and only pins a pod the operator asked to retire.
func WorkerHoldsPoolHeadroom(workerRepo repository.WorkerRepository, config types.AppConfig, worker *types.Worker) bool {
	if worker == nil || worker.PoolName == "" || worker.Status != types.WorkerStatusAvailable {
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

// filterWorkersByPoolHeadroom drops, for an opportunistic request, every worker
// of a pool whose ready free capacity would fall under its minFree* floor once
// the request is placed. Capacity here is already debited by earlier requests
// in the batch.
func (s *Scheduler) filterWorkersByPoolHeadroom(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if request == nil || !request.OpportunisticOnly {
		return workers
	}
	ready := map[string]*WorkerPoolCapacity{}
	for _, w := range workers {
		if w.Status != types.WorkerStatusAvailable {
			continue
		}
		c := ready[w.PoolName]
		if c == nil {
			c = &WorkerPoolCapacity{}
			ready[w.PoolName] = c
		}
		c.FreeCpu += w.FreeCpu
		c.FreeMemory += w.FreeMemory
		if w.Gpu != "" {
			c.FreeGpu += uint(w.FreeGpuCount)
		}
	}
	keep := map[string]bool{}
	for name, c := range ready {
		sizing := s.poolSizing(name)
		if sizing == nil {
			keep[name] = true
			continue
		}
		after := WorkerPoolCapacity{
			FreeCpu:    c.FreeCpu - request.Cpu,
			FreeMemory: c.FreeMemory - capacityMemoryForScheduling(request),
			FreeGpu:    c.FreeGpu - min(c.FreeGpu, uint(gpuCountForScheduling(request))),
		}
		keep[name] = !after.belowMinimum(sizing)
	}
	filtered := make([]*types.Worker, 0, len(workers))
	for _, w := range workers {
		if keep[w.PoolName] {
			filtered = append(filtered, w)
		}
	}
	return filtered
}

func (s *Scheduler) poolSizing(poolName string) *types.WorkerPoolSizingConfig {
	poolConfig, ok := s.config.Worker.Pools[poolName]
	if !ok {
		return nil
	}
	sizing, err := parsePoolSizingConfig(poolConfig.PoolSizing)
	if err != nil {
		return nil
	}
	applyBuildPoolSizingMinimums(poolName, s.config, sizing)
	return sizing
}
