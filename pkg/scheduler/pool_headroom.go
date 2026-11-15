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
func (s *Scheduler) WorkerHoldsPoolHeadroom(worker *types.Worker) bool {
	if s == nil || worker == nil {
		return false
	}
	pool, ok := s.poolOf(worker)
	if !ok {
		return false
	}
	return workerHoldsPoolHeadroom(s.workerRepo, pool.Config, worker)
}

func workerHoldsPoolHeadroom(workerRepo repository.WorkerRepository, pool types.WorkerPoolConfig, worker *types.Worker) bool {
	if worker == nil || worker.PoolName == "" || worker.Status != types.WorkerStatusAvailable || poolFloor(pool) == nil {
		return false
	}
	workers, err := workerRepo.GetAllWorkersInPool(worker.PoolName)
	if err != nil {
		return true
	}
	return HoldsHeadroom(pool, workers, worker)
}

// HoldsHeadroom is the rule of WorkerHoldsPoolHeadroom for a caller that has
// the pool's config and its workers at hand: it reports whether worker is one
// of the workers that keep the pool's free capacity at its floor.
func HoldsHeadroom(pool types.WorkerPoolConfig, workers []*types.Worker, worker *types.Worker) bool {
	if worker == nil || worker.Status != types.WorkerStatusAvailable {
		return false
	}
	floor := poolFloor(pool)
	if floor == nil {
		return false
	}
	return freeCapacityAhead(workers, worker.Id).belowMinimum(floor)
}

// poolFloor returns the free capacity the pool keeps (poolSizing.minFree*),
// or nil when it keeps none. It reads the config the pool runs with, so a
// pool created through the pools API has a floor like one from the config
// file.
func poolFloor(pool types.WorkerPoolConfig) *types.WorkerPoolSizingConfig {
	sizing, _ := readPoolSizing(pool)
	if sizing.MinFreeCpu <= 0 && sizing.MinFreeMemory <= 0 && sizing.MinFreeGpu <= 0 {
		return nil
	}
	return sizing
}

// poolOf returns the pool the worker belongs to. A workspace's own pool may
// share its name with a shared pool, so it is looked up first.
func (s *Scheduler) poolOf(worker *types.Worker) (*WorkerPool, bool) {
	if s.workerPoolManager == nil {
		return nil, false
	}
	name := workerPoolSelector(worker)
	if pool, ok := s.privateAgentPool(worker.WorkspaceId, name); ok {
		return pool, true
	}
	return s.workerPoolManager.GetPool(name)
}

// freeCapacityAhead sums the free capacity of the ready workers in workers
// whose id orders before id (pending and disabled workers are left out).
func freeCapacityAhead(workers []*types.Worker, id string) *WorkerPoolCapacity {
	capacity := &WorkerPoolCapacity{}
	for _, w := range workers {
		if w.Id >= id || w.Status == types.WorkerStatusDisabled || w.Status == types.WorkerStatusPending {
			continue
		}
		capacity.addFree(w)
	}
	return capacity
}

// belowMinimum reports whether the free capacity is under the pool's floor.
func (c *WorkerPoolCapacity) belowMinimum(sizing *types.WorkerPoolSizingConfig) bool {
	return c.FreeCpu < sizing.MinFreeCpu ||
		c.FreeMemory < sizing.MinFreeMemory ||
		(sizing.MinFreeGpu > 0 && c.FreeGpu < sizing.MinFreeGpu)
}

// withPending counts the capacity of the workers that are starting as free.
// The sizer does, so that it adds one worker for a shortfall and not one on
// every tick until the first has started.
func (c *WorkerPoolCapacity) withPending() *WorkerPoolCapacity {
	return &WorkerPoolCapacity{
		FreeCpu:    c.FreeCpu + c.PendingCpu,
		FreeMemory: c.FreeMemory + c.PendingMemory,
		FreeGpu:    c.FreeGpu + c.PendingGpu,
	}
}

// filterWorkersByPoolHeadroom drops, for an opportunistic request, every worker
// of a pool whose ready free capacity would fall under its minFree* floor once
// the request is placed. Capacity here is already debited by earlier requests
// in the batch. Spot also skips the workers holding the floor, which evict
// Spot work (see WorkerHoldsPoolHeadroom).
func (s *Scheduler) filterWorkersByPoolHeadroom(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if request == nil || !request.OpportunisticOnly {
		return workers
	}
	ready := map[string]*WorkerPoolCapacity{}
	pools := map[string][]*types.Worker{}
	for _, w := range workers {
		if w.Status != types.WorkerStatusAvailable {
			continue
		}
		pools[w.PoolName] = append(pools[w.PoolName], w)
		c := ready[w.PoolName]
		if c == nil {
			c = &WorkerPoolCapacity{}
			ready[w.PoolName] = c
		}
		c.addFree(w)
	}
	tier := request.CPUTier()
	keep := map[string]bool{}
	sizings := map[string]*types.WorkerPoolSizingConfig{}
	for name, c := range ready {
		var pool types.WorkerPoolConfig
		if loaded, ok := s.poolOf(pools[name][0]); ok {
			pool = loaded.Config
		}
		sizing := poolFloor(pool)
		if sizing == nil {
			keep[name] = true
			continue
		}
		sizings[name] = sizing
		after := WorkerPoolCapacity{
			FreeCpu:    c.FreeCpu - tier.CPUReservation(pool, request.Cpu),
			FreeMemory: c.FreeMemory - capacityMemoryForScheduling(request),
			FreeGpu:    c.FreeGpu - min(c.FreeGpu, uint(gpuCountForScheduling(request))),
		}
		keep[name] = !after.belowMinimum(sizing)
	}
	borrowed := tier.Borrows()
	filtered := make([]*types.Worker, 0, len(workers))
	for _, w := range workers {
		if !keep[w.PoolName] {
			continue
		}
		if sizing := sizings[w.PoolName]; borrowed && sizing != nil && w.Status == types.WorkerStatusAvailable && freeCapacityAhead(pools[w.PoolName], w.Id).belowMinimum(sizing) {
			continue
		}
		filtered = append(filtered, w)
	}
	return filtered
}
