package scheduler

import (
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// A Kubernetes pool is stalled while a worker it added has been Pending for
// stalledAfter or longer: the cluster took the Job and has no node for it.
// A stalled pool is not asked for more workers (CanAddWorker), and requests
// stop waiting for the workers that have not started, so they move on to the
// next pool in their route. If such a worker does start, it is used like any
// other.
//
// The rule is opt-in per pool. Without stalledAfter a pool never stalls and
// its requests wait for it until the scheduling limit.

// stallCheckInterval is how long a pool's answer is reused. Every waiting
// request asks on every pass, and the answer changes over minutes.
const stallCheckInterval = time.Second

// workerStalled reports whether the worker has been Pending for stalledAfter
// or longer. A worker of unknown age is not stalled.
func workerStalled(worker *types.Worker, stalledAfter time.Duration, now time.Time) bool {
	if stalledAfter <= 0 || worker.Status != types.WorkerStatusPending || worker.CreatedAt == 0 {
		return false
	}
	return now.Sub(time.Unix(worker.CreatedAt, 0)) >= stalledAfter
}

// stallCheck remembers whether a pool was stalled when it was last looked at.
type stallCheck struct {
	mu        sync.Mutex
	checkedAt time.Time
	stalled   bool
}

// stalledAt returns the remembered answer, or asks look for a new one when
// the remembered answer is older than stallCheckInterval.
func (c *stallCheck) stalledAt(now time.Time, look func() (bool, error)) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.checkedAt.IsZero() && now.Sub(c.checkedAt) < stallCheckInterval {
		return c.stalled, nil
	}
	stalled, err := look()
	if err != nil {
		return false, err
	}
	c.checkedAt, c.stalled = now, stalled
	return stalled, nil
}

// stalled reports whether the pool has a worker that has been Pending for
// too long.
func (wpc *LocalKubernetesWorkerPoolController) stalled() (bool, error) {
	stalledAfter := time.Duration(wpc.workerPoolConfig.StalledAfter)
	if stalledAfter <= 0 {
		return false, nil
	}
	now := wpc.clock.Now()
	return wpc.stall.stalledAt(now, func() (bool, error) {
		workers, err := wpc.workerRepo.GetAllWorkersInPool(wpc.name)
		if err != nil {
			return false, err
		}
		return slices.ContainsFunc(workers, func(worker *types.Worker) bool {
			return workerStalled(worker, stalledAfter, now)
		}), nil
	})
}

// stalled reports whether the scheduler should stop waiting for the worker
// to start.
func (s *Scheduler) stalled(worker *types.Worker, now time.Time) bool {
	if worker.Status != types.WorkerStatusPending {
		return false
	}
	pool, ok := s.poolOf(worker)
	return ok && workerStalled(worker, time.Duration(pool.Config.StalledAfter), now)
}

// filterStalledWorkers drops the workers that are not capacity to wait for.
func (s *Scheduler) filterStalledWorkers(workers []*types.Worker) []*types.Worker {
	now := s.now()
	stalled := func(worker *types.Worker) bool { return s.stalled(worker, now) }
	if !slices.ContainsFunc(workers, stalled) {
		return workers
	}
	return slices.DeleteFunc(slices.Clone(workers), stalled)
}
