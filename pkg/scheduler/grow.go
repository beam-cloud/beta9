package scheduler

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog/log"
)

// Growing a pool.
//
// When no worker in a request's route has room, and none that is starting
// will have, one pool adds a worker: the first in route order that can.
//
// A pool that says it cannot (CanAddWorker) costs the request nothing, and
// the next pool is asked in the same pass. A pool whose AddWorker fails is
// left alone for growRetryDelay. A request asks any one pool for at most
// maxGrowAttempts workers. Once it has asked every pool in its route that
// many times it fails; until then it waits, and is planned again with every
// other request that waits, because a container that finishes leaves room
// for it too.

const (
	// maxGrowAttempts is how many workers one request may ask one pool for.
	// The count is per pool, so a long route of pools that fail to add a
	// worker does not use it up before the pools further down are asked.
	maxGrowAttempts = 3
	// growRetryDelay is how long a pool is left alone after AddWorker failed.
	growRetryDelay = 5 * time.Second
)

// workerSize is the capacity a pool is asked to add.
type workerSize struct {
	cpu      int64
	memory   int64
	gpuCount uint32
}

// workerSizeFor returns the size of the worker a pool should add for the
// request. An agent pool binds a machine that has joined or buys one, so it
// is asked for what the request needs and answers with the machine it has. A
// Kubernetes pool starts a worker pod, which is at least the default size so
// it can take more than one container.
func (s *Scheduler) workerSizeFor(pool *WorkerPool, request *types.ContainerRequest) workerSize {
	size := workerSize{
		cpu:      request.Cpu,
		memory:   capacityMemoryForScheduling(request),
		gpuCount: gpuCountForScheduling(request),
	}
	if pool.Config.AgentHosted() {
		return size
	}

	sizing, _ := readPoolSizing(pool.Config)
	return workerSize{
		cpu:      max(size.cpu, s.config.Worker.DefaultWorkerCPURequest, sizing.DefaultWorkerCpu),
		memory:   max(size.memory, s.config.Worker.DefaultWorkerMemoryRequest, sizing.DefaultWorkerMemory),
		gpuCount: max(size.gpuCount, sizing.DefaultWorkerGpuCount),
	}
}

// growth is the decision for one request: which pool adds a worker for it.
type growth struct {
	// pool is the pool to ask. Nil means no pool can add a worker now.
	pool *WorkerPool
	size workerSize
	// exhausted means the request has asked every pool in its route
	// maxGrowAttempts times, so waiting cannot help it.
	exhausted bool
}

// chooseGrowth walks the route in order and picks the first pool that the
// request may still ask, that did not just fail, and that says it can add a
// worker. It changes nothing.
func (s *Scheduler) chooseGrowth(route route, request *types.ContainerRequest) growth {
	exhausted := true
	for _, candidate := range route.pools {
		pool := candidate.WorkerPool
		if request.GrowAttempts[pool.Name] >= maxGrowAttempts {
			continue
		}
		exhausted = false

		if s.workerProvisioningBackoff.leftAlone(pool.Name, s.now()) {
			continue
		}

		size := s.workerSizeFor(pool, request)
		ok, err := s.canAddWorker(pool, request, size)
		if err != nil {
			requestLog(log.Warn(), request).
				Str("pool_name", pool.Name).
				Err(err).
				Msg("unable to ask pool whether it can add a worker")
			continue
		}
		if ok {
			return growth{pool: pool, size: size}
		}
	}
	return growth{exhausted: exhausted}
}

// PoolOutlook is what the managed endpoint controller weighs before it
// places a replica that needs this much room in the pool: whether the pool
// would add a worker for it now and, where it buys machines, how long one
// takes to come up and how often an hour they are taken back.
func (s *Scheduler) PoolOutlook(name string, cpu, memory int64, gpuCount uint32) MachineOutlook {
	pool, ok := s.workerPoolManager.GetPool(name)
	if !ok {
		return MachineOutlook{}
	}
	if buying := buyer(pool.Controller); buying != nil {
		need := buying.machineNeed(cpu, memory, gpuCount)
		need.HoursLost = replicaHoursLost
		return buying.provisioner.Expect(buying.ctx, buying.poolName(), need)
	}
	can, err := pool.Controller.CanAddWorker(cpu, memory, gpuCount)
	return MachineOutlook{CanBuy: err == nil && can}
}

// canGrow reports whether some pool would add a worker for the request now.
func (a *schedulingAttempt) canGrow() bool {
	route, err := a.scheduler.growthRouteFor(a.request)
	return err == nil && a.scheduler.chooseGrowth(route, a.request).pool != nil
}

// grow asks one pool in the request's route for a worker, then requeues the
// request to wait for it. The request fails when no pool accepts it or it
// has asked every pool as often as it may.
func (a *schedulingAttempt) grow() {
	s := a.scheduler

	route, err := s.growthRouteFor(a.request)
	switch {
	case errors.Is(err, errNoPoolForRequest):
		requestLog(log.Error(), a.request).Err(err).Msg("no pool found for request")
		a.fail(types.ContainerSchedulingFailureNoController)
		return
	case err != nil:
		// The pool the request names could not be read. That says nothing
		// about the request.
		requestLog(log.Error(), a.request).Err(err).Msg("unable to look up the pool for request")
		a.retry("pool_lookup_failed")
		return
	}

	choice := s.chooseGrowth(route, a.request)
	switch {
	case choice.exhausted, choice.pool == nil && isManagedRequest(a.request) && !s.machineComing(route):
		// A managed endpoint's replica does not wait for a pool that has no
		// machine coming and can buy none: its controller places it again,
		// in a pool that can.
		a.fail(types.ContainerSchedulingFailureProvisioningLimit)
		return
	case choice.pool == nil:
		a.askAgainLater("worker_provisioning_backoff")
		return
	case buyer(choice.pool.Controller) != nil:
		// Such a pool buys for all the requests that wait for it together
		// (see pool_buying.go).
		s.joinWaitlist(a.request, choice.pool)
		a.askAgainLater("worker_provisioning_in_progress")
		return
	}

	// One worker at a time per container across scheduler replicas. A
	// successful attempt keeps the lock for as long as the request waits for
	// the worker, so a replica that has not yet seen the new worker cannot
	// add a second one.
	lockKey := common.RedisKeys.SchedulerWorkerProvisioningLock(a.request.ContainerId)
	if err := s.workerProvisioningLock.Acquire(s.ctx, lockKey, common.RedisLockOptions{TtlS: int(growLockTTL(choice.pool).Seconds())}); err != nil {
		if !errors.Is(err, redislock.ErrNotObtained) {
			requestLog(log.Error(), a.request).Err(err).Msg("unable to acquire worker provisioning lock")
		}
		a.askAgainLater("worker_provisioning_in_progress")
		return
	}

	metrics.RecordSchedulerWorkerWait(s.since(a.request.Timestamp), a.request, "no_worker")

	if a.request.GrowAttempts == nil {
		a.request.GrowAttempts = map[string]int{}
	}
	a.request.GrowAttempts[choice.pool.Name]++
	reservationID := s.provisioning.addReservation(s, a.request, choice.pool, choice.size)
	s.background(func() { s.addWorker(a.request, choice.pool, choice.size, reservationID) })
	a.requeueForWorkerWait()
}

// machineComing reports whether a pool of the route has bought a machine
// that has yet to run a worker. Between its purchase and its Pending worker
// a request finds neither a worker to wait for nor a pool that can buy. A
// pool that cannot be asked is taken to have one coming.
func (s *Scheduler) machineComing(route route) bool {
	return slices.ContainsFunc(route.pools, func(pool routedPool) bool {
		buying := buyer(pool.Controller)
		if buying == nil {
			return false
		}
		booting, err := buying.provisioner.Booting(buying.ctx, buying.poolName())
		return err != nil || len(booting) > 0
	})
}

// askAgainLater requeues a request that no pool could be asked for a worker
// for, to ask again on a later pass.
func (a *schedulingAttempt) askAgainLater(reason string) {
	a.recordBacklogWait(false, reason)
	a.requeueForWorkerWaitDelay(provisioningWorkerRequeueDelay, reason)
}

// growLockTTL is how long a request waits for the worker it asked the pool
// for before it may ask for another: until its reservation on the worker
// expires, or until the pool calls the worker stalled if that comes first.
func growLockTTL(pool *WorkerPool) time.Duration {
	if stalledAfter := time.Duration(pool.Config.StalledAfter); stalledAfter > 0 && stalledAfter < pendingWorkerReservationTTL {
		return stalledAfter
	}
	return pendingWorkerReservationTTL
}

// addWorker asks the pool for the worker and hands the request's reservation
// over to it. If the pool fails, the reservation ends and the pool is left
// alone for growRetryDelay.
func (s *Scheduler) addWorker(request *types.ContainerRequest, pool *WorkerPool, size workerSize, reservationID string) {
	if s.ctx.Err() != nil {
		s.provisioning.release(s, reservationID)
		return
	}

	start := s.now()
	worker, err := pool.Controller.AddWorker(size.cpu, size.memory, size.gpuCount)
	s.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerProvisionWorker, start, s.now(), err == nil, map[string]string{
		"pool_name": pool.Name,
	})
	if err != nil {
		s.provisioning.release(s, reservationID)
		s.workerProvisioningBackoff.failed(pool.Name, s.now())
		logAddWorkerFailure(request, err)
		metrics.RecordSchedulerWorkerWait(s.since(request.Timestamp), request, "add_worker_failed")
		return
	}

	workerLog(requestLog(log.Info(), request), worker).Msg("added new worker")
	s.provisioning.handoff(s, reservationID, worker)
}

// releaseGrowLocks lets containers ask a pool for a worker again, where this
// replica holds the lock that keeps them from doing so.
func (s *Scheduler) releaseGrowLocks(containerIDs []string) {
	for _, containerID := range containerIDs {
		lockKey := common.RedisKeys.SchedulerWorkerProvisioningLock(containerID)
		if err := s.workerProvisioningLock.Release(lockKey); err != nil && !errors.Is(err, redislock.ErrLockNotHeld) {
			log.Error().Err(err).Str("container_id", containerID).Msg("unable to release worker provisioning lock")
		}
	}
}

func logAddWorkerFailure(request *types.ContainerRequest, err error) {
	var capacityErr *AgentPoolCapacityError
	if errors.As(err, &capacityErr) {
		requestLog(log.Debug(), request).
			Str("pool_name", capacityErr.PoolName).
			Int("machines", capacityErr.Machines).
			Int("schedulable_machines", capacityErr.SchedulableMachines).
			Int64("max_available_cpu", capacityErr.MaxAvailableCPU).
			Int64("max_available_memory", capacityErr.MaxAvailableMemory).
			Uint32("max_available_gpu", capacityErr.MaxAvailableGPU).
			Err(err).
			Msg("agent pool capacity unavailable")
		return
	}

	requestLog(log.Error(), request).Err(err).Msg("unable to add worker")
}

// workerProvisioningBackoff remembers the pools whose AddWorker just failed.
// It is kept per scheduler replica: each replica finds out for itself, once.
type workerProvisioningBackoff struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func newWorkerProvisioningBackoff() *workerProvisioningBackoff {
	return &workerProvisioningBackoff{until: map[string]time.Time{}}
}

// failed records that the pool failed to add a worker now.
func (b *workerProvisioningBackoff) failed(pool string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.until[pool] = now.Add(growRetryDelay)
}

// leftAlone reports whether the pool failed to add a worker less than
// growRetryDelay before now.
func (b *workerProvisioningBackoff) leftAlone(pool string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	until, failed := b.until[pool]
	if failed && !now.Before(until) {
		delete(b.until, pool)
		return false
	}
	return failed
}

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
