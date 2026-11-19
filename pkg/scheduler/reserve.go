package scheduler

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type schedulingAttempt struct {
	scheduler *Scheduler
	request   *types.ContainerRequest
	workers   []*types.Worker
}

func newSchedulingAttempt(scheduler *Scheduler, request *types.ContainerRequest, workers []*types.Worker) *schedulingAttempt {
	return &schedulingAttempt{
		scheduler: scheduler,
		request:   request,
		workers:   workers,
	}
}

func (a *schedulingAttempt) runWaitingOrProvisioning() {
	// Opportunistic requests only ever fill idle capacity. They never queue
	// against pending workers or provision new ones. Spot waits for capacity
	// to free up; managed endpoint replicas fail at once, and their controller
	// retries on its own cadence.
	if a.request.OpportunisticOnly {
		waited := a.scheduler.since(a.request.Timestamp)
		if a.request.Borrowed() && waited < maxScheduleRetryDuration {
			metrics.RecordSchedulerWorkerWait(waited, a.request, "spare_capacity_wait")
			a.requeueForWorkerWait()
			return
		}
		metrics.RecordSchedulerWorkerWait(waited, a.request, string(types.ContainerSchedulingFailureNoOpportunisticCapacity))
		a.fail(types.ContainerSchedulingFailureNoOpportunisticCapacity)
		return
	}

	if a.reservePendingWorkerCapacity() {
		return
	}

	// A machine-pinned request can only ever run on its machine's worker;
	// provisioning elsewhere can't help, so wait for capacity to free up.
	if a.request.MachineId != "" {
		metrics.RecordSchedulerWorkerWait(a.scheduler.since(a.request.Timestamp), a.request, "pinned_machine_busy")
		a.requeueForWorkerWait()
		return
	}

	if a.tryPrivatePoolFallback() {
		return
	}

	a.grow()
}

func (a *schedulingAttempt) runnable() bool {
	pending, err := a.scheduler.containerRequestPending(a.request.ContainerId)
	if err == nil {
		return pending
	}
	requestLog(log.Error(), a.request).Err(err).Msg("failed to verify container request state")
	a.retry("container_state_lookup_failed")
	return false
}

func (a *schedulingAttempt) reservePendingWorkerCapacity() bool {
	reservationStart := a.scheduler.now()
	reserved := a.scheduler.provisioning.reserveCapacity(a.scheduler, a.workers, a.request)
	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerReservation, reservationStart, a.scheduler.now(), reserved, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(a.workers)),
	})
	if !reserved {
		return false
	}

	metrics.RecordSchedulerWorkerWait(a.scheduler.since(a.request.Timestamp), a.request, "waiting_for_worker")
	// The pool's plan counts the request against what is coming.
	if pool, ok := a.scheduler.awaitedPool(a.request.ContainerId); ok {
		a.scheduler.joinWaitlist(a.request, pool)
	}
	a.requeueForWorkerWait()
	return true
}

func (a *schedulingAttempt) requeueForWorkerWait() {
	a.requeueForWorkerWaitDelay(provisioningWorkerRequeueDelay, "worker_capacity_wait")
}

func (a *schedulingAttempt) requeueForWorkerWaitDelay(delay time.Duration, reason string) {
	if a.scheduler.since(a.request.Timestamp) >= maxScheduleRetryDuration {
		a.fail(types.ContainerSchedulingFailureWorkerCapacityTimeout)
		return
	}

	if err := a.scheduler.pushBacklog(a.request, delay); err != nil {
		requestLog(log.Error(), a.request).Err(err).Msg("failed to requeue request waiting for worker capacity")
		a.fail(types.ContainerSchedulingFailureReason(reason + "_requeue_failed"))
	}
}

func (a *schedulingAttempt) retry(reason string) {
	if a.request.RetryCount >= maxScheduleRetryCount {
		a.fail(types.ContainerSchedulingFailureRetryLimit)
		return
	}

	if a.scheduler.since(a.request.Timestamp) >= maxScheduleRetryDuration {
		a.fail(types.ContainerSchedulingFailureWorkerCapacityTimeout)
		return
	}

	a.request.RetryCount++
	metrics.RecordRequestRetry(a.request)
	if err := a.scheduler.pushBacklog(a.request, requestProcessingInterval); err != nil {
		requestLog(log.Error(), a.request).
			Str("reason", reason).
			Err(err).
			Msg("failed to requeue request")
		a.fail(types.ContainerSchedulingFailureReason(reason + "_requeue_failed"))
	}
}

func (a *schedulingAttempt) fail(reason types.ContainerSchedulingFailureReason) {
	requestLog(log.Error(), a.request).
		Str("reason", string(reason)).
		Msg("giving up on request")

	a.recordBacklogWait(false, string(reason))
	a.scheduler.leaveWaitlist(a.request)
	// Fence assignment and publish the no-worker terminal result together.
	// A delayed scheduler attempt must never erase an assigned worker's state
	// or manufacture proof that its disks were finalized.
	if strings.HasPrefix(a.request.ContainerId, types.StubTypeVM+"-") {
		marked, err := a.scheduler.containerRepo.MarkPendingContainerStoppingIfUnassigned(
			a.request.ContainerId, types.ContainerStateTtlSWhileStopping, int(types.ContainerExitCodeScheduler),
		)
		if err != nil {
			requestLog(log.Error(), a.request).Err(err).Msg("failed to commit scheduling failure")
			return
		}
		if !marked {
			return
		}
	}

	if err := a.scheduler.containerRepo.DeleteContainerState(a.request.ContainerId); err != nil {
		requestLog(log.Error(), a.request).Err(err).Msg("failed to delete container state after scheduling failure")
	}
	if err := a.scheduler.containerRepo.SetContainerRequestStatus(a.request.ContainerId, types.ContainerRequestStatusFailed); err != nil {
		requestLog(log.Error(), a.request).Err(err).Msg("failed to record container request scheduling failure")
	}
	if a.request.TaskId != "" && a.scheduler.eventBus != nil {
		if _, err := a.scheduler.eventBus.Send(common.NewContainerSchedulingFailedEvent(common.ContainerSchedulingFailure{
			TaskID:       a.request.TaskId,
			ContainerID:  a.request.ContainerId,
			PoolSelector: a.request.PoolSelector,
			Reason:       reason,
		})); err != nil {
			requestLog(log.Error(), a.request).Err(err).Msg("failed to publish container scheduling failure")
		}
	}
	metrics.RecordRequestScheduleFailure(a.request)
}

func (a *schedulingAttempt) recordBacklogWait(success bool, reason string) {
	if a.request.Timestamp.IsZero() {
		return
	}

	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerBacklogWait, a.request.Timestamp, a.scheduler.now(), success, map[string]string{
		"reason":      reason,
		"retry_count": fmt.Sprintf("%d", a.request.RetryCount),
	})
}

type provisioningTracker struct {
	mu                  sync.Mutex
	sequence            uint64
	reservations        map[string]*provisioningReservation
	requestReservations map[string]string
}

// provisioningReservation is the requests that wait for one worker. worker
// stands for it: it has the worker's id once the pool has added it, and its
// free capacity is what the requests have not taken.
type provisioningReservation struct {
	worker     *types.Worker
	requestIDs map[string]struct{}
	// addedAt is when the worker was known to exist: when its pool had
	// added it. It is zero while the pool is still being asked.
	addedAt time.Time
}

// workerListLag is how long a worker that was just added may be missing
// from the workers a pass plans with, which were read before it.
const workerListLag = 5 * time.Second

// gone reports whether the worker was removed: its pool had added it, and it
// is not among the workers any more. A machine that was bought and taken
// back, or that failed to start, leaves its requests waiting like this.
//
// A worker that has started is not gone. Placement gives its room to the
// requests that are first in line, which need not be the ones that waited
// for it. One that finds it full goes on waiting until the reservation
// lapses: the request that took its place often asked for a worker of its
// own, which is still on its way.
func (r *provisioningReservation) gone(workers []*types.Worker, now time.Time) bool {
	if r.addedAt.IsZero() || now.Sub(r.addedAt) < workerListLag {
		return false
	}
	return !slices.ContainsFunc(workers, func(worker *types.Worker) bool { return worker.Id == r.worker.Id })
}

func newProvisioningTracker() *provisioningTracker {
	return &provisioningTracker{
		reservations:        map[string]*provisioningReservation{},
		requestReservations: map[string]string{},
	}
}

// awaitedPool returns the pool of the worker the request waits for.
func (s *Scheduler) awaitedPool(requestID string) (*WorkerPool, bool) {
	p := s.provisioning
	p.mu.Lock()
	defer p.mu.Unlock()
	reservation, ok := p.reservations[p.requestReservations[requestID]]
	if !ok {
		return nil, false
	}
	return s.poolOf(reservation.worker)
}

// reserveCapacity has the request wait for a worker that is on its way, and
// reports whether there is one with room for it.
func (p *provisioningTracker) reserveCapacity(s *Scheduler, workers []*types.Worker, request *types.ContainerRequest) bool {
	reserved, free := p.reserve(s, workers, request)
	// The requests that stopped waiting may ask for another worker at once,
	// this one in the pass it is in.
	s.releaseGrowLocks(free)
	return reserved
}

// reserve is reserveCapacity. Next to its answer it returns the requests
// that stopped waiting for the worker they waited for.
func (p *provisioningTracker) reserve(s *Scheduler, workers []*types.Worker, request *types.ContainerRequest) (reserved bool, free []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	waiting, free := p.keepWaitingLocked(s, workers, request)
	if waiting {
		return true, nil
	}
	return p.waitForWorkerLocked(s, workers, request), free
}

// keepWaitingLocked reports whether the request goes on waiting for the
// worker it waited for. When that worker is no longer worth waiting for, it
// returns the requests that stopped waiting.
func (p *provisioningTracker) keepWaitingLocked(s *Scheduler, workers []*types.Worker, request *types.ContainerRequest) (waiting bool, free []string) {
	reservationID, ok := p.requestReservations[request.ContainerId]
	if !ok {
		return false, nil
	}
	reservation, exists := p.reservations[reservationID]
	switch {
	case exists && reservation.gone(workers, s.now()):
		// Nothing can wait for it any more.
		return false, p.releaseLocked(reservationID)
	case exists && !s.stalled(reservation.worker, s.now()):
		return true, nil
	}
	// The worker has stalled: this request stops waiting for it.
	delete(p.requestReservations, request.ContainerId)
	if exists {
		delete(reservation.requestIDs, request.ContainerId)
	}
	return false, []string{request.ContainerId}
}

// waitForWorkerLocked has the request wait for a worker that is on its way
// and has room for it: one that requests wait for already, or else one that
// is starting. It reports whether there is one.
func (p *provisioningTracker) waitForWorkerLocked(s *Scheduler, workers []*types.Worker, request *types.ContainerRequest) bool {
	if worker := p.selectInFlightWorkerLocked(s, request); worker != nil && s.reserveWorkerCapacity(worker, request) {
		p.trackRequestLocked(worker.Id, request.ContainerId)
		return true
	}

	worker, err := s.selectWorkerFromWorkersByStatus(p.unreservedWorkersLocked(workers), request, types.WorkerStatusPending)
	if err != nil || worker == nil || !s.reserveWorkerCapacity(worker, request) {
		return false
	}
	p.trackPendingWorkerLocked(s, worker, request)
	return true
}

// addReservation holds room for the request on a worker the pool is about to
// be asked for, and returns the reservation's id.
func (p *provisioningTracker) addReservation(s *Scheduler, request *types.ContainerRequest, pool *WorkerPool, size workerSize) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	normalizeGPURequest(request)

	p.sequence++
	reservation := s.newProvisioningReservation(request, pool, size, p.sequence)
	s.reserveWorkerCapacity(reservation.worker, request)
	reservation.requestIDs[request.ContainerId] = struct{}{}

	p.reservations[reservation.worker.Id] = reservation
	p.requestReservations[request.ContainerId] = reservation.worker.Id
	return reservation.worker.Id
}

// release ends a reservation. Its requests stop waiting, and may ask a pool
// for a worker again.
func (p *provisioningTracker) release(s *Scheduler, reservationID string) {
	p.mu.Lock()
	free := p.releaseLocked(reservationID)
	p.mu.Unlock()
	s.releaseGrowLocks(free)
}

// releaseLocked ends a reservation and returns the requests that waited on
// it.
func (p *provisioningTracker) releaseLocked(reservationID string) []string {
	reservation, ok := p.reservations[reservationID]
	if !ok {
		return nil
	}

	for requestID := range reservation.requestIDs {
		delete(p.requestReservations, requestID)
	}
	delete(p.reservations, reservationID)
	return slices.Collect(maps.Keys(reservation.requestIDs))
}

// expireAfterTTL ends a reservation that has lasted as long as a worker may
// take to start.
func (p *provisioningTracker) expireAfterTTL(s *Scheduler, reservationID string) {
	common.ClockOrReal(s.clock).AfterFunc(pendingWorkerReservationTTL, func() { p.release(s, reservationID) })
}

// handoff gives a reservation to the worker its pool added. A pool of
// machines that joined by hand answers with the machine's worker, which other
// requests may be waiting for already: they go on waiting with this one.
func (p *provisioningTracker) handoff(s *Scheduler, reservationID string, worker *types.Worker) {
	p.mu.Lock()
	reservation, ok := p.reservations[reservationID]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.reservations, reservationID)
	reservation.worker.Id = worker.Id
	reservation.worker.MachineId = worker.MachineId
	reservation.worker.Runtime = worker.Runtime
	reservation.worker.CreatedAt = worker.CreatedAt
	reservation.addedAt = s.now()
	takeCapacityOf(reservation.worker, worker)
	if waiting, ok := p.reservations[worker.Id]; ok {
		maps.Copy(reservation.requestIDs, waiting.requestIDs)
	}
	p.reservations[worker.Id] = reservation
	for requestID := range reservation.requestIDs {
		p.requestReservations[requestID] = worker.Id
	}
	p.mu.Unlock()

	p.expireAfterTTL(s, worker.Id)
}

// takeCapacityOf gives a placeholder the capacity of the worker the pool
// added, less what the requests waiting on the placeholder already hold. The
// pool may add a bigger worker than it was asked for: a machine that is
// bought has the size it is sold in. The room that is left over is then there
// for other requests to wait on, instead of each buying a machine of its own.
func takeCapacityOf(placeholder, worker *types.Worker) {
	heldCPU := placeholder.TotalCpu - placeholder.FreeCpu
	heldMemory := placeholder.TotalMemory - placeholder.FreeMemory
	heldGPU := placeholder.TotalGpuCount - placeholder.FreeGpuCount

	placeholder.TotalCpu = worker.TotalCpu
	placeholder.TotalMemory = worker.TotalMemory
	placeholder.TotalGpuCount = worker.TotalGpuCount
	placeholder.FreeCpu = max(worker.FreeCpu-heldCPU, 0)
	placeholder.FreeMemory = max(worker.FreeMemory-heldMemory, 0)
	placeholder.FreeGpuCount = worker.FreeGpuCount - min(worker.FreeGpuCount, heldGPU)
	if worker.Gpu != "" {
		placeholder.Gpu = worker.Gpu
	}
}

func (p *provisioningTracker) selectInFlightWorkerLocked(s *Scheduler, request *types.ContainerRequest) *types.Worker {
	if len(p.reservations) == 0 {
		return nil
	}

	workers := make([]*types.Worker, 0, len(p.reservations))
	for _, reservation := range p.reservations {
		workers = append(workers, reservation.worker)
	}

	worker, err := s.selectWorkerFromWorkersByStatus(workers, request, types.WorkerStatusPending)
	if err != nil {
		return nil
	}

	return worker
}

func (p *provisioningTracker) trackRequestLocked(reservationID string, requestID string) {
	reservation, ok := p.reservations[reservationID]
	if !ok {
		return
	}

	reservation.requestIDs[requestID] = struct{}{}
	p.requestReservations[requestID] = reservationID
}

func (p *provisioningTracker) trackPendingWorkerLocked(s *Scheduler, worker *types.Worker, request *types.ContainerRequest) {
	reservationID := worker.Id
	if _, ok := p.reservations[reservationID]; !ok {
		p.reservations[reservationID] = &provisioningReservation{
			worker:     worker,
			requestIDs: map[string]struct{}{},
			addedAt:    s.now(),
		}
		p.expireAfterTTL(s, reservationID)
	}

	p.trackRequestLocked(reservationID, request.ContainerId)
}

func (p *provisioningTracker) unreservedWorkersLocked(workers []*types.Worker) []*types.Worker {
	unreservedWorkers := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if _, reserved := p.reservations[worker.Id]; reserved {
			continue
		}
		unreservedWorkers = append(unreservedWorkers, worker)
	}
	return unreservedWorkers
}

// newProvisioningReservation builds the placeholder worker that stands for a
// worker being added. sequence keeps placeholder ids unique within a tracker.
func (s *Scheduler) newProvisioningReservation(request *types.ContainerRequest, pool *WorkerPool, size workerSize, sequence uint64) *provisioningReservation {
	gpu := ""
	if request.RequiresGPU() {
		gpu = pool.Config.GPUType
	}

	// An agent placeholder is the size of the request: what the pool will
	// add is not known before it answers, a machine that joined by hand or
	// one that is bought in the size it is sold in. Requests that arrive
	// before the answer find no room on it, and each asks for a worker of its
	// own. A Kubernetes placeholder matches the worker it stands for, whose
	// CPU the pool may overcommit.
	cpu := size.cpu
	if !pool.Config.AgentHosted() {
		cpu = pool.Config.CPUShareMultiplier.Schedulable(size.cpu)
	}
	worker := pendingWorker(fmt.Sprintf("provisioning-%s-%d", pool.Name, sequence), cpu, size.memory, gpu, size.gpuCount)
	worker.PoolName = pool.Name
	worker.RequiresPoolSelector = pool.Controller.RequiresPoolSelector()
	worker.Runtime = pool.Controller.ContainerRuntime()
	worker.BuildVersion = s.config.Worker.ImageTag
	worker.Preemptable = pool.Controller.IsPreemptable()
	worker.Priority = pool.Config.Priority
	worker.CreatedAt = s.now().Unix()

	return &provisioningReservation{
		worker:     worker,
		requestIDs: map[string]struct{}{},
	}
}

func normalizeGPURequest(request *types.ContainerRequest) {
	if request == nil || !request.RequiresGPU() || request.GpuCount > 0 {
		return
	}
	request.GpuCount = 1
}

func gpuCountForScheduling(request *types.ContainerRequest) uint32 {
	if !request.RequiresGPU() {
		return 0
	}
	if request.GpuCount == 0 {
		return 1
	}
	return request.GpuCount
}

// reserveWorkerCapacity debits a request from the in-memory worker so later
// requests in the same batch see what is left. Requests that may evict draw
// idle capacity first and evictable capacity for the remainder, mirroring
// what the repository does when it commits the schedule.
func (s *Scheduler) reserveWorkerCapacity(worker *types.Worker, request *types.ContainerRequest) bool {
	normalizeGPURequest(request)

	cpu := s.cpuReservation(worker, request)
	memory := capacityMemoryForScheduling(request)
	freeCPU, freeMemory, freeGPU := schedulableCapacity(worker, request)
	if freeCPU < cpu || freeMemory < memory {
		return false
	}

	if request.RequiresGPU() {
		if freeGPU < request.GpuCount {
			return false
		}
		worker.FreeGpuCount, worker.EvictableGpuCount = drawCapacity(worker.FreeGpuCount, worker.EvictableGpuCount, request.GpuCount)
	}

	worker.FreeCpu, worker.EvictableCpu = drawCapacity(worker.FreeCpu, worker.EvictableCpu, cpu)
	worker.FreeMemory, worker.EvictableMemory = drawCapacity(worker.FreeMemory, worker.EvictableMemory, memory)
	return true
}

// drawCapacity takes need from free first and the shortfall from evictable.
// Callers check fit beforehand; evictable is floored at zero regardless.
func drawCapacity[T int64 | uint32](free, evictable, need T) (T, T) {
	if need <= free {
		return free - need, evictable
	}
	shortfall := need - free
	if shortfall > evictable {
		evictable = 0
	} else {
		evictable -= shortfall
	}
	return 0, evictable
}
