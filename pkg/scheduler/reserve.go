package scheduler

import (
	"errors"
	"fmt"
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

func (a *schedulingAttempt) run() {
	normalizeGPURequest(a.request)

	// Keep the scheduling order explicit: use ready capacity first, wait on
	// pending capacity second, and only provision when neither path can fit.
	if a.scheduleOnAvailableWorker() {
		return
	}

	a.runWaitingOrProvisioning()
}

func (a *schedulingAttempt) runWaitingOrProvisioning() {
	if a.reservePendingWorkerCapacity() {
		return
	}

	// A machine-pinned request can only ever run on its machine's worker;
	// provisioning elsewhere can't help, so wait for capacity to free up.
	if a.request.MachineId != "" {
		metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "pinned_machine_busy")
		a.requeueForWorkerWait()
		return
	}

	if a.tryPrivatePoolFallback() {
		return
	}

	a.provisionWorker()
}

func (a *schedulingAttempt) scheduleOnAvailableWorker() bool {
	selectionStart := time.Now()
	worker, err := a.scheduler.selectWorkerFromWorkers(a.workers, a.request)
	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerWorkerSelection, selectionStart, time.Now(), err == nil && worker != nil, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(a.workers)),
	})
	if err != nil || worker == nil {
		return false
	}

	if err := a.scheduler.scheduleRequest(worker, a.request); err != nil {
		workerLog(requestLog(log.Error(), a.request), worker).
			Err(err).
			Msg("unable to schedule request on existing worker")
		a.recordBacklogWait(false, "schedule_failed")
		metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "schedule_failed")
		a.retrySoon("schedule_failed")
		return true
	}

	duration := time.Since(a.request.Timestamp)
	a.recordBacklogWait(true, "scheduled")
	metrics.RecordRequestSchedulingDuration(duration, a.request)
	metrics.RecordSchedulerWorkerWait(duration, a.request, "scheduled")
	return true
}

func (a *schedulingAttempt) reservePendingWorkerCapacity() bool {
	reservationStart := time.Now()
	reserved := a.scheduler.provisioning.reserveCapacity(a.scheduler, a.workers, a.request)
	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerReservation, reservationStart, time.Now(), reserved, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(a.workers)),
	})
	if !reserved {
		return false
	}

	metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "waiting_for_worker")
	a.requeueForWorkerWait()
	return true
}

func (a *schedulingAttempt) provisionWorker() {
	controllers, err := a.scheduler.getControllers(a.request)
	if err != nil {
		requestLog(log.Error(), a.request).
			Err(err).
			Msg("no controller found for request")
		a.fail(types.ContainerSchedulingFailureNoController)
		return
	}

	controller, delay := a.scheduler.workerProvisioningController(controllers)
	if controller == nil {
		a.recordBacklogWait(false, "worker_provisioning_backoff")
		a.requeueForWorkerWaitDelay(delay, "worker_provisioning_backoff")
		return
	}

	metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "no_worker")

	reservationID := a.scheduler.provisioning.addReservation(a.scheduler, a.request, controller)
	go newWorkerProvisioningAttempt(a.scheduler, a.request, controller, reservationID).run()
	a.requeueForWorkerWait()
}

func (a *schedulingAttempt) requeueForWorkerWait() {
	a.requeueForWorkerWaitDelay(provisioningWorkerRequeueDelay, "worker_capacity_wait")
}

func (a *schedulingAttempt) requeueForWorkerWaitDelay(delay time.Duration, reason string) {
	if time.Since(a.request.Timestamp) >= maxScheduleRetryDuration {
		a.fail(types.ContainerSchedulingFailureWorkerCapacityTimeout)
		return
	}

	if delay < requestProcessingInterval {
		delay = requestProcessingInterval
	}

	if err := a.scheduler.pushBacklog(a.request, delay); err != nil {
		requestLog(log.Error(), a.request).Err(err).Msg("failed to requeue request waiting for worker capacity")
		a.fail(types.ContainerSchedulingFailureReason(reason + "_requeue_failed"))
	}
}

func (a *schedulingAttempt) retry(reason string) {
	a.retrySoon(reason)
}

func (a *schedulingAttempt) retrySoon(reason string) {
	if a.request.RetryCount >= maxScheduleRetryCount {
		a.fail(types.ContainerSchedulingFailureRetryLimit)
		return
	}

	if time.Since(a.request.Timestamp) >= maxScheduleRetryDuration {
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

	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerBacklogWait, a.request.Timestamp, time.Now(), success, map[string]string{
		"reason":      reason,
		"retry_count": fmt.Sprintf("%d", a.request.RetryCount),
	})
}

type workerProvisioningAttempt struct {
	scheduler     *Scheduler
	request       *types.ContainerRequest
	controller    WorkerPoolController
	reservationID string
}

func newWorkerProvisioningAttempt(scheduler *Scheduler, request *types.ContainerRequest, controller WorkerPoolController, reservationID string) *workerProvisioningAttempt {
	return &workerProvisioningAttempt{
		scheduler:     scheduler,
		request:       request,
		controller:    controller,
		reservationID: reservationID,
	}
}

func (a *workerProvisioningAttempt) run() {
	releaseOnReturn := true
	defer func() {
		if releaseOnReturn {
			a.scheduler.provisioning.release(a.reservationID)
		}
	}()

	controller := a.controller
	if controller == nil {
		return
	}

	select {
	case <-a.scheduler.ctx.Done():
		return
	default:
	}

	provisionStart := time.Now()
	newWorker, err := controller.AddWorker(
		a.scheduler.workerCPUForControllerRequest(controller, a.request),
		a.scheduler.workerMemoryForControllerRequest(controller, a.request),
		a.scheduler.workerGPUCountForControllerRequest(controller, a.request),
	)
	a.scheduler.recordContainerLifecycle(a.request, types.ContainerLifecycleSchedulerProvisionWorker, provisionStart, time.Now(), err == nil, map[string]string{
		"pool_name": controller.Name(),
	})
	if err != nil {
		a.scheduler.recordWorkerProvisioningFailure(controller, err)
		a.logAddWorkerFailure(err)
		metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "add_worker_failed")
		return
	}

	workerLog(requestLog(log.Info(), a.request), newWorker).Msg("added new worker")
	releaseOnReturn = false
	time.AfterFunc(provisioningReservationHandoff, func() {
		a.scheduler.provisioning.release(a.reservationID)
	})
}

func (a *workerProvisioningAttempt) logAddWorkerFailure(err error) {
	var capacityErr *AgentPoolCapacityError
	if errors.As(err, &capacityErr) {
		requestLog(log.Debug(), a.request).
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

	requestLog(log.Error(), a.request).
		Err(err).
		Msg("unable to add worker")
}

type provisioningTracker struct {
	mu                  sync.Mutex
	reservations        map[string]*provisioningReservation
	requestReservations map[string]string
}

type provisioningReservation struct {
	worker     *types.Worker
	requestIDs map[string]struct{}
}

func newProvisioningTracker() *provisioningTracker {
	return &provisioningTracker{
		reservations:        map[string]*provisioningReservation{},
		requestReservations: map[string]string{},
	}
}

func (p *provisioningTracker) reserveCapacity(s *Scheduler, workers []*types.Worker, request *types.ContainerRequest) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()

	if reservationID, ok := p.requestReservations[request.ContainerId]; ok {
		if _, exists := p.reservations[reservationID]; exists {
			return true
		}
		delete(p.requestReservations, request.ContainerId)
	}

	if worker := p.selectInFlightWorkerLocked(s, request); worker != nil {
		if s.reserveWorkerCapacity(worker, request) {
			p.trackRequestLocked(worker.Id, request.ContainerId)
			return true
		}
	}

	worker, err := s.selectWorkerFromWorkersByStatus(p.unreservedWorkersLocked(workers), request, types.WorkerStatusPending)
	if err != nil || worker == nil {
		return false
	}

	if !s.reserveWorkerCapacity(worker, request) {
		return false
	}

	p.trackPendingWorkerLocked(worker, request)
	return true
}

func (p *provisioningTracker) addReservation(s *Scheduler, request *types.ContainerRequest, controller WorkerPoolController) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()
	normalizeGPURequest(request)

	reservation := s.newProvisioningReservation(request, controller)
	s.reserveWorkerCapacity(reservation.worker, request)
	reservation.requestIDs[request.ContainerId] = struct{}{}

	p.reservations[reservation.worker.Id] = reservation
	p.requestReservations[request.ContainerId] = reservation.worker.Id
	return reservation.worker.Id
}

func (p *provisioningTracker) release(reservationID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	reservation, ok := p.reservations[reservationID]
	if !ok {
		return
	}

	for requestID := range reservation.requestIDs {
		delete(p.requestReservations, requestID)
	}
	delete(p.reservations, reservationID)
}

func (p *provisioningTracker) ensureMaps() {
	if p.reservations == nil {
		p.reservations = map[string]*provisioningReservation{}
	}
	if p.requestReservations == nil {
		p.requestReservations = map[string]string{}
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

func (p *provisioningTracker) trackPendingWorkerLocked(worker *types.Worker, request *types.ContainerRequest) {
	reservationID := worker.Id
	if _, ok := p.reservations[reservationID]; !ok {
		p.reservations[reservationID] = &provisioningReservation{
			worker:     worker,
			requestIDs: map[string]struct{}{},
		}

		time.AfterFunc(pendingWorkerReservationTTL, func() {
			p.release(reservationID)
		})
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

func (s *Scheduler) newProvisioningReservation(request *types.ContainerRequest, controller WorkerPoolController) *provisioningReservation {
	cpu := s.workerCPUForControllerRequest(controller, request)
	memory := s.workerMemoryForControllerRequest(controller, request)

	gpu := ""
	gpuCount := s.workerGPUCountForControllerRequest(controller, request)
	if request.RequiresGPU() {
		if pool, ok := s.workerPoolManager.GetPool(controller.Name()); ok {
			gpu = pool.Config.GPUType
		}
	}

	worker := &types.Worker{
		Id:                   fmt.Sprintf("provisioning-%s-%d", controller.Name(), time.Now().UnixNano()),
		Status:               types.WorkerStatusPending,
		TotalCpu:             cpu,
		TotalMemory:          memory,
		TotalGpuCount:        gpuCount,
		FreeCpu:              cpu,
		FreeMemory:           memory,
		FreeGpuCount:         gpuCount,
		Gpu:                  gpu,
		PoolName:             controller.Name(),
		RequiresPoolSelector: controller.RequiresPoolSelector(),
		Runtime:              controller.ContainerRuntime(),
		BuildVersion:         s.config.Worker.ImageTag,
		Preemptable:          controller.IsPreemptable(),
	}

	if pool, ok := s.workerPoolManager.GetPool(controller.Name()); ok {
		worker.Priority = pool.Config.Priority
	}

	return &provisioningReservation{
		worker:     worker,
		requestIDs: map[string]struct{}{},
	}
}

func (s *Scheduler) workerCPUForRequest(request *types.ContainerRequest) int64 {
	cpu := request.Cpu
	if s.config.Worker.DefaultWorkerCPURequest > cpu {
		cpu = s.config.Worker.DefaultWorkerCPURequest
	}
	return cpu
}

func (s *Scheduler) workerCPUForControllerRequest(controller WorkerPoolController, request *types.ContainerRequest) int64 {
	if controllerUsesAgentCapacity(controller) {
		return request.Cpu
	}

	cpu := s.workerCPUForRequest(request)
	sizing, ok := s.workerPoolSizingForController(controller)
	if !ok {
		return cpu
	}
	if sizing.DefaultWorkerCpu > cpu {
		return sizing.DefaultWorkerCpu
	}
	return cpu
}

func (s *Scheduler) workerMemoryForRequest(request *types.ContainerRequest) int64 {
	memory := capacityMemoryForScheduling(request)
	if s.config.Worker.DefaultWorkerMemoryRequest > memory {
		memory = s.config.Worker.DefaultWorkerMemoryRequest
	}
	return memory
}

func (s *Scheduler) workerMemoryForControllerRequest(controller WorkerPoolController, request *types.ContainerRequest) int64 {
	if controllerUsesAgentCapacity(controller) {
		return capacityMemoryForScheduling(request)
	}

	memory := s.workerMemoryForRequest(request)
	sizing, ok := s.workerPoolSizingForController(controller)
	if !ok {
		return memory
	}
	if sizing.DefaultWorkerMemory > memory {
		return sizing.DefaultWorkerMemory
	}
	return memory
}

func (s *Scheduler) workerGPUCountForRequest(request *types.ContainerRequest) uint32 {
	return gpuCountForScheduling(request)
}

func (s *Scheduler) workerGPUCountForControllerRequest(controller WorkerPoolController, request *types.ContainerRequest) uint32 {
	if controllerUsesAgentCapacity(controller) {
		return gpuCountForScheduling(request)
	}

	gpuCount := s.workerGPUCountForRequest(request)
	sizing, ok := s.workerPoolSizingForController(controller)
	if !ok {
		return gpuCount
	}
	if sizing.DefaultWorkerGpuCount > gpuCount {
		return sizing.DefaultWorkerGpuCount
	}
	return gpuCount
}

func controllerUsesAgentCapacity(controller WorkerPoolController) bool {
	if controller == nil {
		return false
	}
	if _, ok := controller.(*AgentWorkerPoolController); ok {
		return true
	}
	return controller.Mode().AgentHosted()
}

func (s *Scheduler) workerPoolSizingForController(controller WorkerPoolController) (*types.WorkerPoolSizingConfig, bool) {
	if controller == nil {
		return nil, false
	}
	pool, ok := s.workerPoolManager.GetPool(controller.Name())
	if !ok {
		return nil, false
	}
	sizing, err := parsePoolSizingConfig(pool.Config.PoolSizing)
	if err != nil {
		return nil, false
	}
	return sizing, true
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

func (s *Scheduler) reserveWorkerCapacity(worker *types.Worker, request *types.ContainerRequest) bool {
	normalizeGPURequest(request)

	cpu := request.Cpu
	memory := capacityMemoryForScheduling(request)
	if worker.FreeCpu < cpu || worker.FreeMemory < memory {
		return false
	}

	if request.RequiresGPU() {
		if worker.FreeGpuCount < request.GpuCount {
			return false
		}
		worker.FreeGpuCount -= request.GpuCount
	}

	worker.FreeCpu -= cpu
	worker.FreeMemory -= memory
	return true
}
