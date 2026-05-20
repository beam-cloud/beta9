package scheduler

import (
	"fmt"
	"sync"
	"time"

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
	// Keep the scheduling order explicit: use ready capacity first, wait on
	// pending capacity second, and only provision when neither path can fit.
	if a.scheduleOnAvailableWorker() {
		return
	}

	if a.reservePendingWorkerCapacity() {
		return
	}

	a.provisionWorker()
}

func (a *schedulingAttempt) scheduleOnAvailableWorker() bool {
	worker, err := a.scheduler.selectWorkerFromWorkers(a.workers, a.request)
	if err != nil || worker == nil {
		return false
	}

	if err := a.scheduler.scheduleRequest(worker, a.request); err != nil {
		log.Error().
			Str("container_id", a.request.ContainerId).
			Str("worker_id", worker.Id).
			Err(err).
			Msg("unable to schedule request on existing worker")
		metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "schedule_failed")
		a.retry("schedule_failed")
		return true
	}

	duration := time.Since(a.request.Timestamp)
	metrics.RecordRequestSchedulingDuration(duration, a.request)
	metrics.RecordSchedulerWorkerWait(duration, a.request, "scheduled")
	return true
}

func (a *schedulingAttempt) reservePendingWorkerCapacity() bool {
	if !a.scheduler.provisioning.reserveCapacity(a.scheduler, a.workers, a.request) {
		return false
	}

	metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "waiting_for_worker")
	a.requeueForWorkerWait()
	return true
}

func (a *schedulingAttempt) provisionWorker() {
	controllers, err := a.scheduler.getControllers(a.request)
	if err != nil {
		log.Error().Interface("request", a.request).Err(err).Msg("no controller found for request")
		a.fail("no_controller")
		return
	}

	metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "no_worker")

	reservationID := a.scheduler.provisioning.addReservation(a.scheduler, a.request, controllers[0])
	go newWorkerProvisioningAttempt(a.scheduler, a.request, controllers, reservationID).run()
	a.requeueForWorkerWait()
}

func (a *schedulingAttempt) requeueForWorkerWait() {
	if time.Since(a.request.Timestamp) >= maxScheduleRetryDuration {
		a.fail("worker_capacity_timeout")
		return
	}

	if err := a.scheduler.requestBacklog.PushAfter(a.request, provisioningWorkerRequeueDelay); err != nil {
		log.Error().Str("container_id", a.request.ContainerId).Err(err).Msg("failed to requeue request waiting for worker capacity")
		a.fail("worker_wait_requeue_failed")
	}
}

func (a *schedulingAttempt) retry(reason string) {
	if err := a.scheduler.addRequestToBacklog(a.request); err != nil {
		log.Error().
			Str("container_id", a.request.ContainerId).
			Str("reason", reason).
			Err(err).
			Msg("failed to requeue request")
		a.fail(reason + "_requeue_failed")
	}
}

func (a *schedulingAttempt) fail(reason string) {
	log.Error().
		Str("container_id", a.request.ContainerId).
		Str("reason", reason).
		Int("retry_count", a.request.RetryCount).
		Msg("giving up on request")

	if err := a.scheduler.containerRepo.DeleteContainerState(a.request.ContainerId); err != nil {
		log.Error().Str("container_id", a.request.ContainerId).Err(err).Msg("failed to delete container state after scheduling failure")
	}
	if err := a.scheduler.containerRepo.SetContainerRequestStatus(a.request.ContainerId, types.ContainerRequestStatusFailed); err != nil {
		log.Error().Str("container_id", a.request.ContainerId).Err(err).Msg("failed to record container request scheduling failure")
	}
	metrics.RecordRequestScheduleFailure(a.request)
}

type workerProvisioningAttempt struct {
	scheduler     *Scheduler
	request       *types.ContainerRequest
	controllers   []WorkerPoolController
	reservationID string
}

func newWorkerProvisioningAttempt(scheduler *Scheduler, request *types.ContainerRequest, controllers []WorkerPoolController, reservationID string) *workerProvisioningAttempt {
	return &workerProvisioningAttempt{
		scheduler:     scheduler,
		request:       request,
		controllers:   controllers,
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

	var addWorkerErr error
	for _, controller := range a.controllers {
		if controller == nil {
			continue
		}

		select {
		case <-a.scheduler.ctx.Done():
			return
		default:
		}

		newWorker, err := controller.AddWorker(
			a.scheduler.workerCPUForRequest(a.request),
			a.scheduler.workerMemoryForRequest(a.request),
			a.scheduler.workerGPUCountForRequest(a.request),
		)
		if err != nil {
			addWorkerErr = err
			continue
		}

		log.Info().Str("worker_id", newWorker.Id).Str("container_id", a.request.ContainerId).Msg("added new worker")
		releaseOnReturn = false
		time.AfterFunc(provisioningReservationHandoff, func() {
			a.scheduler.provisioning.release(a.reservationID)
		})
		return
	}

	log.Error().Str("container_id", a.request.ContainerId).Err(addWorkerErr).Msg("unable to add worker")
	metrics.RecordSchedulerWorkerWait(time.Since(a.request.Timestamp), a.request, "add_worker_failed")
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
		if reserveWorkerCapacity(worker, request) {
			p.trackRequestLocked(worker.Id, request.ContainerId)
			return true
		}
	}

	worker, err := s.selectWorkerFromWorkersByStatus(workers, request, types.WorkerStatusPending)
	if err != nil || worker == nil {
		return false
	}

	return reserveWorkerCapacity(worker, request)
}

func (p *provisioningTracker) addReservation(s *Scheduler, request *types.ContainerRequest, controller WorkerPoolController) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()

	reservation := s.newProvisioningReservation(request, controller)
	reserveWorkerCapacity(reservation.worker, request)
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

func (s *Scheduler) newProvisioningReservation(request *types.ContainerRequest, controller WorkerPoolController) *provisioningReservation {
	cpu := s.workerCPUForRequest(request)
	memory := s.workerMemoryForRequest(request)

	gpu := ""
	gpuCount := s.workerGPUCountForRequest(request)
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

func (s *Scheduler) workerMemoryForRequest(request *types.ContainerRequest) int64 {
	memory := capacityMemoryForScheduling(request)
	if s.config.Worker.DefaultWorkerMemoryRequest > memory {
		memory = s.config.Worker.DefaultWorkerMemoryRequest
	}
	return memory
}

func (s *Scheduler) workerGPUCountForRequest(request *types.ContainerRequest) uint32 {
	if !request.RequiresGPU() {
		return 0
	}
	if request.GpuCount == 0 {
		return 1
	}
	return request.GpuCount
}

func reserveWorkerCapacity(worker *types.Worker, request *types.ContainerRequest) bool {
	memory := capacityMemoryForScheduling(request)
	if worker.FreeCpu < request.Cpu || worker.FreeMemory < memory {
		return false
	}

	if request.RequiresGPU() {
		if worker.FreeGpuCount < request.GpuCount {
			return false
		}
		worker.FreeGpuCount -= request.GpuCount
	}

	worker.FreeCpu -= request.Cpu
	worker.FreeMemory -= memory
	return true
}
