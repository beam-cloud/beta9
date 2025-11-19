package scheduler

import (
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// schedulingDecision represents a container assigned to a worker
type schedulingDecision struct {
	request *types.ContainerRequest
	worker  *types.Worker
}

// workerCapacityTracker tracks available capacity as scheduling decisions are made
type workerCapacityTracker struct {
	freeCpu      int64
	freeMemory   int64
	freeGpuCount uint32
}

// processBatch schedules multiple containers together, updating worker capacity in a single batch
func (s *Scheduler) processBatch(requests []*types.ContainerRequest) {
	if len(requests) == 0 {
		return
	}

	log.Debug().Int("batch_size", len(requests)).Msg("processing batch of container requests")

	// Get worker state once for the entire batch
	workers, err := s.workerCache.Get(s.ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to get workers for batch")
		for _, req := range requests {
			s.addRequestToBacklog(req)
		}
		return
	}

	// Make scheduling decisions for all requests
	decisions := make([]schedulingDecision, 0, len(requests))
	needNewWorker := make([]*types.ContainerRequest, 0)

	// Track capacity as we make decisions to prevent double-booking
	workerCapacity := make(map[string]*workerCapacityTracker)
	for _, w := range workers {
		workerCapacity[w.Id] = &workerCapacityTracker{
			freeCpu:      w.FreeCpu,
			freeMemory:   w.FreeMemory,
			freeGpuCount: w.FreeGpuCount,
		}
	}

	for _, request := range requests {
		// Filter workers for this request
		filteredWorkers := filterWorkersByPoolSelector(workers, request)
		filteredWorkers = filterWorkersByResources(filteredWorkers, request)
		filteredWorkers = filterWorkersByFlags(filteredWorkers, request)

		// Check against tracked capacity (not just Redis state)
		availableWorkers := make([]*types.Worker, 0)
		for _, w := range filteredWorkers {
			capacity := workerCapacity[w.Id]
			if capacity.freeCpu >= request.Cpu &&
				capacity.freeMemory >= request.Memory &&
				(request.GpuCount == 0 || capacity.freeGpuCount >= request.GpuCount) {
				availableWorkers = append(availableWorkers, w)
			}
		}

		if len(availableWorkers) == 0 {
			needNewWorker = append(needNewWorker, request)
			continue
		}

		// Apply worker affinity
		if !request.RequiresGPU() && len(availableWorkers) > 1 {
			availableWorkers = applyWorkerAffinity(request.ContainerId, availableWorkers)
		}

		// Score workers
		bestWorker := s.selectBestWorker(availableWorkers)
		if bestWorker == nil {
			needNewWorker = append(needNewWorker, request)
			continue
		}

		// Reserve capacity
		capacity := workerCapacity[bestWorker.Id]
		capacity.freeCpu -= request.Cpu
		capacity.freeMemory -= request.Memory
		if request.GpuCount > 0 {
			capacity.freeGpuCount -= request.GpuCount
		}

		decisions = append(decisions, schedulingDecision{
			request: request,
			worker:  bestWorker,
		})
	}

	log.Debug().
		Int("scheduled", len(decisions)).
		Int("need_new_worker", len(needNewWorker)).
		Msg("batch scheduling decisions made")

	// Schedule all decisions and batch update capacity
	if len(decisions) > 0 {
		s.executeBatchScheduling(decisions)
	}

	// Handle requests that need new workers
	for _, request := range needNewWorker {
		s.scheduleWithNewWorker(request)
	}
}

// selectBestWorker picks the best worker from a filtered list
func (s *Scheduler) selectBestWorker(workers []*types.Worker) *types.Worker {
	if len(workers) == 0 {
		return nil
	}

	scoredWorkers := []scoredWorker{}
	for _, worker := range workers {
		score := int32(0)
		if worker.Status == types.WorkerStatusAvailable {
			score += scoreAvailableWorker
		}
		score += worker.Priority
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	sort.Slice(scoredWorkers, func(i, j int) bool {
		return scoredWorkers[i].score > scoredWorkers[j].score
	})

	return scoredWorkers[0].worker
}

// executeBatchScheduling schedules multiple containers and updates worker capacity in one batch
func (s *Scheduler) executeBatchScheduling(decisions []schedulingDecision) {
	// Group reservations by worker
	workerReservations := make(map[string][]repo.ResourceReservation)

	for _, decision := range decisions {
		workerReservations[decision.worker.Id] = append(
			workerReservations[decision.worker.Id],
			repo.ResourceReservation{
				ContainerId: decision.request.ContainerId,
				CPU:         decision.request.Cpu,
				Memory:      decision.request.Memory,
				GPU:         int64(decision.request.GpuCount),
				GPUType:     decision.request.Gpu,
			},
		)
	}

	// Build batch reservations
	batchReservations := make([]repo.CapacityReservation, 0, len(workerReservations))
	for workerId, reservations := range workerReservations {
		batchReservations = append(batchReservations, repo.CapacityReservation{
			WorkerId:     workerId,
			Reservations: reservations,
		})
	}

	// Batch update worker capacity
	updateErrors, err := s.workerRepo.BatchUpdateWorkerCapacity(
		s.ctx,
		batchReservations,
		types.RemoveCapacity,
	)

	if err != nil || len(updateErrors) > 0 {
		log.Error().Err(err).Interface("errors", updateErrors).Msg("failed to batch update worker capacity")
		// Re-queue failed requests
		failedWorkers := make(map[string]bool)
		for workerId := range updateErrors {
			failedWorkers[workerId] = true
		}
		for _, decision := range decisions {
			if failedWorkers[decision.worker.Id] {
				s.addRequestToBacklog(decision.request)
			}
		}
		return
	}

	// Invalidate cache for all affected workers
	for workerId := range workerReservations {
		s.workerCache.InvalidateWorker(workerId)
	}

	// Now schedule all the requests (push to worker queues)
	for _, decision := range decisions {
		go s.finalizeScheduling(decision.worker, decision.request)
	}
}

// finalizeScheduling completes the scheduling process for a container
func (s *Scheduler) finalizeScheduling(worker *types.Worker, request *types.ContainerRequest) {
	if err := s.containerRepo.UpdateAssignedContainerGPU(request.ContainerId, worker.Gpu); err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to update assigned container gpu")
		s.addRequestToBacklog(request)
		return
	}

	request.Gpu = worker.Gpu

	// Attach OCI credentials for runtime lazy layer loading
	if err := s.attachImageCredentials(request); err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("image_id", request.ImageId).
			Msg("failed to attach OCI credentials, will use default provider")
	}

	// Attach build registry credentials for push + runtime layer loading
	if err := s.attachBuildRegistryCredentials(request); err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Msg("failed to attach build registry credentials to request")
	}

	go s.schedulerUsageMetrics.CounterIncContainerScheduled(request)
	go s.eventRepo.PushContainerScheduledEvent(request.ContainerId, worker.Id, request)

	err := s.workerRepo.ScheduleContainerRequest(worker, request)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to schedule container request")
		s.addRequestToBacklog(request)
		return
	}

	// Record the request processing duration
	schedulingDuration := time.Since(request.Timestamp)
	metrics.RecordRequestSchedulingDuration(schedulingDuration, request)
}

// scheduleWithNewWorker handles requests that need a new worker
func (s *Scheduler) scheduleWithNewWorker(request *types.ContainerRequest) {
	controllers, err := s.getControllers(request)
	if err != nil {
		log.Error().Interface("request", request).Err(err).Msg("no controller found for request")
		s.addRequestToBacklog(request)
		return
	}

	go func() {
		var err error
		for _, c := range controllers {
			if c == nil {
				continue
			}

			var newWorker *types.Worker
			newWorker, err = c.AddWorker(request.Cpu, request.Memory, request.GpuCount)
			if err == nil {
				log.Info().Str("worker_id", newWorker.Id).Str("container_id", request.ContainerId).Msg("added new worker")

				// Invalidate cache to pick up new worker
				s.workerCache.InvalidateAll()

				s.finalizeScheduling(newWorker, request)
				return
			}
		}

		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to add worker")
		s.addRequestToBacklog(request)
	}()
}

// collectRequestBatch collects multiple requests from the backlog for batch processing
func (s *Scheduler) collectRequestBatch() []*types.ContainerRequest {
	batch := make([]*types.ContainerRequest, 0, maxBatchSize)
	deadline := time.Now().Add(batchSchedulingWindow)

	for {
		// Stop if we hit the batch size limit
		if len(batch) >= maxBatchSize {
			break
		}

		// Stop if we've spent enough time collecting
		if time.Now().After(deadline) && len(batch) > 0 {
			break
		}

		// Stop if backlog is empty
		if s.requestBacklog.Len() == 0 {
			if len(batch) > 0 {
				break
			}
			// If we have no requests yet, wait a bit
			time.Sleep(10 * time.Millisecond)
			continue
		}

		request, err := s.requestBacklog.Pop()
		if err != nil {
			continue
		}

		batch = append(batch, request)
	}

	return batch
}
