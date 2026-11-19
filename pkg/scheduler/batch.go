package scheduler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/beam-cloud/beta9/pkg/metrics"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const requestSchedulingParallelism = 128

// backgroundSettle bounds concurrent background failure handling.
var backgroundSettle = make(chan struct{}, 16)

type schedulingBatch struct {
	scheduler *Scheduler
	workers   []*types.Worker
	batchSize int

	schedules []plannedSchedule
	// deferred is background work that found no idle capacity; it is failed
	// after dispatch so serverless requests never wait behind its cleanup.
	deferred []*schedulingAttempt
}

type plannedSchedule struct {
	worker  *types.Worker
	request *types.ContainerRequest
}

type containerStatusBatchReader interface {
	GetContainerStatuses(containerIds []string) (map[string]types.ContainerStatus, error)
}

func newSchedulingBatch(scheduler *Scheduler, workers []*types.Worker, batchSize int) *schedulingBatch {
	return &schedulingBatch{
		scheduler: scheduler,
		workers:   workers,
		batchSize: batchSize,
		schedules: make([]plannedSchedule, 0),
	}
}

func (s *Scheduler) processRequestBatch(requests []*types.ContainerRequest, workers []*types.Worker) {
	batch := newSchedulingBatch(s, workers, len(requests))
	batch.plan(requests)
	batch.dispatch()
	batch.settleDeferred()
}

func (b *schedulingBatch) plan(requests []*types.ContainerRequest) {
	var statuses map[string]types.ContainerStatus
	reader, batched := b.scheduler.containerRepo.(containerStatusBatchReader)
	if batched {
		containerIds := make([]string, 0, len(requests))
		for _, request := range requests {
			containerIds = append(containerIds, request.ContainerId)
		}
		var err error
		statuses, err = reader.GetContainerStatuses(containerIds)
		batched = err == nil
	}

	// Serverless work claims idle capacity before managed endpoint replicas.
	requests = foregroundFirst(requests)
	for _, request := range requests {
		attempt := newSchedulingAttempt(b.scheduler, request, b.workers)
		runnable := false
		if batched {
			runnable = statuses[request.ContainerId] == types.ContainerStatusPending
		} else {
			runnable = attempt.runnable()
		}
		b.planRequest(request, attempt, runnable)
	}
}

func (b *schedulingBatch) planRequest(request *types.ContainerRequest, attempt *schedulingAttempt, runnable bool) {
	planStart := b.scheduler.now()
	planned := false
	defer func() {
		b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerBatchPlan, planStart, b.scheduler.now(), true, map[string]string{
			"batch_size":           fmt.Sprintf("%d", b.batchSize),
			"worker_count":         fmt.Sprintf("%d", len(b.workers)),
			"planned":              fmt.Sprintf("%t", planned),
			"planned_count_so_far": fmt.Sprintf("%d", len(b.schedules)),
		})
	}()
	if !runnable {
		return
	}
	if !b.scheduler.checkpointReady(request) {
		if attempt.runnable() {
			attempt.requeueForWorkerWaitDelay(checkpointHandoffRetryDelay, "checkpoint_handoff")
		}
		return
	}

	normalizeGPURequest(request)

	selectionStart := b.scheduler.now()
	worker, err := b.scheduler.selectWorkerFromWorkers(b.workers, request)
	b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerWorkerSelection, selectionStart, b.scheduler.now(), err == nil && worker != nil, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(b.workers)),
	})
	if err != nil || worker == nil {
		b.unplaced(attempt)
		return
	}

	workerForSchedule := cloneWorker(worker)
	reserveStart := b.scheduler.now()
	reserved := b.scheduler.reserveWorkerCapacity(worker, request)
	b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerReserveCapacity, reserveStart, b.scheduler.now(), reserved, map[string]string{
		"worker_id": worker.Id,
	})
	if !reserved {
		b.unplaced(attempt)
		return
	}

	b.schedules = append(b.schedules, plannedSchedule{
		worker:  workerForSchedule,
		request: request,
	})
	planned = true
}

// unplaced handles a request that fits no worker in this batch.
func (b *schedulingBatch) unplaced(attempt *schedulingAttempt) {
	if attempt.request.Background() {
		b.deferred = append(b.deferred, attempt)
		return
	}
	if attempt.runnable() {
		attempt.runWaitingOrProvisioning()
	}
}

func (b *schedulingBatch) settleDeferred() {
	for _, attempt := range b.deferred {
		backgroundSettle <- struct{}{}
		b.scheduler.background(func() {
			defer func() { <-backgroundSettle }()
			if attempt.runnable() {
				attempt.runWaitingOrProvisioning()
			}
		})
	}
}

func (b *schedulingBatch) dispatch() {
	if len(b.schedules) == 0 {
		return
	}

	schedulesByWorker := map[string][]plannedSchedule{}
	for _, schedule := range b.schedules {
		schedulesByWorker[schedule.worker.Id] = append(schedulesByWorker[schedule.worker.Id], schedule)
	}

	parallelism := requestSchedulingParallelism
	if len(schedulesByWorker) < parallelism {
		parallelism = len(schedulesByWorker)
	}

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for _, schedules := range schedulesByWorker {
		schedules := schedules
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			b.dispatchSchedules(schedules)
		}()
	}

	wg.Wait()
}

func (b *schedulingBatch) dispatchSchedules(schedules []plannedSchedule) {
	if len(schedules) == 0 {
		return
	}

	// Requests stop waiting for a machine before they take their room, so
	// that no pool sees both and buys for them again.
	requests := make([]*types.ContainerRequest, len(schedules))
	for i, schedule := range schedules {
		requests[i] = schedule.request
	}
	b.scheduler.leaveWaitlist(requests...)

	workerRequests := make([]*types.ContainerRequest, len(schedules))
	for i, schedule := range schedules {
		workerRequests[i] = b.scheduler.prepareWorkerRequest(schedule.worker, schedule.request)
	}
	err := b.scheduler.pushWorkerRequests(schedules[0].worker, workerRequests)
	if err == nil {
		for _, request := range workerRequests {
			go b.scheduler.schedulerUsageMetrics.CounterIncContainerScheduled(request.Clone())
		}
	}
	for _, schedule := range schedules {
		b.completeSchedule(schedule, err)
	}
}

func (b *schedulingBatch) completeSchedule(schedule plannedSchedule, err error) {
	attempt := newSchedulingAttempt(b.scheduler, schedule.request, b.workers)
	if err != nil {
		// A stale selection is expected when scheduler replicas race for the same
		// worker: replan now against a fresh snapshot, keeping the request's place
		// in line. Reclaimable capacity that moved is a capacity wait.
		reason, level := "schedule_failed", zerolog.ErrorLevel
		if errors.Is(err, repo.ErrWorkerCapacityChanged) {
			reason, level = "worker_capacity_changed", zerolog.DebugLevel
		}
		attempt.recordBacklogWait(false, reason)
		metrics.RecordSchedulerWorkerWait(b.scheduler.since(schedule.request.Timestamp), schedule.request, reason)
		workerLog(requestLog(log.WithLevel(level), schedule.request), schedule.worker).Err(err).Msg("unable to schedule planned request on worker")
		switch {
		case !attempt.runnable():
		case errors.Is(err, repo.ErrWorkerCapacityChanged):
			attempt.requeueForWorkerWaitDelay(0, reason)
		case errors.Is(err, repo.ErrEvictionVictimsChanged) || errors.Is(err, repo.ErrInsufficientEvictableCapacity):
			attempt.requeueForWorkerWaitDelay(provisioningWorkerRequeueDelay, "reclaimable_capacity_changed")
		default:
			attempt.retry(reason)
		}
		return
	}

	duration := b.scheduler.since(schedule.request.Timestamp)
	attempt.recordBacklogWait(true, "scheduled")
	b.scheduler.recordPlacement(schedule.worker, schedule.request)
	metrics.RecordRequestSchedulingDuration(duration, schedule.request)
	metrics.RecordSchedulerWorkerWait(duration, schedule.request, "scheduled")
}

// recordPlacement reports where a request landed: a container.placed event,
// and a counter by requested GPU, pool and route rank. Rank 0 is a pool the
// request asked for; anything higher is overflow into a pool that also serves
// it, so the share of placements above rank 0 is the overflow rate.
func (s *Scheduler) recordPlacement(worker *types.Worker, request *types.ContainerRequest) {
	if worker == nil || request == nil {
		return
	}

	rank, _ := s.routeFor(request).rankOf(worker)
	requestedGPUs := gpuRequestsForScheduling(request)
	if request.PoolSelector == "" {
		// A request that names its pool always lands there, and counting
		// those would add a series for every pool a workspace names.
		preferred := ""
		if len(requestedGPUs) > 0 {
			preferred = requestedGPUs[0]
		}
		metrics.RecordContainerPlacement(preferred, worker.PoolName, rank)
	}

	if s.pushComputeEvent == nil {
		return
	}
	waitMs := int64(0)
	if !request.Timestamp.IsZero() {
		waitMs = s.since(request.Timestamp).Milliseconds()
	}
	s.pushComputeEvent(types.EventComputeSchema{
		PoolName:    worker.PoolName,
		WorkerID:    worker.Id,
		MachineID:   worker.MachineId,
		ContainerID: request.ContainerId,
		Action:      types.EventComputeActionContainerPlaced,
		Attrs: map[string]string{
			types.EventComputeAttrRouteRank:    strconv.Itoa(rank),
			types.EventComputeAttrRequestedGPU: strings.Join(requestedGPUs, ","),
			types.EventComputeAttrPlacedGPU:    worker.Gpu,
			types.EventComputeAttrWaitMs:       strconv.FormatInt(waitMs, 10),
		},
	})
}

// foregroundFirst returns requests with background ones moved to the end,
// keeping the relative order within each group.
func foregroundFirst(requests []*types.ContainerRequest) []*types.ContainerRequest {
	ordered := make([]*types.ContainerRequest, 0, len(requests))
	for _, request := range requests {
		if !request.Background() {
			ordered = append(ordered, request)
		}
	}
	for _, request := range requests {
		if request.Background() {
			ordered = append(ordered, request)
		}
	}
	return ordered
}

func cloneWorker(worker *types.Worker) *types.Worker {
	if worker == nil {
		return nil
	}
	cloned := *worker
	return &cloned
}
