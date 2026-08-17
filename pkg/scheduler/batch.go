package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const requestSchedulingParallelism = 128

type schedulingBatch struct {
	scheduler *Scheduler
	workers   []*types.Worker
	batchSize int

	schedules []plannedSchedule
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
	planStart := time.Now()
	planned := false
	defer func() {
		b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerBatchPlan, planStart, time.Now(), true, map[string]string{
			"batch_size":           fmt.Sprintf("%d", b.batchSize),
			"worker_count":         fmt.Sprintf("%d", len(b.workers)),
			"planned":              fmt.Sprintf("%t", planned),
			"planned_count_so_far": fmt.Sprintf("%d", len(b.schedules)),
		})
	}()
	if !runnable {
		return
	}
	normalizeGPURequest(request)

	selectionStart := time.Now()
	worker, err := b.scheduler.selectWorkerFromWorkers(b.workers, request)
	b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerWorkerSelection, selectionStart, time.Now(), err == nil && worker != nil, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(b.workers)),
	})
	if err != nil || worker == nil {
		if attempt.runnable() {
			attempt.runWaitingOrProvisioning()
		}
		return
	}

	workerForSchedule := cloneWorker(worker)
	reserveStart := time.Now()
	reserved := b.scheduler.reserveWorkerCapacity(worker, request)
	b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerReserveCapacity, reserveStart, time.Now(), reserved, map[string]string{
		"worker_id": worker.Id,
	})
	if !reserved {
		if attempt.runnable() {
			attempt.runWaitingOrProvisioning()
		}
		return
	}

	b.schedules = append(b.schedules, plannedSchedule{
		worker:  workerForSchedule,
		request: request,
	})
	planned = true
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
		workerLog(requestLog(log.Error(), schedule.request), schedule.worker).
			Err(err).
			Msg("unable to schedule planned request on worker")

		attempt.recordBacklogWait(false, "schedule_failed")
		metrics.RecordSchedulerWorkerWait(time.Since(schedule.request.Timestamp), schedule.request, "schedule_failed")
		attempt.retryIfRunnable("schedule_failed")
		return
	}
	if schedule.request.StateVolumePlanId != "" {
		if ackErr := b.scheduler.requestBacklog.AcknowledgeStateVolumePlanDispatch(schedule.request.StateVolumePlanId); ackErr != nil {
			requestLog(log.Warn(), schedule.request).Err(ackErr).Msg("failed to acknowledge state-volume batch processing lease")
		}
	}

	duration := time.Since(schedule.request.Timestamp)
	attempt.recordBacklogWait(true, "scheduled")
	b.scheduler.emitContainerPlaced(
		schedule.worker,
		schedule.request,
		b.scheduler.failoverChainFor(schedule.request),
	)
	metrics.RecordRequestSchedulingDuration(duration, schedule.request)
	metrics.RecordSchedulerWorkerWait(duration, schedule.request, "scheduled")
}

func cloneWorker(worker *types.Worker) *types.Worker {
	if worker == nil {
		return nil
	}
	cloned := *worker
	return &cloned
}
