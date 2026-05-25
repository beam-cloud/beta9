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

	schedules []plannedSchedule
}

type plannedSchedule struct {
	worker  *types.Worker
	request *types.ContainerRequest
}

func newSchedulingBatch(scheduler *Scheduler, workers []*types.Worker) *schedulingBatch {
	return &schedulingBatch{
		scheduler: scheduler,
		workers:   workers,
		schedules: make([]plannedSchedule, 0),
	}
}

func (s *Scheduler) processRequestBatch(requests []*types.ContainerRequest, workers []*types.Worker) {
	batch := newSchedulingBatch(s, workers)
	batch.plan(requests)
	batch.dispatch()
}

func (b *schedulingBatch) plan(requests []*types.ContainerRequest) {
	for _, request := range requests {
		b.planRequest(request)
	}
}

func (b *schedulingBatch) planRequest(request *types.ContainerRequest) {
	normalizeGPURequest(request)

	selectionStart := time.Now()
	worker, err := b.scheduler.selectWorkerFromWorkers(b.workers, request)
	b.scheduler.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerWorkerSelection, selectionStart, time.Now(), err == nil && worker != nil, map[string]string{
		"candidate_workers": fmt.Sprintf("%d", len(b.workers)),
	})
	if err != nil || worker == nil {
		newSchedulingAttempt(b.scheduler, request, b.workers).runWaitingOrProvisioning()
		return
	}

	workerForSchedule := cloneWorker(worker)
	if !reserveWorkerCapacity(worker, request) {
		newSchedulingAttempt(b.scheduler, request, b.workers).runWaitingOrProvisioning()
		return
	}

	b.schedules = append(b.schedules, plannedSchedule{
		worker:  workerForSchedule,
		request: request,
	})
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
			for _, schedule := range schedules {
				b.dispatchSchedule(schedule)
			}
		}()
	}

	wg.Wait()
}

func (b *schedulingBatch) dispatchSchedule(schedule plannedSchedule) {
	if err := b.scheduler.scheduleRequest(schedule.worker, schedule.request); err != nil {
		log.Error().
			Str("container_id", schedule.request.ContainerId).
			Str("worker_id", schedule.worker.Id).
			Err(err).
			Msg("unable to schedule planned request on worker")

		attempt := newSchedulingAttempt(b.scheduler, schedule.request, b.workers)
		attempt.recordBacklogWait(false, "schedule_failed")
		metrics.RecordSchedulerWorkerWait(time.Since(schedule.request.Timestamp), schedule.request, "schedule_failed")
		attempt.retrySoon("schedule_failed")
		return
	}

	duration := time.Since(schedule.request.Timestamp)
	attempt := newSchedulingAttempt(b.scheduler, schedule.request, b.workers)
	attempt.recordBacklogWait(true, "scheduled")
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
