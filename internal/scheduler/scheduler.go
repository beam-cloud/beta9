package scheduler

import (
	"context"
	"errors"
	"log"
	"math"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	repo "github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

const (
	requestProcessingInterval time.Duration = 100 * time.Millisecond
)

type Scheduler struct {
	ctx               context.Context
	backendRepo       repo.BackendRepository
	workerRepo        repo.WorkerRepository
	workerPoolManager *WorkerPoolManager
	requestBacklog    *RequestBacklog
	containerRepo     repo.ContainerRepository
	eventRepo         repo.EventRepository
	schedulerMetrics  SchedulerMetrics
	eventBus          *common.EventBus
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, metricsRepo repo.MetricsRepository, backendRepo repo.BackendRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)

	schedulerMetrics := NewSchedulerMetrics(metricsRepo)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	// Load worker pools
	workerPoolManager := NewWorkerPoolManager(workerPoolRepo)
	for name, pool := range config.Worker.Pools {
		var controller WorkerPoolController = nil
		var err error = nil

		switch pool.Mode {
		case types.PoolModeLocal:
			controller, err = NewLocalKubernetesWorkerPoolController(ctx, config, name, workerRepo)
		case types.PoolModeMetal:
			controller, err = NewMetalWorkerPoolController(ctx, config, name, backendRepo, workerRepo, workerPoolRepo, providerRepo, tailscale, pool.Provider)
		default:
			log.Printf("no valid controller found for pool <%s> with mode: %s\n", name, pool.Mode)
			continue
		}

		if err != nil {
			log.Printf("unable to load controller <%s>: %+v\n", name, err)
			continue
		}

		workerPoolManager.SetPool(name, pool, controller)
	}

	return &Scheduler{
		ctx:               ctx,
		eventBus:          eventBus,
		backendRepo:       backendRepo,
		workerRepo:        workerRepo,
		workerPoolManager: workerPoolManager,
		requestBacklog:    requestBacklog,
		containerRepo:     containerRepo,
		schedulerMetrics:  schedulerMetrics,
		eventRepo:         eventRepo,
	}, nil
}

func (s *Scheduler) Run(request *types.ContainerRequest) error {
	log.Printf("Received RUN request: %+v\n", request)

	request.Timestamp = time.Now()

	containerState, err := s.containerRepo.GetContainerState(request.ContainerId)
	if err == nil {
		switch types.ContainerStatus(containerState.Status) {
		case types.ContainerStatusPending, types.ContainerStatusRunning:
			return &types.ContainerAlreadyScheduledError{Msg: "a container with this id is already running or pending"}
		default:
			// Do nothing
		}
	}

	go s.schedulerMetrics.CounterIncContainerRequested(request)
	go s.eventRepo.PushContainerRequestedEvent(request)

	err = s.containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
		Status:      types.ContainerStatusPending,
		ScheduledAt: time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	return s.addRequestToBacklog(request)
}

func (s *Scheduler) Stop(containerId string) error {
	log.Printf("Received STOP request: %s\n", containerId)

	err := s.containerRepo.UpdateContainerStatus(containerId, types.ContainerStatusStopping, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second)
	if err != nil {
		return err
	}

	_, err = s.eventBus.Send(&common.Event{
		Type: common.EventTypeStopContainer,
		Args: map[string]any{
			"container_id": containerId,
		},
		LockAndDelete: false,
	})
	if err != nil {
		log.Printf("Could not stop container: %+v\n", err)
		return err
	}

	return nil
}

func (s *Scheduler) getController(request *types.ContainerRequest) (WorkerPoolController, error) {
	var ok bool
	var workerPool *WorkerPool

	if request.Gpu == "" {
		workerPool, ok = s.workerPoolManager.GetPool("default")
	} else {
		workerPool, ok = s.workerPoolManager.GetPoolByGPU(request.Gpu)
	}

	if !ok {
		return nil, errors.New("no controller found for request")
	}

	return workerPool.Controller, nil
}

func (s *Scheduler) StartProcessingRequests() {
	for {
		if s.requestBacklog.Len() == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		request, err := s.requestBacklog.Pop()
		if err != nil {
			time.Sleep(requestProcessingInterval)
			continue
		}

		// Find a worker to schedule ContainerRequests on
		worker, err := s.selectWorker(request)
		if err != nil || worker == nil {
			// We didn't find a Worker that fit the ContainerRequest's requirements. Let's find a controller
			// so we can add a new worker.
			controller, err := s.getController(request)
			if err != nil {
				log.Printf("No controller found for request: %+v, error: %v\n", request, err)
				continue
			}

			go func() {
				newWorker, err := controller.AddWorker(request.Cpu, request.Memory, request.Gpu, request.GpuCount)
				if err != nil {
					log.Printf("Unable to add worker job for container <%s>: %+v\n", request.ContainerId, err)
					s.addRequestToBacklog(request)
					return
				}

				log.Printf("Added new worker <%s> for container %s\n", newWorker.Id, request.ContainerId)
				err = s.scheduleRequest(newWorker, request)
				if err != nil {
					log.Printf("Unable to schedule request for container<%s>: %v\n", request.ContainerId, err)
					s.addRequestToBacklog(request)
				}
			}()

			continue
		}

		// We found a worker with that met the ContainerRequest's requirements. Schedule the request
		// on that worker.
		err = s.scheduleRequest(worker, request)
		if err != nil {
			s.addRequestToBacklog(request)
		}

	}
}

func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	go s.schedulerMetrics.CounterIncContainerScheduled(request)
	go s.eventRepo.PushContainerScheduledEvent(request.ContainerId, worker.Id)

	return s.workerRepo.ScheduleContainerRequest(worker, request)
}

func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	// Sort workers: available first, then pending
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].Status < workers[j].Status
	})

	for _, worker := range workers {
		if worker.Cpu >= int64(request.Cpu) && worker.Memory >= int64(request.Memory) && worker.Gpu == request.Gpu && worker.GpuCount >= request.GpuCount {
			return worker, nil
		}
	}

	return nil, &types.ErrNoSuitableWorkerFound{}
}

const maxScheduleRetryCount = 3
const maxScheduleRetryDuration = 10 * time.Minute

func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	if request.Gpu != "" && request.GpuCount <= 0 {
		request.GpuCount = 1
	}

	if request.RetryCount == 0 {
		request.RetryCount++
		return s.requestBacklog.Push(request)
	}

	// TODO: add some sort of signaling mechanism to alert the caller if the request failed to be pushed to the requestBacklog
	go func() {
		if request.RetryCount < maxScheduleRetryCount && time.Since(request.Timestamp) < maxScheduleRetryDuration {
			delay := calculateBackoffDelay(request.RetryCount)
			time.Sleep(delay)
			request.RetryCount++
			s.requestBacklog.Push(request)
			return
		}

		log.Printf("Giving up on request <%s> after %d attempts or due to max retry duration exceeded\n", request.ContainerId, request.RetryCount)
	}()

	return nil
}

func calculateBackoffDelay(retryCount int) time.Duration {
	if retryCount == 0 {
		return 0
	}

	baseDelay := 5 * time.Second
	maxDelay := 30 * time.Second
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
