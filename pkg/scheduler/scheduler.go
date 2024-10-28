package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
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
	workspaceRepo     repo.WorkspaceRepository
	eventRepo         repo.EventRepository
	schedulerMetrics  SchedulerMetrics
	eventBus          *common.EventBus
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, metricsRepo repo.MetricsRepository, backendRepo repo.BackendRepository, workspaceRepo repo.WorkspaceRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)

	schedulerMetrics := NewSchedulerMetrics(metricsRepo)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	// Load worker pools
	workerPoolManager := NewWorkerPoolManager()
	for name, pool := range config.Worker.Pools {
		var controller WorkerPoolController = nil
		var err error = nil

		switch pool.Mode {
		case types.PoolModeLocal:
			controller, err = NewLocalKubernetesWorkerPoolController(ctx, config, name, workerRepo, providerRepo)
		case types.PoolModeExternal:
			controller, err = NewExternalWorkerPoolController(ctx, config, name, backendRepo, workerRepo, providerRepo, tailscale, pool.Provider)
		default:
			log.Printf("no valid controller found for pool <%s> with mode: %s\n", name, pool.Mode)
			continue
		}

		if err != nil {
			log.Printf("unable to load controller <%s>: %+v\n", name, err)
			continue
		}

		workerPoolManager.SetPool(name, pool, controller)
		log.Printf("loaded controller for pool <%s> with mode: %s and GPU type: %s\n", name, pool.Mode, pool.GPUType)
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
		workspaceRepo:     workspaceRepo,
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

	quota, err := s.getConcurrencyLimit(request)
	if err != nil {
		return err
	}

	err = s.containerRepo.SetContainerStateWithConcurrencyLimit(quota, request)
	if err != nil {
		return err
	}

	return s.addRequestToBacklog(request)
}

func (s *Scheduler) getConcurrencyLimit(request *types.ContainerRequest) (*types.ConcurrencyLimit, error) {
	// First try to get the cached quota
	var quota *types.ConcurrencyLimit
	quota, err := s.workspaceRepo.GetConcurrencyLimitByWorkspaceId(request.WorkspaceId)
	if err != nil {
		return nil, err
	}

	if quota == nil {
		quota, err = s.backendRepo.GetConcurrencyLimitByWorkspaceId(s.ctx, request.WorkspaceId)
		if err != nil && err == sql.ErrNoRows {
			return nil, nil // No quota set for this workspace
		}
		if err != nil {
			return nil, err
		}

		err = s.workspaceRepo.SetConcurrencyLimitByWorkspaceId(request.WorkspaceId, quota)
		if err != nil {
			return nil, err
		}
	}

	return quota, nil
}

func (s *Scheduler) Stop(stopArgs *types.StopContainerArgs) error {
	log.Printf("Received STOP request: %+v\n", stopArgs)

	err := s.containerRepo.UpdateContainerStatus(stopArgs.ContainerId, types.ContainerStatusStopping, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second)
	if err != nil {
		return err
	}

	eventArgs, err := stopArgs.ToMap()
	if err != nil {
		return err
	}

	_, err = s.eventBus.Send(&common.Event{
		Type:          common.EventTypeStopContainer,
		Args:          eventArgs,
		LockAndDelete: false,
	})
	if err != nil {
		log.Printf("Could not stop container: %+v\n", err)
		return err
	}

	return nil
}

func (s *Scheduler) getControllers(request *types.ContainerRequest) ([]WorkerPoolController, error) {
	controllers := []WorkerPoolController{}

	if request.PoolSelector != "" {
		wp, ok := s.workerPoolManager.GetPool(request.PoolSelector)
		if !ok {
			return nil, errors.New("no controller found for request")
		}
		controllers = append(controllers, wp.Controller)
	} else if len(request.GpuRequest) == 0 {
		wp, ok := s.workerPoolManager.GetPool("default")
		if !ok {
			return nil, errors.New("no controller found for request")
		}
		controllers = append(controllers, wp.Controller)
	} else {
		for _, gpu := range request.GpuRequest {
			wp, ok := s.workerPoolManager.GetPoolByGPU(gpu)
			if ok {
				controllers = append(controllers, wp.Controller)
			}
		}
	}

	if len(controllers) == 0 {
		return nil, errors.New("no controller found for request")
	}

	return controllers, nil
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

			controllers, err := s.getControllers(request)
			if err != nil {
				log.Printf("No controller found for request: %+v, error: %v\n", request, err)
				continue
			}

			go func() {
				for _, c := range controllers {
					// Iterates through controllers in the order of prioritized gpus to attempt to add a worker
					if c == nil {
						continue
					}

					newWorker, err := c.AddWorker(request.Cpu, request.Memory, request.GpuCount)
					if err == nil {
						log.Printf("Added new worker <%s> for container %s\n", newWorker.Id, request.ContainerId)

						err = s.scheduleRequest(newWorker, request)
						if err != nil {
							log.Printf("Unable to schedule request for container<%s>: %v\n", request.ContainerId, err)
							s.addRequestToBacklog(request)
						}
						return
					}
				}

				log.Printf("Unable to add worker for container<%s>: %v\n", request.ContainerId, err)
				s.addRequestToBacklog(request)
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
	go s.eventRepo.PushContainerScheduledEvent(request.ContainerId, worker.Id, request)

	if err := s.containerRepo.UpdateAssignedContainerGPU(request.ContainerId, worker.Gpu); err != nil {
		return err
	}

	request.Gpu = worker.Gpu
	return s.workerRepo.ScheduleContainerRequest(worker, request)
}

func filterWorkersByPoolSelector(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if (request.PoolSelector != "" && worker.PoolName == request.PoolSelector) ||
			(request.PoolSelector == "" && !worker.RequiresPoolSelector) {
			filteredWorkers = append(filteredWorkers, worker)
		}
	}
	return filteredWorkers
}

func filterWorkersByResources(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	gpuRequestsMap := map[string]int{}
	requiresGPU := request.RequiresGPU()

	for index, gpu := range request.GpuRequest {
		gpuRequestsMap[gpu] = index
	}

	for _, worker := range workers {
		isGpuWorker := worker.Gpu != ""

		// Check if the worker has enough free cpu and memory to run the container
		if worker.FreeCpu < int64(request.Cpu) || worker.FreeMemory < int64(request.Memory) {
			continue
		}

		// Check if the worker has been cordoned
		if worker.Status == types.WorkerStatusDisabled {
			continue
		}

		if (requiresGPU && !isGpuWorker) || (!requiresGPU && isGpuWorker) {
			// If the worker doesn't have a GPU and the request requires one, skip
			// Likewise, if the worker has a GPU and the request doesn't require one, skip
			continue
		}

		if requiresGPU {
			// Validate GPU resource availability
			priorityModifier, validGpu := gpuRequestsMap[worker.Gpu]
			if !validGpu || worker.FreeGpuCount < request.GpuCount {
				continue
			}

			// This will account for the preset priorities for the pool type as well as the order of the GPU requests
			// NOTE: will only work properly if all GPU types and their pools start from 0 and pool priority are incremental by changes of ±1
			worker.Priority -= int32(priorityModifier)
		}

		filteredWorkers = append(filteredWorkers, worker)
	}
	return filteredWorkers
}

type scoredWorker struct {
	worker *types.Worker
	score  int32
}

// Constants used for scoring workers
const (
	scoreAvailableWorker int32 = 10
)

func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	filteredWorkers := filterWorkersByPoolSelector(workers, request)     // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request) // Filter workers resource requirements

	if len(filteredWorkers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	// Score workers based on status and priority
	scoredWorkers := []scoredWorker{}
	for _, worker := range filteredWorkers {
		score := int32(0)

		if worker.Status == types.WorkerStatusAvailable {
			score += scoreAvailableWorker
		}

		score += worker.Priority
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	// Select the worker with the highest score
	sort.Slice(scoredWorkers, func(i, j int) bool {
		// TODO: Figure out a short way to randomize order of workers with the same score

		return scoredWorkers[i].score > scoredWorkers[j].score
	})

	return scoredWorkers[0].worker, nil
}

const maxScheduleRetryCount = 3
const maxScheduleRetryDuration = 10 * time.Minute

func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	if request.RequiresGPU() {
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
		s.containerRepo.DeleteContainerState(request.ContainerId)
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
