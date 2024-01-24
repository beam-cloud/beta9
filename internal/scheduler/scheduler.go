package scheduler

import (
	"errors"
	"log"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	repo "github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

const (
	RequestProcessingInterval time.Duration = 100 * time.Millisecond
)

type Scheduler struct {
	workerRepo        repo.WorkerRepository
	workerPoolManager *WorkerPoolManager
	requestBacklog    *RequestBacklog
	containerRepo     repo.ContainerRepository
	schedulerMetrics  SchedulerMetrics
	eventBus          *common.EventBus
}

func NewScheduler(config types.AppConfig, redisClient *common.RedisClient, metricsRepo repo.PrometheusRepository) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)
	schedulerMetrics := NewSchedulerMetrics(metricsRepo)

	workerPoolManager := NewWorkerPoolManager(workerPoolRepo)
	for name, pool := range config.Worker.Pools {
		controller, _ := NewKubernetesWorkerPoolController(config, name, workerRepo)
		workerPoolManager.SetPool(name, pool, controller)
	}

	return &Scheduler{
		eventBus:          eventBus,
		workerRepo:        workerRepo,
		workerPoolManager: workerPoolManager,
		requestBacklog:    requestBacklog,
		containerRepo:     containerRepo,
		schedulerMetrics:  schedulerMetrics,
	}, nil
}

func (s *Scheduler) Run(request *types.ContainerRequest) error {
	log.Printf("Received RUN request: %+v\n", request)

	request.Timestamp = time.Now()
	request.OnScheduleChan = make(chan bool, 1)

	containerState, err := s.containerRepo.GetContainerState(request.ContainerId)
	if err == nil {
		switch types.ContainerStatus(containerState.Status) {
		case types.ContainerStatusPending, types.ContainerStatusRunning:
			return &types.ContainerAlreadyScheduledError{Msg: "a container with this id is already running or pending"}
		default:
			// Do nothing
		}
	}

	go s.schedulerMetrics.ContainerRequested()

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
			time.Sleep(RequestProcessingInterval)
			continue
		}

		request, err := s.requestBacklog.Pop()
		if err != nil {
			time.Sleep(RequestProcessingInterval)
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

			newWorker, err := controller.AddWorker(request.Cpu, request.Memory, request.Gpu)
			if err != nil {
				log.Printf("Unable to add job for worker: %+v\n", err)
				s.addRequestToBacklog(request)
				continue
			}

			log.Printf("Added new worker <%s> for container %s\n", newWorker.Id, request.ContainerId)
			err = s.scheduleRequest(newWorker, request)
			if err != nil {
				log.Printf("Unable to schedule request for container %s: %v\n", request.ContainerId, err)
				s.addRequestToBacklog(request)
			}

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
	go s.schedulerMetrics.ContainerScheduled()
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
		if worker.Cpu >= int64(request.Cpu) && worker.Memory >= int64(request.Memory) && worker.Gpu == request.Gpu {
			return worker, nil
		}
	}

	return nil, &types.ErrNoSuitableWorkerFound{}
}

func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	return s.requestBacklog.Push(request)
}
