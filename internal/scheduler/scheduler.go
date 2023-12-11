package scheduler

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	repo "github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/samber/lo"
)

const (
	RequestProcessingInterval time.Duration = 100 * time.Millisecond
)

type Scheduler struct {
	workerRepo        repo.WorkerRepository
	workerPoolManager *WorkerPoolManager
	requestBacklog    *RequestBacklog
	containerRepo     repo.ContainerRepository
	taskRepo          repo.TaskRepository
	beamRepo          repo.BeamRepository
	metricsRepo       repo.MetricsStatsdRepository
	eventBus          *common.EventBus
	redisClient       *common.RedisClient
}

func NewScheduler() (*Scheduler, error) {
	redisClient, err := common.NewRedisClient(common.WithClientName("BeamScheduler"))
	if err != nil {
		return nil, err
	}

	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)
	taskRepo := repo.NewTaskRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)

	controllerFactory := KubernetesWorkerPoolControllerFactory()
	controllerFactoryConfig, err := NewKubernetesWorkerPoolControllerConfig(workerRepo)
	if err != nil {
		return nil, err
	}

	workerPoolManager := NewWorkerPoolManager(workerPoolRepo)
	workerPoolResources, err := GetWorkerPoolResources(controllerFactoryConfig.Namespace)
	if err != nil {
		return nil, err
	}

	err = workerPoolManager.LoadPools(controllerFactory, controllerFactoryConfig, workerPoolResources)
	if err != nil {
		return nil, err
	}

	beamRepo, err := repo.NewBeamPostgresRepository()
	if err != nil {
		return nil, err
	}

	return &Scheduler{
		beamRepo:          beamRepo,
		eventBus:          eventBus,
		workerRepo:        workerRepo,
		workerPoolManager: workerPoolManager,
		requestBacklog:    requestBacklog,
		containerRepo:     containerRepo,
		taskRepo:          taskRepo,
		metricsRepo:       repo.NewMetricsStatsdRepository(),
		redisClient:       redisClient,
	}, nil
}

func (wb *Scheduler) Run(request *types.ContainerRequest) error {
	log.Printf("Received RUN request: %+v\n", request)

	request.Timestamp = time.Now()
	request.OnScheduleChan = make(chan bool, 1)

	containerState, err := wb.containerRepo.GetContainerState(request.ContainerId)
	if err == nil {
		switch types.ContainerStatus(containerState.Status) {
		case types.ContainerStatusPending, types.ContainerStatusRunning:
			return &types.ContainerAlreadyScheduledError{Msg: "a container with this id is already running or pending"}
		default:
			// Do nothing
		}
	}

	wb.metricsRepo.ContainerRequested(request.ContainerId)

	err = wb.containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
		Status:      types.ContainerStatusPending,
		ScheduledAt: time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	return wb.addRequestToBacklog(request)
}

func (wb *Scheduler) Stop(containerId string) error {
	log.Printf("Received STOP request: %s\n", containerId)

	err := wb.containerRepo.UpdateContainerStatus(containerId, types.ContainerStatusStopping, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second)
	if err != nil {
		return err
	}

	_, err = wb.eventBus.Send(&common.Event{
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

// Get a controller.
// When an agent is provided by the user, we attempt to find a worker pool controller associated with that
// agent. If we don't find a controller, we default to returning a Beam hosted controller.
func (wb *Scheduler) getController(request *types.ContainerRequest) (WorkerPoolController, error) {
	if request.Agent != "" {
		controller, err := wb.getRemoteController(request)
		if err != nil {
			log.Printf("unable to find remote controllers for user-specified agent <%v>: %v", request.Agent, err)
		} else {
			return controller, nil
		}
	}

	return wb.getHostedController(request)
}

func (wb *Scheduler) getHostedController(request *types.ContainerRequest) (WorkerPoolController, error) {
	poolName := "beam-cpu"

	if request.Gpu != "" {
		switch types.GPUType(request.Gpu) {
		case types.GPU_T4, types.GPU_A10G:
			poolName = fmt.Sprintf("beam-%s", strings.ToLower(request.Gpu))
		case types.GPU_L4, types.GPU_A100_40, types.GPU_A100_80:
			poolName = fmt.Sprintf("beam-%s-gcp", strings.ToLower(request.Gpu))
		default:
			return nil, errors.New("unsupported gpu")
		}
	}

	workerPool, ok := wb.workerPoolManager.GetPool(poolName)
	if !ok {
		return nil, fmt.Errorf("no controller found for worker pool name: %s", poolName)
	}

	return workerPool.Controller, nil
}

func (wb *Scheduler) getRemoteController(request *types.ContainerRequest) (WorkerPoolController, error) {
	agentName := request.Agent

	agent, err := wb.beamRepo.GetAgent(agentName, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get agent <%s> from database: %v", agentName, err)
	}

	if !agent.IsOnline {
		return nil, errors.New("unable to use worker pools because agent is not online")
	}

	pools, err := agent.GetPools()
	if err != nil {
		return nil, fmt.Errorf("failed to parse pools on agent <%s>: %v", agentName, err)
	}

	if len(pools) == 0 {
		return nil, fmt.Errorf("unable to find worker pools because agent <%s> does not have any pools", agentName)
	}

	for poolName := range pools {
		workerPool, ok := wb.workerPoolManager.GetPool(poolName)
		if ok {
			return workerPool.Controller, nil
		}
	}

	return nil, fmt.Errorf("unable to find controllers for agent <%s>", agentName)
}

func (wb *Scheduler) processRequests() {
	for {
		if wb.requestBacklog.Len() == 0 {
			time.Sleep(RequestProcessingInterval)
			continue
		}

		request, err := wb.requestBacklog.Pop()
		if err != nil {
			time.Sleep(RequestProcessingInterval)
			continue
		}

		// Find a worker to schedule ContainerRequests on
		worker, err := wb.selectWorker(request)
		if err != nil || worker == nil {
			// We didn't find a Worker that fit the ContainerRequest's requirements. Let's find a controller
			// so we can add a new worker.
			controller, err := wb.getController(request)
			if err != nil {
				log.Printf("No controller found for request: %+v, error: %v\n", request, err)
				continue
			}

			newWorker, err := controller.AddWorker(request.Cpu, request.Memory, request.Gpu)
			if err != nil {
				log.Printf("Unable to add job for worker: %+v\n", err)
				wb.addRequestToBacklog(request)
				continue
			}

			log.Printf("Added new worker <%s> for container %s\n", newWorker.Id, request.ContainerId)
			err = wb.scheduleRequest(newWorker, request)
			if err != nil {
				log.Printf("Unable to schedule request for container %s: %v\n", request.ContainerId, err)
				wb.addRequestToBacklog(request)
			}

			continue
		}

		// We found a worker with that met the ContainerRequest's requirements. Schedule the request
		// on that worker.
		err = wb.scheduleRequest(worker, request)
		if err != nil {
			wb.addRequestToBacklog(request)
		}

	}
}

func (wb *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	wb.metricsRepo.ContainerScheduled(request.ContainerId)
	return wb.workerRepo.ScheduleContainerRequest(worker, request)
}

func (wb *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	workers, err := wb.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	// When agent is present, filter workers with agents
	if request.Agent != "" {
		workers = lo.Filter(workers, func(w *types.Worker, _ int) bool {
			return w.Agent == request.Agent
		})
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

func (wb *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	return wb.requestBacklog.Push(request)
}
