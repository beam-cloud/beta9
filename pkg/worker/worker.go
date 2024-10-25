package worker

import (
	"context"

	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"

	common "github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	requestProcessingInterval     time.Duration = 100 * time.Millisecond
	containerStatusUpdateInterval time.Duration = 30 * time.Second

	containerLogsPath          string  = "/var/log/worker"
	defaultWorkerSpindownTimeS float64 = 300 // 5 minutes
)

type Worker struct {
	cpuLimit                int64
	memoryLimit             int64
	gpuType                 string
	gpuCount                uint32
	podAddr                 string
	podHostName             string
	imageMountPath          string
	runcHandle              runc.Runc
	runcServer              *RunCServer
	cedanaClient            *CedanaClient
	checkpointingAvailable  bool
	fileCacheManager        *FileCacheManager
	containerNetworkManager *ContainerNetworkManager
	containerCudaManager    GPUManager
	containerMountManager   *ContainerMountManager
	redisClient             *common.RedisClient
	imageClient             *ImageClient
	workerId                string
	eventBus                *common.EventBus
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerLock           sync.Mutex
	containerWg             sync.WaitGroup
	containerRepo           repo.ContainerRepository
	containerLogger         *ContainerLogger
	workerMetrics           *WorkerMetrics
	completedRequests       chan *types.ContainerRequest
	stopContainerChan       chan stopContainerEvent
	workerRepo              repo.WorkerRepository
	eventRepo               repo.EventRepository
	storage                 storage.Storage
	ctx                     context.Context
	cancel                  func()
	config                  types.AppConfig
}

type ContainerInstance struct {
	Id           string
	StubId       string
	BundlePath   string
	Overlay      *common.ContainerOverlay
	Spec         *specs.Spec
	Err          error
	ExitCode     int
	Port         int
	OutputWriter *common.OutputWriter
	LogBuffer    *common.LogBuffer
}

type ContainerOptions struct {
	BundlePath  string
	BindPort    int
	InitialSpec *specs.Spec
}

type stopContainerEvent struct {
	ContainerId string
	Kill        bool
}

func NewWorker() (*Worker, error) {
	containerInstances := common.NewSafeMap[*ContainerInstance]()

	gpuType := os.Getenv("GPU_TYPE")
	workerId := os.Getenv("WORKER_ID")
	podHostName := os.Getenv("HOSTNAME")

	podAddr, err := GetPodAddr()
	if err != nil {
		return nil, err
	}

	gpuCount, err := strconv.ParseInt(os.Getenv("GPU_COUNT"), 10, 64)
	if err != nil {
		return nil, err
	}

	cpuLimit, err := strconv.ParseInt(os.Getenv("CPU_LIMIT"), 10, 64)
	if err != nil {
		return nil, err
	}

	memoryLimit, err := strconv.ParseInt(os.Getenv("MEMORY_LIMIT"), 10, 64)
	if err != nil {
		return nil, err
	}

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Worker"))
	if err != nil {
		return nil, err
	}

	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	var cacheClient *blobcache.BlobCacheClient = nil
	if config.Worker.BlobCacheEnabled {
		cacheClient, err = blobcache.NewBlobCacheClient(context.TODO(), config.BlobCache)
		if err != nil {
			log.Printf("[WARNING] Cache unavailable, performance may be degraded: %+v\n", err)
		}
	}

	fileCacheManager := NewFileCacheManager(config, cacheClient)
	imageClient, err := NewImageClient(config, workerId, workerRepo, fileCacheManager)
	if err != nil {
		return nil, err
	}

	var cedanaClient *CedanaClient = nil
	if config.Worker.Checkpointing.Enabled {
		cedanaClient, err = NewCedanaClient(context.Background(), config.Worker.Checkpointing.Cedana, gpuType != "")
		if err != nil {
			log.Printf("[WARNING] C/R unavailable, failed to create cedana client: %v\n", err)
		}
	}

	runcServer, err := NewRunCServer(containerInstances, imageClient)
	if err != nil {
		return nil, err
	}

	err = runcServer.Start()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	containerNetworkManager, err := NewContainerNetworkManager(ctx, workerId, workerRepo, containerRepo, config)
	if err != nil {
		cancel()
		return nil, err
	}

	workerMetrics, err := NewWorkerMetrics(ctx, workerId, workerRepo, config.Monitoring)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Worker{
		ctx:                     ctx,
		cancel:                  cancel,
		config:                  config,
		imageMountPath:          getImageMountPath(workerId),
		cpuLimit:                cpuLimit,
		memoryLimit:             memoryLimit,
		gpuType:                 gpuType,
		gpuCount:                uint32(gpuCount),
		runcHandle:              runc.Runc{},
		runcServer:              runcServer,
		fileCacheManager:        fileCacheManager,
		containerCudaManager:    NewContainerNvidiaManager(uint32(gpuCount)),
		containerNetworkManager: containerNetworkManager,
		redisClient:             redisClient,
		containerMountManager:   NewContainerMountManager(config),
		podAddr:                 podAddr,
		imageClient:             imageClient,
		cedanaClient:            cedanaClient,
		checkpointingAvailable:  cedanaClient != nil,
		podHostName:             podHostName,
		eventBus:                nil,
		workerId:                workerId,
		containerInstances:      containerInstances,
		containerLock:           sync.Mutex{},
		containerWg:             sync.WaitGroup{},
		containerRepo:           containerRepo,
		containerLogger: &ContainerLogger{
			containerInstances: containerInstances,
		},
		workerMetrics:     workerMetrics,
		workerRepo:        workerRepo,
		eventRepo:         eventRepo,
		completedRequests: make(chan *types.ContainerRequest, 1000),
		stopContainerChan: make(chan stopContainerEvent, 1000),
		storage:           storage,
	}, nil
}

func (s *Worker) Run() error {
	err := s.startup()
	if err != nil {
		return err
	}

	go s.listenForShutdown()
	go s.manageWorkerCapacity()
	go s.processStopContainerEvents()

	lastContainerRequest := time.Now()
	for {
		request, err := s.workerRepo.GetNextContainerRequest(s.workerId)
		if request != nil && err == nil {
			lastContainerRequest = time.Now()
			containerId := request.ContainerId

			s.containerLock.Lock()

			_, exists := s.containerInstances.Get(containerId)
			if !exists {
				log.Printf("<%s> - running container.\n", containerId)

				err := s.RunContainer(request)
				if err != nil {
					log.Printf("Unable to run container <%s>: %v\n", containerId, err)

					// Set a non-zero exit code for the container (both in memory, and in repo)
					exitCode := 1

					serr, ok := err.(*types.ExitCodeError)
					if ok {
						exitCode = serr.ExitCode
					}

					err := s.containerRepo.SetContainerExitCode(containerId, exitCode)
					if err != nil {
						log.Printf("<%s> - failed to set exit code: %v\n", containerId, err)
					}

					s.containerLock.Unlock()
					s.clearContainer(containerId, request, time.Duration(0), exitCode)
					continue
				}
			}

			s.containerLock.Unlock()
		}

		if exit := s.shouldShutDown(lastContainerRequest); exit {
			break
		}

		time.Sleep(requestProcessingInterval)
	}

	return s.shutdown()
}

// listenForShutdown listens for SIGINT and SIGTERM signals and cancels the worker context
func (s *Worker) listenForShutdown() {
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)

	<-terminate
	log.Println("Shutdown signal received.")

	s.cancel()
}

// Exit if there are no containers running and no containers have recently been spun up on this
// worker, or if a shutdown signal has been received.
func (s *Worker) shouldShutDown(lastContainerRequest time.Time) bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		if (time.Since(lastContainerRequest).Seconds() > defaultWorkerSpindownTimeS) && s.containerInstances.Len() == 0 {
			return true
		}
		return false
	}
}

func (s *Worker) updateContainerStatus(request *types.ContainerRequest) error {
	ticker := time.NewTicker(containerStatusUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			s.containerLock.Lock()
			_, exists := s.containerInstances.Get(request.ContainerId)
			s.containerLock.Unlock()

			if !exists {
				return nil
			}

			log.Printf("<%s> - container still running: %s\n", request.ContainerId, request.ImageId)

			// Stop container if it is "orphaned" - meaning it's running but has no associated state
			state, err := s.containerRepo.GetContainerState(request.ContainerId)
			if err != nil {
				if _, ok := err.(*types.ErrContainerStateNotFound); ok {
					log.Printf("<%s> - container state not found, stopping container\n", request.ContainerId)
					s.stopContainerChan <- stopContainerEvent{ContainerId: request.ContainerId, Kill: true}
					return nil
				}

				continue
			}

			err = s.containerRepo.UpdateContainerStatus(request.ContainerId, state.Status, time.Duration(types.ContainerStateTtlS)*time.Second)
			if err != nil {
				log.Printf("<%s> - unable to update container state: %v\n", request.ContainerId, err)
			}

			// If container is supposed to be stopped, but isn't gone after TerminationGracePeriod seconds
			// ensure it is killed after that
			if state.Status == types.ContainerStatusStopping {
				go func() {
					time.Sleep(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second)

					_, exists := s.containerInstances.Get(request.ContainerId)
					if !exists {
						return
					}

					log.Printf("<%s> - container still running after stop event %ds ago - force killing\n", request.ContainerId, s.config.Worker.TerminationGracePeriod)
					s.stopContainerChan <- stopContainerEvent{
						ContainerId: request.ContainerId,
						Kill:        true,
					}
				}()
			}
		}
	}
}

func (s *Worker) processStopContainerEvents() {
	for event := range s.stopContainerChan {
		select {
		case <-s.ctx.Done():
			return
		default:
			err := s.stopContainer(event.ContainerId, event.Kill)
			if err != nil {
				s.stopContainerChan <- event
				time.Sleep(time.Second)
			}
		}
	}
}

func (s *Worker) manageWorkerCapacity() {
	for request := range s.completedRequests {
		err := s.processCompletedRequest(request)
		if err != nil {
			if _, ok := err.(*types.ErrWorkerNotFound); ok {
				s.cancel()
				return
			}

			log.Printf("Unable to process completed request: %v\n", err)
			s.completedRequests <- request
			continue
		}
	}
}

func (s *Worker) processCompletedRequest(request *types.ContainerRequest) error {
	worker, err := s.workerRepo.GetWorkerById(s.workerId)
	if err != nil {
		return err
	}

	return s.workerRepo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
}

func (s *Worker) keepalive() {
	ticker := time.NewTicker(types.WorkerKeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.workerRepo.SetWorkerKeepAlive(s.workerId)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Worker) startup() error {
	log.Println("Worker starting up.")

	err := s.workerRepo.ToggleWorkerAvailable(s.workerId)
	if err != nil {
		return err
	}

	eventBus := common.NewEventBus(
		s.redisClient,
		common.EventBusSubscriber{Type: common.EventTypeStopContainer, Callback: s.handleStopContainerEvent},
	)

	s.eventBus = eventBus
	go s.eventBus.ReceiveEvents(s.ctx)
	go s.keepalive()

	err = os.MkdirAll(containerLogsPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create logs directory: %v", err)
	}

	go s.eventRepo.PushWorkerStartedEvent(s.workerId)

	return nil
}

func (s *Worker) shutdown() error {
	log.Println("Shutting down...")
	defer s.eventRepo.PushWorkerStoppedEvent(s.workerId)

	// Stops goroutines
	s.cancel()

	// Stop all running containers forcefully
	s.containerInstances.Range(func(containerId string, _ *ContainerInstance) bool {
		if err := s.stopContainer(containerId, true); err != nil {
			log.Printf("Failed to stop container: %v\n", err)
		}
		return true // continue iterating
	})

	var errs error
	if worker, err := s.workerRepo.GetWorkerById(s.workerId); err != nil {
		errs = errors.Join(errs, err)
	} else if worker != nil {
		err = s.workerRepo.RemoveWorker(worker)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	err := s.storage.Unmount(s.config.Storage.FilesystemPath)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to unmount storage: %v", err))
	}

	err = s.imageClient.Cleanup()
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to cleanup fuse mounts: %v", err))
	}

	err = os.RemoveAll(s.imageMountPath)
	if err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}
