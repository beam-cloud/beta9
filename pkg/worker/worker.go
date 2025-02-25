package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"

	common "github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"

	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	containerLogsPath              string        = "/var/log/worker"
	defaultWorkerSpindownTimeS     float64       = 300 // 5 minutes
	defaultCacheWaitTime           time.Duration = 30 * time.Second
	containerStatusUpdateInterval  time.Duration = 30 * time.Second
	containerRequestStreamInterval time.Duration = 100 * time.Millisecond
)

type Worker struct {
	workerId                string
	workerToken             string
	cpuLimit                int64
	memoryLimit             int64
	gpuType                 string
	gpuCount                uint32
	podAddr                 string
	podHostName             string
	imageMountPath          string
	runcHandle              runc.Runc
	runcServer              *RunCServer
	fileCacheManager        *FileCacheManager
	criuManager             CRIUManager
	containerNetworkManager *ContainerNetworkManager
	containerCudaManager    GPUManager
	containerMountManager   *ContainerMountManager
	redisClient             *common.RedisClient
	imageClient             *ImageClient
	eventBus                *common.EventBus
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerLock           sync.Mutex
	containerWg             sync.WaitGroup
	containerLogger         *ContainerLogger
	workerMetrics           *WorkerMetrics
	completedRequests       chan *types.ContainerRequest
	stopContainerChan       chan stopContainerEvent
	workerRepoClient        pb.WorkerRepositoryServiceClient
	containerRepoClient     pb.ContainerRepositoryServiceClient
	eventRepo               repo.EventRepository
	userDataStorage         storage.Storage
	checkpointStorage       storage.Storage
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
	Request      *types.ContainerRequest
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
	ctx, cancel := context.WithCancel(context.Background())

	containerInstances := common.NewSafeMap[*ContainerInstance]()

	gpuType := os.Getenv("GPU_TYPE")
	workerId := os.Getenv("WORKER_ID")
	workerToken := os.Getenv("WORKER_TOKEN")
	workerPoolName := os.Getenv("WORKER_POOL_NAME")
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

	containerRepoClient, err := NewContainerRepositoryClient(context.TODO(), config, workerToken)
	if err != nil {
		return nil, err
	}

	workerRepoClient, err := NewWorkerRepositoryClient(context.TODO(), config, workerToken)
	if err != nil {
		return nil, err
	}

	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	userDataStorage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	var cacheClient *blobcache.BlobCacheClient = nil
	if config.Worker.BlobCacheEnabled {
		cacheClient, err = blobcache.NewBlobCacheClient(ctx, config.BlobCache)
		if err == nil {
			err = cacheClient.WaitForHosts(defaultCacheWaitTime)
		}

		if err != nil {
			log.Warn().Err(err).Msg("cache unavailable, performance may be degraded")
		}
	}

	fileCacheManager := NewFileCacheManager(config, cacheClient)
	imageClient, err := NewImageClient(config, workerId, workerRepoClient, fileCacheManager)
	if err != nil {
		return nil, err
	}

	var criuManager CRIUManager = nil
	var checkpointStorage storage.Storage = nil
	if pool, ok := config.Worker.Pools[workerPoolName]; ok && pool.CRIUEnabled {
		criuManager, err = InitializeCRIUManager(context.Background(), config.Worker.CRIU, fileCacheManager)
		if err != nil {
			log.Warn().Str("worker_id", workerId).Msgf("C/R unavailable, failed to create CRIU manager: %v", err)
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

	containerNetworkManager, err := NewContainerNetworkManager(ctx, workerId, workerRepoClient, containerRepoClient, config)
	if err != nil {
		cancel()
		return nil, err
	}

	workerMetrics, err := NewWorkerMetrics(ctx, workerId, config.Monitoring)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Worker{
		ctx:                     ctx,
		workerId:                workerId,
		workerToken:             workerToken,
		cancel:                  cancel,
		config:                  config,
		imageMountPath:          getImageMountPath(workerId),
		cpuLimit:                cpuLimit,
		memoryLimit:             memoryLimit,
		gpuType:                 gpuType,
		gpuCount:                uint32(gpuCount),
		runcHandle:              runc.Runc{Debug: config.DebugMode},
		runcServer:              runcServer,
		fileCacheManager:        fileCacheManager,
		containerCudaManager:    NewContainerNvidiaManager(uint32(gpuCount)),
		containerNetworkManager: containerNetworkManager,
		containerMountManager:   NewContainerMountManager(config),
		redisClient:             redisClient,
		podAddr:                 podAddr,
		imageClient:             imageClient,
		criuManager:             criuManager,
		podHostName:             podHostName,
		eventBus:                nil,
		containerInstances:      containerInstances,
		containerLock:           sync.Mutex{},
		containerWg:             sync.WaitGroup{},
		containerLogger: &ContainerLogger{
			containerInstances: containerInstances,
			logLinesPerHour:    config.Worker.ContainerLogLinesPerHour,
		},
		workerMetrics:       workerMetrics,
		containerRepoClient: containerRepoClient,
		workerRepoClient:    workerRepoClient,
		eventRepo:           eventRepo,
		completedRequests:   make(chan *types.ContainerRequest, 1000),
		stopContainerChan:   make(chan stopContainerEvent, 1000),
		userDataStorage:     userDataStorage,
		checkpointStorage:   checkpointStorage,
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

	// Listen for container requests for this worker
containerRequestStream:
	for {
		stream, err := s.workerRepoClient.GetNextContainerRequest(s.ctx, &pb.GetNextContainerRequestRequest{
			WorkerId: s.workerId,
		})
		if err != nil {
			if s.ctx.Err() == context.Canceled {
				break
			}

			time.Sleep(containerRequestStreamInterval)
			continue
		}

		for {
			response, err := stream.Recv()
			if err != nil {
				break
			}

			if response.ContainerRequest != nil {
				lastContainerRequest = time.Now()
				request := types.NewContainerRequestFromProto(response.ContainerRequest)
				s.handleContainerRequest(request)
			}

			if exit := s.shouldShutDown(lastContainerRequest); exit {
				break containerRequestStream
			}
		}
	}

	return s.shutdown()
}

// handleContainerRequest handles an individual container request, spawning a new runc container
func (s *Worker) handleContainerRequest(request *types.ContainerRequest) {
	containerId := request.ContainerId

	s.containerLock.Lock()
	_, exists := s.containerInstances.Get(containerId)
	if !exists {
		log.Info().Str("container_id", containerId).Msg("running container")

		ctx, cancel := context.WithCancel(s.ctx)

		if request.IsBuildRequest() {
			go s.listenForStopBuildEvent(ctx, cancel, containerId)
		}

		if err := s.RunContainer(ctx, request); err != nil {
			s.containerLock.Unlock()

			log.Error().Str("container_id", containerId).Err(err).Msg("unable to run container")

			// Set a non-zero exit code for the container (both in memory, and in repo)
			exitCode := 1

			serr, ok := err.(*types.ExitCodeError)
			if ok {
				exitCode = serr.ExitCode
			}

			_, err = handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(ctx, &pb.SetContainerExitCodeRequest{
				ContainerId: containerId,
				ExitCode:    int32(exitCode),
			}))
			if err != nil {
				log.Error().Str("container_id", containerId).Err(err).Msg("failed to set exit code")
			}

			s.clearContainer(containerId, request, exitCode)
			return
		}

		s.containerLock.Unlock()
	}

}

// listenForStopBuildEvent listens for a stop build event and cancels the context
func (s *Worker) listenForStopBuildEvent(ctx context.Context, cancel context.CancelFunc, containerId string) {
	eventbus := common.NewEventBus(s.redisClient, common.EventBusSubscriber{Type: common.StopBuildEventType(containerId), Callback: func(e *common.Event) bool {
		log.Info().Str("container_id", containerId).Msg("received stop build event")
		cancel()
		return true
	}})
	go eventbus.ReceiveEvents(ctx)
}

// listenForShutdown listens for SIGINT and SIGTERM signals and cancels the worker context
func (s *Worker) listenForShutdown() {
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)

	<-terminate
	log.Info().Msg("shutdown signal received")

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
			s.cancel() // Stops goroutines
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

			// Stop container if it is "orphaned" - meaning it's running but has no associated state
			getStateResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{
				ContainerId: request.ContainerId,
			}))
			if err != nil {
				notFoundErr := &types.ErrContainerStateNotFound{}
				if notFoundErr.From(err) {
					s.stopContainerChan <- stopContainerEvent{ContainerId: request.ContainerId, Kill: true}
					return nil
				}

				continue
			}

			state := getStateResponse.State
			status := types.ContainerStatus(state.Status)

			log.Info().Str("container_id", request.ContainerId).Str("image_id", request.ImageId).Msg("container still running")

			// TODO: remove this hotfix
			if status == types.ContainerStatusPending {
				log.Info().Str("container_id", request.ContainerId).Msg("forcing container status to running")
				state.Status = string(types.ContainerStatusRunning)
			}

			_, err = handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(context.Background(), &pb.UpdateContainerStatusRequest{
				ContainerId:   request.ContainerId,
				Status:        string(state.Status),
				ExpirySeconds: int64(types.ContainerStateTtlS),
			}))
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to update container state")
			}

			// If container is supposed to be stopped, but isn't gone after TerminationGracePeriod seconds
			// ensure it is killed after that
			if status == types.ContainerStatusStopping {
				go func() {
					time.Sleep(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second)

					_, exists := s.containerInstances.Get(request.ContainerId)
					if !exists {
						return
					}

					log.Info().Str("container_id", request.ContainerId).Int64("grace_period_seconds", s.config.Worker.TerminationGracePeriod).Msg("container still running after stop event")
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
				time.Sleep(time.Second)
			}
		}
	}
}

func (s *Worker) manageWorkerCapacity() {
	for request := range s.completedRequests {
		err := s.processCompletedRequest(request)
		notFoundErr := &types.ErrWorkerNotFound{}
		if err != nil {
			if notFoundErr.From(err) {
				s.cancel()
				return
			}

			log.Error().Err(err).Msg("unable to process completed request")
			s.completedRequests <- request
			time.Sleep(time.Second)
			continue
		}
	}
}

func (s *Worker) processCompletedRequest(request *types.ContainerRequest) error {
	_, err := handleGRPCResponse(s.workerRepoClient.UpdateWorkerCapacity(s.ctx, &pb.UpdateWorkerCapacityRequest{
		WorkerId:         s.workerId,
		CapacityChange:   int64(types.AddCapacity),
		ContainerRequest: request.ToProto(),
	}))
	if err != nil {
		return err
	}

	return nil
}

func (s *Worker) keepalive() {
	ticker := time.NewTicker(types.WorkerKeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.workerRepoClient.SetWorkerKeepAlive(s.ctx, &pb.SetWorkerKeepAliveRequest{
				WorkerId: s.workerId,
			})
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Worker) startup() error {
	log.Info().Msg("worker starting up")

	_, err := handleGRPCResponse(s.workerRepoClient.ToggleWorkerAvailable(s.ctx, &pb.ToggleWorkerAvailableRequest{
		WorkerId: s.workerId,
	}))
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
	log.Info().Msg("shutting down")
	defer s.eventRepo.PushWorkerStoppedEvent(s.workerId)

	var errs error
	if _, err := handleGRPCResponse(s.workerRepoClient.RemoveWorker(context.Background(), &pb.RemoveWorkerRequest{
		WorkerId: s.workerId,
	})); err != nil {
		errs = errors.Join(errs, err)
	}

	err := s.userDataStorage.Unmount(s.config.Storage.FilesystemPath)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to unmount data storage: %v", err))
	}

	if s.checkpointStorage != nil {
		err = s.checkpointStorage.Unmount(s.config.Worker.CRIU.Storage.MountPath)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to unmount checkpoint storage: %v", err))
		}
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
