package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Import for side effects
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"

	common "github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/runtime"
	pb "github.com/beam-cloud/beta9/proto"

	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	goproc "github.com/beam-cloud/goproc/pkg"
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
	poolName                string
	poolConfig              types.WorkerPoolConfig
	cpuLimit                int64
	memoryLimit             int64
	gpuType                 string
	gpuCount                uint32
	podAddr                 string
	podHostName             string
	imageMountPath          string
	runtime                 runtime.Runtime // Primary runtime (default from config)
	runcRuntime             runtime.Runtime // Always runc for fallback
	gvisorRuntime           runtime.Runtime // Optional gVisor runtime
	containerServer         *ContainerRuntimeServer
	fileCacheManager        *FileCacheManager
	criuManager             CRIUManager
	containerNetworkManager *ContainerNetworkManager
	containerGPUManager     GPUManager
	containerMountManager   *ContainerMountManager
	redisClient             *common.RedisClient
	imageClient             *ImageClient
	eventBus                *common.EventBus
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerLock           sync.Mutex
	containerWg             sync.WaitGroup
	containerLogger         *ContainerLogger
	workerUsageMetrics      *WorkerUsageMetrics
	completedRequests       chan *types.ContainerRequest
	stopContainerChan       chan stopContainerEvent
	workerRepoClient        pb.WorkerRepositoryServiceClient
	containerRepoClient     pb.ContainerRepositoryServiceClient
	backendRepoClient       pb.BackendRepositoryServiceClient
	eventRepo               repo.EventRepository
	storageManager          *WorkspaceStorageManager
	userDataStorage         storage.Storage
	checkpointStorage       storage.Storage
	ctx                     context.Context
	cancel                  func()
	config                  types.AppConfig
}

type ContainerInstance struct {
	Id                    string
	StubId                string
	BundlePath            string
	Overlay               *common.ContainerOverlay
	Spec                  *specs.Spec
	Err                   error
	ExitCode              int
	Port                  int
	OutputWriter          *common.OutputWriter
	LogBuffer             *common.LogBuffer
	Request               *types.ContainerRequest
	StopReason            types.StopContainerReason
	SandboxProcessManager *goproc.GoProcClient
	ContainerIp           string
	Runtime               runtime.Runtime     // The runtime used for this container
	OOMWatcher            *runtime.OOMWatcher // OOM watcher for this container
}

type ContainerOptions struct {
	BundlePath   string
	HostBindPort int
	BindPorts    []int
	InitialSpec  *specs.Spec
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

	backendRepoClient, err := NewBackendRepositoryClient(context.TODO(), config, workerToken)
	if err != nil {
		return nil, err
	}

	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	var cacheClient *blobcache.BlobCacheClient = nil
	if config.Worker.BlobCacheEnabled {
		cacheClient, err = blobcache.NewBlobCacheClient(ctx, config.BlobCache)
		if err == nil {
			err = cacheClient.WaitForHosts(defaultCacheWaitTime)
		}

		if err != nil {
			log.Warn().Err(err).Msg("cache unavailable, performance may be degraded")
			cacheClient = nil
		}
	}

	poolConfig, poolFound := config.Worker.Pools[workerPoolName]
	if !poolFound {
		return nil, errors.New("invalid worker pool name")
	}

	// Create container runtimes based on pool configuration
	// Always create runc as fallback
	runcRuntime, err := runtime.New(runtime.Config{
		Type:  "runc",
		Debug: config.DebugMode,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create runc runtime: %v", err)
	}

	// Create default runtime based on pool configuration
	var defaultRuntime runtime.Runtime
	var gvisorRuntime runtime.Runtime

	// Get runtime type from pool config, fall back to global config
	runtimeType := poolConfig.ContainerRuntime
	if runtimeType == "" {
		runtimeType = config.Worker.ContainerRuntime
	}
	if runtimeType == "" {
		runtimeType = "runc"
	}

	log.Info().
		Str("pool", workerPoolName).
		Str("runtime", runtimeType).
		Msg("initializing container runtime for worker pool")

	switch runtimeType {
	case "runc":
		defaultRuntime = runcRuntime
	case "gvisor":
		// Get gVisor configuration from pool config
		gvisorRoot := poolConfig.ContainerRuntimeConfig.GVisorRoot
		if gvisorRoot == "" {
			gvisorRoot = "/run/gvisor"
		}

		gvisorPlatform := poolConfig.ContainerRuntimeConfig.GVisorPlatform
		if gvisorPlatform == "" {
			gvisorPlatform = "systrap"
		}

		gvisorRuntime, err = runtime.New(runtime.Config{
			Type:          "gvisor",
			RunscPath:     "runsc",
			RunscRoot:     gvisorRoot,
			RunscPlatform: gvisorPlatform,
			Debug:         config.DebugMode,
		})
		if err != nil {
			log.Warn().Err(err).Msg("failed to create gvisor runtime, falling back to runc")
			defaultRuntime = runcRuntime
		} else {
			defaultRuntime = gvisorRuntime
			log.Info().
				Str("platform", gvisorPlatform).
				Str("root", gvisorRoot).
				Msg("gVisor runtime initialized successfully")
		}
	default:
		log.Warn().Str("runtime", runtimeType).Msg("unknown runtime type, using runc")
		defaultRuntime = runcRuntime
	}

	userDataStorage, err := storage.NewStorage(config.Storage, cacheClient)
	if err != nil {
		return nil, err
	}

	storageManager, err := NewWorkspaceStorageManager(ctx, config.Storage, poolConfig, containerInstances, cacheClient)
	if err != nil {
		return nil, err
	}

	fileCacheManager := NewFileCacheManager(config, cacheClient)
	imageClient, err := NewImageClient(config, workerId, workerRepoClient, fileCacheManager)
	if err != nil {
		return nil, err
	}

	var criuManager CRIUManager = nil
	var checkpointStorage storage.Storage = nil
	if pool, ok := config.Worker.Pools[workerPoolName]; ok && pool.CRIUEnabled {
		criuManager, err = InitializeCRIUManager(ctx, config.Worker.CRIU)
		if err != nil {
			log.Warn().Str("worker_id", workerId).Msgf("C/R unavailable, failed to create CRIU manager: %v", err)
		}
	}

	containerNetworkManager, err := NewContainerNetworkManager(ctx, workerId, workerRepoClient, containerRepoClient, config, containerInstances)
	if err != nil {
		cancel()
		return nil, err
	}

	worker := &Worker{
		ctx:                     ctx,
		workerId:                workerId,
		workerToken:             workerToken,
		poolName:                workerPoolName,
		poolConfig:              poolConfig,
		cancel:                  cancel,
		config:                  config,
		imageMountPath:          getImageMountPath(workerId),
		cpuLimit:                cpuLimit,
		memoryLimit:             memoryLimit,
		gpuType:                 gpuType,
		gpuCount:                uint32(gpuCount),
		runtime:                 defaultRuntime,
		runcRuntime:             runcRuntime,
		gvisorRuntime:           gvisorRuntime,
		storageManager:          storageManager,
		fileCacheManager:        fileCacheManager,
		containerGPUManager:     NewContainerNvidiaManager(uint32(gpuCount)),
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
		containerRepoClient: containerRepoClient,
		workerRepoClient:    workerRepoClient,
		backendRepoClient:   backendRepoClient,
		eventRepo:           eventRepo,
		completedRequests:   make(chan *types.ContainerRequest, 1000),
		stopContainerChan:   make(chan stopContainerEvent, 1000),
		userDataStorage:     userDataStorage,
		checkpointStorage:   checkpointStorage,
	}

	containerServer, err := NewContainerRuntimeServer(&ContainerRuntimeServerOpts{
		PodAddr:                 podAddr,
		Runtime:                 defaultRuntime,
		ContainerInstances:      containerInstances,
		ImageClient:             imageClient,
		ContainerRepoClient:     containerRepoClient,
		ContainerNetworkManager: containerNetworkManager,
		CreateCheckpoint:        worker.createCheckpoint,
	})
	if err != nil {
		cancel()
		return nil, err
	}

	err = containerServer.Start()
	if err != nil {
		cancel()
		return nil, err
	}

	workerMetrics, err := NewWorkerUsageMetrics(ctx, workerId, config.Monitoring, gpuType)
	if err != nil {
		cancel()
		return nil, err
	}

	worker.workerUsageMetrics = workerMetrics
	worker.containerServer = containerServer

	return worker, nil
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

	// Listen for container requests
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
			go s.checkForStoppedBuilds(ctx, cancel, containerId)
		}

		// If isolated workspace storage is available, mount it
		if request.StorageAvailable() {
			log.Info().Str("container_id", containerId).Msg("mounting workspace storage")

			_, err := s.storageManager.Mount(request.Workspace.Name, request.Workspace.Storage)
			if err != nil {
				log.Error().Str("container_id", containerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to mount workspace storage")
				return
			}
		}

		if err := s.RunContainer(ctx, request); err != nil {
			s.containerLock.Unlock()

			log.Error().Str("container_id", containerId).Err(err).Msg("unable to run container")

			// Set a non-zero exit code for the container (both in memory, and in repo)
			exitCode := 1

			serr, ok := err.(*types.ExitCodeError)
			if ok {
				exitCode = int(serr.ExitCode)
			}

			_, err = handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(context.Background(), &pb.SetContainerExitCodeRequest{
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

// checkForStoppedBuilds checks if a build has been cancelled and cancels the context if it has.
// If not it will listen for a stop build event.
func (s *Worker) checkForStoppedBuilds(ctx context.Context, cancel context.CancelFunc, containerId string) {
	containerState, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{ContainerId: containerId}))
	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to get container state")
		return
	}

	if types.ContainerStatus(containerState.State.Status) == types.ContainerStatusStopping {
		log.Info().Str("container_id", containerId).Msg("incoming container state is stopping, cancelling context")
		cancel()
		return
	}

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
			err := s.storageManager.Cleanup()
			if err != nil {
				log.Error().Err(err).Msg("failed to cleanup workspace storage")
			}

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
	_, err := handleGRPCResponse(s.workerRepoClient.UpdateWorkerCapacity(context.Background(), &pb.UpdateWorkerCapacityRequest{
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

func (s *Worker) profile() {
	if !s.config.DebugMode {
		return
	}

	port, err := getRandomFreePort()
	if err != nil {
		log.Error().Err(err).Msg("failed to get random free port for pprof server")
		return
	}

	srv := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
	}

	go func() {
		log.Info().Msgf("starting pprof server on :%d", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("pprof server error")
		}
	}()

	go func() {
		<-s.ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Error().Err(err).Msg("error shutting down pprof server")
		}
	}()
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

	s.profile()

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

	err = s.storageManager.Cleanup()
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to cleanup workspace storage: %v", err))
	}

	err = os.RemoveAll(s.imageMountPath)
	if err != nil {
		errs = errors.Join(errs, err)
	}

	// Close runtimes
	if s.runcRuntime != nil {
		s.runcRuntime.Close()
	}
	if s.gvisorRuntime != nil {
		s.gvisorRuntime.Close()
	}

	return errs
}

