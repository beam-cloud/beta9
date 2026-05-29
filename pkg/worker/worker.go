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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
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
	workerEventStreamReconnectMin  time.Duration = time.Second
	workerEventStreamReconnectMax  time.Duration = 5 * time.Second
	defaultRuncStartConcurrency    int           = types.DefaultRuncStartConcurrency
	defaultGvisorStartConcurrency  int           = types.DefaultGvisorStartConcurrency
	defaultWorkerStopGracePeriodS  int64         = 30
	shutdownDrainPollInterval      time.Duration = 100 * time.Millisecond
	shutdownDrainMax               time.Duration = 5 * time.Second
	shutdownForceWait              time.Duration = 5 * time.Second
	shutdownCleanupReserve         time.Duration = 5 * time.Second
	defaultContainerStartupTimeout time.Duration = 5 * time.Minute
)

type Worker struct {
	workerId                string
	workerToken             string
	poolName                string
	machineID               string
	poolConfig              types.WorkerPoolConfig
	cpuLimit                int64
	memoryLimit             int64
	gpuType                 string
	gpuCount                uint32
	podAddr                 string
	podHostName             string
	hybridLocalTargetHost   string
	imageMountPath          string
	runtime                 runtime.Runtime
	runcRuntime             runtime.Runtime
	gvisorRuntime           runtime.Runtime
	containerServer         *ContainerRuntimeServer
	cacheManager            *WorkerCacheManager
	fileCacheManager        *FileCacheManager
	criuManager             CRIUManager
	containerNetworkManager *ContainerNetworkManager
	containerGPUManager     GPUManager
	containerMountManager   *ContainerMountManager
	imageClient             *ImageClient
	containerInstances      *common.SafeMap[*ContainerInstance]
	buildCancels            *common.SafeMap[context.CancelFunc]
	containerLock           sync.Mutex
	containerStartSem       chan struct{}
	containerStartLimit     int
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
	persistent              bool
	hybridTransport         string
	ctx                     context.Context
	cancel                  func()
	config                  types.AppConfig
}

type ContainerInstance struct {
	Id                         string
	StubId                     string
	BundlePath                 string
	Overlay                    *common.ContainerOverlay
	Spec                       *specs.Spec
	Err                        error
	ExitCode                   int
	Port                       int
	OutputWriter               *common.OutputWriter
	LogBuffer                  *common.LogBuffer
	Request                    *types.ContainerRequest
	StopReason                 types.StopContainerReason
	RuntimeStarted             bool
	RuntimePid                 int
	RuntimeStartedAt           int64
	SandboxProcessManager      *goproc.GoProcClient
	SandboxProcessManagerReady bool
	DeferredCPUQuota           *specs.LinuxCPU
	ProcessManagerReadyOnce    sync.Once
	ProcessManagerReadyChan    chan struct{}
	ContainerIp                string
	Runtime                    runtime.Runtime
	OOMWatcher                 runtime.OOMWatcher
}

func (i *ContainerInstance) signalProcessManagerReadiness(ready bool) {
	if i.SandboxProcessManagerReady && !ready {
		return
	}
	i.SandboxProcessManagerReady = ready
	if i.ProcessManagerReadyChan != nil {
		i.ProcessManagerReadyOnce.Do(func() {
			close(i.ProcessManagerReadyChan)
		})
	}
}

type ContainerOptions struct {
	BundlePath          string
	HostBindPort        int
	BindPorts           []int
	StartupPortBindings []PortBinding
	InitialSpec         *specs.Spec
	StartupStartedAt    time.Time
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
	machineID := os.Getenv("WORKER_MACHINE_ID")
	podHostName := os.Getenv("HOSTNAME")
	persistent := envBool("WORKER_PERSISTENT") || envBool("HYBRID_WORKER")
	hybridTransport := os.Getenv("HYBRID_TRANSPORT")
	if hybridTransport == "" && persistent {
		hybridTransport = types.BackendRouteTransportTSNet
	}
	hybridLocalTargetHost := os.Getenv("HYBRID_LOCAL_TARGET_HOST")

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

	eventRepo := repo.NewEventClientRepo(config)

	poolConfig, poolFound := config.Worker.Pools[workerPoolName]
	if !poolFound {
		return nil, errors.New("invalid worker pool name")
	}

	var cacheManager *WorkerCacheManager
	var cacheClient *cache.Client
	if config.Cache.Enabled && config.Worker.CacheEnabled {
		cacheManager = NewWorkerCacheManager(ctx, config, poolConfig, workerRepoClient, workerId, workerPoolName, podAddr)
		cacheClient, err = cacheManager.Start()
		if err != nil {
			log.Warn().Err(err).Msg("cache unavailable, performance may be degraded")
			cacheClient = nil
			cacheManager = nil
		}
	}

	// Create container runtimes based on pool configuration
	// Always create runc as a fallback
	runcRuntime, err := runtime.New(runtime.Config{
		Type:  types.ContainerRuntimeRunc.String(),
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
		runtimeType = types.ContainerRuntimeRunc.String()
	}

	log.Info().
		Str("pool", workerPoolName).
		Str("runtime", runtimeType).
		Msg("initializing container runtime for worker pool")

	switch runtimeType {
	case types.ContainerRuntimeRunc.String():
		defaultRuntime = runcRuntime
	case types.ContainerRuntimeGvisor.String():
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
			Type:           types.ContainerRuntimeGvisor.String(),
			RunscPath:      "runsc",
			RunscRoot:      gvisorRoot,
			RunscPlatform:  gvisorPlatform,
			RunscExtraArgs: poolConfig.ContainerRuntimeConfig.GVisorExtraArgs,
			Debug:          config.DebugMode,
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

	containerStartLimit := containerStartLimitForPoolRuntime(poolConfig, config.Worker.ContainerRuntime, defaultRuntime.Name(), cpuLimit)

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
	imageClient.eventRepo = eventRepo

	var criuManager CRIUManager = nil
	var checkpointStorage storage.Storage = nil
	if pool, ok := config.Worker.Pools[workerPoolName]; ok && pool.CRIUEnabled {
		criuManager, err = InitializeCRIUManager(ctx, config.Worker.CRIU)
		if err != nil {
			log.Warn().Str("worker_id", workerId).Msgf("C/R unavailable, failed to create CRIU manager: %v", err)
		}
	}

	containerNetworkManager, err := NewContainerNetworkManager(ctx, workerId, workerPoolName, workerRepoClient, containerRepoClient, eventRepo, config, containerInstances, poolConfig, containerStartLimit)
	if err != nil {
		cancel()
		return nil, err
	}

	worker := &Worker{
		ctx:                     ctx,
		workerId:                workerId,
		workerToken:             workerToken,
		poolName:                workerPoolName,
		machineID:               machineID,
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
		cacheManager:            cacheManager,
		storageManager:          storageManager,
		fileCacheManager:        fileCacheManager,
		containerGPUManager:     NewContainerNvidiaManager(uint32(gpuCount)),
		containerNetworkManager: containerNetworkManager,
		containerMountManager:   NewContainerMountManager(config),
		podAddr:                 podAddr,
		hybridLocalTargetHost:   hybridLocalTargetHost,
		imageClient:             imageClient,
		criuManager:             criuManager,
		podHostName:             podHostName,
		containerInstances:      containerInstances,
		buildCancels:            common.NewSafeMap[context.CancelFunc](),
		containerLock:           sync.Mutex{},
		containerStartSem:       make(chan struct{}, containerStartLimit),
		containerStartLimit:     containerStartLimit,
		containerWg:             sync.WaitGroup{},
		containerLogger: &ContainerLogger{
			containerInstances: containerInstances,
			eventRepo:          eventRepo,
			workerID:           workerId,
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
		persistent:          persistent,
		hybridTransport:     hybridTransport,
	}

	containerServer, err := NewContainerRuntimeServer(&ContainerRuntimeServerOpts{
		PodAddr:                 podAddr,
		Runtime:                 defaultRuntime,
		ContainerInstances:      containerInstances,
		ImageClient:             imageClient,
		ContainerRepoClient:     containerRepoClient,
		ContainerNetworkManager: containerNetworkManager,
		BackendRoute:            worker.backendRouteFor,
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
				if !request.Timestamp.IsZero() {
					s.recordContainerLifecycle(s.ctx, request, containerLifecycleFromDuration(types.ContainerLifecycleWorkerQueueReceive, request, request.Timestamp, time.Since(request.Timestamp), true, map[string]string{
						"worker_id": s.workerId,
					}))
				}
				s.handleContainerRequest(request)
			}

			if exit := s.shouldShutDown(lastContainerRequest); exit {
				break containerRequestStream
			}
		}
	}

	return s.shutdown()
}

func containerStartLimitForRuntime(runtimeType string) int {
	return containerStartLimitForRuntimeWithDefaults(runtimeType, defaultRuncStartConcurrency, defaultGvisorStartConcurrency)
}

func containerStartLimitForRuntimeWithDefaults(runtimeType string, runcLimit, gvisorLimit int) int {
	return containerStartLimitWithEnvOverride(defaultContainerStartLimitForRuntime(runtimeType, runcLimit, gvisorLimit))
}

func defaultContainerStartLimitForRuntime(runtimeType string, runcLimit, gvisorLimit int) int {
	limit := runcLimit
	if runtimeType == types.ContainerRuntimeGvisor.String() {
		limit = gvisorLimit
	}

	return limit
}

func containerStartLimitForPoolRuntime(poolConfig types.WorkerPoolConfig, globalRuntime, runtimeType string, workerCPU int64) int {
	limit := types.WorkerStartConcurrencyForPool(poolConfig, globalRuntime, runtimeType, workerCPU)
	return containerStartLimitWithEnvOverride(limit)
}

func containerStartLimitWithEnvOverride(limit int) int {
	raw := os.Getenv("WORKER_CONTAINER_START_CONCURRENCY")
	if raw == "" {
		return limit
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		log.Warn().Str("value", raw).Err(err).Msg("invalid WORKER_CONTAINER_START_CONCURRENCY")
		return limit
	}
	if parsed <= 0 {
		return limit
	}

	return parsed
}

func (s *Worker) reserveContainerInstance(request *types.ContainerRequest) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	if _, exists := s.containerInstances.Get(request.ContainerId); exists {
		return false
	}

	s.containerInstances.Set(request.ContainerId, &ContainerInstance{
		Id:        request.ContainerId,
		StubId:    request.StubId,
		LogBuffer: common.NewLogBuffer(),
		Request:   request,
		Runtime:   s.runtime,
	})

	return true
}

func (s *Worker) dropCancelledContainerRequest(request *types.ContainerRequest) bool {
	if s.containerRepoClient == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(ctx, &pb.GetContainerStateRequest{
		ContainerId: request.ContainerId,
	}))
	if err != nil {
		notFoundErr := &types.ErrContainerStateNotFound{}
		if notFoundErr.From(err) {
			log.Info().Str("container_id", request.ContainerId).Msg("dropping container request because state is missing")
			s.completedRequests <- request
			return true
		}

		log.Warn().Str("container_id", request.ContainerId).Err(err).Msg("unable to check container state before start")
		return false
	}

	if resp.State == nil || types.ContainerStatus(resp.State.Status) != types.ContainerStatusStopping {
		return false
	}

	log.Info().Str("container_id", request.ContainerId).Msg("dropping container request because state is already stopping")
	if _, err := handleGRPCResponse(s.containerRepoClient.DeleteContainerState(ctx, &pb.DeleteContainerStateRequest{ContainerId: request.ContainerId})); err != nil {
		log.Debug().Str("container_id", request.ContainerId).Err(err).Msg("failed to remove stopping container state")
	}

	s.completedRequests <- request
	return true
}

// handleContainerRequest handles an individual container request.
func (s *Worker) handleContainerRequest(request *types.ContainerRequest) {
	if s.dropCancelledContainerRequest(request) {
		return
	}

	if !s.reserveContainerInstance(request) {
		return
	}

	go s.runContainerRequest(request)
}

func (s *Worker) runContainerRequest(request *types.ContainerRequest) {
	containerId := request.ContainerId
	log.Info().Str("container_id", containerId).Msg("running container")

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	if request.IsBuildRequest() {
		s.registerBuildCancel(containerId, cancel)
		defer s.unregisterBuildCancel(containerId)
		go s.cancelBuildIfAlreadyStopping(ctx, cancel, containerId)
	}

	run := func() error {
		if s.containerMountManager.RequiresWorkspaceStorageMount(request) {
			log.Info().Str("container_id", containerId).Msg("mounting workspace storage")
			if _, err := s.storageManager.Mount(request.Workspace.Name, request.Workspace.Storage); err != nil {
				log.Error().Str("container_id", containerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to mount workspace storage")
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		return s.RunContainer(ctx, request)
	}

	var err error
	if request.IsBuildRequest() {
		err = run()
	} else {
		timeout := defaultContainerStartupTimeout
		if s.config.Worker.Failover.MaxSchedulingLatencyMs > 0 {
			timeout = time.Duration(s.config.Worker.Failover.MaxSchedulingLatencyMs) * time.Millisecond
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- run()
		}()

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case err = <-errCh:
		case <-timer.C:
			cancel()
			err = fmt.Errorf("container startup timed out after %s", timeout)
		case <-s.ctx.Done():
			cancel()
			err = fmt.Errorf("worker shutting down before container startup completed: %w", s.ctx.Err())
		}
	}

	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("unable to run container")
		s.failContainerRequest(containerId, request, err)
		return
	}
}

func (s *Worker) failContainerRequest(containerId string, request *types.ContainerRequest, runErr error) {
	// Set a non-zero exit code for the container (both in memory, and in repo)
	exitCode := 1

	serr, ok := runErr.(*types.ExitCodeError)
	if ok {
		exitCode = int(serr.ExitCode)
	}

	_, err := handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(context.Background(), &pb.SetContainerExitCodeRequest{
		ContainerId: containerId,
		ExitCode:    int32(exitCode),
	}))
	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to set exit code")
	}

	s.clearContainer(containerId, request, exitCode)
}

// cancelBuildIfAlreadyStopping checks if a build has already been cancelled and cancels the context if it has.
func (s *Worker) cancelBuildIfAlreadyStopping(ctx context.Context, cancel context.CancelFunc, containerId string) {
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
}

// listenForShutdown listens for SIGINT and SIGTERM signals and cancels the worker context
func (s *Worker) listenForShutdown() {
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)

	<-terminate
	log.Info().Msg("shutdown signal received")

	s.disableSchedulingForShutdown()
	s.cancel()
}

func (s *Worker) disableSchedulingForShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := handleGRPCResponse(s.workerRepoClient.DisableWorker(ctx, &pb.DisableWorkerRequest{
		WorkerId: s.workerId,
	})); err != nil {
		log.Warn().Err(err).Msg("failed to disable worker scheduling during shutdown")
	}
}

// Exit if there are no containers running and no containers have recently been spun up on this
// worker, or if a shutdown signal has been received.
func (s *Worker) shouldShutDown(lastContainerRequest time.Time) bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		if s.persistent {
			return false
		}
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

func envBool(key string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func (s *Worker) updateContainerStatus(request *types.ContainerRequest) error {
	ticker := time.NewTicker(containerStatusUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			done, err := s.updateContainerStatusOnce(request)
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to update container state")
			}
			if done {
				return nil
			}
		}
	}
}

func (s *Worker) updateContainerStatusOnce(request *types.ContainerRequest) (bool, error) {
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return true, nil
	}

	if instance.ExitCode >= 0 {
		log.Debug().
			Str("container_id", request.ContainerId).
			Int("exit_code", instance.ExitCode).
			Msg("container exited, stopping status heartbeat")
		return true, nil
	}

	// Stop container if it is "orphaned" - meaning it's running but has no associated state.
	getStateResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{
		ContainerId: request.ContainerId,
	}))
	if err != nil {
		notFoundErr := &types.ErrContainerStateNotFound{}
		if notFoundErr.From(err) {
			instance.StopReason = types.StopContainerReasonUnknown
			s.containerInstances.Set(request.ContainerId, instance)
			s.stopContainerChan <- stopContainerEvent{ContainerId: request.ContainerId, Kill: true}
			go s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
				ID:          types.ContainerEventWorkerOrphanStateMissing,
				ContainerID: request.ContainerId,
				Reason:      string(types.StopContainerReasonUnknown),
				Source:      types.EventSourceWorkerStatusHeartbeat.String(),
				Message:     types.EventMessageWorkerOrphanStateMissing.String(),
			})
			return true, nil
		}

		return false, nil
	}

	state := getStateResponse.State
	if state == nil {
		return false, fmt.Errorf("container state response missing state")
	}

	status := types.ContainerStatus(state.Status)

	log.Debug().Str("container_id", request.ContainerId).Str("image_id", request.ImageId).Msg("container still running")

	expirySeconds := int64(types.ContainerStateTtlS)
	if status == types.ContainerStatusPending {
		if instance.RuntimeStarted {
			log.Info().
				Str("container_id", request.ContainerId).
				Int("pid", instance.RuntimePid).
				Msg("reconciling pending container to running from runtime start signal")
			s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
				ID:          types.ContainerEventWorkerPendingReconciled,
				ContainerID: request.ContainerId,
				Source:      types.EventSourceWorkerStatusHeartbeat.String(),
				Message:     types.EventMessagePendingReconciledRunning.String(),
				Attrs: map[string]string{
					types.EventAttrRuntimePID: fmt.Sprintf("%d", instance.RuntimePid),
				},
			})
			state.Status = string(types.ContainerStatusRunning)
		} else {
			expirySeconds = int64(types.ContainerStateTtlSWhilePending)
		}
	}

	_, err = handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(context.Background(), &pb.UpdateContainerStatusRequest{
		ContainerId:   request.ContainerId,
		Status:        string(state.Status),
		ExpirySeconds: expirySeconds,
	}))
	if err != nil {
		return false, err
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
			s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
				ID:          types.ContainerEventWorkerStoppingGraceKill,
				ContainerID: request.ContainerId,
				Reason:      string(instance.StopReason),
				Source:      types.EventSourceWorkerStatusHeartbeat.String(),
				Message:     types.EventMessageStoppingGraceKill.String(),
				Attrs: map[string]string{
					types.EventAttrGracePeriodSeconds: fmt.Sprintf("%d", s.config.Worker.TerminationGracePeriod),
				},
			})
			s.stopContainerChan <- stopContainerEvent{
				ContainerId: request.ContainerId,
				Kill:        true,
			}
		}()
	}

	return false, nil
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

	go s.listenForWorkerEvents()
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
	if s.cacheManager != nil {
		if err := s.cacheManager.Drain(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to drain cache registration: %v", err))
		}
	}

	s.waitForActiveContainersBeforeShutdown()
	s.stopActiveContainersForShutdown()

	if s.containerNetworkManager != nil {
		if err := s.containerNetworkManager.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to cleanup preallocated container networks: %v", err))
		}
	}

	if _, err := handleGRPCResponse(s.workerRepoClient.RemoveWorker(context.Background(), &pb.RemoveWorkerRequest{
		WorkerId: s.workerId,
	})); err != nil {
		errs = errors.Join(errs, err)
	}

	if s.cacheManager != nil {
		err := s.cacheManager.Close()
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to cleanup cache: %v", err))
		}
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

	if err := cleanupImageMountPath(s.imageMountPath); err != nil {
		log.Warn().Str("path", s.imageMountPath).Err(err).Msg("failed to cleanup image mount path")
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

func (s *Worker) waitForActiveContainersBeforeShutdown() {
	if s.containerInstances == nil || s.containerInstances.Len() == 0 {
		return
	}

	timeout := workerShutdownDrainTimeout(s.config.Worker.TerminationGracePeriod)
	log.Info().
		Int("containers", s.containerInstances.Len()).
		Dur("timeout", timeout).
		Msg("waiting for active containers before worker shutdown")
	if s.waitForActiveContainers(timeout) {
		return
	}

	log.Warn().
		Int("containers", s.containerInstances.Len()).
		Dur("timeout", timeout).
		Msg("active containers still present after worker shutdown drain")
}

func (s *Worker) stopActiveContainersForShutdown() {
	if s.containerInstances == nil || s.containerInstances.Len() == 0 {
		return
	}

	ids := s.activeContainerIDs()
	log.Info().Int("containers", len(ids)).Msg("stopping active containers before worker shutdown")

	for _, id := range ids {
		if instance, exists := s.containerInstances.Get(id); exists {
			instance.StopReason = types.StopContainerReasonAdmin
			s.containerInstances.Set(id, instance)
		}
		if err := s.stopContainer(id, false); err != nil {
			log.Warn().Str("container_id", id).Err(err).Msg("failed to stop container during worker shutdown")
		}
	}

	grace := workerContainerStopGrace(s.config.Worker.TerminationGracePeriod)
	if s.waitForActiveContainers(grace) {
		return
	}

	remaining := s.activeContainerIDs()
	log.Warn().
		Int("containers", len(remaining)).
		Dur("grace", grace).
		Msg("force stopping active containers during worker shutdown")
	for _, id := range remaining {
		if err := s.stopContainer(id, true); err != nil {
			log.Warn().Str("container_id", id).Err(err).Msg("failed to force stop container during worker shutdown")
		}
	}

	s.waitForActiveContainers(shutdownForceWait)
}

func (s *Worker) activeContainerIDs() []string {
	ids := []string{}
	if s.containerInstances == nil {
		return ids
	}
	s.containerInstances.Range(func(key string, _ *ContainerInstance) bool {
		ids = append(ids, key)
		return true
	})
	return ids
}

func (s *Worker) waitForActiveContainers(timeout time.Duration) bool {
	if s.containerInstances == nil || s.containerInstances.Len() == 0 {
		return true
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for s.containerInstances.Len() > 0 {
			s.containerWg.Wait()
			if s.containerInstances.Len() == 0 {
				return
			}
			time.Sleep(shutdownDrainPollInterval)
		}
	}()

	if timeout <= 0 {
		<-done
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return s.containerInstances.Len() == 0
	}
}

func workerContainerStopGrace(configuredSeconds int64) time.Duration {
	if configuredSeconds <= 0 {
		configuredSeconds = defaultWorkerStopGracePeriodS
	}
	budget := time.Duration(configuredSeconds) * time.Second
	grace := budget - workerShutdownDrainTimeout(configuredSeconds) - shutdownForceWait - shutdownCleanupReserve
	if grace <= 0 {
		return budget
	}
	return grace
}

func workerShutdownDrainTimeout(configuredSeconds int64) time.Duration {
	if configuredSeconds <= 0 {
		configuredSeconds = defaultWorkerStopGracePeriodS
	}
	budget := time.Duration(configuredSeconds) * time.Second
	if budget <= 10*time.Second {
		return 0
	}
	drain := budget / 6
	if drain > shutdownDrainMax {
		return shutdownDrainMax
	}
	return drain
}
