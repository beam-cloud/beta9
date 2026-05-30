package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	requestProcessingInterval      time.Duration = 50 * time.Millisecond
	requestProcessingBatchSize                   = 512
	provisioningWorkerRequeueDelay time.Duration = 250 * time.Millisecond
	provisioningReservationHandoff time.Duration = 2 * requestProcessingInterval
	pendingWorkerReservationTTL    time.Duration = 30 * time.Second
)

type Scheduler struct {
	ctx                   context.Context
	config                types.AppConfig
	backendRepo           repo.BackendRepository
	providerRepo          repo.ProviderRepository
	workerRepo            repo.WorkerRepository
	workerPoolRepo        repo.WorkerPoolRepository
	hybridRepo            repo.HybridRepository
	workerPoolManager     *WorkerPoolManager
	requestBacklog        *RequestBacklog
	containerRepo         repo.ContainerRepository
	workspaceRepo         repo.WorkspaceRepository
	eventRepo             repo.EventRepository
	schedulerUsageMetrics SchedulerUsageMetrics
	eventBus              *common.EventBus

	provisioning *provisioningTracker
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, usageRepo repo.UsageMetricsRepository, backendRepo repo.BackendRepository, workspaceRepo repo.WorkspaceRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)
	hybridRepo := repo.NewHybridRedisRepository(redisClient)

	schedulerUsage := NewSchedulerUsageMetrics(usageRepo)
	eventRepo := repo.NewEventClientRepo(config)

	// Load worker pools
	workerPoolManager := NewWorkerPoolManager(config.Worker.Failover.Enabled)
	for name, pool := range config.Worker.Pools {
		var controller WorkerPoolController = nil
		var err error = nil

		switch pool.Mode {
		case types.PoolModeLocal:
			controller, err = NewLocalKubernetesWorkerPoolController(WorkerPoolControllerOptions{
				Context:        ctx,
				Name:           name,
				Config:         config,
				BackendRepo:    backendRepo,
				WorkerRepo:     workerRepo,
				ProviderRepo:   providerRepo,
				WorkerPoolRepo: workerPoolRepo,
				ContainerRepo:  containerRepo,
				EventRepo:      eventRepo,
			})
		case types.PoolModeExternal:
			controller, err = NewExternalWorkerPoolController(WorkerPoolControllerOptions{
				Context:        ctx,
				Name:           name,
				Config:         config,
				BackendRepo:    backendRepo,
				WorkerRepo:     workerRepo,
				ProviderRepo:   providerRepo,
				WorkerPoolRepo: workerPoolRepo,
				ContainerRepo:  containerRepo,
				ProviderName:   pool.Provider,
				Tailscale:      tailscale,
				EventRepo:      eventRepo,
			})
		case types.PoolModeHybrid:
			log.Debug().Str("pool_name", name).Msg("skipping static hybrid pool without workspace state")
			continue
		default:
			log.Error().Str("pool_name", name).Str("mode", string(pool.Mode)).Msg("no valid controller found for pool")
			continue
		}

		if err != nil {
			log.Error().Str("pool_name", name).Err(err).Msg("unable to load controller")
			continue
		}

		workerPoolManager.SetPool(name, pool, controller)
		log.Info().Str("pool_name", name).Str("mode", string(pool.Mode)).Str("gpu_type", pool.GPUType).Msg("loaded controller")
	}

	return &Scheduler{
		ctx:                   ctx,
		config:                config,
		eventBus:              eventBus,
		backendRepo:           backendRepo,
		providerRepo:          providerRepo,
		workerRepo:            workerRepo,
		workerPoolRepo:        workerPoolRepo,
		hybridRepo:            hybridRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		schedulerUsageMetrics: schedulerUsage,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,

		provisioning: newProvisioningTracker(),
	}, nil
}

func (s *Scheduler) RegisterAgentPool(workspaceID string, state *hybrid.PoolState) error {
	if s == nil || state == nil {
		return nil
	}
	name := firstNonEmpty(state.Selector, state.Name)
	if name == "" {
		return errors.New("pool selector is required")
	}

	config := normalizeAgentWorkerPoolConfig(state)
	controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context:        s.ctx,
		Name:           name,
		WorkspaceID:    workspaceID,
		Config:         s.config,
		WorkerPool:     config,
		PoolState:      state,
		WorkerRepo:     s.workerRepo,
		WorkerPoolRepo: s.workerPoolRepo,
		HybridRepo:     s.hybridRepo,
	})
	if err != nil {
		return err
	}
	s.workerPoolManager.SetPool(name, config, controller)
	return nil
}

func (s *Scheduler) DeleteAgentPool(selector string) {
	if s == nil || selector == "" {
		return
	}
	s.workerPoolManager.DeletePool(selector)
}

func normalizeAgentWorkerPoolConfig(state *hybrid.PoolState) types.WorkerPoolConfig {
	config := types.WorkerPoolConfig{
		Mode:                 types.PoolModeHybrid,
		ContainerRuntime:     types.ContainerRuntimeRunc.String(),
		RequiresPoolSelector: true,
		Priority:             int32(1000),
	}
	if state == nil {
		return config
	}
	if state.Priority != 0 {
		config.Priority = state.Priority
	}
	if state.Config != nil {
		if len(state.Config.Gpu) > 0 {
			config.GPUType = state.Config.Gpu[0]
		}
		if state.Config.Priority != 0 {
			config.Priority = state.Config.Priority
		}
	}
	return config
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (s *Scheduler) Run(request *types.ContainerRequest) error {
	log.Info().Interface("request", request).Msg("received run request")

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

	// Add checkpoint state to request if auto checkpoint is enabled and checkpoint is not set
	if request.CheckpointEnabled && request.Checkpoint == nil {
		checkpoint, err := s.backendRepo.GetLatestCheckpointByStubId(context.Background(), request.StubId)
		if err == nil && checkpoint != nil {
			log.Info().Str("container_id", request.ContainerId).Str("stub_id", request.StubId).Str("checkpoint_id", checkpoint.CheckpointId).Msg("adding checkpoint to request")
			request.Checkpoint = checkpoint
		}
	}

	requestedEvent := cloneContainerRequest(request)
	go s.schedulerUsageMetrics.CounterIncContainerRequested(requestedEvent)

	quota, err := s.getConcurrencyLimit(request)
	if err != nil {
		return err
	}

	err = s.containerRepo.SetContainerStateWithConcurrencyLimit(quota, request)
	if err != nil {
		return err
	}

	queueStart := time.Now()
	err = s.addRequestToBacklog(request)
	s.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerQueuePush, queueStart, time.Now(), err == nil, map[string]string{
		"retry_count": fmt.Sprintf("%d", request.RetryCount),
	})
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to add request to backlog")
		newSchedulingAttempt(s, request, nil).fail("backlog_push_failed")
		return err
	}

	return nil
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
	log.Info().Interface("stop_args", stopArgs).Msg("received stop request")
	reason := types.NormalizeEventReason(string(stopArgs.Reason))
	stopArgs.Reason = types.StopContainerReason(reason)
	state, _ := s.containerRepo.GetContainerState(stopArgs.ContainerId)
	event := types.EventContainerEventSchema{
		ID:          types.ContainerEventSchedulerStopRequested,
		ContainerID: stopArgs.ContainerId,
		Reason:      reason,
		Source:      types.EventSourceSchedulerStop.String(),
		Message:     types.EventMessageSchedulerStopRequested.String(),
		Attrs: map[string]string{
			types.EventAttrForce: fmt.Sprintf("%t", stopArgs.Force),
		},
	}
	if state != nil {
		event.StubID = state.StubId
		event.WorkspaceID = state.WorkspaceId
		event.Attrs[types.EventAttrPreviousStatus] = string(state.Status)
	}
	s.eventRepo.PushContainerEvent(event)

	if state != nil && strings.HasPrefix(stopArgs.ContainerId, types.BuildContainerPrefix) && types.ContainerStatus(state.Status) == types.ContainerStatusPending {
		if err := s.containerRepo.DeleteContainerState(stopArgs.ContainerId); err != nil {
			return err
		}
	} else {
		err := s.containerRepo.UpdateContainerStatus(stopArgs.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
		if err != nil {
			return err
		}
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
		log.Error().Err(err).Msg("could not stop container")
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

	} else if !request.RequiresGPU() {
		pools := s.workerPoolManager.GetPoolByFilters(poolFilters{
			GPUType: "",
		})
		for _, pool := range pools {
			controllers = append(controllers, pool.Controller)
		}
	} else {
		for _, gpu := range gpuRequestsForScheduling(request) {
			pools := s.workerPoolManager.GetPoolByFilters(poolFilters{
				GPUType: gpu,
			})

			for _, pool := range pools {
				controllers = append(controllers, pool.Controller)
			}

			// If the request contains the "any" GPU selector, we've already retrieved all pools
			if gpu == string(types.GPU_ANY) {
				break
			}
		}
	}

	controllers = filterControllersByFlags(controllers, request)
	if len(controllers) == 0 {
		return nil, errors.New("no controller found for request")
	}

	return controllers, nil
}

func (s *Scheduler) StartProcessingRequests() {
	for {
		select {
		case <-s.ctx.Done():
			// Context has been cancelled
			return
		default:
			// Continue processing requests
		}

		requests, err := s.requestBacklog.PopN(requestProcessingBatchSize)
		if err != nil {
			time.Sleep(requestProcessingInterval)
			continue
		}

		workers, err := s.workerRepo.GetAllWorkers()
		if err != nil {
			for _, request := range requests {
				newSchedulingAttempt(s, request, nil).retry("worker_list_failed")
			}
			continue
		}

		s.processRequestBatch(requests, workers)
	}
}

func (s *Scheduler) processRequest(request *types.ContainerRequest, workers []*types.Worker) {
	newSchedulingAttempt(s, request, workers).run()
}

func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	normalizeGPURequest(request)

	if err := s.containerRepo.UpdateAssignedContainerGPU(request.ContainerId, worker.Gpu); err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to update assigned container gpu")
		return err
	}

	request.Gpu = worker.Gpu

	// Attach OCI credentials for runtime lazy layer loading
	if err := s.attachImageCredentials(request); err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("image_id", request.ImageId).
			Msg("failed to attach OCI credentials, will use default provider")
	}

	// Attach build registry credentials for push + runtime layer loading
	if err := s.attachBuildRegistryCredentials(request); err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Msg("failed to attach build registry credentials to request")
	}

	workerRequest := cloneContainerRequest(request)
	workerRequest.Timestamp = time.Now()
	if err := s.workerRepo.ScheduleContainerRequest(worker, workerRequest); err != nil {
		return err
	}

	scheduledEvent := cloneContainerRequest(workerRequest)
	go s.schedulerUsageMetrics.CounterIncContainerScheduled(scheduledEvent)
	return nil
}

func cloneContainerRequest(request *types.ContainerRequest) *types.ContainerRequest {
	if request == nil {
		return nil
	}

	cloned := *request
	cloned.EntryPoint = append([]string(nil), request.EntryPoint...)
	cloned.Env = append([]string(nil), request.Env...)
	cloned.GpuRequest = append([]string(nil), request.GpuRequest...)
	cloned.Mounts = append([]types.Mount(nil), request.Mounts...)
	cloned.Ports = append([]uint32(nil), request.Ports...)
	cloned.AllowList = append([]string(nil), request.AllowList...)
	return &cloned
}

func (s *Scheduler) recordContainerLifecycle(request *types.ContainerRequest, lifecycleID types.ContainerLifecycleID, start time.Time, end time.Time, success bool, attrs map[string]string) {
	if s.eventRepo == nil || request == nil || request.ContainerId == "" || start.IsZero() || end.Before(start) {
		return
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	def := types.ContainerLifecycleDefinitionFor(lifecycleID)
	s.eventRepo.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
		ID:          lifecycleID,
		Domain:      def.Domain,
		ParentID:    def.ParentID,
		StartTime:   start.UTC(),
		EndTime:     end.UTC(),
		DurationMs:  end.Sub(start).Milliseconds(),
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      taskIDFromRequestEnv(request.Env),
		WorkspaceID: request.WorkspaceId,
		Success:     &success,
		Source:      types.EventSourceScheduler.String(),
		Attrs:       attrs,
	})
}

func taskIDFromRequestEnv(env []string) string {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "TASK_ID="); ok {
			return value
		}
	}
	return ""
}

// attachImageCredentials fetches and attaches OCI credentials to a container request
func (s *Scheduler) attachImageCredentials(request *types.ContainerRequest) error {
	if request.ImageId == "" {
		return nil
	}

	// Skip credential attachment for build containers - they already have credentials
	// in BuildOptions.SourceImageCreds for pulling the base image during the build
	if strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix) {
		return nil
	}

	secretName, _, err := s.backendRepo.GetImageCredentialSecret(context.TODO(), request.ImageId)
	if err != nil {
		log.Debug().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("image_id", request.ImageId).
			Msg("error getting image credential secret")
		return err
	}

	if secretName == "" {
		return nil
	}

	secret, err := s.backendRepo.GetSecretByNameDecrypted(context.TODO(), &request.Workspace, secretName)
	if err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("image_id", request.ImageId).
			Str("secret_name", secretName).
			Msg("failed to get secret by name")
		return err
	}

	request.ImageCredentials = secret.Value

	log.Info().
		Str("container_id", request.ContainerId).
		Str("image_id", request.ImageId).
		Str("secret_name", secretName).
		Int("credentials_length", len(secret.Value)).
		Msg("attached OCI credentials")

	return nil
}

// attachBuildRegistryCredentials generates and attaches build registry credentials to a container request
// These credentials are used for both build-time (push) and runtime (CLIP layer mounting)
func (s *Scheduler) attachBuildRegistryCredentials(request *types.ContainerRequest) error {
	buildRegistry := s.config.ImageService.BuildRegistry
	if buildRegistry == "" || isLocalBuildRegistry(buildRegistry) {
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("build_registry", buildRegistry).
			Msg("no remote build registry configured, skipping credential generation")
		return nil
	}

	// Check if we have credentials configured for the build registry.
	buildRegistryCredentials := s.config.ImageService.BuildRegistryCredentials
	dummyImageRef := fmt.Sprintf("%s/%s:dummy", buildRegistry, s.config.ImageService.BuildRepositoryName)

	var token string
	authSource := "ambient"
	if buildRegistryCredentials.Type != "" && len(buildRegistryCredentials.Credentials) > 0 {
		var err error
		token, err = reg.GetRegistryTokenForImage(dummyImageRef, buildRegistryCredentials.Credentials)
		if err != nil {
			log.Warn().
				Err(err).
				Str("container_id", request.ContainerId).
				Str("build_registry", buildRegistry).
				Str("cred_type", buildRegistryCredentials.Type).
				Msg("failed to generate build registry token from configured credentials")
		}
		if token != "" {
			authSource = buildRegistryCredentials.Type
		}
	}

	if token == "" && reg.IsECRRegistry(buildRegistry) {
		var err error
		token, err = reg.GetAmbientECRTokenForImage(context.TODO(), dummyImageRef)
		if err != nil {
			log.Warn().
				Err(err).
				Str("container_id", request.ContainerId).
				Str("build_registry", buildRegistry).
				Msg("failed to generate build registry token from ambient credentials")
			return nil
		}
	}

	if token == "" {
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("build_registry", buildRegistry).
			Str("cred_type", buildRegistryCredentials.Type).
			Msg("no token generated (public registry?), will use ambient auth")
		return nil
	}

	request.BuildRegistryCredentials = token

	log.Info().
		Str("container_id", request.ContainerId).
		Str("build_registry", buildRegistry).
		Str("auth_source", authSource).
		Msg("attached build registry credentials to request")

	return nil
}

func isLocalBuildRegistry(buildRegistry string) bool {
	registry := strings.TrimPrefix(buildRegistry, "https://")
	registry = strings.TrimPrefix(registry, "http://")
	registry = strings.Split(registry, "/")[0]

	host := registry
	if splitHost, _, err := net.SplitHostPort(registry); err == nil {
		host = splitHost
	} else if i := strings.LastIndex(registry, ":"); i >= 0 && strings.Count(registry, ":") == 1 {
		host = registry[:i]
	}

	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasPrefix(host, "127.") ||
		host == "::1"
}

func filterControllersByFlags(controllers []WorkerPoolController, request *types.ContainerRequest) []WorkerPoolController {
	filteredControllers := []WorkerPoolController{}

	for _, controller := range controllers {
		if !request.Preemptable && controller.IsPreemptable() {
			continue
		}

		if (request.PoolSelector != "" && controller.Name() != request.PoolSelector) ||
			(request.PoolSelector == "" && controller.RequiresPoolSelector()) {
			continue
		}

		if request.DockerEnabled && controller.ContainerRuntime() != "gvisor" {
			continue
		}

		filteredControllers = append(filteredControllers, controller)
	}

	return filteredControllers
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
	gpuCount := gpuCountForScheduling(request)

	gpuRequests := gpuRequestsForScheduling(request)
	for index, gpu := range gpuRequests {
		gpuRequestsMap[gpu] = index
	}

	// If the request contains the "any" GPU selector, we need to check all GPU types
	if slices.Contains(gpuRequests, string(types.GPU_ANY)) {
		gpuRequestsMap = types.GPUTypesToMap(types.AllGPUTypes())
	}

	for _, worker := range workers {
		isGpuWorker := worker.Gpu != ""
		cpu := request.Cpu
		memory := capacityMemoryForScheduling(request)

		// Check if the worker has enough free cpu and memory to run the container
		if worker.FreeCpu < cpu || worker.FreeMemory < memory {
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
			_, validGpu := gpuRequestsMap[worker.Gpu]
			if !validGpu || worker.FreeGpuCount < gpuCount {
				continue
			}
		}

		filteredWorkers = append(filteredWorkers, worker)
	}
	return filteredWorkers
}

func gpuRequestsForScheduling(request *types.ContainerRequest) []string {
	gpus := make([]string, 0, len(request.GpuRequest)+1)
	gpus = append(gpus, request.GpuRequest...)
	if request.Gpu == "" || slices.Contains(gpus, request.Gpu) {
		return gpus
	}
	return append(gpus, request.Gpu)
}

func capacityMemoryForScheduling(request *types.ContainerRequest) int64 {
	if request.Memory <= 0 {
		return request.Memory
	}

	return (request.Memory*125 + 99) / 100
}

func filterWorkersByFlags(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if !request.Preemptable && worker.Preemptable {
			continue
		}

		if request.DockerEnabled && worker.Runtime != types.ContainerRuntimeGvisor.String() {
			continue
		}

		filteredWorkers = append(filteredWorkers, worker)
	}

	return filteredWorkers
}

func filterWorkersByStatus(workers []*types.Worker, statuses ...types.WorkerStatus) []*types.Worker {
	statusSet := map[types.WorkerStatus]struct{}{}
	for _, status := range statuses {
		statusSet[status] = struct{}{}
	}

	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if _, ok := statusSet[worker.Status]; ok {
			filteredWorkers = append(filteredWorkers, worker)
		}
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

	return s.selectWorkerFromWorkers(workers, request)
}

func (s *Scheduler) selectWorkerFromWorkers(workers []*types.Worker, request *types.ContainerRequest) (*types.Worker, error) {
	return s.selectWorkerFromWorkersByStatus(workers, request, types.WorkerStatusAvailable)
}

func (s *Scheduler) selectWorkerFromWorkersByStatus(workers []*types.Worker, request *types.ContainerRequest, statuses ...types.WorkerStatus) (*types.Worker, error) {
	normalizeGPURequest(request)

	if len(workers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	filteredWorkers := filterWorkersByPoolSelector(workers, request)      // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request)  // Filter workers resource requirements
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)      // Filter workers by flags
	filteredWorkers = filterWorkersByStatus(filteredWorkers, statuses...) // Filter workers by lifecycle status

	if len(filteredWorkers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	// Score workers based on status and priority
	scoredWorkers := []scoredWorker{}
	for _, worker := range filteredWorkers {
		score := scoreWorkerForRequest(worker, request)
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	// Select the worker with the highest score
	sort.Slice(scoredWorkers, func(i, j int) bool {
		if scoredWorkers[i].score != scoredWorkers[j].score {
			return scoredWorkers[i].score > scoredWorkers[j].score
		}
		return workerFreeCapacityScore(scoredWorkers[i].worker, request) > workerFreeCapacityScore(scoredWorkers[j].worker, request)
	})

	return scoredWorkers[0].worker, nil
}

func scoreWorkerForRequest(worker *types.Worker, request *types.ContainerRequest) int32 {
	score := worker.Priority
	if worker.Status == types.WorkerStatusAvailable {
		score += scoreAvailableWorker
	}
	if request.RequiresGPU() {
		score -= int32(gpuPriorityModifier(request, worker.Gpu))
	}
	return score
}

func gpuPriorityModifier(request *types.ContainerRequest, gpu string) int {
	gpuRequests := gpuRequestsForScheduling(request)
	if slices.Contains(gpuRequests, string(types.GPU_ANY)) {
		modifiers := types.GPUTypesToMap(types.AllGPUTypes())
		return modifiers[gpu]
	}

	for index, requestedGPU := range gpuRequests {
		if requestedGPU == gpu {
			return index
		}
	}
	return 0
}

func workerFreeCapacityScore(worker *types.Worker, request *types.ContainerRequest) int64 {
	if worker == nil {
		return 0
	}

	score := worker.FreeCpu + worker.FreeMemory
	if request.RequiresGPU() {
		score += int64(worker.FreeGpuCount) * 1_000_000
	}
	return score
}

const maxScheduleRetryCount = 120
const maxScheduleRetryDuration = 10 * time.Minute

func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	normalizeGPURequest(request)

	if request.RetryCount == 0 {
		request.RetryCount++
		return s.requestBacklog.Push(request)
	}

	if request.RetryCount >= maxScheduleRetryCount || time.Since(request.Timestamp) >= maxScheduleRetryDuration {
		newSchedulingAttempt(s, request, nil).fail("retry_limit")
		return nil
	}

	delay := calculateBackoffDelay(request.RetryCount)
	request.RetryCount++
	metrics.RecordRequestRetry(request)
	return s.requestBacklog.PushAfter(request, delay)
}

func calculateBackoffDelay(retryCount int) time.Duration {
	if retryCount == 0 {
		return 0
	}

	baseDelay := 1 * time.Second
	maxDelay := 5 * time.Second
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
