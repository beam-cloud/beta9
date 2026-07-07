package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
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

var (
	marketplaceBlockedHostMounts = map[string]struct{}{
		"/":     {},
		"/home": {},
		"/var":  {},
	}

	marketplaceBlockedHostMountPrefixes = []string{
		"/dev",
		"/etc",
		"/proc",
		"/root",
		"/run",
		"/sys",
		"/var/lib",
		"/var/run",
	}
)

type Scheduler struct {
	ctx                   context.Context
	config                types.AppConfig
	backendRepo           repo.BackendRepository
	providerRepo          repo.ProviderRepository
	workerRepo            repo.WorkerRepository
	workerPoolRepo        repo.WorkerPoolRepository
	computeRepo           repo.ComputeRepository
	workerPoolManager     *WorkerPoolManager
	requestBacklog        *RequestBacklog
	containerRepo         repo.ContainerRepository
	workspaceRepo         repo.WorkspaceRepository
	eventRepo             repo.EventRepository
	schedulerUsageMetrics SchedulerUsageMetrics
	eventBus              *common.EventBus

	provisioning              *provisioningTracker
	workerProvisioningBackoff *workerProvisioningBackoff
	credentials               *schedulerCredentialCache
}

type schedulerCredentialAttachResult struct {
	hasCredentials bool
	cacheHit       bool
	source         string
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, usageRepo repo.UsageMetricsRepository, backendRepo repo.BackendRepository, workspaceRepo repo.WorkspaceRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)
	computeRepo := repo.NewComputeRedisRepository(redisClient)

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
		case types.PoolModePrivate, types.PoolModeMarketplace:
			log.Debug().Str("pool_name", name).Str("mode", string(pool.Mode)).Msg("skipping static agent pool without workspace state")
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
		computeRepo:           computeRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		schedulerUsageMetrics: schedulerUsage,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,

		provisioning:              newProvisioningTracker(),
		workerProvisioningBackoff: newWorkerProvisioningBackoff(),
		credentials:               newSchedulerCredentialCache(),
	}, nil
}

func NewSchedulerForCapacityChecks(workerRepo repo.WorkerRepository, computeRepo repo.ComputeRepository, workerPoolManager *WorkerPoolManager) *Scheduler {
	return &Scheduler{
		workerRepo:        workerRepo,
		computeRepo:       computeRepo,
		workerPoolManager: workerPoolManager,
	}
}

func (s *Scheduler) RegisterAgentPool(workspaceID string, state *compute.PoolState) error {
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
		ComputeRepo:    s.computeRepo,
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

func normalizeAgentWorkerPoolConfig(state *compute.PoolState) types.WorkerPoolConfig {
	config := types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		ContainerRuntime:     types.ContainerRuntimeRunc.String(),
		RequiresPoolSelector: true,
		Priority:             int32(1000),
	}
	if state == nil {
		return config
	}
	if state.Mode == string(types.PoolModeMarketplace) {
		config.Mode = types.PoolModeMarketplace
		config.ContainerRuntime = marketplacePoolRuntime(state)
		config.RequiresPoolSelector = false
		config.Priority = int32(100)
		config.Preemptable = state.Preemptible
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

func marketplacePoolRuntime(state *compute.PoolState) string {
	if state != nil && state.Config != nil && len(state.Config.Gpu) > 0 {
		return types.MarketplaceContainerRuntimeForGPU(state.Config.Gpu[0])
	}
	return types.ContainerRuntimeGvisor.String()
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
	requestLog(log.Info(), request).
		Str("stub_type", string(request.Stub.Type.Kind())).
		Msg("received run request")

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
		if err == nil && checkpoint != nil && checkpoint.Status == string(types.CheckpointStatusAvailable) {
			requestLog(log.Info(), request).Str("checkpoint_id", checkpoint.CheckpointId).Msg("adding checkpoint to request")
			request.Checkpoint = checkpoint
		}
	}

	requestedEvent := request.Clone()
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
		requestLog(log.Error(), request).Err(err).Msg("failed to add request to backlog")
		newSchedulingAttempt(s, request, nil).fail("backlog_push_failed")
		return err
	}

	return nil
}

func (s *Scheduler) getConcurrencyLimit(request *types.ContainerRequest) (*types.ConcurrencyLimit, error) {
	if s.privatePoolQuotaExempt(request) {
		return nil, nil
	}

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

func (s *Scheduler) privatePoolQuotaExempt(request *types.ContainerRequest) bool {
	if s == nil || request == nil || request.PoolSelector == "" || s.workerPoolManager == nil {
		return false
	}
	pool, ok := s.workerPoolManager.GetPool(request.PoolSelector)
	if !ok || pool.Config.Mode != types.PoolModePrivate {
		return false
	}
	stubConfig, err := request.Stub.UnmarshalConfig()
	if err != nil || stubConfig == nil || stubConfig.Pool == nil {
		return false
	}
	fallback := stubConfig.Pool.Fallback
	return fallback == types.PrivatePoolFallbackFail || fallback == types.PrivatePoolFallbackWait
}

func (s *Scheduler) CheckConcurrencyLimit(request *types.ContainerRequest) error {
	quota, err := s.getConcurrencyLimit(request)
	if err != nil {
		return err
	}

	return s.containerRepo.CheckContainerConcurrencyLimit(quota, request)
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
		wp, err := s.ensureAgentPoolForRequest(request)
		if err != nil {
			return nil, err
		}
		if wp == nil {
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

		workerListStart := time.Now()
		workers, err := s.workerRepo.GetAllWorkers()
		workerListEnd := time.Now()
		for _, request := range requests {
			s.recordContainerLifecycle(request, types.ContainerLifecycleSchedulerWorkerList, workerListStart, workerListEnd, err == nil, map[string]string{
				"batch_size": fmt.Sprintf("%d", len(requests)),
			})
		}
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
		workerLog(requestLog(log.Error(), request), worker).Err(err).Msg("failed to update assigned container gpu")
		return err
	}

	request.Gpu = worker.Gpu

	s.attachImageCredentials(request)
	s.attachBuildRegistryCredentials(request)

	workerRequest := s.workerRequest(worker, request)
	workerRequest.Timestamp = time.Now()
	if err := s.pushWorkerRequest(worker, request, workerRequest); err != nil {
		return err
	}

	scheduledEvent := workerRequest.Clone()
	go s.schedulerUsageMetrics.CounterIncContainerScheduled(scheduledEvent)
	return nil
}

func (s *Scheduler) pushWorkerRequest(worker *types.Worker, originalRequest, workerRequest *types.ContainerRequest) error {
	start := time.Now()
	err := s.workerRepo.ScheduleContainerRequest(worker, workerRequest)
	s.recordContainerLifecycle(originalRequest, types.ContainerLifecycleSchedulerWorkerQueuePush, start, time.Now(), err == nil, map[string]string{
		"worker_id": worker.Id,
	})
	return err
}

func (s *Scheduler) workerRequest(worker *types.Worker, request *types.ContainerRequest) *types.ContainerRequest {
	workerRequest := request.Clone()
	if s.privateWorkerRequest(worker) {
		return workerRequest.PrivateWorkerRequest()
	}
	return workerRequest
}

func (s *Scheduler) privateWorkerRequest(worker *types.Worker) bool {
	if s == nil || worker == nil || s.workerPoolManager == nil {
		return false
	}

	pool, ok := s.workerPoolManager.GetPool(worker.PoolName)
	return ok && pool.Config.Mode == types.PoolModePrivate
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
		AppID:       request.AppId,
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

func (r schedulerCredentialAttachResult) attrs() map[string]string {
	return map[string]string{
		"has_credentials": fmt.Sprintf("%t", r.hasCredentials),
		"cache_hit":       fmt.Sprintf("%t", r.cacheHit),
		"source":          r.source,
	}
}

func (s *Scheduler) recordCredentialLifecycle(request *types.ContainerRequest, id types.ContainerLifecycleID, start time.Time, result schedulerCredentialAttachResult, err error) {
	s.recordContainerLifecycle(request, id, start, time.Now(), err == nil, result.attrs())
}

func (s *Scheduler) attachImageCredentials(request *types.ContainerRequest) {
	start := time.Now()
	result, err := s.loadImageCredentials(request)
	s.recordCredentialLifecycle(request, types.ContainerLifecycleSchedulerImageCredentials, start, result, err)
	if err != nil {
		requestLog(log.Warn(), request).
			Str("image_id", request.ImageId).
			Err(err).
			Msg("failed to attach OCI credentials, will use default provider")
	}
}

// loadImageCredentials fetches and attaches OCI credentials to a container request.
func (s *Scheduler) loadImageCredentials(request *types.ContainerRequest) (schedulerCredentialAttachResult, error) {
	if request.ImageId == "" {
		return schedulerCredentialAttachResult{}, nil
	}

	// Skip credential attachment for build containers - they already have credentials
	// in BuildOptions.SourceImageCreds for pulling the base image during the build
	if strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix) {
		return schedulerCredentialAttachResult{}, nil
	}

	cacheKey := imageCredentialCacheKey(request.WorkspaceId, request.ImageId)
	credential, cacheHit, err := s.credentials.getOrLoad(cacheKey, schedulerImageCredentialTTL, func() (cachedSchedulerCredential, error) {
		secretName, _, err := s.backendRepo.GetImageCredentialSecret(context.TODO(), request.ImageId)
		if err != nil {
			requestLog(log.Debug(), request).
				Str("image_id", request.ImageId).
				Err(err).
				Msg("error getting image credential secret")
			return cachedSchedulerCredential{}, err
		}

		if secretName == "" {
			return cachedSchedulerCredential{exists: false}, nil
		}

		secret, err := s.backendRepo.GetSecretByNameDecrypted(context.TODO(), &request.Workspace, secretName)
		if err != nil {
			requestLog(log.Warn(), request).
				Str("image_id", request.ImageId).
				Str("secret_name", secretName).
				Err(err).
				Msg("failed to get secret by name")
			return cachedSchedulerCredential{}, err
		}

		return cachedSchedulerCredential{
			value:  secret.Value,
			source: secretName,
			exists: true,
		}, nil
	})
	if err != nil {
		return schedulerCredentialAttachResult{cacheHit: cacheHit}, err
	}
	if !credential.exists {
		return schedulerCredentialAttachResult{cacheHit: cacheHit}, nil
	}

	request.ImageCredentials = credential.value

	requestLog(log.Debug(), request).
		Str("image_id", request.ImageId).
		Str("secret_name", credential.source).
		Bool("cache_hit", cacheHit).
		Int("credentials_length", len(credential.value)).
		Msg("attached OCI credentials")

	return schedulerCredentialAttachResult{
		hasCredentials: true,
		cacheHit:       cacheHit,
		source:         credential.source,
	}, nil
}

func (s *Scheduler) attachBuildRegistryCredentials(request *types.ContainerRequest) {
	start := time.Now()
	result, err := s.loadBuildRegistryCredentials(request)
	s.recordCredentialLifecycle(request, types.ContainerLifecycleSchedulerBuildCredentials, start, result, err)
	if err != nil {
		requestLog(log.Warn(), request).
			Err(err).
			Msg("failed to attach build registry credentials to request")
	}
}

// loadBuildRegistryCredentials generates and attaches build registry credentials to a container request.
// These credentials are used for both build-time push and runtime CLIP layer mounting.
func (s *Scheduler) loadBuildRegistryCredentials(request *types.ContainerRequest) (schedulerCredentialAttachResult, error) {
	buildRegistry := s.config.ImageService.BuildRegistry
	if buildRegistry == "" || isLocalBuildRegistry(buildRegistry) {
		requestLog(log.Debug(), request).
			Str("build_registry", buildRegistry).
			Msg("no remote build registry configured, skipping credential generation")
		return schedulerCredentialAttachResult{}, nil
	}

	// Check if we have credentials configured for the build registry.
	buildRegistryCredentials := s.config.ImageService.BuildRegistryCredentials
	dummyImageRef := fmt.Sprintf("%s/%s:dummy", buildRegistry, s.config.ImageService.BuildRepositoryName)

	cacheKey := buildRegistryCredentialCacheKey(buildRegistry, s.config.ImageService.BuildRepositoryName, buildRegistryCredentials)
	credential, cacheHit, err := s.credentials.getOrLoad(cacheKey, schedulerBuildRegistryCredentialTTL, func() (cachedSchedulerCredential, error) {
		var token string
		authSource := "ambient"
		if buildRegistryCredentials.Type != "" && len(buildRegistryCredentials.Credentials) > 0 {
			var err error
			token, err = reg.GetRegistryTokenForImage(dummyImageRef, buildRegistryCredentials.Credentials)
			if err != nil {
				requestLog(log.Warn(), request).
					Str("build_registry", buildRegistry).
					Str("cred_type", buildRegistryCredentials.Type).
					Err(err).
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
				requestLog(log.Warn(), request).
					Str("build_registry", buildRegistry).
					Err(err).
					Msg("failed to generate build registry token from ambient credentials")
				return cachedSchedulerCredential{}, err
			}
		}

		return cachedSchedulerCredential{
			value:  token,
			source: authSource,
			exists: token != "",
		}, nil
	})
	if err != nil {
		return schedulerCredentialAttachResult{cacheHit: cacheHit}, err
	}

	if !credential.exists {
		requestLog(log.Debug(), request).
			Str("build_registry", buildRegistry).
			Str("cred_type", buildRegistryCredentials.Type).
			Msg("no token generated (public registry?), will use ambient auth")
		return schedulerCredentialAttachResult{cacheHit: cacheHit, source: credential.source}, nil
	}

	request.BuildRegistryCredentials = credential.value

	requestLog(log.Debug(), request).
		Str("build_registry", buildRegistry).
		Str("auth_source", credential.source).
		Bool("cache_hit", cacheHit).
		Msg("attached build registry credentials to request")

	return schedulerCredentialAttachResult{
		hasCredentials: true,
		cacheHit:       cacheHit,
		source:         credential.source,
	}, nil
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
		if !marketplaceControllerAllowed(controller, request) {
			continue
		}

		// Marketplace capacity is inherently interruptible (seller machines can
		// vanish); opting in with AllowMarketplace implies accepting preemption,
		// so the preemptable gate only applies to non-marketplace pools.
		if !request.Preemptable && controller.IsPreemptable() && controller.Mode() != types.PoolModeMarketplace {
			continue
		}

		if (request.PoolSelector != "" && controller.Name() != request.PoolSelector) ||
			(request.PoolSelector == "" && controller.RequiresPoolSelector()) {
			continue
		}

		filteredControllers = append(filteredControllers, controller)
	}

	return filteredControllers
}

func marketplaceControllerAllowed(controller WorkerPoolController, request *types.ContainerRequest) bool {
	if controller == nil || controller.Mode() != types.PoolModeMarketplace {
		return true
	}
	if request == nil || !request.AllowMarketplace {
		return false
	}
	return marketplaceRequestSafe(request)
}

// filterWorkersByMachine restricts machine-pinned requests (marketplace
// rentals) to the pinned machine's worker. The pin is stronger than any pool
// selector: it identifies exactly one worker.
func filterWorkersByMachine(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if request.MachineId == "" {
		return workers
	}
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if worker.MachineId == request.MachineId {
			filteredWorkers = append(filteredWorkers, worker)
		}
	}
	return filteredWorkers
}

func filterWorkersByPoolSelector(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if request.MachineId != "" {
		// The machine pin already selected the worker; its pool may require a
		// selector the request doesn't carry.
		return workers
	}
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

func (s *Scheduler) filterWorkersByFlags(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		// Preemptible marketplace workers stay eligible: reaching this filter
		// means the request already opted in with AllowMarketplace, which
		// implies accepting seller-side preemption.
		if !request.Preemptable && worker.Preemptable && !s.isMarketplaceWorker(worker) {
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

	filteredWorkers := filterWorkersByMachine(workers, request) // Machine-pinned requests only see their machine's worker
	filteredWorkers = filterWorkersByPoolSelector(filteredWorkers, request)
	filteredWorkers = s.filterMarketplaceWorkers(filteredWorkers, request)
	filteredWorkers = s.filterLivePrivateAgentWorkers(filteredWorkers, request)
	filteredWorkers = filterWorkersByResources(filteredWorkers, request)  // Filter workers resource requirements
	filteredWorkers = s.filterWorkersByFlags(filteredWorkers, request)    // Filter workers by flags
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

func (s *Scheduler) filterMarketplaceWorkers(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if len(workers) == 0 || s == nil || s.workerPoolManager == nil {
		return workers
	}
	// Loaded once per filter pass (single indexed read), only when a
	// serverless marketplace candidate actually needs it.
	var rentedByMachine map[string]uint32
	rentedLoaded := false

	filtered := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		if !s.isMarketplaceWorker(worker) {
			filtered = append(filtered, worker)
			continue
		}
		if request == nil || !request.AllowMarketplace || !marketplaceRequestSafe(request) {
			continue
		}
		// Rented GPUs are invisible to serverless requests; only the renter's
		// machine-pinned workloads may consume them. Machine-pinned requests
		// are entitled to the rented capacity, so no subtraction applies.
		if request.MachineId == "" {
			if !rentedLoaded {
				rentedByMachine = s.rentedGPUsByMachine()
				rentedLoaded = true
			}
			// Fail closed: without rental visibility we can't prove this
			// capacity isn't exclusively held by a renter.
			if rentedByMachine == nil {
				continue
			}
			if !workerFitsWithRentals(worker, request, rentedByMachine[worker.MachineId]) {
				continue
			}
		}
		filtered = append(filtered, worker)
	}
	return filtered
}

// rentedGPUsByMachine sums active rental GPUs per machine. Returns nil when
// the lookup fails so callers can fail closed; a scheduler without a compute
// repo (no marketplace support) reports no rentals.
func (s *Scheduler) rentedGPUsByMachine() map[string]uint32 {
	rented := map[string]uint32{}
	if s.computeRepo == nil {
		return rented
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	rentals, err := s.computeRepo.ListAllMarketplaceRentals(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to list marketplace rentals; hiding marketplace capacity from serverless requests")
		return nil
	}
	for _, rental := range rentals {
		if rental != nil && rental.MachineID != "" {
			rented[rental.MachineID] += rental.GPUCount
		}
	}
	return rented
}

// workerFitsWithRentals reports whether a serverless request still fits after
// subtracting the machine's rented GPUs from free capacity. Free already
// accounts for running containers (including rental workloads), so
// subtracting full rentals is conservative: rented-but-idle GPUs are held
// back, rented-and-busy GPUs are never double counted below zero fit.
func workerFitsWithRentals(worker *types.Worker, request *types.ContainerRequest, rented uint32) bool {
	if rented == 0 || !request.RequiresGPU() {
		return true
	}
	if worker.FreeGpuCount < rented {
		return false
	}
	return worker.FreeGpuCount-rented >= gpuCountForScheduling(request)
}

func (s *Scheduler) isMarketplaceWorker(worker *types.Worker) bool {
	if s == nil || s.workerPoolManager == nil || worker == nil {
		return false
	}
	pool, ok := s.workerPoolManager.GetPool(worker.PoolName)
	return ok && pool.Config.Mode == types.PoolModeMarketplace
}

func marketplaceRequestSafe(request *types.ContainerRequest) bool {
	if request == nil {
		return false
	}
	if request.DockerEnabled {
		return false
	}
	if request.PoolSelector != "" {
		return false
	}
	if marketplaceRequestHasUnsafeMount(request) {
		return false
	}
	return true
}

func marketplaceRequestHasUnsafeMount(request *types.ContainerRequest) bool {
	if request == nil {
		return false
	}
	for _, mount := range request.Mounts {
		if mount.MountType == types.StorageModeDurableDisk || mount.DurableDisk != nil {
			return true
		}
		if marketplaceMountUnsafe(mount) {
			return true
		}
	}
	return false
}

func marketplaceMountUnsafe(mount types.Mount) bool {
	localPath := cleanMarketplaceMountPath(mount.LocalPath)
	mountPath := cleanMarketplaceMountPath(mount.MountPath)
	return strings.Contains(localPath, "docker.sock") ||
		strings.Contains(mountPath, "docker.sock") ||
		marketplaceBroadHostPath(localPath)
}

func cleanMarketplaceMountPath(value string) string {
	return path.Clean(strings.TrimSpace(value))
}

func marketplaceBroadHostPath(localPath string) bool {
	if localPath == "" || localPath == "." {
		return false
	}
	if _, ok := marketplaceBlockedHostMounts[localPath]; ok {
		return true
	}
	for _, prefix := range marketplaceBlockedHostMountPrefixes {
		if localPath == prefix {
			return true
		}
		if strings.HasPrefix(localPath, prefix+"/") {
			return true
		}
	}
	return false
}

func (s *Scheduler) filterLivePrivateAgentWorkers(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	if len(workers) == 0 || s == nil || request == nil || request.PoolSelector == "" || request.WorkspaceId == "" || s.workerPoolManager == nil || s.computeRepo == nil {
		return workers
	}

	pool, ok := s.workerPoolManager.GetPool(request.PoolSelector)
	if !ok || pool.Controller == nil || pool.Controller.Mode() != types.PoolModePrivate {
		return workers
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	state, err := s.agentPoolStateForRequest(request)
	if err != nil || state == nil {
		return workers
	}
	poolName := firstNonEmpty(state.Name, state.Selector, request.PoolSelector)
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, request.WorkspaceId, poolName)
	if err != nil {
		return workers
	}

	liveWorkers := make(map[string]struct{}, len(machines))
	now := time.Now()
	for _, machine := range machines {
		if machine == nil || machine.WorkspaceID != request.WorkspaceId || machine.PoolName != poolName || machine.Executor != types.DefaultAgentWorkerContainerMode || !compute.AgentMachineConnected(machine, now) {
			continue
		}
		liveWorkers[compute.AgentMachineWorkerID(machine.MachineID)] = struct{}{}
	}

	filteredWorkers := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		if _, ok := liveWorkers[worker.Id]; ok {
			filteredWorkers = append(filteredWorkers, worker)
		}
	}
	return filteredWorkers
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
		return s.pushBacklog(request, 0)
	}

	if request.RetryCount >= maxScheduleRetryCount || time.Since(request.Timestamp) >= maxScheduleRetryDuration {
		newSchedulingAttempt(s, request, nil).fail("retry_limit")
		return nil
	}

	delay := calculateBackoffDelay(request.RetryCount)
	request.RetryCount++
	metrics.RecordRequestRetry(request)
	return s.pushBacklog(request, delay)
}

func (s *Scheduler) pushBacklog(request *types.ContainerRequest, delay time.Duration) error {
	private, err := s.privateBacklogRequest(request)
	if err != nil {
		return err
	}
	if private {
		request = request.PrivateWorkerRequest()
	}
	return s.requestBacklog.PushAfter(request, delay)
}

func (s *Scheduler) privateBacklogRequest(request *types.ContainerRequest) (bool, error) {
	if s == nil || request == nil || request.PoolSelector == "" || s.workerPoolManager == nil {
		return false, nil
	}
	pool, err := s.ensureAgentPoolForRequest(request)
	if err != nil {
		return false, err
	}
	return pool != nil && pool.Config.Mode == types.PoolModePrivate, nil
}

func (s *Scheduler) ensureAgentPoolForRequest(request *types.ContainerRequest) (*WorkerPool, error) {
	if s == nil || request == nil || request.PoolSelector == "" || s.workerPoolManager == nil {
		return nil, nil
	}
	if pool, ok := s.workerPoolManager.GetPool(request.PoolSelector); ok {
		return pool, nil
	}

	state, err := s.agentPoolStateForRequest(request)
	if err != nil || state == nil {
		return nil, err
	}
	if err := s.RegisterAgentPool(request.WorkspaceId, state); err != nil {
		return nil, err
	}
	if pool, ok := s.workerPoolManager.GetPool(request.PoolSelector); ok {
		return pool, nil
	}
	if name := firstNonEmpty(state.Selector, state.Name); name != request.PoolSelector {
		if pool, ok := s.workerPoolManager.GetPool(name); ok {
			return pool, nil
		}
	}
	return nil, nil
}

func (s *Scheduler) agentPoolStateForRequest(request *types.ContainerRequest) (*compute.PoolState, error) {
	if s == nil || request == nil || request.WorkspaceId == "" || request.PoolSelector == "" || s.computeRepo == nil {
		return nil, nil
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	selector := request.PoolSelector
	state, err := s.computeRepo.GetPoolState(ctx, request.WorkspaceId, selector)
	if err != nil {
		return nil, err
	}
	if poolStateMatchesSelector(state, selector) {
		return state, nil
	}

	states, err := s.computeRepo.ListPoolStates(ctx, request.WorkspaceId, 0)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if poolStateMatchesSelector(state, selector) {
			return state, nil
		}
	}
	return nil, nil
}

func poolStateMatchesSelector(state *compute.PoolState, selector string) bool {
	if state == nil || selector == "" {
		return false
	}
	if state.Name == selector || state.Selector == selector {
		return true
	}
	if state.Config == nil {
		return false
	}
	return state.Config.Name == selector || state.Config.Selector == selector
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
