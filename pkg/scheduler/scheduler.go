package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// General scheduling
	requestProcessingInterval  time.Duration = 5 * time.Millisecond
	maxConcurrentScheduling    int           = 250
	schedulerWorkerPoolSize    int           = 100
	schedulingTimeoutPerWorker time.Duration = 250 * time.Millisecond
	maxScheduleRetryCount      int           = 3
	maxScheduleRetryDuration   time.Duration = 10 * time.Minute

	// Batch scheduling
	batchSize           int           = 15
	workerCacheDuration time.Duration = 0

	// Bulk provisioning (formerly burst provisioning)
	bulkBacklogThreshold int     = 20
	bulkSizeMultiplier   float64 = 2.0
	maxBulkWorkerCpu     int64   = 200000
	maxBulkWorkerMemory  int64   = 200000

	// Worker scoring
	scoreAvailableWorker int32 = 10
)

type Scheduler struct {
	ctx                   context.Context
	config                types.AppConfig
	backendRepo           repo.BackendRepository
	workerRepo            repo.WorkerRepository
	workerPoolManager     *WorkerPoolManager
	requestBacklog        *RequestBacklog
	containerRepo         repo.ContainerRepository
	workspaceRepo         repo.WorkspaceRepository
	eventRepo             repo.EventRepository
	schedulerUsageMetrics SchedulerUsageMetrics
	eventBus              *common.EventBus
	schedulingSemaphore   chan struct{} // Semaphore to limit concurrent scheduling

	// Worker cache for batch processing
	cachedWorkers   []*types.Worker
	workerCacheTime time.Time
	workerCacheMu   sync.RWMutex
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, usageRepo repo.UsageMetricsRepository, backendRepo repo.BackendRepository, workspaceRepo repo.WorkspaceRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)

	schedulerUsage := NewSchedulerUsageMetrics(usageRepo)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

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
		workerRepo:            workerRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		schedulerUsageMetrics: schedulerUsage,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,
		schedulingSemaphore:   make(chan struct{}, maxConcurrentScheduling),
	}, nil
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

	go s.schedulerUsageMetrics.CounterIncContainerRequested(request)
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

	err := s.containerRepo.UpdateContainerStatus(stopArgs.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
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
		for _, gpu := range request.GpuRequest {
			pools := s.workerPoolManager.GetPoolByFilters(poolFilters{
				GPUType: gpu,
			})

			for _, pool := range pools {
				controllers = append(controllers, pool.Controller)
			}

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
	for i := 0; i < schedulerWorkerPoolSize; i++ {
		go s.processRequestWorker()
	}
	<-s.ctx.Done()
}

func (s *Scheduler) processRequestWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		if s.requestBacklog.Len() == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		requests, err := s.requestBacklog.PopBatch(int64(batchSize))
		if err != nil || len(requests) == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		s.processBatch(requests)
	}
}

func (s *Scheduler) getCachedWorkers() ([]*types.Worker, error) {
	s.workerCacheMu.RLock()
	if time.Since(s.workerCacheTime) < workerCacheDuration && s.cachedWorkers != nil {
		workers := s.cachedWorkers
		s.workerCacheMu.RUnlock()
		return workers, nil
	}
	s.workerCacheMu.RUnlock()

	workers, err := s.workerRepo.GetAllWorkersLockFree()
	if err != nil {
		return nil, err
	}

	s.workerCacheMu.Lock()
	s.cachedWorkers = workers
	s.workerCacheTime = time.Now()
	s.workerCacheMu.Unlock()

	return workers, nil
}

func (s *Scheduler) processBatch(requests []*types.ContainerRequest) {
	workers, err := s.getCachedWorkers()
	if err != nil {
		for _, req := range requests {
			s.addRequestToBacklog(req)
		}
		return
	}

	// Sort workers by utilization (bin packing) to improve density
	workers = sortWorkersByUtilization(workers)

	// Process all requests in parallel with shared worker list
	for _, request := range requests {
		req := request
		s.schedulingSemaphore <- struct{}{}
		go func(r *types.ContainerRequest) {
			defer func() { <-s.schedulingSemaphore }()
			s.processRequestWithWorkers(r, workers)
		}(req)
	}
}

// sortWorkersByUtilization sorts workers to prioritize those already in use
// This helps pack containers onto fewer workers, improving efficiency
func sortWorkersByUtilization(workers []*types.Worker) []*types.Worker {
	// Create a copy to avoid mutating shared slice
	sorted := make([]*types.Worker, len(workers))
	copy(sorted, workers)

	// Sort by utilization (descending) - pack onto workers already in use
	// This reduces fragmentation and makes better use of provisioned resources
	sort.Slice(sorted, func(i, j int) bool {
		utilI := float64(sorted[i].TotalCpu-sorted[i].FreeCpu) / float64(sorted[i].TotalCpu)
		utilJ := float64(sorted[j].TotalCpu-sorted[j].FreeCpu) / float64(sorted[j].TotalCpu)
		return utilI > utilJ // Higher utilization first
	})

	return sorted
}

// shuffleWorkers creates a shuffled copy of the worker list (used by tests)
func shuffleWorkers(workers []*types.Worker) []*types.Worker {
	if len(workers) <= 1 {
		return workers
	}

	shuffled := make([]*types.Worker, len(workers))
	copy(shuffled, workers)

	for i := len(shuffled) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled
}

// isWorkerSuitable checks if a worker meets all scheduling constraints for a request.
// This centralizes all filtering logic to ensure consistency across scheduling paths.
func (s *Scheduler) isWorkerSuitable(worker *types.Worker, request *types.ContainerRequest) bool {
	// Check worker status
	if worker.Status == types.WorkerStatusDisabled {
		return false
	}

	// Check pool selector
	if request.PoolSelector != "" {
		if worker.PoolName != request.PoolSelector {
			return false
		}
	} else {
		// If request has no pool selector, worker must not require one
		if worker.RequiresPoolSelector {
			return false
		}
	}

	// Check preemptability
	if !request.Preemptable && worker.Preemptable {
		return false
	}

	// Check runtime (Docker requires gvisor)
	if request.DockerEnabled && worker.Runtime != types.ContainerRuntimeGvisor.String() {
		return false
	}

	// Check GPU requirements
	requiresGPU := request.RequiresGPU()
	hasGPU := worker.Gpu != ""

	if requiresGPU != hasGPU {
		return false
	}

	if requiresGPU {
		// Check if worker GPU matches request
		gpuRequestsMap := map[string]bool{}
		for _, gpu := range request.GpuRequest {
			gpuRequestsMap[gpu] = true
		}

		// If the request contains the "any" GPU selector, accept any GPU type
		if slices.Contains(request.GpuRequest, string(types.GPU_ANY)) {
			// Any GPU type is acceptable
		} else if !gpuRequestsMap[worker.Gpu] {
			return false
		}

		// Check GPU count
		if worker.FreeGpuCount < request.GpuCount {
			return false
		}
	}

	// Check CPU and memory capacity
	if worker.FreeCpu < request.Cpu || worker.FreeMemory < request.Memory {
		return false
	}

	return true
}

// Process single request with pre-fetched workers
func (s *Scheduler) processRequestWithWorkers(request *types.ContainerRequest, workers []*types.Worker) {
	// Try existing workers using the unified isWorkerSuitable check
	for _, w := range workers {
		if !s.isWorkerSuitable(w, request) {
			continue
		}

		// Try to schedule
		if err := s.scheduleOnWorker(w, request); err == nil {
			metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
			return
		}
	}

	// No existing worker - check bulk provisioning for high load
	if s.shouldProvisionBulk(request) {
		if s.provisionBulkWorker(request) {
			return
		}
	}

	// Standard individual provisioning
	s.provisionIndividualWorker(request)
}

// provisionIndividualWorker provisions a single worker for one request
func (s *Scheduler) provisionIndividualWorker(request *types.ContainerRequest) {
	controllers, err := s.getControllers(request)
	if err != nil {
		s.addRequestToBacklog(request)
		return
	}

	for _, c := range controllers {
		if c == nil {
			continue
		}

		newWorker, err := c.AddWorker(request.Cpu, request.Memory, request.GpuCount)
		if err == nil && s.scheduleOnWorker(newWorker, request) == nil {
			metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
			return
		}
	}

	s.addRequestToBacklog(request)
}


// shouldProvisionBulk checks if bulk provisioning should be used
func (s *Scheduler) shouldProvisionBulk(request *types.ContainerRequest) bool {
	return request.GpuCount == 0 && s.requestBacklog.Len() >= int64(bulkBacklogThreshold)
}

// provisionBulkWorker provisions a larger worker for handling high load
func (s *Scheduler) provisionBulkWorker(request *types.ContainerRequest) bool {
	cpu := int64(float64(request.Cpu) * bulkSizeMultiplier)
	memory := int64(float64(request.Memory) * bulkSizeMultiplier)

	// Cap to prevent huge workers
	if cpu > maxBulkWorkerCpu {
		cpu = maxBulkWorkerCpu
	}
	if memory > maxBulkWorkerMemory {
		memory = maxBulkWorkerMemory
	}

	controllers, err := s.getControllers(request)
	if err != nil {
		return false
	}

	for _, c := range controllers {
		if c == nil {
			continue
		}

		newWorker, err := c.AddWorker(cpu, memory, 0)
		if err == nil && s.scheduleOnWorker(newWorker, request) == nil {
			log.Info().
				Int64("bulk_cpu", cpu).
				Int64("bulk_memory", memory).
				Int64("backlog_size", s.requestBacklog.Len()).
				Str("worker_id", newWorker.Id).
				Msg("bulk worker provisioned for high load")

			metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
			return true
		}
	}
	return false
}


func (s *Scheduler) scheduleOnWorker(worker *types.Worker, request *types.ContainerRequest) error {
	const maxRetries = 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Millisecond * time.Duration(attempt))
			freshWorker, err := s.workerRepo.GetWorkerById(worker.Id)
			if err != nil {
				return fmt.Errorf("failed to refresh worker: %w", err)
			}
			worker = freshWorker
		}

		err := s.scheduleRequest(worker, request)
		if err == nil {
			return nil
		}

		if !strings.Contains(err.Error(), "invalid worker resource version") {
			return fmt.Errorf("scheduling failed: %w", err)
		}
	}

	return fmt.Errorf("max retries exceeded due to version conflicts")
}

func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
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

	go s.schedulerUsageMetrics.CounterIncContainerScheduled(request)
	go s.eventRepo.PushContainerScheduledEvent(request.ContainerId, worker.Id, request)
	return s.workerRepo.ScheduleContainerRequest(worker, request)
}

// attachImageCredentials fetches and attaches OCI credentials to a container request
func (s *Scheduler) attachImageCredentials(request *types.ContainerRequest) error {
	if request.ImageId == "" {
		log.Debug().
			Str("container_id", request.ContainerId).
			Msg("no image ID, skipping credential attachment")
		return nil
	}

	// Skip credential attachment for build containers - they already have credentials
	// in BuildOptions.SourceImageCreds for pulling the base image during the build
	if strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix) {
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("image_id", request.ImageId).
			Msg("build container, skipping credential attachment")
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
		Str("credentials", secret.Value).
		Msg("attached OCI credentials")

	return nil
}

// attachBuildRegistryCredentials generates and attaches build registry credentials to a container request
// These credentials are used for both build-time (push) and runtime (CLIP layer mounting)
func (s *Scheduler) attachBuildRegistryCredentials(request *types.ContainerRequest) error {
	buildRegistry := s.config.ImageService.BuildRegistry
	if buildRegistry == "" || buildRegistry == "localhost" || strings.HasPrefix(buildRegistry, "127.0.0.1") {
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("build_registry", buildRegistry).
			Msg("no remote build registry configured, skipping credential generation")
		return nil
	}

	// Check if we have credentials configured for the build registry
	buildRegistryCredentials := s.config.ImageService.BuildRegistryCredentials
	if buildRegistryCredentials.Type == "" || len(buildRegistryCredentials.Credentials) == 0 {
		return nil
	}

	// Build a dummy image reference for the build registry
	dummyImageRef := fmt.Sprintf("%s/%s:dummy", buildRegistry, s.config.ImageService.BuildRepositoryName)

	// Generate fresh token using the credentials from config
	token, err := reg.GetRegistryTokenForImage(dummyImageRef, buildRegistryCredentials.Credentials)
	if err != nil {
		log.Warn().
			Err(err).
			Str("container_id", request.ContainerId).
			Str("build_registry", buildRegistry).
			Str("cred_type", buildRegistryCredentials.Type).
			Msg("failed to generate build registry token, will use ambient auth")
		return nil // Don't fail the request, just log and continue
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
		Str("cred_type", buildRegistryCredentials.Type).
		Msg("attached build registry credentials to request")

	return nil
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

	for index, gpu := range request.GpuRequest {
		gpuRequestsMap[gpu] = index
	}

	// If the request contains the "any" GPU selector, we need to check all GPU types
	if slices.Contains(request.GpuRequest, string(types.GPU_ANY)) {
		gpuRequestsMap = types.GPUTypesToMap(types.AllGPUTypes())
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
			// NOTE: will only work properly if all GPU types and their pools start from 0 and pool priority are incremental by changes of ?1
			worker.Priority -= int32(priorityModifier)
		}

		filteredWorkers = append(filteredWorkers, worker)
	}
	return filteredWorkers
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

type scoredWorker struct {
	worker *types.Worker
	score  int32
}

// Constants used for scoring workers

func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	// Use lock-free read to get all workers - we'll rely on optimistic locking during capacity update
	workers, err := s.workerRepo.GetAllWorkersLockFree()
	if err != nil {
		return nil, err
	}

	filteredWorkers := filterWorkersByPoolSelector(workers, request)     // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request) // Filter workers resource requirements
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)     // Filter workers by flags

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


func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	if request.RequiresGPU() && request.GpuCount <= 0 {
		request.GpuCount = 1
	}

	if request.RetryCount == 0 {
		request.RetryCount++
		return s.requestBacklog.Push(request)
	}

	go func() {
		if request.RetryCount < maxScheduleRetryCount && time.Since(request.Timestamp) < maxScheduleRetryDuration {
			delay := calculateBackoffDelay(request.RetryCount)
			time.Sleep(delay)
			request.RetryCount++
			s.requestBacklog.Push(request)
			return
		}

		log.Error().Str("container_id", request.ContainerId).Int("retry_count", request.RetryCount).Msg("giving up on request")
		s.containerRepo.DeleteContainerState(request.ContainerId)
		s.containerRepo.SetContainerRequestStatus(request.ContainerId, types.ContainerRequestStatusFailed)
		metrics.RecordRequestScheduleFailure(request)
	}()

	return nil
}

func calculateBackoffDelay(retryCount int) time.Duration {
	if retryCount == 0 {
		return 0
	}

	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
