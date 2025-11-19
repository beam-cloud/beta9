package scheduler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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
	requestProcessingInterval time.Duration = 100 * time.Millisecond
	workerCacheDuration       time.Duration = 500 * time.Millisecond
	batchSchedulingWindow     time.Duration = 50 * time.Millisecond
	maxBatchSize              int           = 100
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
	workerCache           *WorkerStateCache
}

// WorkerStateCache provides a memory-level cache for worker state
type WorkerStateCache struct {
	mu           sync.RWMutex
	workers      map[string]*types.Worker
	lastUpdate   time.Time
	maxStaleness time.Duration
	workerRepo   repo.WorkerRepository
}

// NewWorkerStateCache creates a new worker state cache
func NewWorkerStateCache(workerRepo repo.WorkerRepository, maxStaleness time.Duration) *WorkerStateCache {
	return &WorkerStateCache{
		workers:      make(map[string]*types.Worker),
		maxStaleness: maxStaleness,
		workerRepo:   workerRepo,
	}
}

// Get retrieves workers from cache or refreshes if stale
func (c *WorkerStateCache) Get(ctx context.Context) ([]*types.Worker, error) {
	c.mu.RLock()
	if time.Since(c.lastUpdate) < c.maxStaleness && len(c.workers) > 0 {
		result := make([]*types.Worker, 0, len(c.workers))
		for _, w := range c.workers {
			// Return a copy to prevent external modifications
			workerCopy := &types.Worker{}
			if err := common.CopyStruct(w, workerCopy); err != nil {
				c.mu.RUnlock()
				return nil, err
			}
			result = append(result, workerCopy)
		}
		c.mu.RUnlock()
		return result, nil
	}
	c.mu.RUnlock()

	// Cache miss or stale - refresh from repository
	return c.refresh(ctx)
}

// refresh fetches fresh worker state from the repository
func (c *WorkerStateCache) refresh(ctx context.Context) ([]*types.Worker, error) {
	workers, err := c.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Update cache
	c.workers = make(map[string]*types.Worker)
	for _, w := range workers {
		c.workers[w.Id] = w
	}
	c.lastUpdate = time.Now()

	return workers, nil
}

// InvalidateWorker removes a specific worker from the cache
func (c *WorkerStateCache) InvalidateWorker(workerId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, workerId)
}

// InvalidateAll clears the entire cache
func (c *WorkerStateCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers = make(map[string]*types.Worker)
	c.lastUpdate = time.Time{}
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

	// Initialize worker cache
	workerCache := NewWorkerStateCache(workerRepo, workerCacheDuration)

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
		workerCache:           workerCache,
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

		if s.requestBacklog.Len() == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		// Collect a batch of requests to process together
		batch := s.collectRequestBatch()
		if len(batch) == 0 {
			continue
		}

		// Process the batch together to reduce lock contention and worker thrashing
		s.processBatch(batch)
	}
}

// scheduleRequest is deprecated - use finalizeScheduling instead for batch processing
// Kept for backward compatibility with external callers
func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	s.finalizeScheduling(worker, request)
	return nil
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
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("build_registry", buildRegistry).
			Msg("no build registry credentials in config, will use ambient auth")
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
const (
	scoreAvailableWorker int32 = 10
)

func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	// Use cached worker state instead of fetching from repository every time
	workers, err := s.workerCache.Get(s.ctx)
	if err != nil {
		return nil, err
	}

	filteredWorkers := filterWorkersByPoolSelector(workers, request)     // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request) // Filter workers resource requirements
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)     // Filter workers by flags

	if len(filteredWorkers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	// Apply worker affinity for CPU-only containers to distribute load
	if !request.RequiresGPU() && len(filteredWorkers) > 1 {
		filteredWorkers = applyWorkerAffinity(request.ContainerId, filteredWorkers)
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

// hashContainerId computes a stable hash for a container ID
func hashContainerId(containerId string) uint64 {
	hash := sha256.Sum256([]byte(containerId))
	return binary.BigEndian.Uint64(hash[:8])
}

// applyWorkerAffinity rotates the worker list based on container ID hash
// This distributes containers across workers to reduce lock contention
func applyWorkerAffinity(containerId string, workers []*types.Worker) []*types.Worker {
	if len(workers) <= 1 {
		return workers
	}

	// Calculate affinity index based on container ID hash
	hash := hashContainerId(containerId)
	affinityIndex := int(hash % uint64(len(workers)))

	// Rotate workers list to put preferred worker first
	rotated := make([]*types.Worker, len(workers))
	for i := 0; i < len(workers); i++ {
		rotated[i] = workers[(affinityIndex+i)%len(workers)]
	}

	return rotated
}

const maxScheduleRetryCount = 3
const maxScheduleRetryDuration = 10 * time.Minute

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
