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

	// CPU batch provisioning
	cpuBatchBacklogThreshold int   = 6
	cpuBatchSize             int   = 6
	cpuBatchWorkerMaxCpu     int64 = 200000
	cpuBatchWorkerMaxMemory  int64 = 200000

	// Burst provisioning
	burstBacklogThreshold int     = 20
	burstSizeMultiplier   float64 = 2.0
	maxBurstWorkerCpu     int64   = 200000
	maxBurstWorkerMemory  int64   = 200000

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

	// Batch provisioning for failed CPU-only requests
	failedCpuRequests []*types.ContainerRequest
	failedRequestsMu  sync.Mutex

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

// Process single request with pre-fetched workers
func (s *Scheduler) processRequestWithWorkers(request *types.ContainerRequest, workers []*types.Worker) {
	filteredWorkers := filterWorkersByPoolSelector(workers, request)
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)

	// Try existing workers
	for _, w := range filteredWorkers {
		// Quick capacity check
		if w.FreeCpu < request.Cpu || w.FreeMemory < request.Memory {
			continue
		}
		if request.GpuCount > 0 && (w.Gpu != request.Gpu || w.FreeGpuCount < request.GpuCount) {
			continue
		}

		// Try to schedule
		if err := s.scheduleOnWorker(w, request); err == nil {
			metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
			return
		}
	}

	// No existing worker - check CPU batch provisioning
	if request.GpuCount == 0 && s.shouldUseCpuBatching() {
		if s.addToCpuBatch(request) {
			// Batch threshold reached - provision and schedule all
			if s.provisionCpuBatchWorker() {
				return
			}
		}
		// Batch not ready yet, requeue for next attempt
		s.addRequestToBacklog(request)
		return
	}

	// Burst provisioning for high load
	if s.shouldProvisionBurst(request) {
		if s.provisionBurstWorker(request) {
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

func (s *Scheduler) processRequest(request *types.ContainerRequest) {
	// Try existing workers first (up to 3 attempts with different workers)
	for attempt := 0; attempt < 3; attempt++ {
		workers, err := s.workerRepo.GetAllWorkersLockFree()
		if err != nil {
			continue
		}

		filteredWorkers := filterWorkersByPoolSelector(workers, request)
		filteredWorkers = filterWorkersByFlags(filteredWorkers, request)

		// Try each suitable worker
		for _, worker := range filteredWorkers {
			if worker.FreeCpu < request.Cpu || worker.FreeMemory < request.Memory {
				continue
			}
			if request.GpuCount > 0 && (worker.Gpu != request.Gpu || worker.FreeGpuCount < request.GpuCount) {
				continue
			}

			err := s.scheduleOnWorker(worker, request)
			if err == nil {
				metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
				return
			}
			// Version conflict or capacity taken - try next worker
		}

		// Brief backoff before retrying worker list
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * 2 * time.Millisecond)
		}
	}

	// Provision new worker (burst mode for CPU-only if backlog high)
	if s.shouldProvisionBurst(request) {
		if s.provisionBurstWorker(request) {
			return
		}
	}

	// Standard provisioning
	controllers, err := s.getControllers(request)
	if err == nil {
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
	}

	// CPU batching
	if request.GpuCount == 0 && s.shouldUseCpuBatching() {
		if s.addToCpuBatch(request) {
			if s.provisionCpuBatchWorker() {
				return
			}
		}
		s.addRequestToBacklog(request)
		return
	}

	// Standard provisioning fallback
	s.provisionIndividualWorker(request)
}

// Check if burst provisioning should be used
func (s *Scheduler) shouldProvisionBurst(request *types.ContainerRequest) bool {
	return request.GpuCount == 0 && s.requestBacklog.Len() >= int64(burstBacklogThreshold)
}

// Provision larger worker for burst capacity
func (s *Scheduler) provisionBurstWorker(request *types.ContainerRequest) bool {
	cpu := int64(float64(request.Cpu) * burstSizeMultiplier)
	memory := int64(float64(request.Memory) * burstSizeMultiplier)

	// Cap to prevent huge workers
	if cpu > maxBurstWorkerCpu {
		cpu = maxBurstWorkerCpu
	}
	if memory > maxBurstWorkerMemory {
		memory = maxBurstWorkerMemory
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
				Int64("burst_cpu", cpu).
				Int64("burst_memory", memory).
				Int64("backlog_size", s.requestBacklog.Len()).
				Str("worker_id", newWorker.Id).
				Msg("burst worker provisioned")

			metrics.RecordRequestSchedulingDuration(time.Since(request.Timestamp), request)
			return true
		}
	}
	return false
}

func (s *Scheduler) shouldUseCpuBatching() bool {
	return s.requestBacklog.Len() >= int64(cpuBatchBacklogThreshold)
}

func (s *Scheduler) addToCpuBatch(request *types.ContainerRequest) bool {
	s.failedRequestsMu.Lock()
	defer s.failedRequestsMu.Unlock()

	s.failedCpuRequests = append(s.failedCpuRequests, request)
	return len(s.failedCpuRequests) >= cpuBatchSize
}

func (s *Scheduler) provisionCpuBatchWorker() bool {
	s.failedRequestsMu.Lock()

	if len(s.failedCpuRequests) < cpuBatchSize {
		s.failedRequestsMu.Unlock()
		return false
	}

	batch := make([]*types.ContainerRequest, len(s.failedCpuRequests))
	copy(batch, s.failedCpuRequests)
	s.failedCpuRequests = nil
	s.failedRequestsMu.Unlock()

	var totalCpu, totalMemory int64
	for _, req := range batch {
		totalCpu += req.Cpu
		totalMemory += req.Memory
	}

	if totalCpu > cpuBatchWorkerMaxCpu {
		totalCpu = cpuBatchWorkerMaxCpu
	}
	if totalMemory > cpuBatchWorkerMaxMemory {
		totalMemory = cpuBatchWorkerMaxMemory
	}

	controllers, err := s.getControllers(batch[0])
	if err != nil {
		for _, req := range batch {
			s.addRequestToBacklog(req)
		}
		return false
	}

	for _, c := range controllers {
		if c == nil {
			continue
		}

		worker, err := c.AddWorker(totalCpu, totalMemory, 0)
		if err != nil {
			continue
		}

		scheduled := 0
		failed := []*types.ContainerRequest{}

		for _, req := range batch {
			if s.scheduleOnWorker(worker, req) == nil {
				metrics.RecordRequestSchedulingDuration(time.Since(req.Timestamp), req)
				scheduled++
			} else {
				failed = append(failed, req)
			}
		}

		for _, req := range failed {
			s.addRequestToBacklog(req)
		}

		log.Info().
			Int("batch_size", len(batch)).
			Int("scheduled", scheduled).
			Int("failed", len(failed)).
			Int64("cpu", totalCpu).
			Int64("memory", totalMemory).
			Str("worker_id", worker.Id).
			Msg("CPU batch worker provisioned")

		return scheduled > 0
	}

	for _, req := range batch {
		s.addRequestToBacklog(req)
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
