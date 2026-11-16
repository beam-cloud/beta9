package scheduler

import "github.com/beam-cloud/beta9/pkg/types"

// GetServerlessGPUAvailability reports live GPU capacity that a selector-less
// serverless request can actually use. Private and provider workers must not
// leak into the global availability shown to other workspaces.
func (s *Scheduler) GetServerlessGPUAvailability() (map[string]bool, error) {
	availability := emptyGPUAvailability()
	if s == nil || s.workerRepo == nil || s.workerPoolManager == nil {
		return availability, nil
	}

	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	return s.serverlessGPUAvailability(workers), nil
}

func (s *Scheduler) serverlessGPUAvailability(workers []*types.Worker) map[string]bool {
	availability := emptyGPUAvailability()
	for _, worker := range workers {
		if worker == nil || worker.Status != types.WorkerStatusAvailable || worker.Gpu == "" || (worker.FreeGpuCount == 0 && worker.EvictableGpuCount == 0) {
			continue
		}
		pool, ok := s.workerPoolManager.GetPool(workerPoolSelector(worker))
		if !ok || pool.Controller == nil || pool.Controller.RequiresPoolSelector() || worker.RequiresPoolSelector {
			continue
		}
		if pool.Config.Mode == types.PoolModeProvider || pool.Config.Mode == types.PoolModePrivate {
			continue
		}
		if workspaceOwned(worker) {
			continue
		}
		availability[worker.Gpu] = true
	}
	return availability
}

func emptyGPUAvailability() map[string]bool {
	availability := make(map[string]bool, len(types.AllGPUTypes()))
	for _, gpu := range types.AllGPUTypes() {
		if gpu != types.GPU_ANY {
			availability[gpu.String()] = false
		}
	}
	return availability
}

type CapacityCheckResult struct {
	CanSchedule bool
	WorkerId    string
	Err         error
}

func (s *Scheduler) CheckCapacity(request *types.ContainerRequest, replicas uint, ignoreStubId string, activeContainers []types.ContainerState) CapacityCheckResult {
	return (&capacityCheck{
		scheduler:        s,
		request:          request,
		replicas:         replicas,
		replacedStubID:   ignoreStubId,
		activeContainers: activeContainers,
	}).run()
}

type capacityCheck struct {
	scheduler        *Scheduler
	request          *types.ContainerRequest
	replicas         uint
	replacedStubID   string
	activeContainers []types.ContainerState
	workers          []*types.Worker
}

func (c *capacityCheck) run() CapacityCheckResult {
	if c.scheduler == nil || c.scheduler.workerRepo == nil {
		return CapacityCheckResult{Err: &types.ErrNoSuitableWorkerFound{}}
	}
	if c.replicas == 0 {
		c.replicas = 1
	}
	if err := c.loadWorkers(); err != nil {
		return CapacityCheckResult{Err: err}
	}
	c.restoreReplacedCapacity()
	return c.placeReplicas()
}

func (c *capacityCheck) loadWorkers() error {
	workers, err := c.scheduler.workerRepo.GetAllWorkers()
	if err != nil {
		return err
	}
	c.workers = make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		next := *worker
		next.ActiveContainers = append([]types.Container(nil), worker.ActiveContainers...)
		c.workers = append(c.workers, &next)
	}
	return nil
}

func (c *capacityCheck) restoreReplacedCapacity() {
	if c.replacedStubID == "" {
		return
	}
	workersByID := map[string]*types.Worker{}
	for _, worker := range c.workers {
		workersByID[worker.Id] = worker
	}
	for _, container := range c.activeContainers {
		if container.StubId != c.replacedStubID {
			continue
		}
		worker := workersByID[container.WorkerId]
		if worker == nil {
			continue
		}
		replacedRequest := containerRequestFromState(container)
		worker.FreeCpu += container.CapacityCpu()
		worker.FreeMemory += capacityMemoryForScheduling(replacedRequest)
		worker.FreeGpuCount += gpuCountForScheduling(replacedRequest)
		c.capFreeCapacity(worker)
	}
}

func containerRequestFromState(container types.ContainerState) *types.ContainerRequest {
	return &types.ContainerRequest{
		Cpu:        container.Cpu,
		Memory:     container.Memory,
		Gpu:        container.Gpu,
		GpuRequest: types.GpuTypesToStrings(types.GPUTypesFromString(container.Gpu)),
		GpuCount:   container.GpuCount,
	}
}

// placeReplicas places the replicas one after another, as a scheduling pass
// would, each on what the ones before it left.
func (c *capacityCheck) placeReplicas() CapacityCheckResult {
	var selected *types.Worker
	for i := uint(0); i < c.replicas; i++ {
		request := c.request.Clone()
		worker, err := c.scheduler.selectWorkerFromWorkers(c.workers, request)
		if err != nil {
			return CapacityCheckResult{Err: err}
		}
		if !c.scheduler.reserveWorkerCapacity(worker, request) {
			return CapacityCheckResult{Err: &types.ErrNoSuitableWorkerFound{}}
		}
		selected = worker
	}
	return CapacityCheckResult{CanSchedule: true, WorkerId: selected.Id}
}

func (c *capacityCheck) capFreeCapacity(worker *types.Worker) {
	if worker.TotalCpu > 0 && worker.FreeCpu > worker.TotalCpu {
		worker.FreeCpu = worker.TotalCpu
	}
	if worker.TotalMemory > 0 && worker.FreeMemory > worker.TotalMemory {
		worker.FreeMemory = worker.TotalMemory
	}
	if worker.TotalGpuCount > 0 && worker.FreeGpuCount > worker.TotalGpuCount {
		worker.FreeGpuCount = worker.TotalGpuCount
	}
}
