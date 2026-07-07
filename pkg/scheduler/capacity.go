package scheduler

import "github.com/beam-cloud/beta9/pkg/types"

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
		worker.FreeCpu += container.Cpu
		worker.FreeMemory += container.Memory
		worker.FreeGpuCount += container.GpuCount
		c.capFreeCapacity(worker)
	}
}

func (c *capacityCheck) placeReplicas() CapacityCheckResult {
	var selected *types.Worker
	for i := uint(0); i < c.replicas; i++ {
		worker, err := c.scheduler.selectWorkerFromWorkers(c.workers, c.request.Clone())
		if err != nil {
			return CapacityCheckResult{Err: err}
		}
		c.reserve(worker)
		selected = worker
	}
	return CapacityCheckResult{CanSchedule: true, WorkerId: selected.Id}
}

func (c *capacityCheck) reserve(worker *types.Worker) {
	worker.FreeCpu -= c.request.Cpu
	worker.FreeMemory -= capacityMemoryForScheduling(c.request)
	if c.request.RequiresGPU() {
		worker.FreeGpuCount -= gpuCountForScheduling(c.request)
	}
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
