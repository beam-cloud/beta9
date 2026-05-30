package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type AgentWorkerPoolController struct {
	ctx              context.Context
	name             string
	workspaceID      string
	config           types.AppConfig
	workerPoolConfig types.WorkerPoolConfig
	poolState        *compute.PoolState
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	computeRepo      repository.ComputeRepository
}

type AgentWorkerPoolControllerOptions struct {
	Context        context.Context
	Name           string
	WorkspaceID    string
	Config         types.AppConfig
	WorkerPool     types.WorkerPoolConfig
	PoolState      *compute.PoolState
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
	ComputeRepo    repository.ComputeRepository
}

type agentMachineWorker struct {
	id                   string
	cpu                  int64
	memory               int64
	gpu                  string
	gpuCount             uint32
	poolName             string
	machineID            string
	requiresPoolSelector bool
	priority             int32
	preemptable          bool
	runtime              string
	buildVersion         string
}

func NewAgentWorkerPoolController(opts AgentWorkerPoolControllerOptions) (WorkerPoolController, error) {
	if opts.WorkspaceID == "" {
		return nil, errors.New("workspace id is required")
	}
	if opts.Name == "" {
		return nil, errors.New("pool name is required")
	}
	if opts.ComputeRepo == nil {
		return nil, errors.New("compute repository is required")
	}

	wpc := &AgentWorkerPoolController{
		ctx:              opts.Context,
		name:             opts.Name,
		workspaceID:      opts.WorkspaceID,
		config:           opts.Config,
		workerPoolConfig: opts.WorkerPool,
		poolState:        opts.PoolState,
		workerRepo:       opts.WorkerRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		computeRepo:      opts.ComputeRepo,
	}
	if err := wpc.ensureMachineWorkers(); err != nil {
		return nil, err
	}
	return wpc, nil
}

func (wpc *AgentWorkerPoolController) Context() context.Context {
	return wpc.ctx
}

func (wpc *AgentWorkerPoolController) IsPreemptable() bool {
	return wpc.workerPoolConfig.Preemptable
}

func (wpc *AgentWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *AgentWorkerPoolController) ContainerRuntime() string {
	runtime := wpc.workerPoolConfig.ContainerRuntime
	if runtime == "" {
		return types.ContainerRuntimeRunc.String()
	}
	return runtime
}

func (wpc *AgentWorkerPoolController) RequiresPoolSelector() bool {
	return wpc.workerPoolConfig.RequiresPoolSelector
}

func (wpc *AgentWorkerPoolController) Mode() types.PoolMode {
	return types.PoolModePrivate
}

func (wpc *AgentWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc)
}

func (wpc *AgentWorkerPoolController) State() (*types.WorkerPoolState, error) {
	if wpc.workerPoolRepo != nil {
		if state, err := wpc.workerPoolRepo.GetWorkerPoolState(wpc.ctx, wpc.name); err == nil && state != nil {
			return state, nil
		}
	}

	machines, err := wpc.computeRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, err
	}
	readyMachines := int64(0)
	for _, machine := range machines {
		if wpc.machineSchedulable(machine) {
			readyMachines++
		}
	}
	status := types.WorkerPoolStatusHealthy
	if readyMachines == 0 {
		status = types.WorkerPoolStatusDegraded
	}
	return &types.WorkerPoolState{
		Status:             status,
		RegisteredMachines: int64(len(machines)),
		ReadyMachines:      readyMachines,
	}, nil
}

func (wpc *AgentWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	machine, err := wpc.findMachine(func(machine *compute.AgentTokenState) bool {
		return wpc.machineCanFit(machine, cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
	})
	if err != nil {
		return nil, err
	}
	if machine == nil {
		return nil, fmt.Errorf("no joined agent machine in pool %q has enough capacity", wpc.name)
	}
	return wpc.ensureMachineWorker(machine)
}

func (wpc *AgentWorkerPoolController) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineID string) (*types.Worker, error) {
	machine, err := wpc.findMachine(func(machine *compute.AgentTokenState) bool {
		return machine.MachineID == machineID && wpc.machineCanFit(machine, cpu, memory, gpuType, gpuCount)
	})
	if err != nil {
		return nil, err
	}
	if machine == nil {
		return nil, nil
	}
	return wpc.ensureMachineWorker(machine)
}

func (wpc *AgentWorkerPoolController) WorkspaceID() string {
	return wpc.workspaceID
}

func (wpc *AgentWorkerPoolController) poolName() string {
	if wpc.poolState != nil && wpc.poolState.Name != "" {
		return wpc.poolState.Name
	}
	return wpc.name
}

func (wpc *AgentWorkerPoolController) ensureMachineWorkers() error {
	machines, err := wpc.computeRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return err
	}
	for _, machine := range machines {
		if !wpc.machineSchedulable(machine) {
			if worker, err := wpc.machineWorker(machine); err == nil && worker != nil {
				_ = wpc.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled)
			}
			continue
		}
		if _, err := wpc.ensureMachineWorker(machine); err != nil {
			return err
		}
	}
	return nil
}

func (wpc *AgentWorkerPoolController) findMachine(match func(*compute.AgentTokenState) bool) (*compute.AgentTokenState, error) {
	machines, err := wpc.computeRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, err
	}
	for _, machine := range machines {
		if !wpc.machineSchedulable(machine) {
			continue
		}
		if match == nil || match(machine) {
			return machine, nil
		}
	}
	return nil, nil
}

func (wpc *AgentWorkerPoolController) machineSchedulable(machine *compute.AgentTokenState) bool {
	return machine != nil &&
		machine.Schedulable &&
		machine.Executor == types.DefaultAgentWorkerContainerMode &&
		machine.WorkspaceID == wpc.workspaceID &&
		machine.PoolName == wpc.poolName()
}

func (wpc *AgentWorkerPoolController) machineCanFit(machine *compute.AgentTokenState, cpu int64, memory int64, gpuType string, gpuCount uint32) bool {
	if machine == nil {
		return false
	}
	worker, err := wpc.machineWorker(machine)
	if err != nil {
		return false
	}
	capacity := wpc.agentMachineWorker(machine)
	availableCPU := capacity.cpu
	availableMemory := capacity.memory
	availableGPU := capacity.gpuCount
	if worker != nil {
		switch worker.Status {
		case types.WorkerStatusAvailable:
			availableCPU = worker.FreeCpu
			availableMemory = worker.FreeMemory
			availableGPU = worker.FreeGpuCount
		case types.WorkerStatusPending:
			availableCPU = worker.TotalCpu
			availableMemory = worker.TotalMemory
			availableGPU = worker.TotalGpuCount
		default:
			return false
		}
	}
	if availableCPU < cpu || availableMemory < memory || availableGPU < gpuCount {
		return false
	}
	if gpuCount == 0 {
		return true
	}
	if gpuType == "" || strings.EqualFold(gpuType, string(types.GPU_ANY)) {
		return true
	}
	for _, machineGPU := range machine.GPUs {
		if strings.EqualFold(machineGPU, gpuType) {
			return true
		}
	}
	return false
}

func (wpc *AgentWorkerPoolController) machineWorker(machine *compute.AgentTokenState) (*types.Worker, error) {
	if machine == nil {
		return nil, nil
	}
	worker, err := wpc.workerRepo.GetWorkerById(compute.AgentMachineWorkerID(machine.MachineID))
	if err != nil {
		notFoundErr := &types.ErrWorkerNotFound{}
		if notFoundErr.From(err) {
			return nil, nil
		}
		return nil, err
	}
	if worker.PoolName != wpc.name || worker.MachineId != machine.MachineID {
		return nil, nil
	}
	return worker, nil
}

func (wpc *AgentWorkerPoolController) ensureMachineWorker(machine *compute.AgentTokenState) (*types.Worker, error) {
	spec := wpc.agentMachineWorker(machine)
	if worker, err := wpc.machineWorker(machine); err != nil || worker != nil {
		return worker, err
	}

	worker := spec.worker()
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		return nil, err
	}
	return worker, nil
}

func (wpc *AgentWorkerPoolController) agentMachineWorker(machine *compute.AgentTokenState) agentMachineWorker {
	cpu := int64(machine.CPUCount) * 1000
	if machine.CPUMillicores > 0 {
		cpu = machine.CPUMillicores
	}
	gpu := wpc.workerPoolConfig.GPUType
	if gpu == "" && len(machine.GPUs) > 0 {
		gpu = machine.GPUs[0]
	}
	return agentMachineWorker{
		id:                   compute.AgentMachineWorkerID(machine.MachineID),
		cpu:                  cpu,
		memory:               int64(machine.MemoryMB),
		gpu:                  gpu,
		gpuCount:             machine.GPUCount,
		poolName:             wpc.poolName(),
		machineID:            machine.MachineID,
		requiresPoolSelector: wpc.workerPoolConfig.RequiresPoolSelector,
		priority:             wpc.workerPoolConfig.Priority,
		preemptable:          wpc.workerPoolConfig.Preemptable,
		runtime:              wpc.ContainerRuntime(),
		buildVersion:         wpc.config.Worker.ImageTag,
	}
}

func (m agentMachineWorker) worker() *types.Worker {
	return &types.Worker{
		Id:                   m.id,
		Status:               types.WorkerStatusPending,
		TotalCpu:             m.cpu,
		TotalMemory:          m.memory,
		TotalGpuCount:        m.gpuCount,
		FreeCpu:              m.cpu,
		FreeMemory:           m.memory,
		FreeGpuCount:         m.gpuCount,
		Gpu:                  m.gpu,
		PoolName:             m.poolName,
		MachineId:            m.machineID,
		RequiresPoolSelector: m.requiresPoolSelector,
		Priority:             m.priority,
		Preemptable:          m.preemptable,
		Runtime:              m.runtime,
		BuildVersion:         m.buildVersion,
	}
}
