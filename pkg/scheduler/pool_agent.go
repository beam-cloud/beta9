package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type AgentWorkerPoolController struct {
	ctx              context.Context
	name             string
	workspaceID      string
	config           types.AppConfig
	workerPoolConfig types.WorkerPoolConfig
	poolState        *hybrid.PoolState
	backendRepo      repository.BackendRepository
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	hybridRepo       repository.HybridRepository
	eventRepo        repository.EventRepository
}

type AgentWorkerPoolControllerOptions struct {
	Context        context.Context
	Name           string
	WorkspaceID    string
	Config         types.AppConfig
	WorkerPool     types.WorkerPoolConfig
	PoolState      *hybrid.PoolState
	BackendRepo    repository.BackendRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
	HybridRepo     repository.HybridRepository
	EventRepo      repository.EventRepository
}

func NewAgentWorkerPoolController(opts AgentWorkerPoolControllerOptions) (WorkerPoolController, error) {
	if opts.WorkspaceID == "" {
		return nil, errors.New("workspace id is required")
	}
	if opts.Name == "" {
		return nil, errors.New("pool name is required")
	}
	if opts.HybridRepo == nil {
		return nil, errors.New("hybrid repository is required")
	}

	return &AgentWorkerPoolController{
		ctx:              opts.Context,
		name:             opts.Name,
		workspaceID:      opts.WorkspaceID,
		config:           opts.Config,
		workerPoolConfig: opts.WorkerPool,
		poolState:        opts.PoolState,
		backendRepo:      opts.BackendRepo,
		workerRepo:       opts.WorkerRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		hybridRepo:       opts.HybridRepo,
		eventRepo:        opts.EventRepo,
	}, nil
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
	return types.PoolModeHybrid
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

	machines, err := wpc.hybridRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, err
	}
	status := types.WorkerPoolStatusHealthy
	if len(machines) == 0 {
		status = types.WorkerPoolStatusDegraded
	}
	return &types.WorkerPoolState{
		Status:             status,
		RegisteredMachines: int64(len(machines)),
		ReadyMachines:      int64(len(machines)),
	}, nil
}

func (wpc *AgentWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	machine, gpuAssignment, gpuType, err := wpc.selectMachine(cpu, memory, gpuCount)
	if err != nil {
		return nil, err
	}
	return wpc.addWorkerToAgentMachine(machine, cpu, memory, gpuType, gpuCount, gpuAssignment)
}

func (wpc *AgentWorkerPoolController) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineID string) (*types.Worker, error) {
	machines, err := wpc.hybridRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, err
	}
	for _, machine := range machines {
		if machine.MachineID != machineID {
			continue
		}
		if !wpc.machineSchedulable(machine) {
			return nil, nil
		}
		assignment, resolvedGPU, ok := wpc.availableGPUAssignment(machine, gpuType, gpuCount)
		if !ok {
			return nil, nil
		}
		return wpc.addWorkerToAgentMachine(machine, cpu, memory, resolvedGPU, gpuCount, assignment)
	}
	return nil, nil
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

func (wpc *AgentWorkerPoolController) selectMachine(cpu int64, memory int64, gpuCount uint32) (*hybrid.AgentTokenState, string, string, error) {
	machines, err := wpc.hybridRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, "", "", err
	}
	for _, machine := range machines {
		if !wpc.machineSchedulable(machine) {
			continue
		}
		if !wpc.machineHasCapacity(machine, cpu, memory, gpuCount) {
			continue
		}
		assignment, gpu, ok := wpc.availableGPUAssignment(machine, wpc.workerPoolConfig.GPUType, gpuCount)
		if !ok {
			continue
		}
		return machine, assignment, gpu, nil
	}
	return nil, "", "", fmt.Errorf("no joined agent machine in pool %q has enough capacity", wpc.name)
}

func (wpc *AgentWorkerPoolController) machineSchedulable(machine *hybrid.AgentTokenState) bool {
	return machine != nil &&
		machine.Schedulable &&
		machine.Executor == types.DefaultAgentWorkerContainerMode &&
		machine.WorkspaceID == wpc.workspaceID &&
		machine.PoolName == wpc.poolName()
}

func (wpc *AgentWorkerPoolController) machineHasCapacity(machine *hybrid.AgentTokenState, cpu int64, memory int64, gpuCount uint32) bool {
	usedCPU, usedMemory, usedGPU := wpc.machineAllocatedCapacity(machine.MachineID)
	return int64(machine.CPUCount)*1000-usedCPU >= cpu &&
		int64(machine.MemoryMB)-usedMemory >= memory &&
		int64(machine.GPUCount)-usedGPU >= int64(gpuCount)
}

func (wpc *AgentWorkerPoolController) machineAllocatedCapacity(machineID string) (int64, int64, int64) {
	workers, err := wpc.workerRepo.GetAllWorkersOnMachine(machineID)
	if err != nil {
		return 0, 0, 0
	}
	var cpu int64
	var memory int64
	var gpu int64
	for _, worker := range workers {
		if worker.PoolName != wpc.name || worker.Status == types.WorkerStatusDisabled {
			continue
		}
		cpu += worker.TotalCpu
		memory += worker.TotalMemory
		gpu += int64(worker.TotalGpuCount)
	}
	return cpu, memory, gpu
}

func (wpc *AgentWorkerPoolController) availableGPUAssignment(machine *hybrid.AgentTokenState, requestedGPU string, gpuCount uint32) (string, string, bool) {
	if gpuCount == 0 {
		return "", "", true
	}

	used := wpc.usedGPUAssignments(machine.MachineID)
	for i := uint32(0); i < machine.GPUCount; i++ {
		assignment := strconv.FormatUint(uint64(i), 10)
		if slices.Contains(used, assignment) {
			continue
		}
		gpuType := requestedGPU
		if int(i) < len(machine.GPUs) && machine.GPUs[i] != "" {
			gpuType = machine.GPUs[i]
		}
		if requestedGPU != "" && gpuType != "" && !strings.EqualFold(requestedGPU, gpuType) {
			continue
		}
		return assignment, gpuType, true
	}
	return "", "", false
}

func (wpc *AgentWorkerPoolController) usedGPUAssignments(machineID string) []string {
	slots, err := wpc.hybridRepo.ListAgentWorkerSlotStates(wpc.ctx, wpc.workspaceID, wpc.poolName(), machineID)
	if err != nil {
		return nil
	}
	used := make([]string, 0, len(slots))
	for _, slot := range slots {
		if slot.GPUAssignment != "" {
			used = append(used, slot.GPUAssignment)
		}
	}
	return used
}

func (wpc *AgentWorkerPoolController) addWorkerToAgentMachine(machine *hybrid.AgentTokenState, cpu int64, memory int64, gpuType string, gpuCount uint32, gpuAssignment string) (*types.Worker, error) {
	workspace, err := wpc.backendRepo.GetWorkspaceByExternalId(wpc.ctx, wpc.workspaceID)
	if err != nil {
		return nil, err
	}
	token, err := wpc.backendRepo.CreateToken(wpc.ctx, workspace.Id, types.TokenTypeWorker, true)
	if err != nil {
		return nil, err
	}

	workerID := GenerateWorkerId()
	worker := &types.Worker{
		Id:                   workerID,
		Status:               types.WorkerStatusPending,
		TotalCpu:             cpu,
		TotalMemory:          memory,
		TotalGpuCount:        gpuCount,
		FreeCpu:              cpu,
		FreeMemory:           memory,
		FreeGpuCount:         gpuCount,
		Gpu:                  gpuType,
		PoolName:             wpc.name,
		MachineId:            machine.MachineID,
		RequiresPoolSelector: wpc.workerPoolConfig.RequiresPoolSelector,
		Priority:             wpc.workerPoolConfig.Priority,
		Preemptable:          wpc.workerPoolConfig.Preemptable,
		Runtime:              wpc.ContainerRuntime(),
		BuildVersion:         wpc.config.Worker.ImageTag,
	}
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		return nil, err
	}

	slot := &hybrid.AgentWorkerSlotState{
		WorkerID:      workerID,
		WorkerToken:   token.Key,
		WorkspaceID:   wpc.workspaceID,
		PoolName:      wpc.poolName(),
		MachineID:     machine.MachineID,
		CPU:           cpu,
		Memory:        memory,
		GPU:           gpuType,
		GPUCount:      gpuCount,
		GPUAssignment: gpuAssignment,
		NetworkPrefix: common.WorkerNetworkPrefix(wpc.config.ClusterName, machine.MachineID),
		WorkerImage:   agentWorkerImage(wpc.config),
		CreatedAt:     time.Now(),
	}
	if err := wpc.hybridRepo.SaveAgentWorkerSlotState(wpc.ctx, slot); err != nil {
		_ = wpc.workerRepo.RemoveWorker(workerID)
		return nil, err
	}

	if wpc.eventRepo != nil {
		wpc.eventRepo.PushHybridEvent(types.EventHybridMachine, types.EventHybridSchema{
			WorkspaceID: wpc.workspaceID,
			PoolName:    wpc.poolName(),
			MachineID:   machine.MachineID,
			WorkerID:    workerID,
			Action:      types.EventHybridActionWorkerSlotCreated,
			Status:      string(types.WorkerStatusPending),
			CPUCount:    uint32(cpu),
			MemoryMB:    uint64(memory),
			GPUCount:    gpuCount,
			GPUs:        []string{gpuType},
		})
	}
	return worker, nil
}

func agentWorkerImage(config types.AppConfig) string {
	image := strings.TrimSuffix(config.Worker.ImageRegistry, "/")
	if image != "" {
		image += "/"
	}
	image += config.Worker.ImageName
	if config.Worker.ImageTag != "" {
		image += ":" + config.Worker.ImageTag
	}
	return image
}
