package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// AgentWorkerPoolController manages machines that connect to the control plane
// through the agent protocol, including private and managed external pools.
type AgentWorkerPoolController struct {
	ctx              context.Context
	cancel           context.CancelFunc
	name             string
	workspaceID      string
	config           types.AppConfig
	workerPoolConfig types.WorkerPoolConfig
	poolState        *compute.PoolState
	workerRepo       repository.WorkerRepository
	computeRepo      repository.ComputeRepository
	provisioner      Provisioner
	waitlist         *Waitlist
	clock            common.Clock
}

type AgentWorkerPoolControllerOptions struct {
	Context        context.Context
	Name           string
	WorkspaceID    string
	Config         types.AppConfig
	WorkerPool     types.WorkerPoolConfig
	PoolState      *compute.PoolState
	WorkerRepo     repository.WorkerRepository
	ComputeRepo    repository.ComputeRepository
	WorkerPoolRepo repository.WorkerPoolRepository
	ContainerRepo  repository.ContainerRepository
	PushMetrics    func(types.EventComputeSchema)
	// Provisioner is what a pool with a provisioning block buys its machines
	// from, and Waitlist the requests it buys them for. Other pools use
	// neither.
	Provisioner Provisioner
	Waitlist    *Waitlist
	// Clock decides whether a machine's heartbeat is fresh and drives the
	// controller's loops. Nil means the wall clock.
	Clock common.Clock
}

type agentMachineWorker struct {
	id                   string
	workspaceID          string
	controlPlaneManaged  bool
	cpu                  int64
	memory               int64
	gpu                  string
	gpuCount             uint32
	poolName             string
	poolSelector         string
	machineID            string
	requiresPoolSelector bool
	priority             int32
	preemptable          bool
	runtime              string
	buildVersion         string
	workerImageOverride  string
	cordoned             bool
}

type AgentPoolCapacityError struct {
	WorkspaceID         string
	PoolName            string
	CPU                 int64
	Memory              int64
	GPUType             string
	GPUCount            uint32
	Machines            int
	SchedulableMachines int
	MaxAvailableCPU     int64
	MaxAvailableMemory  int64
	MaxAvailableGPU     uint32
}

func (e *AgentPoolCapacityError) Error() string {
	if e == nil {
		return "agent pool capacity unavailable"
	}
	return fmt.Sprintf(
		"no joined agent machine in workspace %q pool %q has enough capacity (requested cpu=%d memory=%d gpu=%q gpu_count=%d, schedulable_machines=%d/%d, max_available_cpu=%d max_available_memory=%d max_available_gpu=%d)",
		e.WorkspaceID,
		e.PoolName,
		e.CPU,
		e.Memory,
		e.GPUType,
		e.GPUCount,
		e.SchedulableMachines,
		e.Machines,
		e.MaxAvailableCPU,
		e.MaxAvailableMemory,
		e.MaxAvailableGPU,
	)
}

func NewAgentWorkerPoolController(opts AgentWorkerPoolControllerOptions) (*AgentWorkerPoolController, error) {
	if opts.WorkspaceID == "" {
		return nil, errors.New("workspace id is required")
	}
	if opts.Name == "" {
		return nil, errors.New("pool name is required")
	}
	if opts.ComputeRepo == nil {
		return nil, errors.New("compute repository is required")
	}

	ctx, cancel := context.WithCancel(opts.Context)
	wpc := &AgentWorkerPoolController{
		ctx:              ctx,
		cancel:           cancel,
		name:             opts.Name,
		workspaceID:      opts.WorkspaceID,
		config:           opts.Config,
		workerPoolConfig: opts.WorkerPool,
		poolState:        opts.PoolState,
		workerRepo:       opts.WorkerRepo,
		computeRepo:      opts.ComputeRepo,
		provisioner:      opts.Provisioner,
		waitlist:         opts.Waitlist,
		clock:            common.ClockOrReal(opts.Clock),
	}
	// A pool that buys its machines can grow, so it can keep a floor free.
	// A pool of machines that joined by hand cannot add one.
	if opts.WorkerPool.Buys() && opts.WorkerPoolRepo != nil {
		MonitorPoolSize(wpc, opts.Config, &wpc.workerPoolConfig, opts.WorkerPoolRepo, opts.Clock)
	}
	if opts.PoolState.ManagementSource != "" &&
		opts.WorkerPoolRepo != nil &&
		opts.ContainerRepo != nil &&
		opts.PushMetrics != nil {
		MonitorPoolHealth(PoolHealthMonitorOptions{
			Controller:       wpc,
			WorkerPoolConfig: opts.WorkerPool,
			WorkerRepo:       opts.WorkerRepo,
			WorkerPoolRepo:   opts.WorkerPoolRepo,
			ContainerRepo:    opts.ContainerRepo,
			PushMetrics:      opts.PushMetrics,
			Clock:            opts.Clock,
		})
	}
	return wpc, nil
}

func (wpc *AgentWorkerPoolController) close() {
	if wpc != nil && wpc.cancel != nil {
		wpc.cancel()
	}
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

func (wpc *AgentWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc.poolName())
}

func (wpc *AgentWorkerPoolController) State() (*types.WorkerPoolState, error) {
	machines, registered, err := wpc.schedulableMachines()
	if err != nil {
		return nil, err
	}
	readyMachines := int64(0)
	pendingMachines := int64(0)
	for _, machine := range machines {
		worker, err := wpc.machineWorker(machine)
		if err != nil {
			return nil, err
		}
		if worker != nil && worker.Status == types.WorkerStatusAvailable {
			readyMachines++
			continue
		}
		pendingMachines++
	}
	status := types.WorkerPoolStatusHealthy
	if readyMachines == 0 {
		status = types.WorkerPoolStatusDegraded
	}
	return &types.WorkerPoolState{
		Status:             status,
		RegisteredMachines: int64(registered),
		PendingMachines:    pendingMachines,
		ReadyMachines:      readyMachines,
	}, nil
}

// CanAddWorker reports whether the pool can add a worker of this size. A
// pool that buys its machines can when one can be bought; any other can when
// a joined machine has room.
func (wpc *AgentWorkerPoolController) CanAddWorker(cpu int64, memory int64, gpuCount uint32) (bool, error) {
	if wpc.workerPoolConfig.Buys() {
		return wpc.canBuyMachine(wpc.machineNeed(cpu, memory, gpuCount)), nil
	}
	machine, err := wpc.findMachineForRequest(cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
	return machine != nil, err
}

func (wpc *AgentWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	if wpc.workerPoolConfig.Buys() {
		return wpc.buyMachine(wpc.machineNeed(cpu, memory, gpuCount))
	}
	machine, err := wpc.findMachineForRequest(cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
	if err != nil {
		return nil, err
	}
	if machine == nil {
		return nil, wpc.capacityError(cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
	}
	return wpc.ensureMachineWorker(machine)
}

func (wpc *AgentWorkerPoolController) WorkspaceID() string {
	return wpc.workspaceID
}

func (wpc *AgentWorkerPoolController) owns(workspaceID, selector string, state *compute.PoolState) bool {
	if state == nil || wpc.poolState == nil {
		return false
	}
	if wpc.name != selector || wpc.poolName() != state.Name || wpc.poolState.Mode != state.Mode {
		return false
	}
	return wpc.workspaceID == workspaceID && wpc.poolState.ManagementSource == state.ManagementSource
}

func (wpc *AgentWorkerPoolController) poolName() string {
	if wpc.poolState != nil && wpc.poolState.Name != "" {
		return wpc.poolState.Name
	}
	return wpc.name
}

// ensureMachine looks after one machine, when its agent is heard from.
func (wpc *AgentWorkerPoolController) ensureMachine(machineID string) error {
	machine, err := wpc.computeRepo.GetAgentMachineState(wpc.ctx, wpc.workspaceID, wpc.poolName(), machineID)
	if err != nil || machine == nil {
		return err
	}
	return wpc.reconcileMachine(machine)
}

// reconcileMachines looks after every machine of the pool.
func (wpc *AgentWorkerPoolController) reconcileMachines() error {
	machines, err := wpc.computeRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return err
	}
	var errs []error
	for _, machine := range machines {
		errs = append(errs, wpc.reconcileMachine(machine))
	}
	if wpc.workerPoolConfig.Buys() {
		errs = append(errs, wpc.keepBootingWorkers(machines))
	}
	return errors.Join(errs...)
}

// reconcileMachine brings a machine's worker in line with the machine: one
// that work can be placed on has a worker, and the worker of one that cannot
// take work is disabled.
func (wpc *AgentWorkerPoolController) reconcileMachine(machine *compute.AgentTokenState) error {
	if wpc.machineSchedulable(machine) {
		_, err := wpc.ensureMachineWorker(machine)
		return err
	}

	// A worker that is disabled already is left alone: every write gives its
	// record another minute, and the record of a machine that died has to
	// expire for its requests to be requeued.
	worker, err := wpc.machineWorker(machine)
	if err != nil || worker == nil || worker.Status == types.WorkerStatusDisabled {
		return err
	}
	return wpc.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled)
}

// schedulableMachines lists the pool's machines and returns those that work
// can be placed on, with the number of machines the pool has in all.
func (wpc *AgentWorkerPoolController) schedulableMachines() (schedulable []*compute.AgentTokenState, registered int, err error) {
	machines, err := wpc.computeRepo.ListAgentTokenStates(wpc.ctx, wpc.workspaceID, wpc.poolName())
	if err != nil {
		return nil, 0, err
	}
	for _, machine := range machines {
		if wpc.machineSchedulable(machine) {
			schedulable = append(schedulable, machine)
		}
	}
	return schedulable, len(machines), nil
}

// findMachineForRequest returns a machine with room for a worker of this
// size, or nil when the pool has none.
func (wpc *AgentWorkerPoolController) findMachineForRequest(cpu int64, memory int64, gpuType string, gpuCount uint32) (*compute.AgentTokenState, error) {
	machines, _, err := wpc.schedulableMachines()
	if err != nil {
		return nil, err
	}
	for _, machine := range machines {
		if wpc.machineCanFit(machine, cpu, memory, gpuType, gpuCount) {
			return machine, nil
		}
	}
	return nil, nil
}

// capacityError says what the pool's machines had when none had room for a
// worker of this size.
func (wpc *AgentWorkerPoolController) capacityError(cpu int64, memory int64, gpuType string, gpuCount uint32) error {
	machines, registered, err := wpc.schedulableMachines()
	if err != nil {
		return err
	}
	noRoom := &AgentPoolCapacityError{
		WorkspaceID:         wpc.workspaceID,
		PoolName:            wpc.poolName(),
		CPU:                 cpu,
		Memory:              memory,
		GPUType:             gpuType,
		GPUCount:            gpuCount,
		Machines:            registered,
		SchedulableMachines: len(machines),
	}
	for _, machine := range machines {
		worker, err := wpc.machineWorker(machine)
		if err != nil {
			continue
		}
		availableCPU, availableMemory, availableGPU := wpc.machineAvailableCapacity(machine, worker)
		noRoom.MaxAvailableCPU = max(noRoom.MaxAvailableCPU, availableCPU)
		noRoom.MaxAvailableMemory = max(noRoom.MaxAvailableMemory, availableMemory)
		noRoom.MaxAvailableGPU = max(noRoom.MaxAvailableGPU, availableGPU)
	}
	return noRoom
}

// liveWorkerIDs returns the ids of the workers of the pool's machines that
// work can be placed on now: a machine that has dropped out leaves its
// worker on record for a while.
func (wpc *AgentWorkerPoolController) liveWorkerIDs() (map[string]struct{}, error) {
	machines, _, err := wpc.schedulableMachines()
	if err != nil {
		return nil, err
	}
	live := make(map[string]struct{}, len(machines))
	for _, machine := range machines {
		live[compute.AgentMachineWorkerID(machine.MachineID)] = struct{}{}
	}
	return live, nil
}

func (wpc *AgentWorkerPoolController) machineSchedulable(machine *compute.AgentTokenState) bool {
	return machine != nil &&
		compute.AgentMachineConnected(machine, wpc.clock.Now()) &&
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
	availableCPU, availableMemory, availableGPU := wpc.machineAvailableCapacity(machine, worker)
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
		if machineGPU == gpuType {
			return true
		}
	}
	return false
}

func (wpc *AgentWorkerPoolController) machineAvailableCapacity(machine *compute.AgentTokenState, worker *types.Worker) (int64, int64, uint32) {
	capacity := wpc.agentMachineWorker(machine)
	if worker == nil {
		return capacity.cpu, capacity.memory, capacity.gpuCount
	}
	switch worker.Status {
	case types.WorkerStatusAvailable:
		// Capacity held by evictable containers is reclaimable, so it counts
		// as available for the serverless requests that decide provisioning.
		return worker.FreeCpu + worker.EvictableCpu, worker.FreeMemory + worker.EvictableMemory, worker.FreeGpuCount + worker.EvictableGpuCount
	case types.WorkerStatusPending:
		return worker.TotalCpu, worker.TotalMemory, worker.TotalGpuCount
	case types.WorkerStatusDisabled:
		if worker.CordonRequested || worker.RolloutGeneration != "" {
			return 0, 0, 0
		}
		return capacity.cpu, capacity.memory, capacity.gpuCount
	default:
		return 0, 0, 0
	}
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
	if worker.PoolName != wpc.poolName() || worker.MachineId != machine.MachineID {
		return nil, nil
	}
	return worker, nil
}

func (wpc *AgentWorkerPoolController) ensureMachineWorker(machine *compute.AgentTokenState) (*types.Worker, error) {
	spec := wpc.agentMachineWorker(machine)
	worker, err := wpc.machineWorker(machine)
	if err != nil {
		return nil, err
	}
	// A pool that buys may buy both kinds of machine: a machine's worker is
	// of the kind the machine was bought as. Its worker says which that is,
	// and the record of its purchase when its worker is gone.
	switch {
	case buyer(wpc) == nil:
	case worker != nil:
		spec.preemptable = worker.Preemptable
	default:
		bought, err := wpc.provisioner.Machine(wpc.ctx, machine.MachineID)
		if err != nil {
			return nil, err
		}
		spec.preemptable = wpc.preemptable(bought)
	}
	if worker != nil {
		spec.applyToWorker(worker)
		if worker.CordonRequested || worker.RolloutGeneration != "" {
			worker.Status = types.WorkerStatusDisabled
			return worker, wpc.workerRepo.AddWorker(worker)
		}
		if worker.Status != types.WorkerStatusDisabled {
			return worker, wpc.workerRepo.AddWorker(worker)
		}
		// A disabled worker without durable cordon/rollout intent was disabled
		// by transient health reconciliation. Recreate it as pending while
		// preserving the last acknowledged runtime generation.
		next := spec.worker()
		if worker.BuildVersion != "" {
			next.BuildVersion = worker.BuildVersion
		}
		if err := wpc.workerRepo.AddWorker(next); err != nil {
			return nil, err
		}
		return next, nil
	}

	next := spec.worker()
	if err := wpc.workerRepo.AddWorker(next); err != nil {
		return nil, err
	}
	return next, nil
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
	worker := wpc.machineWorkerSpec(machine.MachineID, cpu, int64(machine.MemoryMB), gpu, machine.GPUCount)
	worker.workspaceID = machine.WorkspaceID
	worker.controlPlaneManaged = machine.ManagedPoolInstanceID != "" && machine.Mode == string(types.PoolModeExternal)
	worker.buildVersion = types.WorkerImageVersion(machine.WorkerImageOverride, wpc.config.Worker.ImageTag)
	worker.workerImageOverride = machine.WorkerImageOverride
	worker.cordoned = machine.Cordoned
	return worker
}

// machineWorkerSpec is the worker for a machine of this pool with this much
// physical capacity: what the pool decides about it, whoever the machine is.
func (wpc *AgentWorkerPoolController) machineWorkerSpec(machineID string, cpu, memory int64, gpu string, gpuCount uint32) agentMachineWorker {
	return agentMachineWorker{
		id:                   compute.AgentMachineWorkerID(machineID),
		cpu:                  wpc.workerPoolConfig.CPUShareMultiplier.Schedulable(cpu),
		memory:               memory,
		gpu:                  gpu,
		gpuCount:             gpuCount,
		poolName:             wpc.poolName(),
		poolSelector:         wpc.name,
		machineID:            machineID,
		requiresPoolSelector: wpc.workerPoolConfig.RequiresPoolSelector,
		priority:             wpc.workerPoolConfig.Priority,
		preemptable:          wpc.preemptable(nil),
		runtime:              wpc.ContainerRuntime(),
	}
}

// preemptable reports whether the work on a machine of the pool may be
// interrupted: all of it in a pool that says so, and that on a machine its
// provider may take back. bought is the machine as the pool bought it. It
// is nil when there is no record that the pool did, and the machine is then
// taken for one that may be taken back if the pool buys such machines.
func (wpc *AgentWorkerPoolController) preemptable(bought *ProvisionedMachine) bool {
	spot := wpc.workerPoolConfig.Provisioning.Spot
	if bought != nil {
		spot = bought.Spot
	}
	return spot || wpc.workerPoolConfig.Preemptable
}

func (m agentMachineWorker) worker() *types.Worker {
	worker := pendingWorker(m.id, m.cpu, m.memory, m.gpu, m.gpuCount)
	worker.MachineId = m.machineID
	if m.cordoned {
		worker.Status = types.WorkerStatusDisabled
	}
	m.applyToWorker(worker)
	return worker
}

func (m agentMachineWorker) applyToWorker(worker *types.Worker) {
	usedCPU := max(worker.TotalCpu-worker.FreeCpu, 0)
	usedMemory := max(worker.TotalMemory-worker.FreeMemory, 0)
	usedGPU := uint32(0)
	if worker.TotalGpuCount > worker.FreeGpuCount {
		usedGPU = worker.TotalGpuCount - worker.FreeGpuCount
	}

	worker.TotalCpu = m.cpu
	worker.FreeCpu = max(m.cpu-usedCPU, 0)
	worker.TotalMemory = m.memory
	worker.FreeMemory = max(m.memory-usedMemory, 0)
	worker.TotalGpuCount = m.gpuCount
	if usedGPU >= m.gpuCount {
		worker.FreeGpuCount = 0
	} else {
		worker.FreeGpuCount = m.gpuCount - usedGPU
	}

	worker.Gpu = m.gpu
	worker.WorkspaceId = m.workspaceID
	worker.ControlPlaneManaged = m.controlPlaneManaged
	worker.PoolName = m.poolName
	worker.PoolSelector = m.poolSelector
	worker.RequiresPoolSelector = m.requiresPoolSelector
	worker.Priority = m.priority
	worker.Preemptable = m.preemptable
	worker.Runtime = m.runtime
	worker.WorkerImageOverride = m.workerImageOverride
	worker.CordonRequested = m.cordoned
	if worker.BuildVersion == "" {
		worker.BuildVersion = m.buildVersion
	}
}

// A pool with a provisioning block adds a worker by buying a machine.
//
// The machine has a worker from the moment it is bought: a Pending one, of
// the machine's size, under the id the machine's worker will have. Requests
// wait for it and the sizer counts it, as they do for a Kubernetes worker
// that is starting. When the machine joins, the agent path takes that same
// worker over.
//
// Such a pool grows no other way. A machine that has joined already has its
// worker, so there is never one with room and no worker to give it.

// canBuyMachine reports whether a machine with room for any of needs can be
// bought now.
func (wpc *AgentWorkerPoolController) canBuyMachine(needs ...MachineNeed) bool {
	return wpc.provisioner != nil && wpc.provisioner.CanProvision(wpc.ctx, wpc.poolName(), needs...)
}

// buyMachine buys the first machine the pool is to have for needs and returns
// the Pending worker that stands for it.
func (wpc *AgentWorkerPoolController) buyMachine(needs ...MachineNeed) (*types.Worker, error) {
	if wpc.provisioner == nil {
		return nil, fmt.Errorf("pool %q buys its machines, but this gateway has nothing to buy them with", wpc.poolName())
	}
	machine, err := wpc.provisioner.Provision(wpc.ctx, wpc.poolName(), needs...)
	if err != nil {
		return nil, err
	}
	worker := wpc.bootingMachineWorker(*machine)
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		// The machine is bought all the same. Calling this a failure would
		// have the pool asked again, and a second machine bought. The
		// worker is made again by keepBootingWorkers.
		log.Warn().Err(err).Str("pool_name", wpc.poolName()).Str("machine_id", machine.MachineID).
			Msg("bought a machine but could not add the worker that stands for it")
	}
	return worker, nil
}

// machineNeed is the machine that holds a worker of this size. A worker's
// CPU is counted in schedulable millicores, a machine's in physical ones.
func (wpc *AgentWorkerPoolController) machineNeed(cpu int64, memory int64, gpuCount uint32) MachineNeed {
	return MachineNeed{
		CPU:      wpc.workerPoolConfig.CPUShareMultiplier.Physical(cpu),
		Memory:   memory,
		GPUCount: gpuCount,
	}
}

// bootingMachineWorker is the Pending worker for a machine that has been
// bought and has not joined. It matches what the agent path will make of the
// machine once it has, so the worker carries over.
func (wpc *AgentWorkerPoolController) bootingMachineWorker(machine ProvisionedMachine) *types.Worker {
	gpu := ""
	if machine.GPUCount > 0 {
		gpu = wpc.workerPoolConfig.GPUType
	}
	spec := wpc.machineWorkerSpec(machine.MachineID, machine.CPU, machine.Memory, gpu, machine.GPUCount)
	spec.workspaceID = wpc.workspaceID
	spec.controlPlaneManaged = true
	spec.buildVersion = types.WorkerImageVersion("", wpc.config.Worker.ImageTag)
	spec.preemptable = wpc.preemptable(&machine)
	return spec.worker()
}

// keepBootingWorkers keeps the Pending worker of every machine that is
// booting. A Pending worker expires after cleanupPendingWorkerAgeLimit,
// which a machine can take longer than to boot; without its worker the
// machine would stop counting as capacity and the pool would buy another.
//
// joined are the machines the agent path already looks after.
func (wpc *AgentWorkerPoolController) keepBootingWorkers(joined []*compute.AgentTokenState) error {
	if wpc.provisioner == nil {
		return nil
	}
	booting, err := wpc.provisioner.Booting(wpc.ctx, wpc.poolName())
	if err != nil {
		return err
	}

	hasJoined := map[string]bool{}
	for _, machine := range joined {
		if machine != nil {
			hasJoined[machine.MachineID] = true
		}
	}

	var errs []error
	for _, machine := range booting {
		if hasJoined[machine.MachineID] {
			continue
		}
		kept, err := wpc.workerRepo.KeepPendingWorker(compute.AgentMachineWorkerID(machine.MachineID))
		if err == nil && !kept {
			// The worker expired or Redis lost it: the machine is still coming.
			err = wpc.workerRepo.AddWorker(wpc.bootingMachineWorker(machine))
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Provisioner buys machines for pools that have a provisioning block. Such a
// pool adds a worker by buying a machine.
//
// It is declared here, next to the pool that calls it, so the scheduler
// depends on no provider code. pkg/provisioner implements it.
type Provisioner interface {
	// CanProvision reports whether the pool could buy a machine with room for
	// any of needs right now, not counting one it began to buy a moment ago.
	// It is false when no provider of the pool has matching stock, the pool
	// is at maxNodes or maxGPUs, a spend cap is reached, or a purchase for
	// the pool from each of them has just failed for any reason but lack of
	// stock. Lack of stock rules out only the offer that was asked for.
	CanProvision(ctx context.Context, pool string, needs ...MachineNeed) bool

	// Provision buys one machine for the pool and returns it before it has
	// booted: the first of the machines that are best to buy for all of
	// needs. The machine joins the pool on its own once it is up.
	Provision(ctx context.Context, pool string, needs ...MachineNeed) (*ProvisionedMachine, error)

	// Booting returns the pool's machines that have been bought and have not
	// yet run a worker.
	Booting(ctx context.Context, pool string) ([]ProvisionedMachine, error)

	// Machine returns a machine that a pool bought and holds, booting or
	// not, or nil when none has that ID.
	Machine(ctx context.Context, machineID string) (*ProvisionedMachine, error)
}

// MachineNeed is the capacity a new machine must have.
type MachineNeed struct {
	CPU      int64 // physical millicores
	Memory   int64 // MiB
	GPUCount uint32
	// OnDemand rules out a machine its provider may take back.
	OnDemand bool
	// HoursLost is how many hours of work are lost when one is taken back.
	HoursLost float64
}

// Compare orders needs by how hard it is to find them room, the hardest
// first: work that may not be interrupted, then the biggest.
func (n MachineNeed) Compare(other MachineNeed) int {
	return cmp.Or(compareBool(other.OnDemand, n.OnDemand), cmp.Compare(other.GPUCount, n.GPUCount), cmp.Compare(other.Memory, n.Memory),
		cmp.Compare(other.CPU, n.CPU), cmp.Compare(other.HoursLost, n.HoursLost))
}

// ProvisionedMachine is a machine that a pool bought.
type ProvisionedMachine struct {
	// MachineID is the ID the machine will join the pool with. It is chosen
	// before the purchase, so the worker that stands for the machine can be
	// created at once.
	MachineID string
	CPU       int64 // physical millicores
	Memory    int64 // MiB
	GPUCount  uint32
	// Spot is whether its provider may take it back.
	Spot bool
}
