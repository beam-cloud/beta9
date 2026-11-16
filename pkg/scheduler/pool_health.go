package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog/log"
)

// PoolHealthMonitorOptions is the pool a monitor measures, where it reads
// the pool's workers and containers, and where what it finds goes.
type PoolHealthMonitorOptions struct {
	Controller       WorkerPoolController
	WorkerPoolConfig types.WorkerPoolConfig
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	ContainerRepo    repository.ContainerRepository
	PushMetrics      func(types.EventComputeSchema)
	// Clock drives the monitor. Nil means the wall clock.
	Clock common.Clock
}

// PoolHealthMonitor measures a pool on an interval. What it finds it stores
// as the pool's state, for the rest of the gateway to read, and sends as the
// pool's heartbeat on the compute event stream.
type PoolHealthMonitor struct {
	opts  PoolHealthMonitorOptions
	ctx   context.Context
	clock common.Clock
}

func NewPoolHealthMonitor(opts PoolHealthMonitorOptions) *PoolHealthMonitor {
	return &PoolHealthMonitor{
		opts:  opts,
		ctx:   opts.Controller.Context(),
		clock: common.ClockOrReal(opts.Clock),
	}
}

// tick measures the pool and publishes its state. Only one scheduler replica
// publishes a pool's state at a time.
func (p *PoolHealthMonitor) tick() {
	err := p.opts.WorkerPoolRepo.WithWorkerPoolStateLock(p.opts.Controller.Name(), p.report)
	if err != nil && !errors.Is(err, redislock.ErrNotObtained) {
		log.Error().Str("pool_name", p.opts.Controller.Name()).Err(err).Msg("failed to hold worker pool state lock")
	}
}

// report measures the pool, saves what it found and sends the pool's
// heartbeat.
func (p *PoolHealthMonitor) report() {
	poolName := p.opts.Controller.Name()
	poolState, workers, err := p.getPoolState()
	if err != nil {
		log.Error().Str("pool_name", poolName).Err(err).Msg("failed to get pool state")
		return
	}

	err = p.opts.WorkerPoolRepo.SetWorkerPoolState(p.ctx, poolName, poolState)
	if err != nil {
		log.Error().Str("pool_name", poolName).Err(err).Msg("failed to set pool state")
		return
	}
	if p.opts.PushMetrics != nil {
		p.opts.PushMetrics(poolMetricsEvent(poolName, p.opts.WorkerPoolConfig, poolState, workers))
	}
}

// getPoolState measures the pool: its workers, what they run and how long
// that took to start, and the machines of a pool that has machines.
func (p *PoolHealthMonitor) getPoolState() (*types.WorkerPoolState, []*types.Worker, error) {
	workers, err := p.opts.WorkerRepo.GetAllWorkersInPool(p.opts.Controller.Name())
	if err != nil {
		return nil, nil, err
	}

	// A pool that can be measured is reported healthy.
	state := &types.WorkerPoolState{Status: types.WorkerPoolStatusHealthy}
	if p.opts.WorkerPoolConfig.AgentHosted() {
		machines, err := p.opts.Controller.State()
		if err != nil {
			return nil, nil, err
		}
		state.RegisteredMachines = machines.RegisteredMachines
		state.PendingMachines = machines.PendingMachines
		state.ReadyMachines = machines.ReadyMachines
	}

	schedulingLatencies := []time.Duration{}
	for _, worker := range workers {
		switch worker.Status {
		case types.WorkerStatusPending:
			state.PendingWorkers++
		case types.WorkerStatusAvailable:
			state.AvailableWorkers++
		}

		// Every container on the worker's index, stopping ones included.
		containers, err := p.opts.ContainerRepo.GetActiveContainersByWorkerId(worker.Id)
		if err != nil {
			continue
		}

		for _, container := range containers {
			switch container.Status {
			case types.ContainerStatusPending:
				state.PendingContainers++
			case types.ContainerStatusRunning:
				state.RunningContainers++
			}

			// Skip containers with invalid StartedAt times
			if container.StartedAt == 0 && container.Status == types.ContainerStatusRunning {
				continue
			}

			if container.Status == types.ContainerStatusPending {
				latency := p.clock.Now().Sub(time.Unix(container.ScheduledAt, 0))
				schedulingLatencies = append(schedulingLatencies, latency)
				continue
			}

			latency := time.Unix(container.StartedAt, 0).Sub(time.Unix(container.ScheduledAt, 0))
			schedulingLatencies = append(schedulingLatencies, latency)
		}
	}

	// Calculate the average scheduling latency
	// -- which is the time between when a container is scheduled and when it actually starts running
	averageSchedulingLatency := time.Duration(0)
	if count := len(schedulingLatencies); count > 0 {
		var total time.Duration
		for _, latency := range schedulingLatencies {
			total += latency
		}

		averageSchedulingLatency = total / time.Duration(count)
	}

	freeCapacity, err := p.opts.Controller.FreeCapacity()
	if err != nil {
		return nil, nil, err
	}

	state.SchedulingLatency = averageSchedulingLatency.Milliseconds()
	state.FreeGpu = freeCapacity.FreeGpu
	state.FreeCpu = freeCapacity.FreeCpu
	state.FreeMemory = freeCapacity.FreeMemory
	return state, workers, nil
}

func poolMetricsEvent(poolName string, config types.WorkerPoolConfig, state *types.WorkerPoolState, workers []*types.Worker) types.EventComputeSchema {
	var totalCPU, freeCPU, totalMemory, freeMemory int64
	var totalGPU, freeGPU uint32
	machines := map[string]struct{}{}
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		// Pending workers with machine IDs are already billable.
		if id := worker.MachineId; id != "" {
			machines[id] = struct{}{}
		} else if worker.Status == types.WorkerStatusAvailable && worker.Id != "" {
			machines[worker.Id] = struct{}{}
		}
		if worker.Status != types.WorkerStatusAvailable {
			continue
		}
		totalCPU += max(worker.TotalCpu, 0)
		freeCPU += max(worker.FreeCpu, 0)
		totalMemory += max(worker.TotalMemory, 0)
		freeMemory += max(worker.FreeMemory, 0)
		totalGPU += worker.TotalGpuCount
		freeGPU += min(worker.FreeGpuCount, worker.TotalGpuCount)
	}

	percentage := func(free, total int64) float64 {
		if total <= 0 {
			return 0
		}
		return float64(total-min(free, total)) / float64(total) * 100
	}
	machineCount := int64(len(machines))
	if config.AgentHosted() {
		machineCount = max(machineCount, state.RegisteredMachines)
	}
	hourlyCostCents := config.HourlyCostCents * machineCount
	return types.EventComputeSchema{
		PoolName:     poolName,
		Action:       types.EventComputeActionPoolHeartbeat,
		Status:       string(state.Status),
		CPUCount:     uint32((totalCPU + 999) / 1000),
		MemoryMB:     uint64(totalMemory),
		GPUCount:     totalGPU,
		MachineCount: uint32(max(machineCount, 0)),
		Attrs: map[string]string{
			types.EventComputeAttrContainerCount:        fmt.Sprintf("%d", state.RunningContainers),
			types.EventComputeAttrFreeGPUCount:          fmt.Sprintf("%d", freeGPU),
			types.EventComputeAttrCPUUtilizationPct:     fmt.Sprintf("%.2f", percentage(freeCPU, totalCPU)),
			types.EventComputeAttrMemoryUsedMB:          fmt.Sprintf("%d", totalMemory-min(freeMemory, totalMemory)),
			types.EventComputeAttrMemoryUtilizationPct:  fmt.Sprintf("%.2f", percentage(freeMemory, totalMemory)),
			types.EventComputeAttrHourlyCostCents:       fmt.Sprintf("%d", hourlyCostCents),
			types.EventComputeAttrWorkerCount:           fmt.Sprintf("%d", len(workers)),
			types.EventComputeAttrAvailableWorkerCount:  fmt.Sprintf("%d", state.AvailableWorkers),
			types.EventComputeAttrPendingWorkerCount:    fmt.Sprintf("%d", state.PendingWorkers),
			types.EventComputeAttrPendingContainerCount: fmt.Sprintf("%d", state.PendingContainers),
			types.EventComputeAttrSchedulingLatencyMs:   fmt.Sprintf("%d", state.SchedulingLatency),
			types.EventComputeAttrPoolMode:              string(config.Mode),
		},
	}
}

func newPoolMetricsPusher(ctx context.Context, backendRepo repository.BackendRepository, eventRepo repository.EventRepository, c common.Clock) func(types.EventComputeSchema) {
	if backendRepo == nil || eventRepo == nil {
		return func(types.EventComputeSchema) {}
	}
	var mu sync.Mutex
	var workspaceID string
	var nextLookup time.Time

	return func(event types.EventComputeSchema) {
		now := common.ClockOrReal(c).Now().UTC()
		mu.Lock()
		if workspaceID == "" {
			if now.Before(nextLookup) {
				mu.Unlock()
				return
			}
			nextLookup = now.Add(5 * time.Second)
			workspace, err := backendRepo.GetAdminWorkspace(ctx)
			if err != nil || workspace == nil || workspace.ExternalId == "" {
				mu.Unlock()
				return
			}
			workspaceID = workspace.ExternalId
		}
		event.WorkspaceID = workspaceID
		mu.Unlock()

		event.Timestamp = now
		eventRepo.PushComputeEvent(types.EventComputePool, event)
	}
}
