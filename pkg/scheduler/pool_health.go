package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type PoolHealthMonitorOptions struct {
	Controller       WorkerPoolController
	WorkerPoolConfig types.WorkerPoolConfig
	WorkerConfig     types.WorkerConfig
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	ProviderRepo     repository.ProviderRepository
	ContainerRepo    repository.ContainerRepository
	BackendRepo      repository.BackendRepository
	EventRepo        repository.EventRepository
}

type PoolHealthMonitor struct {
	ctx              context.Context
	wpc              WorkerPoolController
	workerPoolConfig types.WorkerPoolConfig
	workerConfig     types.WorkerConfig
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	containerRepo    repository.ContainerRepository
	providerRepo     repository.ProviderRepository
	backendRepo      repository.BackendRepository
	eventRepo        repository.EventRepository
	workspaceID      string
}

func NewPoolHealthMonitor(opts PoolHealthMonitorOptions) *PoolHealthMonitor {
	return &PoolHealthMonitor{
		ctx:              opts.Controller.Context(),
		wpc:              opts.Controller,
		workerPoolConfig: opts.WorkerPoolConfig,
		workerConfig:     opts.WorkerConfig,
		workerRepo:       opts.WorkerRepo,
		containerRepo:    opts.ContainerRepo,
		providerRepo:     opts.ProviderRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		backendRepo:      opts.BackendRepo,
		eventRepo:        opts.EventRepo,
	}
}

func (p *PoolHealthMonitor) Start() {
	ticker := time.NewTicker(poolHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			func() {
				if err := p.workerPoolRepo.SetWorkerPoolStateLock(p.wpc.Name()); err != nil {
					return
				}
				defer p.workerPoolRepo.RemoveWorkerPoolStateLock(p.wpc.Name())

				poolState, workers, err := p.getPoolState()
				if err != nil {
					log.Error().Str("pool_name", p.wpc.Name()).Err(err).Msg("failed to get pool state")
					return
				}

				err = p.updatePoolStatus(poolState)
				if err != nil {
					log.Error().Str("pool_name", p.wpc.Name()).Err(err).Msg("failed to update pool status")
					return
				}

				err = p.workerPoolRepo.SetWorkerPoolState(p.ctx, p.wpc.Name(), poolState)
				if err != nil {
					log.Error().Str("pool_name", p.wpc.Name()).Err(err).Msg("failed to set pool state")
					return
				}
				p.pushPoolMetrics(poolMetricsEvent(p.wpc.Name(), p.workerPoolConfig, poolState, workers))
			}()
		}
	}
}

// getPoolState measures various metrics about pool health and returns them
func (p *PoolHealthMonitor) getPoolState() (*types.WorkerPoolState, []*types.Worker, error) {
	schedulingLatencies := []time.Duration{}
	availableWorkers := 0
	pendingWorkers := 0
	pendingContainers := 0
	runningContainers := 0
	registeredMachines := 0
	pendingMachines := 0
	readyMachines := 0

	workers, err := p.workerRepo.GetAllWorkersInPool(p.wpc.Name())
	if err != nil {
		return nil, nil, err
	}

	switch p.wpc.Mode() {
	case types.PoolModeExternal:
		if p.workerPoolConfig.Provider == nil {
			poolState, err := p.wpc.State()
			if err != nil {
				return nil, nil, err
			}
			registeredMachines = int(poolState.RegisteredMachines)
			pendingMachines = int(poolState.PendingMachines)
			readyMachines = int(poolState.ReadyMachines)
		} else {
			providerName := string(*p.workerPoolConfig.Provider)
			machines, err := p.providerRepo.ListAllMachines(providerName, p.wpc.Name(), false)
			if err != nil {
				return nil, nil, err
			}

			for _, machine := range machines {
				switch machine.State.Status {
				case types.MachineStatusPending:
					pendingMachines++
				case types.MachineStatusRegistered:
					registeredMachines++
				case types.MachineStatusReady:
					readyMachines++
				}
			}
		}
	case types.PoolModePrivate:
		poolState, err := p.wpc.State()
		if err != nil {
			return nil, nil, err
		}
		registeredMachines = int(poolState.RegisteredMachines)
		pendingMachines = int(poolState.PendingMachines)
		readyMachines = int(poolState.ReadyMachines)
	}

	for _, worker := range workers {
		switch worker.Status {
		case types.WorkerStatusPending:
			pendingWorkers++
		case types.WorkerStatusAvailable:
			availableWorkers++
		}

		// Retrieve active containers for a worker (all containers associated w/ a worker that are not "STOPPING")
		containers, err := p.containerRepo.GetActiveContainersByWorkerId(worker.Id)
		if err != nil {
			continue
		}

		for _, container := range containers {
			switch container.Status {
			case types.ContainerStatusPending:
				pendingContainers++
			case types.ContainerStatusRunning:
				runningContainers++
			}

			// Skip containers with invalid StartedAt times
			if container.StartedAt == 0 && container.Status == types.ContainerStatusRunning {
				continue
			}

			if container.Status == types.ContainerStatusPending {
				latency := time.Since(time.Unix(container.ScheduledAt, 0))
				schedulingLatencies = append(schedulingLatencies, latency)
				continue
			}

			latency := time.Unix(container.StartedAt, 0).Sub(time.Unix(container.ScheduledAt, 0))
			metrics.RecordContainerStartLatency(&container, latency)
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

	freeCapacity, err := p.wpc.FreeCapacity()
	if err != nil {
		return nil, nil, err
	}

	return &types.WorkerPoolState{
		SchedulingLatency:  int64(averageSchedulingLatency.Milliseconds()),
		PendingWorkers:     int64(pendingWorkers),
		AvailableWorkers:   int64(availableWorkers),
		PendingContainers:  int64(pendingContainers),
		RunningContainers:  int64(runningContainers),
		FreeGpu:            freeCapacity.FreeGpu,
		FreeCpu:            freeCapacity.FreeCpu,
		FreeMemory:         freeCapacity.FreeMemory,
		RegisteredMachines: int64(registeredMachines),
		PendingMachines:    int64(pendingMachines),
		ReadyMachines:      int64(readyMachines),
	}, workers, nil
}

func poolMetricsEvent(poolName string, config types.WorkerPoolConfig, state *types.WorkerPoolState, workers []*types.Worker) types.EventComputeSchema {
	var totalCPU, freeCPU, totalMemory, freeMemory int64
	var totalGPU, freeGPU uint32
	machines := map[string]struct{}{}
	for _, worker := range workers {
		if worker == nil || worker.Status != types.WorkerStatusAvailable {
			continue
		}
		if id := firstNonEmpty(worker.MachineId, worker.Id); id != "" {
			machines[id] = struct{}{}
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
	hourlyCostMicros := types.ComputeDollarsToMicros(config.DefaultMachineCost * 3600 * float64(len(machines)))
	return types.EventComputeSchema{
		PoolName:     poolName,
		Action:       types.EventComputeActionPoolHeartbeat,
		Status:       string(state.Status),
		CPUCount:     uint32((totalCPU + 999) / 1000),
		MemoryMB:     uint64(totalMemory),
		GPUCount:     totalGPU,
		MachineCount: uint32(len(machines)),
		Attrs: map[string]string{
			"container_count":         fmt.Sprintf("%d", state.RunningContainers),
			"free_gpu_count":          fmt.Sprintf("%d", freeGPU),
			"cpu_utilization_pct":     fmt.Sprintf("%.2f", percentage(freeCPU, totalCPU)),
			"memory_used_mb":          fmt.Sprintf("%d", totalMemory-min(freeMemory, totalMemory)),
			"memory_utilization_pct":  fmt.Sprintf("%.2f", percentage(freeMemory, totalMemory)),
			"hourly_cost_micros":      fmt.Sprintf("%d", hourlyCostMicros),
			"worker_count":            fmt.Sprintf("%d", len(workers)),
			"available_worker_count":  fmt.Sprintf("%d", state.AvailableWorkers),
			"pending_worker_count":    fmt.Sprintf("%d", state.PendingWorkers),
			"pending_container_count": fmt.Sprintf("%d", state.PendingContainers),
			"scheduling_latency_ms":   fmt.Sprintf("%d", state.SchedulingLatency),
			"pool_mode":               string(config.Mode),
		},
	}
}

func (p *PoolHealthMonitor) pushPoolMetrics(event types.EventComputeSchema) {
	if p.eventRepo == nil || p.backendRepo == nil {
		return
	}
	if p.workspaceID == "" {
		workspace, err := p.backendRepo.GetAdminWorkspace(p.ctx)
		if err != nil || workspace == nil || workspace.ExternalId == "" {
			return
		}
		p.workspaceID = workspace.ExternalId
	}
	event.Timestamp = time.Now().UTC()
	event.WorkspaceID = p.workspaceID
	p.eventRepo.PushComputeEvent(types.EventComputePool, event)
}

// updatePoolStatus updates the status of the pool based on the current state
func (p *PoolHealthMonitor) updatePoolStatus(nextState *types.WorkerPoolState) error {
	status := types.WorkerPoolStatusHealthy
	failoverReasons := []string{}

	// Go through each condition that could trigger a degraded status
	if nextState.PendingWorkers >= p.workerConfig.Failover.MaxPendingWorkers &&
		nextState.SchedulingLatency > p.workerConfig.Failover.MaxSchedulingLatencyMs {
		status = types.WorkerPoolStatusDegraded
		failoverReasons = append(failoverReasons, "exceeded max pending workers with high scheduling latency")
	}

	if (nextState.ReadyMachines < p.workerConfig.Failover.MinMachinesAvailable) && p.wpc.Mode() == types.PoolModeExternal {
		status = types.WorkerPoolStatusDegraded
		failoverReasons = append(failoverReasons, "not enough ready machines")
	}

	nextState.Status = status

	// Retrieve the previous state to compare against
	previousState, err := p.wpc.State()
	if err != nil {
		var notFoundErr *types.ErrWorkerPoolStateNotFound
		if errors.As(err, &notFoundErr) {
			previousState = &types.WorkerPoolState{
				Status: types.WorkerPoolStatusHealthy,
			}
		} else {
			return err
		}
	}

	if p.workerConfig.Failover.Enabled {
		// If failover is enabled and status is degraded, we need to cordon all workers in the pool
		if previousState.Status != status && nextState.Status == types.WorkerPoolStatusDegraded {
			p.eventRepo.PushWorkerPoolDegradedEvent(p.wpc.Name(), failoverReasons, nextState)

			log.Warn().Str("pool_name", p.wpc.Name()).Msg("pool is degraded, cordoning all workers")

			err = p.workerRepo.CordonAllPendingWorkersInPool(p.wpc.Name())
			if err != nil {
				log.Error().Str("pool_name", p.wpc.Name()).Err(err).Msg("failed to cordon all workers in pool")
				return err
			}
		} else if previousState.Status != status && nextState.Status == types.WorkerPoolStatusHealthy {
			p.eventRepo.PushWorkerPoolHealthyEvent(p.wpc.Name(), nextState)
		}
	}

	return nil
}
