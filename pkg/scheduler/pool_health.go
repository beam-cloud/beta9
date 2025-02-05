package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type PoolHealthMonitorOptions struct {
	Controller       WorkerPoolController
	WorkerPoolConfig *types.WorkerPoolConfig
	WorkerConfig     *types.WorkerConfig
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	ProviderRepo     repository.ProviderRepository
	ContainerRepo    repository.ContainerRepository
}

type PoolHealthMonitor struct {
	ctx              context.Context
	wpc              WorkerPoolController
	workerPoolConfig *types.WorkerPoolConfig
	workerConfig     *types.WorkerConfig
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	containerRepo    repository.ContainerRepository
	providerRepo     repository.ProviderRepository
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

				poolState, err := p.getPoolState()
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
			}()
		}
	}
}

// getPoolState measures various metrics about pool health and returns them
func (p *PoolHealthMonitor) getPoolState() (*types.WorkerPoolState, error) {
	schedulingLatencies := []time.Duration{}
	availableWorkers := 0
	pendingWorkers := 0
	pendingContainers := 0
	runningContainers := 0
	registeredMachines := 0
	pendingMachines := 0

	workers, err := p.workerRepo.GetAllWorkersInPool(p.wpc.Name())
	if err != nil {
		return nil, err
	}

	machines, err := p.providerRepo.ListAllMachines(p.wpc.Name(), p.wpc.Name(), false)
	if err != nil {
		return nil, err
	}

	for _, machine := range machines {
		switch machine.State.Status {
		case types.MachineStatusPending:
			pendingMachines++
		case types.MachineStatusRegistered:
			registeredMachines++
		}
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

			if container.StartedAt == 0 || container.Status == types.ContainerStatusPending {
				latency := time.Since(time.Unix(container.ScheduledAt, 0))
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

	freeCapacity, err := p.wpc.FreeCapacity()
	if err != nil {
		return nil, err
	}

	return &types.WorkerPoolState{
		SchedulingLatency:  int64(averageSchedulingLatency.Milliseconds()),
		AvailableWorkers:   int64(availableWorkers),
		PendingWorkers:     int64(pendingWorkers),
		PendingContainers:  int64(pendingContainers),
		RunningContainers:  int64(runningContainers),
		FreeGpu:            freeCapacity.FreeGpu,
		FreeCpu:            freeCapacity.FreeCpu,
		FreeMemory:         freeCapacity.FreeMemory,
		RegisteredMachines: int64(registeredMachines),
		PendingMachines:    int64(pendingMachines),
	}, nil
}

// updatePoolStatus updates the status of the pool based on the current state
func (p *PoolHealthMonitor) updatePoolStatus(nextState *types.WorkerPoolState) error {
	status := types.WorkerPoolStatusHealthy

	// Go through each condition that could trigger a degraded status
	if nextState.PendingWorkers >= p.workerConfig.Failover.MaxPendingWorkers {
		status = types.WorkerPoolStatusDegraded
	}

	if nextState.SchedulingLatency > p.workerConfig.Failover.MaxSchedulingLatencyMs {
		status = types.WorkerPoolStatusDegraded
	}

	if (nextState.RegisteredMachines < p.workerConfig.Failover.MinMachinesAvailable) && p.wpc.Mode() == types.PoolModeExternal {
		status = types.WorkerPoolStatusDegraded
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
			log.Warn().Str("pool_name", p.wpc.Name()).Msg("pool is degraded, cordoning all workers")

			err = p.workerRepo.CordonAllWorkersInPool(p.wpc.Name())
			if err != nil {
				log.Error().Err(err).Msg("failed to cordon all workers in pool")
				return err
			}
		}
	}

	return nil
}
