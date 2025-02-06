package scheduler

import (
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type WorkerPoolSizer struct {
	controller             WorkerPoolController
	workerRepo             repository.WorkerRepository
	providerRepo           repository.ProviderRepository
	workerPoolRepo         repository.WorkerPoolRepository
	workerPoolConfig       *types.WorkerPoolConfig
	workerPoolSizingConfig *types.WorkerPoolSizingConfig
}

func NewWorkerPoolSizer(controller WorkerPoolController,
	workerPoolConfig *types.WorkerPoolConfig,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerRepo repository.ProviderRepository) (*WorkerPoolSizer, error) {
	poolSizingConfig, err := parsePoolSizingConfig(workerPoolConfig.PoolSizing)
	if err != nil {
		return nil, err
	}

	return &WorkerPoolSizer{
		controller:             controller,
		workerPoolConfig:       workerPoolConfig,
		workerPoolSizingConfig: poolSizingConfig,
		workerRepo:             workerRepo,
		workerPoolRepo:         workerPoolRepo,
		providerRepo:           providerRepo,
	}, nil
}

func (s *WorkerPoolSizer) Start() {
	ctx := s.controller.Context()
	ticker := time.NewTicker(poolMonitoringInterval)
	defer ticker.Stop()

	sampledLogger := log.Sample(zerolog.LevelSampler{
		WarnSampler: &zerolog.BurstSampler{
			Burst:       1,
			Period:      10 * time.Second,
			NextSampler: nil,
		},
	})

	previousState := &types.WorkerPoolState{}
	for range ticker.C {
		select {
		case <-ctx.Done():
			return // Context has been cancelled
		default: // Continue processing requests
		}

		func() {
			nextState, err := s.controller.State() // Get the current state of the pool
			if err != nil {
				return
			}

			if previousState.Status != nextState.Status && nextState.Status == types.WorkerPoolStatusDegraded {
				sampledLogger.Warn().Str("pool_name", s.controller.Name()).Msg("pool is degraded, skipping pool sizing")
			} else if previousState.Status != nextState.Status && nextState.Status == types.WorkerPoolStatusHealthy {
				sampledLogger.Info().Str("pool_name", s.controller.Name()).Msg("pool is healthy, resuming pool sizing")
			}

			previousState = nextState

			// If the pool is degraded, we don't want to keep adding more workers
			if nextState.Status == types.WorkerPoolStatusDegraded {
				return
			}

			// Get the current free capacity of the pool
			freeCapacity, err := s.controller.FreeCapacity()
			if err != nil {
				log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to get free capacity")
				return
			}

			// Handle case where pool sizing says we want to keep a buffer
			newWorker, err := s.addWorkerIfNeeded(freeCapacity)
			if err != nil {
				log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to add worker")
			} else if newWorker != nil {
				log.Info().Str("pool_name", s.controller.Name()).Interface("worker", newWorker).Msg("added new worker to maintain pool size")
			}

			// Handle case where we want to make sure all available manually provisioned nodes have available workers
			if s.workerPoolConfig.Mode == types.PoolModeExternal {
				err := s.occupyAvailableMachines()
				if err != nil && !errors.Is(err, redislock.ErrNotObtained) {
					log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to list machines in external pool")
				}
			}
		}()
	}
}

// occupyAvailableMachines ensures that all manually provisioned machines always have workers occupying them
// This only adds one worker per machine, so if a machine has more capacity, it will not be fully utilized unless
// this is called multiple times.
func (s *WorkerPoolSizer) occupyAvailableMachines() error {
	if err := s.workerPoolRepo.SetWorkerPoolSizerLock(s.controller.Name()); err != nil {
		return err
	}
	defer s.workerPoolRepo.RemoveWorkerPoolSizerLock(s.controller.Name())

	machines, err := s.providerRepo.ListAllMachines(string(*s.workerPoolConfig.Provider), s.controller.Name(), true)
	if err != nil {
		return err
	}

	for _, m := range machines {
		if m.State.AutoConsolidate {
			continue
		}

		cpu := s.workerPoolSizingConfig.DefaultWorkerCpu
		memory := s.workerPoolSizingConfig.DefaultWorkerMemory
		gpuType := s.workerPoolSizingConfig.DefaultWorkerGpuType
		gpuCount := s.workerPoolSizingConfig.DefaultWorkerGpuCount

		worker, err := s.controller.AddWorkerToMachine(cpu, memory, gpuType, gpuCount, m.State.MachineId)
		if err != nil {
			continue
		}

		log.Info().Str("pool_name", s.controller.Name()).Interface("worker", worker).Msg("added new worker to occupy existing machine")
	}

	return nil
}

func (s *WorkerPoolSizer) addWorkerIfNeeded(freeCapacity *WorkerPoolCapacity) (*types.Worker, error) {
	var err error = nil
	var newWorker *types.Worker = nil

	// Check if the free capacity is below the configured minimum and add a worker if needed
	if shouldAddWorker(freeCapacity, s.workerPoolSizingConfig) {
		newWorker, err = s.controller.AddWorker(
			s.workerPoolSizingConfig.DefaultWorkerCpu,
			s.workerPoolSizingConfig.DefaultWorkerMemory,
			s.workerPoolSizingConfig.DefaultWorkerGpuCount,
		)
		if err != nil {
			return nil, err
		}

	}

	return newWorker, nil
}

// shouldAddWorker checks if the conditions are met for a new worker to be added
func shouldAddWorker(freeCapacity *WorkerPoolCapacity, config *types.WorkerPoolSizingConfig) bool {
	return freeCapacity.FreeCpu < config.MinFreeCpu ||
		freeCapacity.FreeMemory < config.MinFreeMemory ||
		(config.MinFreeGpu > 0 && freeCapacity.FreeGpu < config.MinFreeGpu)
}

// parsePoolSizingConfig converts a common.WorkerPoolJobSpecPoolSizingConfig to a types.WorkerPoolSizingConfig.
// When a value is not parsable or is invalid, we ignore the error and set a default.
func parsePoolSizingConfig(config types.WorkerPoolJobSpecPoolSizingConfig) (*types.WorkerPoolSizingConfig, error) {
	c := types.NewWorkerPoolSizingConfig()

	if minFreeCpu, err := ParseCPU(config.MinFreeCPU); err == nil {
		c.MinFreeCpu = minFreeCpu
	}

	if minFreeMemory, err := ParseMemory(config.MinFreeMemory); err == nil {
		c.MinFreeMemory = minFreeMemory
	}

	if minFreeGpu, err := ParseGPU(config.MinFreeGPU); err == nil {
		c.MinFreeGpu = minFreeGpu
	}

	if defaultWorkerCpu, err := ParseCPU(config.DefaultWorkerCPU); err == nil {
		c.DefaultWorkerCpu = defaultWorkerCpu
	}

	if defaultWorkerMemory, err := ParseMemory(config.DefaultWorkerMemory); err == nil {
		c.DefaultWorkerMemory = defaultWorkerMemory
	}

	defaultWorkerGpuType, err := ParseGPUType(config.DefaultWorkerGpuType)
	if err == nil {
		c.DefaultWorkerGpuType = defaultWorkerGpuType.String()
	}

	if defaultWorkerGpuCount, err := ParseGpuCount(config.DefaultWorkerGpuCount); err == nil {
		c.DefaultWorkerGpuCount = uint32(defaultWorkerGpuCount)
	}

	// Don't allow creation of workers with no gpu count if there is a GPU type set
	if c.DefaultWorkerGpuCount <= 0 && defaultWorkerGpuType != "" {
		c.DefaultWorkerGpuCount = 1
	}

	return c, nil
}
