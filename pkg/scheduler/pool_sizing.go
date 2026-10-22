package scheduler

import (
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog/log"
)

type WorkerPoolSizer struct {
	controller             WorkerPoolController
	workerPoolRepo         repository.WorkerPoolRepository
	workerPoolSizingConfig *types.WorkerPoolSizingConfig
}

func NewWorkerPoolSizer(controller WorkerPoolController,
	config types.AppConfig,
	workerPoolConfig *types.WorkerPoolConfig,
	workerPoolRepo repository.WorkerPoolRepository) (*WorkerPoolSizer, error) {
	poolSizingConfig, err := parsePoolSizingConfig(workerPoolConfig.PoolSizing)
	if err != nil {
		return nil, err
	}
	applyBuildPoolSizingMinimums(controller.Name(), config, poolSizingConfig)

	return &WorkerPoolSizer{
		controller:             controller,
		workerPoolSizingConfig: poolSizingConfig,
		workerPoolRepo:         workerPoolRepo,
	}, nil
}

func (s *WorkerPoolSizer) Start() {
	ctx := s.controller.Context()
	ticker := time.NewTicker(poolMonitoringInterval)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return // Context has been cancelled
		default: // Continue processing requests
		}

		func() {
			if err := s.workerPoolRepo.SetWorkerPoolSizerLock(s.controller.Name()); err != nil {
				if !errors.Is(err, redislock.ErrNotObtained) {
					log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to acquire worker pool sizing lock")
				}
				return
			}
			defer s.workerPoolRepo.RemoveWorkerPoolSizerLock(s.controller.Name())

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
				workerLog(log.Info(), newWorker).Msg("added new worker to maintain pool size")
			}
		}()
	}
}

func (s *WorkerPoolSizer) addWorkerIfNeeded(freeCapacity *WorkerPoolCapacity) (*types.Worker, error) {
	// Check if the free capacity is below the configured minimum and add a worker if needed
	if !shouldAddWorker(freeCapacity, s.workerPoolSizingConfig) {
		return nil, nil
	}

	return s.controller.AddWorker(
		s.workerPoolSizingConfig.DefaultWorkerCpu,
		s.workerPoolSizingConfig.DefaultWorkerMemory,
		s.workerPoolSizingConfig.DefaultWorkerGpuCount,
	)
}

// shouldAddWorker checks if the conditions are met for a new worker to be added
func shouldAddWorker(freeCapacity *WorkerPoolCapacity, config *types.WorkerPoolSizingConfig) bool {
	freeCpu := freeCapacity.FreeCpu + freeCapacity.PendingCpu
	freeMemory := freeCapacity.FreeMemory + freeCapacity.PendingMemory
	freeGpu := freeCapacity.FreeGpu + freeCapacity.PendingGpu

	return freeCpu < config.MinFreeCpu ||
		freeMemory < config.MinFreeMemory ||
		(config.MinFreeGpu > 0 && freeGpu < config.MinFreeGpu)
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

func applyBuildPoolSizingMinimums(poolName string, config types.AppConfig, sizing *types.WorkerPoolSizingConfig) {
	if sizing == nil || poolName == "" || poolName != config.ImageService.BuildContainerPoolSelector {
		return
	}

	if config.ImageService.BuildContainerCpu > sizing.DefaultWorkerCpu {
		sizing.DefaultWorkerCpu = config.ImageService.BuildContainerCpu
	}

	buildMemory := capacityMemoryForScheduling(&types.ContainerRequest{Memory: config.ImageService.BuildContainerMemory})
	if buildMemory > sizing.DefaultWorkerMemory {
		sizing.DefaultWorkerMemory = buildMemory
	}
}
