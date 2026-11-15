package scheduler

import (
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog/log"
)

// WorkerPoolSizer keeps a pool's free capacity at its floor
// (poolSizing.minFree*). It works through the pool interface alone, so it
// sizes any pool that can add workers.
type WorkerPoolSizer struct {
	controller     WorkerPoolController
	workerPoolRepo repository.WorkerPoolRepository
	sizing         *types.WorkerPoolSizingConfig
}

// NewWorkerPoolSizer reads the floor and the default worker size from the
// pool's own config, and says once what it could not use.
func NewWorkerPoolSizer(controller WorkerPoolController,
	config types.AppConfig,
	workerPoolConfig *types.WorkerPoolConfig,
	workerPoolRepo repository.WorkerPoolRepository) *WorkerPoolSizer {
	sizing, ignored := readPoolSizing(*workerPoolConfig)
	for _, setting := range ignored {
		log.Warn().Str("pool_name", controller.Name()).Msg("ignoring poolSizing." + setting)
	}
	applyBuildPoolSizingMinimums(controller.Name(), config, sizing)

	return &WorkerPoolSizer{
		controller:     controller,
		sizing:         sizing,
		workerPoolRepo: workerPoolRepo,
	}
}

// hasFloor reports whether the pool keeps any capacity free.
func (s *WorkerPoolSizer) hasFloor() bool {
	return s.sizing.MinFreeCpu > 0 || s.sizing.MinFreeMemory > 0 || s.sizing.MinFreeGpu > 0
}

// tick adds one worker when the pool's free capacity, counting workers that
// are still starting, is under its floor. Only one scheduler replica sizes a
// pool at a time.
func (s *WorkerPoolSizer) tick() {
	err := s.workerPoolRepo.WithWorkerPoolSizerLock(s.controller.Name(), s.size)
	if err != nil && !errors.Is(err, redislock.ErrNotObtained) {
		log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to hold worker pool sizing lock")
	}
}

func (s *WorkerPoolSizer) size() {
	freeCapacity, err := s.controller.FreeCapacity()
	if err != nil {
		log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to get free capacity")
		return
	}

	newWorker, err := s.addWorkerIfNeeded(freeCapacity)
	if err != nil {
		log.Error().Str("pool_name", s.controller.Name()).Err(err).Msg("failed to add worker")
	} else if newWorker != nil {
		workerLog(log.Info(), newWorker).Msg("added new worker to maintain pool size")
	}
}

// addWorkerIfNeeded adds a worker of the pool's default size when the pool is
// under its floor and can add one. A pool that cannot (out of stock, at its
// limit, stalled) holds what it has; the next tick asks again.
func (s *WorkerPoolSizer) addWorkerIfNeeded(freeCapacity *WorkerPoolCapacity) (*types.Worker, error) {
	if !freeCapacity.withPending().belowMinimum(s.sizing) {
		return nil, nil
	}

	cpu, memory, gpuCount := s.sizing.DefaultWorkerCpu, s.sizing.DefaultWorkerMemory, s.sizing.DefaultWorkerGpuCount
	canAdd, err := s.controller.CanAddWorker(cpu, memory, gpuCount)
	if err != nil || !canAdd {
		return nil, err
	}
	return s.controller.AddWorker(cpu, memory, gpuCount)
}

// readPoolSizing reads a pool's floor (poolSizing.minFree*) and the size of
// the worker it adds to keep it. A setting that is left out takes its
// default: no floor, and a worker of one CPU and 1Gi of memory with no GPU.
// So does one that cannot be used, which is named in ignored.
func readPoolSizing(pool types.WorkerPoolConfig) (sizing *types.WorkerPoolSizingConfig, ignored []string) {
	sizing = types.NewWorkerPoolSizingConfig()
	written := pool.PoolSizing
	read := func(key, value, example string, parse func(string) error) {
		if value = strings.TrimSpace(value); value != "" && parse(value) != nil {
			ignored = append(ignored, fmt.Sprintf("%s %q: write it like %s", key, value, example))
		}
	}

	read("minFreeCpu", written.MinFreeCPU, "2000m", func(value string) (err error) {
		sizing.MinFreeCpu, err = ParseCPU(value)
		return err
	})
	read("minFreeMemory", written.MinFreeMemory, "4Gi", func(value string) (err error) {
		sizing.MinFreeMemory, err = ParseMemory(value)
		return err
	})
	read("minFreeGpu", written.MinFreeGPU, "1", func(value string) (err error) {
		sizing.MinFreeGpu, err = ParseGPU(value)
		return err
	})
	read("defaultWorkerCpu", written.DefaultWorkerCPU, "2000m", func(value string) error {
		cpu, err := ParseCPU(value)
		if err == nil {
			sizing.DefaultWorkerCpu = cpu
		}
		return err
	})
	read("defaultWorkerMemory", written.DefaultWorkerMemory, "4Gi", func(value string) error {
		memory, err := ParseMemory(value)
		if err == nil {
			sizing.DefaultWorkerMemory = memory
		}
		return err
	})
	read("defaultWorkerGpuType", written.DefaultWorkerGpuType, "A10G", func(value string) error {
		gpu, err := ParseGPUType(value)
		sizing.DefaultWorkerGpuType = gpu.String()
		return err
	})
	read("defaultWorkerGpuCount", written.DefaultWorkerGpuCount, "1", func(value string) error {
		count, err := ParseGpuCount(value)
		sizing.DefaultWorkerGpuCount = uint32(count)
		return err
	})

	// A pool gives the workers it adds its own gpuType. One with no gpuType
	// adds none with a GPU, and would add workers for ever to keep one free.
	if sizing.MinFreeGpu > 0 && pool.GPUType == "" && (pool.Buys() || !pool.AgentHosted()) {
		sizing.MinFreeGpu = 0
		ignored = append(ignored, fmt.Sprintf("minFreeGpu %q: the pool has no gpuType, so its workers have no GPU", strings.TrimSpace(written.MinFreeGPU)))
	}
	// A worker of a GPU type has a GPU, and so has one added to keep GPUs
	// free.
	if sizing.DefaultWorkerGpuCount == 0 && (sizing.DefaultWorkerGpuType != "" || sizing.MinFreeGpu > 0) {
		sizing.DefaultWorkerGpuCount = 1
	}
	return sizing, ignored
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
