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
// sizes any pool that can add workers. A pool that buys its machines plans
// them itself, for its floor and for the requests that wait for it.
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

// hasFloor reports whether the sizing keeps any capacity free.
func hasFloor(sizing *types.WorkerPoolSizingConfig) bool {
	return sizing.MinFreeCpu > 0 || sizing.MinFreeMemory > 0 || sizing.MinFreeGpu > 0
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
	if pool := buyer(s.controller); pool != nil {
		pool.plan(s.sizing)
		return
	}
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

	readSetting(&ignored, "minFreeCpu", written.MinFreeCPU, "2000m", &sizing.MinFreeCpu, ParseCPU)
	readSetting(&ignored, "minFreeMemory", written.MinFreeMemory, "4Gi", &sizing.MinFreeMemory, ParseMemory)
	readSetting(&ignored, "minFreeGpu", written.MinFreeGPU, "1", &sizing.MinFreeGpu, ParseGPU)
	readSetting(&ignored, "defaultWorkerCpu", written.DefaultWorkerCPU, "2000m", &sizing.DefaultWorkerCpu, ParseCPU)
	readSetting(&ignored, "defaultWorkerMemory", written.DefaultWorkerMemory, "4Gi", &sizing.DefaultWorkerMemory, ParseMemory)
	readSetting(&ignored, "defaultWorkerGpuType", written.DefaultWorkerGpuType, "A10G", &sizing.DefaultWorkerGpuType, func(value any) (string, error) {
		gpu, err := ParseGPUType(value)
		return gpu.String(), err
	})
	readSetting(&ignored, "defaultWorkerGpuCount", written.DefaultWorkerGpuCount, "1", &sizing.DefaultWorkerGpuCount, func(value any) (uint32, error) {
		count, err := ParseGpuCount(value)
		return uint32(count), err
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

// readSetting parses one poolSizing setting into field. One that is left out
// leaves the field at its default. So does one that cannot be parsed, which
// is added to ignored.
func readSetting[T any](ignored *[]string, key, value, example string, field *T, parse func(any) (T, error)) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	parsed, err := parse(value)
	if err != nil {
		*ignored = append(*ignored, fmt.Sprintf("%s %q: write it like %s", key, value, example))
		return
	}
	*field = parsed
}

// applyBuildPoolSizingMinimums makes the workers the image build pool adds
// big enough for a build.
func applyBuildPoolSizingMinimums(poolName string, config types.AppConfig, sizing *types.WorkerPoolSizingConfig) {
	if sizing == nil || poolName == "" || poolName != config.ImageService.BuildContainerPoolSelector {
		return
	}
	buildMemory := capacityMemoryForScheduling(&types.ContainerRequest{Memory: config.ImageService.BuildContainerMemory})
	sizing.DefaultWorkerCpu = max(sizing.DefaultWorkerCpu, config.ImageService.BuildContainerCpu)
	sizing.DefaultWorkerMemory = max(sizing.DefaultWorkerMemory, buildMemory)
}
