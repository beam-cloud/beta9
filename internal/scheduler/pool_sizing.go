package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beta9/internal/types"
)

const poolMonitoringInterval = 1 * time.Second

type WorkerPoolSizer struct {
	ctx        context.Context
	controller WorkerPoolController
	config     *types.WorkerPoolSizingConfig
}

func NewWorkerPoolSizer(controller WorkerPoolController, poolSizingConfig *types.WorkerPoolSizingConfig) (*WorkerPoolSizer, error) {
	return &WorkerPoolSizer{
		controller: controller,
		config:     poolSizingConfig,
	}, nil
}

func (s *WorkerPoolSizer) Start() {
	ticker := time.NewTicker(poolMonitoringInterval)
	defer ticker.Stop()

	for range ticker.C {
		freeCapacity, err := s.controller.FreeCapacity()
		if err != nil {
			log.Printf("<pool %s> Error getting free capacity: %v\n", s.controller.Name(), err)
			continue
		}

		s.addWorkerIfNeeded(freeCapacity)
	}
}

func (s *WorkerPoolSizer) addWorkerIfNeeded(freeCapacity *WorkerPoolCapacity) (*types.Worker, error) {
	var err error = nil
	var newWorker *types.Worker = nil

	// Check if the free capacity is below the configured minimum and add a worker if needed
	if freeCapacity.FreeCpu < s.config.MinFreeCpu || freeCapacity.FreeMemory < s.config.MinFreeMemory || (s.config.MinFreeGpu > 0 && freeCapacity.FreeGpu < s.config.MinFreeGpu) {
		newWorker, err = s.controller.AddWorker(s.config.DefaultWorkerCpu, s.config.DefaultWorkerMemory, s.config.DefaultWorkerGpuType)
		if err != nil {
			log.Printf("<pool %s> Error adding new worker: %v\n", s.controller.Name(), err)
			return nil, err
		}

		log.Printf("<pool %s> Added new worker to maintain pool size: %+v\n", s.controller.Name(), newWorker)
	}

	return newWorker, nil
}

// ParsePoolSizingConfig converts a common.WorkerPoolJobSpecPoolSizingConfig to a types.WorkerPoolSizingConfig.
// When a value is not parsable or is invalid, we ignore the error and set a default.
func ParsePoolSizingConfig(config types.WorkerPoolJobSpecPoolSizingConfig) (*types.WorkerPoolSizingConfig, error) {
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

	if defaultWorkerGpuType, err := ParseGPUType(config.DefaultWorkerGPUType); err == nil {
		c.DefaultWorkerGpuType = defaultWorkerGpuType.String()
	}

	return c, nil
}
