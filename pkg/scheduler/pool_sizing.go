package scheduler

import (
	"encoding/json"
	"log"
	"time"

	"github.com/beam-cloud/beam/pkg/types"
)

const poolMonitoringInterval = 1 * time.Second

type WorkerPoolSizer struct {
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

// Parses a JSON string represented as []byte.
// When a value is not parsable or is invalid, we ignore the error and set a default.
func ParsePoolSizingConfig(raw []byte) (*types.WorkerPoolSizingConfig, error) {
	var configMap map[string]interface{}
	err := json.Unmarshal(raw, &configMap)
	if err != nil {
		return nil, err
	}

	c := types.NewWorkerPoolSizingConfig()

	if val, ok := configMap["minFreeCpu"]; ok {
		if minFreeCpu, err := ParseCPU(val); err == nil {
			c.MinFreeCpu = minFreeCpu
		}
	}

	if val, ok := configMap["minFreeMemory"]; ok {
		if minFreeMemory, err := ParseMemory(val); err == nil {
			c.MinFreeMemory = minFreeMemory
		}
	}

	if val, ok := configMap["minFreeGpu"]; ok {
		if minFreeGpu, err := ParseGPU(val); err == nil {
			c.MinFreeGpu = minFreeGpu
		}
	}

	if val, ok := configMap["defaultWorkerCpu"]; ok {
		if defaultWorkerCpu, err := ParseCPU(val); err == nil {
			c.DefaultWorkerCpu = defaultWorkerCpu
		}
	}

	if val, ok := configMap["defaultWorkerMemory"]; ok {
		if defaultWorkerMemory, err := ParseMemory(val); err == nil {
			c.DefaultWorkerMemory = defaultWorkerMemory
		}
	}

	if val, ok := configMap["defaultWorkerGpuType"]; ok {
		if defaultWorkerGpuType, err := ParseGPUType(val); err == nil {
			c.DefaultWorkerGpuType = defaultWorkerGpuType.String()
		}
	}

	return c, nil
}
