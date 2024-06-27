package scheduler

import (
	"log"
	"math"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

const poolMonitoringInterval = 1 * time.Second

type WorkerPoolSizer struct {
	controller             WorkerPoolController
	workerRepo             repository.WorkerRepository
	workerPoolRepo         repository.WorkerPoolRepository
	providerRepo           repository.ProviderRepository
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
	ticker := time.NewTicker(poolMonitoringInterval)
	defer ticker.Stop()

	for range ticker.C {
		func() {
			err := s.workerPoolRepo.SetPoolLock(s.controller.Name())
			if err != nil {
				return
			}
			defer s.workerPoolRepo.RemovePoolLock(s.controller.Name())

			freeCapacity, err := s.controller.FreeCapacity()
			if err != nil {
				log.Printf("<pool %s> Error getting free capacity: %v\n", s.controller.Name(), err)
				return
			}

			// Handle case where pool sizing says we want to keep a buffer
			newWorker, err := s.addWorkerIfNeeded(freeCapacity)
			if err != nil {
				log.Printf("<pool %s> Error adding new worker: %v\n", s.controller.Name(), err)
			} else if newWorker != nil {
				log.Printf("<pool %s> Added new worker to maintain pool size: %+v\n", s.controller.Name(), newWorker)
			}

			// Handle case where we want to make sure all available manually provisioned nodes have available workers
			if s.workerPoolConfig.Mode == types.PoolModeExternal {
				err := s.occupyAvailableMachines()
				if err != nil {
					log.Printf("<pool %s> Failed to list machines in external pool: %+v\n", s.controller.Name(), err)
				}
			}
		}()
	}
}

// occupyAvailableMachines ensures that all manually provisioned machines always have workers occupying them
func (s *WorkerPoolSizer) occupyAvailableMachines() error {
	log.Println("calling occupy machines")
	machines, err := s.providerRepo.ListAllMachines(string(*s.workerPoolConfig.Provider), s.controller.Name())
	if err != nil {
		return err
	}

	for _, m := range machines {
		if m.State.AutoConsolidate {
			continue
		}

		workers, err := s.workerRepo.GetAllWorkersOnMachine(m.State.MachineId)
		if err != nil {
			continue
		}

		remainingMachineCpu := m.State.Cpu
		remainingMachineMemory := m.State.Memory
		remainingMachineGpuCount := m.State.GpuCount
		for _, worker := range workers {
			remainingMachineCpu -= worker.TotalCpu
			remainingMachineMemory -= worker.TotalMemory
			remainingMachineGpuCount -= uint32(worker.TotalGpuCount)
		}

		if remainingMachineGpuCount == 0 {
			continue
		}

		cpu := s.workerPoolSizingConfig.DefaultWorkerCpu
		memory := s.workerPoolSizingConfig.DefaultWorkerMemory
		gpuType := s.workerPoolSizingConfig.DefaultWorkerGpuType
		gpuCount := s.workerPoolSizingConfig.DefaultWorkerGpuCount

		cpu = int64(math.Min(float64(cpu), float64(remainingMachineCpu)))
		memory = int64(math.Min(float64(memory), float64(remainingMachineMemory)))

		// If there is only one GPU available on the machine, give the worker access to everything
		// This prevents situations where a user requests a small amount of compute, and the subsequent
		// request has higher compute requirements
		if m.State.GpuCount == 1 {
			cpu = m.State.Cpu
			memory = m.State.Memory
		}

		worker, err := s.controller.AddWorkerOnMachine(cpu, memory, gpuType, gpuCount, m.State.MachineId)
		if err != nil {
			log.Printf("<pool %s> Error adding new worker to machine: %v\n", s.controller.Name(), err)
			continue
		}

		log.Printf("<pool %s> Added new worker to occupy existing machine: %+v\n", s.controller.Name(), worker)
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
			s.workerPoolSizingConfig.DefaultWorkerGpuType,
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

	if defaultWorkerGpuType, err := ParseGPUType(config.DefaultWorkerGpuType); err == nil {
		c.DefaultWorkerGpuType = defaultWorkerGpuType.String()
	}

	if defaultWorkerGpuCount, err := ParseGpuCount(config.DefaultWorkerGpuCount); err == nil {
		c.DefaultWorkerGpuCount = uint32(defaultWorkerGpuCount)
	}

	return c, nil
}
