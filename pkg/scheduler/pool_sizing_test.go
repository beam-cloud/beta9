package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestAddWorkerIfNeeded(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepositoryForTest(redisClient)
	config := &types.WorkerPoolSizingConfig{
		MinFreeCpu:           10000,
		MinFreeMemory:        5000,
		MinFreeGpu:           0,
		DefaultWorkerCpu:     2000,
		DefaultWorkerMemory:  500,
		DefaultWorkerGpuType: "",
	}

	configWithGpu := &types.WorkerPoolSizingConfig{
		MinFreeCpu:           10000,
		MinFreeMemory:        5000,
		MinFreeGpu:           2,
		DefaultWorkerCpu:     2000,
		DefaultWorkerMemory:  500,
		DefaultWorkerGpuType: "A10G",
	}

	tests := []struct {
		name            string
		config          *types.WorkerPoolSizingConfig
		freeCapacity    *WorkerPoolCapacity
		shouldAddWorker bool
	}{
		{
			name:   "should not add worker when capacity is sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    10000,
				FreeMemory: 5000,
				FreeGpu:    0,
			},
			shouldAddWorker: false,
		},
		{
			name:   "should not add worker when pending capacity is sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				PendingCpu:    10000,
				PendingMemory: 5000,
				PendingGpu:    0,
			},
			shouldAddWorker: false,
		},
		{
			name:   "should add worker when capacity is not sufficient",
			config: config,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    5000,
				FreeMemory: 5000,
				FreeGpu:    0,
			},
			shouldAddWorker: true,
		},
		{
			name:   "should add worker (w/ GPU) when free CPU is not sufficient",
			config: configWithGpu,
			freeCapacity: &WorkerPoolCapacity{
				FreeCpu:    5000,
				FreeMemory: 1000,
				FreeGpu:    1,
			},
			shouldAddWorker: true,
		},
		{
			name:   "should add worker (w/ GPU) when free + pending CPU is not sufficient",
			config: configWithGpu,
			freeCapacity: &WorkerPoolCapacity{
				PendingCpu:    2500,
				PendingMemory: 500,
				PendingGpu:    1,
				FreeCpu:       2500,
				FreeMemory:    500,
				FreeGpu:       1,
			},
			shouldAddWorker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &LocalWorkerPoolControllerForTest{
				name:       "TestPool",
				workerRepo: workerRepo,
			}
			sizer := &WorkerPoolSizer{
				controller:             controller,
				workerPoolSizingConfig: tt.config,
				workerPoolRepo:         workerPoolRepo,
			}

			newWorker, err := sizer.addWorkerIfNeeded(tt.freeCapacity)
			assert.NoError(t, err)

			if tt.shouldAddWorker {
				assert.NotNil(t, newWorker, "New worker should be added")
			} else {
				assert.Nil(t, newWorker, "New worker should not be added")
			}
		})
	}
}

func TestParsePoolSizingConfig(t *testing.T) {
	tests := []struct {
		name             string
		sizingConfigHave *types.WorkerPoolJobSpecPoolSizingConfig
		sizingConfigWant *types.WorkerPoolSizingConfig
	}{
		{
			name:             "should set defaults when no values provided",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:           0,
				MinFreeMemory:        0,
				MinFreeGpu:           0,
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name: "should ignore bad values and use default values",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU:           "bad value",
				MinFreeMemory:        "bad value",
				MinFreeGPU:           "bad value",
				DefaultWorkerCPU:     "bad value",
				DefaultWorkerMemory:  "bad value",
				DefaultWorkerGpuType: "bad value",
			},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:           0,
				MinFreeMemory:        0,
				MinFreeGpu:           0,
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name: "should parse minFreeCpu minFreeMemory and minFreeGpu",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{
				MinFreeCPU:    "132000m",
				MinFreeMemory: "100Gi",
				MinFreeGPU:    "0",
			},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				MinFreeCpu:          132000,
				MinFreeMemory:       102400,
				MinFreeGpu:          0,
				DefaultWorkerCpu:    1000,
				DefaultWorkerMemory: 1024,
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as T4",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "T4"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:      1000,
				DefaultWorkerMemory:   1024,
				DefaultWorkerGpuType:  "T4",
				DefaultWorkerGpuCount: 1,
			},
		},
		{
			name:             "should parse defaultWorkerGpuType as A100-40",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "A100-40"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:      1000,
				DefaultWorkerMemory:   1024,
				DefaultWorkerGpuType:  "A100-40",
				DefaultWorkerGpuCount: 1,
			},
		},
		{
			name:             "should return empty DefaultWorkerGpuType when using 3060 (str) as defaultWorkerGpuType",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "3060"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
		{
			name:             "should set empty DefaultWorkerGpuType when using 4090 (int) as defaultWorkerGpuType",
			sizingConfigHave: &types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerGpuType: "4090"},
			sizingConfigWant: &types.WorkerPoolSizingConfig{
				DefaultWorkerCpu:     1000,
				DefaultWorkerMemory:  1024,
				DefaultWorkerGpuType: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parsePoolSizingConfig(*tt.sizingConfigHave)
			assert.NoError(t, err)

			assert.Equal(t, tt.sizingConfigWant, config)
		})
	}
}

func TestOccupyAvailableMachines(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.Nil(t, err)

	providerRepo := repo.NewProviderRedisRepositoryForTest(redisClient)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepositoryForTest(redisClient)

	lambdaPoolName := "LambdaLabsPool"
	controller := &ExternalWorkerPoolControllerForTest{
		ctx:            context.Background(),
		name:           lambdaPoolName,
		workerRepo:     workerRepo,
		providerRepo:   providerRepo,
		poolName:       lambdaPoolName,
		providerName:   string(types.ProviderLambdaLabs),
		workerPoolRepo: workerPoolRepo,
	}

	sizer := &WorkerPoolSizer{
		workerRepo:     workerRepo,
		providerRepo:   providerRepo,
		workerPoolRepo: workerPoolRepo,
		controller:     controller,
		workerPoolConfig: &types.WorkerPoolConfig{
			Provider: &types.ProviderLambdaLabs,
			GPUType:  "A10G",
			Mode:     types.PoolModeExternal,
		},
		workerPoolSizingConfig: &types.WorkerPoolSizingConfig{
			DefaultWorkerCpu:      8000,
			DefaultWorkerMemory:   16000,
			DefaultWorkerGpuType:  "A10G",
			DefaultWorkerGpuCount: 1,
		},
	}

	// Add some "manually" provisioned machines
	err = providerRepo.AddMachine(string(types.ProviderLambdaLabs), lambdaPoolName, "machine1", &types.ProviderMachineState{
		Gpu:             "A10G",
		GpuCount:        1,
		AutoConsolidate: false,
		Cpu:             30000,
		Memory:          16000,
		Status:          types.MachineStatusRegistered,
	})
	assert.NoError(t, err)

	poolConfig := &types.WorkerPoolConfig{
		Provider: &types.ProviderLambdaLabs,
		GPUType:  "A10G",
		Mode:     types.PoolModeExternal,
	}

	err = providerRepo.RegisterMachine(string(types.ProviderLambdaLabs), lambdaPoolName, "machine1", &types.ProviderMachineState{
		Gpu:             "A10G",
		GpuCount:        1,
		AutoConsolidate: false,
		Cpu:             30000,
		Memory:          16000,
		Status:          types.MachineStatusRegistered,
	}, poolConfig)
	assert.NoError(t, err)

	err = providerRepo.AddMachine(string(types.ProviderLambdaLabs), lambdaPoolName, "machine2", &types.ProviderMachineState{
		Gpu:             "A10G",
		GpuCount:        1,
		AutoConsolidate: false,
		Cpu:             30000,
		Memory:          10000,
		Status:          types.MachineStatusRegistered,
	})
	assert.NoError(t, err)

	err = providerRepo.RegisterMachine(string(types.ProviderLambdaLabs), lambdaPoolName, "machine2", &types.ProviderMachineState{
		Gpu:             "A10G",
		GpuCount:        1,
		AutoConsolidate: false,
		Cpu:             30000,
		Memory:          16000,
		Status:          types.MachineStatusRegistered,
	}, poolConfig)
	assert.NoError(t, err)
	assert.NoError(t, err)

	tests := []struct {
		name                           string
		expectedError                  error
		expectedWorkersOnMachineBefore int
		expectedWorkersOnMachineAfter  int
		machineId                      string
	}{

		{
			name:                           "successfully add workers to machine1 and machine2",
			expectedError:                  nil,
			expectedWorkersOnMachineBefore: 0,
			expectedWorkersOnMachineAfter:  1,
			machineId:                      "machine1",
		},
		{
			name:                           "did not add additional workers to machine2",
			expectedError:                  nil,
			expectedWorkersOnMachineBefore: 1,
			expectedWorkersOnMachineAfter:  1,
			machineId:                      "machine2",
		},
	}

	machines, err := providerRepo.ListAllMachines(string(types.ProviderLambdaLabs), lambdaPoolName, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(machines))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = sizer.occupyAvailableMachines()
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}

			workers, err := workerRepo.GetAllWorkersOnMachine(tt.machineId)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedWorkersOnMachineAfter, len(workers))
		})
	}

	workers, err := workerRepo.GetAllWorkersInPool(lambdaPoolName)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(workers))
}

func TestOccupyAvailableMachinesConcurrency(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.Nil(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.Nil(t, err)

	providerRepo := repo.NewProviderRedisRepositoryForTest(redisClient)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepositoryForTest(redisClient)

	ctx, cancel := context.WithCancel(context.Background())

	poolName := "pool1"
	controller := &ExternalWorkerPoolControllerForTest{
		ctx:            ctx,
		name:           poolName,
		workerRepo:     workerRepo,
		providerRepo:   providerRepo,
		workerPoolRepo: workerPoolRepo,
		poolName:       poolName,
		providerName:   string(types.ProviderGeneric),
	}

	sizer1 := &WorkerPoolSizer{
		providerRepo:   providerRepo,
		workerRepo:     workerRepo,
		workerPoolRepo: workerPoolRepo,
		controller:     controller,
		workerPoolConfig: &types.WorkerPoolConfig{
			Provider: &types.ProviderGeneric,
			GPUType:  "A10G",
			Mode:     types.PoolModeExternal,
		},
		workerPoolSizingConfig: &types.WorkerPoolSizingConfig{
			DefaultWorkerCpu:      1000,
			DefaultWorkerMemory:   1000,
			DefaultWorkerGpuType:  "A10G",
			DefaultWorkerGpuCount: 1,
		},
	}

	sizer2 := &WorkerPoolSizer{
		providerRepo:   providerRepo,
		workerRepo:     workerRepo,
		workerPoolRepo: workerPoolRepo,
		controller:     controller,
		workerPoolConfig: &types.WorkerPoolConfig{
			Provider: &types.ProviderGeneric,
			GPUType:  "A10G",
			Mode:     types.PoolModeExternal,
		},
		workerPoolSizingConfig: &types.WorkerPoolSizingConfig{
			DefaultWorkerCpu:      1000,
			DefaultWorkerMemory:   1000,
			DefaultWorkerGpuType:  "A10G",
			DefaultWorkerGpuCount: 1,
		},
	}

	poolConfig := &types.WorkerPoolConfig{
		Provider: &types.ProviderGeneric,
		GPUType:  "A10G",
		Mode:     types.PoolModeExternal,
	}

	maxMachinesAndWorkers := 100
	for i := 0; i < maxMachinesAndWorkers; i++ {
		machineName := fmt.Sprintf("machine-%d", i)
		machineState := &types.ProviderMachineState{
			Gpu:             "A10G",
			GpuCount:        2,
			AutoConsolidate: false,
			Cpu:             10000,
			Memory:          10000,
			Status:          types.MachineStatusRegistered,
		}
		err = providerRepo.AddMachine(string(types.ProviderGeneric), poolName, machineName, machineState)
		assert.NoError(t, err)

		err = providerRepo.RegisterMachine(string(types.ProviderGeneric), poolName, machineName, machineState, poolConfig)
		assert.NoError(t, err)
	}

	var g errgroup.Group

	g.Go(func() error {
		return sizer1.occupyAvailableMachines()
	})
	g.Go(func() error {
		return sizer2.occupyAvailableMachines()
	})

	// One of the sizers should fail to occupy the machines because of a lock
	err = g.Wait()
	assert.Error(t, err)

	// Check that only 100 workers were added, not 102, 105, etc.
	// This is because occupyAvailableMachines adds just one worker per machine (if there's capacity and no lock)
	// each itme it is called.
	workers, err := workerRepo.GetAllWorkers()
	assert.NoError(t, err)
	assert.Equal(t, maxMachinesAndWorkers, len(workers))

	cancel()
}
