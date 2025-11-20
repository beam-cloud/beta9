package scheduler

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/tj/assert"
)

func NewSchedulerForTest() (*Scheduler, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		return nil, err
	}

	eventBus := common.NewEventBus(rdb)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	containerRepo := repo.NewContainerRedisRepositoryForTest(rdb)
	workspaceRepo := repo.NewWorkspaceRedisRepositoryForTest(rdb)
	requestBacklog := NewRequestBacklogForTest(rdb)

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}

	poolJson := []byte(`{"worker":{"pools":{"beta9-build":{},"beta9-cpu":{},"beta9-a10g":{"gpuType": "A10G"},"beta9-t4":{"gpuType": "T4"}}}}}`)
	configManager.LoadConfig(common.YAMLConfigFormat, rawbytes.Provider(poolJson))
	config := configManager.GetConfig()
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	schedulerUsageMetrics := SchedulerUsageMetrics{
		UsageRepo: nil,
	}

	workerPoolManager := NewWorkerPoolManager(false)
	for name, pool := range config.Worker.Pools {
		workerPoolManager.SetPool(name, pool, &LocalWorkerPoolControllerForTest{
			ctx:        context.Background(),
			name:       name,
			config:     config,
			workerRepo: workerRepo,
		})
	}

	return &Scheduler{
		ctx:                   context.Background(),
		eventBus:              eventBus,
		workerRepo:            workerRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		schedulerUsageMetrics: schedulerUsageMetrics,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,
	}, nil
}

type LocalWorkerPoolControllerForTest struct {
	ctx         context.Context
	name        string
	config      types.AppConfig
	workerRepo  repo.WorkerRepository
	preemptable bool
}

func (wpc *LocalWorkerPoolControllerForTest) Context() context.Context {
	return wpc.ctx
}

func (wpc *LocalWorkerPoolControllerForTest) IsPreemptable() bool {
	return wpc.preemptable
}

func (wpc *LocalWorkerPoolControllerForTest) State() (*types.WorkerPoolState, error) {
	return &types.WorkerPoolState{}, nil
}

func (wpc *LocalWorkerPoolControllerForTest) Mode() types.PoolMode {
	return types.PoolModeLocal
}

func (wpc *LocalWorkerPoolControllerForTest) RequiresPoolSelector() bool {
	return false
}

func (wpc *LocalWorkerPoolControllerForTest) ContainerRuntime() string {
	return "runc"
}

func (wpc *LocalWorkerPoolControllerForTest) generateWorkerId() string {
	return uuid.New().String()[:8]
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	workerId := wpc.generateWorkerId()
	worker := &types.Worker{
		Id:           workerId,
		FreeCpu:      cpu,
		FreeMemory:   memory,
		FreeGpuCount: gpuCount,
		Status:       types.WorkerStatusPending,
	}

	// Add the worker state
	err := wpc.workerRepo.AddWorker(worker)
	if err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorkerWithDelay(request *types.ContainerRequest, delay time.Duration) (*types.Worker, error) {
	return nil, errors.New("not implemented")
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error) {
	return nil, errors.New("unimplemented")
}

func (wpc *LocalWorkerPoolControllerForTest) Name() string {
	return wpc.name
}

func (wpc *LocalWorkerPoolControllerForTest) FreeCapacity() (*WorkerPoolCapacity, error) {
	return &WorkerPoolCapacity{}, nil
}

type ExternalWorkerPoolControllerForTest struct {
	ctx            context.Context
	name           string
	workerRepo     repo.WorkerRepository
	providerRepo   repo.ProviderRepository
	workerPoolRepo repo.WorkerPoolRepository
	poolName       string
	providerName   string
}

func (wpc *ExternalWorkerPoolControllerForTest) Context() context.Context {
	return wpc.ctx
}

func (wpc *ExternalWorkerPoolControllerForTest) IsPreemptable() bool {
	return false
}

func (wpc *ExternalWorkerPoolControllerForTest) State() (*types.WorkerPoolState, error) {
	return &types.WorkerPoolState{}, nil
}

func (wpc *ExternalWorkerPoolControllerForTest) Mode() types.PoolMode {
	return types.PoolModeExternal
}

func (wpc *ExternalWorkerPoolControllerForTest) RequiresPoolSelector() bool {
	return false
}

func (wpc *ExternalWorkerPoolControllerForTest) ContainerRuntime() string {
	return "runc"
}

func (wpc *ExternalWorkerPoolControllerForTest) generateWorkerId() string {
	return uuid.New().String()[:8]
}

func (wpc *ExternalWorkerPoolControllerForTest) AddWorkerWithDelay(request *types.ContainerRequest, delay time.Duration) (*types.Worker, error) {
	return nil, errors.New("not implemented")
}

func (wpc *ExternalWorkerPoolControllerForTest) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	workerId := wpc.generateWorkerId()
	worker := &types.Worker{
		Id:           workerId,
		FreeCpu:      cpu,
		FreeMemory:   memory,
		FreeGpuCount: gpuCount,
		Status:       types.WorkerStatusPending,
		PoolName:     wpc.poolName,
	}

	// Add the worker state
	err := wpc.workerRepo.AddWorker(worker)
	if err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolControllerForTest) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	err := wpc.providerRepo.SetMachineLock(wpc.providerName, wpc.name, machineId)
	if err != nil {
		return nil, err
	}
	defer wpc.providerRepo.RemoveMachineLock(wpc.providerName, wpc.name, machineId)

	machine, err := wpc.providerRepo.GetMachine(wpc.providerName, wpc.name, machineId)
	if err != nil {
		return nil, err
	}

	workers, err := wpc.workerRepo.GetAllWorkersOnMachine(machineId)
	if err != nil {
		return nil, err
	}

	if machine.State.Status != types.MachineStatusRegistered {
		return nil, errors.New("machine not registered")
	}

	remainingMachineCpu := machine.State.Cpu
	remainingMachineMemory := machine.State.Memory
	remainingMachineGpuCount := machine.State.GpuCount
	for _, worker := range workers {
		remainingMachineCpu -= worker.TotalCpu
		remainingMachineMemory -= worker.TotalMemory
		remainingMachineGpuCount -= uint32(worker.TotalGpuCount)
	}

	if remainingMachineCpu >= int64(cpu) && remainingMachineMemory >= int64(memory) && machine.State.Gpu == gpuType && remainingMachineGpuCount >= gpuCount {
		// If there is only one GPU available on the machine, give the worker access to everything
		// This prevents situations where a user requests a small amount of compute, and the subsequent
		// request has higher compute requirements
		if machine.State.GpuCount == 1 {
			cpu = machine.State.Cpu
			memory = machine.State.Memory
		}
	} else {
		return nil, errors.New("machine out of capacity")
	}

	worker := &types.Worker{
		Id:            workerId,
		FreeCpu:       cpu,
		FreeMemory:    memory,
		Gpu:           gpuType,
		FreeGpuCount:  gpuCount,
		Status:        types.WorkerStatusPending,
		PoolName:      wpc.poolName,
		TotalGpuCount: gpuCount,
		TotalCpu:      cpu,
		TotalMemory:   memory,
	}

	worker.MachineId = machineId

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolControllerForTest) Name() string {
	return wpc.name
}

func (wpc *ExternalWorkerPoolControllerForTest) FreeCapacity() (*WorkerPoolCapacity, error) {
	return &WorkerPoolCapacity{}, nil
}

func TestNewSchedulerForTest(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)
}

func TestRunContainer(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backendRepo,
	}

	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).GPUConcurrencyLimit = 0
	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = 10000

	// Schedule a container
	err = wb.Run(&types.ContainerRequest{
		ContainerId: "test-container",
	})
	assert.Nil(t, err)

	// Make sure you can't schedule a container with the same ID twice
	err = wb.Run(&types.ContainerRequest{
		ContainerId: "test-container",
	})

	if err != nil {
		_, ok := err.(*types.ContainerAlreadyScheduledError)
		assert.True(t, ok, "error is not of type *types.ContainerAlreadyScheduledError")
	} else {
		t.Error("Expected error, but got nil")
	}
}

func TestProcessRequests(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backendRepo,
	}

	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).GPUConcurrencyLimit = 10
	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = 100000

	// Prepare some requests to process.
	requests := []*types.ContainerRequest{
		{
			ContainerId: uuid.New().String(),
			Cpu:         1000,
			Memory:      2000,
			Gpu:         "A10G",
			GpuCount:    1,
		},
		{
			ContainerId: uuid.New().String(),
			Cpu:         1000,
			Memory:      2000,
			Gpu:         "T4",
			GpuCount:    1,
		},
		{
			ContainerId: uuid.New().String(),
			Cpu:         1000,
			Memory:      2000,
			Gpu:         "",
		},
		{
			ContainerId:  uuid.New().String(),
			Cpu:          1000,
			Memory:       2000,
			Gpu:          "",
			PoolSelector: "beta9-build",
		},
	}

	for _, req := range requests {
		err = wb.Run(req)
		if err != nil {
			t.Errorf("Unexpected error while adding request to backlog: %s", err)
		}
	}

	assert.Equal(t, int64(4), wb.requestBacklog.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				wb.StartProcessingRequests()
			}
		}
	}()

	<-ctx.Done()

	assert.Equal(t, int64(0), wb.requestBacklog.Len())
}

func TestGetController(t *testing.T) {
	wb, _ := NewSchedulerForTest()

	t.Run("returns correct controller", func(t *testing.T) {
		cpuRequest := &types.ContainerRequest{Gpu: ""}
		defaultController, err := wb.getControllers(cpuRequest)
		if err != nil || defaultController[0].Name() != "default" {
			t.Errorf("Expected default controller, got %v, error: %v", defaultController, err)
		}

		a10gRequest := &types.ContainerRequest{GpuRequest: []string{"A10G"}}
		a10gController, err := wb.getControllers(a10gRequest)
		if err != nil || a10gController[0].Name() != "beta9-a10g" {
			t.Errorf("Expected beta9-a10g controller, got %v, error: %v", a10gController, err)
		}

		t4Request := &types.ContainerRequest{GpuRequest: []string{"T4"}}
		t4Controller, err := wb.getControllers(t4Request)
		if err != nil || t4Controller[0].Name() != "beta9-t4" {
			t.Errorf("Expected beta9-t4 controller, got %v, error: %v", t4Controller, err)
		}

		buildRequest := &types.ContainerRequest{PoolSelector: "beta9-build"}
		buildController, err := wb.getControllers(buildRequest)
		if err != nil || buildController[0].Name() != "beta9-build" {
			t.Errorf("Expected beta9-build controller, got %v, error: %v", buildController, err)
		}
	})

	t.Run("returns error if no suitable controller found", func(t *testing.T) {
		unknownRequest := &types.ContainerRequest{GpuRequest: []string{"UNKNOWN_GPU"}}
		_, err := wb.getControllers(unknownRequest)
		if err == nil {
			t.Errorf("Expected error for unknown GPU type, got nil")
		}
	})
}

func TestSelectGPUWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorker := &types.Worker{
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "A10G",
	}

	// Create a new worker
	err = wb.workerRepo.AddWorker(newWorker)
	assert.Nil(t, err)

	cpuRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
	}

	firstRequest := &types.ContainerRequest{
		Cpu:        1000,
		Memory:     1000,
		GpuRequest: []string{"A10G"},
	}

	secondRequest := &types.ContainerRequest{
		Cpu:        1000,
		Memory:     1000,
		GpuRequest: []string{"T4"},
	}

	// CPU request should not be able to select a GPU worker
	_, err = wb.selectWorker(cpuRequest)
	assert.Error(t, err)

	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	assert.True(t, ok)

	// Select a worker for the request
	worker, err := wb.selectWorker(firstRequest)
	assert.Nil(t, err)

	// Check if the worker selected has the "A10G" GPU
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Id, worker.Id)

	// Actually schedule the request
	err = wb.scheduleRequest(worker, firstRequest)
	assert.Nil(t, err)

	// We have no workers left, so this one should fail
	_, err = wb.selectWorker(secondRequest)
	assert.Error(t, err)

	_, ok = err.(*types.ErrNoSuitableWorkerFound)
	assert.True(t, ok)
}

func TestSelectCPUWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorker := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusPending,
		FreeCpu:    2000,
		FreeMemory: 2000,
		Gpu:        "",
	}

	newWorker2 := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	// Create a new worker
	err = wb.workerRepo.AddWorker(newWorker)
	assert.Nil(t, err)

	err = wb.workerRepo.AddWorker(newWorker2)
	assert.Nil(t, err)

	gpuRequest := &types.ContainerRequest{
		Cpu:        1000,
		Memory:     1000,
		GpuRequest: []string{"A10G"},
	}

	firstRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	secondRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	thirdRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// GPU request should not be able to select a CPU worker
	_, err = wb.selectWorker(gpuRequest)
	assert.Error(t, err)

	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	assert.True(t, ok)

	// Add GPU worker to test that CPU workers won't select it
	gpuWorker := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "A10G",
	}

	err = wb.workerRepo.AddWorker(gpuWorker)
	assert.Nil(t, err)

	// Select a worker for the request
	worker, err := wb.selectWorker(firstRequest)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, firstRequest)
	assert.Nil(t, err)

	worker, err = wb.selectWorker(secondRequest)
	assert.Nil(t, err)
	assert.Equal(t, "", worker.Gpu)

	err = wb.scheduleRequest(worker, secondRequest)
	assert.Nil(t, err)

	worker, err = wb.selectWorker(thirdRequest)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, thirdRequest)
	assert.Nil(t, err)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, "", updatedWorker.Gpu)
	assert.Equal(t, types.WorkerStatusPending, updatedWorker.Status)

}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func TestSelectWorkersWithBackupGPU(t *testing.T) {
	tests := []struct {
		name               string
		requests           []*types.ContainerRequest
		expectedGpuResults []string
		gpus               []string
	}{
		{
			name: "simple",
			requests: []*types.ContainerRequest{
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G", "T4"},
				},
			},
			expectedGpuResults: []string{"A10G", "T4"},
			gpus:               []string{"A10G", "T4"},
		},
		{
			name: "complex",
			requests: []*types.ContainerRequest{
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"T4", "A6000"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G", "T4", "A6000"},
				},
			},
			expectedGpuResults: []string{"A10G", "T4", "A6000"},
			gpus:               []string{"A10G", "T4", "A6000"},
		},
		{
			name: "not enough backup GPUs",
			requests: []*types.ContainerRequest{
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G", "T4"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G", "T4"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"A10G", "T4"},
				},
			},
			expectedGpuResults: []string{"A10G", "T4", ""},
			gpus:               []string{"A10G", "T4", "A6000"},
		},
		{
			name: "backward compatibility",
			requests: []*types.ContainerRequest{
				{
					Cpu:        1000,
					Memory:     1000,
					Gpu:        "A10G",
					GpuRequest: []string{"T4"},
				},
			},
			expectedGpuResults: []string{"A10G"},
			gpus:               []string{"A10G", "T4"},
		},
		{
			name: "any gpu",
			requests: []*types.ContainerRequest{
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"any"},
				},
				{
					Cpu:        1000,
					Memory:     1000,
					GpuRequest: []string{"any"},
				},
			},
			expectedGpuResults: []string{"A10G", "T4", "A6000", "H100"},
			gpus:               []string{"A10G", "T4", "A6000", "H100"},
		},
	}

	for _, tt := range tests {
		wb, err := NewSchedulerForTest()
		assert.Nil(t, err)
		assert.NotNil(t, wb)

		t.Run(tt.name, func(t *testing.T) {
			for _, gpu := range tt.gpus {
				newWorker := &types.Worker{
					Id:         uuid.New().String(),
					Status:     types.WorkerStatusPending,
					FreeCpu:    1000,
					FreeMemory: 1000,
					Gpu:        gpu,
				}

				// Create a new worker
				err = wb.workerRepo.AddWorker(newWorker)
				assert.Nil(t, err)
			}

			for i, req := range tt.requests {
				worker, err := wb.selectWorker(req)
				if err != nil {
					assert.EqualError(t, err, (&types.ErrNoSuitableWorkerFound{}).Error())
					assert.Equal(t, tt.expectedGpuResults[i], "")
					continue
				}

				reqGpus := req.GpuRequest
				if req.Gpu != "" {
					reqGpus = append(reqGpus, req.Gpu)
				}

				if !slices.Contains(req.GpuRequest, string(types.GPU_ANY)) {
					assert.True(t, stringInSlice(worker.Gpu, reqGpus))
				}

				err = wb.scheduleRequest(worker, req)
				assert.Nil(t, err)
			}
		})
	}
}

func TestRequiresPoolSelectorWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorkerWithRequiresPoolSelector := &types.Worker{
		Id:                   "worker1",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              2000,
		FreeMemory:           2000,
		Gpu:                  "",
		RequiresPoolSelector: true,
		PoolName:             "cpu",
	}

	newWorkerWithoutRequiresPoolSelector := &types.Worker{
		Id:         "worker2",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2000,
		Gpu:        "",
		PoolName:   "cpu2",
	}

	// Create a new worker with the correct pool selector
	err = wb.workerRepo.AddWorker(newWorkerWithRequiresPoolSelector)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1000,
		Gpu:          "",
		PoolSelector: "cpu",
	}

	// Select a worker for the request, this one should succeed since it has a pool selector
	worker, err := wb.selectWorker(firstRequest)
	assert.Nil(t, err)
	assert.Equal(t, newWorkerWithRequiresPoolSelector.Id, worker.Id)

	err = wb.scheduleRequest(worker, firstRequest)
	assert.Nil(t, err)

	// Try creating another worker, which has no pool selector
	secondRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should fail since it has no pool selector
	_, err = wb.selectWorker(secondRequest)
	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	assert.True(t, ok)

	// Create a new worker without a pool selector
	err = wb.workerRepo.AddWorker(newWorkerWithoutRequiresPoolSelector)
	assert.Nil(t, err)

	// Select a worker for the request, this one should fail since it has no pool selector
	worker, err = wb.selectWorker(secondRequest)
	assert.Nil(t, err)

	assert.Equal(t, worker.Id, newWorkerWithoutRequiresPoolSelector.Id)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorkerWithRequiresPoolSelector.Id)
	assert.Nil(t, err)

	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)
	assert.Equal(t, "", updatedWorker.Gpu)
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
}

func TestPreemptableWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newPreemptableWorker := &types.Worker{
		Id:          "worker1",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     2000,
		FreeMemory:  2000,
		Gpu:         "",
		PoolName:    "cpu",
		Preemptable: true,
	}

	// Create a new worker that is preemptable
	err = wb.workerRepo.AddWorker(newPreemptableWorker)
	assert.Nil(t, err)

	nonPreemptableRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should not succeed since the only available worker is preemptable
	_, err = wb.selectWorker(nonPreemptableRequest)
	assert.Equal(t, &types.ErrNoSuitableWorkerFound{}, err)

	preemptableRequest := &types.ContainerRequest{
		Cpu:         1000,
		Memory:      1000,
		Gpu:         "",
		Preemptable: true,
	}

	// Select a worker for the request, this one should succeed since there is an available preemptable worker
	_, err = wb.selectWorker(preemptableRequest)
	assert.Nil(t, err)

	newNonPreemptableWorker := &types.Worker{
		Id:         "worker2",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2000,
		Gpu:        "",
		PoolName:   "cpu2",
	}

	// Create a new worker that is non preemptable
	err = wb.workerRepo.AddWorker(newNonPreemptableWorker)
	assert.Nil(t, err)

	// Select a worker for the request, this one should succeed since there is an available preemptable worker
	worker, err := wb.selectWorker(nonPreemptableRequest)
	assert.Nil(t, err)
	assert.Equal(t, worker.Id, newNonPreemptableWorker.Id)
}

func TestPoolPriority(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorkerWithLowPriority := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2000,
		Gpu:        "",
		PoolName:   "cpu",
		Priority:   0,
	}

	newWorkerWithHigherPriority := &types.Worker{
		Id:         "worker2",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2000,
		Gpu:        "",
		PoolName:   "cpu2",
		Priority:   1,
	}

	err = wb.workerRepo.AddWorker(newWorkerWithLowPriority)
	assert.Nil(t, err)

	err = wb.workerRepo.AddWorker(newWorkerWithHigherPriority)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should land on worker2
	// since it has higher priority
	worker, err := wb.selectWorker(request)
	assert.Nil(t, err)
	assert.Equal(t, newWorkerWithHigherPriority.Id, worker.Id)

	err = wb.scheduleRequest(worker, request)
	assert.Nil(t, err)

	secondRequest := &types.ContainerRequest{
		Cpu:    2000,
		Memory: 2000,
		Gpu:    "",
	}

	// Select a worker for the second request, this one should land on worker1
	worker, err = wb.selectWorker(secondRequest)
	assert.Nil(t, err)
	assert.Equal(t, newWorkerWithLowPriority.Id, worker.Id)
}

func TestSelectBuildWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorker := &types.Worker{
		Status:               types.WorkerStatusPending,
		FreeCpu:              2000,
		FreeMemory:           2000,
		Gpu:                  "",
		PoolName:             "beta9-build",
		RequiresPoolSelector: true,
	}

	// Create a new worker
	err = wb.workerRepo.AddWorker(newWorker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:          2000,
		Memory:       2000,
		Gpu:          "",
		PoolSelector: "beta9-build",
	}

	// Select a worker for the request
	worker, err := wb.selectWorker(request)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, request)
	assert.Nil(t, err)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, "", updatedWorker.Gpu)
	assert.Equal(t, types.WorkerStatusPending, updatedWorker.Status)
}

type BackendRepoConcurrencyLimitsForTest struct {
	repo.BackendRepository
	GPUConcurrencyLimit uint32
	CPUConcurrencyLimit uint32
}

func (b *BackendRepoConcurrencyLimitsForTest) GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error) {
	return &types.ConcurrencyLimit{
		GPULimit:          b.GPUConcurrencyLimit,
		CPUMillicoreLimit: b.CPUConcurrencyLimit,
	}, nil
}

func TestConcurrencyLimit(t *testing.T) {
	tests := []struct {
		name           string
		gpuConcurrency uint32
		cpuConcurrency uint32
		requests       []*types.ContainerRequest
		errorToMatch   error
	}{
		{
			name:           "limits are 0",
			gpuConcurrency: 0,
			cpuConcurrency: 0,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Cpu:         1000,
					Memory:      2000,
				},
			},
			errorToMatch: &types.ThrottledByConcurrencyLimitError{
				Reason: "cpu quota exceeded",
			},
		},
		{
			name:           "cpu requests are barely within limits",
			gpuConcurrency: 0,
			cpuConcurrency: 1000,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Cpu:         1000,
				},
			},
			errorToMatch: nil,
		},
		{
			name:           "cpu requests exceed limits",
			gpuConcurrency: 0,
			cpuConcurrency: 1000,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Cpu:         1000,
				},
				{
					ContainerId: uuid.New().String(),
					Cpu:         1,
				},
			},
			errorToMatch: &types.ThrottledByConcurrencyLimitError{
				Reason: "cpu quota exceeded",
			},
		},
		{
			name:           "gpu requests are barely within limits",
			gpuConcurrency: 1,
			cpuConcurrency: 0,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
				},
			},
			errorToMatch: nil,
		},
		{
			name:           "gpu requests exceed limits",
			gpuConcurrency: 1,
			cpuConcurrency: 0,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
				},
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
				},
			},
			errorToMatch: &types.ThrottledByConcurrencyLimitError{
				Reason: "gpu quota exceeded",
			},
		},
		{
			name:           "gpu and cpu requests are barely within limits",
			gpuConcurrency: 1,
			cpuConcurrency: 1000,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
					Cpu:         1000,
				},
			},
			errorToMatch: nil,
		},
		{
			name:           "gpu and cpu requests exceed limits",
			gpuConcurrency: 1,
			cpuConcurrency: 1000,
			requests: []*types.ContainerRequest{
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
					Cpu:         1000,
				},
				{
					ContainerId: uuid.New().String(),
					Gpu:         "A10G",
					GpuCount:    1,
					Cpu:         1,
				},
			},
			errorToMatch: &types.ThrottledByConcurrencyLimitError{
				Reason: "gpu quota exceeded",
			},
		},
	}

	// Add a test with 100 containers
	oneHundredContainersTest := []*types.ContainerRequest{}
	for i := 0; i < 100; i++ {
		oneHundredContainersTest = append(oneHundredContainersTest, &types.ContainerRequest{
			ContainerId: uuid.New().String(),
			Cpu:         1000,
		})
	}

	tests = append(tests, []struct {
		name           string
		gpuConcurrency uint32
		cpuConcurrency uint32
		requests       []*types.ContainerRequest
		errorToMatch   error
	}{
		{
			name:           "cpu requests succeeds with 100 containers within limit",
			gpuConcurrency: 0,
			cpuConcurrency: 1000 * 100,
			requests:       oneHundredContainersTest,
			errorToMatch:   nil,
		},
	}...)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wb, err := NewSchedulerForTest()
			assert.Nil(t, err)
			assert.NotNil(t, wb)

			backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
			wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
				BackendRepository: backendRepo,
			}

			wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).GPUConcurrencyLimit = test.gpuConcurrency
			wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = test.cpuConcurrency

			var errToExpect error
			for _, req := range test.requests {
				errToExpect = wb.Run(req)
				if errToExpect != nil {
					break
				}
			}

			if errToExpect != nil {
				assert.Equal(t, test.errorToMatch, errToExpect)
			}
		})
	}
}
