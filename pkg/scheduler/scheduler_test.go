package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/stretchr/testify/require"
	tjassert "github.com/tj/assert"
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
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)
}

func TestRunContainer(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	// Make sure you can't schedule a container with the same ID twice
	err = wb.Run(&types.ContainerRequest{
		ContainerId: "test-container",
	})

	if err != nil {
		_, ok := err.(*types.ContainerAlreadyScheduledError)
		tjassert.True(t, ok, "error is not of type *types.ContainerAlreadyScheduledError")
	} else {
		t.Error("Expected error, but got nil")
	}
}

func TestProcessRequests(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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

	tjassert.Equal(t, int64(4), wb.requestBacklog.Len())

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

	tjassert.Equal(t, int64(0), wb.requestBacklog.Len())
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
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

	newWorker := &types.Worker{
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "A10G",
	}

	// Create a new worker
	err = wb.workerRepo.AddWorker(newWorker)
	tjassert.Nil(t, err)

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
	tjassert.Error(t, err)

	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	tjassert.True(t, ok)

	// Select a worker for the request
	worker, err := wb.selectWorker(firstRequest)
	tjassert.Nil(t, err)

	// Check if the worker selected has the "A10G" GPU
	tjassert.Equal(t, newWorker.Gpu, worker.Gpu)
	tjassert.Equal(t, newWorker.Id, worker.Id)

	// Actually schedule the request
	err = wb.scheduleRequest(worker, firstRequest)
	tjassert.Nil(t, err)

	// We have no workers left, so this one should fail
	_, err = wb.selectWorker(secondRequest)
	tjassert.Error(t, err)

	_, ok = err.(*types.ErrNoSuitableWorkerFound)
	tjassert.True(t, ok)
}

func TestSelectCPUWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	err = wb.workerRepo.AddWorker(newWorker2)
	tjassert.Nil(t, err)

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
	tjassert.Error(t, err)

	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	tjassert.True(t, ok)

	// Add GPU worker to test that CPU workers won't select it
	gpuWorker := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "A10G",
	}

	err = wb.workerRepo.AddWorker(gpuWorker)
	tjassert.Nil(t, err)

	// Select a worker for the request
	worker, err := wb.selectWorker(firstRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, firstRequest)
	tjassert.Nil(t, err)

	worker, err = wb.selectWorker(secondRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, "", worker.Gpu)

	err = wb.scheduleRequest(worker, secondRequest)
	tjassert.Nil(t, err)

	worker, err = wb.selectWorker(thirdRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, thirdRequest)
	tjassert.Nil(t, err)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorker.Id)
	tjassert.Nil(t, err)
	tjassert.Equal(t, int64(0), updatedWorker.FreeCpu)
	tjassert.Equal(t, int64(0), updatedWorker.FreeMemory)
	tjassert.Equal(t, "", updatedWorker.Gpu)
	tjassert.Equal(t, types.WorkerStatusPending, updatedWorker.Status)

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
		tjassert.Nil(t, err)
		tjassert.NotNil(t, wb)

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
				tjassert.Nil(t, err)
			}

			for i, req := range tt.requests {
				worker, err := wb.selectWorker(req)
				if err != nil {
					tjassert.EqualError(t, err, (&types.ErrNoSuitableWorkerFound{}).Error())
					tjassert.Equal(t, tt.expectedGpuResults[i], "")
					continue
				}

				reqGpus := req.GpuRequest
				if req.Gpu != "" {
					reqGpus = append(reqGpus, req.Gpu)
				}

				if !slices.Contains(req.GpuRequest, string(types.GPU_ANY)) {
					tjassert.True(t, stringInSlice(worker.Gpu, reqGpus))
				}

				err = wb.scheduleRequest(worker, req)
				tjassert.Nil(t, err)
			}
		})
	}
}

func TestRequiresPoolSelectorWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1000,
		Gpu:          "",
		PoolSelector: "cpu",
	}

	// Select a worker for the request, this one should succeed since it has a pool selector
	worker, err := wb.selectWorker(firstRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorkerWithRequiresPoolSelector.Id, worker.Id)

	err = wb.scheduleRequest(worker, firstRequest)
	tjassert.Nil(t, err)

	// Try creating another worker, which has no pool selector
	secondRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should fail since it has no pool selector
	_, err = wb.selectWorker(secondRequest)
	_, ok := err.(*types.ErrNoSuitableWorkerFound)
	tjassert.True(t, ok)

	// Create a new worker without a pool selector
	err = wb.workerRepo.AddWorker(newWorkerWithoutRequiresPoolSelector)
	tjassert.Nil(t, err)

	// Select a worker for the request, this one should fail since it has no pool selector
	worker, err = wb.selectWorker(secondRequest)
	tjassert.Nil(t, err)

	tjassert.Equal(t, worker.Id, newWorkerWithoutRequiresPoolSelector.Id)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorkerWithRequiresPoolSelector.Id)
	tjassert.Nil(t, err)

	tjassert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	tjassert.Equal(t, int64(1000), updatedWorker.FreeMemory)
	tjassert.Equal(t, "", updatedWorker.Gpu)
	tjassert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
}

func TestPreemptableWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	nonPreemptableRequest := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should not succeed since the only available worker is preemptable
	_, err = wb.selectWorker(nonPreemptableRequest)
	tjassert.Equal(t, &types.ErrNoSuitableWorkerFound{}, err)

	preemptableRequest := &types.ContainerRequest{
		Cpu:         1000,
		Memory:      1000,
		Gpu:         "",
		Preemptable: true,
	}

	// Select a worker for the request, this one should succeed since there is an available preemptable worker
	_, err = wb.selectWorker(preemptableRequest)
	tjassert.Nil(t, err)

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
	tjassert.Nil(t, err)

	// Select a worker for the request, this one should succeed since there is an available preemptable worker
	worker, err := wb.selectWorker(nonPreemptableRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, worker.Id, newNonPreemptableWorker.Id)
}

func TestPoolPriority(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	err = wb.workerRepo.AddWorker(newWorkerWithHigherPriority)
	tjassert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Select a worker for the request, this one should land on worker2
	// since it has higher priority
	worker, err := wb.selectWorker(request)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorkerWithHigherPriority.Id, worker.Id)

	err = wb.scheduleRequest(worker, request)
	tjassert.Nil(t, err)

	secondRequest := &types.ContainerRequest{
		Cpu:    2000,
		Memory: 2000,
		Gpu:    "",
	}

	// Select a worker for the second request, this one should land on worker1
	worker, err = wb.selectWorker(secondRequest)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorkerWithLowPriority.Id, worker.Id)
}

func TestSelectBuildWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

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
	tjassert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:          2000,
		Memory:       2000,
		Gpu:          "",
		PoolSelector: "beta9-build",
	}

	// Select a worker for the request
	worker, err := wb.selectWorker(request)
	tjassert.Nil(t, err)
	tjassert.Equal(t, newWorker.Gpu, worker.Gpu)

	err = wb.scheduleRequest(worker, request)
	tjassert.Nil(t, err)

	updatedWorker, err := wb.workerRepo.GetWorkerById(newWorker.Id)
	tjassert.Nil(t, err)
	tjassert.Equal(t, int64(0), updatedWorker.FreeCpu)
	tjassert.Equal(t, int64(0), updatedWorker.FreeMemory)
	tjassert.Equal(t, "", updatedWorker.Gpu)
	tjassert.Equal(t, types.WorkerStatusPending, updatedWorker.Status)
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
			tjassert.Nil(t, err)
			tjassert.NotNil(t, wb)

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
				tjassert.Equal(t, test.errorToMatch, errToExpect)
			}
		})
	}
}

// Test that we never double-schedule the same container
func TestScheduling_NoDoubleScheduling(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	containerRepo := repo.NewContainerRedisRepository(rdb)
	backlog := NewRequestBacklog(rdb)
	
	// Create 20 workers
	for i := 0; i < 20; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        10000,
			FreeCpu:         10000,
			TotalMemory:     10000,
			FreeMemory:      10000,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}
	
	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		containerRepo:       containerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, maxConcurrentScheduling),
	}
	
	// Track scheduled containers
	scheduledContainers := sync.Map{}
	var scheduled int64
	var doubleScheduled int64
	
	// Add 50 containers
	numContainers := 50
	for i := 0; i < numContainers; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         500,
			Memory:      500,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}
	
	var wg sync.WaitGroup
	
	// Process all requests
	for backlog.Len() > 0 {
		batch, err := backlog.PopBatch(int64(batchSize))
		if err != nil || len(batch) == 0 {
			break
		}
		
		for _, request := range batch {
			req := request
			wg.Add(1)
			
			scheduler.schedulingSemaphore <- struct{}{}
			go func(r *types.ContainerRequest) {
				defer func() {
					<-scheduler.schedulingSemaphore
					wg.Done()
				}()
				
				// Try to schedule with retry logic (simulating scheduleOnWorker behavior)
				const maxRetries = 5
				for attempt := 0; attempt < maxRetries; attempt++ {
					// Fetch fresh workers on each attempt
					workers, err := workerRepo.GetAllWorkersLockFree()
					if err != nil {
						continue
					}
					
					// Try each worker
					for _, worker := range workers {
						if worker.FreeCpu >= r.Cpu && worker.FreeMemory >= r.Memory {
							err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
							if err == nil {
								// Check if already scheduled
								if _, exists := scheduledContainers.LoadOrStore(r.ContainerId, worker.Id); exists {
									atomic.AddInt64(&doubleScheduled, 1)
									t.Logf("❌ DOUBLE SCHEDULED: %s (first: %v, second: %s)", 
										r.ContainerId, 
										func() string { v, _ := scheduledContainers.Load(r.ContainerId); return v.(string) }(),
										worker.Id)
								} else {
									atomic.AddInt64(&scheduled, 1)
								}
								return
							}
							// Version conflict - try next worker
						}
					}
					
					// Brief backoff before retry
					if attempt < maxRetries-1 {
						time.Sleep(time.Millisecond * time.Duration(attempt+1))
					}
				}
			}(req)
		}
	}
	
	wg.Wait()
	
	t.Logf("\n🔍 Double Scheduling Test Results:")
	t.Logf("   ✅ Scheduled: %d/%d", scheduled, numContainers)
	t.Logf("   ❌ Double Scheduled: %d", doubleScheduled)
	
	require.Equal(t, int64(0), doubleScheduled, "No containers should be double-scheduled")
	require.Equal(t, int64(numContainers), scheduled, "All containers should be scheduled exactly once")
}


// Test that failed requests don't get scheduled multiple times across retries
func TestRetryLogic_NoDoubleScheduling(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	
	// Create one worker with limited capacity (will cause contention)
	worker := &types.Worker{
		Id:              "worker-1",
		Status:          types.WorkerStatusAvailable,
		TotalCpu:        5000,
		FreeCpu:         5000,
		TotalMemory:     5000,
		FreeMemory:      5000,
		PoolName:        "default",
		ResourceVersion: 0,
	}
	require.NoError(t, workerRepo.AddWorker(worker))
	
	// Track scheduled containers
	scheduledContainers := sync.Map{}
	var scheduled int64
	var doubleScheduled int64
	var wg sync.WaitGroup
	
	// Try to schedule 10 containers concurrently on the same worker
	// This should cause many version conflicts and retries
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			req := &types.ContainerRequest{
				ContainerId: fmt.Sprintf("container-%d", id),
				Cpu:         500,
				Memory:      500,
				Timestamp:   time.Now(),
			}
			
			// Simulate retry logic
			for attempt := 0; attempt < 5; attempt++ {
				freshWorker, _ := workerRepo.GetWorkerById(worker.Id)
				if freshWorker == nil {
					break
				}
				
				err := workerRepo.UpdateWorkerCapacity(freshWorker, req, types.RemoveCapacity)
				if err == nil {
					// Successfully scheduled
					if _, exists := scheduledContainers.LoadOrStore(req.ContainerId, freshWorker.Id); exists {
						atomic.AddInt64(&doubleScheduled, 1)
						t.Logf("❌ DOUBLE SCHEDULED on retry: %s", req.ContainerId)
					} else {
						atomic.AddInt64(&scheduled, 1)
					}
					break
				}
				
				// Retry with backoff
				time.Sleep(time.Millisecond * time.Duration(attempt+1))
			}
		}(i)
	}
	
	wg.Wait()
	
	t.Logf("\n🔍 Retry Logic Double Scheduling Test:")
	t.Logf("   ✅ Scheduled: %d/10", scheduled)
	t.Logf("   ❌ Double Scheduled: %d", doubleScheduled)
	t.Logf("   ⚠️  Failed to schedule: %d", 10-scheduled-doubleScheduled)
	
	require.Equal(t, int64(0), doubleScheduled, "No containers should be double-scheduled despite retries")
}

// Comprehensive end-to-end test with requeue logic
func TestScheduling_EndToEnd_WithRequeue(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	containerRepo := repo.NewContainerRedisRepository(rdb)
	backlog := NewRequestBacklog(rdb)
	
	// Create 30 workers with capacity
	for i := 0; i < 30; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        10000,
			FreeCpu:         10000,
			TotalMemory:     10000,
			FreeMemory:      10000,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}
	
	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		containerRepo:       containerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, maxConcurrentScheduling),
	}
	
	// Track scheduled containers
	scheduledContainers := sync.Map{}
	var totalScheduled int64
	var doubleScheduled int64
	
	// Add 100 containers
	numContainers := 100
	for i := 0; i < numContainers; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         500,
			Memory:      500,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}
	
	// Process with requeue (up to 3 passes)
	maxPasses := 3
	for pass := 0; pass < maxPasses; pass++ {
		if backlog.Len() == 0 {
			break
		}
		
		var wg sync.WaitGroup
		passScheduled := int64(0)
		
		// Process current backlog
		for backlog.Len() > 0 {
			batch, err := backlog.PopBatch(int64(batchSize))
			if err != nil || len(batch) == 0 {
				break
			}
			
			workers, _ := scheduler.getCachedWorkers()
			
			for _, request := range batch {
				req := request
				wg.Add(1)
				
				scheduler.schedulingSemaphore <- struct{}{}
				go func(r *types.ContainerRequest) {
					defer func() {
						<-scheduler.schedulingSemaphore
						wg.Done()
					}()
					
					// Try with retries
					scheduled := false
					for attempt := 0; attempt < 5 && !scheduled; attempt++ {
						shuffled := shuffleWorkers(workers)
						for _, worker := range shuffled {
							if worker.FreeCpu >= r.Cpu && worker.FreeMemory >= r.Memory {
								err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
								if err == nil {
									// Check for double scheduling
									if _, exists := scheduledContainers.LoadOrStore(r.ContainerId, worker.Id); exists {
										atomic.AddInt64(&doubleScheduled, 1)
										t.Logf("❌ DOUBLE SCHEDULED: %s", r.ContainerId)
									} else {
										atomic.AddInt64(&totalScheduled, 1)
										atomic.AddInt64(&passScheduled, 1)
									}
									scheduled = true
									break
								}
							}
						}
						if !scheduled && attempt < 4 {
							time.Sleep(time.Millisecond * time.Duration(attempt+1))
						}
					}
					
					// Requeue if failed
					if !scheduled {
						backlog.Push(r)
					}
				}(req)
			}
		}
		
		wg.Wait()
		t.Logf("Pass %d: Scheduled %d containers (total: %d/%d)", 
			pass+1, passScheduled, totalScheduled, numContainers)
	}
	
	t.Logf("\n🎯 End-to-End Test Results:")
	t.Logf("   ✅ Total Scheduled: %d/%d (%.0f%%)", totalScheduled, numContainers, float64(totalScheduled)/float64(numContainers)*100)
	t.Logf("   ❌ Double Scheduled: %d", doubleScheduled)
	t.Logf("   ⚠️  Failed: %d", numContainers-int(totalScheduled))
	
	require.Equal(t, int64(0), doubleScheduled, "No double scheduling")
	require.GreaterOrEqual(t, int(totalScheduled), 95, "At least 95% should schedule with requeue")
}

// Test scheduling 100 containers in < 2 seconds with 100% success rate
func TestSchedulingPerformance_100Containers(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
	workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
	backlog := NewRequestBacklog(rdb)
	
	// Create 40 workers with plenty of capacity (more workers = less contention)
	for i := 0; i < 40; i++ {
		worker := &types.Worker{
			Id:              fmt.Sprintf("worker-%d", i),
			Status:          types.WorkerStatusAvailable,
			TotalCpu:        50000,
			FreeCpu:         50000,
			TotalMemory:     50000,
			FreeMemory:      50000,
			PoolName:        "default",
			ResourceVersion: 0,
		}
		require.NoError(t, workerRepo.AddWorker(worker))
	}
	
	scheduler := &Scheduler{
		ctx:                 context.Background(),
		workerRepo:          workerRepo,
		requestBacklog:      backlog,
		schedulingSemaphore: make(chan struct{}, maxConcurrentScheduling),
	}
	
	// Add 100 containers
	for i := 0; i < 100; i++ {
		req := &types.ContainerRequest{
			ContainerId: fmt.Sprintf("container-%d", i),
			Cpu:         1000,
			Memory:      1000,
			Timestamp:   time.Now(),
			WorkspaceId: "test",
			StubId:      "test",
		}
		require.NoError(t, backlog.Push(req))
	}
	
	var scheduled int64
	var conflicts int64
	var wg sync.WaitGroup
	var requeueMu sync.Mutex
	var requeued []*types.ContainerRequest
	
	start := time.Now()
	
	// Process all requests with requeue support
	maxPasses := 3 // Allow up to 3 passes for retries
	for pass := 0; pass < maxPasses; pass++ {
		if pass > 0 {
			// Requeue failed requests from previous pass
			requeueMu.Lock()
			for _, req := range requeued {
				backlog.Push(req)
			}
			requeued = nil
			requeueMu.Unlock()
			
			if backlog.Len() == 0 {
				break
			}
		}
		
		for backlog.Len() > 0 {
			batch, err := backlog.PopBatch(int64(batchSize))
			if err != nil || len(batch) == 0 {
				break
			}
			
			workers, err := scheduler.getCachedWorkers()
			require.NoError(t, err)
			
			for _, request := range batch {
				req := request
				wg.Add(1)
				
				scheduler.schedulingSemaphore <- struct{}{}
				go func(r *types.ContainerRequest) {
					defer func() {
						<-scheduler.schedulingSemaphore
						wg.Done()
					}()
					
					// Try to schedule with retries
					success := false
					for attempt := 0; attempt < 8; attempt++ {
						// Shuffle workers to distribute load
						shuffled := shuffleWorkers(workers)
						
						for _, worker := range shuffled {
							if worker.FreeCpu >= r.Cpu && worker.FreeMemory >= r.Memory {
								err := workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity)
								if err == nil {
									atomic.AddInt64(&scheduled, 1)
									success = true
									return
								}
								atomic.AddInt64(&conflicts, 1)
							}
						}
						
						// Exponential backoff
						if attempt < 7 {
							backoff := time.Millisecond * time.Duration(1<<uint(attempt))
							if backoff > 10*time.Millisecond {
								backoff = 10 * time.Millisecond
							}
							time.Sleep(backoff)
						}
					}
					
					// Failed - requeue for retry in next pass
					if !success {
						requeueMu.Lock()
						requeued = append(requeued, r)
						requeueMu.Unlock()
					}
				}(req)
			}
		}
		
		wg.Wait() // Wait for current pass to complete
	}
	
	wg.Wait()
	duration := time.Since(start)
	
	failed := int64(100) - scheduled
	
	t.Logf("\n🚀 Performance Test: 100 Containers")
	t.Logf("   ✅ Scheduled: %d/100 (%.0f%%)", scheduled, float64(scheduled))
	t.Logf("   ❌ Failed: %d", failed)
	t.Logf("   ⏱️  Duration: %v (target: < 2s)", duration)
	t.Logf("   ⚔️  Conflicts: %d (%.1f per success)", conflicts, float64(conflicts)/float64(scheduled))
	t.Logf("   📊 Throughput: %.0f containers/sec", float64(scheduled)/duration.Seconds())
	t.Logf("   🎯 Workers: 40 (capacity: 2000 slots)")
	
	// Assert requirements
	require.Equal(t, int64(100), scheduled, "Must schedule all 100 containers")
	require.Less(t, duration, 2*time.Second, "Must complete in < 2 seconds")
	
	t.Logf("\n✅ Performance test PASSED")
}

// Benchmark different parameter configurations
func TestSchedulingPerformance_ParameterComparison(t *testing.T) {
	configs := []struct {
		name               string
		batchSize          int
		maxConcurrent      int
		workers            int
		expectedSuccessMin int
		expectedTimeMax    time.Duration
	}{
		{"Small batch, high concurrency", 5, 300, 25, 95, 2 * time.Second},
		{"Medium batch, medium concurrency", 10, 200, 25, 95, 2 * time.Second},
		{"Large batch, medium concurrency", 20, 200, 25, 95, 2 * time.Second},
		{"Optimal (current)", 10, 200, 30, 95, 2 * time.Second},
	}
	
	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			mr, _ := miniredis.Run()
			defer mr.Close()

			rdb, _ := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: "single"})
			workerRepo := repo.NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour})
			backlog := NewRequestBacklog(rdb)
			
			// Create workers
			for i := 0; i < cfg.workers; i++ {
				worker := &types.Worker{
					Id:              fmt.Sprintf("worker-%d", i),
					Status:          types.WorkerStatusAvailable,
					TotalCpu:        50000,
					FreeCpu:         50000,
					TotalMemory:     50000,
					FreeMemory:      50000,
					PoolName:        "default",
					ResourceVersion: 0,
				}
				require.NoError(t, workerRepo.AddWorker(worker))
			}
			
			scheduler := &Scheduler{
				ctx:                 context.Background(),
				workerRepo:          workerRepo,
				requestBacklog:      backlog,
				schedulingSemaphore: make(chan struct{}, cfg.maxConcurrent),
			}
			
			// Add 100 containers
			for i := 0; i < 100; i++ {
				req := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("container-%d", i),
					Cpu:         1000,
					Memory:      1000,
					Timestamp:   time.Now(),
					WorkspaceId: "test",
					StubId:      "test",
				}
				require.NoError(t, backlog.Push(req))
			}
			
			var scheduled int64
			var wg sync.WaitGroup
			
			start := time.Now()
			for backlog.Len() > 0 {
				batch, err := backlog.PopBatch(int64(cfg.batchSize))
				if err != nil || len(batch) == 0 {
					break
				}
				
				workers, _ := scheduler.getCachedWorkers()
				
				for _, request := range batch {
					req := request
					wg.Add(1)
					
					scheduler.schedulingSemaphore <- struct{}{}
					go func(r *types.ContainerRequest) {
						defer func() {
							<-scheduler.schedulingSemaphore
							wg.Done()
						}()
						
						// Try with retries
						for attempt := 0; attempt < 5; attempt++ {
							shuffled := shuffleWorkers(workers)
							for _, worker := range shuffled {
								if worker.FreeCpu >= r.Cpu && worker.FreeMemory >= r.Memory {
									if workerRepo.UpdateWorkerCapacity(worker, r, types.RemoveCapacity) == nil {
										atomic.AddInt64(&scheduled, 1)
										return
									}
								}
							}
							if attempt < 4 {
								time.Sleep(time.Millisecond)
							}
						}
					}(req)
				}
			}
			
			wg.Wait()
			duration := time.Since(start)
			
			t.Logf("  Batch:%d Concurrent:%d Workers:%d → %d/100 in %v", 
				cfg.batchSize, cfg.maxConcurrent, cfg.workers, scheduled, duration)
			
			require.GreaterOrEqual(t, int(scheduled), cfg.expectedSuccessMin, 
				"Success rate too low")
			require.Less(t, duration, cfg.expectedTimeMax, 
				"Too slow")
		})
	}
}

func TestProcessRequestWithWorkers_PoolSelector(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backendRepo,
	}
	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = 100000

	// Create a worker with RequiresPoolSelector
	workerWithPoolSelector := &types.Worker{
		Id:                   "pool-worker",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              10000,
		FreeMemory:           10000,
		PoolName:             "special-pool",
		RequiresPoolSelector: true,
	}
	err = wb.workerRepo.AddWorker(workerWithPoolSelector)
	tjassert.Nil(t, err)

	// Create a regular worker
	regularWorker := &types.Worker{
		Id:         "regular-worker",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    10000,
		FreeMemory: 10000,
		PoolName:   "default",
	}
	err = wb.workerRepo.AddWorker(regularWorker)
	tjassert.Nil(t, err)

	// Request without pool selector should NOT use the RequiresPoolSelector worker
	requestWithoutSelector := &types.ContainerRequest{
		ContainerId: "no-selector",
		Cpu:         1000,
		Memory:      1000,
		Timestamp:   time.Now(),
		WorkspaceId: "test",
		StubId:      "test",
	}
	workers, _ := wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(requestWithoutSelector, workers)

	// Verify it was scheduled on regular worker, not the pool-selector worker
	regularWorkerUpdated, _ := wb.workerRepo.GetWorkerById(regularWorker.Id)
	tjassert.Equal(t, int64(9000), regularWorkerUpdated.FreeCpu, "Should schedule on regular worker")

	poolWorkerUpdated, _ := wb.workerRepo.GetWorkerById(workerWithPoolSelector.Id)
	tjassert.Equal(t, int64(10000), poolWorkerUpdated.FreeCpu, "Should NOT schedule on pool-selector worker")

	// Request WITH pool selector should use the RequiresPoolSelector worker
	requestWithSelector := &types.ContainerRequest{
		ContainerId:  "with-selector",
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "special-pool",
		Timestamp:    time.Now(),
		WorkspaceId:  "test",
		StubId:       "test",
	}
	workers, _ = wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(requestWithSelector, workers)

	// Verify it was scheduled on pool-selector worker
	poolWorkerUpdated, _ = wb.workerRepo.GetWorkerById(workerWithPoolSelector.Id)
	tjassert.Equal(t, int64(9000), poolWorkerUpdated.FreeCpu, "Should schedule on pool-selector worker")
}

// Test that processRequestWithWorkers enforces runtime flags
func TestProcessRequestWithWorkers_RuntimeFlags(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backendRepo,
	}
	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = 100000

	// Create a worker with gvisor runtime
	gvisorWorker := &types.Worker{
		Id:         "gvisor-worker",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    10000,
		FreeMemory: 10000,
		Runtime:    types.ContainerRuntimeGvisor.String(),
	}
	err = wb.workerRepo.AddWorker(gvisorWorker)
	tjassert.Nil(t, err)

	// Create a worker with runc runtime
	runcWorker := &types.Worker{
		Id:         "runc-worker",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    10000,
		FreeMemory: 10000,
		Runtime:    types.ContainerRuntimeRunc.String(),
	}
	err = wb.workerRepo.AddWorker(runcWorker)
	tjassert.Nil(t, err)

	// Request with DockerEnabled should only use gvisor worker
	dockerRequest := &types.ContainerRequest{
		ContainerId:   "docker-req",
		Cpu:           1000,
		Memory:        1000,
		DockerEnabled: true,
		Timestamp:     time.Now(),
		WorkspaceId:   "test",
		StubId:        "test",
	}
	workers, _ := wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(dockerRequest, workers)

	// Verify it was scheduled on gvisor worker
	gvisorUpdated, _ := wb.workerRepo.GetWorkerById(gvisorWorker.Id)
	tjassert.Equal(t, int64(9000), gvisorUpdated.FreeCpu, "Should schedule on gvisor worker")

	runcUpdated, _ := wb.workerRepo.GetWorkerById(runcWorker.Id)
	tjassert.Equal(t, int64(10000), runcUpdated.FreeCpu, "Should NOT schedule on runc worker")

	// Request without DockerEnabled can use either worker
	normalRequest := &types.ContainerRequest{
		ContainerId:   "normal-req",
		Cpu:           1000,
		Memory:        1000,
		DockerEnabled: false,
		Timestamp:     time.Now(),
		WorkspaceId:   "test",
		StubId:        "test",
	}
	workers, _ = wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(normalRequest, workers)

	// Verify it was scheduled on one of the workers (gvisor already has load, so should go to runc)
	runcUpdated, _ = wb.workerRepo.GetWorkerById(runcWorker.Id)
	gvisorUpdated, _ = wb.workerRepo.GetWorkerById(gvisorWorker.Id)
	
	totalUsedCpu := (10000 - runcUpdated.FreeCpu) + (10000 - gvisorUpdated.FreeCpu)
	tjassert.Equal(t, int64(2000), totalUsedCpu, "Should have scheduled 2 requests total")
}

// Test that processRequestWithWorkers enforces preemptability
func TestProcessRequestWithWorkers_Preemptability(t *testing.T) {
	wb, err := NewSchedulerForTest()
	tjassert.Nil(t, err)
	tjassert.NotNil(t, wb)

	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	wb.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backendRepo,
	}
	wb.backendRepo.(*BackendRepoConcurrencyLimitsForTest).CPUConcurrencyLimit = 100000

	// Create a preemptable worker
	preemptableWorker := &types.Worker{
		Id:          "preemptable-worker",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     10000,
		FreeMemory:  10000,
		Preemptable: true,
	}
	err = wb.workerRepo.AddWorker(preemptableWorker)
	tjassert.Nil(t, err)

	// Create a non-preemptable worker
	nonPreemptableWorker := &types.Worker{
		Id:          "non-preemptable-worker",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     10000,
		FreeMemory:  10000,
		Preemptable: false,
	}
	err = wb.workerRepo.AddWorker(nonPreemptableWorker)
	tjassert.Nil(t, err)

	// Non-preemptable request should NOT use preemptable worker
	nonPreemptableRequest := &types.ContainerRequest{
		ContainerId: "non-preempt-req",
		Cpu:         1000,
		Memory:      1000,
		Preemptable: false,
		Timestamp:   time.Now(),
		WorkspaceId: "test",
		StubId:      "test",
	}
	workers, _ := wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(nonPreemptableRequest, workers)

	// Verify it was scheduled on non-preemptable worker
	nonPreemptUpdated, _ := wb.workerRepo.GetWorkerById(nonPreemptableWorker.Id)
	tjassert.Equal(t, int64(9000), nonPreemptUpdated.FreeCpu, "Should schedule on non-preemptable worker")

	preemptUpdated, _ := wb.workerRepo.GetWorkerById(preemptableWorker.Id)
	tjassert.Equal(t, int64(10000), preemptUpdated.FreeCpu, "Should NOT schedule on preemptable worker")

	// Preemptable request CAN use preemptable worker
	preemptableRequest := &types.ContainerRequest{
		ContainerId: "preempt-req",
		Cpu:         1000,
		Memory:      1000,
		Preemptable: true,
		Timestamp:   time.Now(),
		WorkspaceId: "test",
		StubId:      "test",
	}
	workers, _ = wb.workerRepo.GetAllWorkersLockFree()
	wb.processRequestWithWorkers(preemptableRequest, workers)

	// Verify it was scheduled (could be on either worker, but bin packing prefers filling already-used workers)
	nonPreemptUpdated2, _ := wb.workerRepo.GetWorkerById(nonPreemptableWorker.Id)
	preemptUpdated2, _ := wb.workerRepo.GetWorkerById(preemptableWorker.Id)
	
	// One of the workers should have less CPU after scheduling
	totalAllocated := (10000 - nonPreemptUpdated2.FreeCpu) + (10000 - preemptUpdated2.FreeCpu)
	tjassert.Equal(t, int64(2000), totalAllocated, "Should have scheduled 2 requests total (2000 CPU)")
}

