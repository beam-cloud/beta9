package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
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
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(rdb)
	computeRepo := repo.NewComputeRedisRepository(rdb)
	backendRepo, _ := repo.NewBackendPostgresRepositoryForTest()
	requestBacklog := NewRequestBacklogForTest(rdb)

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}

	config := configManager.GetConfig()
	defaultPool := config.Worker.Pools["default"]
	defaultPool.Priority = 1
	config.Worker.Pools = map[string]types.WorkerPoolConfig{
		"default":     defaultPool,
		"beta9-build": {RequiresPoolSelector: true},
		"beta9-cpu":   {},
		"beta9-a10g":  {GPUType: "A10G"},
		"beta9-t4":    {GPUType: "T4"},
	}
	eventRepo := repo.NewEventClientRepo(config)

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
		backendRepo:           &BackendRepoConcurrencyLimitsForTest{BackendRepository: backendRepo, GPUConcurrencyLimit: 100, CPUConcurrencyLimit: 100000},
		workerRepo:            workerRepo,
		workerPoolRepo:        workerPoolRepo,
		computeRepo:           computeRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		config:                config,
		schedulerUsageMetrics: schedulerUsageMetrics,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,

		provisioning:              newProvisioningTracker(),
		workerProvisioningBackoff: newWorkerProvisioningBackoff(),
		credentials:               newSchedulerCredentialCache(),
	}, nil
}

func TestPrivateWorkerRequestUsesPoolMode(t *testing.T) {
	manager := NewWorkerPoolManager(false)
	privateState := &compute.PoolState{Selector: "private-pool"}
	manager.SetPoolAt(agentPoolControllerKey("workspace-1", privateState), "private-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	manager.SetPool("local-pool", types.WorkerPoolConfig{Mode: types.PoolModeLocal}, nil)
	scheduler := &Scheduler{workerPoolManager: manager}

	request := &types.ContainerRequest{WorkspaceId: "workspace-1"}
	assert.True(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "private-pool"}, request))
	assert.False(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "local-pool"}, request))
	assert.False(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "missing-pool"}, request))
}

func TestPrivatePoolRequestsBypassManagedQuotaLookup(t *testing.T) {
	manager := NewWorkerPoolManager(false)
	privateState := &compute.PoolState{Selector: "private-gpu-pool"}
	manager.SetPoolAt(agentPoolControllerKey("workspace-1", privateState), "private-gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	scheduler := &Scheduler{workerPoolManager: manager}
	stubConfig, err := json.Marshal(types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "private-gpu-pool", Selector: "private-gpu-pool", Fallback: types.PrivatePoolFallbackInternal},
	})
	assert.NoError(t, err)

	assert.True(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{WorkspaceId: "workspace-1", PoolSelector: "private-gpu-pool"}))
	assert.True(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{
		WorkspaceId:  "workspace-1",
		PoolSelector: "private-gpu-pool",
		Stub:         types.StubWithRelated{Stub: types.Stub{Config: string(stubConfig)}},
	}))
	assert.True(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{
		WorkspaceId: "workspace-1",
		Stub:        types.StubWithRelated{Stub: types.Stub{Config: string(stubConfig)}},
	}))
	assert.False(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{WorkspaceId: "workspace-2", PoolSelector: "private-gpu-pool"}))
	assert.False(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{}))
}

func TestPrivateGPUWorkerCapacityRequiresRequestedGPUCount(t *testing.T) {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager(false)}
	scheduler.workerPoolManager.SetPool("private-gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate, GPUType: "H100"}, nil)
	request := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1024,
		GpuRequest:   []string{"H100"},
		GpuCount:     8,
		PoolSelector: "private-gpu-pool",
	}

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{{
		Id:                   "eight-gpu-worker",
		PoolName:             "private-gpu-pool",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              2000,
		FreeMemory:           2048,
		Gpu:                  "H100",
		FreeGpuCount:         8,
		RequiresPoolSelector: true,
	}}, request)
	assert.NoError(t, err)
	assert.Equal(t, "eight-gpu-worker", worker.Id)

	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{{
		Id:                   "short-worker",
		PoolName:             "private-gpu-pool",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              2000,
		FreeMemory:           2048,
		Gpu:                  "H100",
		FreeGpuCount:         7,
		RequiresPoolSelector: true,
	}}, request)
	assert.Error(t, err)
}

func TestDockerEnabledRequestsCanUseRuncWorkersAndControllers(t *testing.T) {
	request := &types.ContainerRequest{DockerEnabled: true}

	controllers := []WorkerPoolController{
		&LocalWorkerPoolControllerForTest{name: "runc", containerRuntime: types.ContainerRuntimeRunc.String()},
		&LocalWorkerPoolControllerForTest{name: "gvisor", containerRuntime: types.ContainerRuntimeGvisor.String()},
	}

	filteredControllers := filterControllersByFlags(controllers, request)
	assert.Equal(t, controllers, filteredControllers)

	workers := []*types.Worker{
		{Id: "runc-worker", Runtime: types.ContainerRuntimeRunc.String()},
		{Id: "gvisor-worker", Runtime: types.ContainerRuntimeGvisor.String()},
	}

	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager(false)}
	filteredWorkers := scheduler.filterWorkersByFlags(workers, request)
	assert.Equal(t, workers, filteredWorkers)
}

// A preemptible marketplace listing (e.g. Vast-listed hardware) must stay
// schedulable for requests that opted in with AllowMarketplace: joining the
// marketplace implies accepting seller-side preemption, and nothing else sets
// request.Preemptable for typical serverless stubs.
func TestPreemptibleMarketplaceCapacityReachableWithAllowMarketplace(t *testing.T) {
	preemptibleMarketplace := &LocalWorkerPoolControllerForTest{
		name:        "marketplace",
		mode:        types.PoolModeMarketplace,
		preemptable: true,
	}
	request := &types.ContainerRequest{AllowMarketplace: true}
	assert.Equal(
		t,
		[]WorkerPoolController{preemptibleMarketplace},
		filterControllersByFlags([]WorkerPoolController{preemptibleMarketplace}, request),
	)

	// Preemptible capacity outside the marketplace still requires an explicit
	// request.Preemptable opt-in.
	preemptibleSpot := &LocalWorkerPoolControllerForTest{name: "spot", preemptable: true}
	assert.Empty(t, filterControllersByFlags([]WorkerPoolController{preemptibleSpot}, request))

	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager(false)}
	scheduler.workerPoolManager.SetPool("marketplace", types.WorkerPoolConfig{
		Mode:        types.PoolModeMarketplace,
		GPUType:     "A10G",
		Preemptable: true,
	}, nil)

	preemptibleWorker := &types.Worker{
		Id:            "marketplace-worker",
		PoolName:      "marketplace",
		Status:        types.WorkerStatusAvailable,
		Preemptable:   true,
		TotalCpu:      1000,
		FreeCpu:       1000,
		TotalMemory:   1024,
		FreeMemory:    1024,
		TotalGpuCount: 1,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{preemptibleWorker}, &types.ContainerRequest{
		Cpu:              1000,
		Memory:           512,
		GpuRequest:       []string{"A10G"},
		GpuCount:         1,
		AllowMarketplace: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, preemptibleWorker.Id, worker.Id)

	// A preemptible worker outside the marketplace stays unreachable without
	// request.Preemptable.
	spotWorker := &types.Worker{
		Id:          "spot-worker",
		PoolName:    "spot",
		Status:      types.WorkerStatusAvailable,
		Preemptable: true,
		TotalCpu:    1000,
		FreeCpu:     1000,
		TotalMemory: 1024,
		FreeMemory:  1024,
	}
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{spotWorker}, &types.ContainerRequest{
		Cpu:              1000,
		Memory:           512,
		AllowMarketplace: true,
	})
	assert.Error(t, err)
}

func TestMarketplaceControllersRequireExplicitSafeOptIn(t *testing.T) {
	marketplace := &LocalWorkerPoolControllerForTest{name: "marketplace", mode: types.PoolModeMarketplace}
	regular := &LocalWorkerPoolControllerForTest{name: "regular", mode: types.PoolModeLocal}

	assert.Equal(t, []WorkerPoolController{regular}, filterControllersByFlags([]WorkerPoolController{marketplace, regular}, &types.ContainerRequest{}))
	assert.Equal(t, []WorkerPoolController{marketplace, regular}, filterControllersByFlags([]WorkerPoolController{marketplace, regular}, &types.ContainerRequest{AllowMarketplace: true}))
	assert.Equal(t, []WorkerPoolController{regular}, filterControllersByFlags([]WorkerPoolController{marketplace, regular}, &types.ContainerRequest{AllowMarketplace: true, DockerEnabled: true}))
	assert.Equal(t, []WorkerPoolController{regular}, filterControllersByFlags([]WorkerPoolController{marketplace, regular}, &types.ContainerRequest{AllowMarketplace: true, PoolSelector: "regular"}))
}

func TestMarketplaceWorkersRequireExplicitSafeOptIn(t *testing.T) {
	marketplaceWorker := &types.Worker{
		Id:            "marketplace-worker",
		PoolName:      "marketplace",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      1000,
		FreeCpu:       1000,
		TotalMemory:   1024,
		FreeMemory:    1024,
		TotalGpuCount: 1,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager(false)}
	scheduler.workerPoolManager.SetPool("marketplace", types.WorkerPoolConfig{
		Mode:                 types.PoolModeMarketplace,
		GPUType:              "A10G",
		RequiresPoolSelector: false,
	}, nil)

	baseRequest := &types.ContainerRequest{Cpu: 1000, Memory: 512, GpuRequest: []string{"A10G"}, GpuCount: 1}

	_, err := scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, baseRequest.Clone())
	assert.Error(t, err)

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, &types.ContainerRequest{Cpu: 1000, Memory: 512, GpuRequest: []string{"A10G"}, GpuCount: 1, AllowMarketplace: true})
	assert.NoError(t, err)
	assert.Equal(t, marketplaceWorker.Id, worker.Id)

	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, &types.ContainerRequest{Cpu: 1000, Memory: 512, GpuRequest: []string{"A10G"}, GpuCount: 1, AllowMarketplace: true, DockerEnabled: true})
	assert.Error(t, err)

	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, &types.ContainerRequest{Cpu: 1000, Memory: 512, GpuRequest: []string{"A10G"}, GpuCount: 1, AllowMarketplace: true, PoolSelector: "marketplace"})
	assert.Error(t, err)

	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, &types.ContainerRequest{
		Cpu:              1000,
		Memory:           512,
		GpuRequest:       []string{"A10G"},
		GpuCount:         1,
		AllowMarketplace: true,
		Mounts: []types.Mount{{
			LocalPath: "/var/run/docker.sock",
			MountPath: "/var/run/docker.sock",
		}},
	})
	assert.Error(t, err)

	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{marketplaceWorker}, &types.ContainerRequest{
		Cpu:              1000,
		Memory:           512,
		GpuRequest:       []string{"A10G"},
		GpuCount:         1,
		AllowMarketplace: true,
		Mounts: []types.Mount{{
			LocalPath: "/data/objects/workspace/stub",
			MountPath: "/mnt/code",
		}},
	})
	assert.NoError(t, err)
}

func TestMarketplacePoolRuntimeFallsBackForUnsupportedGPU(t *testing.T) {
	state := &compute.PoolState{
		Mode: string(types.PoolModeMarketplace),
		Config: &pb.PoolConfig{
			Gpu: []string{"V100"},
		},
	}
	config := normalizeAgentWorkerPoolConfig(state)
	assert.Equal(t, types.ContainerRuntimeRunc.String(), config.ContainerRuntime)

	state.Config.Gpu = []string{"A10G"}
	config = normalizeAgentWorkerPoolConfig(state)
	assert.Equal(t, types.ContainerRuntimeGvisor.String(), config.ContainerRuntime)
}

func TestEnsureAgentPoolNormalizesPersistedManagedConfig(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager(false)

	provider := types.ProviderGeneric
	err = scheduler.EnsureAgentPool("admin-workspace", &compute.PoolState{
		Name:             "api-pool",
		Selector:         "api-pool",
		ManagementSource: types.WorkerPoolManagementSourceAPI,
		WorkerConfig: &types.WorkerPoolConfig{
			Mode:     types.PoolModeLocal,
			Provider: &provider,
			Priority: 7,
			GPUType:  "A10G",
		},
	})
	assert.NoError(t, err)

	pool, ok := scheduler.workerPoolManager.GetPool("api-pool")
	assert.True(t, ok)
	assert.Equal(t, types.PoolModeExternal, pool.Config.Mode)
	assert.Nil(t, pool.Config.Provider)
	assert.Equal(t, types.ContainerRuntimeRunc.String(), pool.Config.ContainerRuntime)
	assert.Equal(t, int32(7), pool.Config.Priority)
}

func TestEnsureAgentPoolReconcilesExistingMachineWorkers(t *testing.T) {
	s, err := NewSchedulerForTest()
	assert.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager(false)

	state := &compute.PoolState{
		Name:              "api-pool",
		Selector:          "api-pool",
		ManagementSource:  types.WorkerPoolManagementSourceAPI,
		ManagedInstanceID: "instance-1",
		WorkerConfig: &types.WorkerPoolConfig{
			Mode:             types.PoolModeExternal,
			ContainerRuntime: types.ContainerRuntimeGvisor.String(),
		},
	}
	machine := &compute.AgentTokenState{
		WorkspaceID:     "admin-workspace",
		PoolName:        state.Name,
		MachineID:       "machine-1",
		CPUCount:        4,
		MemoryMB:        8192,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastHeartbeatAt: time.Now(),
	}
	assert.NoError(t, s.computeRepo.SaveAgentTokenState(context.Background(), machine, time.Hour))
	assert.NoError(t, s.EnsureAgentPool(machine.WorkspaceID, state))

	worker, err := s.workerRepo.GetWorkerById(compute.AgentMachineWorkerID(machine.MachineID))
	assert.NoError(t, err)
	assert.Equal(t, types.ContainerRuntimeGvisor.String(), worker.Runtime)

	state.WorkerConfig.ContainerRuntime = types.ContainerRuntimeRunc.String()
	assert.NoError(t, s.EnsureAgentPool(machine.WorkspaceID, state))
	worker, err = s.workerRepo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, types.ContainerRuntimeRunc.String(), worker.Runtime)
}

func TestTenantAgentPoolsUseWorkspaceScopedControllerKeys(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager(false)
	scheduler.workerPoolManager.SetPool("shared", types.WorkerPoolConfig{Mode: types.PoolModeLocal}, nil)

	for _, workspaceID := range []string{"workspace-a", "workspace-b"} {
		state := &compute.PoolState{
			Name:     "private-pool",
			Selector: "shared",
			Mode:     string(types.PoolModePrivate),
			Config:   &pb.PoolConfig{Name: "private-pool", Selector: "shared", Mode: string(types.PoolModePrivate)},
		}
		assert.NoError(t, scheduler.computeRepo.SavePoolState(context.Background(), workspaceID, state))

		pool, err := scheduler.ensureAgentPoolForRequest(&types.ContainerRequest{WorkspaceId: workspaceID, PoolSelector: "shared"})
		assert.NoError(t, err)
		if pool == nil {
			t.Fatal("private pool controller was not loaded")
		}
		assert.Equal(t, types.PoolModePrivate, pool.Config.Mode)
		assert.Equal(t, "shared", pool.Name)
		assert.Equal(t, workspaceID, pool.Controller.(*AgentWorkerPoolController).WorkspaceID())
		assert.True(t, scheduler.privatePoolQuotaExempt(&types.ContainerRequest{WorkspaceId: workspaceID, PoolSelector: "shared"}))
	}

	global, ok := scheduler.workerPoolManager.GetPool("shared")
	assert.True(t, ok)
	assert.Equal(t, types.PoolModeLocal, global.Config.Mode)
}

func TestLoadedPrivatePoolDoesNotReconcileOnRequest(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager(false)

	const workspaceID = "workspace-private"
	state := &compute.PoolState{
		Name:     "private-pool",
		Selector: "private-selector",
		Mode:     string(types.PoolModePrivate),
		Config:   &pb.PoolConfig{Name: "private-pool", Selector: "private-selector", Mode: string(types.PoolModePrivate)},
	}
	machine := &compute.AgentTokenState{
		WorkspaceID:     workspaceID,
		PoolName:        state.Name,
		MachineID:       "machine-private",
		CPUCount:        4,
		MemoryMB:        8192,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastHeartbeatAt: time.Now(),
	}
	assert.NoError(t, scheduler.computeRepo.SavePoolState(context.Background(), workspaceID, state))
	assert.NoError(t, scheduler.computeRepo.SaveAgentTokenState(context.Background(), machine, time.Hour))

	request := &types.ContainerRequest{WorkspaceId: workspaceID, PoolSelector: state.Selector}
	_, err = scheduler.ensureAgentPoolForRequest(request)
	assert.NoError(t, err)

	worker, err := scheduler.workerRepo.GetWorkerById(compute.AgentMachineWorkerID(machine.MachineID))
	assert.NoError(t, err)
	assert.NoError(t, scheduler.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusAvailable))
	assert.NoError(t, scheduler.workerRepo.UpdateWorkerCapacity(worker, &types.ContainerRequest{Cpu: 100, Memory: 100}, types.RemoveCapacity))
	version := worker.ResourceVersion

	_, err = scheduler.ensureAgentPoolForRequest(request)
	assert.NoError(t, err)
	worker, err = scheduler.workerRepo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, version, worker.ResourceVersion)
}

// Machine-pinned requests (marketplace rentals) must only ever see the pinned
// machine's worker, regardless of pool selector requirements.
func TestFilterWorkersByMachinePinsWorker(t *testing.T) {
	workers := []*types.Worker{
		{Id: "w1", MachineId: "machine-1", PoolName: "marketplace-a100", RequiresPoolSelector: false},
		{Id: "w2", MachineId: "machine-2", PoolName: "private-pool", RequiresPoolSelector: true},
	}

	pinned := filterWorkersByMachine(workers, &types.ContainerRequest{MachineId: "machine-2"})
	assert.Len(t, pinned, 1)
	assert.Equal(t, "w2", pinned[0].Id)

	// The pin overrides pool-selector filtering: w2 requires a selector the
	// request doesn't carry, but it was already selected by the pin.
	selected := filterWorkersByPoolSelector(pinned, &types.ContainerRequest{MachineId: "machine-2"})
	assert.Len(t, selected, 1)

	unpinned := filterWorkersByMachine(workers, &types.ContainerRequest{})
	assert.Len(t, unpinned, 2)
}

// Rented GPUs are invisible to serverless marketplace requests; machine-pinned
// rental workloads still see the full machine.
func TestMarketplaceRentalCapacityHiddenFromServerless(t *testing.T) {
	redisServer, err := miniredis.Run()
	assert.NoError(t, err)
	t.Cleanup(redisServer.Close)
	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{redisServer.Addr()},
		Mode:  types.RedisModeSingle,
	})
	assert.NoError(t, err)

	computeRepo := repo.NewComputeRedisRepository(redisClient)
	assert.NoError(t, computeRepo.SaveMarketplaceRental(context.Background(), &compute.MarketplaceRentalState{
		ID:               "rental-1",
		BuyerWorkspaceID: "buyer-1",
		MachineID:        "machine-1",
		GPUCount:         2,
	}))

	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager(false), computeRepo: computeRepo}
	scheduler.workerPoolManager.SetPool("marketplace-a100", types.WorkerPoolConfig{
		Mode: types.PoolModeMarketplace,
	}, nil)

	worker := &types.Worker{
		Id:            "w1",
		MachineId:     "machine-1",
		PoolName:      "marketplace-a100",
		Gpu:           "A100-40",
		FreeGpuCount:  8,
		TotalGpuCount: 8,
	}

	serverless := &types.ContainerRequest{AllowMarketplace: true, GpuRequest: []string{"A100-40"}, GpuCount: 7}
	assert.Empty(t, scheduler.filterMarketplaceWorkers([]*types.Worker{worker}, serverless),
		"7 GPUs must not fit when 2 of 8 are rented")

	serverless.GpuCount = 6
	assert.Len(t, scheduler.filterMarketplaceWorkers([]*types.Worker{worker}, serverless), 1,
		"6 GPUs fit alongside the 2-GPU rental")

	pinned := &types.ContainerRequest{AllowMarketplace: true, MachineId: "machine-1", GpuRequest: []string{"A100-40"}, GpuCount: 2}
	assert.Len(t, scheduler.filterMarketplaceWorkers([]*types.Worker{worker}, pinned), 1,
		"the renter's machine-pinned workload consumes the rented capacity")

	// Fail closed: if rentals can't be read, serverless requests must not see
	// marketplace capacity (it may be exclusively rented), while pinned rental
	// workloads keep working.
	redisServer.Close()
	serverless.GpuCount = 1
	assert.Empty(t, scheduler.filterMarketplaceWorkers([]*types.Worker{worker}, serverless),
		"rental lookup failure must hide marketplace capacity from serverless requests")
	assert.Len(t, scheduler.filterMarketplaceWorkers([]*types.Worker{worker}, pinned), 1,
		"pinned rental workloads don't depend on the rental index")
}

type LocalWorkerPoolControllerForTest struct {
	ctx              context.Context
	name             string
	mode             types.PoolMode
	config           types.AppConfig
	workerRepo       repo.WorkerRepository
	preemptable      bool
	workerStatus     types.WorkerStatus
	addWorkerStarted chan struct{}
	unblockAddWorker chan struct{}
	addWorkerErr     error
	requiresSelector bool
	containerRuntime string
	addWorkerMu      sync.Mutex
	addWorkerCalls   int
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
	if wpc.mode != "" {
		return wpc.mode
	}
	return types.PoolModeLocal
}

func (wpc *LocalWorkerPoolControllerForTest) RequiresPoolSelector() bool {
	return wpc.requiresSelector
}

func (wpc *LocalWorkerPoolControllerForTest) ContainerRuntime() string {
	if wpc.containerRuntime != "" {
		return wpc.containerRuntime
	}
	return "runc"
}

func (wpc *LocalWorkerPoolControllerForTest) generateWorkerId() string {
	return uuid.New().String()[:8]
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	wpc.addWorkerMu.Lock()
	wpc.addWorkerCalls++
	addWorkerErr := wpc.addWorkerErr
	wpc.addWorkerMu.Unlock()

	if wpc.addWorkerStarted != nil {
		select {
		case wpc.addWorkerStarted <- struct{}{}:
		default:
		}
	}

	if addWorkerErr != nil {
		return nil, addWorkerErr
	}

	if wpc.unblockAddWorker != nil {
		select {
		case <-wpc.unblockAddWorker:
		case <-wpc.ctx.Done():
			return nil, wpc.ctx.Err()
		}
	}

	workerId := wpc.generateWorkerId()
	gpuType := ""
	if pool, ok := wpc.config.Worker.Pools[wpc.name]; ok {
		gpuType = pool.GPUType
	}
	status := wpc.workerStatus
	if status == "" {
		status = types.WorkerStatusPending
	}

	worker := &types.Worker{
		Id:                   workerId,
		TotalCpu:             cpu,
		TotalMemory:          memory,
		TotalGpuCount:        gpuCount,
		FreeCpu:              cpu,
		FreeMemory:           memory,
		FreeGpuCount:         gpuCount,
		Gpu:                  gpuType,
		PoolName:             wpc.name,
		RequiresPoolSelector: wpc.RequiresPoolSelector(),
		Status:               status,
	}

	// Add the worker state
	err := wpc.workerRepo.AddWorker(worker)
	if err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorkerCallCount() int {
	wpc.addWorkerMu.Lock()
	defer wpc.addWorkerMu.Unlock()
	return wpc.addWorkerCalls
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

func TestStopDeletesPendingBuildContainerState(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	containerId := types.BuildContainerPrefix + "pending"
	err = wb.containerRepo.SetContainerState(containerId, &types.ContainerState{
		ContainerId: containerId,
		Status:      types.ContainerStatusPending,
		WorkspaceId: "workspace-1",
		ScheduledAt: time.Now().Unix(),
	})
	assert.Nil(t, err)

	err = wb.Stop(&types.StopContainerArgs{
		ContainerId: containerId,
		Reason:      types.StopContainerReasonUser,
	})
	assert.Nil(t, err)

	_, err = wb.containerRepo.GetContainerState(containerId)
	notFound := &types.ErrContainerStateNotFound{}
	assert.True(t, notFound.From(err), "expected deleted pending build state, got %v", err)
}

func TestRunCleansContainerStateWhenBacklogPushFails(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	containerId := uuid.New().String()
	err = wb.requestBacklog.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), "wrong-type", 0).Err()
	assert.Nil(t, err)

	err = wb.Run(&types.ContainerRequest{
		ContainerId: containerId,
		WorkspaceId: "test-workspace",
		Cpu:         100,
		Memory:      100,
	})
	assert.Error(t, err)

	_, err = wb.containerRepo.GetContainerState(containerId)
	assert.Error(t, err)

	statusRepo := wb.containerRepo.(*repo.ContainerRedisRepository)
	status, err := statusRepo.GetContainerRequestStatus(containerId)
	assert.Nil(t, err)
	assert.Equal(t, types.ContainerRequestStatusFailed, status)
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
	for name, pool := range wb.config.Worker.Pools {
		wb.workerPoolManager.SetPool(name, pool, &LocalWorkerPoolControllerForTest{
			ctx:          wb.ctx,
			name:         name,
			config:       wb.config,
			workerRepo:   wb.workerRepo,
			workerStatus: types.WorkerStatusAvailable,
		})
	}

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

	deadline := time.After(2 * time.Second)
	for {
		if wb.requestBacklog.Len() == 0 {
			return
		}

		select {
		case <-deadline:
			assert.Equal(t, int64(0), wb.requestBacklog.Len())
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSchedulerProcessesLargeReadyBatch(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wb.ctx = ctx

	const requestCount = 1000
	worker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    requestCount,
		TotalMemory: requestCount * 2,
		FreeCpu:     requestCount,
		FreeMemory:  requestCount * 2,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	for i := 0; i < requestCount; i++ {
		err = wb.Run(&types.ContainerRequest{
			ContainerId: uuid.New().String(),
			Cpu:         1,
			Memory:      1,
		})
		assert.Nil(t, err)
	}
	assert.Equal(t, int64(requestCount), wb.requestBacklog.Len())

	go wb.StartProcessingRequests()

	received := map[string]struct{}{}
	// Generous deadline so the assertion is not flaky when the suite runs in
	// parallel under load; a genuinely stuck scheduler still fails promptly.
	deadline := time.After(30 * time.Second)
	for len(received) < requestCount {
		select {
		case <-deadline:
			t.Fatalf("expected %d scheduled requests, got %d", requestCount, len(received))
		default:
		}

		request, err := wb.workerRepo.GetNextContainerRequest(worker.Id)
		assert.Nil(t, err)
		if request == nil {
			time.Sleep(time.Millisecond)
			continue
		}

		received[request.ContainerId] = struct{}{}
	}

	assert.Equal(t, int64(0), wb.requestBacklog.Len())
	updatedWorker, err := wb.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
}

func TestSchedulerDoesNotBlockOnSlowWorkerProvisioning(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wb.ctx = ctx

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	wb.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{
		ctx:              ctx,
		name:             "beta9-cpu",
		workerRepo:       wb.workerRepo,
		addWorkerStarted: started,
		unblockAddWorker: unblock,
	})

	fastWorker := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 200,
		PoolName:   "beta9-build",
	}
	err = wb.workerRepo.AddWorker(fastWorker)
	assert.Nil(t, err)

	slowRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
	}
	fastRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-build",
	}

	err = wb.Run(slowRequest)
	assert.Nil(t, err)
	time.Sleep(time.Millisecond)
	err = wb.Run(fastRequest)
	assert.Nil(t, err)

	go wb.StartProcessingRequests()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected scheduler to start worker provisioning")
	}

	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("expected unrelated request to schedule while worker provisioning is blocked")
		default:
		}

		request, err := wb.workerRepo.GetNextContainerRequest(fastWorker.Id)
		assert.Nil(t, err)
		if request != nil && request.ContainerId == fastRequest.ContainerId {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestProcessRequestUsesInFlightProvisioningForCurrentBatch(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wb.ctx = ctx
	wb.config.Worker.DefaultWorkerCPURequest = 200
	wb.config.Worker.DefaultWorkerMemoryRequest = 250

	started := make(chan struct{}, 2)
	unblock := make(chan struct{})
	defer close(unblock)

	wb.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{
		ctx:              ctx,
		name:             "beta9-cpu",
		workerRepo:       wb.workerRepo,
		addWorkerStarted: started,
		unblockAddWorker: unblock,
	})

	firstRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	secondRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}

	wb.processRequest(firstRequest, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected scheduler to start worker provisioning")
	}

	wb.processRequest(secondRequest, nil)

	select {
	case <-started:
		t.Fatal("expected second request to reserve the in-flight worker instead of provisioning another")
	default:
	}

	wb.provisioning.mu.Lock()
	defer wb.provisioning.mu.Unlock()

	assert.Equal(t, 1, len(wb.provisioning.reservations))
	for _, reservation := range wb.provisioning.reservations {
		_, firstReserved := reservation.requestIDs[firstRequest.ContainerId]
		_, secondReserved := reservation.requestIDs[secondRequest.ContainerId]
		assert.True(t, firstReserved)
		assert.True(t, secondReserved)
	}
}

func TestProcessRequestTracksPendingWorkerCapacityAcrossBatches(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wb.ctx = ctx

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	defer close(unblock)

	wb.workerPoolManager.SetPool("beta9-a10g", types.WorkerPoolConfig{GPUType: "A10G"}, &LocalWorkerPoolControllerForTest{
		ctx:              ctx,
		name:             "beta9-a10g",
		config:           wb.config,
		workerRepo:       wb.workerRepo,
		addWorkerStarted: started,
		unblockAddWorker: unblock,
	})

	pendingWorker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusPending,
		TotalCpu:      1000,
		TotalMemory:   1250,
		TotalGpuCount: 1,
		FreeCpu:       1000,
		FreeMemory:    1250,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = wb.workerRepo.AddWorker(pendingWorker)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          1000,
		Memory:       1000,
		GpuRequest:   []string{"A10G"},
		PoolSelector: "beta9-a10g",
		Timestamp:    time.Now(),
	}
	secondRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          1000,
		Memory:       1000,
		GpuRequest:   []string{"A10G"},
		PoolSelector: "beta9-a10g",
		Timestamp:    time.Now(),
	}

	firstBatchWorkers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequest(firstRequest, firstBatchWorkers)

	secondBatchWorkers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequest(secondRequest, secondBatchWorkers)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected second request to provision another worker instead of over-reserving the pending GPU worker")
	}

	wb.provisioning.mu.Lock()
	defer wb.provisioning.mu.Unlock()

	pendingReservation := wb.provisioning.reservations[pendingWorker.Id]
	assert.NotNil(t, pendingReservation)
	_, firstReserved := pendingReservation.requestIDs[firstRequest.ContainerId]
	assert.True(t, firstReserved)
	_, secondReserved := pendingReservation.requestIDs[secondRequest.ContainerId]
	assert.False(t, secondReserved)
}

func TestProcessRequestUpdatesBatchWorkerCapacity(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	worker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    1000,
		TotalMemory: 1250,
		FreeCpu:     1000,
		FreeMemory:  1250,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))

	firstRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		Timestamp:   time.Now(),
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1,
		Memory:      1,
		Timestamp:   time.Now(),
	}

	wb.processRequest(firstRequest, workers)

	assert.Equal(t, int64(0), workers[0].FreeCpu)
	assert.Equal(t, int64(0), workers[0].FreeMemory)

	_, err = wb.selectWorkerFromWorkers(workers, secondRequest)
	assert.Error(t, err)
}

func TestProcessRequestBatchDoesNotOverScheduleWorkerSnapshot(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	worker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    200,
		TotalMemory: 250,
		FreeCpu:     200,
		FreeMemory:  250,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))

	requests := []*types.ContainerRequest{
		{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
		},
		{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
		},
		{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
		},
	}

	wb.processRequestBatch(requests, workers)

	queued := map[string]struct{}{}
	for i := 0; i < 2; i++ {
		request, err := wb.workerRepo.GetNextContainerRequest(worker.Id)
		assert.Nil(t, err)
		assert.NotNil(t, request)
		queued[request.ContainerId] = struct{}{}
	}
	extraRequest, err := wb.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.Nil(t, extraRequest)

	assert.Equal(t, 2, len(queued))
	updatedWorker, err := wb.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
}

func TestProcessRequestBatchSpreadsAcrossEqualWorkers(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	firstWorker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    200,
		TotalMemory: 250,
		FreeCpu:     200,
		FreeMemory:  250,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(firstWorker)
	assert.Nil(t, err)

	secondWorker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    200,
		TotalMemory: 250,
		FreeCpu:     200,
		FreeMemory:  250,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(secondWorker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)

	requests := []*types.ContainerRequest{
		{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
		},
		{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
		},
	}

	wb.processRequestBatch(requests, workers)

	firstQueued, err := wb.workerRepo.GetNextContainerRequest(firstWorker.Id)
	assert.Nil(t, err)
	secondQueued, err := wb.workerRepo.GetNextContainerRequest(secondWorker.Id)
	assert.Nil(t, err)

	assert.NotNil(t, firstQueued)
	assert.NotNil(t, secondQueued)
	assert.NotEqual(t, firstQueued.ContainerId, secondQueued.ContainerId)
}

func TestProcessRequestBatchSchedulesTinySandboxBurstByRequestedCapacity(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	wb.config.Worker.Pools["beta9-cpu"] = types.WorkerPoolConfig{
		ContainerRuntime:          types.ContainerRuntimeRunc.String(),
		ContainerStartConcurrency: 32,
	}

	workers := []*types.Worker{
		{
			Id:          uuid.New().String(),
			Status:      types.WorkerStatusAvailable,
			TotalCpu:    16000,
			TotalMemory: 32000,
			FreeCpu:     16000,
			FreeMemory:  32000,
			PoolName:    "beta9-cpu",
			Runtime:     types.ContainerRuntimeRunc.String(),
		},
		{
			Id:          uuid.New().String(),
			Status:      types.WorkerStatusAvailable,
			TotalCpu:    16000,
			TotalMemory: 32000,
			FreeCpu:     16000,
			FreeMemory:  32000,
			PoolName:    "beta9-cpu",
			Runtime:     types.ContainerRuntimeRunc.String(),
		},
	}
	for _, worker := range workers {
		err = wb.workerRepo.AddWorker(worker)
		assert.Nil(t, err)
	}

	requests := make([]*types.ContainerRequest, 70)
	for i := range requests {
		requests[i] = &types.ContainerRequest{
			ContainerId: uuid.New().String(),
			Cpu:         100,
			Memory:      100,
			Timestamp:   time.Now(),
			Stub: types.StubWithRelated{
				Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)},
			},
		}
	}

	workerSnapshot, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequestBatch(requests, workerSnapshot)

	for _, worker := range workers {
		queued := 0
		for {
			request, err := wb.workerRepo.GetNextContainerRequest(worker.Id)
			assert.Nil(t, err)
			if request == nil {
				break
			}
			queued++
		}
		assert.Equal(t, 35, queued)
	}
}

func TestProcessRequestBatchKeepsCPUAndGPUCapacitySeparate(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	cpuWorker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    100,
		TotalMemory: 125,
		FreeCpu:     100,
		FreeMemory:  125,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(cpuWorker)
	assert.Nil(t, err)

	gpuWorker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      100,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       100,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = wb.workerRepo.AddWorker(gpuWorker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)

	cpuRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         100,
		Memory:      100,
		Timestamp:   time.Now(),
	}
	gpuRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         100,
		Memory:      100,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}

	wb.processRequestBatch([]*types.ContainerRequest{gpuRequest, cpuRequest}, workers)

	queuedCPURequest, err := wb.workerRepo.GetNextContainerRequest(cpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedCPURequest)
	assert.Equal(t, cpuRequest.ContainerId, queuedCPURequest.ContainerId)
	assert.Equal(t, "", queuedCPURequest.Gpu)

	queuedGPURequest, err := wb.workerRepo.GetNextContainerRequest(gpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedGPURequest)
	assert.Equal(t, gpuRequest.ContainerId, queuedGPURequest.ContainerId)
	assert.Equal(t, "A10G", queuedGPURequest.Gpu)
	assert.Equal(t, uint32(1), queuedGPURequest.GpuCount)
}

func TestProcessRequestKeepsCPUAndGPUWorkersSeparate(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	cpuWorker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    2000,
		TotalMemory: 2500,
		FreeCpu:     2000,
		FreeMemory:  2500,
		PoolName:    "beta9-cpu",
	}
	err = wb.workerRepo.AddWorker(cpuWorker)
	assert.Nil(t, err)

	gpuWorker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      2000,
		TotalMemory:   2500,
		TotalGpuCount: 1,
		FreeCpu:       2000,
		FreeMemory:    2500,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = wb.workerRepo.AddWorker(gpuWorker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(workers))

	cpuRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		Timestamp:   time.Now(),
	}
	gpuRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		GpuCount:    1,
		Timestamp:   time.Now(),
	}

	wb.processRequest(cpuRequest, workers)
	wb.processRequest(gpuRequest, workers)

	queuedCPURequest, err := wb.workerRepo.GetNextContainerRequest(cpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedCPURequest)
	assert.Equal(t, cpuRequest.ContainerId, queuedCPURequest.ContainerId)
	assert.Equal(t, "", queuedCPURequest.Gpu)

	queuedGPURequest, err := wb.workerRepo.GetNextContainerRequest(gpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedGPURequest)
	assert.Equal(t, gpuRequest.ContainerId, queuedGPURequest.ContainerId)
	assert.Equal(t, "A10G", queuedGPURequest.Gpu)

	updatedCPUWorker, err := wb.workerRepo.GetWorkerById(cpuWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedCPUWorker.FreeCpu)
	assert.Equal(t, int64(1250), updatedCPUWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedCPUWorker.FreeGpuCount)

	updatedGPUWorker, err := wb.workerRepo.GetWorkerById(gpuWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedGPUWorker.FreeCpu)
	assert.Equal(t, int64(1250), updatedGPUWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedGPUWorker.FreeGpuCount)
}

func TestProcessRequestDefaultsGPUCountBeforeScheduling(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	worker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      2000,
		TotalMemory:   2500,
		TotalGpuCount: 1,
		FreeCpu:       2000,
		FreeMemory:    2500,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = wb.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}

	wb.processRequest(request, workers)

	queuedRequest, err := wb.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)
	assert.Equal(t, uint32(1), queuedRequest.GpuCount)
	assert.Equal(t, "A10G", queuedRequest.Gpu)

	updatedWorker, err := wb.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
}

func TestProcessRequestStaleReplicaGPUReservationRequeues(t *testing.T) {
	firstScheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	secondScheduler := *firstScheduler
	secondScheduler.provisioning = newProvisioningTracker()

	worker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      4000,
		TotalMemory:   5000,
		TotalGpuCount: 1,
		FreeCpu:       4000,
		FreeMemory:    5000,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = firstScheduler.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	firstReplicaWorkers, err := firstScheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	secondReplicaWorkers, err := secondScheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}

	firstScheduler.processRequest(firstRequest, firstReplicaWorkers)
	secondScheduler.processRequest(secondRequest, secondReplicaWorkers)

	queuedRequest, err := firstScheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)
	assert.Equal(t, firstRequest.ContainerId, queuedRequest.ContainerId)

	extraQueuedRequest, err := firstScheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.Nil(t, extraQueuedRequest)

	updatedWorker, err := firstScheduler.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(3000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(3750), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	deadline := time.After(time.Second)
	var requeuedRequest *types.ContainerRequest
	for requeuedRequest == nil {
		var err error
		requeuedRequest, err = firstScheduler.requestBacklog.Pop()
		if err == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatal("expected stale reservation to be requeued")
		case <-time.After(10 * time.Millisecond):
		}
	}

	assert.Equal(t, secondRequest.ContainerId, requeuedRequest.ContainerId)
	assert.Equal(t, uint32(1), requeuedRequest.GpuCount)
	assert.Equal(t, "A10G", requeuedRequest.Gpu)
}

func TestProcessRequestConcurrentStaleReplicaGPUReservationRequeues(t *testing.T) {
	firstScheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	secondScheduler := *firstScheduler
	secondScheduler.provisioning = newProvisioningTracker()

	worker := &types.Worker{
		Id:            uuid.New().String(),
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      4000,
		TotalMemory:   5000,
		TotalGpuCount: 1,
		FreeCpu:       4000,
		FreeMemory:    5000,
		FreeGpuCount:  1,
		Gpu:           "A10G",
		PoolName:      "beta9-a10g",
	}
	err = firstScheduler.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	firstReplicaWorkers, err := firstScheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	secondReplicaWorkers, err := secondScheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         1000,
		Memory:      1000,
		GpuRequest:  []string{"A10G"},
		Timestamp:   time.Now(),
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		firstScheduler.processRequest(firstRequest, firstReplicaWorkers)
	}()
	go func() {
		defer wg.Done()
		<-start
		secondScheduler.processRequest(secondRequest, secondReplicaWorkers)
	}()

	close(start)
	wg.Wait()

	queuedRequest, err := firstScheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)

	extraQueuedRequest, err := firstScheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.Nil(t, extraQueuedRequest)

	updatedWorker, err := firstScheduler.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(3000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(3750), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	queuedIds := map[string]struct{}{queuedRequest.ContainerId: {}}
	expectedIds := map[string]struct{}{
		firstRequest.ContainerId:  {},
		secondRequest.ContainerId: {},
	}
	_, queuedWasExpected := expectedIds[queuedRequest.ContainerId]
	assert.True(t, queuedWasExpected)

	deadline := time.After(time.Second)
	var requeuedRequest *types.ContainerRequest
	for requeuedRequest == nil {
		var err error
		requeuedRequest, err = firstScheduler.requestBacklog.Pop()
		if err == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatal("expected stale reservation to be requeued")
		case <-time.After(10 * time.Millisecond):
		}
	}

	_, requeuedWasExpected := expectedIds[requeuedRequest.ContainerId]
	assert.True(t, requeuedWasExpected)
	_, requeuedWasAlreadyQueued := queuedIds[requeuedRequest.ContainerId]
	assert.False(t, requeuedWasAlreadyQueued)
	assert.Equal(t, uint32(1), requeuedRequest.GpuCount)
	assert.Equal(t, "A10G", requeuedRequest.Gpu)

	extraRequeuedRequest, err := firstScheduler.requestBacklog.Pop()
	assert.Error(t, err)
	assert.Nil(t, extraRequeuedRequest)
}

func TestAddWorkerForReservationReleasesOnContextCancellation(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	wb.ctx = ctx

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	wb.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{
		ctx:              ctx,
		name:             "beta9-cpu",
		workerRepo:       wb.workerRepo,
		addWorkerStarted: started,
		unblockAddWorker: unblock,
	})

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}

	controllers, err := wb.getControllers(request)
	assert.Nil(t, err)
	reservationID := wb.provisioning.addReservation(wb, request, controllers[0])

	done := make(chan struct{})
	go func() {
		defer close(done)
		newWorkerProvisioningAttempt(wb, request, controllers[0], reservationID).run()
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected worker provisioning to start")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected worker provisioning goroutine to exit after cancellation")
	}

	wb.provisioning.mu.Lock()
	defer wb.provisioning.mu.Unlock()
	assert.Equal(t, 0, len(wb.provisioning.reservations))
	assert.Equal(t, 0, len(wb.provisioning.requestReservations))
}

func TestProvisionedWorkerUsesSchedulingMemory(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}

	controllers, err := wb.getControllers(request)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(controllers))

	expectedMemory := capacityMemoryForScheduling(request)
	reservationID := wb.provisioning.addReservation(wb, request, controllers[0])

	wb.provisioning.mu.Lock()
	reservation := wb.provisioning.reservations[reservationID]
	assert.NotNil(t, reservation)
	assert.Equal(t, expectedMemory, reservation.worker.TotalMemory)
	assert.Equal(t, int64(0), reservation.worker.FreeMemory)
	wb.provisioning.mu.Unlock()

	newWorkerProvisioningAttempt(wb, request, controllers[0], reservationID).run()

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))
	assert.Equal(t, expectedMemory, workers[0].TotalMemory)
	assert.Equal(t, expectedMemory, workers[0].FreeMemory)
}

func TestProvisionedWorkerUsesPoolSizingDefaults(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	poolConfig := types.WorkerPoolConfig{
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU:      "16000m",
			DefaultWorkerMemory:   "16Gi",
			DefaultWorkerGpuCount: "0",
		},
	}
	wb.workerPoolManager.SetPool("beta9-cpu", poolConfig, &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "beta9-cpu",
		config:     wb.config,
		workerRepo: wb.workerRepo,
	})

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}

	controllers, err := wb.getControllers(request)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(controllers))

	reservationID := wb.provisioning.addReservation(wb, request, controllers[0])
	wb.provisioning.mu.Lock()
	reservation := wb.provisioning.reservations[reservationID]
	assert.NotNil(t, reservation)
	assert.Equal(t, int64(16000), reservation.worker.TotalCpu)
	assert.Equal(t, int64(15900), reservation.worker.FreeCpu)
	assert.Equal(t, int64(16384), reservation.worker.TotalMemory)
	assert.Equal(t, int64(16384-capacityMemoryForScheduling(request)), reservation.worker.FreeMemory)
	wb.provisioning.mu.Unlock()

	newWorkerProvisioningAttempt(wb, request, controllers[0], reservationID).run()

	workers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))
	assert.Equal(t, int64(16000), workers[0].TotalCpu)
	assert.Equal(t, int64(16000), workers[0].FreeCpu)
	assert.Equal(t, int64(16384), workers[0].TotalMemory)
	assert.Equal(t, int64(16384), workers[0].FreeMemory)
}

func TestAgentHostedProvisioningUsesRequestSizing(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	wb.config.Worker.DefaultWorkerCPURequest = 16000
	wb.config.Worker.DefaultWorkerMemoryRequest = 16 * 1024

	poolConfig := types.WorkerPoolConfig{
		Mode: types.PoolModePrivate,
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU:      "32000m",
			DefaultWorkerMemory:   "32Gi",
			DefaultWorkerGpuCount: "8",
		},
	}
	wb.workerPoolManager.SetPool("private-pool", poolConfig, &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "private-pool",
		mode:       types.PoolModePrivate,
		config:     wb.config,
		workerRepo: wb.workerRepo,
	})

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		GpuRequest:   []string{"H100"},
		GpuCount:     1,
		PoolSelector: "private-pool",
		Timestamp:    time.Now(),
	}

	controllers, err := wb.getControllers(request)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(controllers))

	reservationID := wb.provisioning.addReservation(wb, request, controllers[0])
	wb.provisioning.mu.Lock()
	reservation := wb.provisioning.reservations[reservationID]
	assert.NotNil(t, reservation)
	assert.Equal(t, request.Cpu, reservation.worker.TotalCpu)
	assert.Equal(t, capacityMemoryForScheduling(request), reservation.worker.TotalMemory)
	assert.Equal(t, request.GpuCount, reservation.worker.TotalGpuCount)
	wb.provisioning.mu.Unlock()
}

func TestProcessRequestMarksNoControllerRequestFailed(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, _ := wb.requestBacklog.rdb.Subscribe(ctx, common.EventChannelKey(common.EventTypeContainerSchedulingFailed))

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		TaskId:      uuid.New().String(),
		WorkspaceId: "test-workspace",
		Cpu:         100,
		Memory:      100,
		GpuRequest:  []string{"UNKNOWN_GPU"},
		Timestamp:   time.Now(),
	}
	err = wb.containerRepo.SetContainerStateWithConcurrencyLimit(nil, request)
	assert.Nil(t, err)

	wb.processRequest(request, nil)

	_, err = wb.containerRepo.GetContainerState(request.ContainerId)
	assert.Error(t, err)

	statusRepo := wb.containerRepo.(*repo.ContainerRedisRepository)
	status, err := statusRepo.GetContainerRequestStatus(request.ContainerId)
	assert.Nil(t, err)
	assert.Equal(t, types.ContainerRequestStatusFailed, status)

	select {
	case message := <-events:
		event, _, claimed := wb.eventBus.Claim(message.Payload)
		assert.True(t, claimed)
		failure, ok := common.ParseContainerSchedulingFailure(event)
		assert.True(t, ok)
		assert.Equal(t, request.TaskId, failure.TaskID)
		assert.Equal(t, request.ContainerId, failure.ContainerID)
		assert.Equal(t, types.ContainerSchedulingFailureNoController, failure.Reason)
	case <-time.After(time.Second):
		t.Fatal("expected container scheduling failure event")
	}
}

func TestGetController(t *testing.T) {
	wb, _ := NewSchedulerForTest()

	t.Run("returns correct controller", func(t *testing.T) {
		cpuRequest := &types.ContainerRequest{Gpu: ""}
		defaultController, err := wb.getControllers(cpuRequest)
		if err != nil || defaultController[0].Name() != "default" {
			t.Errorf("Expected default controller, got %v, error: %v", defaultController, err)
		}

		noGPURequest := &types.ContainerRequest{Gpu: string(types.NO_GPU)}
		noGPUController, err := wb.getControllers(noGPURequest)
		if err != nil || noGPUController[0].Name() != "default" {
			t.Errorf("Expected default controller for NO_GPU request, got %v, error: %v", noGPUController, err)
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

func TestGetControllersRegistersAgentPoolFromRepository(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.NoError(t, err)
	wb.workerPoolManager = NewWorkerPoolManager(false)

	ctx := context.Background()
	workspaceID := "workspace-lazy-pool"
	poolState := &compute.PoolState{
		Name:     "stored-pool",
		Selector: "private-cpu",
		Config: &pb.PoolConfig{
			Name:     "stored-pool",
			Selector: "private-cpu",
			Mode:     string(types.PoolModePrivate),
			Priority: 1000,
		},
		Mode:     string(types.PoolModePrivate),
		Priority: 1000,
	}
	assert.NoError(t, wb.computeRepo.SavePoolState(ctx, workspaceID, poolState))

	now := time.Now()
	assert.NoError(t, wb.computeRepo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
		TokenHash:          "agent-token",
		WorkspaceID:        workspaceID,
		PoolName:           poolState.Name,
		MachineID:          "machine-1",
		MachineFingerprint: "machine-1",
		Hostname:           "agent-machine",
		OS:                 "linux",
		Arch:               "amd64",
		CPUCount:           4,
		CPUMillicores:      4000,
		MemoryMB:           8192,
		Executor:           types.DefaultAgentWorkerContainerMode,
		Schedulable:        true,
		CreatedAt:          now,
		LastJoinAt:         now,
		LastHeartbeatAt:    now,
	}, time.Hour))

	request := &types.ContainerRequest{
		WorkspaceId:  workspaceID,
		PoolSelector: poolState.Selector,
		Cpu:          1000,
		Memory:       1000,
	}

	controllers, err := wb.getControllers(request)
	assert.NoError(t, err)
	assert.Len(t, controllers, 1)
	if len(controllers) == 1 {
		assert.Equal(t, poolState.Selector, controllers[0].Name())
	}
	assert.NoError(t, wb.EnsureAgentMachine(workspaceID, poolState, "machine-1"))
	worker, err := wb.workerRepo.GetWorkerById(compute.AgentMachineWorkerID("machine-1"))
	assert.NoError(t, err)
	assert.Equal(t, poolState.Name, worker.PoolName)
	assert.Equal(t, poolState.Selector, worker.PoolSelector)
	assert.NoError(t, wb.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusAvailable))
	selected, err := wb.selectWorker(request)
	assert.NoError(t, err)
	assert.Equal(t, worker.Id, selected.Id)

	shouldSanitize, err := wb.privateBacklogRequest(request)
	assert.NoError(t, err)
	assert.True(t, shouldSanitize)
}

func TestSelectPrivateAgentWorkerIgnoresStaleMachine(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.NoError(t, err)
	wb.workerPoolManager = NewWorkerPoolManager(false)

	ctx := context.Background()
	workspaceID := "workspace-private-select"
	poolState := &compute.PoolState{
		Name:     "stored-private-pool",
		Selector: "private-cpu",
		Config: &pb.PoolConfig{
			Name:     "stored-private-pool",
			Selector: "private-cpu",
			Mode:     string(types.PoolModePrivate),
			Priority: 1000,
		},
		Mode:     string(types.PoolModePrivate),
		Priority: 1000,
	}
	assert.NoError(t, wb.computeRepo.SavePoolState(ctx, workspaceID, poolState))

	now := time.Now()
	assert.NoError(t, wb.computeRepo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
		TokenHash:       "live-agent-token",
		WorkspaceID:     workspaceID,
		PoolName:        poolState.Name,
		MachineID:       "live-machine",
		Hostname:        "live-agent",
		OS:              "linux",
		Arch:            "amd64",
		CPUMillicores:   1000,
		MemoryMB:        2048,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		CreatedAt:       now,
		LastJoinAt:      now,
		LastHeartbeatAt: now,
	}, time.Hour))
	assert.NoError(t, wb.computeRepo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
		TokenHash:       "stale-agent-token",
		WorkspaceID:     workspaceID,
		PoolName:        poolState.Name,
		MachineID:       "stale-machine",
		Hostname:        "stale-agent",
		OS:              "linux",
		Arch:            "amd64",
		CPUMillicores:   8000,
		MemoryMB:        16384,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		CreatedAt:       now.Add(-2 * compute.AgentHeartbeatTimeout),
		LastJoinAt:      now.Add(-2 * compute.AgentHeartbeatTimeout),
		LastHeartbeatAt: now.Add(-2 * compute.AgentHeartbeatTimeout),
	}, time.Hour))

	assert.NoError(t, wb.EnsureAgentMachine(workspaceID, poolState, "live-machine"))
	assert.NoError(t, wb.workerRepo.UpdateWorkerStatus(compute.AgentMachineWorkerID("live-machine"), types.WorkerStatusAvailable))
	assert.NoError(t, wb.workerRepo.AddWorker(&types.Worker{
		Id:                   compute.AgentMachineWorkerID("stale-machine"),
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             8000,
		TotalMemory:          16384,
		FreeCpu:              8000,
		FreeMemory:           16384,
		PoolName:             poolState.Selector,
		MachineId:            "stale-machine",
		RequiresPoolSelector: true,
		Priority:             1000,
	}))

	worker, err := wb.selectWorker(&types.ContainerRequest{
		WorkspaceId:  workspaceID,
		PoolSelector: poolState.Selector,
		Cpu:          1000,
		Memory:       1000,
	})
	assert.NoError(t, err)
	assert.Equal(t, compute.AgentMachineWorkerID("live-machine"), worker.Id)
}

func TestSelectGPUWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorker := &types.Worker{
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   1250,
		FreeGpuCount: 1,
		Gpu:          "A10G",
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
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2500,
		Gpu:        "",
	}

	newWorker2 := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1250,
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
		Id:           uuid.New().String(),
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   1250,
		FreeGpuCount: 1,
		Gpu:          "A10G",
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
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)

}

func TestSelectWorkerIgnoresPendingWorkers(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	worker := &types.Worker{
		Id:         uuid.New().String(),
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1250,
		Gpu:        "",
	}

	err = wb.workerRepo.AddWorker(worker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	_, err = wb.selectWorker(request)
	assert.Equal(t, &types.ErrNoSuitableWorkerFound{}, err)

	err = wb.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusAvailable)
	assert.Nil(t, err)

	selectedWorker, err := wb.selectWorker(request)
	assert.Nil(t, err)
	assert.Equal(t, worker.Id, selectedWorker.Id)
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
					Id:           uuid.New().String(),
					Status:       types.WorkerStatusAvailable,
					FreeCpu:      1000,
					FreeMemory:   1250,
					FreeGpuCount: 1,
					Gpu:          gpu,
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

func TestSelectorBoundAnyGPUAcceptsUncataloguedHardware(t *testing.T) {
	worker := &types.Worker{
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              1000,
		FreeMemory:           1250,
		FreeGpuCount:         1,
		Gpu:                  "NVIDIA GeForce RTX 3080 Ti",
		PoolName:             "my-pool",
		RequiresPoolSelector: true,
	}
	request := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1000,
		GpuRequest:   []string{string(types.GPU_ANY)},
		GpuCount:     1,
		PoolSelector: "my-pool",
	}

	assert.Equal(t, []*types.Worker{worker}, filterWorkersByResources([]*types.Worker{worker}, request))

	request.PoolSelector = ""
	assert.Empty(t, filterWorkersByResources([]*types.Worker{worker}, request))
}

func TestSelectGPUWorkerDoesNotMutatePriority(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	a10gWorker := &types.Worker{
		Id:           uuid.New().String(),
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   1250,
		FreeGpuCount: 1,
		Gpu:          "A10G",
		Priority:     7,
	}
	t4Worker := &types.Worker{
		Id:           uuid.New().String(),
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   1250,
		FreeGpuCount: 1,
		Gpu:          "T4",
		Priority:     7,
	}

	request := &types.ContainerRequest{
		Cpu:        1000,
		Memory:     1000,
		GpuRequest: []string{"T4", "A10G"},
	}

	worker, err := wb.selectWorkerFromWorkers([]*types.Worker{a10gWorker, t4Worker}, request)
	assert.Nil(t, err)
	assert.Equal(t, t4Worker.Id, worker.Id)
	assert.Equal(t, int32(7), a10gWorker.Priority)
	assert.Equal(t, int32(7), t4Worker.Priority)
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
	assert.Equal(t, int64(2000)-capacityMemoryForScheduling(firstRequest), updatedWorker.FreeMemory)
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

func TestServerlessPoolFallsBackWhenHighPriorityPoolIsFull(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	defaultWorker := &types.Worker{
		Id:         "worker-default",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2500,
		PoolName:   "default",
		Priority:   100,
	}

	gladiatorWorker := &types.Worker{
		Id:         "worker-gladiator",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2500,
		PoolName:   "gladiator",
		Priority:   1000,
	}

	err = wb.workerRepo.AddWorker(defaultWorker)
	assert.Nil(t, err)

	err = wb.workerRepo.AddWorker(gladiatorWorker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		Cpu:    1000,
		Memory: 1000,
		Gpu:    "",
	}

	// Unselected serverless work prefers gladiator while it has capacity.
	worker, err := wb.selectWorker(request)
	assert.Nil(t, err)
	assert.Equal(t, gladiatorWorker.Id, worker.Id)

	err = wb.scheduleRequest(worker, request)
	assert.Nil(t, err)

	secondRequest := &types.ContainerRequest{
		Cpu:    2000,
		Memory: 2000,
		Gpu:    "",
	}

	// Gladiator now has only 1000m free, so the request falls through to the
	// lower-priority default serverless pool.
	worker, err = wb.selectWorker(secondRequest)
	assert.Nil(t, err)
	assert.Equal(t, defaultWorker.Id, worker.Id)
}

func TestSelectBuildWorker(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	assert.NotNil(t, wb)

	newWorker := &types.Worker{
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              2000,
		FreeMemory:           2500,
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
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
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
