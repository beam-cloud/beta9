package scheduler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	k8stesting "k8s.io/client-go/testing"
)

func testWorkspaceWithStorage() types.Workspace {
	id := uint(1)
	return types.Workspace{Storage: &types.WorkspaceStorage{Id: &id}}
}

// inline runs the scheduler's background work on the calling goroutine, so
// that a scheduling pass is complete when it returns.
func inline(fn func()) { fn() }

// run schedules the attempt's request as a batch of one, on the calling
// goroutine.
func (a *schedulingAttempt) run() {
	a.scheduler.spawn = inline
	a.scheduler.processRequestBatch([]*types.ContainerRequest{a.request}, a.workers)
}

// selectWorker picks a worker for the request from the workers on record.
func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	return s.selectWorkerFromWorkers(workers, request)
}

// scheduleRequest places the request on the worker, as dispatching a batch
// does with each request it planned.
func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	s.leaveWaitlist(request)
	return s.pushWorkerRequests(worker, []*types.ContainerRequest{s.prepareWorkerRequest(worker, request)})
}

// nextContainerRequest takes the next request queued on the worker, as the
// worker does, or nil when none is queued.
func nextContainerRequest(workers repo.WorkerRepository, workerId string) (*types.ContainerRequest, error) {
	requests, err := workers.GetNextContainerRequests(workerId, 1)
	if err != nil || len(requests) == 0 {
		return nil, err
	}
	return requests[0], nil
}

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

	workerPoolManager := NewWorkerPoolManager()
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
		workerProvisioningLock:    common.NewRedisLock(rdb),
		workerProvisioningBackoff: newWorkerProvisioningBackoff(),
		credentials:               newSchedulerCredentialCache(),
		waitlist:                  NewWaitlist(rdb, nil),
	}, nil
}

func setPendingSchedulerRequests(t *testing.T, scheduler *Scheduler, requests ...*types.ContainerRequest) {
	t.Helper()

	for _, request := range requests {
		if request.ContainerId == "" {
			request.ContainerId = uuid.NewString()
		}
		assert.NoError(t, scheduler.containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
			ContainerId: request.ContainerId,
			Status:      types.ContainerStatusPending,
			WorkspaceId: request.WorkspaceId,
			ScheduledAt: time.Now().Unix(),
		}))
	}
}

func TestPrivateWorkerRequestUsesPoolMode(t *testing.T) {
	manager := NewWorkerPoolManager()
	privateState := &compute.PoolState{Selector: "private-pool"}
	manager.SetPoolAt(agentPoolControllerKey("workspace-1", privateState), "private-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	manager.SetPool("serverless-pool", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, nil)
	manager.SetPool("local-pool", types.WorkerPoolConfig{Mode: types.PoolModeLocal}, nil)
	scheduler := &Scheduler{workerPoolManager: manager}

	request := &types.ContainerRequest{WorkspaceId: "workspace-1"}
	assert.True(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "private-pool"}, request))
	assert.False(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "serverless-pool", ControlPlaneManaged: true}, request))
	assert.False(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "local-pool"}, request))
	assert.False(t, scheduler.privateWorkerRequest(&types.Worker{PoolName: "missing-pool"}, request))
}

func TestPrepareWorkerRequestCredentialsByPoolMode(t *testing.T) {
	manager := NewWorkerPoolManager()
	privateState := &compute.PoolState{Selector: "private-pool"}
	manager.SetPoolAt(agentPoolControllerKey("workspace-1", privateState), "private-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	manager.SetPool("serverless-pool", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, nil)
	manager.SetPool("provider-pool", types.WorkerPoolConfig{Mode: types.PoolModeProvider}, nil)
	scheduler := &Scheduler{
		workerPoolManager: manager,
		credentials:       newSchedulerCredentialCache(),
	}
	stubConfig, err := json.Marshal(types.StubConfigV1{
		Secrets: []types.Secret{{Name: "SECRET"}},
	})
	assert.NoError(t, err)
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		WorkspaceId: "workspace-1",
		Env:         []string{"BETA9_TOKEN=inline-token", "SECRET=inline-secret", "SAFE=value"},
		Stub: types.StubWithRelated{
			Stub: types.Stub{Config: string(stubConfig)},
		},
	}

	privateRequest := scheduler.prepareWorkerRequest(
		&types.Worker{PoolName: "private-pool", PoolSelector: "private-pool"},
		request,
	)
	assert.Equal(t, []string{"SAFE=value"}, privateRequest.Env)
	assert.Equal(t, []string{"SECRET"}, privateRequest.RuntimeSecretNames)
	assert.True(t, privateRequest.RuntimeTokenRequired)

	providerRequest := scheduler.prepareWorkerRequest(
		&types.Worker{PoolName: "provider-pool", PoolSelector: "provider-pool"},
		request,
	)
	assert.Equal(t, []string{"SAFE=value"}, providerRequest.Env)
	assert.Equal(t, []string{"SECRET"}, providerRequest.RuntimeSecretNames)
	assert.True(t, providerRequest.RuntimeTokenRequired)

	serverlessRequest := scheduler.prepareWorkerRequest(
		&types.Worker{PoolName: "serverless-pool", PoolSelector: "serverless-pool", ControlPlaneManaged: true},
		request,
	)
	assert.Equal(t, request.Env, serverlessRequest.Env)
	assert.Empty(t, serverlessRequest.RuntimeSecretNames)
	assert.False(t, serverlessRequest.RuntimeTokenRequired)
}

func TestPrivatePoolRequestsBypassManagedQuotaLookup(t *testing.T) {
	manager := NewWorkerPoolManager()
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
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	scheduler.workerPoolManager.SetPool("private-gpu-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate, GPUType: "H100"}, nil)
	request := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1024,
		GpuRequest:   []string{"H100"},
		GpuCount:     8,
		PoolSelector: "private-gpu-pool",
		Workspace:    testWorkspaceWithStorage(),
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

func TestDockerEnabledRequestRuntimeSelection(t *testing.T) {
	request := &types.ContainerRequest{DockerEnabled: true}

	assert.Equal(t, []string{"gvisor"}, poolsAccepting(request, map[string]types.WorkerPoolConfig{
		"runc":   {ContainerRuntime: types.ContainerRuntimeRunc.String()},
		"gvisor": {ContainerRuntime: types.ContainerRuntimeGvisor.String()},
	}))

	workers := []*types.Worker{
		{Id: "runc-worker", Runtime: types.ContainerRuntimeRunc.String()},
		{Id: "gvisor-worker", Runtime: types.ContainerRuntimeGvisor.String()},
	}

	filteredWorkers := filterWorkersByResources(workers, request)
	assert.Equal(t, workers[1:], filteredWorkers)
}

// The microvm runtime is opt-in in both directions: only use_vm sandboxes
// reach it, and use_vm sandboxes reach nothing else.
func TestMicroVMRuntimeIsOptInBothWays(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"runc":    {ContainerRuntime: types.ContainerRuntimeRunc.String()},
		"gvisor":  {ContainerRuntime: types.ContainerRuntimeGvisor.String()},
		"microvm": {ContainerRuntime: types.ContainerRuntimeMicroVM.String()},
	}
	microvm := map[string]types.WorkerPoolConfig{"microvm": pools["microvm"]}
	gpuMicrovm := map[string]types.WorkerPoolConfig{
		"microvm": {GPUType: "RTX4090", ContainerRuntime: types.ContainerRuntimeMicroVM.String()},
	}

	worker := func(id, runtimeName string) *types.Worker {
		return &types.Worker{Id: id, Status: types.WorkerStatusAvailable, FreeCpu: 4000, FreeMemory: 8000, Runtime: runtimeName}
	}
	runcWorker := worker("runc-worker", types.ContainerRuntimeRunc.String())
	gvisorWorker := worker("gvisor-worker", types.ContainerRuntimeGvisor.String())
	microvmWorker := worker("microvm-worker", types.ContainerRuntimeMicroVM.String())
	workers := []*types.Worker{runcWorker, gvisorWorker, microvmWorker}

	sandbox := func(useVM bool) *types.ContainerRequest {
		request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, UseVM: useVM}
		request.Stub.Type = types.StubType(types.StubTypeSandbox)
		return request
	}

	// A plain sandbox never sees VM capacity, even when it is the only idle worker.
	assert.Equal(t, []string{"gvisor", "runc"}, poolsAccepting(sandbox(false), pools))
	assert.Equal(t, []*types.Worker{runcWorker, gvisorWorker}, filterWorkersByResources(workers, sandbox(false)))
	assert.Equal(t, []*types.Worker{}, filterWorkersByResources([]*types.Worker{microvmWorker}, sandbox(false)))

	// use_vm goes only to microvm, and fails closed when there is none.
	assert.Equal(t, []string{"microvm"}, poolsAccepting(sandbox(true), pools))
	assert.Equal(t, []*types.Worker{microvmWorker}, filterWorkersByResources(workers, sandbox(true)))
	assert.Equal(t, []*types.Worker{}, filterWorkersByResources([]*types.Worker{runcWorker, gvisorWorker}, sandbox(true)))

	// Persistent VMs retain their own kind while using the same placement.
	vm := sandbox(true)
	vm.Stub.Type = types.StubType(types.StubTypeVM)
	assert.Equal(t, []string{"microvm"}, poolsAccepting(vm, pools))
	assert.Equal(t, []*types.Worker{microvmWorker}, filterWorkersByResources(workers, vm))
	vm.UseVM = false
	assert.Empty(t, poolsAccepting(vm, pools), "VMs must never fall back to a container runtime")
	assert.Empty(t, filterWorkersByResources(workers, vm))

	// GPU VMs use the same model and free-device accounting as containers.
	gpu := sandbox(true)
	gpu.Gpu = "RTX4090"
	gpu.GpuCount = 1
	assert.Equal(t, []string{"microvm"}, poolsAccepting(gpu, gpuMicrovm))
	gpuWorker := worker("gpu-worker", types.ContainerRuntimeMicroVM.String())
	gpuWorker.Gpu, gpuWorker.FreeGpuCount = "RTX4090", 1
	assert.Equal(t, []*types.Worker{gpuWorker}, filterWorkersByResources(append(workers, gpuWorker), gpu))
	gpuWorker.FreeGpuCount = 0
	assert.Empty(t, filterWorkersByResources([]*types.Worker{gpuWorker}, gpu))
	gpuWorker.FreeGpuCount, gpuWorker.Gpu = 1, "A100"
	assert.Empty(t, newRoute(gpu, nil, "").eligible([]*types.Worker{gpuWorker}))
	gpu.GpuCount = 2
	assert.Empty(t, poolsAccepting(gpu, gpuMicrovm))

	checkpointing := sandbox(true)
	checkpointing.CheckpointEnabled = true
	assert.Len(t, poolsAccepting(checkpointing, microvm), 1, "VM sandboxes snapshot through the runtime's own checkpoint support")

	restoring := sandbox(true)
	restoring.Checkpoint = &types.Checkpoint{CheckpointId: "c1", Status: string(types.CheckpointStatusAvailable), Runtime: types.ContainerRuntimeGvisor.String()}
	assert.Empty(t, poolsAccepting(restoring, microvm))

	pod := &types.ContainerRequest{Cpu: 1000, Memory: 1024, UseVM: true}
	pod.Stub.Type = types.StubType(types.StubTypePodRun)
	assert.Empty(t, poolsAccepting(pod, microvm))
	assert.Empty(t, filterWorkersByResources([]*types.Worker{microvmWorker}, pod))
}

func buildPoolSchedulerForTest(poolName string) *Scheduler {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	scheduler.config.ImageService.BuildContainerPoolSelector = poolName
	for _, name := range []string{poolName, "regular"} {
		scheduler.workerPoolManager.SetPool(name, types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{name: name})
	}
	return scheduler
}

func TestBuildPoolRejectsOrdinaryRunRequests(t *testing.T) {
	for _, poolName := range []string{"build", "custom-build"} {
		t.Run(poolName, func(t *testing.T) {
			scheduler, err := NewSchedulerForTest()
			require.NoError(t, err)
			scheduler.config.ImageService.BuildContainerPoolSelector = poolName
			request := &types.ContainerRequest{ContainerId: "sandbox-1", PoolSelector: poolName}
			request.Stub.Type = types.StubType(types.StubTypeSandbox)

			require.EqualError(t, scheduler.Run(request), "pool does not support this request")
			require.Zero(t, scheduler.requestBacklog.Len())
			_, err = scheduler.containerRepo.GetContainerState(request.ContainerId)
			require.Error(t, err)
		})
	}
}

func TestBuildPoolAdmitsBuildRunRequests(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	scheduler.config.ImageService.BuildContainerPoolSelector = "beta9-build"
	dockerfile := "FROM python:3.12"
	request := &types.ContainerRequest{
		ContainerId: "build-1", PoolSelector: "beta9-build",
		BuildOptions: types.BuildOptions{Dockerfile: &dockerfile},
	}
	require.NoError(t, scheduler.Run(request))
	require.EqualValues(t, 1, scheduler.requestBacklog.Len())
	state, err := scheduler.containerRepo.GetContainerState(request.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusPending, state.Status)
}

func TestBuildPoolControllerPlacement(t *testing.T) {
	for _, poolName := range []string{"build", "custom-build"} {
		t.Run(poolName, func(t *testing.T) {
			scheduler := buildPoolSchedulerForTest(poolName)
			request := &types.ContainerRequest{PoolSelector: poolName}
			controllers, err := scheduler.getControllers(request)
			require.Error(t, err)
			require.Empty(t, controllers)

			request.PoolSelector = ""
			controllers, err = scheduler.getControllers(request)
			require.NoError(t, err)
			require.Len(t, controllers, 1)
			require.Equal(t, "regular", controllers[0].Name())

			request.PoolSelector = poolName
			sourceImage := "python:3.12"
			dockerfile := "FROM python:3.12"
			for _, options := range []types.BuildOptions{{SourceImage: &sourceImage}, {Dockerfile: &dockerfile}, {GitSource: &types.GitSource{}}} {
				request.BuildOptions = options
				controllers, err = scheduler.getControllers(request)
				require.NoError(t, err)
				require.Len(t, controllers, 1)
				require.Equal(t, poolName, controllers[0].Name())
			}
		})
	}
}

func TestBuildPoolWorkerPlacement(t *testing.T) {
	for _, poolName := range []string{"build", "custom-build"} {
		t.Run(poolName, func(t *testing.T) {
			scheduler := buildPoolSchedulerForTest(poolName)
			worker := &types.Worker{
				Id: "build-worker", MachineId: "machine-1", PoolName: poolName,
				Status: types.WorkerStatusAvailable, FreeCpu: 2000, FreeMemory: 2000,
			}
			for _, status := range []types.WorkerStatus{types.WorkerStatusAvailable, types.WorkerStatusPending} {
				worker.Status = status
				for _, request := range []*types.ContainerRequest{
					{PoolSelector: poolName},
					{},
					{MachineId: worker.MachineId},
				} {
					selected, err := scheduler.selectWorkerFromWorkersByStatus([]*types.Worker{worker}, request, status)
					require.Error(t, err)
					require.Nil(t, selected)
				}
			}
			worker.Status = types.WorkerStatusAvailable

			dockerfile := "FROM python:3.12"
			request := &types.ContainerRequest{PoolSelector: poolName, BuildOptions: types.BuildOptions{Dockerfile: &dockerfile}}
			selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
			require.NoError(t, err)
			require.Equal(t, worker.Id, selected.Id)

			worker.PoolName = "regular"
			worker.PoolSelector = poolName
			selected, err = scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, &types.ContainerRequest{PoolSelector: poolName, Workspace: testWorkspaceWithStorage()})
			require.Error(t, err)
			require.Nil(t, selected)
		})
	}
}

func TestBuildPoolTakesNoOverflow(t *testing.T) {
	scheduler := buildPoolSchedulerForTest("build")
	scheduler.workerPoolManager.SetPool("build", types.WorkerPoolConfig{
		GPUType: "RTX4090", RequiresPoolSelector: true, AlsoServes: []string{"A10G"},
	}, &LocalWorkerPoolControllerForTest{name: "build", requiresSelector: true})
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1}
	controllers, err := scheduler.getControllers(request)
	require.Error(t, err)
	require.Empty(t, controllers)
	selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{gpuWorkerForTest("build-worker", "build", "RTX4090", 1)}, request)
	require.Error(t, err)
	require.Nil(t, selected)
}

func TestBuildPoolLeavesOtherRuncPoolsAvailable(t *testing.T) {
	for _, test := range []struct {
		name     string
		stubType string
		gpu      string
		selector string
	}{
		{name: "cpu sandbox", stubType: types.StubTypeSandbox},
		{name: "selected cpu function", stubType: types.StubTypeFunction, selector: "default-ovh"},
		{name: "selected cpu pod", stubType: types.StubTypePodRun, selector: "default-ovh"},
		{name: "cpu endpoint", stubType: types.StubTypeEndpointDeployment},
		{name: "gpu function", stubType: types.StubTypeFunction, gpu: "A10G"},
		{name: "selected gpu sandbox", stubType: types.StubTypeSandbox, gpu: "A10G", selector: "default-ovh"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduler := buildPoolSchedulerForTest("build")
			controller := &LocalWorkerPoolControllerForTest{name: "default-ovh", requiresSelector: test.selector != "", containerRuntime: types.ContainerRuntimeRunc.String()}
			scheduler.workerPoolManager.SetPool(controller.Name(), types.WorkerPoolConfig{
				GPUType: test.gpu, ContainerRuntime: types.ContainerRuntimeRunc.String(), RequiresPoolSelector: test.selector != "",
			}, controller)
			request := &types.ContainerRequest{PoolSelector: test.selector, Cpu: 1000, Memory: 1024, Gpu: test.gpu}
			request.Stub.Type = types.StubType(test.stubType)
			if test.gpu != "" {
				request.GpuCount = 1
			}
			controllers, err := scheduler.getControllers(request)
			require.NoError(t, err)
			require.Contains(t, controllers, controller)

			worker := &types.Worker{
				Id: "ovh-worker", PoolName: controller.Name(), RequiresPoolSelector: test.selector != "",
				Runtime: types.ContainerRuntimeRunc.String(), Status: types.WorkerStatusAvailable,
				FreeCpu: 2000, FreeMemory: 4096, Gpu: test.gpu, FreeGpuCount: 1,
			}
			selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
			require.NoError(t, err)
			require.Equal(t, worker.Id, selected.Id)
		})
	}
}

// Provider pools (workspace-supplied machines) only run platform-owned managed
// endpoint replicas; ordinary requests never reach them, even by selector.
func TestProviderPoolsOnlyAdmitManagedRequests(t *testing.T) {
	provider := types.WorkerPoolConfig{Mode: types.PoolModeProvider, RequiresPoolSelector: true}
	pools := map[string]types.WorkerPoolConfig{"provider": provider, "regular": {Mode: types.PoolModeLocal}}
	workspace := testWorkspaceWithStorage()

	assert.Equal(t, []string{"regular"}, poolsAccepting(&types.ContainerRequest{Workspace: workspace}, pools))
	assert.Empty(t, poolsAccepting(&types.ContainerRequest{PoolSelector: "provider", Workspace: workspace}, pools))

	managed := &types.ContainerRequest{PoolSelector: "provider", Workspace: workspace}
	managed.Stub.Type = types.StubType(types.StubTypeManagedEndpointDeployment)
	assert.Equal(t, []string{"provider"}, poolsAccepting(managed, pools))

	// Provider capacity gets no preemption exception.
	provider.Preemptable = true
	assert.Empty(t, poolsAccepting(managed, map[string]types.WorkerPoolConfig{"provider": provider}))
}

func TestProviderWorkersOnlyAdmitManagedRequests(t *testing.T) {
	providerWorker := &types.Worker{
		Id:                   "provider-worker",
		PoolName:             "provider",
		PoolSelector:         "provider",
		RequiresPoolSelector: true,
		WorkspaceId:          "provider-workspace",
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             1000,
		FreeCpu:              1000,
		TotalMemory:          1024,
		FreeMemory:           1024,
		TotalGpuCount:        1,
		FreeGpuCount:         1,
		Gpu:                  "A10G",
	}
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	scheduler.workerPoolManager.SetPool("provider", types.WorkerPoolConfig{
		Mode:                 types.PoolModeProvider,
		GPUType:              "A10G",
		RequiresPoolSelector: true,
	}, nil)

	baseRequest := &types.ContainerRequest{Cpu: 1000, Memory: 512, GpuRequest: []string{"A10G"}, GpuCount: 1, WorkspaceId: "admin-workspace", Workspace: testWorkspaceWithStorage()}

	// Ordinary requests never see provider capacity, with or without a selector.
	_, err := scheduler.selectWorkerFromWorkers([]*types.Worker{providerWorker}, baseRequest.Clone())
	assert.Error(t, err)

	selected := baseRequest.Clone()
	selected.PoolSelector = "provider"
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{providerWorker}, selected)
	assert.Error(t, err)

	// A managed endpoint replica addressed to the pool is admitted.
	managed := selected.Clone()
	managed.Stub.Type = types.StubType(types.StubTypeManagedEndpointDeployment)
	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{providerWorker}, managed)
	assert.NoError(t, err)
	assert.Equal(t, providerWorker.Id, worker.Id)

	// Managed replicas still need the selector: provider pools are opt-in only.
	unselected := managed.Clone()
	unselected.PoolSelector = ""
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{providerWorker}, unselected)
	assert.Error(t, err)
}

func TestNormalizeProviderAgentPool(t *testing.T) {
	config := normalizeAgentWorkerPoolConfig(&compute.PoolState{
		Mode:   string(types.PoolModeProvider),
		Config: &pb.PoolConfig{Gpu: []string{"A10G"}},
	})

	assert.Equal(t, types.PoolModeProvider, config.Mode)
	assert.Equal(t, types.ContainerRuntimeRunc.String(), config.ContainerRuntime)
	assert.True(t, config.RequiresPoolSelector)
	assert.False(t, config.Preemptable)
	assert.Equal(t, int32(100), config.Priority)
	assert.Equal(t, "A10G", config.GPUType)
	assert.Equal(t, types.WorkerPoolManagedEndpointsConfig{Enabled: true}, config.ManagedEndpoints)

	// Provider pools are addressed globally by pool name, not per workspace.
	assert.Equal(t, "provider-a10g", agentPoolControllerKey("provider-workspace", &compute.PoolState{
		Mode: string(types.PoolModeProvider),
		Name: "provider-a10g",
	}))
}

func TestEnsureAgentPoolNormalizesPersistedManagedConfig(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	err = scheduler.EnsureAgentPool("admin-workspace", &compute.PoolState{
		Name:             "api-pool",
		Selector:         "api-pool",
		ManagementSource: types.WorkerPoolManagementSourceAPI,
		WorkerConfig: &types.WorkerPoolConfig{
			Mode:     types.PoolModeLocal,
			Priority: 7,
			GPUType:  "A10G",
		},
	})
	assert.NoError(t, err)

	pool, ok := scheduler.workerPoolManager.GetPool("api-pool")
	assert.True(t, ok)
	assert.Equal(t, types.PoolModeExternal, pool.Config.Mode)
	assert.Equal(t, types.ContainerRuntimeRunc.String(), pool.Config.ContainerRuntime)
	assert.Equal(t, int32(7), pool.Config.Priority)
}

func TestEnsureAgentPoolReconcilesExistingMachineWorkers(t *testing.T) {
	s, err := NewSchedulerForTest()
	assert.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()

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
	scheduler.workerPoolManager = NewWorkerPoolManager()
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
	scheduler.workerPoolManager = NewWorkerPoolManager()

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
	placed := &types.ContainerRequest{ContainerId: "container-private", Cpu: 100, Memory: 100}
	setPendingSchedulerRequests(t, scheduler, placed)
	assert.NoError(t, scheduler.workerRepo.ScheduleContainerRequests(worker, []*types.ContainerRequest{placed}))
	worker, err = scheduler.workerRepo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	version := worker.ResourceVersion
	assert.NotZero(t, version)

	_, err = scheduler.ensureAgentPoolForRequest(request)
	assert.NoError(t, err)
	worker, err = scheduler.workerRepo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, version, worker.ResourceVersion)
}

func TestFilterWorkersByMachinePinsWorker(t *testing.T) {
	workers := []*types.Worker{
		{Id: "w1", MachineId: "machine-1", PoolName: "provider-a100", RequiresPoolSelector: true},
		{Id: "w2", MachineId: "machine-2", PoolName: "private-pool", RequiresPoolSelector: true},
	}

	pinned := filterWorkersByMachine(workers, &types.ContainerRequest{MachineId: "machine-2"})
	assert.Len(t, pinned, 1)
	assert.Equal(t, "w2", pinned[0].Id)

	// The pin overrides pool-selector filtering: w2 requires a selector the
	// request doesn't carry, but it was already selected by the pin.
	request := &types.ContainerRequest{MachineId: "machine-2"}
	selected := newRoute(request, nil, "").eligible(pinned)
	assert.Len(t, selected, 1)

	unpinned := filterWorkersByMachine(workers, &types.ContainerRequest{})
	assert.Len(t, unpinned, 2)
}

func TestFilterWorkersByWorkspaceScope(t *testing.T) {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	scheduler.workerPoolManager.SetPool("provider", types.WorkerPoolConfig{Mode: types.PoolModeProvider}, nil)

	workers := []*types.Worker{
		{Id: "local"},
		{Id: "owned-private", WorkspaceId: "workspace-1"},
		{Id: "foreign-private", WorkspaceId: "workspace-2"},
		{Id: "managed", WorkspaceId: "admin-workspace", ControlPlaneManaged: true},
		{Id: "provider", WorkspaceId: "provider-workspace", PoolName: "provider"},
	}

	// Provider workers are foreign capacity for ordinary requests...
	filtered := scheduler.filterWorkersByWorkspaceScope(workers, &types.ContainerRequest{WorkspaceId: "workspace-1"})
	assert.Equal(t, []*types.Worker{workers[0], workers[1], workers[3]}, filtered)

	// ...but global for managed endpoint replicas.
	managed := &types.ContainerRequest{WorkspaceId: "workspace-1"}
	managed.Stub.Type = types.StubType(types.StubTypeManagedEndpointDeployment)
	filtered = scheduler.filterWorkersByWorkspaceScope(workers, managed)
	assert.Equal(t, []*types.Worker{workers[0], workers[1], workers[3], workers[4]}, filtered)
}

func TestSelectorCannotScheduleForeignPrivateWorker(t *testing.T) {
	scheduler := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	worker := &types.Worker{
		Id:                   "private-worker",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              2000,
		FreeMemory:           2500,
		PoolName:             "private-pool",
		PoolSelector:         "private-pool",
		RequiresPoolSelector: true,
		WorkspaceId:          "workspace-owner",
	}
	request := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-pool",
		WorkspaceId:  "workspace-other",
		Workspace:    testWorkspaceWithStorage(),
	}

	_, err := scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
	var noWorker *types.ErrNoSuitableWorkerFound
	assert.Error(t, err)
	assert.True(t, errors.As(err, &noWorker))

	request.WorkspaceId = worker.WorkspaceId
	selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
	assert.NoError(t, err)
	assert.Equal(t, worker, selected)

	request.WorkspaceId = "workspace-other"
	worker.ControlPlaneManaged = true
	selected, err = scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
	assert.NoError(t, err)
	assert.Equal(t, worker, selected)
}

type LocalWorkerPoolControllerForTest struct {
	ctx              context.Context
	name             string
	config           types.AppConfig
	workerRepo       repo.WorkerRepository
	preemptable      bool
	workerStatus     types.WorkerStatus
	addWorkerStarted chan struct{}
	addWorkerDone    chan struct{}
	unblockAddWorker chan struct{}
	addWorkerErr     error
	// cannotAddWorker and canAddWorkerErr are what CanAddWorker answers.
	cannotAddWorker  bool
	canAddWorkerErr  error
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

func (wpc *LocalWorkerPoolControllerForTest) RequiresPoolSelector() bool {
	if wpc.requiresSelector {
		return true
	}
	pool, ok := wpc.config.Worker.Pools[wpc.name]
	return ok && pool.RequiresPoolSelector
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

func (wpc *LocalWorkerPoolControllerForTest) CanAddWorker(cpu int64, memory int64, gpuCount uint32) (bool, error) {
	return !wpc.cannotAddWorker, wpc.canAddWorkerErr
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
		Runtime:              wpc.ContainerRuntime(),
		Status:               status,
	}

	// Add the worker state
	err := wpc.workerRepo.AddWorker(worker)
	if err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}
	if wpc.addWorkerDone != nil {
		select {
		case wpc.addWorkerDone <- struct{}{}:
		default:
		}
	}

	return worker, nil
}

func (wpc *LocalWorkerPoolControllerForTest) AddWorkerCallCount() int {
	wpc.addWorkerMu.Lock()
	defer wpc.addWorkerMu.Unlock()
	return wpc.addWorkerCalls
}

func (wpc *LocalWorkerPoolControllerForTest) Name() string {
	return wpc.name
}

func (wpc *LocalWorkerPoolControllerForTest) FreeCapacity() (*WorkerPoolCapacity, error) {
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

func TestStopDeletesUnassignedPendingContainerState(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)

	containerId := "pending-container"
	request := &types.ContainerRequest{ContainerId: containerId, Timestamp: time.Now()}
	err = wb.containerRepo.SetContainerState(containerId, &types.ContainerState{
		ContainerId: containerId,
		Status:      types.ContainerStatusPending,
		WorkspaceId: "workspace-1",
		ScheduledAt: time.Now().Unix(),
	})
	assert.Nil(t, err)
	assert.NoError(t, wb.requestBacklog.Push(request))

	err = wb.Stop(&types.StopContainerArgs{
		ContainerId: containerId,
		Reason:      types.StopContainerReasonUser,
	})
	assert.Nil(t, err)

	assertUnassignedStopRemoved(t, wb, containerId)

	popped, err := wb.requestBacklog.Pop()
	assert.NoError(t, err)
	wb.processRequestBatch([]*types.ContainerRequest{popped}, nil)
	assert.Equal(t, int64(0), wb.requestBacklog.Len())
}

func TestStopRecoversInterruptedUnassignedCancellation(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.NoError(t, err)
	id := "unassigned-stopping-container"
	assert.NoError(t, wb.containerRepo.SetContainerState(id, &types.ContainerState{ContainerId: id, Status: types.ContainerStatusStopping, WorkspaceId: "workspace-1"}))
	assert.NoError(t, wb.Stop(&types.StopContainerArgs{ContainerId: id, Reason: types.StopContainerReasonUser}))
	assertUnassignedStopRemoved(t, wb, id)
}

func TestSchedulingFailureCannotFinalizeAnAssignedVM(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	for _, assigned := range []bool{false, true} {
		id := "vm-scheduling-failure-unassigned"
		state := &types.ContainerState{ContainerId: id, Status: types.ContainerStatusPending, WorkspaceId: "workspace-1"}
		if assigned {
			id = "vm-scheduling-failure-assigned"
			state.ContainerId, state.WorkerId = id, "worker-1"
		}
		require.NoError(t, scheduler.containerRepo.SetContainerState(id, state))
		newSchedulingAttempt(scheduler, &types.ContainerRequest{ContainerId: id, Timestamp: time.Now()}, nil).fail(types.ContainerSchedulingFailureNoController)
		_, stateErr := scheduler.containerRepo.GetContainerState(id)
		code, codeErr := scheduler.containerRepo.GetContainerExitCode(id)
		if assigned {
			require.NoError(t, stateErr)
			require.Error(t, codeErr, "assigned VM was falsely certified as having no worker writes")
		} else {
			require.Error(t, stateErr)
			require.NoError(t, codeErr)
			require.Equal(t, int(types.ContainerExitCodeScheduler), code)
			ttl := scheduler.requestBacklog.rdb.PTTL(context.Background(), common.RedisKeys.SchedulerContainerExitCode(id)).Val()
			require.Equal(t, time.Duration(-1), ttl)
		}
	}
}

func assertUnassignedStopRemoved(t *testing.T, wb *Scheduler, id string) {
	t.Helper()
	_, err := wb.containerRepo.GetContainerState(id)
	assert.True(t, (&types.ErrContainerStateNotFound{}).From(err), "expected deleted pending state, got %v", err)
	code, err := wb.containerRepo.GetContainerExitCode(id)
	assert.NoError(t, err)
	assert.Equal(t, int(types.ContainerExitCodeUser), code)
}

func TestUnassignedStopPreservesReason(t *testing.T) {
	for reason, want := range map[types.StopContainerReason]types.ContainerExitCode{
		types.StopContainerReasonScheduler:           types.ContainerExitCodeScheduler,
		types.StopContainerReasonUser:                types.ContainerExitCodeUser,
		types.StopContainerReasonTtl:                 types.ContainerExitCodeTtl,
		types.StopContainerReasonAdmin:               types.ContainerExitCodeAdmin,
		types.StopContainerReasonInsufficientCredits: types.ContainerExitCodeAdmin,
		types.StopContainerReasonEvicted:             types.ContainerExitCodeEvicted,
	} {
		t.Run(string(reason), func(t *testing.T) {
			s, err := NewSchedulerForTest()
			assert.NoError(t, err)
			id := "cancelled-" + string(reason)
			assert.NoError(t, s.containerRepo.SetContainerState(id, &types.ContainerState{ContainerId: id, Status: types.ContainerStatusPending}))
			assert.NoError(t, s.Stop(&types.StopContainerArgs{ContainerId: id, Reason: reason}))
			code, err := s.containerRepo.GetContainerExitCode(id)
			assert.NoError(t, err)
			assert.Equal(t, int(want), code)
		})
	}
}

func TestStopPreservesAssignedContainerForWorkerCancellation(t *testing.T) {
	for _, status := range []types.ContainerStatus{
		types.ContainerStatusPending,
		types.ContainerStatusRunning,
	} {
		t.Run(string(status), func(t *testing.T) {
			wb, err := NewSchedulerForTest()
			assert.NoError(t, err)

			containerID := "assigned-" + string(status) + "-container"
			assert.NoError(t, wb.containerRepo.SetContainerState(containerID, &types.ContainerState{
				ContainerId: containerID,
				Status:      status,
				WorkspaceId: "workspace-1",
				WorkerId:    "worker-1",
				ScheduledAt: time.Now().Unix(),
			}))

			assert.NoError(t, wb.Stop(&types.StopContainerArgs{
				ContainerId: containerID,
				Reason:      types.StopContainerReasonUser,
			}))

			state, err := wb.containerRepo.GetContainerState(containerID)
			assert.NoError(t, err)
			assert.Equal(t, types.ContainerStatusStopping, state.Status)
			assert.Equal(t, "worker-1", state.WorkerId)
			assertContainerStateTTL(t, wb, containerID, types.ContainerStateTtlSWhileStopping)
		})
	}
}

func assertContainerStateTTL(t *testing.T, scheduler *Scheduler, containerID string, seconds int64) {
	t.Helper()
	ttl, err := scheduler.requestBacklog.rdb.TTL(
		context.Background(),
		common.RedisKeys.SchedulerContainerState(containerID),
	).Result()
	assert.NoError(t, err)
	want := time.Duration(seconds) * time.Second
	assert.True(t, ttl == want || ttl == want-time.Second,
		"container state TTL = %s, want %s (allowing one-second boundary rounding)", ttl, want)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wb.ctx = ctx

	go wb.StartProcessingRequests()

	deadline := time.After(5 * time.Second)
	for {
		allScheduled := true
		for _, request := range requests {
			state, err := wb.containerRepo.GetContainerState(request.ContainerId)
			if err != nil || state.WorkerId == "" {
				allScheduled = false
				break
			}
		}
		if allScheduled {
			return
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for all requests to be scheduled")
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

		request, err := nextContainerRequest(wb.workerRepo, worker.Id)
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

		request, err := nextContainerRequest(wb.workerRepo, fastWorker.Id)
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
	setPendingSchedulerRequests(t, wb, firstRequest, secondRequest)

	wb.processRequestBatch([]*types.ContainerRequest{firstRequest}, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected scheduler to start worker provisioning")
	}

	wb.processRequestBatch([]*types.ContainerRequest{secondRequest}, nil)

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
	setPendingSchedulerRequests(t, wb, firstRequest, secondRequest)

	firstBatchWorkers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequestBatch([]*types.ContainerRequest{firstRequest}, firstBatchWorkers)

	secondBatchWorkers, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequestBatch([]*types.ContainerRequest{secondRequest}, secondBatchWorkers)

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
	setPendingSchedulerRequests(t, wb, firstRequest)

	wb.processRequestBatch([]*types.ContainerRequest{firstRequest}, workers)

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
	setPendingSchedulerRequests(t, wb, requests...)

	wb.processRequestBatch(requests, workers)

	queued := map[string]struct{}{}
	for i := 0; i < 2; i++ {
		request, err := nextContainerRequest(wb.workerRepo, worker.Id)
		assert.Nil(t, err)
		assert.NotNil(t, request)
		queued[request.ContainerId] = struct{}{}
	}
	extraRequest, err := nextContainerRequest(wb.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.Nil(t, extraRequest)

	assert.Equal(t, 2, len(queued))
	updatedWorker, err := wb.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
}

func TestProcessRequestBatchImmediatelyReplansStaleCapacity(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.NoError(t, err)
	second := schedulerReplicaForTest(first)
	packed := &types.Worker{Id: "packed", Status: types.WorkerStatusAvailable, PoolName: "beta9-cpu", TotalCpu: 1000, TotalMemory: 1250, FreeCpu: 1000, FreeMemory: 1250}
	idle := &types.Worker{Id: "idle", Status: types.WorkerStatusAvailable, PoolName: "beta9-cpu", TotalCpu: 2000, TotalMemory: 2500, FreeCpu: 2000, FreeMemory: 2500}
	assert.NoError(t, first.workerRepo.AddWorker(packed))
	assert.NoError(t, first.workerRepo.AddWorker(idle))
	firstWorkers, err := first.workerRepo.GetAllWorkers()
	assert.NoError(t, err)
	staleWorkers, err := second.workerRepo.GetAllWorkers()
	assert.NoError(t, err)

	firstRequest := &types.ContainerRequest{ContainerId: "first", Cpu: 1000, Memory: 1000, Timestamp: time.Now()}
	secondRequest := &types.ContainerRequest{ContainerId: "second", Cpu: 1000, Memory: 1000, Timestamp: time.Now()}
	setPendingSchedulerRequests(t, first, firstRequest, secondRequest)
	first.processRequestBatch([]*types.ContainerRequest{firstRequest}, firstWorkers)
	second.processRequestBatch([]*types.ContainerRequest{secondRequest}, staleWorkers)

	// Both replicas best-fit "packed"; the loser is ready to replan at once and lands on "idle".
	requeued, err := first.requestBacklog.Pop()
	assert.NoError(t, err)
	currentWorkers, err := first.workerRepo.GetAllWorkers()
	assert.NoError(t, err)
	first.processRequestBatch([]*types.ContainerRequest{requeued}, currentWorkers)
	queued, err := nextContainerRequest(first.workerRepo, idle.Id)
	assert.NoError(t, err)
	assert.Equal(t, secondRequest.ContainerId, queued.ContainerId)
}

func TestProcessRequestBatchPacksOntoSingleWorker(t *testing.T) {
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
	setPendingSchedulerRequests(t, wb, requests...)

	wb.processRequestBatch(requests, workers)

	// Best-fit packing: both requests should land on the same worker so the
	// other worker can drain, spin down, and release its node.
	perWorker := []int{}
	for _, w := range []*types.Worker{firstWorker, secondWorker} {
		queued := 0
		for {
			request, err := nextContainerRequest(wb.workerRepo, w.Id)
			assert.Nil(t, err)
			if request == nil {
				break
			}
			queued++
		}
		perWorker = append(perWorker, queued)
	}
	assert.Equal(t, 2, perWorker[0]+perWorker[1])
	assert.Contains(t, perWorker, 2)
	assert.Contains(t, perWorker, 0)
}

func TestProcessRequestBatchEmitsContainerPlaced(t *testing.T) {
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
	assert.NoError(t, wb.workerRepo.AddWorker(worker))

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         100,
		Memory:      100,
		Timestamp:   time.Now(),
	}
	setPendingSchedulerRequests(t, wb, request)

	var events []types.EventComputeSchema
	wb.pushComputeEvent = func(event types.EventComputeSchema) {
		events = append(events, event)
	}
	workers, err := wb.workerRepo.GetAllWorkers()
	assert.NoError(t, err)

	wb.processRequestBatch([]*types.ContainerRequest{request}, workers)

	assert.Len(t, events, 1)
	if len(events) == 1 {
		assert.Equal(t, types.EventComputeActionContainerPlaced, events[0].Action)
		assert.Equal(t, request.ContainerId, events[0].ContainerID)
		assert.Equal(t, worker.PoolName, events[0].PoolName)
	}
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
	setPendingSchedulerRequests(t, wb, requests...)

	workerSnapshot, err := wb.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	wb.processRequestBatch(requests, workerSnapshot)

	// All 70 tiny requests fit on one 16-CPU worker only when scheduled by
	// their actual requested capacity; best-fit packs them onto one worker.
	queuedPerWorker := []int{}
	for _, worker := range workers {
		queued := 0
		for {
			request, err := nextContainerRequest(wb.workerRepo, worker.Id)
			assert.Nil(t, err)
			if request == nil {
				break
			}
			queued++
		}
		queuedPerWorker = append(queuedPerWorker, queued)
	}
	assert.Equal(t, 70, queuedPerWorker[0]+queuedPerWorker[1])
	assert.Contains(t, queuedPerWorker, 70)
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
	setPendingSchedulerRequests(t, wb, gpuRequest, cpuRequest)

	wb.processRequestBatch([]*types.ContainerRequest{gpuRequest, cpuRequest}, workers)

	queuedCPURequest, err := nextContainerRequest(wb.workerRepo, cpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedCPURequest)
	assert.Equal(t, cpuRequest.ContainerId, queuedCPURequest.ContainerId)
	assert.Equal(t, "", queuedCPURequest.Gpu)

	queuedGPURequest, err := nextContainerRequest(wb.workerRepo, gpuWorker.Id)
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
	setPendingSchedulerRequests(t, wb, cpuRequest, gpuRequest)

	wb.processRequestBatch([]*types.ContainerRequest{cpuRequest}, workers)
	wb.processRequestBatch([]*types.ContainerRequest{gpuRequest}, workers)

	queuedCPURequest, err := nextContainerRequest(wb.workerRepo, cpuWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedCPURequest)
	assert.Equal(t, cpuRequest.ContainerId, queuedCPURequest.ContainerId)
	assert.Equal(t, "", queuedCPURequest.Gpu)

	queuedGPURequest, err := nextContainerRequest(wb.workerRepo, gpuWorker.Id)
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
	setPendingSchedulerRequests(t, wb, request)

	wb.processRequestBatch([]*types.ContainerRequest{request}, workers)

	queuedRequest, err := nextContainerRequest(wb.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)
	assert.Equal(t, uint32(1), queuedRequest.GpuCount)
	assert.Equal(t, "A10G", queuedRequest.Gpu)

	updatedWorker, err := wb.workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
}

func schedulerReplicaForTest(source *Scheduler) *Scheduler {
	return &Scheduler{
		ctx:                       source.ctx,
		config:                    source.config,
		backendRepo:               source.backendRepo,
		workerRepo:                source.workerRepo,
		workerPoolRepo:            source.workerPoolRepo,
		computeRepo:               source.computeRepo,
		workerPoolManager:         source.workerPoolManager,
		requestBacklog:            source.requestBacklog,
		containerRepo:             source.containerRepo,
		workspaceRepo:             source.workspaceRepo,
		eventRepo:                 source.eventRepo,
		schedulerUsageMetrics:     source.schedulerUsageMetrics,
		eventBus:                  source.eventBus,
		provisioning:              newProvisioningTracker(),
		workerProvisioningLock:    common.NewRedisLock(source.requestBacklog.rdb),
		workerProvisioningBackoff: newWorkerProvisioningBackoff(),
		credentials:               newSchedulerCredentialCache(),
		waitlist:                  source.waitlist,
		pushComputeEvent:          source.pushComputeEvent,
		clock:                     source.clock,
	}
}

func TestProcessRequestStaleReplicaGPUReservationRequeues(t *testing.T) {
	firstScheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	secondScheduler := schedulerReplicaForTest(firstScheduler)

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
	setPendingSchedulerRequests(t, firstScheduler, firstRequest, secondRequest)

	firstScheduler.processRequestBatch([]*types.ContainerRequest{firstRequest}, firstReplicaWorkers)
	secondScheduler.processRequestBatch([]*types.ContainerRequest{secondRequest}, secondReplicaWorkers)

	queuedRequest, err := nextContainerRequest(firstScheduler.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)
	assert.Equal(t, firstRequest.ContainerId, queuedRequest.ContainerId)

	extraQueuedRequest, err := nextContainerRequest(firstScheduler.workerRepo, worker.Id)
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
	assert.Empty(t, requeuedRequest.Gpu)
}

func TestProcessRequestConcurrentStaleReplicaGPUReservationRequeues(t *testing.T) {
	firstScheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	secondScheduler := schedulerReplicaForTest(firstScheduler)

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
	setPendingSchedulerRequests(t, firstScheduler, firstRequest, secondRequest)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		firstScheduler.processRequestBatch([]*types.ContainerRequest{firstRequest}, firstReplicaWorkers)
	}()
	go func() {
		defer wg.Done()
		<-start
		secondScheduler.processRequestBatch([]*types.ContainerRequest{secondRequest}, secondReplicaWorkers)
	}()

	close(start)
	wg.Wait()

	queuedRequest, err := nextContainerRequest(firstScheduler.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queuedRequest)

	extraQueuedRequest, err := nextContainerRequest(firstScheduler.workerRepo, worker.Id)
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
	assert.Empty(t, requeuedRequest.Gpu)

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

	choice := growthForTest(t, wb, request)
	reservationID := wb.provisioning.addReservation(wb, request, choice.pool, choice.size)

	done := make(chan struct{})
	go func() {
		defer close(done)
		wb.addWorker(request, choice.pool, choice.size, reservationID)
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

	choice := growthForTest(t, wb, request)
	expectedMemory := capacityMemoryForScheduling(request)
	reservationID := wb.provisioning.addReservation(wb, request, choice.pool, choice.size)

	wb.provisioning.mu.Lock()
	reservation := wb.provisioning.reservations[reservationID]
	assert.NotNil(t, reservation)
	assert.Equal(t, expectedMemory, reservation.worker.TotalMemory)
	assert.Equal(t, int64(0), reservation.worker.FreeMemory)
	wb.provisioning.mu.Unlock()

	wb.addWorker(request, choice.pool, choice.size, reservationID)

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

	choice := growthForTest(t, wb, request)
	reservationID := wb.provisioning.addReservation(wb, request, choice.pool, choice.size)
	wb.provisioning.mu.Lock()
	reservation := wb.provisioning.reservations[reservationID]
	assert.NotNil(t, reservation)
	assert.Equal(t, int64(16000), reservation.worker.TotalCpu)
	assert.Equal(t, int64(15900), reservation.worker.FreeCpu)
	assert.Equal(t, int64(16384), reservation.worker.TotalMemory)
	assert.Equal(t, int64(16384-capacityMemoryForScheduling(request)), reservation.worker.FreeMemory)
	wb.provisioning.mu.Unlock()

	wb.addWorker(request, choice.pool, choice.size, reservationID)

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
		Workspace:    testWorkspaceWithStorage(),
	}

	choice := growthForTest(t, wb, request)
	reservationID := wb.provisioning.addReservation(wb, request, choice.pool, choice.size)
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
	err = wb.containerRepo.CreateContainerStateWithConcurrencyLimit(nil, request)
	assert.Nil(t, err)

	wb.processRequestBatch([]*types.ContainerRequest{request}, nil)

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
	wb.workerPoolManager = NewWorkerPoolManager()

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
		Workspace:    testWorkspaceWithStorage(),
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
	wb.workerPoolManager = NewWorkerPoolManager()

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
		Workspace:    testWorkspaceWithStorage(),
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
	setPendingSchedulerRequests(t, wb, firstRequest)
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
	setPendingSchedulerRequests(t, wb, firstRequest, secondRequest, thirdRequest)
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

				setPendingSchedulerRequests(t, wb, req)
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

	assert.Equal(t, []*types.Worker{worker}, newRoute(request, nil, "").eligible([]*types.Worker{worker}))

	// Outside a named pool, "any" means any GPU in the catalog.
	request.PoolSelector = ""
	worker.RequiresPoolSelector = false
	assert.Empty(t, newRoute(request, nil, "").eligible([]*types.Worker{worker}))
}

func TestCheckpointRuntimeSelection(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	request := &types.ContainerRequest{
		Cpu:               1000,
		Memory:            1000,
		GpuRequest:        []string{"RTX5090"},
		GpuCount:          1,
		CheckpointEnabled: true,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}
	worker := func(id, runtimeName string) *types.Worker {
		return &types.Worker{
			Id:           id,
			Status:       types.WorkerStatusAvailable,
			FreeCpu:      1000,
			FreeMemory:   2000,
			FreeGpuCount: 1,
			Gpu:          "RTX5090",
			Runtime:      runtimeName,
		}
	}
	runc := worker("runc", types.ContainerRuntimeRunc.String())
	gvisor := worker("gvisor", types.ContainerRuntimeGvisor.String())

	workers := []*types.Worker{runc, gvisor}

	assert.Equal(t, workers, filterWorkersByResources(workers, request), "legacy checkpoints have no runtime metadata")

	request.Checkpoint.Runtime = types.ContainerRuntimeRunc.String()
	assert.Equal(t, []*types.Worker{runc}, filterWorkersByResources(workers, request))

	request.Stub.Type = types.StubType(types.StubTypeSandbox)
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{gvisor}, request)
	assert.Error(t, err, "explicit restores must retain their checkpoint")

	request.Stub.Type = types.StubType(types.StubTypeTaskQueueDeployment)
	request.Checkpoint.CompatibilityKey = "source-host"
	runc.CheckpointCompatibilityKey = "source-host"
	gvisor.CheckpointCompatibilityKey = "other-host"
	selected, err := scheduler.selectWorkerFromWorkers(workers, request)
	assert.NoError(t, err)
	assert.Equal(t, runc, selected, "prefer compatible capacity")

	selected, err = scheduler.selectWorkerFromWorkers([]*types.Worker{gvisor}, request)
	assert.NoError(t, err)
	assert.Equal(t, gvisor, selected, "cold-start before provisioning")
	workerRequest := scheduler.prepareWorkerRequest(gvisor, request)
	assert.Nil(t, workerRequest.Checkpoint)
	assert.True(t, workerRequest.CheckpointEnabled)
	assert.NotNil(t, request.Checkpoint, "retries retain the checkpoint")
	assert.True(t, request.CheckpointEnabled)

	gvisor.FreeGpuCount = 0
	_, err = scheduler.selectWorkerFromWorkers([]*types.Worker{gvisor}, request)
	assert.Error(t, err, "cold starts still require free capacity")
	gvisor.FreeGpuCount = 1

	// Matching runtime and logical GPU do not imply a compatible host.
	runc.CheckpointCompatibilityKey = "changed-driver-or-cpu"
	assert.Empty(t, filterWorkersByResources([]*types.Worker{runc}, request))
	selected, err = scheduler.selectWorkerFromWorkers([]*types.Worker{runc}, request)
	assert.NoError(t, err)
	assert.Equal(t, runc, selected, "use ready capacity for a compatible variant or cold start")
	assert.Nil(t, scheduler.prepareWorkerRequest(runc, request).Checkpoint)

	request.Checkpoint.CompatibilityKey = ""
	assert.Empty(t, filterWorkersByResources(workers, request), "legacy deployment snapshots need rebuilding")
	request.Stub.Type = types.StubType(types.StubTypeSandbox)
	request.Checkpoint.Runtime = types.ContainerRuntimeGvisor.String()
	assert.Equal(t, []*types.Worker{gvisor}, filterWorkersByResources(workers, request))

	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	assert.Equal(t, workers, filterWorkersByResources(workers, request), "filesystem checkpoints cold-start on any runtime")

	request.Checkpoint.Status = string(types.CheckpointStatusCheckpointFailed)
	assert.Equal(t, workers, filterWorkersByResources(workers, request), "non-available checkpoints cold-start normally")
}

func TestGPUDeviceCheckpointRequiresCompatibleWorker(t *testing.T) {
	for _, test := range []struct {
		checkpointKey, workerKey string
		want                     bool
	}{
		{"v1:profile", "v1:profile", true},
		{"v2:profile", "v2:profile", true},
		{"v1:profile", "v2:profile", true},
		{"v2:profile", "v1:profile", false},
		{"v1:profile", "v2:other", false},
		{"", "", false},
		{"v2:profile", "", false},
		{"v1:", "v2:", false},
	} {
		assert.Equal(t, test.want, (&types.Checkpoint{CompatibilityKey: test.checkpointKey}).CompatibleWithWorker(test.workerKey))
	}
	assert.False(t, (*types.Checkpoint)(nil).CompatibleWithWorker("v2:profile"))

	request := &types.ContainerRequest{
		Cpu: 1000, Memory: 1000, Gpu: "RTX5090", GpuCount: 1,
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
		Checkpoint: &types.Checkpoint{Status: string(types.CheckpointStatusAvailable),
			Runtime: types.ContainerRuntimeGvisor.String(), CompatibilityKey: "v2:profile"},
	}
	worker := func(key string, status types.WorkerStatus) *types.Worker {
		return &types.Worker{Status: status, FreeCpu: 1000, FreeMemory: 2000, FreeGpuCount: 1,
			Gpu: "RTX5090", Runtime: types.ContainerRuntimeGvisor.String(), CheckpointCompatibilityKey: key}
	}
	legacy := worker("v1:profile", types.WorkerStatusAvailable)
	gpuDevices := worker("v2:profile", types.WorkerStatusAvailable)
	pending := worker("", types.WorkerStatusPending)
	assert.Equal(t, []*types.Worker{gpuDevices}, filterWorkersByResources([]*types.Worker{legacy, gpuDevices, pending}, request))
	assert.False(t, hostMatchesCheckpoint(request.Checkpoint, pending))

	request.Checkpoint.CompatibilityKey = "v1:profile"
	assert.True(t, hostMatchesCheckpoint(request.Checkpoint, gpuDevices))
	assert.True(t, hostMatchesCheckpoint(request.Checkpoint, pending), "legacy pending routing remains unchanged")
	other := worker("v2:other", types.WorkerStatusAvailable)
	assert.Equal(t, []*types.Worker{legacy, gpuDevices, pending}, filterWorkersByResources([]*types.Worker{legacy, gpuDevices, pending, other}, request))
	request.Checkpoint.CompatibilityKey = ""
	assert.Equal(t, []*types.Worker{other}, filterWorkersByResources([]*types.Worker{other}, request), "keyless restores still use free capacity")
	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	request.Checkpoint.CompatibilityKey = "v2:other"
	assert.Equal(t, []*types.Worker{legacy, gpuDevices, pending}, filterWorkersByResources([]*types.Worker{legacy, gpuDevices, pending}, request))
}

func TestCheckpointRuntimeFiltersProvisioningControllers(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"runc":   {ContainerRuntime: types.ContainerRuntimeRunc.String()},
		"gvisor": {ContainerRuntime: types.ContainerRuntimeGvisor.String()},
	}
	request := &types.ContainerRequest{
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Runtime:      types.ContainerRuntimeGvisor.String(),
		},
	}

	assert.Equal(t, []string{"gvisor"}, poolsAccepting(request, pools))

	request.Checkpoint.Runtime = ""
	assert.Equal(t, []string{"gvisor", "runc"}, poolsAccepting(request, pools), "legacy checkpoints retain existing placement")

	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	assert.Equal(t, []string{"gvisor", "runc"}, poolsAccepting(request, pools), "filesystem checkpoints cold-start on any runtime")
}

func TestCheckpointAcceleratorFiltersExistingWorkers(t *testing.T) {
	request := &types.ContainerRequest{
		Cpu:        1000,
		Memory:     1000,
		GpuRequest: []string{string(types.GPU_ANY)},
		GpuCount:   1,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Accelerator:  "NVIDIA GeForce RTX 5090",
		},
	}
	worker := func(id, gpu string) *types.Worker {
		return &types.Worker{
			Id:           id,
			Status:       types.WorkerStatusAvailable,
			FreeCpu:      1000,
			FreeMemory:   2000,
			FreeGpuCount: 1,
			Gpu:          gpu,
		}
	}
	rtx5090 := worker("rtx5090", "RTX5090")
	l40s := worker("l40s", "L40S")
	workers := []*types.Worker{rtx5090, l40s}

	assert.Equal(t, []*types.Worker{rtx5090}, filterWorkersByResources(workers, request))

	request.Checkpoint.Accelerator = ""
	assert.Equal(t, workers, filterWorkersByResources(workers, request), "legacy checkpoints retain GPU preference placement")

	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	request.Checkpoint.Accelerator = "RTX5090"
	assert.Equal(t, workers, filterWorkersByResources(workers, request), "filesystem checkpoints do not require the source accelerator")

	request.Checkpoint.Accelerator = "RTX5090"
	request.Checkpoint.Status = string(types.CheckpointStatusRestoreFailed)
	assert.Equal(t, workers, filterWorkersByResources(workers, request), "non-available checkpoints cold-start normally")
}

func TestCheckpointAcceleratorFiltersProvisioningControllers(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	rtx5090 := &LocalWorkerPoolControllerForTest{name: "rtx5090"}
	l40s := &LocalWorkerPoolControllerForTest{name: "l40s"}
	scheduler.workerPoolManager.SetPool("rtx5090", types.WorkerPoolConfig{GPUType: "RTX5090"}, rtx5090)
	scheduler.workerPoolManager.SetPool("l40s", types.WorkerPoolConfig{GPUType: "L40S"}, l40s)

	request := &types.ContainerRequest{
		GpuRequest: []string{string(types.GPU_ANY)},
		GpuCount:   1,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Accelerator:  "NVIDIA GeForce RTX 5090",
		},
	}
	controllers, err := scheduler.getControllers(request)
	assert.Nil(t, err)
	assert.Equal(t, []WorkerPoolController{rtx5090}, controllers)

	request.Checkpoint.Accelerator = ""
	controllers, err = scheduler.getControllers(request)
	assert.Nil(t, err)
	assert.ElementsMatch(t, []WorkerPoolController{rtx5090, l40s}, controllers)
}

func TestCheckpointAcceleratorDefersUnknownAgentGPUToWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	pools := []*WorkerPool{
		routeTestPool("private", types.WorkerPoolConfig{Mode: types.PoolModePrivate}),
		routeTestPool("local-cpu", types.WorkerPoolConfig{}),
		routeTestPool("private-l40s", types.WorkerPoolConfig{Mode: types.PoolModePrivate, GPUType: "L40S"}),
	}

	request := &types.ContainerRequest{
		Cpu:          1000,
		Memory:       1000,
		GpuRequest:   []string{string(types.GPU_ANY)},
		GpuCount:     1,
		PoolSelector: "private",
		Workspace:    testWorkspaceWithStorage(),
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Accelerator:  "RTX5090",
		},
	}

	// Whichever pool the request names, these are the ones able to restore it.
	ableToRun := func() []string {
		names := []string{}
		for _, pool := range pools {
			if poolCanRun(pool, newRouteQuery(request, "")) {
				names = append(names, pool.Name)
			}
		}
		return names
	}
	assert.Equal(t, []string{"private"}, ableToRun())

	matching := &types.Worker{
		Id:           "rtx5090",
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   2000,
		FreeGpuCount: 1,
		Gpu:          "RTX5090",
	}
	mismatching := &types.Worker{
		Id:           "l40s",
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      1000,
		FreeMemory:   2000,
		FreeGpuCount: 1,
		Gpu:          "L40S",
	}
	assert.Equal(t, []*types.Worker{matching}, filterWorkersByResources([]*types.Worker{matching, mismatching}, request))

	request.GpuRequest = nil
	request.GpuCount = 0
	request.Checkpoint.Accelerator = "CPU"
	assert.Equal(t, []string{"private", "local-cpu"}, ableToRun())

	request.Checkpoint.Accelerator = ""
	assert.Equal(t, []string{"private", "local-cpu", "private-l40s"}, ableToRun())
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

	setPendingSchedulerRequests(t, wb, firstRequest)
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

	setPendingSchedulerRequests(t, wb, request)
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

func TestAgentManagedCapacityRequiresWorkspaceStorage(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	pools := map[string]types.WorkerPoolConfig{"gladiator": {Mode: types.PoolModeExternal}, "default": {}}
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1000}

	assert.Equal(t, []string{"default"}, poolsAccepting(request, pools))

	workers := []*types.Worker{
		{Id: "gladiator", PoolName: "gladiator", PoolSelector: "gladiator", Status: types.WorkerStatusAvailable, FreeCpu: 2000, FreeMemory: 2500, Priority: 1000},
		{Id: "default", PoolName: "default", Status: types.WorkerStatusAvailable, FreeCpu: 2000, FreeMemory: 2500, Priority: 3},
	}

	worker, err := scheduler.selectWorkerFromWorkers(workers, request)
	assert.Nil(t, err)
	assert.Equal(t, "default", worker.Id)

	request.Workspace = testWorkspaceWithStorage()
	worker, err = scheduler.selectWorkerFromWorkers(workers, request)
	assert.Nil(t, err)
	assert.Equal(t, "gladiator", worker.Id)
	assert.Equal(t, []string{"default", "gladiator"}, poolsAccepting(request, pools))
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

	setPendingSchedulerRequests(t, wb, request)
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

type BackendRepoNoConcurrencyLimitForTest struct {
	repo.BackendRepository
}

func (b *BackendRepoNoConcurrencyLimitForTest) GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error) {
	return nil, sql.ErrNoRows
}

func TestDefaultConcurrencyLimitForWorkspacesWithoutQuota(t *testing.T) {
	wb, err := NewSchedulerForTest()
	assert.Nil(t, err)
	wb.backendRepo = &BackendRepoNoConcurrencyLimitForTest{}

	// Default not configured: workspaces without a quota row stay unlimited.
	quota, err := wb.managedConcurrencyLimit(&types.ContainerRequest{WorkspaceId: uuid.New().String()})
	assert.Nil(t, err)
	assert.Nil(t, quota)

	// Default configured: applied to workspaces without a quota row.
	wb.config.GatewayService.DefaultConcurrencyLimit = types.DefaultConcurrencyLimitConfig{CPUMillicores: 64000, GPUCount: 10}
	quota, err = wb.managedConcurrencyLimit(&types.ContainerRequest{WorkspaceId: uuid.New().String()})
	assert.Nil(t, err)
	assert.NotNil(t, quota)
	assert.Equal(t, uint32(64000), quota.CPUMillicoreLimit)
	assert.Equal(t, uint32(10), quota.GPULimit)
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

// A managed pool resizes its live worker when the multiplier changes, and
// reservations above a lowered capacity block admission until they drain.
func TestCPUShareAgentPoolAdmission(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	machine := &compute.AgentTokenState{
		TokenHash:             "agent-token",
		WorkspaceID:           "workspace",
		PoolName:              "shared",
		MachineID:             "machine-1",
		Mode:                  string(types.PoolModeExternal),
		ManagedPoolInstanceID: "instance-1",
		CPUCount:              16,
		CPUMillicores:         16000,
		MemoryMB:              65536,
		Executor:              types.DefaultAgentWorkerContainerMode,
		Schedulable:           true,
		CreatedAt:             time.Now(),
		LastJoinAt:            time.Now(),
	}
	require.NoError(t, s.computeRepo.SaveAgentTokenState(context.Background(), machine, time.Hour))
	t.Cleanup(func() { s.DeleteManagedAgentPool(machine.PoolName, "") })

	workerID := compute.AgentMachineWorkerID(machine.MachineID)
	reconcile := func(multiplier types.CPUShareMultiplier) (int64, int64) {
		require.NoError(t, s.EnsureAgentPool(machine.WorkspaceID, &compute.PoolState{
			Name:              machine.PoolName,
			Selector:          machine.PoolName,
			ManagementSource:  types.WorkerPoolManagementSourceAPI,
			ManagedInstanceID: machine.ManagedPoolInstanceID,
			WorkerConfig:      &types.WorkerPoolConfig{CPUShareMultiplier: multiplier},
		}))
		worker, err := s.workerRepo.GetWorkerById(workerID)
		require.NoError(t, err)
		require.EqualValues(t, machine.MemoryMB, worker.TotalMemory)
		return worker.TotalCpu, worker.FreeCpu
	}
	schedule := func(containerID string, cpu int64) error {
		worker, err := s.workerRepo.GetWorkerById(workerID)
		require.NoError(t, err)
		require.NoError(t, s.containerRepo.SetContainerState(containerID, &types.ContainerState{ContainerId: containerID, Status: types.ContainerStatusPending}))
		return s.workerRepo.ScheduleContainerRequests(worker, []*types.ContainerRequest{{ContainerId: containerID, Cpu: cpu, Memory: 1024}})
	}

	total, free := reconcile(1.5)
	require.EqualValues(t, 24000, total)
	require.EqualValues(t, 24000, free)
	require.NoError(t, s.workerRepo.UpdateWorkerStatus(workerID, types.WorkerStatusAvailable))
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, schedule(id, 8000))
	}
	require.Error(t, schedule("d", 1000))

	total, free = reconcile(1)
	require.EqualValues(t, 16000, total)
	require.Zero(t, free)
	require.Error(t, schedule("e", 1000))

	total, free = reconcile(2)
	require.EqualValues(t, 32000, total)
	require.EqualValues(t, 8000, free)
	require.NoError(t, schedule("f", 8000))
}

func TestCPUShareLocalWorkerKeepsPodCPUPhysical(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{
		name:             "shared",
		workerPoolConfig: types.WorkerPoolConfig{CPUShareMultiplier: 1.5},
		config: types.AppConfig{Worker: types.WorkerConfig{
			JobResourcesEnforced:       true,
			DefaultWorkerCPURequest:    1000,
			DefaultWorkerMemoryRequest: 1024,
		}},
	}

	job, worker := controller.createWorkerJob("worker-1", 4000, 8192, "", 0, "token")
	container := job.Spec.Template.Spec.Containers[0]
	require.EqualValues(t, 4000, container.Resources.Limits.Cpu().MilliValue())
	require.Contains(t, container.Env, corev1.EnvVar{Name: "CPU_LIMIT", Value: "4000"})
	require.EqualValues(t, 6000, worker.TotalCpu)
	require.EqualValues(t, 6000, worker.FreeCpu)
}

func TestCPUShareProvisioningPlaceholderMatchesLocalWorker(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	request := &types.ContainerRequest{ContainerId: "c", Cpu: 3000, Memory: 1024}
	for name, mode := range map[string]types.PoolMode{"local": types.PoolModeLocal, "agent": types.PoolModePrivate} {
		s.workerPoolManager.SetPool(name, types.WorkerPoolConfig{CPUShareMultiplier: 2, Mode: mode}, &LocalWorkerPoolControllerForTest{name: name})
	}

	local, _ := s.workerPoolManager.GetPool("local")
	size := s.workerSizeFor(local, request)
	require.Equal(t, 2*size.cpu, s.newProvisioningReservation(request, local, size, 1).worker.TotalCpu)
	agent, _ := s.workerPoolManager.GetPool("agent")
	require.EqualValues(t, 3000, s.newProvisioningReservation(request, agent, s.workerSizeFor(agent, request), 1).worker.TotalCpu)
}

func TestNewSchedulerRejectsInvalidCPUShareMultiplier(t *testing.T) {
	config := types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"shared": {CPUShareMultiplier: 0.5},
	}}}
	_, err := NewScheduler(context.Background(), config, nil, nil, nil, nil)
	require.ErrorContains(t, err, `worker pool "shared"`)
}

// routeFleet is one of each kind of pool the routing rule tells apart.
var routeFleet = map[string]types.WorkerPoolConfig{
	"cpu":        {},
	"cpu-first":  {Priority: 10},
	"cpu-shared": {Priority: 20, CPUShareMultiplier: 2.5},
	"cpu-named":  {RequiresPoolSelector: true},
	"build":      {RequiresPoolSelector: true},
	"gvisor":     {ContainerRuntime: types.ContainerRuntimeGvisor.String()},
	"microvm":    {ContainerRuntime: types.ContainerRuntimeMicroVM.String()},

	"a10g":         {GPUType: "A10G", Priority: 10},
	"a10g-reserve": {GPUType: "A10G", Priority: 5},
	"a10g-agent":   {GPUType: "A10G", Priority: 1, Mode: types.PoolModeExternal},
	"a10g-spot":    {GPUType: "A10G", Priority: 20, Preemptable: true},
	"a10g-partner": {GPUType: "A10G", Priority: 30, Mode: types.PoolModeProvider, RequiresPoolSelector: true},
	"t4":           {GPUType: "T4"},
	"rtx4090":      {GPUType: "RTX4090", Priority: 5, AlsoServes: []string{"A10G"}},
	"l40s":         {GPUType: "L40S", Priority: 3, RequiresPoolSelector: true, AlsoServes: []string{"A10G", "T4"}},
	"h100-spot":    {GPUType: "H100", Priority: 50, Preemptable: true, AlsoServes: []string{"a10g"}},
	"microvm-4090": {GPUType: "RTX4090", ContainerRuntime: types.ContainerRuntimeMicroVM.String()},
}

const routeFleetBuildPool = "build"

func routeTestPool(name string, config types.WorkerPoolConfig) *WorkerPool {
	return &WorkerPool{Name: name, Config: config, Controller: &LocalWorkerPoolControllerForTest{name: name}}
}

func routeTestPools(configs map[string]types.WorkerPoolConfig) []*WorkerPool {
	pools := make([]*WorkerPool, 0, len(configs))
	for name, config := range configs {
		pools = append(pools, routeTestPool(name, config))
	}
	return pools
}

// describeRoute lists a route's pools in order. A pool that only also serves
// the request is followed by its rank.
func describeRoute(r route) []string {
	names := []string{}
	for _, pool := range r.pools {
		name := pool.Name
		if pool.rank > 0 {
			name = fmt.Sprintf("%s (%d)", name, pool.rank)
		}
		names = append(names, name)
	}
	return names
}

// getControllers returns the controllers of the pools a request may grow, in
// route order.
func (s *Scheduler) getControllers(request *types.ContainerRequest) ([]WorkerPoolController, error) {
	r, err := s.growthRouteFor(request)
	if err != nil {
		return nil, err
	}
	controllers := make([]WorkerPoolController, 0, len(r.pools))
	for _, pool := range r.pools {
		controllers = append(controllers, pool.Controller)
	}
	return controllers, nil
}

func controllerNames(controllers []WorkerPoolController) []string {
	names := make([]string, 0, len(controllers))
	for _, controller := range controllers {
		names = append(names, controller.Name())
	}
	return names
}

// poolsAccepting returns the names of the pools that accept the request, in
// route order. The pools are the test controllers' own, each with the given
// config.
func poolsAccepting(request *types.ContainerRequest, configs map[string]types.WorkerPoolConfig) []string {
	return describeRoute(newRoute(request, routeTestPools(configs), ""))
}

func TestRoute(t *testing.T) {
	gpu := func(gpus ...string) *types.ContainerRequest {
		return &types.ContainerRequest{GpuRequest: gpus, GpuCount: 1}
	}
	withStorage := func(request *types.ContainerRequest) *types.ContainerRequest {
		request.Workspace = testWorkspaceWithStorage()
		return request
	}
	preemptable := func(request *types.ContainerRequest) *types.ContainerRequest {
		request.Preemptable = true
		return request
	}
	named := func(pool string, request *types.ContainerRequest) *types.ContainerRequest {
		request.PoolSelector = pool
		return request
	}
	stub := func(stubType string, request *types.ContainerRequest) *types.ContainerRequest {
		request.Stub.Type = types.StubType(stubType)
		return request
	}
	tier := func(tier string) *types.ContainerRequest {
		request := &types.ContainerRequest{}
		request.Stub.Config = `{"cpu_tier":"` + tier + `"}`
		return request
	}
	dockerfile := "FROM python:3.12"

	tests := []struct {
		name    string
		request *types.ContainerRequest
		want    []string
	}{
		// CPU work runs on CPU pools that need no naming, by priority then
		// name. Work that does not share CPU overflows onto Shared pools
		// last, whatever their priority.
		{"cpu work", &types.ContainerRequest{}, []string{"cpu-first", "cpu", "gvisor", "cpu-shared"}},
		{"performance cpu work", tier("performance"), []string{"cpu-first", "cpu", "gvisor", "cpu-shared"}},
		{"shared cpu work", tier("shared"), []string{"cpu-shared"}},
		{"spot cpu work borrows from every hosted cpu pool", tier("spot"), []string{"cpu-shared", "cpu-first", "cpu", "gvisor"}},

		// A named pool is the whole route.
		{"named pool", named("cpu-named", &types.ContainerRequest{}), []string{"cpu-named"}},
		{"named pool that does not exist", named("retired", &types.ContainerRequest{}), []string{}},
		{"named gpu pool", named("l40s", gpu("L40S")), []string{"l40s"}},
		{"naming a pool does not make it accept the request", named("a10g-spot", gpu("A10G")), []string{}},

		// A GPU request runs on the pools of its type, then on the pools
		// that also serve it.
		{"gpu", gpu("A10G"), []string{"a10g", "a10g-reserve", "rtx4090 (1)", "l40s (2)"}},
		{"gpu with a fallback behind a selector", gpu("T4"), []string{"t4", "l40s (1)"}},
		{"alsoServes goes one way", gpu("RTX4090"), []string{"rtx4090"}},
		{"gpu whose only pool must be named", gpu("L40S"), []string{}},
		{"gpu no pool has or serves", gpu("B200"), []string{}},
		{"several gpu types use the fallbacks of each", gpu("H100", "T4", "A10G"),
			[]string{"t4", "a10g", "a10g-reserve", "rtx4090 (1)", "l40s (2)"}},
		// The order a request lists GPU types in comes before priority.
		{"gpu types in the order the request lists them", gpu("RTX4090", "A10G"),
			[]string{"rtx4090", "a10g", "a10g-reserve", "l40s (1)"}},
		{"any stands for every gpu type not listed before it", gpu("T4", string(types.GPU_ANY)),
			[]string{"t4", "a10g", "a10g-reserve", "rtx4090"}},

		// What the request brings decides which of those pools it may use.
		{"agent pools need workspace storage", withStorage(gpu("A10G")),
			[]string{"a10g", "a10g-reserve", "a10g-agent", "rtx4090 (1)", "l40s (2)"}},
		{"preemptable pools take work that accepts interruption", preemptable(gpu("A10G")),
			[]string{"a10g-spot", "a10g", "a10g-reserve", "h100-spot (1)", "rtx4090 (2)", "l40s (3)"}},
		{"only preemptable pool of its type", gpu("H100"), []string{}},
		{"partner machines run managed endpoints only", named("a10g-partner", withStorage(gpu("A10G"))), []string{}},
		{"managed endpoint on partner machines",
			stub(types.StubTypeManagedEndpointDeployment, named("a10g-partner", withStorage(gpu("A10G")))), []string{"a10g-partner"}},

		// The build pool is for builds.
		{"build pool refuses other work", named("build", &types.ContainerRequest{}), []string{}},
		{"build pool takes builds",
			named("build", &types.ContainerRequest{BuildOptions: types.BuildOptions{Dockerfile: &dockerfile}}), []string{"build"}},

		// Runtimes.
		{"docker needs a sandboxed runtime", &types.ContainerRequest{DockerEnabled: true}, []string{"gvisor"}},
		{"vm sandbox runs only on microvm",
			stub(types.StubTypeSandbox, &types.ContainerRequest{UseVM: true}), []string{"microvm"}},
		{"gpu vm sandbox", func() *types.ContainerRequest {
			request := stub(types.StubTypeSandbox, gpu("RTX4090"))
			request.UseVM = true
			return request
		}(), []string{"microvm-4090"}},
		{"vm without use_vm runs nowhere", stub(types.StubTypeVM, &types.ContainerRequest{}), []string{}},
		{"checkpoint restores on the runtime it was taken on", &types.ContainerRequest{Checkpoint: &types.Checkpoint{
			CheckpointId: "c1", Status: string(types.CheckpointStatusAvailable), Runtime: types.ContainerRuntimeGvisor.String(),
		}}, []string{"gvisor"}},
	}

	pools := routeTestPools(routeFleet)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidates := pools
			if test.request.PoolSelector != "" {
				// routeFor hands a request that names a pool only that pool.
				candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
					return pool.Name != test.request.PoolSelector
				})
			}
			require.Equal(t, test.want, describeRoute(newRoute(test.request, candidates, routeFleetBuildPool)))
		})
	}
}

func TestRouteOrderDoesNotDependOnTheOrderPoolsAreListed(t *testing.T) {
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1}
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"primary-b": {GPUType: "A10G"},
		"primary-a": {GPUType: "A10G"},
		"spare-c":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
		"spare-b":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
		"spare-a":   {GPUType: "H100", AlsoServes: []string{"A10G"}},
	})
	want := []string{"primary-a", "primary-b", "spare-a (1)", "spare-b (2)", "spare-c (3)"}

	for range 20 {
		// routeTestPools ranges over a map, so each call lists the pools in
		// a different order.
		require.Equal(t, want, describeRoute(newRoute(request, slices.Clone(pools), "")))
		pools = routeTestPools(map[string]types.WorkerPoolConfig{
			"primary-b": {GPUType: "A10G"},
			"primary-a": {GPUType: "A10G"},
			"spare-c":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
			"spare-b":   {GPUType: "L40S", AlsoServes: []string{"A10G"}},
			"spare-a":   {GPUType: "H100", AlsoServes: []string{"A10G"}},
		})
	}
}

func TestAnyGPURunsOnEveryGPUPoolThatNeedsNoNaming(t *testing.T) {
	request := &types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1}
	got := newRoute(request, routeTestPools(routeFleet), routeFleetBuildPool)

	// No CPU pool, no pool behind a selector, nothing preemptable, and no
	// fallback rank: "any" asks for every type directly, so the pools are
	// tried by priority alone.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "t4"}, describeRoute(got))
}

func TestCheckpointRestoresOnlyOnItsAccelerator(t *testing.T) {
	request := &types.ContainerRequest{
		GpuRequest: []string{"A10G", "RTX4090"},
		GpuCount:   1,
		Workspace:  testWorkspaceWithStorage(),
		Checkpoint: &types.Checkpoint{
			CheckpointId: "c1", Status: string(types.CheckpointStatusAvailable), Accelerator: "RTX4090",
		},
	}
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G"},
		"rtx4090": {GPUType: "RTX4090"},
		// An agent pool with no gpuType holds whatever its machines have,
		// so its workers are checked one by one.
		"mixed-agent": {Mode: types.PoolModePrivate},
	})

	require.Equal(t, []string{"rtx4090"}, describeRoute(newRoute(request, pools, "")))

	request.PoolSelector = "mixed-agent"
	require.Equal(t, []string{"mixed-agent"}, describeRoute(newRoute(request, pools, "")))
}

func TestRouteRankOfWorker(t *testing.T) {
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":      {GPUType: "A10G"},
		"a10g-spot": {GPUType: "A10G", Preemptable: true},
		"rtx4090":   {GPUType: "RTX4090", AlsoServes: []string{"A10G"}},
		"l40s":      {GPUType: "L40S", RequiresPoolSelector: true, AlsoServes: []string{"A10G"}},
	})
	worker := func(pool string, requiresSelector bool) *types.Worker {
		return &types.Worker{PoolName: pool, RequiresPoolSelector: requiresSelector}
	}

	tests := []struct {
		name     string
		selector string
		worker   *types.Worker
		wantRank int
		wantOK   bool
	}{
		{name: "pool the request asked for", worker: worker("a10g", false), wantOK: true},
		{name: "pool that also serves it", worker: worker("rtx4090", false), wantRank: 2, wantOK: true},
		{name: "pool behind a selector that also serves it", worker: worker("l40s", true), wantRank: 1, wantOK: true},
		{name: "pool that does not accept it", worker: worker("a10g-spot", false)},
		// A pool this replica has not loaded is judged by its worker.
		{name: "unloaded pool", worker: worker("a10g-new", false), wantOK: true},
		{name: "unloaded pool behind a selector", worker: worker("someones-private-pool", true)},
		// A workspace's private pool can share a shared pool's name.
		{name: "private pool named like a shared pool", worker: worker("a10g", true)},
		{name: "private pool named like a pool that also serves it", worker: &types.Worker{PoolName: "rtx4090", RequiresPoolSelector: true, WorkspaceId: "workspace"}},
		{name: "managed machine of a pool that also serves it", worker: &types.Worker{PoolName: "rtx4090", WorkspaceId: "workspace", ControlPlaneManaged: true}, wantRank: 2, wantOK: true},
		{name: "named pool", selector: "a10g", worker: worker("a10g", false), wantOK: true},
		{name: "another pool than the one named", selector: "a10g", worker: worker("rtx4090", false)},
		{name: "named pool that is not loaded", selector: "private", worker: worker("private", true), wantOK: true},
		{name: "another pool than the unloaded one named", selector: "private", worker: worker("a10g", false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, PoolSelector: test.selector}
			candidates := pools
			if test.selector != "" {
				candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
					return pool.Name != test.selector
				})
			}

			rank, ok := newRoute(request, candidates, "").rankOf(test.worker)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.wantRank, rank)
		})
	}
}

func TestRouteEligibleWorkers(t *testing.T) {
	pools := routeTestPools(map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G"},
		"agent":   {Mode: types.PoolModeExternal},
		"rtx4090": {GPUType: "RTX4090", AlsoServes: []string{"A10G"}},
		"t4":      {GPUType: "T4"},
		"custom":  {GPUType: "FPGA9000", RequiresPoolSelector: true},
	})
	worker := func(id, pool, gpu string) *types.Worker {
		return &types.Worker{Id: id, PoolName: pool, Gpu: gpu, RequiresPoolSelector: pools[slices.IndexFunc(pools, func(p *WorkerPool) bool {
			return p.Name == pool
		})].Config.RequiresPoolSelector}
	}
	workers := []*types.Worker{
		worker("a10g", "a10g", "A10G"),
		worker("agent-a10g", "agent", "A10G"),
		worker("agent-h100", "agent", "H100"),
		worker("rtx4090", "rtx4090", "RTX4090"),
		worker("t4", "t4", "T4"),
		worker("custom", "custom", "FPGA9000"),
	}
	ids := func(workers []*types.Worker) []string {
		ids := []string{}
		for _, worker := range workers {
			ids = append(ids, worker.Id)
		}
		return ids
	}
	filter := func(request *types.ContainerRequest) []string {
		candidates := pools
		if request.PoolSelector != "" {
			candidates = slices.DeleteFunc(slices.Clone(pools), func(pool *WorkerPool) bool {
				return pool.Name != request.PoolSelector
			})
		}
		request.Workspace = testWorkspaceWithStorage()
		return ids(newRoute(request, candidates, "").eligible(workers))
	}

	// A pool that also serves the request takes it on whatever GPU it has.
	// The agent pool has no gpuType, so it is in no GPU route, and each of
	// its machines takes the requests for the GPU it has.
	require.Equal(t, []string{"a10g", "agent-a10g", "rtx4090"}, filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1}))
	require.Equal(t, []string{"rtx4090"}, filter(&types.ContainerRequest{Gpu: "RTX4090", GpuCount: 1}))
	require.Equal(t, []string{"agent-h100"}, filter(&types.ContainerRequest{Gpu: "H100", GpuCount: 1}))

	// A pool that cannot run the request rules its machines out. Agent
	// machines need workspace storage, which this request does not bring.
	require.Equal(t, []string{"a10g", "rtx4090"},
		ids(newRoute(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1}, pools, "").eligible(workers)))

	// Inside a named pool, the worker still needs a GPU the request can use.
	require.Equal(t, []string{"agent-a10g"}, filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1, PoolSelector: "agent"}))

	// "any" means any GPU in the catalog, or whatever a named pool has.
	require.Equal(t, []string{"a10g", "agent-a10g", "agent-h100", "rtx4090", "t4"},
		filter(&types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1}))
	require.Equal(t, []string{"custom"}, filter(&types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1, PoolSelector: "custom"}))

	// A machine pin has already chosen the worker; its GPU still has to fit.
	require.Equal(t, []string{"a10g", "agent-a10g", "rtx4090"},
		filter(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1, MachineId: "machine-1"}))
}

func TestRequestNamingAPoolGetsItsWorkspacesOwnPoolFirst(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("gpu", types.WorkerPoolConfig{GPUType: "A10G", Priority: 1}, &LocalWorkerPoolControllerForTest{name: "gpu"})
	manager.SetPoolAt(
		agentPoolControllerKey("workspace-1", &compute.PoolState{Selector: "gpu"}), "gpu",
		types.WorkerPoolConfig{Mode: types.PoolModePrivate, RequiresPoolSelector: true, Priority: 1000},
		&LocalWorkerPoolControllerForTest{name: "gpu"},
	)
	scheduler := &Scheduler{workerPoolManager: manager}
	request := func(workspace, selector string) *types.ContainerRequest {
		return &types.ContainerRequest{
			Gpu: "A10G", GpuCount: 1, WorkspaceId: workspace, PoolSelector: selector, Workspace: testWorkspaceWithStorage(),
		}
	}

	own := scheduler.routeFor(request("workspace-1", "gpu"))
	require.Len(t, own.pools, 1)
	require.Equal(t, types.PoolModePrivate, own.pools[0].Config.Mode)

	shared := scheduler.routeFor(request("workspace-2", "gpu"))
	require.Len(t, shared.pools, 1)
	require.Equal(t, int32(1), shared.pools[0].Config.Priority)

	// A request that names no pool is never routed to a private pool.
	unnamed := scheduler.routeFor(request("workspace-1", ""))
	require.Len(t, unnamed.pools, 1)
	require.Equal(t, int32(1), unnamed.pools[0].Config.Priority)
}

func TestHasManagedPoolForGPU(t *testing.T) {
	manager := NewWorkerPoolManager()
	for name, config := range map[string]types.WorkerPoolConfig{
		"beta9-t4":     {GPUType: "T4"},
		"private-h100": {GPUType: "H100", Mode: types.PoolModePrivate, RequiresPoolSelector: true},
		// Provider pools only host managed endpoint replicas, even when a
		// misconfigured one does not require a selector.
		"provider-a6000": {GPUType: "A6000", Mode: types.PoolModeProvider},
		"provider-spare": {GPUType: "L4", Mode: types.PoolModeProvider, AlsoServes: []string{"A16"}},
		"spare-l40s":     {GPUType: "L40S", RequiresPoolSelector: true, AlsoServes: []string{"A10G"}},
	} {
		manager.SetPool(name, config, &LocalWorkerPoolControllerForTest{name: name})
	}
	scheduler := &Scheduler{workerPoolManager: manager}

	// A pool with zero live workers still counts.
	require.True(t, scheduler.HasManagedPoolForGPU("T4"))
	// A GPU type that no pool has, but one also serves, has somewhere to run.
	require.True(t, scheduler.HasManagedPoolForGPU("A10G"))
	// Pools requiring a pool selector can't serve workloads that name none.
	require.False(t, scheduler.HasManagedPoolForGPU("H100"))
	require.False(t, scheduler.HasManagedPoolForGPU("L40S"))
	// Provider pools never count for ordinary workloads.
	require.False(t, scheduler.HasManagedPoolForGPU("A6000"))
	require.False(t, scheduler.HasManagedPoolForGPU("A16"))
	// GPU_ANY matches any GPU pool usable without a selector.
	require.True(t, scheduler.HasManagedPoolForGPU(string(types.GPU_ANY)))
	// A GPU type nothing has or serves would never be placed.
	require.False(t, scheduler.HasManagedPoolForGPU("B200"))
}

// The fail-fast check and scheduling must agree about every GPU type, for
// the work that the most pools take.
func TestHasManagedPoolForGPUAgreesWithTheRoute(t *testing.T) {
	manager := NewWorkerPoolManager()
	for name, config := range routeFleet {
		manager.SetPool(name, config, &LocalWorkerPoolControllerForTest{name: name})
	}
	scheduler := &Scheduler{workerPoolManager: manager}

	for _, gpu := range types.AllGPUTypes() {
		request := &types.ContainerRequest{
			Gpu: gpu.String(), GpuCount: 1, Preemptable: true,
			// A request that brings everything else a pool can ask for.
			Workspace: testWorkspaceWithStorage(),
		}
		routed := len(scheduler.routeFor(request).pools) > 0
		require.Equal(t, routed, scheduler.HasManagedPoolForGPU(gpu.String()), gpu)
	}
}

// overflowSchedulerForTest returns a scheduler whose A10G requests may
// overflow into the 4090 pool first and the L40S pool second.
func overflowSchedulerForTest(t *testing.T) *Scheduler {
	t.Helper()

	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	for name, config := range map[string]types.WorkerPoolConfig{
		"beta9-4090": {GPUType: "RTX4090", Priority: 2, AlsoServes: []string{"A10G"}},
		"beta9-l40s": {GPUType: "L40S", Priority: 1, AlsoServes: []string{"A10G"}},
		"beta9-h100": {GPUType: "H100", Priority: 9},
	} {
		scheduler.workerPoolManager.SetPool(name, config, &LocalWorkerPoolControllerForTest{
			ctx:        scheduler.ctx,
			name:       name,
			config:     scheduler.config,
			workerRepo: scheduler.workerRepo,
		})
	}
	return scheduler
}

func gpuWorkerForTest(id, poolName, gpu string, freeGpuCount uint32) *types.Worker {
	return &types.Worker{
		Id:           id,
		PoolName:     poolName,
		Gpu:          gpu,
		Status:       types.WorkerStatusAvailable,
		FreeCpu:      4000,
		FreeMemory:   8000,
		FreeGpuCount: freeGpuCount,
		Priority:     10,
	}
}

// TestSelectionFollowsTheRoute covers the placement rule: free capacity
// anywhere in the route schedules immediately, the pools of the requested GPU
// type win while they have room, and the pools that also serve it are taken in
// route order. Available overflow capacity beating pending capacity of the
// requested type falls out of the attempt order in reserve.go, which only
// looks at pending workers after no available worker fits.
func TestSelectionFollowsTheRoute(t *testing.T) {
	tests := []struct {
		name       string
		workers    []*types.Worker
		request    *types.ContainerRequest
		wantWorker string
	}{
		{
			name: "pool of the requested type wins while it has capacity",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "a10g",
		},
		{
			name: "pool that also serves the type takes the request when its own pool is full",
			workers: []*types.Worker{
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 0),
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "route order decides between pools that also serve the type",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1),
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "later pool takes the request when the earlier one is full",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 0),
				gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1),
			},
			request:    &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-l40s",
		},
		{
			name: "gpu spelling in alsoServes is case-insensitive",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
			},
			request:    &types.ContainerRequest{GpuRequest: []string{"A10G"}, GpuCount: 1, Cpu: 1000, Memory: 1000},
			wantWorker: "spare-4090",
		},
		{
			name: "capacity in a pool that does not serve the type is invisible",
			workers: []*types.Worker{
				gpuWorkerForTest("h100", "beta9-h100", "H100", 1),
			},
			request: &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000},
		},
		{
			name: "alsoServes goes one way",
			workers: []*types.Worker{
				gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1),
			},
			request: &types.ContainerRequest{Gpu: "RTX4090", GpuCount: 1, Cpu: 1000, Memory: 1000},
		},
		{
			name: "any gpu takes no overflow rank",
			workers: []*types.Worker{
				gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1),
				gpuWorkerForTest("h100", "beta9-h100", "H100", 1),
			},
			request: &types.ContainerRequest{Gpu: string(types.GPU_ANY), GpuCount: 1, Cpu: 1000, Memory: 1000},
			// Both pools are asked for directly, so the ordinary GPU
			// preference decides.
			wantWorker: "h100",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := overflowSchedulerForTest(t)

			worker, err := scheduler.selectWorkerFromWorkers(test.workers, test.request)
			if test.wantWorker == "" {
				assert.Error(t, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.wantWorker, worker.Id)
		})
	}
}

func TestReadyOverflowCapacityPrecedesPendingOrProvisionableCapacityOfTheRequestedType(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	a10gController := &LocalWorkerPoolControllerForTest{
		ctx:        scheduler.ctx,
		name:       "beta9-a10g",
		config:     scheduler.config,
		workerRepo: scheduler.workerRepo,
	}
	scheduler.workerPoolManager.SetPool(
		"beta9-a10g",
		types.WorkerPoolConfig{GPUType: "A10G"},
		a10gController,
	)

	pendingA10G := gpuWorkerForTest("pending-a10g", "beta9-a10g", "A10G", 1)
	pendingA10G.Status = types.WorkerStatusPending
	ready4090 := gpuWorkerForTest("ready-4090", "beta9-4090", "RTX4090", 1)
	assert.NoError(t, scheduler.workerRepo.AddWorker(pendingA10G))
	assert.NoError(t, scheduler.workerRepo.AddWorker(ready4090))

	request := &types.ContainerRequest{
		Gpu:       "A10G",
		GpuCount:  1,
		Cpu:       1000,
		Memory:    1000,
		Timestamp: time.Now(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	scheduler.processRequestBatch(
		[]*types.ContainerRequest{request},
		[]*types.Worker{pendingA10G, ready4090},
	)

	queued, err := nextContainerRequest(scheduler.workerRepo, ready4090.Id)
	assert.NoError(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, request.ContainerId, queued.ContainerId)
	assert.Equal(t, 0, a10gController.AddWorkerCallCount())
}

func TestRouteRankOverridesWorkerPriority(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	a10g := gpuWorkerForTest("a10g", "beta9-a10g", "A10G", 1)
	a10g.Priority = -100
	first := gpuWorkerForTest("spare-4090", "beta9-4090", "RTX4090", 1)
	first.Priority = -50
	second := gpuWorkerForTest("spare-l40s", "beta9-l40s", "L40S", 1)
	second.Priority = 100
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1, Cpu: 1000, Memory: 1000}

	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{second, first, a10g}, request)
	assert.Nil(t, err)
	assert.Equal(t, a10g.Id, worker.Id)

	a10g.FreeGpuCount = 0
	worker, err = scheduler.selectWorkerFromWorkers([]*types.Worker{second, first, a10g}, request)
	assert.Nil(t, err)
	assert.Equal(t, first.Id, worker.Id)
}

// A pool that hosts one of the request's GPU types is asked for directly,
// even though it also serves another of them as overflow.
func TestPoolOfARequestedGPUTypeIsNotTreatedAsOverflow(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	configured := gpuWorkerForTest("configured-4090", "beta9-4090", "RTX4090", 1)
	configured.Priority = 2
	unloaded := gpuWorkerForTest("unloaded-4090", "pool-this-replica-has-not-loaded", "RTX4090", 1)
	unloaded.Priority = -1

	request := &types.ContainerRequest{
		GpuRequest: []string{"RTX4090", "A10G"},
		GpuCount:   1,
		Cpu:        1000,
		Memory:     1000,
	}
	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{unloaded, configured}, request)
	assert.Nil(t, err)
	assert.Equal(t, configured.Id, worker.Id)
}

func TestStorageBackedRequestPrefersManagedAgentCapacity(t *testing.T) {
	scheduler := overflowSchedulerForTest(t)
	agent := gpuWorkerForTest("managed-agent", "agent-4090", "RTX4090", 1)
	agent.ControlPlaneManaged = true
	agent.PoolSelector = "agent-4090"
	agent.Priority = 2
	legacy := gpuWorkerForTest("legacy-4090", "legacy-4090", "RTX4090", 1)
	legacy.Priority = 3

	request := &types.ContainerRequest{
		GpuRequest: []string{"RTX4090", "A10G"},
		GpuCount:   1,
		Cpu:        1000,
		Memory:     1000,
		Workspace:  testWorkspaceWithStorage(),
	}
	worker, err := scheduler.selectWorkerFromWorkers([]*types.Worker{legacy, agent}, request)
	assert.Nil(t, err)
	assert.Equal(t, agent.Id, worker.Id)

	request.Workspace = types.Workspace{}
	worker, err = scheduler.selectWorkerFromWorkers([]*types.Worker{legacy, agent}, request)
	assert.Nil(t, err)
	assert.Equal(t, legacy.Id, worker.Id)
}

func chainsForTest(chains map[string][]string) types.FailoverConfig {
	config := types.FailoverConfig{Enabled: true, Chains: map[string]types.FailoverChain{}}
	for gpu, pools := range chains {
		config.Chains[gpu] = types.FailoverChain{Pools: pools}
	}
	return config
}

func TestChainsBecomeAlsoServes(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G", Priority: 10},
		"rtx4090": {GPUType: "RTX4090", Priority: 5},
		"l40s":    {GPUType: "L40S", Priority: 3},
	}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"a10g": {"rtx4090", "l40s"}}), pools)

	require.Equal(t, map[string][]string{"rtx4090": {"A10G"}, "l40s": {"A10G"}}, migration.alsoServes)
	require.Equal(t, []string{
		"worker.pools.l40s.alsoServes: [A10G]",
		"worker.pools.rtx4090.alsoServes: [A10G]",
	}, migration.write)
	// The chain's order is the order the pools' priorities give.
	require.Empty(t, migration.reordered)
	require.Empty(t, migration.notes)
}

func TestChainWhoseOrderDiffersFromPrioritiesIsFlagged(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"rtx4090": {GPUType: "RTX4090", Priority: 5},
		"l40s":    {GPUType: "L40S", Priority: 9},
		"h100":    {GPUType: "H100", Priority: 5},
	}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"rtx4090", "l40s", "h100"}}), pools)

	require.Equal(t, []string{
		"A10G fallbacks were tried in the order [rtx4090, l40s, h100] and are now tried by priority, then name: " +
			"[l40s, h100, rtx4090]; set worker.pools.<name>.priority to keep the old order",
	}, migration.reordered)
}

func TestChainNamingAPoolOutsideTheConfigFile(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{"rtx4090": {GPUType: "RTX4090"}}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"spot-l40s", "rtx4090"}}), pools)

	require.Equal(t, []string{
		"worker.pools.rtx4090.alsoServes: [A10G]",
		"worker.pools.spot-l40s.alsoServes: [A10G]  (not in this config: set also_serves on it through the pools API)",
	}, migration.write)
	// Its priority is not known here, so its place in the order is not judged.
	require.Empty(t, migration.reordered)
}

// A chain could name a pool with no gpuType, which alsoServes cannot.
func TestChainNamingAPoolWithNoGPUType(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{"fleet": {Mode: types.PoolModeExternal}}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"fleet"}}), pools)

	require.Equal(t, []string{
		"worker.pools.fleet.alsoServes: [A10G]  (a pool with no gpuType cannot list alsoServes: set its gpuType)",
	}, migration.write)
	// Until the section goes, the pool keeps taking the chain's requests.
	require.Equal(t, []string{"A10G"}, migration.apply("fleet", pools["fleet"]).AlsoServes)
}

func TestSeveralChainsAreNotedBecauseRequestsNowUseAllOfThem(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{"l40s": {GPUType: "L40S"}, "rtx4090": {GPUType: "RTX4090"}}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"l40s"}, "T4": {"l40s", "rtx4090"}}), pools)

	require.Equal(t, []string{
		"worker.pools.l40s.alsoServes: [A10G, T4]",
		"worker.pools.rtx4090.alsoServes: [T4]",
	}, migration.write)
	require.Len(t, migration.notes, 1)
	require.Contains(t, migration.notes[0], "uses the fallback pools of all of them")
}

func TestDisabledChainsHaveNoEffect(t *testing.T) {
	config := chainsForTest(map[string][]string{"A10G": {"rtx4090"}})
	config.Enabled = false

	migration := migrateFailoverChains(config, map[string]types.WorkerPoolConfig{"rtx4090": {GPUType: "RTX4090"}})

	require.Empty(t, migration.alsoServes)
	require.Empty(t, migration.write)
	require.Equal(t, []string{"scheduling.failover is disabled, so its chains have no effect; remove the section"}, migration.notes)

	require.Equal(t, chainMigration{alsoServes: map[string][]string{}}, migrateFailoverChains(types.FailoverConfig{}, nil),
		"a config without the section says nothing")
}

func TestChainsAreAddedToWhatAPoolAlreadyServes(t *testing.T) {
	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"l40s"}, "T4": {"l40s"}}), nil)
	written := []string{"t4"}

	config := migration.apply("l40s", types.WorkerPoolConfig{GPUType: "L40S", AlsoServes: written})
	require.Equal(t, []string{"t4", "A10G"}, config.AlsoServes)
	require.Equal(t, []string{"t4"}, written, "the pool's own config is not modified")
	require.NoError(t, config.Validate())

	untouched := migration.apply("a10g", types.WorkerPoolConfig{GPUType: "A10G"})
	require.Empty(t, untouched.AlsoServes)
}

// A chain could name a pool of its own GPU type. That changed nothing for a
// pool requests of the type reach anyway. For one that requires a selector,
// the chain was what let in the requests that name no pool, after the pools
// that need no naming.
func TestChainNamingAPoolOfItsOwnGPUTypeBehindASelector(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"a10g":          {GPUType: "A10G"},
		"a10g-reserved": {GPUType: "A10G", Priority: 50, RequiresPoolSelector: true},
		"l40s":          {GPUType: "L40S", Priority: 5},
	}
	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"a10g", "a10g-reserved", "l40s"}}), pools)

	require.Equal(t, []string{
		"worker.pools.a10g-reserved.alsoServes: [A10G]",
		"worker.pools.l40s.alsoServes: [A10G]",
	}, migration.write, "the open pool of the chain's type has nothing to write")
	require.Empty(t, migration.reordered)

	for name, config := range pools {
		config = migration.apply(name, config)
		require.NoError(t, config.Validate(), name)
		pools[name] = config
	}
	require.Empty(t, pools["a10g"].AlsoServes)
	require.Equal(t, []string{"A10G"}, pools["a10g-reserved"].AlsoServes)

	request := func(selector string, gpus ...string) *types.ContainerRequest {
		return &types.ContainerRequest{GpuRequest: gpus, GpuCount: 1, PoolSelector: selector}
	}
	routed := func(request *types.ContainerRequest) []string {
		candidates := routeTestPools(pools)
		if request.PoolSelector != "" {
			candidates = slices.DeleteFunc(candidates, func(pool *WorkerPool) bool { return pool.Name != request.PoolSelector })
		}
		return describeRoute(newRoute(request, candidates, ""))
	}

	// The open pool first, whatever the priorities, then the chain's pools.
	require.Equal(t, []string{"a10g", "a10g-reserved (1)", "l40s (2)"}, routed(request("", "A10G")))
	require.Equal(t, []string{"a10g-reserved"}, routed(request("a10g-reserved", "A10G")))
	// Chains never applied to requests for any GPU.
	require.Equal(t, []string{"l40s", "a10g"}, routed(request("", string(types.GPU_ANY))))

	// Its workers take the request, and are no overflow: they have the GPU
	// that was asked for. The L40S pool's are.
	reserved := &types.Worker{Id: "reserved", PoolName: "a10g-reserved", Gpu: "A10G", RequiresPoolSelector: true}
	spare := &types.Worker{Id: "spare", PoolName: "l40s", Gpu: "L40S"}
	route := newRoute(request("", "A10G"), routeTestPools(pools), "")
	require.Equal(t, []*types.Worker{reserved, spare}, route.eligible([]*types.Worker{reserved, spare}))
	for worker, want := range map[*types.Worker]int{reserved: 0, spare: 2} {
		rank, ok := route.rankOf(worker)
		require.True(t, ok, worker.Id)
		require.Equal(t, want, rank, worker.Id)
	}
}

// A config written for the previous release may still hold what that release
// accepted, such as a chain with only an on-demand step. The section is on
// its way out: nothing in it may stop the gateway starting.
func TestAnOldFailoverSectionNeverStopsTheSchedulerStarting(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	manager, err := common.NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	config := manager.GetConfig()
	config.Worker.Pools = map[string]types.WorkerPoolConfig{
		"a10g": {GPUType: "A10G", Mode: types.PoolModeLocal},
		"l40s": {GPUType: "L40S", Mode: types.PoolModeLocal},
	}
	config.Scheduling.Failover = types.FailoverConfig{Enabled: true, Chains: map[string]types.FailoverChain{
		"H100": {},
		"A10G": {Pools: []string{"a10g", "l40s", "l40s", " "}},
		" ":    {Pools: []string{"l40s"}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewScheduler(ctx, config, rdb, nil, nil, nil, WithKubernetes(fake.NewSimpleClientset()))
	require.NoError(t, err)

	l40s, _ := s.workerPoolManager.GetPool("l40s")
	require.Equal(t, []string{"A10G"}, l40s.Config.AlsoServes)
	a10g, _ := s.workerPoolManager.GetPool("a10g")
	require.Empty(t, a10g.Config.AlsoServes, "a pool does not also serve its own GPU type")
}

// A workspace names its own pools, and may pick the name of a shared pool.
// The two are different pools: what is asked about the shared one must not
// be answered from the workspace's.

// ownPool registers a pool of machines that belong to one workspace.
func ownPool(manager *WorkerPoolManager, workspaceID, name string) {
	key := agentPoolControllerKey(workspaceID, &compute.PoolState{Selector: name})
	manager.SetPoolAt(key, name, types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
}

func TestPoolConfigsLeavesOutTheWorkspacesOwnPools(t *testing.T) {
	manager := NewWorkerPoolManager()
	manager.SetPool("cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, nil)
	ownPool(manager, "workspace-1", "cpu")
	ownPool(manager, "workspace-1", "rack")
	s := &Scheduler{workerPoolManager: manager}

	// The pools come out in a different order each time.
	for range 100 {
		modes := map[string]types.PoolMode{}
		for name, config := range s.PoolConfigs() {
			modes[name] = config.Mode
		}
		require.Equal(t, map[string]types.PoolMode{"cpu": types.PoolModeExternal}, modes)
	}
}

// A request that names its workspace's own pool falls back to the regular
// pools when its pool has no room for it and its stub allows it. On its own
// pool the request is exempt from the credit gate and from the managed
// concurrency limit. On the regular pools it is not, so the fallback asks
// both before the request leaves its pool.

const (
	fallbackWorkspace = "workspace-private-fallback"
	fallbackOwnPool   = "private-cpu"
)

// fallbackFixture is a scheduler with a workspace's own pool that has no
// room for the request and cannot add a worker, and a regular pool with a
// worker that has room.
type fallbackFixture struct {
	*Scheduler
	t        *testing.T
	clock    *common.FakeClock
	request  *types.ContainerRequest
	regular  *types.Worker
	failures <-chan *redis.Message
}

func newFallbackFixture(t *testing.T) *fallbackFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)

	f := &fallbackFixture{Scheduler: s, t: t, clock: common.NewFakeClock(time.Now())}
	s.clock, s.requestBacklog.now, s.spawn = f.clock, f.clock.Now, inline
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.failures, _ = s.requestBacklog.rdb.Subscribe(ctx, common.EventChannelKey(common.EventTypeContainerSchedulingFailed))

	state := &compute.PoolState{Selector: fallbackOwnPool, Mode: string(types.PoolModePrivate)}
	s.workerPoolManager.SetPoolAt(agentPoolControllerKey(fallbackWorkspace, state), fallbackOwnPool,
		types.WorkerPoolConfig{Mode: types.PoolModePrivate, RequiresPoolSelector: true},
		&LocalWorkerPoolControllerForTest{ctx: ctx, name: fallbackOwnPool, config: s.config, workerRepo: s.workerRepo, requiresSelector: true, cannotAddWorker: true})

	f.regular = &types.Worker{Id: "regular-worker", Status: types.WorkerStatusAvailable, FreeCpu: 2000, FreeMemory: 2000, PoolName: "beta9-cpu"}
	for _, worker := range []*types.Worker{
		{Id: "own-worker", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 2000, PoolName: fallbackOwnPool, RequiresPoolSelector: true},
		f.regular,
	} {
		require.NoError(t, s.workerRepo.AddWorker(worker))
	}

	f.request = &types.ContainerRequest{
		ContainerId: uuid.NewString(), TaskId: uuid.NewString(), WorkspaceId: fallbackWorkspace,
		Cpu: 1000, Memory: 1000, PoolSelector: fallbackOwnPool, Workspace: testWorkspaceWithStorage(),
	}
	return f
}

// submit sends the request to the scheduler. It names its workspace's own
// pool, so Run asks neither for credit nor for the concurrency limit.
func (f *fallbackFixture) submit() {
	f.t.Helper()
	require.NoError(f.t, f.Run(f.request))
}

// pass is one pass of the scheduling loop.
func (f *fallbackFixture) pass() {
	f.t.Helper()
	f.ProcessBacklog()
}

// waiting returns the request as it waits in the backlog, or nil if it does
// not.
func (f *fallbackFixture) waiting() *types.ContainerRequest {
	f.t.Helper()
	members, err := f.requestBacklog.rdb.ZRange(context.Background(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	require.NoError(f.t, err)
	for _, member := range members {
		var request types.ContainerRequest
		require.NoError(f.t, json.Unmarshal([]byte(member), &request))
		if request.ContainerId == f.request.ContainerId {
			return &request
		}
	}
	return nil
}

// placedOn reports whether the request is on the worker's queue.
func (f *fallbackFixture) placedOn(worker *types.Worker) bool {
	f.t.Helper()
	queued, err := f.workerRepo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(f.t, err)
	return len(queued) == 1 && queued[0].ContainerId == f.request.ContainerId
}

// failed reports whether the scheduler gave up on the request.
func (f *fallbackFixture) failed() bool {
	status, err := f.containerRepo.GetContainerRequestStatus(f.request.ContainerId)
	return err == nil && status == types.ContainerRequestStatusFailed
}

// failure returns why the scheduler gave up on the request.
func (f *fallbackFixture) failure() types.ContainerSchedulingFailureReason {
	f.t.Helper()
	require.True(f.t, f.failed(), "the scheduler has not given up on the request")
	select {
	case message := <-f.failures:
		event, _, claimed := f.eventBus.Claim(message.Payload)
		require.True(f.t, claimed)
		failure, ok := common.ParseContainerSchedulingFailure(event)
		require.True(f.t, ok)
		return failure.Reason
	case <-time.After(time.Second):
		f.t.Fatal("the scheduler did not say why it gave up on the request")
		return ""
	}
}

// A request that has left its own pool is a request for the regular pools
// like any other: the fallback places nothing, and the next pass plans it.
func TestPrivatePoolFallbackLeavesTheRequestToTheNextPass(t *testing.T) {
	f := newFallbackFixture(t)
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	waiting := f.waiting()
	require.NotNil(t, waiting, "the request goes back to the backlog")
	require.Empty(t, waiting.PoolSelector, "without its pool")
	require.Equal(t, retries, waiting.RetryCount, "falling back is not a retry")
	require.False(t, f.placedOn(f.regular))

	f.pass()
	require.Nil(t, f.waiting())
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackFailsARequestOfAWorkspaceWithoutCredit(t *testing.T) {
	f := newFallbackFixture(t)
	billing := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	f.creditGate = newCreditGate(types.CreditGateConfig{}, billing, f.requestBacklog.rdb)
	f.submit()

	f.pass()

	require.Equal(t, types.ContainerSchedulingFailureManagedFallbackNoCredit, f.failure())
	require.Nil(t, f.waiting())
	require.False(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackRetriesWhileBillingCannotBeAsked(t *testing.T) {
	f := newFallbackFixture(t)
	failOpen := false
	billing := &fakeCreditBackend{err: errors.New("billing is down")}
	f.creditGate = newCreditGate(types.CreditGateConfig{FailOpen: &failOpen}, billing, f.requestBacklog.rdb)
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	require.False(t, f.failed(), "no answer is not a refusal")
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Equal(t, fallbackOwnPool, waiting.PoolSelector)
	require.Equal(t, retries+1, waiting.RetryCount)

	// Billing answers again, and the request goes on to the regular pools.
	billing.err, billing.decision = nil, creditDecision{OK: true}
	f.clock.Advance(time.Second)
	f.pass()
	f.pass()
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackFailsARequestOverItsWorkspacesConcurrencyLimit(t *testing.T) {
	f := newFallbackFixture(t)
	f.backendRepo = &BackendRepoConcurrencyLimitsForTest{BackendRepository: f.backendRepo, CPUConcurrencyLimit: uint32(f.request.Cpu) / 2}
	// The workspace has asked for the regular pools before, so what it uses
	// of them is being counted.
	earlier := &types.ContainerRequest{ContainerId: uuid.NewString(), WorkspaceId: fallbackWorkspace, Cpu: 100}
	require.NoError(t, f.CheckConcurrencyLimit(earlier))
	f.submit()

	f.pass()

	require.Equal(t, types.ContainerSchedulingFailureManagedFallbackConcurrencyLimit, f.failure())
	require.Nil(t, f.waiting())
	require.False(t, f.placedOn(f.regular))
}

// unreadableConcurrencyLimits is a database that cannot be reached.
type unreadableConcurrencyLimits struct {
	repo.BackendRepository
}

func (unreadableConcurrencyLimits) GetConcurrencyLimitByWorkspaceId(context.Context, string) (*types.ConcurrencyLimit, error) {
	return nil, errors.New("database unavailable")
}

func TestPrivatePoolFallbackRetriesWhileTheConcurrencyLimitCannotBeRead(t *testing.T) {
	f := newFallbackFixture(t)
	limits := f.backendRepo
	f.backendRepo = unreadableConcurrencyLimits{limits}
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	require.False(t, f.failed(), "no answer is not a refusal")
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Equal(t, fallbackOwnPool, waiting.PoolSelector)
	require.Equal(t, retries+1, waiting.RetryCount)

	// The database answers again, and the request goes on to the regular
	// pools.
	f.backendRepo = limits
	f.clock.Advance(time.Second)
	f.pass()
	f.pass()
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackWaitsLikeAnyRequestWhenTheWorkerThatIsStartingIsTaken(t *testing.T) {
	f := newFallbackFixture(t)
	// The only regular pool cannot add a worker. It has one that is
	// starting, with room for one request, and another request waits for it.
	f.workerPoolManager.DeletePool("default")
	pool, ok := f.workerPoolManager.GetPool("beta9-cpu")
	require.True(t, ok)
	pool.Controller.(*LocalWorkerPoolControllerForTest).cannotAddWorker = true
	require.NoError(t, f.workerRepo.RemoveWorker(f.regular.Id))
	starting := &types.Worker{Id: "starting-worker", Status: types.WorkerStatusPending, PoolName: "beta9-cpu",
		TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 2000, FreeMemory: 2000}
	require.NoError(t, f.workerRepo.AddWorker(starting))
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	other := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1000, Timestamp: f.clock.Now()}
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, other))
	f.submit()

	// The request leaves its pool for the worker that is starting, and then
	// finds no room on it. It waits for room, as the other request does.
	for range 3 {
		f.pass()
		f.clock.Advance(time.Second)
	}

	require.False(t, f.failed())
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Empty(t, waiting.PoolSelector)
}

// A machine that drops out of a workspace's own pool leaves its worker on
// record for a while. A request for the pool is placed only on a worker
// whose machine is connected, or it would wait there for nothing.

// unlistableMachines is a compute repository whose machines cannot be listed
// while err is set.
type unlistableMachines struct {
	repo.ComputeRepository
	err error
}

func (r *unlistableMachines) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*compute.AgentTokenState, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.ComputeRepository.ListAgentTokenStates(ctx, workspaceID, poolName)
}

// ownPoolFixture is a workspace's own pool with two machines, each with a
// worker that is up and has room. One machine is connected; the other has
// not been heard from for too long.
type ownPoolFixture struct {
	*Scheduler
	machines   *unlistableMachines
	controller *AgentWorkerPoolController
	request    *types.ContainerRequest
	connected  string
	silent     string
}

func newOwnPoolFixture(t *testing.T) *ownPoolFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	f := &ownPoolFixture{
		Scheduler: s,
		machines:  &unlistableMachines{ComputeRepository: s.computeRepo},
		connected: compute.AgentMachineWorkerID("connected"),
		silent:    compute.AgentMachineWorkerID("silent"),
	}
	s.computeRepo = f.machines

	const workspace = "workspace"
	// The pool's name is not what requests select it by.
	pool := &compute.PoolState{Name: "rack-1", Selector: "own-cpu", Mode: string(types.PoolModePrivate)}
	ctx := context.Background()
	require.NoError(t, s.computeRepo.SavePoolState(ctx, workspace, pool))
	now := time.Now()
	for machine, lastHeard := range map[string]time.Time{"connected": now, "silent": now.Add(-2 * compute.AgentHeartbeatTimeout)} {
		require.NoError(t, s.computeRepo.SaveAgentTokenState(ctx, &compute.AgentTokenState{
			TokenHash: "token-of-" + machine, WorkspaceID: workspace, PoolName: pool.Name, MachineID: machine,
			CPUMillicores: 4000, MemoryMB: 8192, Executor: types.DefaultAgentWorkerContainerMode,
			Schedulable: true, LastHeartbeatAt: lastHeard,
		}, time.Hour))
		require.NoError(t, s.workerRepo.AddWorker(&types.Worker{
			Id: compute.AgentMachineWorkerID(machine), MachineId: machine, Status: types.WorkerStatusAvailable,
			PoolName: pool.Name, PoolSelector: pool.Selector, WorkspaceId: workspace, RequiresPoolSelector: true,
			TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192,
		}))
	}
	require.NoError(t, s.EnsureAgentPool(workspace, pool))
	loaded, ok := s.privateAgentPool(workspace, pool.Selector)
	require.True(t, ok)
	f.controller = loaded.Controller.(*AgentWorkerPoolController)

	f.request = &types.ContainerRequest{
		ContainerId: "container", WorkspaceId: workspace, PoolSelector: pool.Selector,
		Cpu: 1000, Memory: 1000, Workspace: testWorkspaceWithStorage(),
	}
	return f
}

// candidates returns the workers the request may be placed on.
func (f *ownPoolFixture) candidates(t *testing.T) []string {
	t.Helper()
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	var ids []string
	for _, worker := range f.filterLivePrivateAgentWorkers(workers, f.request) {
		ids = append(ids, worker.Id)
	}
	return ids
}

func TestAnOwnPoolsLiveWorkersAreThoseOfItsConnectedMachines(t *testing.T) {
	f := newOwnPoolFixture(t)

	live, err := f.controller.liveWorkerIDs()

	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{f.connected: {}}, live)
}

func TestARequestForAnOwnPoolIsPlacedOnlyOnAConnectedMachine(t *testing.T) {
	f := newOwnPoolFixture(t)
	require.Equal(t, []string{f.connected}, f.candidates(t))

	// The silent machine has the more room, and still is not chosen.
	silent, err := f.workerRepo.GetWorkerById(f.silent)
	require.NoError(t, err)
	silent.TotalCpu, silent.FreeCpu = 64000, 64000
	require.NoError(t, f.workerRepo.AddWorker(silent))
	selected, err := f.selectWorker(f.request)
	require.NoError(t, err)
	require.Equal(t, f.connected, selected.Id)

	// A request of another workspace, or for another pool, is not this
	// pool's to filter.
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	for _, other := range []*types.ContainerRequest{
		{WorkspaceId: "another-workspace", PoolSelector: f.request.PoolSelector},
		{WorkspaceId: f.request.WorkspaceId, PoolSelector: "another-pool"},
		{WorkspaceId: f.request.WorkspaceId},
	} {
		require.Len(t, f.filterLivePrivateAgentWorkers(workers, other), 2)
	}
}

// When the pool's machines cannot be listed, none of its workers can be
// ruled out: the request is placed as if all were connected, and the failure
// is reported.
func TestAnOwnPoolsWorkersAreAllKeptWhenItsMachinesCannotBeListed(t *testing.T) {
	f := newOwnPoolFixture(t)
	f.machines.err = errors.New("redis is down")
	logged := captureLog(t)

	require.ElementsMatch(t, []string{f.connected, f.silent}, f.candidates(t))

	require.Contains(t, logged.String(), "redis is down")
	require.Contains(t, logged.String(), `"pool_name":"own-cpu"`)
	require.Contains(t, logged.String(), `"container_id":"container"`)
}

// growthForTest returns the pool the scheduler would ask for a worker for the
// request, and the size it would ask for.
func growthForTest(t *testing.T, s *Scheduler, request *types.ContainerRequest) growth {
	t.Helper()
	route, err := s.growthRouteFor(request)
	require.NoError(t, err)
	choice := s.chooseGrowth(route, request)
	require.NotNil(t, choice.pool, "no pool can add a worker for the request")
	return choice
}

// growFixture is a scheduler on a fake clock whose route for a CPU request is
// the pools a, b and c, in that order.
type growFixture struct {
	*Scheduler
	clock *common.FakeClock
	pools map[string]*LocalWorkerPoolControllerForTest
}

func newGrowFixture(t *testing.T) *growFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)

	fixture := &growFixture{Scheduler: s, clock: common.NewFakeClock(time.Now()), pools: map[string]*LocalWorkerPoolControllerForTest{}}
	s.clock = fixture.clock
	s.workerPoolManager = NewWorkerPoolManager()
	for i, name := range []string{"a", "b", "c"} {
		controller := &LocalWorkerPoolControllerForTest{ctx: context.Background(), name: name, config: s.config, workerRepo: s.workerRepo}
		fixture.pools[name] = controller
		s.workerPoolManager.SetPool(name, types.WorkerPoolConfig{Priority: int32(30 - 10*i)}, controller)
	}
	return fixture
}

func (f *growFixture) choose(t *testing.T, request *types.ContainerRequest) growth {
	t.Helper()
	route, err := f.growthRouteFor(request)
	require.NoError(t, err)
	return f.chooseGrowth(route, request)
}

func TestChooseGrowth(t *testing.T) {
	const noPool = ""
	asked := func(attempts map[string]int) *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, GrowAttempts: attempts}
	}

	for _, test := range []struct {
		name      string
		setup     func(f *growFixture)
		request   *types.ContainerRequest
		want      string
		exhausted bool
	}{
		{
			name:    "the first pool in the route",
			request: asked(nil),
			want:    "a",
		},
		{
			name:    "a pool that cannot add a worker is passed over",
			setup:   func(f *growFixture) { f.pools["a"].cannotAddWorker = true },
			request: asked(nil),
			want:    "b",
		},
		{
			name:    "a pool that cannot be asked is passed over",
			setup:   func(f *growFixture) { f.pools["a"].canAddWorkerErr = errors.New("redis is down") },
			request: asked(nil),
			want:    "b",
		},
		{
			name:    "a pool that just failed to add a worker is left alone",
			setup:   func(f *growFixture) { f.workerProvisioningBackoff.failed("a", f.now()) },
			request: asked(nil),
			want:    "b",
		},
		{
			name: "and is asked again once the delay has passed",
			setup: func(f *growFixture) {
				f.workerProvisioningBackoff.failed("a", f.now())
				f.clock.Advance(growRetryDelay)
			},
			request: asked(nil),
			want:    "a",
		},
		{
			name:    "a pool the request has asked three times is not asked again",
			request: asked(map[string]int{"a": maxGrowAttempts}),
			want:    "b",
		},
		{
			name:    "attempts on one pool do not count against another",
			request: asked(map[string]int{"a": maxGrowAttempts - 1, "b": maxGrowAttempts, "c": maxGrowAttempts}),
			want:    "a",
		},
		{
			name: "no pool can add a worker now",
			setup: func(f *growFixture) {
				for _, pool := range f.pools {
					pool.cannotAddWorker = true
				}
			},
			request: asked(nil),
			want:    noPool,
		},
		{
			name: "every pool is being left alone",
			setup: func(f *growFixture) {
				for name := range f.pools {
					f.workerProvisioningBackoff.failed(name, f.now())
				}
			},
			request: asked(nil),
			want:    noPool,
		},
		{
			name:      "the request has asked every pool three times",
			request:   asked(map[string]int{"a": maxGrowAttempts, "b": maxGrowAttempts, "c": maxGrowAttempts}),
			want:      noPool,
			exhausted: true,
		},
		{
			name:    "a pool that cannot grow now keeps the request waiting when the others are used up",
			setup:   func(f *growFixture) { f.pools["b"].cannotAddWorker = true },
			request: asked(map[string]int{"a": maxGrowAttempts, "c": maxGrowAttempts}),
			want:    noPool,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newGrowFixture(t)
			if test.setup != nil {
				test.setup(f)
			}
			before := test.request.Clone()

			choice := f.choose(t, test.request)

			got := noPool
			if choice.pool != nil {
				got = choice.pool.Name
			}
			require.Equal(t, test.want, got)
			require.Equal(t, test.exhausted, choice.exhausted)
			// Choosing is a decision only: nothing is asked for and nothing
			// is counted.
			require.Equal(t, before.GrowAttempts, test.request.GrowAttempts)
			for name, pool := range f.pools {
				require.Zero(t, pool.AddWorkerCallCount(), "pool %s", name)
			}
		})
	}
}

// A request counts the workers it asked each pool for, so pools that keep
// failing use up their own attempts and not those of the pools after them.
func TestGrowCountsAttemptsPerPool(t *testing.T) {
	f := newGrowFixture(t)
	f.spawn = func(fn func()) { fn() }
	f.pools["a"].addWorkerErr = errors.New("cluster refused the job")
	f.pools["b"].addWorkerErr = errors.New("cluster refused the job")
	f.pools["c"].cannotAddWorker = true

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	for round := 1; round <= maxGrowAttempts; round++ {
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // a fails
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // a is left alone, b fails
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // both left alone, c cannot grow
		require.Equal(t, map[string]int{"a": round, "b": round}, request.GrowAttempts)
		require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable(), "round %d", round)
		f.clock.Advance(growRetryDelay)
	}

	// a and b are used up. c was never asked for a worker, so it costs
	// nothing and the request keeps waiting for it.
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable())
	require.Zero(t, f.pools["c"].AddWorkerCallCount())

	f.pools["c"].cannotAddWorker = false
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["c"].AddWorkerCallCount())
	require.Equal(t, map[string]int{"a": maxGrowAttempts, "b": maxGrowAttempts, "c": 1}, request.GrowAttempts)
}

func TestWorkerSizeFor(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.DefaultWorkerCPURequest = 2000
	s.config.Worker.DefaultWorkerMemoryRequest = 2048

	sized := types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerCPU: "8000m", DefaultWorkerMemory: "16Gi", DefaultWorkerGpuCount: "4"}
	small := &types.ContainerRequest{Cpu: 500, Memory: 512}
	large := &types.ContainerRequest{Cpu: 32000, Memory: 64 * 1024, Gpu: "A10G", GpuCount: 8}
	memory := capacityMemoryForScheduling

	for _, test := range []struct {
		name    string
		pool    types.WorkerPoolConfig
		request *types.ContainerRequest
		want    workerSize
	}{
		{"kubernetes pool: at least the default worker", types.WorkerPoolConfig{}, small,
			workerSize{cpu: 2000, memory: 2048}},
		{"kubernetes pool: at least the pool's own default", types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: sized}, small,
			workerSize{cpu: 8000, memory: 16 * 1024, gpuCount: 4}},
		{"kubernetes pool: a larger request gets a worker its size", types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: sized}, large,
			workerSize{cpu: 32000, memory: memory(large), gpuCount: 8}},
		{"agent pool: what the request needs", types.WorkerPoolConfig{Mode: types.PoolModeExternal, PoolSizing: sized}, small,
			workerSize{cpu: 500, memory: memory(small)}},
		{"private pool: what the request needs", types.WorkerPoolConfig{Mode: types.PoolModePrivate, GPUType: "A10G", PoolSizing: sized}, large,
			workerSize{cpu: 32000, memory: memory(large), gpuCount: 8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, s.workerSizeFor(&WorkerPool{Name: "pool", Config: test.pool}, test.request))
		})
	}
}

func TestPoolThatFailedToAddAWorkerIsLeftAloneForAMoment(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	started := make(chan struct{}, 2)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "beta9-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: started,
		addWorkerErr:     errors.New("provisioning failed"),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	firstRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, firstRequest, nil).grow()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first provisioning attempt")
	}

	deadline := time.After(time.Second)
	for !scheduler.workerProvisioningBackoff.leftAlone("beta9-cpu", scheduler.now()) {
		select {
		case <-deadline:
			t.Fatal("expected provisioning failure to back off add-worker attempts")
		case <-time.After(time.Millisecond):
		}
	}
	assert.Equal(t, 1, controller.AddWorkerCallCount())

	secondRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, secondRequest, nil).grow()
	time.Sleep(2 * requestProcessingInterval)

	assert.Equal(t, 1, controller.AddWorkerCallCount())
}

func TestGrowPassesOverAPoolThatCannotAddAWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          scheduler.config,
		workerRepo:      scheduler.workerRepo,
		cannotAddWorker: true,
	}
	dynamicStarted := make(chan struct{}, 1)
	dynamic := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "dynamic-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: dynamicStarted,
	}
	scheduler.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeExternal,
		Priority: 10,
	}, agent)
	scheduler.workerPoolManager.SetPool(dynamic.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeLocal,
		Priority: 0,
	}, dynamic)

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(),
		Cpu:         8000,
		Memory:      16 * 1024,
		Timestamp:   time.Now(),
		// With storage the agent pool is in the route, ahead of the dynamic one.
		Workspace: testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, nil).grow()

	select {
	case <-dynamicStarted:
	case <-time.After(time.Second):
		t.Fatal("expected the dynamic pool to provision a worker")
	}
	assert.Equal(t, 0, agent.AddWorkerCallCount())
	assert.Equal(t, 1, dynamic.AddWorkerCallCount())
	// Passing over the agent pool cost the request nothing.
	assert.Equal(t, map[string]int{dynamic.name: 1}, request.GrowAttempts)
}

func TestGrowDoesNotSpendAnAttemptWhenAPoolCannotBeAsked(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          scheduler.config,
		workerRepo:      scheduler.workerRepo,
		canAddWorkerErr: context.DeadlineExceeded,
	}
	scheduler.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode: types.PoolModeExternal,
	}, agent)

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(),
		Cpu:         8000,
		Memory:      16 * 1024,
		Timestamp:   time.Now(),
		Workspace:   testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, nil).grow()

	assert.Equal(t, 0, agent.AddWorkerCallCount())
	assert.Empty(t, request.GrowAttempts)
	time.Sleep(provisioningWorkerRequeueDelay + 10*time.Millisecond)
	requeued, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeued.ContainerId)
}

func TestEveryReplicaPassesOverAPoolThatCannotAddAWorker(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.Nil(t, err)
	first.workerPoolManager = NewWorkerPoolManager()
	replicas := []*Scheduler{first, schedulerReplicaForTest(first), schedulerReplicaForTest(first)}

	dynamic := &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "dynamic-cpu",
		config:     first.config,
		workerRepo: first.workerRepo,
	}
	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          first.config,
		workerRepo:      first.workerRepo,
		cannotAddWorker: true,
	}
	first.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeExternal,
		Priority: 10,
	}, agent)
	first.workerPoolManager.SetPool(dynamic.name, types.WorkerPoolConfig{Mode: types.PoolModeLocal}, dynamic)

	request := &types.ContainerRequest{Cpu: 8000, Memory: 16 * 1024, Workspace: testWorkspaceWithStorage()}
	for _, scheduler := range replicas {
		controllers, err := scheduler.getControllers(request)
		assert.Nil(t, err)
		assert.Equal(t, []string{agent.name, dynamic.name}, controllerNames(controllers))
		assert.Equal(t, dynamic.name, growthForTest(t, scheduler, request).pool.Name)
	}
	assert.Equal(t, 0, agent.AddWorkerCallCount())
}

func TestGrowAsksOnePoolPerPass(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	primaryStarted := make(chan struct{}, 1)
	secondaryStarted := make(chan struct{}, 1)
	primary := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "primary",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: primaryStarted,
		addWorkerErr:     errors.New("provisioning failed"),
	}
	secondary := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "secondary",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: secondaryStarted,
	}

	scheduler.workerPoolManager.SetPool("primary", types.WorkerPoolConfig{Priority: 200}, primary)
	scheduler.workerPoolManager.SetPool("secondary", types.WorkerPoolConfig{Priority: 100}, secondary)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         100,
		Memory:      100,
		Timestamp:   time.Now(),
	}
	newSchedulingAttempt(scheduler, request, nil).grow()

	select {
	case <-primaryStarted:
	case <-time.After(time.Second):
		t.Fatal("expected primary provisioning attempt")
	}

	select {
	case <-secondaryStarted:
		t.Fatal("did not expect same reservation to provision a second pool")
	case <-time.After(2 * requestProcessingInterval):
	}

	assert.Equal(t, 1, primary.AddWorkerCallCount())
	assert.Equal(t, 0, secondary.AddWorkerCallCount())
}

func TestRequestDoesNotAddASecondWorkerWhileItsFirstIsPending(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	started := make(chan struct{}, 2)
	done := make(chan struct{}, 1)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "gvisor-pool",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		containerRuntime: types.ContainerRuntimeGvisor.String(),
		addWorkerStarted: started,
		addWorkerDone:    done,
	}
	scheduler.workerPoolManager.SetPool("gvisor-pool", types.WorkerPoolConfig{
		ContainerRuntime: types.ContainerRuntimeGvisor.String(),
	}, controller)

	request := &types.ContainerRequest{
		ContainerId:       uuid.NewString(),
		Cpu:               100,
		Memory:            100,
		PoolSelector:      "gvisor-pool",
		Timestamp:         time.Now(),
		CheckpointEnabled: true,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Runtime:      types.ContainerRuntimeGvisor.String(),
		},
	}
	setPendingSchedulerRequests(t, scheduler, request)

	scheduler.processRequestBatch([]*types.ContainerRequest{request}, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected the initial worker provisioning attempt")
	}

	requeued := popBacklogRequest(t, scheduler.requestBacklog)
	assert.Equal(t, map[string]int{"gvisor-pool": 1}, requeued.GrowAttempts)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected the initial worker provisioning attempt to complete")
	}

	workers, err := scheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Len(t, workers, 1)
	workers[0].Runtime = types.ContainerRuntimeRunc.String()
	assert.Nil(t, scheduler.workerRepo.AddWorker(workers[0]))

	scheduler.processRequestBatch([]*types.ContainerRequest{requeued}, workers)
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 1, controller.AddWorkerCallCount(), "one request must not create another worker while its first worker is pending")
}

func TestRequestThatHasAskedEveryPoolThreeTimesFails(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	controller := &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "beta9-cpu",
		config:     scheduler.config,
		workerRepo: scheduler.workerRepo,
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)
	request := &types.ContainerRequest{
		ContainerId:  uuid.NewString(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
		GrowAttempts: map[string]int{"beta9-cpu": maxGrowAttempts},
	}
	setPendingSchedulerRequests(t, scheduler, request)

	newSchedulingAttempt(scheduler, request, nil).grow()
	assert.Equal(t, 0, controller.AddWorkerCallCount())
	assert.False(t, newSchedulingAttempt(scheduler, request, nil).runnable())
}

// blockedProvisioning is a pool whose AddWorker parks until unblock closes,
// reporting each call on started.
func blockedProvisioning(t *testing.T, scheduler *Scheduler, n int) (*LocalWorkerPoolControllerForTest, chan struct{}) {
	t.Helper()
	unblock := make(chan struct{})
	controller := &LocalWorkerPoolControllerForTest{
		ctx: context.Background(), name: "beta9-cpu", config: scheduler.config, workerRepo: scheduler.workerRepo,
		addWorkerStarted: make(chan struct{}, n), addWorkerDone: make(chan struct{}, n), unblockAddWorker: unblock,
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)
	return controller, unblock
}

func awaitN(t *testing.T, ch chan struct{}, n int, what string) {
	t.Helper()
	for i := range n {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: %d/%d", what, i, n)
		}
	}
}

// Replicas share one lock per container: while one provisions, or until its
// pending worker's reservation expires, the others do not add a second worker.
// Distinct containers never contend.
func TestGrowLockIsPerContainerAcrossReplicas(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.Nil(t, err)
	second := schedulerReplicaForTest(first)
	controller, unblock := blockedProvisioning(t, first, 3)
	request := func() *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 100, Memory: 100, PoolSelector: "beta9-cpu", Timestamp: time.Now()}
	}
	same, otherA, otherB := request(), request(), request()
	setPendingSchedulerRequests(t, first, same, otherA, otherB)

	newSchedulingAttempt(first, same, nil).grow()
	newSchedulingAttempt(second, same, nil).grow()
	newSchedulingAttempt(second, otherA, nil).grow()
	newSchedulingAttempt(first, otherB, nil).grow()
	awaitN(t, controller.addWorkerStarted, 3, "distinct containers started provisioning")
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 3, controller.AddWorkerCallCount(), "the duplicate attempt waits")

	close(unblock)
	awaitN(t, controller.addWorkerDone, 3, "provisioning finished")
	newSchedulingAttempt(second, same, nil).grow()
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 3, controller.AddWorkerCallCount(), "the lock outlives a successful attempt")
	workers, err := first.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Len(t, workers, 3)
}

func popBacklogRequest(t *testing.T, backlog *RequestBacklog) *types.ContainerRequest {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		request, err := backlog.Pop()
		if err == nil {
			return request
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("expected a request in the scheduler backlog")
	return nil
}

func TestPoolLeftAloneAfterAFailureStillPlacesOnItsWorkers(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	controller := &LocalWorkerPoolControllerForTest{
		ctx:          context.Background(),
		name:         "beta9-cpu",
		config:       scheduler.config,
		workerRepo:   scheduler.workerRepo,
		addWorkerErr: errors.New("provisioning failed"),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	provisionRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, provisionRequest, nil).grow()

	deadline := time.After(time.Second)
	for !scheduler.workerProvisioningBackoff.leftAlone("beta9-cpu", scheduler.now()) {
		select {
		case <-deadline:
			t.Fatal("expected add-worker attempt to be backed off")
		case <-time.After(time.Millisecond):
		}
	}

	worker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    100,
		FreeCpu:     100,
		TotalMemory: 125,
		FreeMemory:  125,
		PoolName:    "beta9-cpu",
	}
	assert.Nil(t, scheduler.workerRepo.AddWorker(worker))

	scheduleRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	setPendingSchedulerRequests(t, scheduler, scheduleRequest)
	scheduler.processRequestBatch([]*types.ContainerRequest{scheduleRequest}, []*types.Worker{worker})

	queued, err := nextContainerRequest(scheduler.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, scheduleRequest.ContainerId, queued.ContainerId)
}

// A request that asked a pool for a worker holds a lock until it would stop
// waiting for that worker. Finding the lock held is the normal case for every
// other scheduling pass and is not an error.
func TestGrowDoesNotReportAHeldLockAsAnError(t *testing.T) {
	f := newGrowFixture(t)
	replica := schedulerReplicaForTest(f.Scheduler)

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	var logged bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&logged)
	t.Cleanup(func() { log.Logger = original })

	f.spawn = func(fn func()) { fn() }
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	newSchedulingAttempt(replica, request.Clone(), nil).grow()

	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())
	require.NotContains(t, logged.String(), `"level":"error"`)
}

func TestGrowLockLastsAsLongAsTheRequestWaitsForTheWorker(t *testing.T) {
	pool := func(stalledAfter time.Duration) *WorkerPool {
		return &WorkerPool{Name: "pool", Config: types.WorkerPoolConfig{StalledAfter: types.Duration(stalledAfter)}}
	}
	require.Equal(t, pendingWorkerReservationTTL, growLockTTL(pool(0)))
	require.Equal(t, time.Minute, growLockTTL(pool(time.Minute)), "a stalled worker is given up on sooner")
	require.Equal(t, pendingWorkerReservationTTL, growLockTTL(pool(time.Hour)))
}

// waitingFixture is a grow fixture whose request has asked pool a for a
// worker and waits for it. The worker has room for four such requests.
type waitingFixture struct {
	*growFixture
	request *types.ContainerRequest
	worker  *types.Worker
}

func newWaitingFixture(t *testing.T) *waitingFixture {
	t.Helper()
	f := newGrowFixture(t)
	f.spawn = func(fn func()) { fn() }
	f.config.Worker.DefaultWorkerCPURequest = 4000
	f.config.Worker.DefaultWorkerMemoryRequest = 8192

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, workers, 1)
	return &waitingFixture{growFixture: f, request: request, worker: workers[0]}
}

// waits reports whether the request goes on waiting for a worker, in a pass
// that plans with the workers there are now.
func (f *waitingFixture) waits(t *testing.T) bool {
	t.Helper()
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	return f.provisioning.reserveCapacity(f.Scheduler, workers, f.request)
}

// A request waits for the worker its pool added until the worker starts or
// is removed. A machine that was bought and that its provider took back, or
// failed to start, is removed like that; the request then asks for another
// worker at once instead of waiting out the time a worker has to start.
func TestRequestStopsWaitingForAWorkerThatWasRemoved(t *testing.T) {
	f := newWaitingFixture(t)
	f.clock.Advance(time.Minute)
	require.True(t, f.waits(t), "the worker is starting")

	require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
	require.False(t, f.waits(t), "the worker is gone")

	newSchedulingAttempt(f.Scheduler, f.request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount(), "another worker is asked for at once")
	require.True(t, f.waits(t), "and waited for")
}

// The pass that asks for a worker, and those that had read the workers just
// before it was added, plan without it.
func TestRequestWaitsForAWorkerThatWasJustAdded(t *testing.T) {
	f := newWaitingFixture(t)
	before := []*types.Worker{}

	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request))
	f.clock.Advance(workerListLag - time.Millisecond)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request))

	f.clock.Advance(time.Millisecond)
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request), "a list that old would have it")
}

// A pool can take its time to add a worker: a machine is bought before the
// worker exists. Until the pool answers there is no worker to find missing.
func TestRequestWaitsWhileItsPoolIsStillAddingTheWorker(t *testing.T) {
	f := newGrowFixture(t)
	pool := f.pools["a"]
	pool.addWorkerStarted, pool.addWorkerDone = make(chan struct{}, 1), make(chan struct{}, 1)
	pool.unblockAddWorker = make(chan struct{})

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	awaitN(t, pool.addWorkerStarted, 1, "the pool was asked")

	f.clock.Advance(time.Minute)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, nil, request))

	close(pool.unblockAddWorker)
	awaitN(t, pool.addWorkerDone, 1, "the pool answered")
}

// A worker that is removed takes every request that waited for it along,
// whichever of them finds out.
func TestEveryRequestThatWaitedForARemovedWorkerAsksAgain(t *testing.T) {
	f := newWaitingFixture(t)
	f.clock.Advance(time.Minute)

	// A second request finds room on the worker that is starting.
	second := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, second))

	require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, nil, second), "the second request finds the worker gone")

	// The first request, which holds the lock on asking, is free to ask.
	newSchedulingAttempt(f.Scheduler, f.request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount())
}

// A worker that was already starting when a request found it is waited for
// in the same way, and given up on in the same way.
func TestRequestStopsWaitingForAStartingWorkerItFoundThatWasRemoved(t *testing.T) {
	f := newGrowFixture(t)
	starting := &types.Worker{
		Id: "starting", PoolName: "a", Status: types.WorkerStatusPending,
		TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192, CreatedAt: f.clock.Now().Unix(),
	}
	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}

	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, []*types.Worker{starting}, request))
	f.clock.Advance(workerListLag)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, []*types.Worker{starting}, request))
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, nil, request))
}

// A pool of machines that joined by hand answers with the machine's worker,
// which may be running already. The request waits for room on it as long as
// it would wait for a worker to start, and then asks again.
func TestRequestWaitsForARunningWorkerItsPoolAnsweredWith(t *testing.T) {
	f := newGrowFixture(t)
	f.spawn = func(fn func()) { fn() }
	f.pools["a"].workerStatus = types.WorkerStatusAvailable

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	waits := func() bool {
		workers, err := f.workerRepo.GetAllWorkers()
		require.NoError(t, err)
		return f.provisioning.reserveCapacity(f.Scheduler, workers, request)
	}

	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	f.clock.Advance(pendingWorkerReservationTTL - time.Second)
	require.True(t, waits(), "the pool has no other worker to answer with")
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	f.clock.Advance(time.Second)
	require.False(t, waits(), "the wait is over")
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount(), "the pool is asked again")
}

// A second request that is answered with the worker another one waits for
// joins that wait; neither is forgotten.
func TestRequestsAnsweredWithTheSameWorkerWaitTogether(t *testing.T) {
	f := newGrowFixture(t)

	machine := &types.Worker{
		Id: "machine", PoolName: "a", Status: types.WorkerStatusAvailable,
		TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16384, FreeMemory: 16384,
	}
	first := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024}
	second := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024}
	pool, ok := f.workerPoolManager.GetPool("a")
	require.True(t, ok)
	for _, request := range []*types.ContainerRequest{first, second} {
		reservation := f.provisioning.addReservation(f.Scheduler, request, pool, workerSize{cpu: 1000, memory: 1024})
		f.provisioning.handoff(f.Scheduler, reservation, machine)
	}

	workers := []*types.Worker{machine}
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, first))
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, second))

	f.clock.Advance(pendingWorkerReservationTTL)
	require.Empty(t, f.provisioning.requestReservations, "the wait ends for both")
	require.Empty(t, f.provisioning.reservations)
}

// A request that asked a pool for a worker holds a lock that keeps it from
// asking again while it waits. The lock is released as soon as the wait is
// over, whatever ended it.
func TestGrowLockIsReleasedWhenTheRequestStopsWaiting(t *testing.T) {
	for _, test := range []struct {
		name string
		end  func(t *testing.T, f *waitingFixture)
	}{
		{"the worker was removed", func(t *testing.T, f *waitingFixture) {
			require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
			require.False(t, f.waits(t))
		}},
		{"the worker stalled", func(t *testing.T, f *waitingFixture) {
			f.workerPoolManager.SetPool("a", types.WorkerPoolConfig{Priority: 30, StalledAfter: types.Duration(time.Minute)}, f.pools["a"])
			require.False(t, f.waits(t))
		}},
		{"the worker took too long to start", func(t *testing.T, f *waitingFixture) {
			f.clock.Advance(pendingWorkerReservationTTL)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newWaitingFixture(t)
			lock := common.RedisKeys.SchedulerWorkerProvisioningLock(f.request.ContainerId)
			held := func() bool { return f.requestBacklog.rdb.Exists(context.Background(), lock).Val() == 1 }

			f.clock.Advance(time.Minute)
			require.True(t, f.waits(t))
			require.True(t, held(), "the request waits")

			test.end(t, f)
			require.False(t, held(), "the request stopped waiting")
		})
	}
}

// A pool that failed to add a worker is left alone for a while. A request
// with no other pool to ask is planned again as soon as any request that
// waits: a container that finishes meanwhile leaves room for it.
func TestRequestWhosePoolsAreLeftAloneIsPlannedAgainSoon(t *testing.T) {
	f := newGrowFixture(t)
	f.requestBacklog.now = f.clock.Now
	for name := range f.pools {
		f.workerProvisioningBackoff.failed(name, f.now())
	}
	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	newSchedulingAttempt(f.Scheduler, request, nil).grow()

	f.clock.Advance(provisioningWorkerRequeueDelay)
	waiting, err := f.requestBacklog.PopN(1)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
}

// flakyPools fails the next reads of a pool's state.
type flakyPools struct {
	repo.ComputeRepository
	failures int
}

func (r *flakyPools) GetPoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	if r.failures > 0 {
		r.failures--
		return nil, errors.New("redis timed out")
	}
	return r.ComputeRepository.GetPoolState(ctx, workspaceID, name)
}

// A request fails for want of a pool only when no pool accepts it. Failing
// to read the pool it names says nothing about the request, which is tried
// again.
func TestGrowTriesAgainWhenThePoolCannotBeLookedUp(t *testing.T) {
	f := newGrowFixture(t)
	pools := &flakyPools{ComputeRepository: f.computeRepo}
	f.computeRepo = pools

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(), WorkspaceId: "workspace", PoolSelector: "a",
		Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now(),
	}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	pools.failures = 1
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable(), "the request is still to be scheduled")
	require.Zero(t, f.pools["a"].AddWorkerCallCount())

	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	nowhere := &types.ContainerRequest{ContainerId: uuid.NewString(), PoolSelector: "no-such-pool", Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, nowhere)
	newSchedulingAttempt(f.Scheduler, nowhere, nil).grow()
	require.False(t, newSchedulingAttempt(f.Scheduler, nowhere, nil).runnable(), "no pool accepts it")
}

func TestWorkerStalled(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	created := func(ago time.Duration) int64 { return now.Add(-ago).Unix() }

	for _, test := range []struct {
		name         string
		worker       types.Worker
		stalledAfter time.Duration
		want         bool
	}{
		{"pending for less than stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(119 * time.Second)}, 2 * time.Minute, false},
		{"pending for stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(2 * time.Minute)}, 2 * time.Minute, true},
		{"pending for longer", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(time.Hour)}, 2 * time.Minute, true},
		{"the pool has no stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(time.Hour)}, 0, false},
		{"a worker that started is not stalled, however old", types.Worker{Status: types.WorkerStatusAvailable, CreatedAt: created(time.Hour)}, 2 * time.Minute, false},
		{"a worker from before workers recorded their age", types.Worker{Status: types.WorkerStatusPending}, 2 * time.Minute, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, workerStalled(&test.worker, test.stalledAfter, now))
		})
	}
}

func TestStallCheckReusesItsAnswerForAnInterval(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	looks := 0
	answer := true
	look := func() (bool, error) { looks++; return answer, nil }

	var check stallCheck
	stalled, err := check.stalledAt(now, look)
	require.NoError(t, err)
	require.True(t, stalled)

	// The pool's workers started, but the answer is not looked up again yet.
	answer = false
	stalled, _ = check.stalledAt(now.Add(stallCheckInterval-time.Millisecond), look)
	require.True(t, stalled)
	require.Equal(t, 1, looks)

	stalled, _ = check.stalledAt(now.Add(stallCheckInterval), look)
	require.False(t, stalled)
	require.Equal(t, 2, looks)
}

func TestStallCheckDoesNotRememberAFailedLook(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var check stallCheck

	_, err := check.stalledAt(now, func() (bool, error) { return false, errors.New("redis is down") })
	require.Error(t, err)

	stalled, err := check.stalledAt(now, func() (bool, error) { return true, nil })
	require.NoError(t, err)
	require.True(t, stalled)
}

// A request stops waiting for a worker only when the worker's own pool says
// so: the pool's stalledAfter, read from the pool the worker belongs to.
func TestSchedulerStopsWaitingForStalledWorkersOnly(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	s.workerPoolManager.SetPool("patient", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{name: "patient"})
	s.workerPoolManager.SetPool("impatient", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "impatient"})

	now := s.now()
	old := now.Add(-time.Hour).Unix()
	workers := []*types.Worker{
		{Id: "patient-old", PoolName: "patient", Status: types.WorkerStatusPending, CreatedAt: old},
		{Id: "impatient-old", PoolName: "impatient", Status: types.WorkerStatusPending, CreatedAt: old},
		{Id: "impatient-new", PoolName: "impatient", Status: types.WorkerStatusPending, CreatedAt: now.Unix()},
		{Id: "impatient-running", PoolName: "impatient", Status: types.WorkerStatusAvailable, CreatedAt: old},
		{Id: "unknown-pool", PoolName: "deleted", Status: types.WorkerStatusPending, CreatedAt: old},
	}

	kept := []string{}
	for _, worker := range s.filterStalledWorkers(workers) {
		kept = append(kept, worker.Id)
	}
	require.Equal(t, []string{"patient-old", "impatient-new", "impatient-running", "unknown-pool"}, kept)
	require.Len(t, workers, 5, "the caller's list is left as it was")
	require.Equal(t, "impatient-old", workers[1].Id)
}

// A workspace's own pool may have the name of a shared pool. Its workers
// follow their own pool's stalledAfter, not the shared pool's.
func TestWorkerOfAPrivatePoolIsNotStalledByASharedPoolOfTheSameName(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	s.workerPoolManager.SetPool("gpu", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "gpu"})
	require.NoError(t, s.EnsureAgentPool("tenant", &compute.PoolState{Name: "gpu", Selector: "gpu", Mode: string(types.PoolModePrivate)}))

	now := s.now()
	old := now.Add(-time.Hour).Unix()
	shared := &types.Worker{Id: "shared-1", Status: types.WorkerStatusPending, PoolName: "gpu", CreatedAt: old}
	private := &types.Worker{Id: "private-1", Status: types.WorkerStatusPending, PoolName: "gpu", PoolSelector: "gpu", WorkspaceId: "tenant", CreatedAt: old}

	require.True(t, s.stalled(shared, now))
	require.False(t, s.stalled(private, now))
}

// A worker that has been starting for too long is not capacity to wait for:
// a request that finds only that worker goes on to ask a pool for another.
func TestRequestDoesNotWaitForAStalledWorker(t *testing.T) {
	now := time.Now()
	pending := func(age time.Duration) *types.Worker {
		return &types.Worker{
			Id: "worker", PoolName: "impatient", Status: types.WorkerStatusPending,
			TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192,
			CreatedAt: now.Add(-age).Unix(),
		}
	}
	waits := func(t *testing.T, s *Scheduler, request *types.ContainerRequest, worker *types.Worker) bool {
		t.Helper()
		return s.provisioning.reserveCapacity(s, []*types.Worker{worker}, request)
	}
	fixture := func(t *testing.T) (*Scheduler, *types.ContainerRequest) {
		t.Helper()
		s, err := NewSchedulerForTest()
		require.NoError(t, err)
		s.workerPoolManager = NewWorkerPoolManager()
		s.workerPoolManager.SetPool("impatient", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "impatient"})
		return s, &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: now}
	}

	t.Run("a worker that is starting is waited for", func(t *testing.T) {
		s, request := fixture(t)
		require.True(t, waits(t, s, request, pending(10*time.Second)))
	})

	t.Run("a stalled worker is not", func(t *testing.T) {
		s, request := fixture(t)
		require.False(t, waits(t, s, request, pending(time.Hour)))
	})

	t.Run("a request stops waiting when its worker stalls", func(t *testing.T) {
		s, request := fixture(t)
		worker := pending(50 * time.Second)
		require.True(t, waits(t, s, request, worker))

		worker.CreatedAt -= 10
		require.False(t, waits(t, s, request, worker))
	})
}

// An agent pool is the machines that joined it. Work can be placed on a
// machine that is connected and runs workers in containers; the pool counts
// every machine, and looks after the worker of each.

// agentPoolFixture is a pool of four machines:
//
//	ready      connected, its worker is up
//	starting   connected, its worker has not started yet
//	silent     not heard from for too long, with the most room of all
//	no-workers connected, but it does not run workers in containers
type agentPoolFixture struct {
	ctx        context.Context
	workers    *failingWorkerRepo
	machines   repo.ComputeRepository
	controller *AgentWorkerPoolController
}

const (
	agentPoolWorkspace = "workspace"
	agentPoolName      = "rack"
)

func newAgentPoolFixture(t *testing.T) *agentPoolFixture {
	t.Helper()
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f := &agentPoolFixture{
		ctx:      ctx,
		workers:  &failingWorkerRepo{WorkerRepository: repo.NewWorkerRedisRepositoryForTest(rdb)},
		machines: repo.NewComputeRedisRepository(rdb),
	}

	now := time.Now()
	for _, machine := range []*compute.AgentTokenState{
		{MachineID: "ready", CPUMillicores: 8000, MemoryMB: 16000, LastHeartbeatAt: now, Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "starting", CPUMillicores: 4000, MemoryMB: 8000, LastHeartbeatAt: now, Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "silent", CPUMillicores: 64000, MemoryMB: 128000, LastHeartbeatAt: now.Add(-2 * compute.AgentHeartbeatTimeout), Executor: types.DefaultAgentWorkerContainerMode},
		{MachineID: "no-workers", CPUMillicores: 32000, MemoryMB: 64000, LastHeartbeatAt: now, Executor: "process"},
	} {
		machine.TokenHash, machine.WorkspaceID, machine.PoolName, machine.Schedulable = "token-of-"+machine.MachineID, agentPoolWorkspace, agentPoolName, true
		require.NoError(t, f.machines.SaveAgentTokenState(ctx, machine, time.Hour))
	}

	f.controller, err = NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: ctx, Name: agentPoolName, WorkspaceID: agentPoolWorkspace,
		WorkerPool: types.WorkerPoolConfig{Mode: types.PoolModeExternal},
		PoolState:  &compute.PoolState{Name: agentPoolName, Selector: agentPoolName, Mode: string(types.PoolModeExternal)},
		WorkerRepo: f.workers, ComputeRepo: f.machines,
	})
	require.NoError(t, err)

	// The connected machines get their workers, and the ready one's starts.
	require.NoError(t, f.controller.reconcileMachines())
	require.NoError(t, f.workers.UpdateWorkerStatus(compute.AgentMachineWorkerID("ready"), types.WorkerStatusAvailable))
	// The silent machine's worker is on record from before it went quiet.
	require.NoError(t, f.workers.AddWorker(&types.Worker{
		Id: compute.AgentMachineWorkerID("silent"), MachineId: "silent", PoolName: agentPoolName, Status: types.WorkerStatusAvailable,
		TotalCpu: 64000, FreeCpu: 64000, TotalMemory: 128000, FreeMemory: 128000,
	}))
	return f
}

func (f *agentPoolFixture) worker(t *testing.T, machineID string) *types.Worker {
	t.Helper()
	worker, err := f.workers.GetWorkerById(compute.AgentMachineWorkerID(machineID))
	require.NoError(t, err)
	return worker
}

func TestAnAgentPoolCountsEveryMachineAndTellsThoseWorkCanBePlacedOn(t *testing.T) {
	f := newAgentPoolFixture(t)

	state, err := f.controller.State()
	require.NoError(t, err)
	require.Equal(t, &types.WorkerPoolState{
		Status:             types.WorkerPoolStatusHealthy,
		RegisteredMachines: 4,
		ReadyMachines:      1,
		PendingMachines:    1,
	}, state)

	// With no machine whose worker is up, the pool is degraded.
	require.NoError(t, f.workers.UpdateWorkerStatus(compute.AgentMachineWorkerID("ready"), types.WorkerStatusPending))
	state, err = f.controller.State()
	require.NoError(t, err)
	require.Equal(t, &types.WorkerPoolState{
		Status:             types.WorkerPoolStatusDegraded,
		RegisteredMachines: 4,
		PendingMachines:    2,
	}, state)

	// A worker that cannot be read is an error, not a machine to count.
	f.workers.getWorker = errors.New("redis is down")
	_, err = f.controller.State()
	require.ErrorIs(t, err, f.workers.getWorker)
}

func TestAnAgentPoolAddsAWorkerOnlyOnAMachineWorkCanBePlacedOn(t *testing.T) {
	f := newAgentPoolFixture(t)

	// Both connected machines have room for a small worker, and only the
	// ready one for a worker of its own size.
	can, err := f.controller.CanAddWorker(1000, 1000, 0)
	require.NoError(t, err)
	require.True(t, can)
	worker, err := f.controller.AddWorker(8000, 1000, 0)
	require.NoError(t, err)
	require.Equal(t, "ready", worker.MachineId)

	// Only the silent machine and the one that runs no workers have room for
	// more than that, and neither can take work.
	can, err = f.controller.CanAddWorker(16000, 1000, 0)
	require.NoError(t, err)
	require.False(t, can)
}

// When no machine has room, the pool says what it had. The scheduler logs
// it as it is for whoever has to find out why a request for a workspace's
// own machines is waiting.
func TestAnAgentPoolWithNoRoomSaysWhatItsMachinesHad(t *testing.T) {
	f := newAgentPoolFixture(t)

	_, err := f.controller.AddWorker(16000, 1000, 0)

	var noRoom *AgentPoolCapacityError
	require.ErrorAs(t, err, &noRoom)
	require.Equal(t, &AgentPoolCapacityError{
		WorkspaceID: agentPoolWorkspace, PoolName: agentPoolName,
		CPU: 16000, Memory: 1000,
		Machines: 4, SchedulableMachines: 2,
		MaxAvailableCPU: 8000, MaxAvailableMemory: 16000,
	}, noRoom)
	require.EqualError(t, err, `no joined agent machine in workspace "workspace" pool "rack" has enough capacity (requested cpu=16000 memory=1000 gpu="" gpu_count=0, schedulable_machines=2/4, max_available_cpu=8000 max_available_memory=16000 max_available_gpu=0)`)

	logged := captureLog(t)
	logAddWorkerFailure(&types.ContainerRequest{ContainerId: "container"}, err)
	var line map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(logged.String())), &line))
	for field, want := range map[string]any{
		"level": "debug", "message": "agent pool capacity unavailable", "container_id": "container",
		"pool_name": "rack", "machines": 4.0, "schedulable_machines": 2.0,
		"max_available_cpu": 8000.0, "max_available_memory": 16000.0, "max_available_gpu": 0.0,
		"error": err.Error(),
	} {
		require.Equal(t, want, line[field], field)
	}

	// A worker that cannot be read does not count towards what was there.
	f.workers.getWorker = errors.New("redis is down")
	require.ErrorAs(t, f.controller.capacityError(16000, 1000, "", 0), &noRoom)
	require.Equal(t, []int{4, 2}, []int{noRoom.Machines, noRoom.SchedulableMachines})
	require.Zero(t, noRoom.MaxAvailableCPU)
}

// The gateway looks at a pool's machines all together when it reconciles
// the pool, and at one alone when that machine's agent sends a heartbeat.
// Either way a machine is treated the same.
func TestAMachineIsLookedAfterTheSameAloneAsWithItsPool(t *testing.T) {
	alone := func(f *agentPoolFixture, machineID string) error { return f.controller.ensureMachine(machineID) }
	withItsPool := func(f *agentPoolFixture, _ string) error { return f.controller.reconcileMachines() }

	for name, reconcile := range map[string]func(*agentPoolFixture, string) error{"alone": alone, "with its pool": withItsPool} {
		t.Run(name, func(t *testing.T) {
			t.Run("a machine that has gone quiet has its worker disabled", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, reconcile(f, "silent"))
				require.Equal(t, types.WorkerStatusDisabled, f.worker(t, "silent").Status)
			})

			t.Run("and that is an error when its worker cannot be read", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				f.workers.getWorker = errors.New("redis is down")
				require.ErrorIs(t, reconcile(f, "silent"), f.workers.getWorker)
			})

			t.Run("a machine that runs no workers gets none", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, reconcile(f, "no-workers"))
				_, err := f.workers.GetWorkerById(compute.AgentMachineWorkerID("no-workers"))
				var notFound *types.ErrWorkerNotFound
				require.ErrorAs(t, err, &notFound)
			})

			t.Run("a connected machine whose worker is gone gets one that is starting", func(t *testing.T) {
				f := newAgentPoolFixture(t)
				require.NoError(t, f.workers.RemoveWorker(compute.AgentMachineWorkerID("starting")))
				require.NoError(t, reconcile(f, "starting"))
				require.Equal(t, types.WorkerStatusPending, f.worker(t, "starting").Status)
			})
		})
	}

	t.Run("a machine that is not the pool's is not an error", func(t *testing.T) {
		f := newAgentPoolFixture(t)
		require.NoError(t, f.controller.ensureMachine("nobody"))
	})
}

// A machine that dies stops heartbeating and its worker stops sending
// keepalives, while the gateway goes on reconciling the machine's pool. The
// worker's record has to expire all the same: only then do the requests
// queued on it go back to the backlog.
func TestReconcilingAPoolLetsTheWorkerRecordOfADeadMachineExpire(t *testing.T) {
	m := newServingExternalMachine(t, false)
	key := common.RedisKeys.SchedulerWorkerState(m.workerID)
	// Most of the record's lifetime passes with no keepalive.
	const left = 5 * time.Second
	timePasses := func() { require.NoError(t, m.rdb.Expire(m.ctx, key, left).Err()) }
	lifetime := func() time.Duration { return m.rdb.TTL(m.ctx, key).Val() }

	// The machine dies with a request queued on its worker.
	request := &types.ContainerRequest{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	require.NoError(t, repo.NewContainerRedisRepositoryForTest(m.rdb).SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId, Status: types.ContainerStatusPending, WorkspaceId: request.WorkspaceId,
	}))
	worker, err := m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.NoError(t, m.workers.ScheduleContainerRequests(worker, []*types.ContainerRequest{request}))

	// Its last heartbeat still counts for a while, and a reconcile in that
	// time saves its worker. That gives the record no more time.
	timePasses()
	require.NoError(t, m.controller.reconcileMachines())
	require.Equal(t, left, lifetime())

	// Once the heartbeat is too old the worker is disabled.
	m.state.LastHeartbeatAt = time.Now().Add(-2 * compute.AgentHeartbeatTimeout)
	require.NoError(t, m.machines.SaveAgentTokenState(m.ctx, m.state, time.Hour))
	require.NoError(t, m.controller.reconcileMachines())
	worker, err = m.workers.GetWorkerById(m.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)

	// A worker that is disabled already is left as it is, whether the pool
	// is reconciled or the gateway looks at the machine alone.
	timePasses()
	for range 3 {
		require.NoError(t, m.controller.reconcileMachines())
		require.NoError(t, m.controller.ensureMachine(m.state.MachineID))
		require.Equal(t, left, lifetime())
	}

	// The record expires. Nothing makes it again, and the next look at the
	// workers returns the request to the backlog.
	require.NoError(t, m.rdb.Del(m.ctx, key).Err())
	require.NoError(t, m.controller.reconcileMachines())
	workers, err := m.workers.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	require.Zero(t, m.rdb.LLen(m.ctx, common.RedisKeys.SchedulerWorkerRequests(m.workerID)).Val())
	require.EqualValues(t, 1, m.rdb.ZCard(m.ctx, common.RedisKeys.SchedulerContainerRequests()).Val())
}

// fakeProvisioner sells machines of one size, or refuses to.
type fakeProvisioner struct {
	inStock bool
	err     error
	// sells is the size of every machine it sells.
	sells ProvisionedMachine
	// asked is what it was asked to hold, by CanProvision and Provision.
	asked []MachineNeed
	// sold are the machines it sold. They are booting until removed.
	sold []ProvisionedMachine
}

func (p *fakeProvisioner) CanProvision(ctx context.Context, pool string, needs ...MachineNeed) bool {
	p.asked = append(p.asked, needs...)
	return p.inStock
}

func (p *fakeProvisioner) Provision(ctx context.Context, pool string, needs ...MachineNeed) (*ProvisionedMachine, error) {
	p.asked = append(p.asked, needs...)
	if p.err != nil {
		return nil, p.err
	}
	machine := p.sells
	machine.MachineID = "machine-" + string(rune('a'+len(p.sold)))
	p.sold = append(p.sold, machine)
	return &machine, nil
}

func (p *fakeProvisioner) Booting(ctx context.Context, pool string) ([]ProvisionedMachine, error) {
	return p.sold, p.err
}

func (p *fakeProvisioner) Machine(ctx context.Context, machineID string) (*ProvisionedMachine, error) {
	for _, machine := range p.sold {
		if machine.MachineID == machineID {
			return &machine, p.err
		}
	}
	return nil, p.err
}

// buyingPool is the controller of a pool with a provisioning block, on its
// own Redis.
type buyingPool struct {
	t          *testing.T
	ctx        context.Context
	redis      *miniredis.Miniredis
	workers    repo.WorkerRepository
	machines   repo.ComputeRepository
	controller *AgentWorkerPoolController
}

const buyingWorkspace = "admin-workspace"

func newBuyingPool(t *testing.T, provisioner Provisioner, change ...func(*types.WorkerPoolConfig)) *buyingPool {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	config := types.WorkerPoolConfig{
		GPUType:          "L40S",
		ContainerRuntime: types.ContainerRuntimeRunc.String(),
		Priority:         5,
		Provisioning:     types.WorkerPoolProvisioningConfig{Providers: []string{"verda"}, Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&config)
	}
	config.ApplyProvisioning()

	p := &buyingPool{
		t: t, ctx: context.Background(), redis: server,
		workers:  repo.NewWorkerRedisRepositoryForTest(rdb),
		machines: repo.NewComputeRedisRepository(rdb),
	}
	p.controller, err = NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: p.ctx, Name: "spot-l40s", WorkspaceID: buyingWorkspace, WorkerPool: config,
		Config:      types.AppConfig{Worker: types.WorkerConfig{ImageTag: "v1"}},
		PoolState:   &compute.PoolState{Name: "spot-l40s", Selector: "spot-l40s", Mode: string(types.PoolModeExternal)},
		WorkerRepo:  p.workers,
		ComputeRepo: p.machines,
		Provisioner: provisioner,
		Waitlist:    NewWaitlist(rdb, nil),
	})
	require.NoError(t, err)
	return p
}

func (p *buyingPool) poolWorkers() []*types.Worker {
	p.t.Helper()
	workers, err := p.workers.GetAllWorkersInPool("spot-l40s")
	require.NoError(p.t, err)
	return workers
}

// join is the machine's agent joining the pool and reporting what it has.
func (p *buyingPool) join(machineID string, cpuMillicores int64, memoryMB uint64) {
	p.t.Helper()
	require.NoError(p.t, p.machines.SaveAgentTokenState(p.ctx, &compute.AgentTokenState{
		TokenHash: "agent-token-" + machineID, WorkspaceID: buyingWorkspace, PoolName: "spot-l40s", MachineID: machineID,
		CPUMillicores: cpuMillicores, MemoryMB: memoryMB, GPUs: []string{"L40S"}, GPUCount: 1,
		Executor: types.DefaultAgentWorkerContainerMode, Schedulable: true, LastHeartbeatAt: time.Now(),
		Mode: string(types.PoolModeExternal), ManagedPoolInstanceID: "instance-1",
	}, time.Hour))
	require.NoError(p.t, p.controller.ensureMachine(machineID))
}

var oneL40S = ProvisionedMachine{CPU: 20_000, Memory: 54_000, GPUCount: 1}

func TestAPoolThatBuysAddsAWorkerByBuyingAMachine(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	require.True(t, p.controller.IsPreemptable(), "spot machines can be taken back")

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.True(t, can)

	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, []MachineNeed{{CPU: 2000, Memory: 8192, GPUCount: 1}, {CPU: 2000, Memory: 8192, GPUCount: 1}}, provisioner.asked)

	// The worker stands for the whole machine, under the ID the machine's
	// own worker will have, and is not usable yet.
	require.Equal(t, compute.AgentMachineWorkerID("machine-a"), worker.Id)
	require.Equal(t, "machine-a", worker.MachineId)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
	require.Equal(t, "spot-l40s", worker.PoolName)
	require.Equal(t, "L40S", worker.Gpu)
	require.EqualValues(t, 20_000, worker.TotalCpu)
	require.EqualValues(t, 54_000, worker.TotalMemory)
	require.EqualValues(t, 1, worker.TotalGpuCount)
	require.EqualValues(t, 5, worker.Priority)
	require.True(t, worker.Preemptable)
	require.True(t, worker.ControlPlaneManaged)
	require.Equal(t, buyingWorkspace, worker.WorkspaceId)

	stored := p.poolWorkers()
	require.Len(t, stored, 1)
	require.Equal(t, worker.Id, stored[0].Id)

	// Invariant 3: a booting machine counts as capacity, so the sizer and
	// waiting requests do not buy another.
	capacity, err := p.controller.FreeCapacity()
	require.NoError(t, err)
	require.EqualValues(t, 1, capacity.PendingGpu)
	require.Zero(t, capacity.FreeGpu)
}

func TestAPoolThatBuysAsksForPhysicalCores(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner, func(pool *types.WorkerPoolConfig) { pool.CPUShareMultiplier = 2 })

	// Four schedulable cores take two physical ones, and a machine with
	// twenty physical cores offers forty.
	worker, err := p.controller.AddWorker(4000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, []MachineNeed{{CPU: 2000, Memory: 8192, GPUCount: 1}}, provisioner.asked)
	require.EqualValues(t, 40_000, worker.TotalCpu)
}

func TestAPoolThatBuysCannotGrowWhenNothingCanBeBought(t *testing.T) {
	refused := errors.New("verda has no L40S")
	provisioner := &fakeProvisioner{inStock: false, err: refused}
	p := newBuyingPool(t, provisioner)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)

	_, err = p.controller.AddWorker(2000, 8192, 1)
	require.ErrorIs(t, err, refused)
	require.Empty(t, p.poolWorkers(), "nothing was bought, so nothing stands for it")
}

func TestAPoolThatBuysGrowsNoOtherWay(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: false, sells: oneL40S}
	p := newBuyingPool(t, provisioner)

	// A machine that has joined has its worker already. There is nothing
	// else to bind, so its free room is no way to add a worker.
	p.join("machine-by-hand", 20_000, 58_000)
	require.Len(t, p.poolWorkers(), 1)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)
}

func TestAPoolThatBuysOnAGatewayWithNothingToBuyWith(t *testing.T) {
	p := newBuyingPool(t, nil)

	can, err := p.controller.CanAddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.False(t, can)
	_, err = p.controller.AddWorker(2000, 8192, 1)
	require.ErrorContains(t, err, `pool "spot-l40s" buys its machines, but this gateway has nothing to buy them with`)
	require.NoError(t, p.controller.reconcileMachines())
}

func TestTheWorkerOfABoughtMachineCarriesOverWhenTheMachineJoins(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	bought, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// The machine reports a little more memory than was counted on.
	p.join("machine-a", 20_000, 58_500)

	workers := p.poolWorkers()
	require.Len(t, workers, 1, "the machine has one worker, not two")
	require.Equal(t, bought.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.EqualValues(t, 58_500, workers[0].TotalMemory)
	require.True(t, workers[0].Preemptable)
	require.True(t, workers[0].ControlPlaneManaged)

	// Its worker starts and takes requests.
	require.NoError(t, p.workers.SetWorkerKeepAlive(bought.Id, types.WorkerKeepAlive{MachineId: "machine-a", RequestStreamOpen: true}))
	workers = p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, types.WorkerStatusAvailable, workers[0].Status)
}

// A pool that buys both kinds of machine cannot tell one from the other by
// its config. A machine's worker is of the kind the machine was bought as,
// also when the worker's record is lost after the machine has joined.
func TestTheWorkerOfABoughtMachineIsOfTheKindTheMachineWasBoughtAs(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner, func(pool *types.WorkerPoolConfig) { pool.Provisioning.OnDemand = true })
	require.False(t, p.controller.IsPreemptable())

	for _, spot := range []bool{false, true} {
		provisioner.sells.Spot = spot
		worker, err := p.controller.AddWorker(2000, 8192, 1)
		require.NoError(t, err)
		require.Equal(t, spot, worker.Preemptable)
		p.join(worker.MachineId, 20_000, 58_500)

		require.NoError(t, p.workers.RemoveWorker(worker.Id))
		require.NoError(t, p.controller.reconcileMachines())
		again, err := p.workers.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.Equal(t, spot, again.Preemptable)
	}

	// A machine the pool has no record of is taken for one that may be
	// taken back: nothing that may not be interrupted is placed on it.
	p.join("machine-by-hand", 20_000, 58_500)
	byHand, err := p.workers.GetWorkerById(compute.AgentMachineWorkerID("machine-by-hand"))
	require.NoError(t, err)
	require.True(t, byHand.Preemptable)

	// What it was bought as cannot be read: its worker waits until it can.
	provisioner.err = errors.New("redis is away")
	require.NoError(t, p.workers.RemoveWorker(compute.AgentMachineWorkerID("machine-a")))
	require.ErrorIs(t, p.controller.reconcileMachines(), provisioner.err)
	_, err = p.workers.GetWorkerById(compute.AgentMachineWorkerID("machine-a"))
	require.Error(t, err)
}

func TestBootingMachinesKeepTheirPendingWorker(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// A Pending worker expires after ten minutes. A machine can take longer
	// than that to boot; the pool's reconcile, every thirty seconds, keeps
	// its worker for as long as the machine is booting.
	for range 50 {
		p.redis.FastForward(30 * time.Second)
		require.NoError(t, p.controller.reconcileMachines())
	}
	require.Len(t, p.poolWorkers(), 1)

	// Redis loses the worker: it is made again, since the machine is still
	// coming.
	require.NoError(t, p.workers.RemoveWorker(worker.Id))
	require.Empty(t, p.poolWorkers())
	require.NoError(t, p.controller.reconcileMachines())
	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, worker.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.EqualValues(t, 54_000, workers[0].TotalMemory)

	// The machine is given up: its worker is no longer kept.
	provisioner.sold = nil
	for range 21 {
		p.redis.FastForward(30 * time.Second)
		require.NoError(t, p.controller.reconcileMachines())
	}
	require.Empty(t, p.poolWorkers())
}

func TestKeepingBootingWorkersLeavesJoinedMachinesToTheirAgent(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	// The machine has joined and its worker is up, but the provisioner has
	// not seen that yet and still lists it as booting.
	p.join("machine-a", 20_000, 58_500)
	require.NoError(t, p.workers.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{MachineId: "machine-a", RequestStreamOpen: true}))
	require.NoError(t, p.controller.reconcileMachines())

	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, types.WorkerStatusAvailable, workers[0].Status, "the worker is not set back to pending")
	require.EqualValues(t, 58_500, workers[0].TotalMemory, "nor to the size that was counted on")
}

func TestKeepBootingWorkersReportsAProvisionerItCannotAsk(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	_, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)

	provisioner.err = errors.New("redis is down")
	require.ErrorContains(t, p.controller.reconcileMachines(), "redis is down")
	require.Len(t, p.poolWorkers(), 1)
}

// A request asked for a small worker and the pool bought a machine. What is
// left of the machine is there for other requests to wait on.
func TestTakeCapacityOf(t *testing.T) {
	// The worker the request was going to get, with the request on it.
	asked := func() *types.Worker {
		return &types.Worker{
			TotalCpu: 2000, FreeCpu: 0, TotalMemory: 8192, FreeMemory: 0, TotalGpuCount: 1, FreeGpuCount: 0,
		}
	}

	tests := []struct {
		name        string
		placeholder *types.Worker
		added       *types.Worker
		want        types.Worker
	}{
		{
			name:        "the worker that was asked for",
			placeholder: asked(),
			added:       &types.Worker{TotalCpu: 2000, FreeCpu: 2000, TotalMemory: 8192, FreeMemory: 8192, TotalGpuCount: 1, FreeGpuCount: 1},
			want:        types.Worker{TotalCpu: 2000, TotalMemory: 8192, TotalGpuCount: 1},
		},
		{
			name:        "a machine with more of everything",
			placeholder: asked(),
			added:       &types.Worker{Gpu: "L40S", TotalCpu: 40_000, FreeCpu: 40_000, TotalMemory: 108_000, FreeMemory: 108_000, TotalGpuCount: 2, FreeGpuCount: 2},
			want:        types.Worker{Gpu: "L40S", TotalCpu: 40_000, FreeCpu: 38_000, TotalMemory: 108_000, FreeMemory: 99_808, TotalGpuCount: 2, FreeGpuCount: 1},
		},
		{
			name:        "a worker that came back smaller than asked for",
			placeholder: asked(),
			added:       &types.Worker{TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 4096, FreeMemory: 4096},
			want:        types.Worker{TotalCpu: 1000, TotalMemory: 4096},
		},
		{
			name:        "the placeholder's GPU type stays when the worker names none",
			placeholder: &types.Worker{Gpu: "L40S", TotalCpu: 2000, TotalGpuCount: 1},
			added:       &types.Worker{TotalCpu: 20_000, FreeCpu: 20_000, TotalGpuCount: 1, FreeGpuCount: 1},
			want:        types.Worker{Gpu: "L40S", TotalCpu: 20_000, FreeCpu: 18_000, TotalGpuCount: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			takeCapacityOf(test.placeholder, test.added)
			require.Equal(t, test.want, *test.placeholder)
		})
	}
}

func TestHoldsHeadroom(t *testing.T) {
	floor := func(gpus string) types.WorkerPoolConfig {
		return types.WorkerPoolConfig{GPUType: "L40S", PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeGPU: gpus}}
	}
	ready := func(id string, freeGPUs uint32) *types.Worker {
		return &types.Worker{
			Id: id, Status: types.WorkerStatusAvailable, Gpu: "L40S",
			TotalCpu: 20_000, FreeCpu: 20_000, TotalMemory: 54_000, FreeMemory: 54_000,
			TotalGpuCount: 1, FreeGpuCount: freeGPUs,
		}
	}
	pending := ready("a-pending", 1)
	pending.Status = types.WorkerStatusPending
	workers := []*types.Worker{pending, ready("b", 1), ready("c", 0), ready("d", 1), ready("e", 1)}

	tests := []struct {
		name   string
		pool   types.WorkerPoolConfig
		worker *types.Worker
		want   bool
	}{
		{"the first ready worker holds a floor of one", floor("1"), workers[1], true},
		{"the next free one does not", floor("1"), workers[3], false},
		{"a busy worker ahead holds nothing, so the next one does", floor("2"), workers[3], true},
		{"the floor is met before the last", floor("2"), workers[4], false},
		{"a pending worker holds nothing", floor("1"), pending, false},
		{"no floor, nothing to hold", floor(""), workers[1], false},
		{"no worker", floor("1"), nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, HoldsHeadroom(test.pool, workers, test.worker))
		})
	}
}

// failingAdds is a worker repository that fails to write the next workers
// it is given.
type failingAdds struct {
	repo.WorkerRepository
	failures int
}

func (r *failingAdds) AddWorker(worker *types.Worker) error {
	if r.failures > 0 {
		r.failures--
		return errors.New("redis is down")
	}
	return r.WorkerRepository.AddWorker(worker)
}

// A machine that was bought is paid for, whatever becomes of the worker that
// stands for it. Reporting it as a worker that could not be added would have
// the scheduler ask the pool again, and the pool buy a second machine.
func TestAMachineWhoseWorkerCouldNotBeWrittenIsNotReportedAsAFailure(t *testing.T) {
	provisioner := &fakeProvisioner{inStock: true, sells: oneL40S}
	p := newBuyingPool(t, provisioner)
	p.controller.workerRepo = &failingAdds{WorkerRepository: p.workers, failures: 1}

	worker, err := p.controller.AddWorker(2000, 8192, 1)
	require.NoError(t, err)
	require.Equal(t, compute.AgentMachineWorkerID("machine-a"), worker.Id)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
	require.Len(t, provisioner.sold, 1)
	require.Empty(t, p.poolWorkers(), "the worker was not written")

	// The machine is still booting, so the pool's reconcile makes its
	// worker again.
	require.NoError(t, p.controller.reconcileMachines())
	workers := p.poolWorkers()
	require.Len(t, workers, 1)
	require.Equal(t, worker.Id, workers[0].Id)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.Len(t, provisioner.sold, 1, "nothing more was bought")
}

// A cleanup pass that cannot do its work leaves it for the next pass.

// failingWorkerRepo is a worker repository whose calls fail with the errors
// that are set.
type failingWorkerRepo struct {
	repo.WorkerRepository
	getWorker, listPool error
}

func (r *failingWorkerRepo) GetWorkerById(workerId string) (*types.Worker, error) {
	if r.getWorker != nil {
		return nil, r.getWorker
	}
	return r.WorkerRepository.GetWorkerById(workerId)
}

func (r *failingWorkerRepo) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	if r.listPool != nil {
		return nil, r.listPool
	}
	return r.WorkerRepository.GetAllWorkersInPool(poolName)
}

// cleanerFixture is a cleaner for a pool with one worker Job and its pod.
type cleanerFixture struct {
	cleaner *WorkerResourceCleaner
	kube    *fake.Clientset
	workers *failingWorkerRepo
}

func newCleanerFixture(t *testing.T, phase corev1.PodPhase) *cleanerFixture {
	t.Helper()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	job := cleanerTestWorkerJob("worker-default-w1", "w1", "default", "")
	pod := cleanerTestWorkerPod("worker-default-w1-pod", job.Name, "w1", "default", "", phase)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	f := &cleanerFixture{
		kube:    fake.NewSimpleClientset(job, pod),
		workers: &failingWorkerRepo{WorkerRepository: repo.NewWorkerRedisRepositoryForTest(rdb)},
	}
	f.cleaner = &WorkerResourceCleaner{
		PoolName:   "default",
		Config:     types.WorkerConfig{Namespace: "beta9", CleanupPendingWorkerAgeLimit: 10 * time.Minute},
		KubeClient: f.kube,
		WorkerRepo: f.workers,
	}
	return f
}

// fail makes the cluster refuse a verb on a resource.
func (f *cleanerFixture) fail(verb, resource string, err error) {
	f.kube.PrependReactor(verb, resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, err
	})
}

// addWorker puts a worker of the pool on record.
func (f *cleanerFixture) addWorker(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, f.workers.AddWorker(cleanerTestWorker(id, "default", "")))
}

// What a pass leaves undone, it leaves as it found it.
func TestACleanupPassThatFailsRemovesNothing(t *testing.T) {
	ctx := context.Background()

	t.Run("a job stays when it cannot be told whether its worker is on record", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.workers.getWorker = errors.New("redis is down")

		f.cleaner.Clean(ctx)

		_, err := f.kube.BatchV1().Jobs("beta9").Get(ctx, "worker-default-w1", metav1.GetOptions{})
		require.NoError(t, err)
	})

	t.Run("a worker stays on record when it cannot be told whether it has a job", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.addWorker(t, "w1")
		f.addWorker(t, "no-job")
		f.fail("get", "jobs", errors.New("the cluster said no"))

		f.cleaner.Clean(ctx)

		_, err := f.workers.GetWorkerById("no-job")
		require.NoError(t, err)
	})

	t.Run("and goes once it can", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.addWorker(t, "w1")
		f.addWorker(t, "no-job")
		logged := captureLog(t)

		f.cleaner.Clean(ctx)

		_, err := f.workers.GetWorkerById("no-job")
		var notFound *types.ErrWorkerNotFound
		require.ErrorAs(t, err, &notFound)
		_, err = f.kube.BatchV1().Jobs("beta9").Get(ctx, "worker-default-w1", metav1.GetOptions{})
		require.NoError(t, err, "a job whose worker is on record stays")
		require.NotContains(t, logged.String(), `"level":"error"`, "a pass that does its work reports no failure")
	})
}

// Every scheduler replica runs the cleanup tick for a pool. One cleans up
// after its workers at a time: a replica that finds another at it leaves the
// pass to that one, which is no failure.

// cleanupFixture is a Kubernetes pool with a finished worker Job to clean
// up, and the tick that does it.
type cleanupFixture struct {
	*cleanerFixture
	rdb  *common.RedisClient
	tick func()
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	f := &cleanupFixture{cleanerFixture: newCleanerFixture(t, corev1.PodSucceeded)}
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f.rdb = rdb

	controller := &LocalKubernetesWorkerPoolController{
		ctx:            context.Background(),
		name:           f.cleaner.PoolName,
		workerPoolRepo: repo.NewWorkerPoolRedisRepositoryForTest(rdb),
	}
	f.tick = func() { controller.cleanupTick(f.cleaner) }
	return f
}

func (f *cleanupFixture) jobCleanedUp(t *testing.T) bool {
	t.Helper()
	jobs, err := f.kube.BatchV1().Jobs("beta9").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return len(jobs.Items) == 0
}

func TestTheCleanupTickLeavesThePassToTheReplicaThatIsAtIt(t *testing.T) {
	f := newCleanupFixture(t)
	other := repo.NewWorkerPoolRedisRepositoryForTest(f.rdb)
	logged := captureLog(t)

	require.NoError(t, other.WithWorkerPoolCleanerLock(f.cleaner.PoolName, f.tick))
	require.False(t, f.jobCleanedUp(t))
	require.Empty(t, logged.String())

	f.tick()
	require.True(t, f.jobCleanedUp(t))
}

func TestTheCleanupTickReportsALockItCouldNotAskFor(t *testing.T) {
	f := newCleanupFixture(t)
	require.NoError(t, f.rdb.HSet(context.Background(), common.RedisKeys.WorkerPoolCleanerLock(f.cleaner.PoolName), "not", "a lock").Err())
	logged := captureLog(t)

	f.tick()

	require.False(t, f.jobCleanedUp(t))
	require.Contains(t, logged.String(), `"level":"error"`)
	require.Contains(t, logged.String(), `"pool_name":"default"`)
}

// A Kubernetes pool adds a worker in three steps: a token for the worker, a
// Job that carries the token, and the worker's record. A worker that is not
// added must leave no token that still works.

// workerTokens is the part of the database that adding a worker uses.
type workerTokens struct {
	repo.BackendRepository
	// workspaceErr is why the admin workspace cannot be found, if set.
	workspaceErr error

	mu      sync.Mutex
	created []string
	revoked []string
}

func (b *workerTokens) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	if b.workspaceErr != nil {
		return nil, b.workspaceErr
	}
	return &types.Workspace{Id: 1, ExternalId: "admin"}, nil
}

func (b *workerTokens) CreateWorkerToken(_ context.Context, _ uint, _ string, workerId string) (types.Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := fmt.Sprintf("token-%d", len(b.created)+1)
	b.created = append(b.created, id)
	return types.Token{ExternalId: id, Key: "key-of-" + id, WorkerID: workerId}, nil
}

func (b *workerTokens) RevokeTokenByExternalId(ctx context.Context, externalId string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.revoked = append(b.revoked, externalId)
	return "key-of-" + externalId, nil
}

// unreliableRecords is a worker repository whose AddWorker reports an error,
// after writing the record if written is set.
type unreliableRecords struct {
	repo.WorkerRepository
	err     error
	written bool
}

func (r *unreliableRecords) AddWorker(worker *types.Worker) error {
	if r.written {
		if err := r.WorkerRepository.AddWorker(worker); err != nil {
			return err
		}
	}
	return r.err
}

// localPoolFixture is a Kubernetes pool, with the cluster, the worker records
// and the tokens it adds workers to.
type localPoolFixture struct {
	controller *LocalKubernetesWorkerPoolController
	kube       *fake.Clientset
	tokens     *workerTokens
	workers    repo.WorkerRepository
	stop       context.CancelFunc
}

func newLocalPoolFixture(t *testing.T) *localPoolFixture {
	t.Helper()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := &localPoolFixture{
		kube:    fake.NewSimpleClientset(),
		tokens:  &workerTokens{},
		workers: repo.NewWorkerRedisRepositoryForTest(rdb),
		stop:    cancel,
	}
	config := types.AppConfig{}
	config.Worker.Namespace = "beta9"
	f.controller = &LocalKubernetesWorkerPoolController{
		ctx:         ctx,
		name:        "cpu",
		config:      config,
		kubeClient:  f.kube,
		workerRepo:  f.workers,
		backendRepo: f.tokens,
	}
	return f
}

func (f *localPoolFixture) jobs(t *testing.T) []batchv1.Job {
	t.Helper()
	jobs, err := f.kube.BatchV1().Jobs("beta9").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return jobs.Items
}

func (f *localPoolFixture) workersOnRecord(t *testing.T) []*types.Worker {
	t.Helper()
	workers, err := f.workers.GetAllWorkersInPool("cpu")
	require.NoError(t, err)
	return workers
}

func TestAWorkerThatIsAddedKeepsItsToken(t *testing.T) {
	f := newLocalPoolFixture(t)

	worker, err := f.controller.AddWorker(1000, 1024, 0)

	require.NoError(t, err)
	require.Equal(t, []string{"token-1"}, f.tokens.created)
	require.Empty(t, f.tokens.revoked)
	require.Len(t, f.jobs(t), 1)
	onRecord := f.workersOnRecord(t)
	require.Len(t, onRecord, 1)
	require.Equal(t, worker.Id, onRecord[0].Id)
}

func TestAWorkerWhoseJobTheClusterRefusesLeavesNoTokenThatWorks(t *testing.T) {
	f := newLocalPoolFixture(t)
	refused := errors.New("exceeded quota")
	f.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, refused
	})

	// The pool's sizer asks again every second for as long as it is short.
	for range 3 {
		_, err := f.controller.AddWorker(1000, 1024, 0)
		require.ErrorIs(t, err, refused)
	}

	require.Equal(t, []string{"token-1", "token-2", "token-3"}, f.tokens.created)
	require.Equal(t, f.tokens.created, f.tokens.revoked)
	require.Empty(t, f.workersOnRecord(t))
}

func TestAWorkerThatCannotBePutOnRecordLeavesNoTokenThatWorks(t *testing.T) {
	f := newLocalPoolFixture(t)
	redisDown := errors.New("redis is down")
	f.controller.workerRepo = &unreliableRecords{WorkerRepository: f.workers, err: redisDown}

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, redisDown)
	require.Equal(t, []string{"token-1"}, f.tokens.revoked)
	require.Empty(t, f.workersOnRecord(t))
	// The Job is left to the cleaner, which removes a Job that has no worker
	// on record.
	require.Len(t, f.jobs(t), 1)
}

// Putting a worker on record can report an error after the record is
// written: when the lock that guards the write cannot be released, say. The
// scheduler then has a worker to wait for, and the worker has to be able to
// start.
func TestAWorkerThatIsOnRecordKeepsItsTokenThoughAddingItReportedAnError(t *testing.T) {
	f := newLocalPoolFixture(t)
	lockLost := errors.New("lock was lost")
	f.controller.workerRepo = &unreliableRecords{WorkerRepository: f.workers, err: lockLost, written: true}

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, lockLost)
	require.Len(t, f.workersOnRecord(t), 1)
	require.Empty(t, f.tokens.revoked)
}

func TestAddingAWorkerSaysWhyTheWorkspaceCouldNotBeFound(t *testing.T) {
	f := newLocalPoolFixture(t)
	f.tokens.workspaceErr = errors.New("connection refused")

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, f.tokens.workspaceErr)
	require.ErrorContains(t, err, "cpu", "the error names the pool")
	require.Empty(t, f.tokens.created)
	require.Empty(t, f.jobs(t))
}

// A pool's sizer and the requests that grow the pool add workers at the same
// time. Run with -race.
func TestWorkersAreAddedAtTheSameTime(t *testing.T) {
	f := newLocalPoolFixture(t)
	const callers = 8

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.controller.AddWorker(1000, 1024, 0)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, f.jobs(t), callers)
	require.Len(t, f.workersOnRecord(t), callers)
}

// silentCluster is a cluster that does not answer when a Job is created,
// until the caller gives up.
type silentCluster struct {
	kubernetes.Interface
	asked chan context.Context
}

func (c *silentCluster) BatchV1() typedbatchv1.BatchV1Interface {
	return &silentBatch{BatchV1Interface: c.Interface.BatchV1(), cluster: c}
}

type silentBatch struct {
	typedbatchv1.BatchV1Interface
	cluster *silentCluster
}

func (b *silentBatch) Jobs(namespace string) typedbatchv1.JobInterface {
	return &silentJobs{JobInterface: b.BatchV1Interface.Jobs(namespace), cluster: b.cluster}
}

type silentJobs struct {
	typedbatchv1.JobInterface
	cluster *silentCluster
}

func (j *silentJobs) Create(ctx context.Context, _ *batchv1.Job, _ metav1.CreateOptions) (*batchv1.Job, error) {
	j.cluster.asked <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAddingAWorkerDoesNotWaitForeverForTheCluster(t *testing.T) {
	f := newLocalPoolFixture(t)
	cluster := &silentCluster{Interface: f.kube, asked: make(chan context.Context, 1)}
	f.controller.kubeClient = cluster

	result := make(chan error, 1)
	go func() {
		_, err := f.controller.AddWorker(1000, 1024, 0)
		result <- err
	}()

	// The cluster is given a limited time to answer.
	var asked context.Context
	select {
	case asked = <-cluster.asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the cluster was not asked for a Job")
	}
	deadline, limited := asked.Deadline()
	require.True(t, limited)
	require.WithinDuration(t, time.Now().Add(workerJobCreateTimeout), deadline, 5*time.Second)

	// And the wait ends with the pool, however long is left.
	f.stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("adding a worker outlived its pool")
	}
	require.Equal(t, []string{"token-1"}, f.tokens.revoked, "the token is revoked though the pool's context has ended")
}
