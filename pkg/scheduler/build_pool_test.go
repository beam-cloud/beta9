package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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

func TestBuildPoolExcludedFromFailover(t *testing.T) {
	scheduler := buildPoolSchedulerForTest("build")
	scheduler.config.Scheduling.Failover = types.FailoverConfig{
		Enabled: true,
		Chains:  map[string]types.FailoverChain{"A10G": {Pools: []string{"build"}}},
	}
	scheduler.workerPoolManager.SetPool("build", types.WorkerPoolConfig{GPUType: "RTX4090"}, &LocalWorkerPoolControllerForTest{name: "build", requiresSelector: true})
	request := &types.ContainerRequest{Gpu: "A10G", GpuCount: 1}
	controllers, err := scheduler.getControllers(request)
	require.Error(t, err)
	require.Empty(t, controllers)
	selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{failoverWorker("build-worker", "build", "RTX4090", 1)}, request)
	require.Error(t, err)
	require.Nil(t, selected)
}

func TestBuildPoolLeavesOtherRuncPoolsAvailable(t *testing.T) {
	scheduler := buildPoolSchedulerForTest("build")
	controller := &LocalWorkerPoolControllerForTest{name: "default-ovh", requiresSelector: true, containerRuntime: types.ContainerRuntimeRunc.String()}
	scheduler.workerPoolManager.SetPool(controller.Name(), types.WorkerPoolConfig{ContainerRuntime: types.ContainerRuntimeRunc.String()}, controller)
	request := &types.ContainerRequest{PoolSelector: controller.Name()}
	controllers, err := scheduler.getControllers(request)
	require.NoError(t, err)
	require.Equal(t, []WorkerPoolController{controller}, controllers)

	worker := &types.Worker{Id: "ovh-worker", PoolName: controller.Name(), RequiresPoolSelector: true, Runtime: types.ContainerRuntimeRunc.String(), Status: types.WorkerStatusAvailable}
	selected, err := scheduler.selectWorkerFromWorkers([]*types.Worker{worker}, request)
	require.NoError(t, err)
	require.Equal(t, worker.Id, selected.Id)
}
