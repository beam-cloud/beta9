package scheduler

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestCPUSharingKeepsWorkerHostLimitsPhysical(t *testing.T) {
	pool := types.WorkerPoolConfig{ContainerRuntimeConfig: types.RuntimeConfig{CPUShareMultiplier: 1.5}}
	config := types.AppConfig{Worker: types.WorkerConfig{JobResourcesEnforced: true, TmpSizeLimit: "1Gi"}}
	local := &LocalKubernetesWorkerPoolController{workerPoolConfig: pool, config: config}
	job, worker := local.createWorkerJob("local", 4000, 16384, "", 0, "token")
	require.EqualValues(t, 6000, worker.TotalCpu)
	require.EqualValues(t, 6000, worker.FreeCpu)
	require.EqualValues(t, 4000, worker.PhysicalCPU())
	require.EqualValues(t, 16384, worker.TotalMemory)
	container := job.Spec.Template.Spec.Containers[0]
	require.EqualValues(t, 4000, container.Resources.Limits.Cpu().MilliValue())
	require.Contains(t, container.Env, corev1.EnvVar{Name: types.WorkerCPUEnv, Value: "4000"})
}

func TestCPUSharingAdmissionStillReservesVMRAM(t *testing.T) {
	for _, tc := range []struct {
		name       string
		multiplier float64
		memory     uint64
		fits       int
	}{{"default", 0, 22000, 1}, {"shared", 2, 22000, 2}, {"RAM bound", 2, 11000, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSchedulerForTest()
			require.NoError(t, err)
			controller := &AgentWorkerPoolController{workerRepo: s.workerRepo, name: "shared", workerPoolConfig: types.WorkerPoolConfig{
				ContainerRuntime:       types.ContainerRuntimeMicroVM.String(),
				ContainerRuntimeConfig: types.RuntimeConfig{CPUShareMultiplier: tc.multiplier},
			}}
			machine := &compute.AgentTokenState{MachineID: "host", CPUCount: 4, MemoryMB: tc.memory}
			worker, err := controller.ensureMachineWorker(machine)
			require.NoError(t, err)
			require.NoError(t, s.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusAvailable))
			for i := 0; i <= tc.fits; i++ {
				worker, err = s.workerRepo.GetWorkerById(worker.Id)
				require.NoError(t, err)
				require.EqualValues(t, 4000, worker.PhysicalCPU(), "physical capacity survives Redis")
				request := &types.ContainerRequest{Cpu: 4000, Memory: 8192, UseVM: true}
				request.Stub.Type = types.StubType(types.StubTypeVM)
				setPendingSchedulerRequests(t, s, request)
				eligible := filterWorkersByResources([]*types.Worker{worker}, request, nil)
				err = s.workerRepo.ScheduleContainerRequest(worker, request)
				if i == tc.fits {
					require.Empty(t, eligible)
					require.Error(t, err)
				} else {
					require.Len(t, eligible, 1, "iteration %d: worker %+v", i, worker)
					require.NoError(t, err)
					require.EqualValues(t, 4000, request.Cpu, "guest CPU and billing stay unchanged")
					require.EqualValues(t, 8192, request.Memory)
				}
			}
		})
	}
}

func TestCPUSharingDecreaseReconcilesOutstandingReservations(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	workers := repository.NewWorkerRedisRepositoryForTest(rdb)
	controller := &AgentWorkerPoolController{workerRepo: workers, name: "shared", workerPoolConfig: types.WorkerPoolConfig{
		ContainerRuntimeConfig: types.RuntimeConfig{CPUShareMultiplier: 3},
	}}
	machine := &compute.AgentTokenState{MachineID: "host", CPUCount: 4, MemoryMB: 32768}
	worker, err := controller.ensureMachineWorker(machine)
	require.NoError(t, err)
	index := common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)
	for _, id := range []string{"a", "b", "c"} {
		state := &types.ContainerState{ContainerId: id, WorkerId: worker.Id, Cpu: 4000, Memory: 1024, Status: types.ContainerStatusRunning}
		key := common.RedisKeys.SchedulerContainerState(id)
		require.NoError(t, rdb.HSet(ctx, key, common.ToSlice(state)).Err())
		require.NoError(t, rdb.SAdd(ctx, index, key).Err())
	}
	controller.workerPoolConfig.ContainerRuntimeConfig.CPUShareMultiplier = 1
	worker, err = controller.ensureMachineWorker(machine)
	require.NoError(t, err)
	require.EqualValues(t, 4000, worker.TotalCpu)
	for _, id := range []string{"a", "b"} {
		require.Zero(t, worker.FreeCpu, "no new CPU capacity while existing reservations exceed the lower ceiling")
		key := common.RedisKeys.SchedulerContainerState(id)
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.SRem(ctx, index, key).Err())
		require.NoError(t, workers.UpdateWorkerCapacity(worker, &types.ContainerRequest{ContainerId: id}, types.AddCapacity))
	}
	require.Zero(t, worker.FreeCpu, "the final running VM still occupies the physical budget")
	require.NoError(t, rdb.Del(ctx, common.RedisKeys.SchedulerContainerState("c")).Err())
	require.NoError(t, workers.UpdateWorkerCapacity(worker, &types.ContainerRequest{ContainerId: "c"}, types.AddCapacity))
	require.EqualValues(t, 4000, worker.FreeCpu)
}

func TestCPUSharingProvisioningRequestsPhysicalCapacity(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.DefaultWorkerCPURequest = 2000
	controller := &LocalWorkerPoolControllerForTest{name: "shared"}
	config := types.WorkerPoolConfig{ContainerRuntimeConfig: types.RuntimeConfig{CPUShareMultiplier: 2}}
	config.PoolSizing.DefaultWorkerCPU = "4000m"
	s.workerPoolManager.SetPool(controller.Name(), config, controller)
	request := &types.ContainerRequest{Cpu: 10001, Memory: 8192}
	require.EqualValues(t, 5001, s.workerCPUForControllerRequest(controller, request))
	reservation := s.newProvisioningReservation(request, controller)
	require.EqualValues(t, 5001, reservation.worker.PhysicalCPU())
	require.EqualValues(t, 10002, reservation.worker.TotalCpu)
	require.EqualValues(t, 10001, request.Cpu)
	request.Cpu = 1000
	require.EqualValues(t, 4000, s.workerCPUForControllerRequest(controller, request), "pool minimum is physical CPU")
}
