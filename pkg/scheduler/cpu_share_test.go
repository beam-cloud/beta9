package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Reconciling a managed pool with a new multiplier resizes its live worker in
// place. Reservations above a lowered capacity block admission until they drain.
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

	reconcile := func(multiplier types.CPUShareMultiplier) {
		require.NoError(t, s.EnsureAgentPool(machine.WorkspaceID, &compute.PoolState{
			Name:              machine.PoolName,
			Selector:          machine.PoolName,
			ManagementSource:  types.WorkerPoolManagementSourceAPI,
			ManagedInstanceID: machine.ManagedPoolInstanceID,
			WorkerConfig:      &types.WorkerPoolConfig{CPUShareMultiplier: multiplier},
		}))
	}
	workerID := compute.AgentMachineWorkerID(machine.MachineID)
	schedule := func(containerID string, cpu int64) error {
		worker, err := s.workerRepo.GetWorkerById(workerID)
		require.NoError(t, err)
		require.NoError(t, s.containerRepo.SetContainerState(containerID, &types.ContainerState{ContainerId: containerID, Status: types.ContainerStatusPending}))
		return s.workerRepo.ScheduleContainerRequests(worker, []*types.ContainerRequest{{ContainerId: containerID, Cpu: cpu, Memory: 1024}})
	}
	capacity := func() (int64, int64) {
		worker, err := s.workerRepo.GetWorkerById(workerID)
		require.NoError(t, err)
		return worker.TotalCpu, worker.FreeCpu
	}

	reconcile(1.5)
	worker, err := s.workerRepo.GetWorkerById(workerID)
	require.NoError(t, err)
	require.EqualValues(t, 24000, worker.TotalCpu)
	require.EqualValues(t, 24000, worker.FreeCpu)
	require.EqualValues(t, machine.MemoryMB, worker.TotalMemory)
	require.NoError(t, s.workerRepo.UpdateWorkerStatus(workerID, types.WorkerStatusAvailable))

	pool, ok := s.workerPoolManager.GetPool(machine.PoolName)
	require.True(t, ok)
	shared := pool.Controller.(*AgentWorkerPoolController)
	fits, err := shared.HasWorkerCapacity(24000, 1024, 0)
	require.NoError(t, err)
	require.True(t, fits)
	fits, err = shared.HasWorkerCapacity(24001, 1024, 0)
	require.NoError(t, err)
	require.False(t, fits)
	fits, err = shared.HasWorkerCapacity(1000, int64(machine.MemoryMB)+1, 0)
	require.NoError(t, err)
	require.False(t, fits, "memory is never shared")

	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, schedule(id, 8000))
	}
	require.Error(t, schedule("d", 1000))

	reconcile(1)
	total, free := capacity()
	require.EqualValues(t, 16000, total)
	require.Zero(t, free)
	require.Error(t, schedule("e", 1000), "reservations above the lowered capacity must drain first")

	reconcile(2)
	total, free = capacity()
	require.EqualValues(t, 32000, total)
	require.EqualValues(t, 8000, free)
	require.NoError(t, schedule("f", 8000))
}

func TestCPUShareLocalWorkerKeepsPodCPUPhysical(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{
		name: "shared",
		config: types.AppConfig{Worker: types.WorkerConfig{
			JobResourcesEnforced:       true,
			DefaultWorkerCPURequest:    1000,
			DefaultWorkerMemoryRequest: 1024,
		}},
		workerPoolConfig: types.WorkerPoolConfig{CPUShareMultiplier: 1.5},
	}

	job, worker := controller.createWorkerJob("worker-1", 4000, 8192, "", 0, "token")
	container := job.Spec.Template.Spec.Containers[0]
	require.EqualValues(t, 4000, container.Resources.Requests.Cpu().MilliValue())
	require.EqualValues(t, 4000, container.Resources.Limits.Cpu().MilliValue())
	cpuLimit := ""
	for _, env := range container.Env {
		if env.Name == "CPU_LIMIT" {
			cpuLimit = env.Value
		}
	}
	require.Equal(t, "4000", cpuLimit)
	require.EqualValues(t, 6000, worker.TotalCpu)
	require.EqualValues(t, 6000, worker.FreeCpu)
	require.EqualValues(t, 8192, worker.TotalMemory)
}

func TestCPUShareProvisioningSizesPhysicalWorkers(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	local := &LocalWorkerPoolControllerForTest{name: "shared"}
	setMultiplier := func(controller WorkerPoolController, multiplier types.CPUShareMultiplier) {
		s.workerPoolManager.SetPool(controller.Name(), types.WorkerPoolConfig{CPUShareMultiplier: multiplier}, controller)
	}

	for _, multiplier := range []types.CPUShareMultiplier{1.4, 1.5, 2.3, 16} {
		setMultiplier(local, multiplier)
		for _, cpu := range []int64{15750, 51750, 63000, 103500} {
			request := &types.ContainerRequest{ContainerId: "large", Cpu: cpu, Memory: 1024}
			physical := s.workerCPUForControllerRequest(local, request)
			require.Less(t, physical, cpu, "multiplier=%v cpu=%d", multiplier, cpu)
			reservation := s.newProvisioningReservation(request, local)
			require.Equal(t, multiplier.Schedulable(physical), reservation.worker.TotalCpu)
			require.True(t, s.reserveWorkerCapacity(reservation.worker, request), "multiplier=%v cpu=%d: the new worker must hold its request", multiplier, cpu)
		}
	}

	small := &types.ContainerRequest{ContainerId: "small", Cpu: 250, Memory: 128}
	setMultiplier(local, 0)
	floor := s.workerCPUForControllerRequest(local, small)
	setMultiplier(local, 2)
	require.Equal(t, floor, s.workerCPUForControllerRequest(local, small), "worker size floors stay physical")
	require.Equal(t, 2*floor, s.newProvisioningReservation(small, local).worker.TotalCpu)

	agent := &LocalWorkerPoolControllerForTest{name: "agent", mode: types.PoolModePrivate}
	setMultiplier(agent, 2)
	request := &types.ContainerRequest{ContainerId: "agent", Cpu: 3000, Memory: 1024}
	require.EqualValues(t, 3000, s.workerCPUForControllerRequest(agent, request))
	require.EqualValues(t, 3000, s.newProvisioningReservation(request, agent).worker.TotalCpu, "agent capacity is already schedulable")
}

func TestNewSchedulerRejectsInvalidCPUShareMultiplier(t *testing.T) {
	config := types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"shared": {CPUShareMultiplier: 0.5},
	}}}
	_, err := NewScheduler(context.Background(), config, nil, nil, nil, nil)
	require.ErrorContains(t, err, `worker pool "shared"`)
}
