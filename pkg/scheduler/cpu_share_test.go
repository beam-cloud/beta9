package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

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
	require.EqualValues(t, 4000, container.Resources.Limits.Cpu().MilliValue())
	require.Contains(t, container.Env, corev1.EnvVar{Name: "CPU_LIMIT", Value: "4000"})
	require.EqualValues(t, 6000, worker.TotalCpu)
	require.EqualValues(t, 6000, worker.FreeCpu)
}

func TestCPUShareProvisioningPlaceholderMatchesLocalWorker(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	request := &types.ContainerRequest{ContainerId: "c", Cpu: 3000, Memory: 1024}
	for _, controller := range []*LocalWorkerPoolControllerForTest{{name: "local"}, {name: "agent", mode: types.PoolModePrivate}} {
		s.workerPoolManager.SetPool(controller.Name(), types.WorkerPoolConfig{CPUShareMultiplier: 2}, controller)
	}

	local, _ := s.workerPoolManager.GetPool("local")
	cpu := s.workerCPUForControllerRequest(local.Controller, request)
	require.Equal(t, 2*cpu, s.newProvisioningReservation(request, local.Controller).worker.TotalCpu)
	agent, _ := s.workerPoolManager.GetPool("agent")
	require.EqualValues(t, 3000, s.newProvisioningReservation(request, agent.Controller).worker.TotalCpu)
}

func TestNewSchedulerRejectsInvalidCPUShareMultiplier(t *testing.T) {
	config := types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"shared": {CPUShareMultiplier: 0.5},
	}}}
	_, err := NewScheduler(context.Background(), config, nil, nil, nil, nil)
	require.ErrorContains(t, err, `worker pool "shared"`)
}
