package scheduler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Exercise the real scheduler -> AgentWorkerPoolController -> Provisioner
// interface from an empty pool. A warm-floor test never takes this path.
func TestServerlessGPUColdDemandBuysSpotWithoutAFloor(t *testing.T) {
	for _, gpu := range []types.GpuType{types.GPU_RTX_PRO6000, types.GPU_H100, types.GPU_H200, types.GPU_B200, types.GPU_B300} {
		t.Run(gpu.String(), func(t *testing.T) {
			s, err := NewSchedulerForTest()
			require.NoError(t, err)
			fakeClock := common.NewFakeClock(time.Now().Add(-3 * poolMonitoringInterval))
			s.clock = fakeClock
			s.spawn = inline
			s.workerPoolManager = NewWorkerPoolManager()

			provider := &fakeProvisioner{inStock: true, sells: ProvisionedMachine{CPU: 16_000, Memory: 64_000, GPUCount: 1}}
			poolConfig := types.WorkerPoolConfig{
				GPUType: gpu.String(), ContainerRuntime: types.ContainerRuntimeGvisor.String(),
				Provisioning: types.WorkerPoolProvisioningConfig{Provider: "nebius", Spot: true, MaxNodes: 1, MaxGPUs: 1},
			}
			poolConfig.ApplyProvisioning()
			name := "spot-" + gpu.String()
			controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
				Context: s.ctx, Name: name, WorkspaceID: buyingWorkspace,
				Config: s.config, WorkerPool: poolConfig,
				PoolState:  &compute.PoolState{Name: name, Selector: name, Mode: string(types.PoolModeExternal)},
				WorkerRepo: s.workerRepo, ComputeRepo: s.computeRepo, WorkerPoolRepo: s.workerPoolRepo,
				Provisioner: provider, Clock: fakeClock,
			})
			require.NoError(t, err)
			t.Cleanup(controller.close)
			s.workerPoolManager.SetPool(name, poolConfig, controller)

			// A zero floor stays empty even after several sizing intervals.
			fakeClock.Advance(3 * poolMonitoringInterval)
			require.Empty(t, provider.asked)
			require.Empty(t, provider.sold)
			workers, err := s.workerRepo.GetAllWorkers()
			require.NoError(t, err)
			require.Empty(t, workers)

			config := &types.StubConfigV1{Runtime: types.Runtime{Gpu: gpu, GpuCount: 1}}
			encoded, err := json.Marshal(config)
			require.NoError(t, err)
			request := &types.ContainerRequest{
				ContainerId: uuid.NewString(), Cpu: 500, Memory: 512, GpuRequest: []string{gpu.String()}, GpuCount: 1,
				WorkspaceId: "customer-workspace", Workspace: testWorkspaceWithStorage(),
				Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodRun), Config: string(encoded)}},
				Preemptable: config.ServerlessGPUPreemptable(),
			}
			require.Empty(t, request.PoolSelector, "ordinary request names no pool")
			require.Empty(t, request.MachineId, "ordinary request pins no machine")
			require.NoError(t, s.Run(request))
			queued, err := s.requestBacklog.Pop()
			require.NoError(t, err)
			require.NotNil(t, queued)
			require.True(t, queued.Preemptable, "eligibility survives the actual backlog wire format")

			newSchedulingAttempt(s, queued, workers).run()
			need := MachineNeed{CPU: 500, Memory: capacityMemoryForScheduling(request), GPUCount: 1}
			require.Equal(t, []MachineNeed{need, need}, provider.asked, "CanProvision and Provision receive the actual request size")
			require.Len(t, provider.sold, 1)
			workers, err = s.workerRepo.GetAllWorkers()
			require.NoError(t, err)
			require.Len(t, workers, 1)
			require.Equal(t, types.WorkerStatusPending, workers[0].Status)
			require.Equal(t, gpu.String(), workers[0].Gpu)
			require.True(t, workers[0].Preemptable)
			require.True(t, workers[0].ControlPlaneManaged)
			require.EqualValues(t, 16_000, workers[0].TotalCpu)
			require.EqualValues(t, 1, workers[0].TotalGpuCount)

			// Waiting for the pending machine must not buy another machine.
			newSchedulingAttempt(s, queued, workers).run()
			require.Len(t, provider.sold, 1)

			// Once the bought machine joins and its request stream is ready,
			// the same ordinary request runs on its worker without a selector.
			machineID := provider.sold[0].MachineID
			require.NoError(t, s.computeRepo.SaveAgentTokenState(s.ctx, &compute.AgentTokenState{
				TokenHash: "test-token", WorkspaceID: buyingWorkspace, PoolName: name, MachineID: machineID,
				CPUMillicores: 16_000, MemoryMB: 64_000, GPUs: []string{gpu.String()}, GPUCount: 1,
				Executor: types.DefaultAgentWorkerContainerMode, Schedulable: true, LastHeartbeatAt: time.Now(),
				Mode: string(types.PoolModeExternal), ManagedPoolInstanceID: "cold-instance",
			}, time.Hour))
			require.NoError(t, controller.ensureMachine(machineID))
			require.NoError(t, s.workerRepo.SetWorkerKeepAlive(workers[0].Id, types.WorkerKeepAlive{MachineId: machineID, RequestStreamOpen: true}))
			workers, err = s.workerRepo.GetAllWorkers()
			require.NoError(t, err)
			require.Equal(t, types.WorkerStatusAvailable, workers[0].Status)
			s.config.ImageService.BuildRegistry = "localhost:5000"
			newSchedulingAttempt(s, queued, workers).run()
			scheduled, err := nextContainerRequest(s.workerRepo, workers[0].Id)
			require.NoError(t, err)
			require.NotNil(t, scheduled)
			require.Equal(t, request.ContainerId, scheduled.ContainerId)
			require.Equal(t, machineID, scheduled.MachineId)
			require.Empty(t, scheduled.PoolSelector)
			require.True(t, scheduled.Preemptable)
			require.Len(t, provider.sold, 1)
		})
	}
}

func TestExplicitNonPreemptableRequestStillRejectsSpot(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	config := types.WorkerPoolConfig{GPUType: "H100", Provisioning: types.WorkerPoolProvisioningConfig{Provider: "nebius", Spot: true, MaxNodes: 1}}
	config.ApplyProvisioning()
	provider := &fakeProvisioner{inStock: true}
	controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: s.ctx, Name: "spot-h100", WorkspaceID: buyingWorkspace, Config: s.config, WorkerPool: config,
		PoolState:  &compute.PoolState{Name: "spot-h100", Selector: "spot-h100", Mode: string(types.PoolModeExternal)},
		WorkerRepo: s.workerRepo, ComputeRepo: s.computeRepo, Provisioner: provider,
	})
	require.NoError(t, err)
	t.Cleanup(controller.close)
	s.workerPoolManager.SetPool("spot-h100", config, controller)
	request := &types.ContainerRequest{Gpu: "H100", GpuCount: 1, Preemptable: false, Workspace: testWorkspaceWithStorage()}
	_, err = s.growthRouteFor(request)
	require.ErrorIs(t, err, errNoPoolForRequest)
	_, err = s.selectWorkerFromWorkers([]*types.Worker{{Id: "spot-worker", Gpu: "H100", TotalGpuCount: 1, FreeGpuCount: 1, Preemptable: true, PoolName: "spot-h100", Status: types.WorkerStatusAvailable}}, request)
	require.Error(t, err)
	require.Empty(t, provider.asked, "the scheduler never weakens an explicit false request")
}
