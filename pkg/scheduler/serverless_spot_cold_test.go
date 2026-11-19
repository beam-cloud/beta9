package scheduler

import (
	"encoding/json"
	"fmt"
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
				Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"nebius"}, Spot: true, MaxNodes: 1, MaxGPUs: 1},
			}
			poolConfig.ApplyProvisioning()
			name := "spot-" + gpu.String()
			controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
				Context: s.ctx, Name: name, WorkspaceID: buyingWorkspace,
				Config: s.config, WorkerPool: poolConfig,
				PoolState:  &compute.PoolState{Name: name, Selector: name, Mode: string(types.PoolModeExternal)},
				WorkerRepo: s.workerRepo, ComputeRepo: s.computeRepo, WorkerPoolRepo: s.workerPoolRepo,
				Provisioner: provider, Waitlist: s.waitlist, Clock: fakeClock,
			})
			require.NoError(t, err)
			t.Cleanup(controller.close)
			s.workerPoolManager.SetPool(name, poolConfig, controller)
			s.SetProvisioner(provider)

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

			// The request says that it waits, and the pool's next plan buys.
			newSchedulingAttempt(s, queued, workers).run()
			require.Empty(t, provider.sold)
			fakeClock.Advance(poolMonitoringInterval)
			need := MachineNeed{CPU: 500, Memory: capacityMemoryForScheduling(request), GPUCount: 1, HoursLost: 8}
			require.Equal(t, []MachineNeed{need, need, need}, provider.asked, "CanProvision and Provision receive the actual request")
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
			fakeClock.Advance(poolMonitoringInterval)
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
			waiting, err := s.waitlist.For(s.ctx, name)
			require.NoError(t, err)
			require.Empty(t, waiting, "a request that is placed no longer waits")
		})
	}
}

func TestExplicitNonPreemptableRequestStillRejectsSpot(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	config := types.WorkerPoolConfig{GPUType: "H100", Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"nebius"}, Spot: true, MaxNodes: 1}}
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

// Every replica has the same waitlist: nothing of it is kept in memory.
func TestTheWaitlistIsTheSameForEveryReplica(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	clock := common.NewFakeClock(time.Now())
	one, other := NewWaitlist(s.requestBacklog.rdb, clock), NewWaitlist(s.requestBacklog.rdb, clock)
	waiting := func(pool string) []MachineNeed {
		needs, err := other.For(s.ctx, pool)
		require.NoError(t, err)
		return needs
	}
	small, big := MachineNeed{GPUCount: 1, HoursLost: 0.1}, MachineNeed{GPUCount: 4, OnDemand: true}
	require.NoError(t, one.Add(s.ctx, "spot-l40s", "b", big))
	require.NoError(t, one.Add(s.ctx, "spot-l40s", "a", small))
	require.NoError(t, one.Add(s.ctx, "spot-a10g", "c", small))
	require.ElementsMatch(t, []MachineNeed{small, big}, waiting("spot-l40s"))

	// A request waits for one pool at a time, until it is taken off the
	// waitlist or stops asking for its place.
	require.NoError(t, one.Add(s.ctx, "spot-a10g", "a", small))
	require.NoError(t, one.Remove(s.ctx, "b"))
	require.Empty(t, waiting("spot-l40s"))
	clock.Advance(waitlistLapse - time.Second)
	require.NoError(t, one.Add(s.ctx, "spot-a10g", "c", big))
	clock.Advance(time.Second)
	require.Equal(t, []MachineNeed{big}, waiting("spot-a10g"))
	require.EqualValues(t, 1, s.requestBacklog.rdb.HLen(s.ctx, common.RedisKeys.SchedulerWaitlist()).Val(), "a place that lapsed is not kept")
}

// A pool that buys plans for every request that waits for it together.
func TestABuyingPoolBuysWhatTheRequestsThatWaitForItLack(t *testing.T) {
	provider := &fakeProvisioner{inStock: true, sells: ProvisionedMachine{CPU: 64_000, Memory: 256_000, GPUCount: 4, Spot: true}}
	p := newBuyingPool(t, provider, func(pool *types.WorkerPoolConfig) { pool.Provisioning.OnDemand = true })
	none := types.WorkerPoolSizingConfig{}
	sold := func(sizing types.WorkerPoolSizingConfig, waiting string, onDemand bool) int {
		for _, id := range waiting {
			require.NoError(t, p.controller.waitlist.Add(p.ctx, "spot-l40s", string(id), MachineNeed{CPU: 1000, Memory: 1024, GPUCount: 1, OnDemand: onDemand}))
		}
		p.controller.plan(&sizing)
		return len(provider.sold)
	}

	require.Equal(t, 1, sold(none, "abc", false), "three requests share a machine with four GPUs")
	require.Equal(t, 1, sold(none, "d", false), "which has room for a fourth while it boots")
	require.Equal(t, 2, sold(none, "e", false), "and none for a fifth")

	// Work that may not be interrupted takes no room on a spot machine.
	provider.sells.Spot = false
	require.Equal(t, 3, sold(none, "f", true))
	require.Equal(t, 3, sold(none, "ghi", false), "work that may be takes what room there is")

	// The floor is kept on top of what waits: nine of twelve GPUs are taken.
	floor := types.WorkerPoolSizingConfig{MinFreeGpu: 4, DefaultWorkerCpu: 1000, DefaultWorkerMemory: 1024, DefaultWorkerGpuCount: 1}
	require.Equal(t, 4, sold(floor, "", false))
	require.Equal(t, 4, sold(floor, "jkl", false))

	// A pool that cannot buy holds what it has. Work that may not be
	// interrupted has its room first, whenever it came.
	provider.inStock, provider.asked = false, nil
	sold(floor, "mnop", false)
	require.Equal(t, 4, sold(floor, "q", true))
	need := MachineNeed{CPU: 1000, Memory: 1024, GPUCount: 1}
	require.Equal(t, []MachineNeed{need, need, need}, provider.asked, "for the floor, then for a request and the floor together")
}

// Of two workers that are otherwise alike, work that is cheap to lose takes
// the machine its provider may take back, and leaves the other for work that
// is not.
func TestWorkTakesTheKindOfMachineItIsBestRunOn(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	workers := []*types.Worker{}
	for _, spot := range []bool{false, true, false} {
		workers = append(workers, &types.Worker{Id: fmt.Sprintf("worker-%d", len(workers)), Status: types.WorkerStatusAvailable, PoolName: "default",
			TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 8192, FreeMemory: 8192, Preemptable: spot})
	}
	for kind, spot := range map[string]bool{types.StubTypeEndpointDeployment: true, types.StubTypeSandbox: false} {
		request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, Preemptable: true, Workspace: testWorkspaceWithStorage(),
			Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(kind)}}}
		worker, err := s.selectWorkerFromWorkers(workers, request)
		require.NoError(t, err)
		require.Equal(t, spot, worker.Preemptable, kind)
	}
}
