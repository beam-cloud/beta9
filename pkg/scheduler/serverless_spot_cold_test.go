package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
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

// spotReplica is the request the managed endpoint controller submits for a
// replica of a Spot placement.
func spotReplica(pool string) *types.ContainerRequest {
	return &types.ContainerRequest{
		ContainerId: uuid.NewString(), Cpu: 16_000, Memory: 512 * 1024,
		GpuRequest: []string{"B200"}, GpuCount: 2, PoolSelector: pool,
		WorkspaceId: buyingWorkspace, Workspace: testWorkspaceWithStorage(),
		Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeManagedEndpointDeployment)}},
		Preemptable: true,
	}
}

// spotPoolFor adds a pool that buys B200 Spot machines to the scheduler.
func spotPoolFor(t *testing.T, s *Scheduler, name string, provider Provisioner, change ...func(*types.WorkerPoolConfig)) *AgentWorkerPoolController {
	t.Helper()
	config := types.WorkerPoolConfig{
		GPUType:          "B200",
		ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true},
		Provisioning:     types.WorkerPoolProvisioningConfig{Providers: []string{"nebius"}, Spot: true, MaxNodes: 1},
	}
	for _, apply := range change {
		apply(&config)
	}
	config.ApplyProvisioning()
	controller, err := NewAgentWorkerPoolController(AgentWorkerPoolControllerOptions{
		Context: s.ctx, Name: name, WorkspaceID: buyingWorkspace, Config: s.config, WorkerPool: config,
		PoolState:  &compute.PoolState{Name: name, Selector: name, Mode: string(types.PoolModeExternal)},
		WorkerRepo: s.workerRepo, ComputeRepo: s.computeRepo, WorkerPoolRepo: s.workerPoolRepo,
		Provisioner: provider, Waitlist: s.waitlist, Clock: s.clock,
	})
	require.NoError(t, err)
	t.Cleanup(controller.close)
	s.workerPoolManager.SetPool(name, config, controller)
	return controller
}

// A replica of a Spot placement names its pool, and the pool buys the
// machine it runs on. Replicas that fit one machine share it.
func TestManagedEndpointSpotReplicaHasItsPoolBuyAMachine(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	clock := common.NewFakeClock(time.Now().Add(-3 * poolMonitoringInterval))
	s.clock, s.spawn, s.workerPoolManager = clock, inline, NewWorkerPoolManager()
	const pool = "spot-b200"
	provider := &fakeProvisioner{inStock: true, sells: ProvisionedMachine{CPU: 64_000, Memory: 2_097_152, GPUCount: 8, Spot: true}, comesUpIn: 4 * time.Minute, taken: 0.25}
	controller := spotPoolFor(t, s, pool, provider)
	s.SetProvisioner(provider)
	// The controller is told what to expect of the machine the pool would
	// buy for the replica, which is asked for as the replica's request is.
	require.Equal(t, MachineOutlook{CanBuy: true, ComeUp: 4 * time.Minute, Taken: 0.25}, s.PoolOutlook(pool, 16_000, 640*1024, 2))
	require.EqualValues(t, replicaHoursLost, provider.asked[0].HoursLost)
	provider.asked = nil
	require.Zero(t, s.PoolOutlook("no-such-pool", 16_000, 640*1024, 2))

	var queued []*types.ContainerRequest
	for range 2 {
		request := spotReplica(pool)
		require.NoError(t, s.Run(request))
		next, err := s.requestBacklog.Pop()
		require.NoError(t, err)
		require.Equal(t, request.ContainerId, next.ContainerId)
		queued = append(queued, next)
	}
	for _, request := range queued {
		newSchedulingAttempt(s, request, nil).run()
	}
	clock.Advance(poolMonitoringInterval)
	require.Len(t, provider.sold, 1, "one machine holds both replicas")
	workers, err := s.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, types.WorkerStatusPending, workers[0].Status)
	require.True(t, workers[0].Preemptable)

	// The replicas wait for the machine that is coming, and no other is bought.
	for _, request := range queued {
		newSchedulingAttempt(s, request, workers).run()
		status, _ := s.containerRepo.GetContainerRequestStatus(request.ContainerId)
		require.NotEqual(t, types.ContainerRequestStatusFailed, status)
	}
	clock.Advance(poolMonitoringInterval)
	require.Len(t, provider.sold, 1)

	machineID := provider.sold[0].MachineID
	require.NoError(t, s.computeRepo.SaveAgentTokenState(s.ctx, &compute.AgentTokenState{
		TokenHash: "test-token", WorkspaceID: buyingWorkspace, PoolName: pool, MachineID: machineID,
		CPUMillicores: 64_000, MemoryMB: 2_097_152, GPUs: []string{"B200"}, GPUCount: 8,
		Executor: types.DefaultAgentWorkerContainerMode, Schedulable: true, LastHeartbeatAt: time.Now(),
		Mode: string(types.PoolModeExternal), ManagedPoolInstanceID: "instance",
	}, time.Hour))
	require.NoError(t, controller.ensureMachine(machineID))
	require.NoError(t, s.workerRepo.SetWorkerKeepAlive(workers[0].Id, types.WorkerKeepAlive{MachineId: machineID, RequestStreamOpen: true}))
	workers, err = s.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusAvailable, workers[0].Status)
	s.config.ImageService.BuildRegistry = "localhost:5000"
	for _, request := range queued {
		workers, err = s.workerRepo.GetAllWorkers()
		require.NoError(t, err)
		newSchedulingAttempt(s, request, workers).run()
		scheduled, err := nextContainerRequest(s.workerRepo, workers[0].Id)
		require.NoError(t, err)
		require.NotNil(t, scheduled)
		require.Equal(t, request.ContainerId, scheduled.ContainerId)
		require.Equal(t, machineID, scheduled.MachineId)
	}
	waiting, err := s.waitlist.For(s.ctx, pool)
	require.NoError(t, err)
	require.Empty(t, waiting)
}

// A replica does not wait on a pool that has no machine coming and can buy
// none: its controller is told at once, and places it in a pool that can.
func TestManagedEndpointSpotReplicaFailsAtOnceWhenItsPoolCannotBuy(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.spawn, s.workerPoolManager = inline, NewWorkerPoolManager()
	const pool = "spot-b200"
	provider := &fakeProvisioner{inStock: false}
	spotPoolFor(t, s, pool, provider)
	s.SetProvisioner(provider)
	require.False(t, s.PoolOutlook(pool, 16_000, 640*1024, 2).CanBuy)

	request := spotReplica(pool)
	require.NoError(t, s.Run(request))
	queued, err := s.requestBacklog.Pop()
	require.NoError(t, err)
	newSchedulingAttempt(s, queued, nil).run()
	status, err := s.containerRepo.GetContainerRequestStatus(request.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerRequestStatusFailed, status)
	require.Empty(t, provider.sold)
	left, _ := s.requestBacklog.Pop()
	require.Nil(t, left, "the request is not left to wait")
}

// A machine counts against what its pool may buy from the moment it is
// bought, and has its Pending worker a moment later. A replica that passes
// in between waits for the machine: a pool of one machine would otherwise
// fail the replica it had just bought for.
func TestManagedEndpointSpotReplicaWaitsForAMachineThatWasJustBought(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.spawn, s.workerPoolManager = inline, NewWorkerPoolManager()
	const pool = "spot-b200"
	provider := &fakeProvisioner{inStock: false, sold: []ProvisionedMachine{{MachineID: "machine-a", CPU: 64_000, Memory: 2_097_152, GPUCount: 8, Spot: true}}}
	spotPoolFor(t, s, pool, provider)
	s.SetProvisioner(provider)

	request := spotReplica(pool)
	require.NoError(t, s.Run(request))
	queued, err := s.requestBacklog.Pop()
	require.NoError(t, err)
	newSchedulingAttempt(s, queued, nil).run()
	status, _ := s.containerRepo.GetContainerRequestStatus(request.ContainerId)
	require.NotEqual(t, types.ContainerRequestStatusFailed, status, "the request waits")
	require.Len(t, provider.sold, 1)
}

// A replica placed on Spot runs only in a Spot pool that takes managed
// endpoints: not in one that also buys on demand, nor in one that did not
// opt in, whatever its request names.
func TestManagedEndpointSpotReplicaRunsOnlyInASpotPoolThatTakesIt(t *testing.T) {
	for name, change := range map[string]func(*types.WorkerPoolConfig){
		"did not opt in":      func(c *types.WorkerPoolConfig) { c.ManagedEndpoints.Enabled = false },
		"also buys on demand": func(c *types.WorkerPoolConfig) { c.Provisioning.OnDemand = true },
	} {
		t.Run(name, func(t *testing.T) {
			s, err := NewSchedulerForTest()
			require.NoError(t, err)
			s.workerPoolManager = NewWorkerPoolManager()
			const pool = "b200"
			provider := &fakeProvisioner{inStock: true}
			spotPoolFor(t, s, pool, provider, change)
			request := spotReplica(pool)
			_, err = s.growthRouteFor(request)
			require.ErrorIs(t, err, errNoPoolForRequest)
			ready := &types.Worker{Id: "ready", PoolName: pool, Gpu: "B200", TotalGpuCount: 8, FreeGpuCount: 8,
				FreeCpu: 64_000, FreeMemory: 2_097_152, Preemptable: true, Status: types.WorkerStatusAvailable}
			_, err = s.selectWorkerFromWorkers([]*types.Worker{ready}, request)
			require.Error(t, err)
			require.Empty(t, provider.asked)
		})
	}
}

// Every replica has the same waitlist: nothing of it is kept in memory.
func TestTheWaitlistIsTheSameForEveryReplica(t *testing.T) {
	rdb, counts, err := repo.NewRedisClientForTestWithCounts()
	require.NoError(t, err)
	ctx, clock := context.Background(), common.NewFakeClock(time.Now())
	one, other := NewWaitlist(rdb, clock), NewWaitlist(rdb, clock)
	waiting := func(pool string) []MachineNeed {
		needs, err := other.For(ctx, pool)
		require.NoError(t, err)
		return needs
	}
	small, big := MachineNeed{GPUCount: 1, HoursLost: 0.1}, MachineNeed{GPUCount: 4, OnDemand: true}
	require.NoError(t, one.Add(ctx, "spot-l40s", "b", big))
	require.NoError(t, one.Add(ctx, "spot-l40s", "a", small))
	require.NoError(t, one.Add(ctx, "spot-a10g", "c", small))
	require.ElementsMatch(t, []MachineNeed{small, big}, waiting("spot-l40s"))

	// A request waits for one pool at a time, until it is taken off the
	// waitlist or stops asking for its place.
	require.NoError(t, one.Add(ctx, "spot-a10g", "a", small))
	require.NoError(t, one.Remove(ctx, "b"))
	require.Empty(t, waiting("spot-l40s"))
	clock.Advance(waitlistLapse - time.Second)
	require.NoError(t, one.Add(ctx, "spot-a10g", "c", big))
	clock.Advance(time.Second)
	require.Equal(t, []MachineNeed{big}, waiting("spot-a10g"))
	require.EqualValues(t, 1, rdb.HLen(ctx, common.RedisKeys.SchedulerWaitlist()).Val(), "a place that lapsed is not kept")

	// A pool that nothing waits for reads none of the waitlist: neither one
	// that no request has joined, nor one whose places have all lapsed.
	for _, pool := range []string{"spot-h100", "spot-l40s"} {
		counts.Start()
		require.Empty(t, waiting(pool))
		commands, _ := counts.Stop()
		require.Equal(t, map[string]int{"HGET": 1}, commands, pool)
	}
}

// A request that waits passes several times a second. It keeps its place on
// the waitlist for as long as it does, and writes it only now and then.
func TestARequestThatWaitsWritesItsPlaceNowAndThen(t *testing.T) {
	p := newBuyingPool(t, &fakeProvisioner{inStock: true})
	s, counts := placementCostScheduler(t, 0)
	clock := common.NewFakeClock(time.Now())
	s.clock, s.waitlist = clock, NewWaitlist(s.requestBacklog.rdb, clock)
	pool := &WorkerPool{Name: p.controller.poolName(), Config: p.controller.workerPoolConfig, Controller: p.controller}
	request := placementCostSandbox("a")
	writes := func() int {
		counts.Start()
		s.joinWaitlist(request, pool)
		commands, _ := counts.Stop()
		return commands["HSET"]
	}

	written := writes()
	for range int(2 * waitlistLapse / time.Second) {
		clock.Advance(time.Second)
		written += writes()
		waiting, err := s.waitlist.For(s.ctx, pool.Name)
		require.NoError(t, err)
		require.Len(t, waiting, 1)
	}
	require.Equal(t, 2*(1+int(2*waitlistLapse/waitlistRefresh)), written, "its place, and that the pool is waited for")

	// It writes at once when it waits for another pool, and when it waits
	// again after it left for a worker that had no room for it after all.
	request.WaitlistPool = "spot-a10g"
	require.Equal(t, 2, writes())
	(&schedulingBatch{scheduler: s}).dispatchSchedules([]plannedSchedule{{request: request, worker: &types.Worker{Id: "gone"}}})
	require.Equal(t, 2, writes())
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
