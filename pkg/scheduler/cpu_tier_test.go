package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

var sharedPool = types.WorkerPoolConfig{CPUClass: types.CPUTierShared, CPUShareMultiplier: 2.5}

// spotRequest is a 1 vCPU / 1 GiB request as Run marks it for Spot.
func spotRequest() *types.ContainerRequest {
	return &types.ContainerRequest{Cpu: 1000, Memory: 1024, OpportunisticOnly: true, Evictable: true, DrainSeconds: 30, Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"cpu_tier":"spot"}`}}}
}

// tierScheduler is a scheduler with only these pools, for worker selection.
func tierScheduler(pools map[string]types.WorkerPoolConfig) *Scheduler {
	s := &Scheduler{workerPoolManager: NewWorkerPoolManager(), config: types.AppConfig{Worker: types.WorkerConfig{Pools: pools}}}
	for name, p := range pools {
		s.workerPoolManager.SetPool(name, p, nil)
	}
	return s
}

func TestRunMarksOnlySpotEvictable(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	scheduler.workerPoolManager.SetPool("shared", sharedPool, &LocalWorkerPoolControllerForTest{ctx: scheduler.ctx, name: "shared", config: scheduler.config, workerRepo: scheduler.workerRepo})
	request := func(id, tier string) *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: id, Cpu: 1000, Memory: 1024, Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"cpu_tier":"` + tier + `"}`}}}
	}
	spot := request("spot", "spot")
	require.NoError(t, scheduler.Run(spot))
	require.True(t, spot.Evictable && spot.OpportunisticOnly)
	require.Equal(t, uint32(30), spot.DrainSeconds)
	shared := request("shared", "shared")
	require.NoError(t, scheduler.Run(shared))
	require.False(t, shared.Evictable || shared.OpportunisticOnly)
}

func TestCPUTierPlacementAndWorkerCompatibility(t *testing.T) {
	s := tierScheduler(map[string]types.WorkerPoolConfig{"legacy": {}, "shared": sharedPool, "performance": {CPUClass: types.CPUTierPerformance}})
	for _, tc := range []struct {
		tier string
		want []string
	}{
		{"", []string{"legacy"}}, {"shared", []string{"shared"}}, {"performance", []string{"performance"}}, {"spot", []string{"shared", "performance"}},
	} {
		request := &types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"cpu_tier":"` + tc.tier + `"}`}}}
		workers := []*types.Worker{}
		for _, name := range []string{"legacy", "shared", "performance"} {
			workers = append(workers, &types.Worker{Id: name, PoolName: name, CPUTierProtocol: types.CPUTierProtocolVersion})
		}
		workers = append(workers, &types.Worker{Id: "old-binary", PoolName: "shared"})
		got := []string{}
		for _, w := range s.filterWorkersByCPUTier(workers, request) {
			got = append(got, w.Id)
		}
		require.ElementsMatch(t, tc.want, got)
	}
}

func TestTieredCPUUsesFreeCapacityBeforeReclaimingSpot(t *testing.T) {
	s := tierScheduler(map[string]types.WorkerPoolConfig{"shared": sharedPool})
	full := &types.Worker{Id: "full", PoolName: "shared", CPUTierProtocol: types.CPUTierProtocolVersion, Status: types.WorkerStatusAvailable, TotalCpu: 8000, TotalMemory: 16000, FreeCpu: 0, FreeMemory: 0, EvictableCpu: 4000, EvictableMemory: 8000, Priority: 100}
	free := *full
	free.Id, free.FreeCpu, free.FreeMemory, free.Priority = "free", 4000, 8000, 0
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"cpu_tier":"shared"}`}}}
	worker, err := s.selectWorkerFromWorkers([]*types.Worker{full, &free}, request)
	require.NoError(t, err)
	require.Equal(t, "free", worker.Id)
	// Spot cannot reclaim another workload or launch beyond truly free capacity.
	request = spotRequest()
	_, err = s.selectWorkerFromWorkers([]*types.Worker{full}, request)
	require.Error(t, err)
	worker, err = s.selectWorkerFromWorkers([]*types.Worker{full, &free}, request)
	require.NoError(t, err)
	require.Equal(t, "free", worker.Id)
}

func TestSpotWaitsForSpareCapacity(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	controller := &LocalWorkerPoolControllerForTest{ctx: scheduler.ctx, name: "shared", config: scheduler.config, workerRepo: scheduler.workerRepo}
	scheduler.workerPoolManager.SetPool("shared", sharedPool, controller)
	request := spotRequest()
	request.Timestamp = time.Now()
	setPendingSchedulerRequests(t, scheduler, request)

	// Without spare capacity, Spot stays pending in the backlog and adds no worker.
	newSchedulingAttempt(scheduler, request, nil).run()
	require.Equal(t, 0, controller.AddWorkerCallCount())
	require.Equal(t, int64(1), scheduler.requestBacklog.Len())
	state, err := scheduler.containerRepo.GetContainerState(request.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusPending, state.Status)

	// It gives up when the scheduling window ends.
	request.Timestamp = time.Now().Add(-maxScheduleRetryDuration)
	newSchedulingAttempt(scheduler, request, nil).run()
	require.Equal(t, 0, controller.AddWorkerCallCount())
	status, err := scheduler.containerRepo.GetContainerRequestStatus(request.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerRequestStatusFailed, status)

	// A managed endpoint replica is opportunistic and evictable on any tier,
	// but only Spot waits: the replica fails at once and its controller retries.
	replica := spotRequest()
	replica.Stub.Config, replica.DrainSeconds, replica.Timestamp = `{"cpu_tier":"shared"}`, 0, time.Now()
	setPendingSchedulerRequests(t, scheduler, replica)
	newSchedulingAttempt(scheduler, replica, nil).run()
	status, err = scheduler.containerRepo.GetContainerRequestStatus(replica.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerRequestStatusFailed, status)
}

func TestSpotSkipsWorkersHoldingPoolHeadroom(t *testing.T) {
	shared := sharedPool
	shared.PoolSizing = types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "4000m", MinFreeMemory: "0", MinFreeGPU: "0"}
	s := tierScheduler(map[string]types.WorkerPoolConfig{"shared": shared})
	worker := func(id string) *types.Worker {
		return &types.Worker{Id: id, PoolName: "shared", CPUTierProtocol: types.CPUTierProtocolVersion, Status: types.WorkerStatusAvailable, TotalCpu: 8000, TotalMemory: 16000, FreeCpu: 8000, FreeMemory: 16000}
	}
	request := spotRequest()
	// "a" holds the pool's 4 vCPU floor and would evict Spot on its next keepalive.
	selected, err := s.selectWorkerFromWorkers([]*types.Worker{worker("a"), worker("b")}, request)
	require.NoError(t, err)
	require.Equal(t, "b", selected.Id)
	_, err = s.selectWorkerFromWorkers([]*types.Worker{worker("a")}, request)
	require.Error(t, err)
}
