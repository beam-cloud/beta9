package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCPUTierPlacementAndWorkerCompatibility(t *testing.T) {
	s := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	for name, p := range map[string]types.WorkerPoolConfig{
		"legacy": {}, "shared": {CPUClass: types.CPUTierShared, CPUShareMultiplier: 2.5},
		"performance": {CPUClass: types.CPUTierPerformance},
	} {
		s.workerPoolManager.SetPool(name, p, &LocalWorkerPoolControllerForTest{name: name})
	}
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
	s := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
	s.workerPoolManager.SetPool("shared", types.WorkerPoolConfig{CPUClass: types.CPUTierShared, CPUShareMultiplier: 2.5}, nil)
	full := &types.Worker{Id: "full", PoolName: "shared", CPUTierProtocol: types.CPUTierProtocolVersion, Status: types.WorkerStatusAvailable, TotalCpu: 8000, TotalMemory: 16000, FreeCpu: 0, FreeMemory: 0, EvictableCpu: 4000, EvictableMemory: 8000, Priority: 100}
	free := *full
	free.Id, free.FreeCpu, free.FreeMemory, free.Priority = "free", 4000, 8000, 0
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"cpu_tier":"shared"}`}}}
	worker, err := s.selectWorkerFromWorkers([]*types.Worker{full, &free}, request)
	require.NoError(t, err)
	require.Equal(t, "free", worker.Id)
	// Spot cannot reclaim another workload or launch beyond truly free capacity.
	request.Stub.Config = `{"cpu_tier":"spot"}`
	request.OpportunisticOnly, request.Evictable = true, true
	_, err = s.selectWorkerFromWorkers([]*types.Worker{full}, request)
	require.Error(t, err)
}
