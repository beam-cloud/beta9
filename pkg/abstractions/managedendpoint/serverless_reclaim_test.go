package managedendpoint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerlessReclaimPreservesHotMinimumAndStartsBeforeRefill(t *testing.T) {
	for _, protectMinimum := range []bool{false, true} {
		s := newFillService(t)
		hot, cold, endpoints := fillEndpoints(t, s)
		minimum := reclaimableReplica(t, s, hot, "minimum", "w1")
		minimum.Protected = protectMinimum
		extra := reclaimableReplica(t, s, hot, "extra", "w1")
		extra.StartedAt = time.Now().Add(time.Minute)
		ctx := context.Background()
		require.NoError(t, s.repo.SaveReplica(ctx, minimum))
		require.NoError(t, s.repo.SaveReplica(ctx, extra))
		// Ordinary serverless owns the rest of the worker. It has no managed
		// replica record and must never enter the hosted victim set.
		ordinary := &types.ContainerState{ContainerId: "ordinary-serverless", WorkerId: "w1", Gpu: "H100", GpuCount: 1, Status: types.ContainerStatusRunning}
		require.NoError(t, s.containers.SetContainerState(ordinary.ContainerId, ordinary))
		entries := []types.FleetEntry{
			{EndpointID: hot.Spec.ID, Priority: 1, MinReplicas: 1, MaxReplicas: 2, ProtectMinimum: protectMinimum},
			{EndpointID: cold.Spec.ID, Priority: 99, Serverless: true, MaxReplicas: 1},
		}
		live := []*types.EndpointReplica{minimum, extra}
		demand := map[string]*endpointDemand{cold.Spec.ID: {active: 1, warm: true}}
		s.controller.fill(ctx, "H100", entries, endpoints, live, noRoomInventory(), demand)
		assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, minimum.ID), "all configured minima survive, even when preemption is enabled")
		assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, extra.ID), "a request can reclaim higher-priority hot surplus")
		assert.Equal(t, "gpu reclaimed for "+cold.Spec.ID, extra.StatusReason)
		requests, _ := scheduler.NewRequestBacklog(s.rdb).PopN(10)
		assert.Empty(t, requests, "capacity remains owned until the donor finishes draining")
		state, err := s.containers.GetContainerState(ordinary.ContainerId)
		require.NoError(t, err)
		assert.Equal(t, types.ContainerStatusRunning, state.Status)
		assert.False(t, state.Evicting)

		// On the next pass the donor's GPU is genuinely free. The request
		// gets it before the high-priority hot endpoint can refill its surplus.
		extra.Status = types.ReplicaStatusStopped
		require.NoError(t, s.repo.SaveReplica(ctx, extra))
		demand = map[string]*endpointDemand{cold.Spec.ID: {active: 1, warm: true}}
		s.controller.fill(ctx, "H100", entries, endpoints, live, idleInventory(1), demand)
		requests, err = scheduler.NewRequestBacklog(s.rdb).PopN(10)
		require.NoError(t, err)
		require.Len(t, requests, 1)
		assert.Equal(t, cold.StubID, requests[0].StubId)
		assert.True(t, requests[0].Evictable)
		assert.True(t, requests[0].OpportunisticOnly)
	}
}

func TestServerlessDemandRunsAfterMinimumsBeforeHotSurplus(t *testing.T) {
	s := newFillService(t)
	hot, cold, endpoints := fillEndpoints(t, s)
	entries := []types.FleetEntry{
		{EndpointID: hot.Spec.ID, Priority: 1, MinReplicas: 1, MaxReplicas: 3, ProtectMinimum: true},
		{EndpointID: cold.Spec.ID, Priority: 99, Serverless: true, MaxReplicas: 1},
	}
	s.controller.fill(context.Background(), "H100", entries, endpoints, nil, idleInventory(3), map[string]*endpointDemand{cold.Spec.ID: {active: 1, warm: true}})
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 3)
	assert.Equal(t, []string{hot.StubID, cold.StubID, hot.StubID}, []string{requests[0].StubId, requests[1].StubId, requests[2].StubId})
	assert.False(t, requests[0].Evictable)
	assert.True(t, requests[1].Evictable)
	assert.True(t, requests[2].Evictable)
}

func TestServerlessReclaimRejectsUnsafeDonorsAndImpossibleTargets(t *testing.T) {
	for _, reason := range []string{"minimum", "protected", "not-evictable", "other-on-demand", "CPU-held-by-serverless", "memory-overhead", "unknown-requirements", "pool-floor", "fragmented-GPUs"} {
		t.Run(reason, func(t *testing.T) {
			s := newFillService(t)
			hot, cold, endpoints := fillEndpoints(t, s)
			victim := reclaimableReplica(t, s, hot, "candidate", "w1")
			live := []*types.EndpointReplica{victim}
			entries := []types.FleetEntry{
				{EndpointID: hot.Spec.ID, Priority: 1, MaxReplicas: 1},
				{EndpointID: cold.Spec.ID, Priority: 99, Serverless: true, MaxReplicas: 1},
			}
			ctx := context.Background()
			inv := noRoomInventory()
			demand := map[string]*endpointDemand{cold.Spec.ID: {active: 1, warm: true}}
			switch reason {
			case "minimum":
				entries[0].MinReplicas = 1
			case "protected":
				victim.Protected = true
				require.NoError(t, s.repo.SaveReplica(ctx, victim))
			case "not-evictable":
				state, err := s.containers.GetContainerState(victim.ContainerID)
				require.NoError(t, err)
				state.Evictable = false
				require.NoError(t, s.containers.SetContainerState(victim.ContainerID, state))
			case "other-on-demand":
				entries[0].Serverless = true
				demand[hot.Spec.ID] = &endpointDemand{active: 1, warm: true, capacity: 1}
			case "CPU-held-by-serverless":
				s.backend = fillBackend{config: `{"runtime":{"cpu":1000,"memory":0}}`}
			case "memory-overhead":
				s.backend = fillBackend{config: `{"runtime":{"cpu":0,"memory":101}}`}
				state, err := s.containers.GetContainerState(victim.ContainerID)
				require.NoError(t, err)
				state.Memory = 100 // releases 125 after padding; the request needs 127
				require.NoError(t, s.containers.SetContainerState(victim.ContainerID, state))
			case "unknown-requirements":
				s.backend = fillBackend{err: errors.New("stub unavailable")}
			case "pool-floor":
				inv.floors = map[string]uint32{"gpu-a": 1}
			case "fragmented-GPUs":
				cold.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 2}}
				other := reclaimableReplica(t, s, hot, "other", "w2")
				live = append(live, other)
				entries[0].MaxReplicas = 2
				inv.workers["w1"].total = 1
				inv.workers["w2"] = &inventoryWorker{pool: "gpu-a", gpu: "H100", total: 1}
			}
			s.controller.fill(ctx, "H100", entries, endpoints, live, inv, demand)
			for _, replica := range live {
				assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, replica.ID), "demand must not destroy capacity it cannot safely use")
			}
			requests, _ := scheduler.NewRequestBacklog(s.rdb).PopN(10)
			assert.Empty(t, requests)
		})
	}
}

func TestServerlessReclaimIsOnePendingCapacityOperationAcrossGPUs(t *testing.T) {
	s := newFillService(t)
	hot, cold, endpoints := fillEndpoints(t, s)
	hot.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 1}, "A100-80": {Count: 1}}
	cold.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 1}, "A100-80": {Count: 1}}
	h100 := reclaimableReplica(t, s, hot, "h100", "w1")
	a100 := reclaimableReplica(t, s, hot, "a100", "w2")
	a100.GPU = "A100-80"
	ctx := context.Background()
	require.NoError(t, s.repo.SaveReplica(ctx, a100))
	state, err := s.containers.GetContainerState(a100.ContainerID)
	require.NoError(t, err)
	state.Gpu = a100.GPU
	require.NoError(t, s.containers.SetContainerState(a100.ContainerID, state))
	live := []*types.EndpointReplica{h100, a100}
	entries := []types.FleetEntry{
		{EndpointID: hot.Spec.ID, Priority: 1, MaxReplicas: 1},
		{EndpointID: cold.Spec.ID, Priority: 99, Serverless: true, MaxReplicas: 1},
	}
	inv := noRoomInventory()
	inv.pools["A100-80"] = []eligiblePool{{Name: "gpu-a", Locality: "gpu-a"}}
	inv.workers["w2"] = &inventoryWorker{pool: "gpu-a", gpu: "A100-80", total: 1}
	demand := map[string]*endpointDemand{cold.Spec.ID: {active: 1, warm: true}}
	s.controller.fill(ctx, "H100", entries, endpoints, live, inv, demand)
	require.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, h100.ID))
	s.controller.fill(ctx, "A100-80", entries, endpoints, live, inv, demand)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, a100.ID), "one request does not reclaim both GPU types in one tick")

	// Demand is freshly observed each tick. A slow drain on H100 must not
	// cause another destructive reclaim on A100 before that capacity returns.
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{cold.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{
		"H100": {Priority: 99, Serverless: true, MaxReplicas: 1}, "A100-80": {Priority: 99, Serverless: true, MaxReplicas: 1},
	}}})
	_, err = s.demand(ctx, cold.Spec.ID, "acquire", "request", 0)
	require.NoError(t, err)
	demand = s.controller.readDemand(ctx, fleet, live)
	require.True(t, demand[cold.Spec.ID].starting, "the donor's pending drain belongs to the requesting model")
	s.controller.fill(ctx, "A100-80", entries, endpoints, live, inv, demand)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, a100.ID), "an existing drain fences reclaim across later ticks too")
}

func TestServerlessReclaimPrefersOnlyUsableIdleAlternatives(t *testing.T) {
	for _, alternative := range []string{"available", "at-cap", "backoff"} {
		t.Run(alternative, func(t *testing.T) {
			s := newFillService(t)
			hot, cold, endpoints := fillEndpoints(t, s)
			cold.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 1}, "A100-80": {Count: 1}}
			victim := reclaimableReplica(t, s, hot, "hot-h100", "w1")
			live := []*types.EndpointReplica{victim}
			ctx := context.Background()
			fleet := seedFleet(t, s, map[string]types.FleetEndpoint{
				hot.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1}}},
				cold.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{
					"H100": {Priority: 99, Serverless: true, MaxReplicas: 1}, "A100-80": {Priority: 99, Serverless: true, MaxReplicas: 1},
				}},
			})
			inv := noRoomInventory()
			inv.pools["A100-80"] = []eligiblePool{{Name: "gpu-b", Locality: "gpu-b"}}
			inv.workers["w2"] = &inventoryWorker{pool: "gpu-b", gpu: "A100-80", total: 2, free: 1}
			switch alternative {
			case "at-cap":
				current := versionReplica(t, s, "cold-a100", cold.Version, types.ReplicaStatusReady)
				current.EndpointID, current.GPU = cold.Spec.ID, "A100-80"
				current.Capacity.MaxConcurrency = 1
				require.NoError(t, s.repo.SaveReplica(ctx, current))
				live = append(live, current)
			case "backoff":
				require.NoError(t, s.repo.SetScheduleBackoff(ctx, cold.Spec.ID, "A100-80", time.Minute))
			}
			for _, id := range []string{"request-1", "request-2"} {
				_, err := s.demand(ctx, cold.Spec.ID, "acquire", id, 0)
				require.NoError(t, err)
			}
			demand := s.controller.readDemand(ctx, fleet, live)
			s.controller.fill(ctx, "H100", fleet.Entries("H100"), endpoints, live, inv, demand)
			if alternative == "available" {
				assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, victim.ID), "idle A100 capacity saves the hot H100 replica")
				s.controller.fill(ctx, "A100-80", fleet.Entries("A100-80"), endpoints, live, inv, demand)
				requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
				require.NoError(t, err)
				require.Len(t, requests, 1)
				assert.Equal(t, cold.StubID, requests[0].StubId)
				assert.Equal(t, "gpu-b", requests[0].PoolSelector)
			} else {
				assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, victim.ID), "unusable alternative capacity must not strand demand")
			}
		})
	}
}
