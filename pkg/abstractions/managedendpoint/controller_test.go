package managedendpoint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullInventory has eligible pools for H100 and CPU but no free GPU anywhere.
func fullInventory() *clusterInventory {
	return &clusterInventory{
		byType: map[string]*gpuInventory{
			"H100": {GPU: "H100", Workers: []workerSlot{{WorkerID: "w1", PoolName: "gpu-b", Locality: "eu-west", Total: 8, Free: 0, Held: 4}}},
		},
		pools: map[string][]eligiblePool{
			"H100":                {{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "eu-west"}},
			types.CPUInventoryKey: {{Name: "cpu-a", Locality: "us-east"}},
		},
	}
}

func h100Inventory() map[string]*gpuInventory {
	return map[string]*gpuInventory{
		"H100": {GPU: "H100", clusterShare: 0.5, Workers: []workerSlot{
			{WorkerID: "w1", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 8},
			{WorkerID: "w2", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 4, Held: 2},
			{WorkerID: "w3", PoolName: "gpu-b", Locality: "eu-west", Total: 8, Free: 8, MaxShare: 0.25},
		}},
	}
}

func fleetTarget(gpu string, count uint32, share float64, minR, maxR uint32) types.FleetTarget {
	return types.FleetTarget{GPU: gpu, Placement: types.Placement{Share: share, Min: minR, Max: maxR, Count: count}}
}

func target(endpointID, gpu string, count uint32, share float64, minR, maxR, demand uint32) fillTarget {
	return fillTarget{EndpointID: endpointID, FleetTarget: fleetTarget(gpu, count, share, minR, maxR), Demand: demand}
}

func TestInventoryAllowance(t *testing.T) {
	inv := h100Inventory()["H100"]
	// 0.5 cluster share over free+held (serverless-held GPUs on w2 excluded):
	// w1 4 + w2 3 + w3 min(0.5,0.25)*8=2 -> 9
	assert.Equal(t, uint32(9), inv.allowance())
	assert.Equal(t, uint32(7), inv.poolAllowance("gpu-a"))
	assert.Equal(t, uint32(2), inv.poolAllowance("gpu-b"))
	assert.Equal(t, uint32(2), inv.poolHeld("gpu-a"))
}

func TestPlanFillDividesByShareAndClamps(t *testing.T) {
	targets := []fillTarget{
		target("a", "H100", 1, 0.5, 0, 0, 0),
		target("b", "H100", 2, 0.5, 1, 1, 0),
		target("c", "A100", 1, 1, 1, 0, 0),
	}
	plans := planFill(h100Inventory(), targets)

	// allowance 9: a gets floor(9*0.5/1)=4, b gets floor(9*0.5/2)=2 clamped to max 1
	assert.Equal(t, fillPlan{Quota: 4, Desired: 4}, plans[targets[0].key()])
	assert.Equal(t, fillPlan{Quota: 2, Desired: 1}, plans[targets[1].key()])
	// no A100 inventory: quota 0, min still requested
	assert.Equal(t, fillPlan{Quota: 0, Desired: 1}, plans[targets[2].key()])
}

func TestPlanFillNormalizesOversubscribedShares(t *testing.T) {
	targets := []fillTarget{target("a", "H100", 1, 1, 0, 0, 0), target("b", "H100", 1, 1, 0, 0, 0)}
	plans := planFill(h100Inventory(), targets)
	total := plans[targets[0].key()].Quota + plans[targets[1].key()].Quota
	assert.LessOrEqual(t, total, uint32(9))
	assert.Equal(t, plans[targets[0].key()].Quota, plans[targets[1].key()].Quota)
}

func TestPlanFillDemandGrowsPastQuotaWithinMax(t *testing.T) {
	targets := []fillTarget{target("a", "H100", 1, 0.1, 0, 3, 5)}
	plans := planFill(h100Inventory(), targets)
	assert.Equal(t, fillPlan{Quota: 0, Desired: 3}, plans[targets[0].key()])

	// Zero share: nothing opportunistic, only the protected min.
	targets = []fillTarget{target("a", "H100", 1, 0, 2, 0, 0)}
	plans = planFill(h100Inventory(), targets)
	assert.Equal(t, fillPlan{Quota: 0, Desired: 2}, plans[targets[0].key()])
}

// TestPlanFillFromFleetPlacements: the controller builds one fillTarget per
// (endpoint, gpu) pair the fleet places, and each GPU type is divided
// independently.
func TestPlanFillFromFleetPlacements(t *testing.T) {
	fleet := &types.Fleet{Targets: map[string]map[string]types.Placement{
		"h100": {"acme/big": {Share: 0.75, Min: 1, Count: 2}, "acme/small": {Share: 0.25, Max: 1}},
		"cpu":  {"acme/small": {Min: 2}},
	}}
	fleet.Normalize()

	var targets []fillTarget
	for _, id := range []string{"acme/big", "acme/small"} {
		for _, ft := range fleet.Placements(id) {
			targets = append(targets, fillTarget{EndpointID: id, FleetTarget: ft})
		}
	}
	require.Len(t, targets, 3)
	plans := planFill(h100Inventory(), targets)

	// allowance 9: big gets floor(9*0.75/2)=3, small floor(9*0.25/1)=2 capped at 1.
	assert.Equal(t, fillPlan{Quota: 3, Desired: 3}, plans["acme/big|H100"])
	assert.Equal(t, fillPlan{Quota: 2, Desired: 1}, plans["acme/small|H100"])
	// No cpu inventory and no share: just the protected min.
	assert.Equal(t, fillPlan{Quota: 0, Desired: 2}, plans["acme/small|cpu"])
	_, unplaced := plans["acme/big|cpu"]
	assert.False(t, unplaced, "an endpoint gets no plan on a GPU the fleet does not place it on")
}

func TestReservePrefersMostFreeWorker(t *testing.T) {
	inv := h100Inventory()["H100"]
	slot, ok := inv.reserve(2)
	require.True(t, ok)
	// w1 and w3 both have 8 free; w1 has fewer held -> tie broken by held then order
	assert.Equal(t, "w1", slot.WorkerID)
	assert.Equal(t, uint32(6), inv.Workers[0].Free)
	assert.Equal(t, uint32(2), inv.Workers[0].Held)

	// Now w3 has the most free GPUs.
	slot, ok = inv.reserve(1)
	require.True(t, ok)
	assert.Equal(t, "w3", slot.WorkerID)

	_, ok = inv.reserve(16)
	assert.False(t, ok)
}

func TestReserveHonoursPoolBudget(t *testing.T) {
	// gpu-b is capped at 0.25 of 8 GPUs: two may be held there, however many are free.
	inv := &gpuInventory{GPU: "H100", clusterShare: 0.5, Workers: []workerSlot{
		{WorkerID: "w3", PoolName: "gpu-b", Total: 8, Free: 8, MaxShare: 0.25},
	}}
	slot, ok := inv.reserve(2)
	require.True(t, ok)
	assert.Equal(t, "w3", slot.WorkerID)
	assert.Equal(t, uint32(6), inv.Workers[0].Free)
	_, ok = inv.reserve(1)
	assert.False(t, ok, "the pool's budget is spent even though the worker has free GPUs")

	// A pool that cannot fit the whole request is skipped for one that can.
	inv = h100Inventory()["H100"]
	slot, ok = inv.reserve(3)
	require.True(t, ok)
	assert.Equal(t, "w1", slot.WorkerID, "gpu-b (allowance 2) cannot host 3 GPUs; gpu-a can")
}

func TestPlaceProtectedFallsBackToEligiblePoolOnly(t *testing.T) {
	c := newServiceForTest(t).controller
	h100 := fleetTarget("H100", 1, 0.5, 0, 0)

	// Free capacity wins regardless of protection.
	free := &clusterInventory{byType: h100Inventory(), pools: map[string][]eligiblePool{"H100": {{Name: "other", Locality: "ap"}}}}
	pool, ok := c.place(free, h100, true)
	require.True(t, ok)
	assert.Equal(t, eligiblePool{Name: "gpu-a", Locality: "us-east"}, pool)

	// No free worker: opportunistic replicas are not placed at all.
	_, ok = c.place(fullInventory(), h100, false)
	assert.False(t, ok)

	// A protected replica gets an eligible pool, never an empty selector.
	pool, ok = c.place(fullInventory(), h100, true)
	require.True(t, ok)
	assert.Equal(t, eligiblePool{Name: "gpu-a", Locality: "us-east"}, pool)

	// A GPU type no pool opted in for places nothing, even when protected.
	pool, ok = c.place(fullInventory(), fleetTarget("A100-80G", 1, 0.5, 1, 0), true)
	assert.False(t, ok)
	assert.Empty(t, pool.Name)

	// CPU has no GPU inventory to reserve: targets go to the first eligible
	// CPU pool whether protected or not, and nowhere when no pool opted in.
	cpu := fleetTarget(types.CPUInventoryKey, 0, 1, 1, 0)
	pool, ok = c.place(fullInventory(), cpu, true)
	require.True(t, ok)
	assert.Equal(t, eligiblePool{Name: "cpu-a", Locality: "us-east"}, pool)
	pool, ok = c.place(fullInventory(), cpu, false)
	require.True(t, ok)
	assert.Equal(t, "cpu-a", pool.Name)
	noCPU := fullInventory()
	delete(noCPU.pools, types.CPUInventoryKey)
	_, ok = c.place(noCPU, cpu, true)
	assert.False(t, ok)
}

func TestInventoryListsEligiblePoolsWithoutFreeWorkers(t *testing.T) {
	s := newServiceForTest(t)
	s.workers = repository.NewWorkerRedisRepositoryForTest(s.rdb)
	s.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{
		"gpu-a":   {GPUType: "H100", Locality: "us-east", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"gpu-b":   {GPUType: "H100", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"cpu-a":   {ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"opt-out": {GPUType: "H100"},
	}
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "w1", PoolName: "gpu-b", Gpu: "H100", TotalGpuCount: 8, FreeGpuCount: 8, Status: types.WorkerStatusAvailable}))
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "w2", PoolName: "opt-out", Gpu: "H100", TotalGpuCount: 8, FreeGpuCount: 8, Status: types.WorkerStatusAvailable}))

	held := []*types.EndpointReplica{{ID: "r", EndpointID: "e", WorkerID: "w1", GPUCount: 2, Status: types.ReplicaStatusReady}}
	inv, err := s.controller.inventory(held)
	require.NoError(t, err)
	assert.Equal(t, []eligiblePool{{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "gpu-b"}}, inv.pools["H100"])
	assert.Equal(t, []eligiblePool{{Name: "cpu-a", Locality: "cpu-a"}}, inv.pools[types.CPUInventoryKey])
	require.Len(t, inv.byType["H100"].Workers, 1, "workers in pools that did not opt in are invisible")
	assert.Equal(t, uint32(2), inv.byType["H100"].Workers[0].Held)

	// Exhaust the only worker: fallback placement must still find a pool.
	inv.byType["H100"].Workers[0].Free = 0
	h100 := fleetTarget("H100", 1, 0.5, 1, 0)
	_, ok := s.controller.place(inv, h100, false)
	assert.False(t, ok, "no free capacity for opportunistic replicas")
	pool, ok := s.controller.place(inv, h100, true)
	require.True(t, ok)
	assert.Equal(t, "gpu-a", pool.Name)
}

func TestPartitionAndScaleDownOrdering(t *testing.T) {
	now := time.Now()
	replicas := []*types.EndpointReplica{
		{ID: "old-ready", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Hour)},
		{ID: "protected", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, Protected: true, StartedAt: now.Add(-2 * time.Hour)},
		{ID: "loading", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusLoading, StartedAt: now},
		{ID: "busy", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Minute), Capacity: types.ReplicaCapacity{InFlight: 4}},
		{ID: "draining", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusDraining},
		{ID: "evicting", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusEvicting},
		{ID: "other-gpu", EndpointID: "e", GPU: "A100", Version: 1, Status: types.ReplicaStatusReady},
		{ID: "other-endpoint", EndpointID: "f", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady},
		{ID: "other-version", EndpointID: "e", GPU: "H100", Version: 2, Status: types.ReplicaStatusReady},
	}
	set := partitionReplicas(replicas, "e", "H100", 1)
	assert.Len(t, set.Live, 4)
	assert.Len(t, set.Ready, 3)
	assert.Equal(t, uint32(1), set.Protected)

	ids := make([]string, 0, len(set.Live))
	for _, r := range scaleDownCandidates(set) {
		ids = append(ids, r.ID)
	}
	// unprotected first; not-ready before ready; least loaded; newest first
	assert.Equal(t, []string{"loading", "old-ready", "busy", "protected"}, ids)
}

func TestDemandFollowsRecentTraffic(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	ft := fleetTarget("H100", 1, 0.5, 0, 0)
	ready := &types.EndpointReplica{ID: "r1", EndpointID: endpoint.Spec.ID, GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, Capacity: types.ReplicaCapacity{MaxConcurrency: 10}}
	live := []*types.EndpointReplica{ready}

	assert.Equal(t, uint32(0), s.controller.demand(ctx, endpoint, ft, nil), "nothing ready, nothing demanded")
	assert.Equal(t, uint32(0), s.controller.demand(ctx, endpoint, ft, live), "idle for the whole window: capacity returns to quota and min")

	// Traffic a couple of minutes ago: the last minute is quiet but the
	// endpoint is not idle, so what is serving is kept.
	require.NoError(t, s.repo.RecordRouteSample(ctx, types.RouteSample{EndpointID: endpoint.Spec.ID, GPU: "H100", StatusCode: 200, At: time.Now().Add(-2 * time.Minute)}))
	assert.Equal(t, uint32(1), s.controller.demand(ctx, endpoint, ft, live))

	// Queueing at the router in the last minute asks for one more.
	require.NoError(t, s.repo.RecordRouteSample(ctx, types.RouteSample{EndpointID: endpoint.Spec.ID, GPU: "H100", StatusCode: 200, QueueWait: 2 * s.config.Routing.MaxQueueWait, At: time.Now()}))
	assert.Equal(t, uint32(2), s.controller.demand(ctx, endpoint, ft, live))
}

func TestAppendEngineArgs(t *testing.T) {
	sh := []string{"sh", "-c", "cd /app && vllm serve model"}
	got := appendEngineArgs(sh, []string{"--max-model-len", "8192", "--kv-cache-dtype", "fp8 e5m2"})
	assert.Equal(t, "cd /app && vllm serve model --max-model-len 8192 --kv-cache-dtype 'fp8 e5m2'", got[2])

	argv := []string{"python", "serve.py"}
	assert.Equal(t, []string{"python", "serve.py", "--tp", "2"}, appendEngineArgs(argv, []string{"--tp", "2"}))
	assert.Equal(t, argv, appendEngineArgs(argv, nil))
}

// --- version replacement ---------------------------------------------------------

// versionReplica is a live replica of acme/model on H100 at the given version.
func versionReplica(t *testing.T, s *Service, id string, version uint, status types.ReplicaStatus) *types.EndpointReplica {
	t.Helper()
	replica := &types.EndpointReplica{
		ID: id, EndpointID: "acme/model", Version: version, GPU: "H100", GPUCount: 1, ContainerID: "managed-" + id,
		Status: status, HarnessEnabled: true, StartedAt: time.Now(),
	}
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	return replica
}

func statusOf(t *testing.T, s *Service, id string) types.ReplicaStatus {
	t.Helper()
	replica, err := s.repo.GetReplica(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, replica)
	return replica.Status
}

func TestRetireStaleVersionsWaitsForCurrentReady(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	ctx := context.Background()

	oldReady := versionReplica(t, s, "v1-ready", 1, types.ReplicaStatusReady)
	oldLoading := versionReplica(t, s, "v1-loading", 1, types.ReplicaStatusLoading)
	newLoading := versionReplica(t, s, "v2-loading", 2, types.ReplicaStatusLoading)
	live := []*types.EndpointReplica{oldReady, oldLoading, newLoading}

	// Nothing of v2 is ready: the serving v1 replica keeps the traffic, but
	// a v1 replica that is not serving anyway is retired right away.
	s.controller.retireStaleVersions(ctx, endpoint, "H100", live)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, oldLoading.ID), "a loading replica is stopped without a drain window")
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, newLoading.ID))

	// Only one stale replica is retired per tick, so a second pass with the
	// same picture drains nothing more (the ready one is still needed).
	s.controller.retireStaleVersions(ctx, endpoint, "H100", live)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))

	// Once v2 has a ready replica the old one is drained with the spec's grace.
	newLoading.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(ctx, newLoading))
	s.controller.retireStaleVersions(ctx, endpoint, "H100", live)
	stale, err := s.repo.GetReplica(ctx, oldReady.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusDraining, stale.Status)
	assert.Contains(t, stale.StatusReason, "version 1 retired")
	assert.WithinDuration(t, time.Now().Add(time.Duration(endpoint.Spec.DrainSeconds)*time.Second), stale.DrainDeadline, 2*time.Second)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, newLoading.ID), "current-version replicas are untouched")

	// Replicas on another GPU type are out of scope for this call.
	otherGPU := versionReplica(t, s, "v1-a100", 1, types.ReplicaStatusReady)
	otherGPU.GPU = "A100"
	require.NoError(t, s.repo.SaveReplica(ctx, otherGPU))
	s.controller.retireStaleVersions(ctx, endpoint, "H100", []*types.EndpointReplica{otherGPU, newLoading})
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, otherGPU.ID))
}

func TestReconcileEndpointDrainsUnplacedAndRetired(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	inv := &clusterInventory{byType: map[string]*gpuInventory{}, pools: map[string][]eligiblePool{}}

	placed := versionReplica(t, s, "on-h100", 1, types.ReplicaStatusLoading)
	unplaced := versionReplica(t, s, "on-a100", 1, types.ReplicaStatusLoading)
	unplaced.GPU = "A100"
	require.NoError(t, s.repo.SaveReplica(ctx, unplaced))
	live := []*types.EndpointReplica{placed, unplaced}

	// fleet.yaml only places acme/model on H100: the A100 replica goes.
	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	plans := map[string]fillPlan{"acme/model|H100": {Quota: 1, Desired: 1}}
	require.NoError(t, s.controller.reconcileEndpoint(ctx, endpoint, fleet, plans, live, inv))
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, placed.ID))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, unplaced.ID))
	reason, _ := s.repo.GetReplica(ctx, unplaced.ID)
	assert.Equal(t, "removed from fleet", reason.StatusReason)

	// Over the plan: the excess is scaled down (nothing protected here).
	extra := versionReplica(t, s, "extra", 1, types.ReplicaStatusLoading)
	require.NoError(t, s.controller.reconcileEndpoint(ctx, endpoint, fleet, plans, []*types.EndpointReplica{placed, extra}, inv))
	stopped := 0
	for _, id := range []string{placed.ID, extra.ID} {
		if statusOf(t, s, id) == types.ReplicaStatusStopped {
			stopped++
		}
	}
	assert.Equal(t, 1, stopped)

	// A retired endpoint drains everything it still has.
	endpoint.Status = types.EndpointStatusRetired
	survivor := versionReplica(t, s, "survivor", 1, types.ReplicaStatusLoading)
	require.NoError(t, s.controller.reconcileEndpoint(ctx, endpoint, fleet, plans, []*types.EndpointReplica{survivor}, inv))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, survivor.ID))
}

// --- eviction ------------------------------------------------------------------

func TestSyncReplicaFollowsSchedulerEviction(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	replica.StartedAt = time.Now().Add(-time.Hour)
	ctx := context.Background()

	require.NoError(t, s.containers.SetContainerState(replica.ContainerID, &types.ContainerState{
		ContainerId: replica.ContainerID, Status: types.ContainerStatusRunning, WorkerId: "w1",
		Evictable: true, DrainSeconds: 30,
	}))
	// The scheduler picked this replica as a victim.
	require.NoError(t, s.rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(replica.ContainerID),
		"status", string(types.ContainerStatusStopping), "evicting", "true").Err())

	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusEvicting, replica.Status)
	assert.WithinDuration(t, time.Now().Add(30*time.Second), replica.DrainDeadline, 5*time.Second)
	assert.Equal(t, "w1", replica.WorkerID)

	// Once the worker finishes the stop, the replica is recorded as evicted.
	require.NoError(t, s.containers.DeleteContainerState(replica.ContainerID))
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, int(types.ContainerExitCodeEvicted)))
	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusEvicted, replica.Status)
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, replica.GPU)
	require.NoError(t, err)
	assert.False(t, backoff, "eviction is not a failure; the fill loop may retry immediately")
}

func TestExitStatusUsesEvictedExitCode(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	// Even a replica that was still loading (never Ready) counts as evicted
	// when the worker reports the eviction exit code.
	replica.Status = types.ReplicaStatusLoading
	replica.Protected = true
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, int(types.ContainerExitCodeEvicted)))
	assert.Equal(t, types.ReplicaStatusEvicted, s.controller.exitStatus(replica))

	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, 1))
	assert.Equal(t, types.ReplicaStatusFailed, s.controller.exitStatus(replica))

	// An engine that drains itself on SIGTERM heartbeats "draining" before
	// the controller observes the victim mark; the eviction exit code still
	// decides the outcome. A plain drain that exits stays a stop.
	replica.Status = types.ReplicaStatusDraining
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, int(types.ContainerExitCodeEvicted)))
	assert.Equal(t, types.ReplicaStatusEvicted, s.controller.exitStatus(replica))
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, 0))
	assert.Equal(t, types.ReplicaStatusStopped, s.controller.exitStatus(replica))
}

// failingContainers reports a repository failure instead of container state.
type failingContainers struct{ repository.ContainerRepository }

func (failingContainers) GetContainerState(string) (*types.ContainerState, error) {
	return nil, errors.New("redis: connection refused")
}

func TestSyncReplicaLeavesReplicaAloneOnRepositoryError(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = failingContainers{repository.NewContainerRedisRepositoryForTest(s.rdb)}
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	replica.StartedAt = time.Now().Add(-time.Hour)

	err := s.controller.syncReplica(context.Background(), replica)
	require.Error(t, err)
	assert.Equal(t, types.ReplicaStatusReady, replica.Status, "a transient error must not finalize a healthy replica")
	assert.True(t, replica.EndedAt.IsZero())

	// The real not-found sentinel still means the container is gone.
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	require.NoError(t, s.controller.syncReplica(context.Background(), replica))
	assert.True(t, replica.Status.Terminal())
}

func TestContainerStateNotFound(t *testing.T) {
	assert.True(t, containerStateNotFound(&types.ErrContainerStateNotFound{ContainerId: "c"}))
	assert.True(t, containerStateNotFound(errors.New((&types.ErrContainerStateNotFound{ContainerId: "c"}).Error())))
	assert.False(t, containerStateNotFound(errors.New("redis: connection refused")))
	assert.False(t, containerStateNotFound(nil))
}

func TestReplicaProbeUsesEndpointHealthPath(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	endpoint := seedEndpoint(t, s)
	probe := s.controller.replicaProbe(ctx, seedReplica(t, s, endpoint))
	assert.Equal(t, uint32(8000), probe.Port)
	assert.Equal(t, "/health", probe.Health)
	require.NotNil(t, probe.Endpoint)
	assert.Equal(t, endpoint.Spec.ID, probe.Endpoint.ID)

	assert.Equal(t, probeTarget{}, s.controller.replicaProbe(ctx, &types.EndpointReplica{EndpointID: "acme/missing"}))
}

func TestSyncReplicaBacksOffWhenOpportunisticPlacementFails(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.StartedAt = time.Now().Add(-time.Minute)
	ctx := context.Background()

	// The scheduler failed the request fast: no state, failed request status.
	require.NoError(t, s.containers.SetContainerRequestStatus(replica.ContainerID, types.ContainerRequestStatusFailed))
	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusFailed, replica.Status)
	assert.Equal(t, "not scheduled: no idle capacity", replica.StatusReason)
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, replica.GPU)
	require.NoError(t, err)
	assert.True(t, backoff)
}
