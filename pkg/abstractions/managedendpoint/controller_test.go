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
		localities: map[string][]string{"H100": {"eu-west"}},
		pools: map[string][]eligiblePool{
			"H100":          {{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "eu-west"}},
			cpuInventoryKey: {{Name: "cpu-a", Locality: "us-east"}},
		},
	}
}

func h100Inventory() map[string]*gpuInventory {
	return map[string]*gpuInventory{
		"H100": {GPU: "H100", Workers: []workerSlot{
			{WorkerID: "w1", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 8},
			{WorkerID: "w2", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 4, Held: 2},
			{WorkerID: "w3", PoolName: "gpu-b", Locality: "eu-west", Total: 8, Free: 8, MaxShare: 0.25},
		}},
	}
}

func target(endpointID, gpu string, count uint32, share float64, minR, maxR, demand uint32) fillTarget {
	return fillTarget{
		EndpointID: endpointID,
		RoleTarget: types.RoleTarget{Role: types.ReplicaRoleServe, Target: types.GpuTarget{Type: gpu, Count: count, Share: share, MinReplicas: minR, MaxReplicas: maxR}},
		Demand:     demand,
	}
}

func TestInventoryAllowance(t *testing.T) {
	inv := h100Inventory()["H100"]
	// 0.5 cluster share over free+held (serverless-held GPUs on w2 excluded):
	// w1 4 + w2 3 + w3 min(0.5,0.25)*8=2 -> 9
	assert.Equal(t, uint32(9), inv.allowance(0.5))
}

func TestPlanFillDividesByShareAndClamps(t *testing.T) {
	targets := []fillTarget{
		target("a", "H100", 1, 0.5, 0, 0, 0),
		target("b", "H100", 2, 0.5, 1, 1, 0),
		target("c", "A100", 1, 1, 1, 0, 0),
	}
	plans := planFill(h100Inventory(), targets, 0.5)

	// allowance 9: a gets floor(9*0.5/1)=4, b gets floor(9*0.5/2)=2 clamped to max 1
	assert.Equal(t, fillPlan{Quota: 4, Desired: 4}, plans[targets[0].key()])
	assert.Equal(t, fillPlan{Quota: 2, Desired: 1}, plans[targets[1].key()])
	// no A100 inventory: quota 0, min_replicas still requested
	assert.Equal(t, fillPlan{Quota: 0, Desired: 1}, plans[targets[2].key()])
}

func TestPlanFillNormalizesOversubscribedShares(t *testing.T) {
	targets := []fillTarget{target("a", "H100", 1, 1, 0, 0, 0), target("b", "H100", 1, 1, 0, 0, 0)}
	plans := planFill(h100Inventory(), targets, 0.5)
	total := plans[targets[0].key()].Quota + plans[targets[1].key()].Quota
	assert.LessOrEqual(t, total, uint32(9))
	assert.Equal(t, plans[targets[0].key()].Quota, plans[targets[1].key()].Quota)
}

func TestPlanFillDemandGrowsPastQuotaWithinMax(t *testing.T) {
	targets := []fillTarget{target("a", "H100", 1, 0.1, 0, 3, 5)}
	plans := planFill(h100Inventory(), targets, 0.5)
	assert.Equal(t, fillPlan{Quota: 0, Desired: 3}, plans[targets[0].key()])
}

func TestReservePrefersMostFreeWorker(t *testing.T) {
	inv := h100Inventory()["H100"]
	any := func(workerSlot) bool { return true }
	slot, ok := inv.reserve(2, any)
	require.True(t, ok)
	// w1 and w3 both have 8 free; w1 has fewer held -> tie broken by held then order
	assert.Equal(t, "w1", slot.WorkerID)
	assert.Equal(t, uint32(6), inv.Workers[0].Free)
	assert.Equal(t, uint32(2), inv.Workers[0].Held)

	slot, ok = inv.reserve(1, func(w workerSlot) bool { return w.Locality == "eu-west" })
	require.True(t, ok)
	assert.Equal(t, "w3", slot.WorkerID)

	_, ok = inv.reserve(16, any)
	assert.False(t, ok)
}

func TestPlaceProtectedFallsBackToEligiblePoolOnly(t *testing.T) {
	c := newServiceForTest(t).controller
	h100 := types.GpuTarget{Type: "H100", Count: 1}

	// Free capacity wins regardless of protection.
	free := &clusterInventory{byType: h100Inventory(), pools: map[string][]eligiblePool{"H100": {{Name: "other", Locality: "ap"}}}}
	pool, loc, ok := c.place(free, h100, nil, true)
	require.True(t, ok)
	assert.Equal(t, "gpu-a", pool)
	assert.Equal(t, "us-east", loc)

	// No free worker: opportunistic replicas are not placed at all.
	_, _, ok = c.place(fullInventory(), h100, nil, false)
	assert.False(t, ok)

	// A protected replica gets an eligible pool, never an empty selector.
	pool, loc, ok = c.place(fullInventory(), h100, nil, true)
	require.True(t, ok)
	assert.Equal(t, "gpu-a", pool)
	assert.Equal(t, "us-east", loc)

	// Requested localities are honored in preference order ...
	pool, loc, ok = c.place(fullInventory(), h100, []string{"eu-west", "us-east"}, true)
	require.True(t, ok)
	assert.Equal(t, "gpu-b", pool)
	assert.Equal(t, "eu-west", loc)

	// ... and never ignored: an unknown locality places nothing.
	pool, _, ok = c.place(fullInventory(), h100, []string{"ap-south"}, true)
	assert.False(t, ok)
	assert.Empty(t, pool)

	// A GPU type no pool opted in for places nothing, even when protected.
	pool, _, ok = c.place(fullInventory(), types.GpuTarget{Type: "A100-80G", Count: 1}, nil, true)
	assert.False(t, ok)
	assert.Empty(t, pool)

	// CPU targets fall back the same way when no CPU worker is registered.
	pool, loc, ok = c.place(fullInventory(), types.CPUTarget(), nil, true)
	require.True(t, ok)
	assert.Equal(t, "cpu-a", pool)
	assert.Equal(t, "us-east", loc)
	_, _, ok = c.place(fullInventory(), types.CPUTarget(), nil, false)
	assert.False(t, ok)
}

func TestPlaceServiceRequiresEligiblePool(t *testing.T) {
	c := newServiceForTest(t).controller
	spec := &types.ManagedServiceSpec{Gpu: []types.GpuTarget{{Type: "A100-80G", Count: 1}, {Type: "H100", Count: 1}}}

	// The first target has no eligible pool; the second falls back to one.
	target, pool, loc, ok := c.placeService(fullInventory(), spec, nil)
	require.True(t, ok)
	assert.Equal(t, "H100", target.Type)
	assert.Equal(t, "gpu-a", pool)
	assert.Equal(t, "us-east", loc)

	// Nothing eligible in the requested locality: the service is not started.
	_, pool, _, ok = c.placeService(fullInventory(), spec, []string{"ap-south"})
	assert.False(t, ok)
	assert.Empty(t, pool)

	inv := fullInventory()
	inv.pools = nil
	_, _, _, ok = c.placeService(inv, spec, nil)
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

	inv, err := s.controller.inventory(nil)
	require.NoError(t, err)
	assert.Equal(t, []eligiblePool{{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "gpu-b"}}, inv.pools["H100"])
	assert.Equal(t, []eligiblePool{{Name: "cpu-a", Locality: "cpu-a"}}, inv.pools[cpuInventoryKey])
	assert.Empty(t, inv.cpuPools, "cpu pools with no worker are only reachable through the fallback")
	require.Len(t, inv.byType["H100"].Workers, 1)

	// Exhaust the only worker: fallback placement must still find a pool.
	inv.byType["H100"].Workers[0].Free = 0
	h100 := types.GpuTarget{Type: "H100", Count: 1}
	_, _, ok := s.controller.place(inv, h100, nil, false)
	assert.False(t, ok, "no free capacity for opportunistic replicas")
	pool, _, ok := s.controller.place(inv, h100, nil, true)
	require.True(t, ok)
	assert.Equal(t, "gpu-a", pool)
	_, _, ok = s.controller.place(inv, h100, []string{"opt-out"}, true)
	assert.False(t, ok, "pools that did not opt in are never selected")
}

func TestPartitionAndScaleDownOrdering(t *testing.T) {
	now := time.Now()
	replicas := []*types.EndpointReplica{
		{ID: "old-ready", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Hour)},
		{ID: "protected", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusReady, Protected: true, StartedAt: now.Add(-2 * time.Hour)},
		{ID: "loading", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusLoading, StartedAt: now},
		{ID: "busy", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Minute), Capacity: types.ReplicaCapacity{InFlight: 4}},
		{ID: "draining", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusDraining},
		{ID: "evicting", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusEvicting},
		{ID: "tuning", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusReady, Tuning: true},
		{ID: "other-version", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 2, Status: types.ReplicaStatusReady},
	}
	set := partitionReplicas(replicas, "e", "serve", "H100x1", 1)
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

func TestAppendEngineArgs(t *testing.T) {
	sh := []string{"sh", "-c", "cd /app && vllm serve model"}
	got := appendEngineArgs(sh, []string{"--max-model-len", "8192", "--kv-cache-dtype", "fp8 e5m2"})
	assert.Equal(t, "cd /app && vllm serve model --max-model-len 8192 --kv-cache-dtype 'fp8 e5m2'", got[2])

	argv := []string{"python", "serve.py"}
	assert.Equal(t, []string{"python", "serve.py", "--tp", "2"}, appendEngineArgs(argv, []string{"--tp", "2"}))
	assert.Equal(t, argv, appendEngineArgs(argv, nil))
}

func TestFleetKey(t *testing.T) {
	assert.Equal(t, "serve:H100x1@v12", fleetKey("serve", "H100x1", 12))
	assert.Equal(t, "serve:cpu@v1", fleetKey("", "cpu", 1))

	target, version := parseFleetKey("decode:H100x2@v7")
	assert.Equal(t, "decode:H100x2", target)
	assert.Equal(t, uint(7), version)

	target, version = parseFleetKey("serve:H100x1")
	assert.Equal(t, "serve:H100x1", target)
	assert.Equal(t, uint(0), version)
}

// --- eviction ------------------------------------------------------------------

func TestEvictOrderPrefersPrefillThenServeThenDecode(t *testing.T) {
	assert.Less(t, evictOrder(types.ReplicaRolePrefill), evictOrder(types.ReplicaRoleServe))
	assert.Less(t, evictOrder(types.ReplicaRoleServe), evictOrder(types.ReplicaRoleDecode))
	assert.Equal(t, evictOrder(types.ReplicaRoleServe), evictOrder(""))
}

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
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, targetKey(replica.Role, replica.GPU))
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

func TestReplicaProbeUsesServiceHealthPath(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveService(ctx, &types.ManagedService{
		Spec: types.ManagedServiceSpec{Name: "mooncake", Port: 9000, Health: "/healthz"}, StubID: "svc-1", Version: 1, Enabled: true,
	}))
	probe := s.controller.replicaProbe(ctx, &types.EndpointReplica{ID: "svc-rep", EndpointID: serviceReplicaID("mooncake")})
	assert.Equal(t, probeTarget{Port: 9000, Health: "/healthz"}, probe)

	endpoint := seedEndpoint(t, s)
	probe = s.controller.replicaProbe(ctx, seedReplica(t, s, endpoint))
	assert.Equal(t, uint32(8000), probe.Port)
	assert.Equal(t, "/health", probe.Health)
	require.NotNil(t, probe.Endpoint)
	assert.Equal(t, endpoint.Spec.ID, probe.Endpoint.ID)

	assert.Equal(t, probeTarget{}, s.controller.replicaProbe(ctx, &types.EndpointReplica{EndpointID: serviceReplicaID("missing")}))
}

// --- rollouts ------------------------------------------------------------------

func TestEvaluateRolloutHonorsMinCanaryRequests(t *testing.T) {
	ready := []*types.EndpointReplica{{Status: types.ReplicaStatusReady}}
	few := &types.RouteMetrics{Requests: 10, Errors: 5}
	cfg := types.ManagedEndpointsRolloutConfig{MinCanaryRequests: 20}

	promote, reason := evaluateRollout(nil, few, ready, ready, cfg)
	assert.True(t, promote, reason)

	cfg.MinCanaryRequests = 5
	promote, reason = evaluateRollout(nil, few, ready, ready, cfg)
	assert.False(t, promote)
	assert.Contains(t, reason, "error rate")
	assert.Contains(t, reason, "min sample 5")

	// The production default applies when the sample size is not configured.
	promote, _ = evaluateRollout(nil, few, ready, ready, rolloutConfig(types.RolloutThresholds{}))
	assert.True(t, promote)
	promote, _ = evaluateRollout(nil, &types.RouteMetrics{Requests: 20, Errors: 5}, ready, ready, rolloutConfig(types.RolloutThresholds{}))
	assert.False(t, promote)
}

func TestRequiredRolesAndReadiness(t *testing.T) {
	mono := &types.ManagedEndpointSpec{Gpu: []types.GpuTarget{{Type: "H100", Count: 1}, {Type: "A100-80G", Count: 1}}}
	assert.Equal(t, []string{types.ReplicaRoleServe}, requiredRoles(mono))

	pd := &types.ManagedEndpointSpec{Topology: &types.TopologySpec{Mode: types.TopologyDisaggregated, Roles: map[string][]types.GpuTarget{
		types.ReplicaRolePrefill: {{Type: "H100", Count: 1}},
		types.ReplicaRoleDecode:  {{Type: "H100", Count: 1}, {Type: "H100", Count: 2}},
	}}}
	roles := requiredRoles(pd)
	assert.ElementsMatch(t, []string{types.ReplicaRolePrefill, types.ReplicaRoleDecode}, roles)

	replicas := []*types.EndpointReplica{
		{Role: types.ReplicaRolePrefill, Status: types.ReplicaStatusReady},
		{Role: types.ReplicaRoleDecode, Status: types.ReplicaStatusLoading},
		{Role: types.ReplicaRoleDecode, Status: types.ReplicaStatusFailed},
	}
	role, missing := missingReadyRole(replicas, roles)
	assert.True(t, missing)
	assert.Equal(t, types.ReplicaRoleDecode, role)
	assert.Len(t, roleReplicas(replicas, types.ReplicaRoleDecode), 1, "terminal replicas do not count")

	replicas[1].Status = types.ReplicaStatusReady
	_, missing = missingReadyRole(replicas, roles)
	assert.False(t, missing)
}

// seedCanary puts a disaggregated version 2 of the endpoint into the bake
// phase with its spec pre-cached, so stepRollout needs no backend.
func seedCanary(t *testing.T, s *Service, endpoint *types.ManagedEndpoint) (*types.ManagedEndpointSpec, *types.RolloutState) {
	t.Helper()
	spec := endpoint.Spec
	spec.Harness = types.HarnessSpec{}
	spec.KVCache = &types.KVCacheSpec{Connector: "lmcache"}
	spec.Topology = &types.TopologySpec{Mode: types.TopologyDisaggregated, Roles: map[string][]types.GpuTarget{
		types.ReplicaRolePrefill: {{Type: "H100", Count: 1}},
		types.ReplicaRoleDecode:  {{Type: "H100", Count: 1}},
	}}
	spec.Normalize()
	s.controller.stubCache["stub-2"] = cachedStub{
		stub:    &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub-2"}},
		config:  &types.StubConfigV1{ManagedEndpoint: &types.ManagedEndpointStubConfig{Endpoint: &spec}},
		fetched: time.Now(),
	}
	ctx := context.Background()
	require.NoError(t, s.repo.SaveVersion(ctx, &types.EndpointVersion{EndpointID: endpoint.Spec.ID, Version: 2, StubID: "stub-2", State: types.VersionStateCanary, CreatedAt: time.Now()}))
	rollout := &types.RolloutState{EndpointID: endpoint.Spec.ID, ActiveVersion: 1, CanaryVersion: 2, Phase: types.RolloutPhaseBaking}
	require.NoError(t, s.repo.SaveRollout(ctx, rollout))
	return &spec, rollout
}

func canaryReplica(id, role string, status types.ReplicaStatus) *types.EndpointReplica {
	return &types.EndpointReplica{ID: id, EndpointID: "acme/model", Version: 2, Role: role, GPU: "H100x1", GPUCount: 1, Status: status, Protected: true, StartedAt: time.Now()}
}

func TestStepRolloutBakesOnlyWhenEveryRoleIsReady(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	_, rollout := seedCanary(t, s, endpoint)
	ctx := context.Background()
	inv := &clusterInventory{byType: map[string]*gpuInventory{}, pools: map[string][]eligiblePool{}}

	// Prefill is ready but decode has no canary at all, and no pool is
	// eligible to start one: the bake must not begin.
	live := []*types.EndpointReplica{canaryReplica("pf", types.ReplicaRolePrefill, types.ReplicaStatusReady)}
	require.NoError(t, s.controller.stepRollout(ctx, endpoint, rollout, live, inv))
	assert.True(t, rollout.BakeStartedAt.IsZero())

	// A decode canary that is still loading does not start the bake either.
	live = append(live, canaryReplica("dc", types.ReplicaRoleDecode, types.ReplicaStatusLoading))
	require.NoError(t, s.controller.stepRollout(ctx, endpoint, rollout, live, inv))
	assert.True(t, rollout.BakeStartedAt.IsZero())

	live[1].Status = types.ReplicaStatusReady
	require.NoError(t, s.controller.stepRollout(ctx, endpoint, rollout, live, inv))
	assert.False(t, rollout.BakeStartedAt.IsZero())
	assert.Equal(t, types.RolloutPhaseBaking, rollout.Phase)

	// A role that loses its only ready replica during the bake rolls back
	// instead of promoting on the surviving role's metrics.
	rollout.BakeStartedAt = time.Now().Add(-time.Duration(s.config.Rollout.BakeSeconds+1) * time.Second)
	live[1].Status = types.ReplicaStatusLoading
	require.NoError(t, s.controller.stepRollout(ctx, endpoint, rollout, live, inv))
	assert.Equal(t, types.RolloutPhaseRolledBack, rollout.Phase)
	assert.Contains(t, rollout.LastDecision, "decode")
}

// failingReplicaList cannot list replicas.
type failingReplicaList struct {
	repository.ManagedEndpointRepository
}

func (failingReplicaList) ListReplicas(context.Context, string) ([]*types.EndpointReplica, error) {
	return nil, errors.New("redis: connection refused")
}

func TestFinishRolloutReturnsListReplicasError(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	_, rollout := seedCanary(t, s, endpoint)
	ctx := context.Background()
	s.repo = failingReplicaList{s.repo}

	require.Error(t, s.controller.finishRollout(ctx, endpoint, rollout, false, "bad canary"))
	assert.Equal(t, types.RolloutPhaseBaking, rollout.Phase, "rollback is not recorded until canaries can be drained")
	assert.Equal(t, uint(2), rollout.CanaryVersion)
	versions, err := s.repo.ListVersions(ctx, endpoint.Spec.ID)
	require.NoError(t, err)
	assert.Equal(t, types.VersionStateCanary, findVersion(versions, 2).State)
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
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, targetKey(replica.Role, replica.GPU))
	require.NoError(t, err)
	assert.True(t, backoff)
}

// rolloutConfig is the default rollout config with th as thresholds.
func rolloutConfig(th types.RolloutThresholds) types.ManagedEndpointsRolloutConfig {
	cfg := types.ManagedEndpointsConfig{Rollout: types.ManagedEndpointsRolloutConfig{Thresholds: th}}
	cfg.ApplyDefaults()
	return cfg.Rollout
}
