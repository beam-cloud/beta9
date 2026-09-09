package managedendpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestPickWorkerPrefersMostFreeAndReserves(t *testing.T) {
	inv := h100Inventory()["H100"]
	i, ok := inv.pickWorker(2, nil)
	require.True(t, ok)
	// w1 and w3 both have 8 free; w1 has fewer held -> tie broken by held then order
	assert.Equal(t, "w1", inv.Workers[i].WorkerID)
	inv.reserve(i, 2)
	assert.Equal(t, uint32(6), inv.Workers[i].Free)
	assert.Equal(t, uint32(2), inv.Workers[i].Held)

	j, ok := inv.pickWorker(1, func(w workerSlot) bool { return w.Locality == "eu-west" })
	require.True(t, ok)
	assert.Equal(t, "w3", inv.Workers[j].WorkerID)

	_, ok = inv.pickWorker(16, nil)
	assert.False(t, ok)
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
