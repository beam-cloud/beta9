package managedendpoint

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func h100Inventory() map[string]gpuInventory {
	return map[string]gpuInventory{
		"H100": {GPU: "H100", Workers: []workerSlot{
			{WorkerID: "w1", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 8},
			{WorkerID: "w2", PoolName: "gpu-a", Locality: "us-east", Total: 8, Free: 4, Held: 2},
			{WorkerID: "w3", PoolName: "gpu-b", Locality: "eu-west", Total: 8, Free: 8, MaxShare: 0.25},
		}},
	}
}

func TestInventoryCapacityAndAllowance(t *testing.T) {
	inv := h100Inventory()["H100"]
	// free + held, serverless-held GPUs (w2: 8-4-2=2) excluded
	assert.Equal(t, uint32(22), inv.capacity())
	assert.Equal(t, uint32(20), inv.free())
	// 0.5 cluster share: w1 4 + w2 3 + w3 min(0.5,0.25)*8=2 -> 9
	assert.Equal(t, uint32(9), inv.allowance(0.5))
}

func TestPlanFillDividesByShareAndClamps(t *testing.T) {
	inv := h100Inventory()
	targets := []fillTarget{
		{EndpointID: "a", Role: "serve", GPU: "H100x1", Type: "H100", Count: 1, Share: 0.5, Min: 0, Max: 0},
		{EndpointID: "b", Role: "serve", GPU: "H100x2", Type: "H100", Count: 2, Share: 0.5, Min: 1, Max: 1},
		{EndpointID: "c", Role: "serve", GPU: "A100x1", Type: "A100", Count: 1, Share: 1, Min: 1},
	}
	plans := planFill(inv, targets, 0.5)

	// allowance 9: a gets floor(9*0.5/1)=4, b gets floor(9*0.5/2)=2 clamped to max 1
	assert.Equal(t, fillPlan{Quota: 4, Desired: 4}, plans[targets[0].key()])
	assert.Equal(t, fillPlan{Quota: 2, Desired: 1}, plans[targets[1].key()])
	// no A100 inventory: quota 0, min_replicas still requested
	assert.Equal(t, fillPlan{Quota: 0, Desired: 1}, plans[targets[2].key()])
}

func TestPlanFillNormalizesOversubscribedShares(t *testing.T) {
	inv := h100Inventory()
	targets := []fillTarget{
		{EndpointID: "a", Role: "serve", GPU: "H100x1", Type: "H100", Count: 1, Share: 1},
		{EndpointID: "b", Role: "serve", GPU: "H100x1", Type: "H100", Count: 1, Share: 1},
	}
	plans := planFill(inv, targets, 0.5)
	total := plans[targets[0].key()].Quota + plans[targets[1].key()].Quota
	assert.LessOrEqual(t, total, uint32(9))
	assert.Equal(t, plans[targets[0].key()].Quota, plans[targets[1].key()].Quota)
}

func TestPlanFillDemandGrowsPastQuotaWithinMax(t *testing.T) {
	inv := h100Inventory()
	targets := []fillTarget{
		{EndpointID: "a", Role: "serve", GPU: "H100x1", Type: "H100", Count: 1, Share: 0.1, Max: 3, Demand: 5},
	}
	plans := planFill(inv, targets, 0.5)
	assert.Equal(t, uint32(0), plans[targets[0].key()].Quota)
	assert.Equal(t, uint32(3), plans[targets[0].key()].Desired)
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

	onlyEU := func(w workerSlot) bool { return w.Locality == "eu-west" }
	j, ok := inv.pickWorker(1, onlyEU)
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
		{ID: "tuning", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 1, Status: types.ReplicaStatusReady, Tuning: true},
		{ID: "other-version", EndpointID: "e", Role: "serve", GPU: "H100x1", Version: 2, Status: types.ReplicaStatusReady},
	}
	set := partitionReplicas(replicas, "e", "serve", "H100x1", 1)
	assert.Len(t, set.Live, 4)
	assert.Len(t, set.Ready, 3)
	assert.Equal(t, uint32(1), set.Protected)

	order := scaleDownCandidates(set)
	ids := make([]string, 0, len(order))
	for _, r := range order {
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

	assert.Equal(t, "git@v12", gitAuthor(12))
	assert.Equal(t, "live@v3:agent@codex", liveAuthor(3, "agent@codex"))
}
