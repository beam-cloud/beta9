package managedendpoint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInventoryPlacesInEligiblePoolsOnly(t *testing.T) {
	s := newServiceForTest(t)
	s.workers = repository.NewWorkerRedisRepositoryForTest(s.rdb)
	s.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{
		"gpu-a":   {GPUType: "H100", Locality: "us-east", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"gpu-b":   {GPUType: "H100", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"cpu-a":   {ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}},
		"opt-out": {GPUType: "H100"},
	}
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "w1", PoolName: "gpu-b", Gpu: "H100", TotalGpuCount: 3, FreeGpuCount: 3, Status: types.WorkerStatusAvailable}))
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "w2", PoolName: "opt-out", Gpu: "H100", TotalGpuCount: 8, FreeGpuCount: 8, Status: types.WorkerStatusAvailable}))

	inv, err := s.controller.inventory(nil)
	require.NoError(t, err)
	assert.Equal(t, []eligiblePool{{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "gpu-b"}}, inv.pools["H100"])
	assert.Equal(t, []eligiblePool{{Name: "cpu-a", Locality: "cpu-a"}}, inv.pools[types.CPUInventoryKey])
	assert.Equal(t, map[string]uint32{"gpu-b": 3}, inv.free["H100"], "workers in pools that did not opt in are invisible")

	// Free capacity wins; the reservation is tracked so the next replica
	// does not count on the same GPUs.
	pool, ok := inv.place("H100", 2)
	require.True(t, ok)
	assert.Equal(t, "gpu-b", pool.Name)
	assert.Equal(t, uint32(1), inv.free["H100"]["gpu-b"])

	// Nothing fits any more: the first eligible pool takes the request and
	// the scheduler provisions (or waits).
	pool, ok = inv.place("H100", 2)
	require.True(t, ok)
	assert.Equal(t, eligiblePool{Name: "gpu-a", Locality: "us-east"}, pool)

	// CPU targets go to the first eligible CPU pool.
	pool, ok = inv.place(types.CPUInventoryKey, 0)
	require.True(t, ok)
	assert.Equal(t, "cpu-a", pool.Name)

	// A GPU type no pool opted in for places nothing.
	pool, ok = inv.place("A100-80G", 1)
	assert.False(t, ok)
	assert.Empty(t, pool.Name)
}

func TestLiveReplicasAndScaleDownOrder(t *testing.T) {
	now := time.Now()
	replicas := []*types.EndpointReplica{
		{ID: "old-ready", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Hour)},
		{ID: "loading", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusLoading, StartedAt: now},
		{ID: "busy", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Minute), Capacity: types.ReplicaCapacity{InFlight: 4}},
		{ID: "new-ready", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, StartedAt: now.Add(-time.Second)},
		{ID: "draining", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusDraining},
		{ID: "evicting", EndpointID: "e", GPU: "H100", Version: 1, Status: types.ReplicaStatusEvicting},
		{ID: "other-gpu", EndpointID: "e", GPU: "A100", Version: 1, Status: types.ReplicaStatusReady},
		{ID: "other-endpoint", EndpointID: "f", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady},
		{ID: "other-version", EndpointID: "e", GPU: "H100", Version: 2, Status: types.ReplicaStatusReady},
	}
	live, ready := liveReplicas(replicas, "e", "H100", 1)
	assert.Len(t, live, 4)
	assert.Equal(t, 3, ready)

	scaleDownOrder(live)
	ids := make([]string, 0, len(live))
	for _, r := range live {
		ids = append(ids, r.ID)
	}
	// not-ready before ready; least loaded; newest first
	assert.Equal(t, []string{"loading", "new-ready", "old-ready", "busy"}, ids)
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

func TestRetireStaleWaitsForCurrentReady(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	ctx := context.Background()
	placed := map[string]bool{"H100": true}

	oldReady := versionReplica(t, s, "v1-ready", 1, types.ReplicaStatusReady)
	oldLoading := versionReplica(t, s, "v1-loading", 1, types.ReplicaStatusLoading)
	newLoading := versionReplica(t, s, "v2-loading", 2, types.ReplicaStatusLoading)
	live := []*types.EndpointReplica{oldReady, oldLoading, newLoading}

	// Nothing of v2 is ready: the serving v1 replica keeps the traffic, but
	// a v1 replica that is not serving anyway is retired right away.
	s.controller.retireStale(ctx, endpoint, placed, 1, live)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, oldLoading.ID), "a loading replica is stopped without a drain window")
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, newLoading.ID))

	// Only one stale replica is retired per tick, so a second pass with the
	// same picture drains nothing more (the ready one is still needed).
	s.controller.retireStale(ctx, endpoint, placed, 1, live)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))

	// Once v2 has a ready replica the old one is drained with the spec's grace.
	newLoading.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(ctx, newLoading))
	s.controller.retireStale(ctx, endpoint, placed, 1, live)
	stale, err := s.repo.GetReplica(ctx, oldReady.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusDraining, stale.Status)
	assert.Contains(t, stale.StatusReason, "version 1 retired")
	assert.WithinDuration(t, time.Now().Add(time.Duration(endpoint.Spec.DrainSeconds)*time.Second), stale.DrainDeadline, 2*time.Second)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, newLoading.ID), "current-version replicas are untouched")
}

// Moving an endpoint to another GPU type is a replacement like any other: the
// serving replica on the old type stays until the new type has one ready.
func TestRetireStaleKeepsServingReplicaAcrossGPUMove(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	onH100 := versionReplica(t, s, "h100", 1, types.ReplicaStatusReady)
	onA100 := versionReplica(t, s, "a100", 1, types.ReplicaStatusLoading)
	onA100.GPU = "A100-80"
	require.NoError(t, s.repo.SaveReplica(ctx, onA100))
	live := []*types.EndpointReplica{onH100, onA100}
	placed := map[string]bool{"A100-80": true}

	s.controller.retireStale(ctx, endpoint, placed, 1, live)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, onH100.ID), "no A100 replica is ready yet")

	onA100.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(ctx, onA100))
	s.controller.retireStale(ctx, endpoint, placed, 1, live)
	drained, err := s.repo.GetReplica(ctx, onH100.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusDraining, drained.Status)
	assert.Equal(t, "removed from fleet.yaml", drained.StatusReason)

	// An endpoint dropped from fleet.yaml entirely is drained outright.
	gone := versionReplica(t, s, "gone", 1, types.ReplicaStatusReady)
	s.controller.retireStale(ctx, endpoint, map[string]bool{}, 0, []*types.EndpointReplica{gone})
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, gone.ID))
}

func TestReconcileEndpointConvergesOnFleetCount(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s) // fleet: acme/model H100: 2
	ctx := context.Background()
	inv := &clusterInventory{pools: map[string][]eligiblePool{}, free: map[string]map[string]uint32{}}

	placed := versionReplica(t, s, "on-h100", 1, types.ReplicaStatusLoading)
	unplaced := versionReplica(t, s, "on-a100", 1, types.ReplicaStatusLoading)
	unplaced.GPU = "A100"
	require.NoError(t, s.repo.SaveReplica(ctx, unplaced))

	// fleet.yaml only places acme/model on H100: the A100 replica goes. Below
	// the count with no pool opted in, nothing can be started.
	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	s.controller.reconcileEndpoint(ctx, endpoint, fleet, []*types.EndpointReplica{placed, unplaced}, inv)
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, placed.ID))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, unplaced.ID))
	reason, _ := s.repo.GetReplica(ctx, unplaced.ID)
	assert.Equal(t, "removed from fleet.yaml", reason.StatusReason)

	// Over the count: the least valuable replicas are drained down to it.
	second := versionReplica(t, s, "second", 1, types.ReplicaStatusReady)
	extra := versionReplica(t, s, "extra", 1, types.ReplicaStatusLoading)
	s.controller.reconcileEndpoint(ctx, endpoint, fleet, []*types.EndpointReplica{placed, second, extra}, inv)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, second.ID))
	stopped := 0
	for _, id := range []string{placed.ID, extra.ID} {
		if statusOf(t, s, id) == types.ReplicaStatusStopped {
			stopped++
		}
	}
	assert.Equal(t, 1, stopped, "one loading replica goes, the ready one stays")

	// A retired endpoint drains everything it still has.
	endpoint.Status = types.EndpointStatusRetired
	survivor := versionReplica(t, s, "survivor", 1, types.ReplicaStatusLoading)
	s.controller.reconcileEndpoint(ctx, endpoint, fleet, []*types.EndpointReplica{survivor}, inv)
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

// An old version keeps serving during replacement, so it is probed with the
// contract it was started with, not the endpoint's latest spec.
func TestReplicaKeepsItsOwnProbeContract(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	assert.Equal(t, types.ReplicaProbe{Port: 8000, Health: "/health", Metrics: "/metrics"}, replica.Probe)

	endpoint.Version, endpoint.Spec.Port, endpoint.Spec.Health = 2, 9000, "/ready-v2"
	require.NoError(t, s.repo.SaveEndpoint(ctx, endpoint))
	stored, err := s.repo.GetReplica(ctx, replica.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 8000, stored.Probe.Port)
	assert.Equal(t, "/health", stored.Probe.Health)

	embedding := &types.ManagedEndpointSpec{Kind: types.EndpointKindEmbedding, Port: 7000, Health: "/healthz"}
	assert.Equal(t, types.ReplicaProbe{Port: 7000, Health: "/healthz"}, probeFor(embedding), "only LLM engines are scraped for capacity")
}

// A ready engine that goes back to loading (reload, failing health check)
// gets a fresh loading grace instead of inheriting the container's age.
func TestReloadHasFreshLoadingGrace(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	r := seedReplica(t, s, endpoint)
	r.StartedAt = time.Now().Add(-time.Hour)
	r.Status = types.ReplicaStatusReady
	r.Address = "existing-address"
	require.NoError(t, s.containers.SetContainerState(r.ContainerID, &types.ContainerState{ContainerId: r.ContainerID, Status: types.ContainerStatusRunning}))

	s.applyHeartbeat(r, &pb.HarnessHeartbeatRequest{Status: "loading"})
	require.NoError(t, s.controller.syncReplica(context.Background(), r))
	assert.Equal(t, types.ReplicaStatusLoading, r.Status)
	assert.WithinDuration(t, time.Now(), r.LoadingSince, time.Minute)

	// The phase clock is not reset by repeated loading heartbeats...
	since := r.LoadingSince
	s.applyHeartbeat(r, &pb.HarnessHeartbeatRequest{Status: "loading"})
	assert.Equal(t, since, r.LoadingSince)
	// ...and does expire once the phase itself outlives the grace.
	r.LoadingSince = time.Now().Add(-loadingGrace - time.Minute)
	require.NoError(t, s.controller.syncReplica(context.Background(), r))
	assert.Equal(t, types.ReplicaStatusFailed, r.Status)

	// Initial startup still measures from container start.
	fresh := seedReplica(t, s, endpoint)
	fresh.StartedAt = time.Now().Add(-loadingGrace - time.Minute)
	require.NoError(t, s.containers.SetContainerState(fresh.ContainerID, &types.ContainerState{ContainerId: fresh.ContainerID, Status: types.ContainerStatusRunning}))
	require.NoError(t, s.controller.syncReplica(context.Background(), fresh))
	assert.Equal(t, types.ReplicaStatusLoading, fresh.Status, "scheduling -> loading starts the phase clock now")
	fresh.LoadingSince = time.Now().Add(-loadingGrace - time.Minute)
	require.NoError(t, s.controller.syncReplica(context.Background(), fresh))
	assert.Equal(t, types.ReplicaStatusFailed, fresh.Status)
}

func TestSyncReplicaBacksOffWhenSchedulerFailsRequest(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.StartedAt = time.Now().Add(-time.Minute)
	ctx := context.Background()

	// The scheduler failed the request: no state, failed request status.
	require.NoError(t, s.containers.SetContainerRequestStatus(replica.ContainerID, types.ContainerRequestStatusFailed))
	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusFailed, replica.Status)
	assert.Equal(t, "scheduler failed the request", replica.StatusReason)
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, replica.GPU)
	require.NoError(t, err)
	assert.True(t, backoff)
}
