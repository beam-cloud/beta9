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

	// Nothing fits any more: nothing is submitted. Replicas only fill idle
	// GPUs; the scheduler is never asked to wait for or provision one.
	pool, ok = inv.place("H100", 2)
	assert.False(t, ok)
	assert.Empty(t, pool.Name)
	assert.Equal(t, uint32(1), inv.free["H100"]["gpu-b"], "a refused placement reserves nothing")
	pool, ok = inv.place("H100", 1)
	require.True(t, ok, "a smaller replica still fits the last GPU")
	assert.Equal(t, "gpu-b", pool.Name)

	// CPU targets go to the first eligible CPU pool.
	pool, ok = inv.place(types.CPUInventoryKey, 0)
	require.True(t, ok)
	assert.Equal(t, "cpu-a", pool.Name)

	// A GPU type no pool opted in for places nothing.
	pool, ok = inv.place("A100-80G", 1)
	assert.False(t, ok)
	assert.Empty(t, pool.Name)
}

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

func TestRetireWaitsForCurrentReady(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	ctx := context.Background()
	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)

	oldReady := versionReplica(t, s, "v1-ready", 1, types.ReplicaStatusReady)
	oldLoading := versionReplica(t, s, "v1-loading", 1, types.ReplicaStatusLoading)
	newLoading := versionReplica(t, s, "v2-loading", 2, types.ReplicaStatusLoading)
	live := []*types.EndpointReplica{oldReady, oldLoading, newLoading}

	// Nothing of v2 is ready: the serving v1 replica keeps the traffic, but
	// a v1 replica that is not serving anyway is retired right away.
	s.controller.retire(ctx, endpoint, fleet, live, nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, oldLoading.ID), "a loading replica is stopped without a drain window")
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, newLoading.ID))

	// Only one stale replica is retired per tick, so a second pass with the
	// same picture drains nothing more (the ready one is still needed).
	s.controller.retire(ctx, endpoint, fleet, live, nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, oldReady.ID))

	// Once v2 has a ready replica the old one is drained with the spec's grace.
	newLoading.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(ctx, newLoading))
	s.controller.retire(ctx, endpoint, fleet, live, nil)
	stale, err := s.repo.GetReplica(ctx, oldReady.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusDraining, stale.Status)
	assert.Contains(t, stale.StatusReason, "version 1 retired")
	assert.WithinDuration(t, time.Now().Add(time.Duration(endpoint.Spec.DrainSeconds)*time.Second), stale.DrainDeadline, 2*time.Second)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, newLoading.ID), "current-version replicas are untouched")
}

// Moving an endpoint to another GPU type is a replacement like any other: the
// serving replica on the old type stays until the new type has one ready.
func TestRetireKeepsServingReplicaAcrossGPUMove(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	onH100 := versionReplica(t, s, "h100", 1, types.ReplicaStatusReady)
	onA100 := versionReplica(t, s, "a100", 1, types.ReplicaStatusLoading)
	onA100.GPU = "A100-80"
	require.NoError(t, s.repo.SaveReplica(ctx, onA100))
	live := []*types.EndpointReplica{onH100, onA100}
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{"acme/model": {Enabled: true, GPUs: map[string]types.FleetPlacement{"A100-80": {Priority: 1}}}})

	s.controller.retire(ctx, endpoint, fleet, live, nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, onH100.ID), "no A100 replica is ready yet")

	onA100.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(ctx, onA100))
	s.controller.retire(ctx, endpoint, fleet, live, nil)
	drained, err := s.repo.GetReplica(ctx, onH100.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusDraining, drained.Status)
	assert.Equal(t, "removed from fleet.yaml", drained.StatusReason)

	// An endpoint listed nowhere is drained outright.
	gone := versionReplica(t, s, "gone", 1, types.ReplicaStatusReady)
	s.controller.retire(ctx, endpoint, &types.Fleet{}, []*types.EndpointReplica{gone}, nil)
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, gone.ID))

	// A retired endpoint drains everything it still has.
	endpoint.Status = types.EndpointStatusRetired
	survivor := versionReplica(t, s, "survivor", 1, types.ReplicaStatusLoading)
	s.controller.retire(ctx, endpoint, fleet, []*types.EndpointReplica{survivor}, nil)
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, survivor.ID))
}

// noRoomInventory has an eligible H100 pool with nothing idle, so growth
// stops for lack of GPUs rather than for lack of a pool.
func noRoomInventory() *clusterInventory {
	return &clusterInventory{
		pools: map[string][]eligiblePool{"H100": {{Name: "gpu-a", Locality: "gpu-a"}}},
		free:  map[string]map[string]uint32{"H100": {"gpu-a": 0}},
	}
}

func TestFillDrainsDownToCap(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s) // fleet: H100: [acme/model: 2]
	ctx := context.Background()
	endpoints := map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}
	entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, MaxReplicas: 2}}

	first := versionReplica(t, s, "first", 1, types.ReplicaStatusLoading)
	second := versionReplica(t, s, "second", 1, types.ReplicaStatusReady)
	extra := versionReplica(t, s, "extra", 1, types.ReplicaStatusLoading)
	s.controller.fill(ctx, "H100", entries, endpoints, []*types.EndpointReplica{first, second, extra}, noRoomInventory())
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, second.ID))
	stopped := 0
	for _, id := range []string{first.ID, extra.ID} {
		if statusOf(t, s, id) == types.ReplicaStatusStopped {
			stopped++
		}
	}
	assert.Equal(t, 1, stopped, "one loading replica goes, the ready one stays")
}

// Priority order is enforced by giving GPUs back: when a higher entry is short
// and nothing is idle, the entries below it drain one replica per tick.
func TestFillReclaimsForHigherPriority(t *testing.T) {
	s := newServiceForTest(t)
	high := seedEndpoint(t, s)
	low := &types.ManagedEndpoint{Spec: high.Spec, StubID: "stub-low", Version: 1, Status: types.EndpointStatusActive}
	low.Spec.ID = "acme/low"
	ctx := context.Background()
	require.NoError(t, s.repo.SaveEndpoint(ctx, low))
	endpoints := map[string]*types.ManagedEndpoint{high.Spec.ID: high, low.Spec.ID: low}
	entries := []types.FleetEntry{{EndpointID: high.Spec.ID}, {EndpointID: low.Spec.ID}}

	lowReplica := func(id string, status types.ReplicaStatus) *types.EndpointReplica {
		r := versionReplica(t, s, id, 1, status)
		r.EndpointID = low.Spec.ID
		require.NoError(t, s.repo.SaveReplica(ctx, r))
		return r
	}
	highReady := versionReplica(t, s, "high-ready", 1, types.ReplicaStatusReady)
	lowBusy := lowReplica("low-busy", types.ReplicaStatusReady)
	lowBusy.Capacity.InFlight = 3
	require.NoError(t, s.repo.SaveReplica(ctx, lowBusy))
	lowIdle := lowReplica("low-idle", types.ReplicaStatusReady)
	live := []*types.EndpointReplica{highReady, lowBusy, lowIdle}

	// The uncapped high entry wants more and nothing is idle: the least
	// valuable low replica is drained, and only one per tick.
	s.controller.fill(ctx, "H100", entries, endpoints, live, noRoomInventory())
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, lowIdle.ID))
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, lowBusy.ID))
	drained, _ := s.repo.GetReplica(ctx, lowIdle.ID)
	assert.Equal(t, "gpu reclaimed for acme/model", drained.StatusReason)

	// While the high entry is still bringing a replica up, nothing more is
	// taken from the low one: a replica that cannot start must not drain
	// the others.
	highLoading := versionReplica(t, s, "high-loading", 1, types.ReplicaStatusLoading)
	s.controller.fill(ctx, "H100", entries, endpoints, []*types.EndpointReplica{highReady, highLoading, lowBusy}, noRoomInventory())
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, lowBusy.ID))

	// A capped high entry that is at its cap wants nothing: the low one keeps its GPUs.
	capped := []types.FleetEntry{{EndpointID: high.Spec.ID, MaxReplicas: 1}, {EndpointID: low.Spec.ID}}
	s.controller.fill(ctx, "H100", capped, endpoints, []*types.EndpointReplica{highReady, lowBusy}, noRoomInventory())
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, lowBusy.ID))
}

func TestInventoryCountsSchedulingReplicasAsTaken(t *testing.T) {
	s := newServiceForTest(t)
	s.workers = repository.NewWorkerRedisRepositoryForTest(s.rdb)
	s.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{"gpu-a": {GPUType: "H100", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}}}
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "w1", PoolName: "gpu-a", Gpu: "H100", TotalGpuCount: 4, FreeGpuCount: 4, Status: types.WorkerStatusAvailable}))

	inv, err := s.controller.inventory([]*types.EndpointReplica{
		{ID: "pending", GPU: "H100", GPUCount: 2, PoolName: "gpu-a", Status: types.ReplicaStatusScheduling},
		{ID: "placed", GPU: "H100", GPUCount: 1, PoolName: "gpu-a", Status: types.ReplicaStatusLoading},
		{ID: "elsewhere", GPU: "H100", GPUCount: 1, PoolName: "other", Status: types.ReplicaStatusScheduling},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), inv.free["H100"]["gpu-a"], "a replica the scheduler has not placed yet still holds its GPUs; a placed one is already in the worker's count")
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
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, replica.GPU)
	require.NoError(t, err)
	assert.False(t, backoff, "eviction is not a failure; the fill loop may retry immediately")
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

func TestReplaceRolloutMakesRoomOnSingleGPU(t *testing.T) {
	for _, protected := range []bool{false, true} {
		s := newServiceForTest(t)
		endpoint := seedEndpoint(t, s)
		endpoint.Version = 2
		endpoint.Spec.Rollout = "replace"
		endpoint.Spec.Protected = protected
		endpoint.Spec.DrainSeconds = 0
		old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
		fleet, err := s.repo.GetFleet(context.Background())
		require.NoError(t, err)
		s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{old}, noRoomInventory())
		assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, old.ID))
	}
}

func TestReplaceRolloutPreservesServiceWhileReplacementLoads(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	endpoint.Spec.Rollout = "replace"
	old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
	starting := versionReplica(t, s, "new", 2, types.ReplicaStatusLoading)
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{old, starting}, noRoomInventory())
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID))
}
