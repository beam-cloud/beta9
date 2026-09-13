package managedendpoint

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
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
	require.NoError(t, s.workers.AddWorker(&types.Worker{Id: "spot", PoolName: "gpu-b", Gpu: "H100", TotalGpuCount: 8, FreeGpuCount: 8, Status: types.WorkerStatusAvailable, Preemptable: true}))

	inv, err := s.controller.inventory(nil)
	require.NoError(t, err)
	assert.Equal(t, []eligiblePool{{Name: "gpu-a", Locality: "us-east"}, {Name: "gpu-b", Locality: "gpu-b"}}, inv.pools["H100"])
	assert.Equal(t, []eligiblePool{{Name: "cpu-a", Locality: "cpu-a"}}, inv.pools[types.CPUInventoryKey])
	assert.Equal(t, uint32(3), inv.idle("H100", "gpu-b"), "opt-out pools and workers rejected by hosted admission are invisible")
	assert.Zero(t, inv.idle("H100", "gpu-a"))

	// Free capacity wins; the reservation is tracked so the next replica
	// does not count on the same GPUs.
	pool, ok := inv.place("H100", 2)
	require.True(t, ok)
	assert.Equal(t, "gpu-b", pool.Name)
	assert.Equal(t, uint32(1), inv.idle("H100", "gpu-b"))

	// Nothing fits any more: nothing is submitted. Replicas only fill idle
	// GPUs; the scheduler is never asked to wait for or provision one.
	pool, ok = inv.place("H100", 2)
	assert.False(t, ok)
	assert.Empty(t, pool.Name)
	assert.Equal(t, uint32(1), inv.idle("H100", "gpu-b"), "a refused placement reserves nothing")
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
	assert.Equal(t, "removed from config.yaml", drained.StatusReason)

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
		pools:   map[string][]eligiblePool{"H100": {{Name: "gpu-a", Locality: "gpu-a"}}},
		workers: map[string]*inventoryWorker{"w1": {pool: "gpu-a", gpu: "H100", total: 8}},
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
	s.controller.fill(ctx, "H100", entries, endpoints, []*types.EndpointReplica{first, second, extra}, noRoomInventory(), nil)
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
		r := reclaimableReplica(t, s, low, id, "w1")
		r.Status = status
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
	s.controller.fill(ctx, "H100", entries, endpoints, live, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, lowIdle.ID))
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, lowBusy.ID))
	drained, _ := s.repo.GetReplica(ctx, lowIdle.ID)
	assert.Equal(t, "gpu reclaimed for acme/model", drained.StatusReason)

	// While the high entry is still bringing a replica up, nothing more is
	// taken from the low one: a replica that cannot start must not drain
	// the others.
	highLoading := versionReplica(t, s, "high-loading", 1, types.ReplicaStatusLoading)
	s.controller.fill(ctx, "H100", entries, endpoints, []*types.EndpointReplica{highReady, highLoading, lowBusy}, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, lowBusy.ID))

	// A capped high entry that is at its cap wants nothing: the low one keeps its GPUs.
	capped := []types.FleetEntry{{EndpointID: high.Spec.ID, MaxReplicas: 1}, {EndpointID: low.Spec.ID}}
	s.controller.fill(ctx, "H100", capped, endpoints, []*types.EndpointReplica{highReady, lowBusy}, noRoomInventory(), nil)
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
	assert.Equal(t, uint32(2), inv.idle("H100", "gpu-a"), "a replica the scheduler has not placed yet still holds its GPUs; a placed one is already in the worker's count")
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
	s.controller.startedAt = time.Now().Add(-containerLostGrace - time.Second)
	require.NoError(t, s.controller.syncReplica(context.Background(), replica))
	assert.True(t, replica.Status.Terminal())
}

func TestSyncReplicaGatewayReconnectGraceDoesNotEraseStopIntent(t *testing.T) {
	for _, test := range []struct {
		name   string
		status types.ReplicaStatus
		exit   int
		wait   bool
	}{
		{"reconnecting live replica", types.ReplicaStatusReady, -1, true},
		{"known process exit", types.ReplicaStatusReady, 137, false},
		{"explicit drain", types.ReplicaStatusDraining, -1, false},
		{"scheduler eviction", types.ReplicaStatusEvicting, -1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newServiceForTest(t)
			s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
			r := seedReplica(t, s, seedEndpoint(t, s))
			r.Status, r.StartedAt = test.status, time.Now().Add(-time.Hour)
			if test.exit >= 0 {
				require.NoError(t, s.containers.SetContainerExitCode(r.ContainerID, test.exit))
			}
			require.NoError(t, s.controller.syncReplica(context.Background(), r))
			assert.Equal(t, !test.wait, r.Status.Terminal())
			if test.wait {
				s.controller.startedAt = time.Now().Add(-containerLostGrace - time.Second)
				require.NoError(t, s.controller.syncReplica(context.Background(), r))
				assert.True(t, r.Status.Terminal(), "missing ownership is not preserved forever")
			}
			_, err := s.containers.GetContainerState(r.ContainerID)
			assert.True(t, containerStateNotFound(err), "controller never reconstructs expired ownership")
		})
	}
}

func TestSyncReplicaGatewayReconnectStillDetectsDeadHarness(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	r := seedReplica(t, s, seedEndpoint(t, s))
	r.Status, r.StartedAt, r.LastHeartbeat = types.ReplicaStatusReady, time.Now().Add(-time.Hour), time.Now().Add(-5*time.Minute)
	r.HarnessEnabled = true
	require.NoError(t, s.containers.SetContainerState(r.ContainerID, &types.ContainerState{ContainerId: r.ContainerID, Status: types.ContainerStatusRunning}))
	require.NoError(t, s.controller.syncReplica(context.Background(), r))
	assert.Equal(t, types.ReplicaStatusReady, r.Status, "gateway outage is not evidence that the engine died")
	s.controller.startedAt = time.Now().Add(-s.config.ReplicaStaleAfter - time.Second)
	require.NoError(t, s.controller.syncReplica(context.Background(), r))
	assert.Equal(t, types.ReplicaStatusFailed, r.Status)
	assert.Equal(t, "harness heartbeat stale", r.StatusReason, "long ownership lease does not delay stale harness detection")
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
		endpoint.Spec.Rollout = types.RolloutReplace
		endpoint.Spec.DrainSeconds = 0
		old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
		old.Protected = protected
		require.NoError(t, s.repo.SaveReplica(context.Background(), old))
		fleet, err := s.repo.GetFleet(context.Background())
		require.NoError(t, err)
		s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{old}, nil)
		assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, old.ID))
	}
}

func TestReplaceRolloutPreservesServiceWhileReplacementLoads(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	endpoint.Spec.Rollout = types.RolloutReplace
	old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
	starting := versionReplica(t, s, "new", 2, types.ReplicaStatusLoading)
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{old, starting}, nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID))
}

// Use the real Redis-backed scheduler admission/backlog without a consumer:
// tests inspect exactly what the controller submits without provisioning.
type fillBackend struct {
	repository.BackendRepository
	config string
	err    error
}

func (b fillBackend) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	if b.err != nil {
		return nil, b.err
	}
	config := b.config
	if config == "" {
		config = `{}`
	}
	return &types.StubWithRelated{
		Stub: types.Stub{ExternalId: id, Type: types.StubType(types.StubTypeManagedEndpointDeployment), Config: config},
		App:  &types.App{},
	}, nil
}

func newFillService(t *testing.T) *Service {
	t.Helper()
	s := newServiceForTest(t)
	s.config.Preemption.Enabled = true
	s.backend = fillBackend{}
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	key := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	s.adminWorkspace.SigningKey = &key
	var err error
	s.scheduler, err = scheduler.NewScheduler(s.ctx, types.AppConfig{}, s.rdb, nil, s.backend, nil, nil)
	require.NoError(t, err)
	return s
}

func fillEndpoints(t *testing.T, s *Service) (*types.ManagedEndpoint, *types.ManagedEndpoint, map[string]*types.ManagedEndpoint) {
	t.Helper()
	high := seedEndpoint(t, s)
	low := *high
	low.Spec.ID = "acme/low"
	low.StubID = "stub-low"
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), &low))
	return high, &low, map[string]*types.ManagedEndpoint{high.Spec.ID: high, low.Spec.ID: &low}
}

func idleInventory(free uint32) *clusterInventory {
	inv := noRoomInventory()
	inv.workers["w1"].free = free
	inv.workers["w1"].total = max(free, inv.workers["w1"].total)
	return inv
}

func reclaimableReplica(t *testing.T, s *Service, endpoint *types.ManagedEndpoint, id, worker string) *types.EndpointReplica {
	t.Helper()
	if s.backend == nil {
		s.backend = fillBackend{}
	}
	if s.containers == nil {
		s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	}
	r := versionReplica(t, s, id, endpoint.Version, types.ReplicaStatusReady)
	r.EndpointID, r.WorkerID, r.PoolName = endpoint.Spec.ID, worker, "gpu-a"
	require.NoError(t, s.repo.SaveReplica(context.Background(), r))
	require.NoError(t, s.containers.SetContainerState(r.ContainerID, &types.ContainerState{
		ContainerId: r.ContainerID, WorkerId: worker, Gpu: r.GPU, GpuCount: r.GPUCount,
		Status: types.ContainerStatusRunning, Evictable: true,
	}))
	return r
}

func TestFillMinimumsPrecedePrioritySurplus(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	entries := []types.FleetEntry{
		{EndpointID: high.Spec.ID, MaxReplicas: 3},
		{EndpointID: low.Spec.ID, MinReplicas: 1, MaxReplicas: 1, ProtectMinimum: true},
	}
	s.controller.fill(context.Background(), "H100", entries, endpoints, nil, idleInventory(3), nil)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 3)
	assert.Equal(t, []string{low.StubID, high.StubID, high.StubID}, []string{requests[0].StubId, requests[1].StubId, requests[2].StubId})
	assert.False(t, requests[0].Evictable, "preemption:false protects a minimum replica")
	assert.True(t, requests[1].Evictable)
	assert.True(t, requests[2].Evictable)
	for _, request := range requests {
		assert.True(t, request.OpportunisticOnly, "even a protected minimum must never evict or provision serverless")
		assert.Equal(t, "gpu-a", request.PoolSelector)
	}
}

func TestFillCountsStartsAcrossMinimumAndSurplusPasses(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		minimum, maximum, expected uint32
	}{
		{"same tick cap", 2, 3, 3},
		{"per tick budget", 9, 10, maxStartsPerTick},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newFillService(t)
			endpoint := seedEndpoint(t, s)
			entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, MinReplicas: tc.minimum, MaxReplicas: tc.maximum, ProtectMinimum: true}}
			s.controller.fill(context.Background(), "H100", entries, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, nil, idleInventory(20), nil)
			replicas, err := s.repo.ListAllReplicas(context.Background())
			require.NoError(t, err)
			require.Len(t, replicas, int(tc.expected))
			var protected uint32
			for _, replica := range replicas {
				assert.Equal(t, types.ReplicaStatusScheduling, replica.Status)
				if replica.Protected {
					protected++
				}
			}
			assert.Equal(t, min(tc.minimum, tc.expected), protected, "only the minimum starts are protected")
		})
	}
}

func TestFillCountsSchedulingAndLoadingTowardLimits(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	live := []*types.EndpointReplica{
		versionReplica(t, s, "ready", 1, types.ReplicaStatusReady),
		versionReplica(t, s, "loading", 1, types.ReplicaStatusLoading),
		versionReplica(t, s, "scheduling", 1, types.ReplicaStatusScheduling),
	}
	entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, MinReplicas: 3, MaxReplicas: 3}}
	s.controller.fill(context.Background(), "H100", entries, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, live, idleInventory(4), nil)
	all, err := s.repo.ListAllReplicas(context.Background())
	require.NoError(t, err)
	assert.Len(t, all, 3, "loading and queued minimum replicas must not be duplicated")
}

func TestFillBackoffDoesNotBlockOtherMinimumOrAllowSurplus(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	require.NoError(t, s.repo.SetScheduleBackoff(context.Background(), high.Spec.ID, "H100", time.Minute))
	entries := []types.FleetEntry{
		{EndpointID: high.Spec.ID, MinReplicas: 1, MaxReplicas: 3},
		{EndpointID: low.Spec.ID, MinReplicas: 1, MaxReplicas: 3},
	}
	inv := idleInventory(3)
	s.controller.fill(context.Background(), "H100", entries, endpoints, nil, inv, nil)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	assert.Equal(t, low.StubID, requests[0].StubId)
	assert.Equal(t, uint32(2), inv.idle("H100", "gpu-a"), "no surplus takes the unmet minimum's capacity")
}

func TestFillMinimumReclaimsHigherPrioritySurplusAndRestores(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	protected := reclaimableReplica(t, s, high, "protected", "w1")
	protected.Protected = true
	require.NoError(t, s.repo.SaveReplica(context.Background(), protected))
	extra := reclaimableReplica(t, s, high, "extra", "w1")
	extra.StartedAt = time.Now().Add(time.Minute)
	require.NoError(t, s.repo.SaveReplica(context.Background(), extra))
	other := reclaimableReplica(t, s, high, "other-surplus", "w1")
	entries := []types.FleetEntry{
		{EndpointID: high.Spec.ID, MinReplicas: 1, MaxReplicas: 3, ProtectMinimum: true},
		{EndpointID: low.Spec.ID, MinReplicas: 1, MaxReplicas: 1, ProtectMinimum: true},
	}
	live := []*types.EndpointReplica{protected, extra, other}
	s.controller.fill(context.Background(), "H100", entries, endpoints, live, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, extra.ID))
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, protected.ID))
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, other.ID))
	// Repeated reconciliation waits for the already selected victim's GPU.
	s.controller.fill(context.Background(), "H100", entries, endpoints, live, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, other.ID))
	// Once the worker reports that GPU free, the missing minimum is restored.
	extra.Status = types.ReplicaStatusStopped
	require.NoError(t, s.repo.SaveReplica(context.Background(), extra))
	s.controller.fill(context.Background(), "H100", entries, endpoints, live, idleInventory(1), nil)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	assert.Equal(t, low.StubID, requests[0].StubId)
	assert.False(t, requests[0].Evictable)
}

func TestFillReclaimRequiresAUsefulWorkerAndPreservesVictimMinimum(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		split                            bool
		minimum, floor                   uint32
		protected, nonEvictable, offline bool
		wantDrain                        bool
	}{
		{name: "same worker makes room", wantDrain: true},
		{name: "fragmented workers", split: true},
		{name: "victim minimum", minimum: 1},
		{name: "serverless GPU floor", floor: 1},
		{name: "protected replica", protected: true},
		{name: "non-evictable container", nonEvictable: true},
		{name: "offline worker", offline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			high, low, endpoints := fillEndpoints(t, s)
			high.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 2}}
			first := reclaimableReplica(t, s, low, "first", "w1")
			secondWorker := "w1"
			inv := noRoomInventory()
			if tc.split {
				secondWorker = "w2"
				inv.workers["w1"].total = 1
				inv.workers["w2"] = &inventoryWorker{pool: "gpu-a", gpu: "H100", total: 1}
			}
			second := reclaimableReplica(t, s, low, "second", secondWorker)
			if tc.protected {
				first.Protected = true
				require.NoError(t, s.repo.SaveReplica(context.Background(), first))
			}
			if tc.nonEvictable {
				state, err := s.containers.GetContainerState(first.ContainerID)
				require.NoError(t, err)
				state.Evictable = false
				require.NoError(t, s.containers.SetContainerState(first.ContainerID, state))
			}
			if tc.offline {
				delete(inv.workers, "w1")
			}
			inv.floors = map[string]uint32{"gpu-a": tc.floor}
			entries := []types.FleetEntry{
				{EndpointID: high.Spec.ID, MinReplicas: 1, MaxReplicas: 1},
				{EndpointID: low.Spec.ID, MinReplicas: tc.minimum, MaxReplicas: 2},
			}
			s.controller.fill(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{first, second}, inv, nil)
			drained := 0
			for _, replica := range []*types.EndpointReplica{first, second} {
				if statusOf(t, s, replica.ID) == types.ReplicaStatusDraining {
					drained++
				}
			}
			if tc.wantDrain {
				assert.Equal(t, 1, drained)
			} else {
				assert.Zero(t, drained, "do not drain replicas when they cannot make one usable slot")
			}
		})
	}
}

func TestFillLowersCapWhileAnotherMinimumWaits(t *testing.T) {
	s := newServiceForTest(t)
	high, low, endpoints := fillEndpoints(t, s)
	first := reclaimableReplica(t, s, low, "first", "w1")
	second := reclaimableReplica(t, s, low, "second", "w1")
	entries := []types.FleetEntry{
		{EndpointID: high.Spec.ID, MinReplicas: 1, MaxReplicas: 1},
		{EndpointID: low.Spec.ID, MaxReplicas: 1},
	}
	s.controller.fill(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{first, second}, noRoomInventory(), nil)
	draining := 0
	for _, replica := range []*types.EndpointReplica{first, second} {
		if replica.Status == types.ReplicaStatusDraining {
			draining++
		}
	}
	assert.Equal(t, 1, draining, "the cap trim already releases enough; do not reclaim another copy while it drains")
}

func TestInventoryRejectsFragmentedGPUs(t *testing.T) {
	s := newServiceForTest(t)
	s.workers = repository.NewWorkerRedisRepositoryForTest(s.rdb)
	s.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{"gpu-a": {GPUType: "H100", ManagedEndpoints: types.WorkerPoolManagedEndpointsConfig{Enabled: true}}}
	for _, id := range []string{"w1", "w2"} {
		require.NoError(t, s.workers.AddWorker(&types.Worker{Id: id, PoolName: "gpu-a", Gpu: "H100", TotalGpuCount: 1, FreeGpuCount: 1, Status: types.WorkerStatusAvailable}))
	}
	inv, err := s.controller.inventory(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), inv.idle("H100", "gpu-a"))
	assert.False(t, inv.canPlace("H100", 2))
	_, ok := inv.place("H100", 2)
	assert.False(t, ok, "two 1-GPU workers cannot host a 2-GPU replica")
	_, ok = inv.place("H100", 1)
	assert.True(t, ok)
	_, ok = inv.place("H100", 1)
	assert.True(t, ok)
	assert.False(t, inv.canPlace("H100", 1), "reservations consume each worker fragment")
}

func TestFillMinimumPriorityWhenCapacityIsInsufficient(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	entries := []types.FleetEntry{
		{EndpointID: high.Spec.ID, MinReplicas: 2, MaxReplicas: 4},
		{EndpointID: low.Spec.ID, MinReplicas: 1, MaxReplicas: 2},
	}
	s.controller.fill(context.Background(), "H100", entries, endpoints, nil, idleInventory(2), nil)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	for _, request := range requests {
		assert.Equal(t, high.StubID, request.StubId)
	}
}

func TestFillRolloutDoesNotDuplicateProtectedMinimum(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	old := versionReplica(t, s, "old-protected", 1, types.ReplicaStatusReady)
	old.Protected = true
	require.NoError(t, s.repo.SaveReplica(context.Background(), old))
	entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, MinReplicas: 2, MaxReplicas: 2, ProtectMinimum: true}}
	s.controller.fill(context.Background(), "H100", entries, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, []*types.EndpointReplica{old}, idleInventory(2), nil)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	assert.False(t, requests[0].Evictable, "one protected slot is still missing")
	assert.True(t, requests[1].Evictable, "old ready replica already owns the other protected slot")
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID))
}

func TestFillCapTrimPreservesProtectedMinimum(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	protected := versionReplica(t, s, "protected", 1, types.ReplicaStatusLoading)
	protected.Protected = true
	require.NoError(t, s.repo.SaveReplica(context.Background(), protected))
	surplus := versionReplica(t, s, "surplus", 1, types.ReplicaStatusReady)
	entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, MinReplicas: 1, MaxReplicas: 1, ProtectMinimum: true}}
	s.controller.fill(context.Background(), "H100", entries, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, []*types.EndpointReplica{protected, surplus}, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusLoading, statusOf(t, s, protected.ID))
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, surplus.ID))
}

func TestFillReclaimChecksCPUAndPaddedMemoryOnOneWorker(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		cpu, memory, victimCPU, victimMemory int64
		otherCPU, otherMemory                int64
		config                               string
		backendError                         bool
		wantDrain                            bool
	}{
		{name: "exact padded fit", cpu: 1000, victimCPU: 1000, victimMemory: 101, wantDrain: true},
		{name: "memory overhead cannot fit", cpu: 1000, victimCPU: 1000, victimMemory: 100},
		{name: "CPU held by serverless", cpu: 0, victimCPU: 1000, victimMemory: 101},
		{name: "CPU fragmented across workers", cpu: 1000, otherCPU: 1000, victimMemory: 101},
		{name: "memory fragmented across workers", cpu: 2000, memory: 64, otherMemory: 63},
		{name: "backend unavailable", backendError: true},
		{name: "malformed runtime", config: `{"runtime":`},
		{name: "negative resources", config: `{"runtime":{"cpu":-1,"memory":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			high, low, endpoints := fillEndpoints(t, s)
			victim := reclaimableReplica(t, s, low, "surplus", "w1")
			config := tc.config
			if config == "" {
				config = `{"runtime":{"cpu":2000,"memory":101}}`
			}
			backend := fillBackend{config: config}
			if tc.backendError {
				backend.err = errors.New("backend unavailable")
			}
			s.backend = backend
			state, err := s.containers.GetContainerState(victim.ContainerID)
			require.NoError(t, err)
			state.Cpu, state.Memory = tc.victimCPU, tc.victimMemory
			require.NoError(t, s.containers.SetContainerState(victim.ContainerID, state))
			inv := noRoomInventory()
			inv.workers["w1"].cpu, inv.workers["w1"].memory = tc.cpu, tc.memory
			inv.workers["w2"] = &inventoryWorker{pool: "gpu-a", gpu: "H100", total: 1, cpu: tc.otherCPU, memory: tc.otherMemory}
			entries := []types.FleetEntry{{EndpointID: high.Spec.ID, MinReplicas: 1, MaxReplicas: 1}, {EndpointID: low.Spec.ID, MaxReplicas: 1}}
			s.controller.fill(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{victim}, inv, nil)
			want := types.ReplicaStatusReady
			if tc.wantDrain {
				want = types.ReplicaStatusDraining
			}
			assert.Equal(t, want, statusOf(t, s, victim.ID))
		})
	}
}

func TestRetirePreservesProtectedMinimumWhileReplacementLoads(t *testing.T) {
	for _, rollout := range []types.Rollout{"", types.RolloutReplace} {
		t.Run("rollout="+string(rollout), func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			endpoint.Version, endpoint.Spec.Rollout = 2, rollout
			fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 2, MaxReplicas: 2}}}})
			old := versionReplica(t, s, "old-protected", 1, types.ReplicaStatusReady)
			ready := versionReplica(t, s, "new-ready", 2, types.ReplicaStatusReady)
			loading := versionReplica(t, s, "new-loading", 2, types.ReplicaStatusLoading)
			old.Protected, ready.Protected = true, true
			require.NoError(t, s.repo.SaveReplica(context.Background(), old))
			require.NoError(t, s.repo.SaveReplica(context.Background(), ready))
			live := []*types.EndpointReplica{old, ready, loading}
			s.controller.retire(context.Background(), endpoint, fleet, live, nil)
			assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID), "one ready replacement cannot cover a two-replica protected floor")
			// The protection pass transfers the slot once the second copy serves.
			loading.Status, loading.Protected, old.Protected = types.ReplicaStatusReady, true, false
			require.NoError(t, s.repo.SaveReplica(context.Background(), loading))
			require.NoError(t, s.repo.SaveReplica(context.Background(), old))
			s.controller.retire(context.Background(), endpoint, fleet, live, nil)
			assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, old.ID))
		})
	}
}

func TestRetireProtectedMinimumIsPerGPU(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	endpoint.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 1}, "A100-80": {Count: 1}}
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{
		"H100": {Priority: 1, MinReplicas: 1, MaxReplicas: 1}, "A100-80": {Priority: 1, MinReplicas: 1, MaxReplicas: 1},
	}}})
	old := versionReplica(t, s, "old-a100", 1, types.ReplicaStatusReady)
	old.GPU, old.Protected = "A100-80", true
	ready := versionReplica(t, s, "new-h100", 2, types.ReplicaStatusReady)
	ready.Protected = true
	loading := versionReplica(t, s, "new-a100", 2, types.ReplicaStatusLoading)
	loading.GPU = "A100-80"
	for _, replica := range []*types.EndpointReplica{old, ready, loading} {
		require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	}
	s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{old, ready, loading}, nil)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID), "a ready H100 cannot replace the protected A100 minimum")
}

func TestRetireProtectedMinimumOnlyAllowsExplicitCapacityReplacement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rollout  types.Rollout
		starting bool
		want     types.ReplicaStatus
	}{
		{"default waits", "", false, types.ReplicaStatusReady},
		{"replace makes room", types.RolloutReplace, false, types.ReplicaStatusDraining},
		{"replace waits for a replacement already starting", types.RolloutReplace, true, types.ReplicaStatusReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			endpoint.Version, endpoint.Spec.Rollout = 2, tc.rollout
			fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 2, MaxReplicas: 2}}}})
			old := versionReplica(t, s, "old-protected", 1, types.ReplicaStatusReady)
			ready := versionReplica(t, s, "new-ready", 2, types.ReplicaStatusReady)
			old.Protected, ready.Protected = true, true
			require.NoError(t, s.repo.SaveReplica(context.Background(), old))
			require.NoError(t, s.repo.SaveReplica(context.Background(), ready))
			live := []*types.EndpointReplica{old, ready}
			if tc.starting {
				live = append(live, versionReplica(t, s, "new-scheduling", 2, types.ReplicaStatusScheduling))
			}
			s.controller.retire(context.Background(), endpoint, fleet, live, nil)
			assert.Equal(t, tc.want, statusOf(t, s, old.ID))
		})
	}
}

func TestRetireMakesRoomFromStaleSurplusBeforeProtectedMinimum(t *testing.T) {
	for _, minimum := range []uint32{1, 2} {
		t.Run(fmt.Sprintf("minimum=%d", minimum), func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			endpoint.Version, endpoint.Spec.Rollout = 2, types.RolloutReplace
			noPreemption := false
			fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{
				"H100": {Priority: 1, MinReplicas: minimum, MaxReplicas: minimum + 1, Preemption: &noPreemption},
			}}})
			var live []*types.EndpointReplica
			for i := uint32(0); i < minimum; i++ {
				protected := versionReplica(t, s, fmt.Sprintf("protected-%d", i), 1, types.ReplicaStatusReady)
				protected.Protected = true
				require.NoError(t, s.repo.SaveReplica(context.Background(), protected))
				live = append(live, protected)
			}
			surplus := versionReplica(t, s, "surplus", 1, types.ReplicaStatusReady)
			live = append(live, surplus)

			// Repository order puts the protected copies first. A rollout must
			// release the stale extra without a next-tick protection transfer.
			s.controller.retire(context.Background(), endpoint, fleet, live, nil)
			assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, surplus.ID))
			for _, protected := range live[:minimum] {
				assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, protected.ID))
				assert.True(t, protected.Protected)
			}
			assert.Same(t, surplus, live[len(live)-1], "retirement does not reorder the shared observation")
		})
	}
}

func TestRetireSkipsOnlyProtectionFailureGroup(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	endpoint.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 1}, "A100-80": {Count: 1}}
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1}, "A100-80": {Priority: 1}}}})
	blockedOld := versionReplica(t, s, "blocked-h100", 1, types.ReplicaStatusReady)
	otherOld := versionReplica(t, s, "old-a100", 1, types.ReplicaStatusReady)
	otherOld.GPU = "A100-80"
	require.NoError(t, s.repo.SaveReplica(context.Background(), otherOld))
	ready := versionReplica(t, s, "new-ready", 2, types.ReplicaStatusReady)
	otherReady := versionReplica(t, s, "new-a100", 2, types.ReplicaStatusReady)
	otherReady.GPU = "A100-80"
	require.NoError(t, s.repo.SaveReplica(context.Background(), otherReady))
	blocked := map[group]bool{{endpoint.Spec.ID, "H100"}: true}
	s.controller.retire(context.Background(), endpoint, fleet, []*types.EndpointReplica{blockedOld, otherOld, ready, otherReady}, blocked)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, blockedOld.ID))
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, otherOld.ID), "an unrelated GPU placement must still progress")
}
