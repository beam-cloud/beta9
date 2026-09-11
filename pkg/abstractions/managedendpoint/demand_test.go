package managedendpoint

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDemandLeasesAreIsolatedIdempotentAndExpiring(t *testing.T) {
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	s := &Service{rdb: rdb}
	ctx := context.Background()
	now := time.Now()
	redis.SetTime(now)
	for _, id := range []string{"gateway-a/request-1", "gateway-a/request-1", "gateway-b/request-2"} {
		_, err := s.demand(ctx, "model", "acquire", id, 0)
		require.NoError(t, err)
	}
	d, err := s.demand(ctx, "model", "read", "", 0)
	require.NoError(t, err)
	assert.EqualValues(t, 2, d.active)
	assert.True(t, d.warm)
	redis.SetTime(now.Add(40 * time.Second))
	_, err = s.demand(ctx, "model", "renew", "gateway-b/request-2", 0)
	require.NoError(t, err)
	redis.SetTime(now.Add(70 * time.Second))
	d, err = s.demand(ctx, "model", "read", "", 0)
	require.NoError(t, err)
	assert.EqualValues(t, 1, d.active, "a crashed gateway's request expires despite traffic on another gateway")
	_, err = s.demand(ctx, "model", "renew", "gateway-a/request-1", 0)
	require.Error(t, err, "an expired owner must not resurrect itself")
	d, err = s.demand(ctx, "model", "release", "gateway-b/request-2", 0)
	require.NoError(t, err)
	assert.Zero(t, d.active)
	assert.True(t, d.warm)
	redis.SetTime(now.Add(70*time.Second + demandIdleTimeout + time.Second))
	d, err = s.demand(ctx, "model", "release", "gateway-b/request-2", 0)
	require.NoError(t, err)
	assert.False(t, d.warm, "duplicate release does not renew idle warmth")
	assert.Zero(t, d.active)
}

func TestDemandBoundsRequestsAcrossGateways(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	for i := range serverlessAdmissionHeadroom {
		_, err := s.demand(ctx, "model", "acquire", fmt.Sprint(i), 0)
		require.NoError(t, err)
	}
	_, err := s.demand(ctx, "model", "acquire", "overflow", 0)
	require.ErrorIs(t, err, errDemandLimit)
	_, err = s.demand(ctx, "model", "renew", "0", 0)
	require.NoError(t, err, "the cap must not interrupt existing work")
	_, err = s.demand(ctx, "model", "release", "0", 0)
	require.NoError(t, err)
	_, err = s.demand(ctx, "model", "acquire", "overflow", 0)
	require.NoError(t, err)
}

func TestDemandAdmissionHeadroomPreservesScaleOut(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, Serverless: true, MaxReplicas: 2}},
	}})
	replica := versionReplica(t, s, "ready", endpoint.Version, types.ReplicaStatusReady)
	replica.Address = "ready:8000"
	replica.Capacity.MaxConcurrency = 128
	ctx := context.Background()
	require.NoError(t, s.repo.SaveReplica(ctx, replica))
	for i := 0; i < 128; i++ {
		_, err := s.demand(ctx, endpoint.Spec.ID, "acquire", fmt.Sprint(i), 128)
		require.NoError(t, err)
	}

	// Reuse the normal routing snapshot instead of adding another registry
	// lookup on admission. Request129 must reach the distributed demand set.
	router := newRouter(s)
	httpCtx, _ := coldRouteContext()
	rq := &routeRequest{ctx: httpCtx, auth: httpCtx.AuthInfo, route: types.EndpointRouteChatCompletions, models: []string{endpoint.Spec.ID}, requestID: "request-129"}
	resolved, rerr := router.resolveEndpoint(ctx, rq)
	require.Nil(t, rerr)
	require.NotNil(t, resolved)
	assert.EqualValues(t, 128, rq.readyCapacity)
	rq.model = resolved.Spec.ID
	release, err := router.holdDemand(rq)
	require.NoError(t, err, "a full128-slot engine must allow a request to ask for the next replica")
	t.Cleanup(release)
	live := []*types.EndpointReplica{replica}
	demand := s.controller.readDemand(ctx, fleet, live)
	assert.EqualValues(t, 129, demand[endpoint.Spec.ID].active)
	assert.EqualValues(t, 128, demand[endpoint.Spec.ID].capacity)
	s.controller.fillWithDemand(ctx, "H100", fleet.Entries("H100"), map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, live, idleInventory(1), demand)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 1, "excess demand scales out onto one spare GPU")
	assert.True(t, requests[0].OpportunisticOnly)
	assert.True(t, requests[0].Evictable)

	// Capacity can disappear after admission without revoking live owners.
	_, err = s.demand(ctx, endpoint.Spec.ID, "acquire", "after-eviction", 0)
	require.ErrorIs(t, err, errDemandLimit)
	_, err = s.demand(ctx, endpoint.Spec.ID, "renew", "request-129", 0)
	require.NoError(t, err, "shrinking capacity must not cancel an admitted request")
	_, err = s.demand(ctx, endpoint.Spec.ID, "release", "0", 0)
	require.NoError(t, err)
}

func TestDemandLimitsAdmissionHeadroomBeyondReadyCapacity(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	const readyCapacity = 192
	for i := 0; i < readyCapacity+serverlessAdmissionHeadroom; i++ {
		_, err := s.demand(ctx, "model", "acquire", fmt.Sprint(i), readyCapacity)
		require.NoError(t, err)
	}
	_, err := s.demand(ctx, "model", "acquire", "overflow", readyCapacity)
	require.ErrorIs(t, err, errDemandLimit, "all gateways share one128-request admission headroom")
	_, err = s.demand(ctx, "model", "release", "0", 0)
	require.NoError(t, err)
	_, err = s.demand(ctx, "model", "acquire", "overflow", readyCapacity)
	require.NoError(t, err)
}

func TestReadDemandUnlimitedCapacityDoesNotScaleOut(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, Serverless: true, MaxReplicas: 3}},
	}})
	unlimited := versionReplica(t, s, "unlimited", endpoint.Version, types.ReplicaStatusReady)
	finite := versionReplica(t, s, "finite", endpoint.Version, types.ReplicaStatusReady)
	finite.Capacity.MaxConcurrency = 128
	ctx := context.Background()
	for i := 0; i < 129; i++ {
		_, err := s.demand(ctx, endpoint.Spec.ID, "acquire", fmt.Sprint(i), 128)
		require.NoError(t, err)
	}
	live := []*types.EndpointReplica{unlimited, finite}
	demand := s.controller.readDemand(ctx, fleet, live)
	assert.EqualValues(t, math.MaxInt64, demand[endpoint.Spec.ID].capacity, "adding finite slots to unlimited capacity cannot overflow")
	s.controller.fillWithDemand(ctx, "H100", fleet.Entries("H100"), map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, live, idleInventory(1), demand)
	requests, _ := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	assert.Empty(t, requests, "a replica that routes unlimited work already covers the demand")
}

func TestDemandCapacityAdmissionDoesNotOverflow(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	for _, id := range []string{"large-one", "large-two"} {
		replica := versionReplica(t, s, id, endpoint.Version, types.ReplicaStatusReady)
		replica.Address = id + ":8000"
		replica.Capacity.MaxConcurrency = math.MaxInt64
		require.NoError(t, s.repo.SaveReplica(ctx, replica))
	}
	router := newRouter(s)
	httpCtx, _ := coldRouteContext()
	rq := &routeRequest{ctx: httpCtx, auth: httpCtx.AuthInfo, route: types.EndpointRouteChatCompletions, models: []string{endpoint.Spec.ID}, requestID: "request"}
	resolved, rerr := router.resolveEndpoint(ctx, rq)
	require.Nil(t, rerr)
	rq.model = resolved.Spec.ID
	assert.EqualValues(t, math.MaxInt64, rq.readyCapacity)
	release, err := router.holdDemand(rq)
	require.NoError(t, err, "adding queue allowance cannot wrap a large capacity into a negative limit")
	release()
}

func TestServerlessConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		fields            string
		valid, serverless bool
	}{
		{"", true, false},
		{"serverless: false", true, false},
		{"serverless: true", true, true},
		{"serverless: true, minReplicas: 0, maxReplicas: 2, preemption: true", true, true},
		{"serverless: true, minReplicas: 1", false, false},
		{"serverless: true, preemption: false", false, false},
		{"serverless: 'true'", false, false},
		{"serverless: null", false, false},
		{"serverless: 1", false, false},
	} {
		t.Run(tc.fields, func(t *testing.T) {
			fleet, err := parseFleet("acme/model: {enabled: true, gpus: {H100: {priority: 1, " + tc.fields + "}}}")
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.serverless, fleet.Serverless("acme/model"))
			assert.Equal(t, tc.serverless, fleet.Entries("H100")[0].Serverless)
		})
	}
}

func TestServerlessStartsOnlyForUnservedActiveDemand(t *testing.T) {
	for _, tc := range []struct {
		name   string
		demand *endpointDemand
		free   uint32
		starts int
	}{
		{"unknown demand", nil, 2, 0},
		{"idle", &endpointDemand{}, 2, 0},
		{"recent canceled request", &endpointDemand{warm: true}, 2, 0},
		{"cold", &endpointDemand{active: 20, warm: true}, 2, 1},
		{"cold request already rejected", &endpointDemand{pending: true, warm: true}, 2, 1},
		{"rejected at full capacity", &endpointDemand{active: 8, capacity: 8, pending: true, warm: true}, 2, 1},
		{"capacity freed since rejection", &endpointDemand{active: 7, capacity: 8, pending: true, warm: true}, 2, 0},
		{"rejected while loading", &endpointDemand{pending: true, warm: true, starting: true}, 2, 0},
		{"loading already", &endpointDemand{active: 20, warm: true, starting: true}, 2, 0},
		{"hot capacity covers requests", &endpointDemand{active: 2, warm: true, capacity: 8}, 2, 0},
		{"saturated", &endpointDemand{active: 9, warm: true, capacity: 8}, 2, 1},
		{"no spare capacity", &endpointDemand{active: 1, warm: true}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newFillService(t)
			s.config.Preemption.Enabled = false // explicit serverless mode always remains evictable
			endpoint := seedEndpoint(t, s)
			entries := []types.FleetEntry{{EndpointID: endpoint.Spec.ID, Serverless: true, MaxReplicas: 2}}
			s.controller.fillWithDemand(context.Background(), "H100", entries, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, nil, idleInventory(tc.free), map[string]*endpointDemand{endpoint.Spec.ID: tc.demand})
			requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
			if tc.starts > 0 {
				require.NoError(t, err)
			}
			require.Len(t, requests, tc.starts)
			for _, request := range requests {
				assert.True(t, request.OpportunisticOnly)
				assert.True(t, request.Evictable)
			}
		})
	}
}

func TestServerlessCapAndIdleRetirementIncludeOldVersions(t *testing.T) {
	for _, warm := range []bool{true, false} {
		s := newFillService(t)
		endpoint := seedEndpoint(t, s)
		current := versionReplica(t, s, "current", 1, types.ReplicaStatusReady)
		old := versionReplica(t, s, "old", 0, types.ReplicaStatusReady)
		live := []*types.EndpointReplica{current, old}
		for _, replica := range live {
			replica.StartedAt = time.Now().Add(-time.Hour)
			replica.ReadyAt = time.Now().Add(-time.Hour)
			require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
		}
		d := &endpointDemand{warm: warm}
		if warm {
			d.active = 100
		}
		s.controller.fillWithDemand(context.Background(), "H100", []types.FleetEntry{{EndpointID: endpoint.Spec.ID, Serverless: true, MaxReplicas: 1}}, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, live, idleInventory(3), map[string]*endpointDemand{endpoint.Spec.ID: d})
		requests, _ := scheduler.NewRequestBacklog(s.rdb).PopN(10)
		assert.Empty(t, requests, "positive max bounds demand even with idle GPUs")
		for _, replica := range live {
			if warm {
				assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, replica.ID))
			} else {
				assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, replica.ID))
			}
		}
	}
}

func TestServerlessReclaimsHotSurplus(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	other := reclaimableReplica(t, s, low, "hot-surplus", "w1")
	entries := []types.FleetEntry{{EndpointID: high.Spec.ID, Serverless: true, MaxReplicas: 1}, {EndpointID: low.Spec.ID, MaxReplicas: 1}}
	s.controller.fillWithDemand(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{other}, noRoomInventory(), map[string]*endpointDemand{high.Spec.ID: {active: 1, warm: true}})
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, other.ID))
}

func TestServerlessRolloutStartsReplacementOnSpareCapacity(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
	old.Capacity.MaxConcurrency = 64
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, Serverless: true, MaxReplicas: 1}}}})
	ctx := context.Background()
	_, err := s.demand(ctx, endpoint.Spec.ID, "acquire", "request", 0)
	require.NoError(t, err)
	live := []*types.EndpointReplica{old}
	demand := s.controller.readDemand(ctx, fleet, live)
	assert.EqualValues(t, 64, demand[endpoint.Spec.ID].capacity)
	s.controller.fillWithDemand(ctx, "H100", fleet.Entries("H100"), map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, live, idleInventory(1), demand)
	requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
	require.NoError(t, err)
	require.Len(t, requests, 1, "a warm old version must not satisfy the new version's rollout")
	assert.True(t, requests[0].Evictable)
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID))
}

func TestServerlessProtectionOverridesClusterDefault(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	r := versionReplica(t, s, "replica", 1, types.ReplicaStatusReady)
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, Serverless: true}}}})
	for _, enabled := range []bool{true, false} {
		assert.Empty(t, protectedReplicas(fleet, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, []*types.EndpointReplica{r}, enabled))
	}
}

func TestHotSurplusDoesNotChurnRequestedServerlessReplica(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	other := reclaimableReplica(t, s, low, "requested", "w1")
	entries := []types.FleetEntry{{EndpointID: high.Spec.ID, MaxReplicas: 1}, {EndpointID: low.Spec.ID, Serverless: true, MaxReplicas: 1}}
	s.controller.fillWithDemand(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{other}, noRoomInventory(), map[string]*endpointDemand{low.Spec.ID: {active: 1, warm: true}})
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, other.ID))
	entries[0].MinReplicas = 1
	s.controller.fillWithDemand(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{other}, noRoomInventory(), map[string]*endpointDemand{low.Spec.ID: {active: 1, warm: true}})
	assert.Equal(t, types.ReplicaStatusDraining, statusOf(t, s, other.ID), "a configured hot minimum still has priority over spare-only copies")
}

func TestRejectedDemandCoalescesAndExpiresWithoutActiveLeases(t *testing.T) {
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	s := &Service{rdb: rdb}
	ctx := context.Background()
	now := time.Now()
	redis.SetTime(now)
	for i := range 20 {
		id := fmt.Sprint(i)
		_, err := s.demand(ctx, "model", "acquire", id, 0)
		require.NoError(t, err)
		_, err = s.demand(ctx, "model", "wake", "", 0)
		require.NoError(t, err)
		_, err = s.demand(ctx, "model", "release", id, 0)
		require.NoError(t, err)
	}
	d, err := s.demand(ctx, "model", "read", "", 0)
	require.NoError(t, err)
	assert.Zero(t, d.active)
	assert.True(t, d.pending)
	members, err := rdb.ZCard(ctx, "managed_endpoint:demand:model").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 2, members, "one wake and one idle marker, regardless of retries")
	for i := range serverlessAdmissionHeadroom {
		_, err := s.demand(ctx, "model", "acquire", fmt.Sprint(i), 0)
		require.NoError(t, err, "markers do not consume admission slots")
	}
	redis.SetTime(now.Add(demandLeaseTTL + time.Second))
	d, err = s.demand(ctx, "model", "read", "", 0)
	require.NoError(t, err)
	assert.Zero(t, d.active)
	assert.False(t, d.pending, "a single rejection cannot keep requesting replicas forever")
	assert.True(t, d.warm)
}

func TestServerlessInitialLoadAndReadyGraceAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  types.ReplicaStatus
		started time.Duration
		ready   time.Duration
		drain   bool
	}{
		{"loading beyond request idle", types.ReplicaStatusLoading, 6 * time.Minute, 0, false},
		{"loading expired", types.ReplicaStatusLoading, 11 * time.Minute, 0, true},
		{"freshly ready after long load", types.ReplicaStatusReady, 9 * time.Minute, time.Minute, false},
		{"ready idle expired", types.ReplicaStatusReady, 15 * time.Minute, 6 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			replica := versionReplica(t, s, "cold-copy", endpoint.Version, tc.status)
			replica.StartedAt = time.Now().Add(-tc.started)
			if tc.ready > 0 {
				replica.ReadyAt = time.Now().Add(-tc.ready)
			}
			require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
			s.controller.fillWithDemand(context.Background(), "H100", []types.FleetEntry{{EndpointID: endpoint.Spec.ID, Serverless: true, MaxReplicas: 1}}, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, []*types.EndpointReplica{replica}, idleInventory(0), map[string]*endpointDemand{endpoint.Spec.ID: {}})
			want := tc.status
			if tc.drain {
				want = types.ReplicaStatusDraining
				if tc.status != types.ReplicaStatusReady {
					want = types.ReplicaStatusStopped
				}
			}
			assert.Equal(t, want, statusOf(t, s, replica.ID))
		})
	}
}

func TestHotSurplusRespectsServerlessStartupGrace(t *testing.T) {
	for _, ready := range []bool{false, true} {
		s := newFillService(t)
		s.scheduler = nil // This test exercises victim selection and retirement.
		high, low, _ := fillEndpoints(t, s)
		replica := reclaimableReplica(t, s, low, "requested", "w1")
		replica.StartedAt = time.Now().Add(-6 * time.Minute)
		if ready {
			replica.ReadyAt = time.Now().Add(-time.Minute)
		} else {
			replica.Status = types.ReplicaStatusLoading
		}
		require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
		targets := []*placementTarget{
			{entry: types.FleetEntry{EndpointID: high.Spec.ID, MaxReplicas: 1}, endpoint: high},
			{entry: types.FleetEntry{EndpointID: low.Spec.ID, Serverless: true, MaxReplicas: 1}, endpoint: low, replicas: []*types.EndpointReplica{replica}, demand: &endpointDemand{}},
		}
		live := []*types.EndpointReplica{replica}
		assert.False(t, s.controller.reclaim(context.Background(), "H100", 0, targets, live, noRoomInventory(), false), "hot extras must respect initial loading and ready grace")
		replica.StartedAt = time.Now().Add(-time.Hour)
		if ready {
			replica.ReadyAt = time.Now().Add(-time.Hour)
		}
		require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
		assert.True(t, s.controller.reclaim(context.Background(), "H100", 0, targets, live, noRoomInventory(), false), "idle copies remain reclaimable after their bounded grace")
	}
}
