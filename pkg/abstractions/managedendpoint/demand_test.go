package managedendpoint

import (
	"context"
	"fmt"
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
		_, err := s.demand(ctx, "model", "acquire", id)
		require.NoError(t, err)
	}
	d, err := s.demand(ctx, "model", "read", "")
	require.NoError(t, err)
	assert.EqualValues(t, 2, d.active)
	assert.True(t, d.warm)
	redis.SetTime(now.Add(40 * time.Second))
	_, err = s.demand(ctx, "model", "renew", "gateway-b/request-2")
	require.NoError(t, err)
	redis.SetTime(now.Add(70 * time.Second))
	d, err = s.demand(ctx, "model", "read", "")
	require.NoError(t, err)
	assert.EqualValues(t, 1, d.active, "a crashed gateway's request expires despite traffic on another gateway")
	_, err = s.demand(ctx, "model", "renew", "gateway-a/request-1")
	require.Error(t, err, "an expired owner must not resurrect itself")
	d, err = s.demand(ctx, "model", "release", "gateway-b/request-2")
	require.NoError(t, err)
	assert.Zero(t, d.active)
	assert.True(t, d.warm)
	redis.SetTime(now.Add(70*time.Second + demandIdleTimeout + time.Second))
	d, err = s.demand(ctx, "model", "release", "gateway-b/request-2")
	require.NoError(t, err)
	assert.False(t, d.warm, "duplicate release does not renew idle warmth")
	assert.Zero(t, d.active)
}

func TestDemandBoundsRequestsAcrossGateways(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	for i := range serverlessMaxRequests {
		_, err := s.demand(ctx, "model", "acquire", fmt.Sprint(i))
		require.NoError(t, err)
	}
	_, err := s.demand(ctx, "model", "acquire", "overflow")
	require.ErrorIs(t, err, errDemandLimit)
	_, err = s.demand(ctx, "model", "renew", "0")
	require.NoError(t, err, "the cap must not interrupt existing work")
	_, err = s.demand(ctx, "model", "release", "0")
	require.NoError(t, err)
	_, err = s.demand(ctx, "model", "acquire", "overflow")
	require.NoError(t, err)
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

func TestServerlessDoesNotReclaimHotReplicas(t *testing.T) {
	s := newFillService(t)
	high, low, endpoints := fillEndpoints(t, s)
	other := reclaimableReplica(t, s, low, "hot-surplus", "w1")
	entries := []types.FleetEntry{{EndpointID: high.Spec.ID, Serverless: true, MaxReplicas: 1}, {EndpointID: low.Spec.ID, MaxReplicas: 1}}
	s.controller.fillWithDemand(context.Background(), "H100", entries, endpoints, []*types.EndpointReplica{other}, noRoomInventory(), map[string]*endpointDemand{high.Spec.ID: {active: 1, warm: true}})
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, other.ID))
}

func TestServerlessRolloutStartsReplacementOnSpareCapacity(t *testing.T) {
	s := newFillService(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Version = 2
	old := versionReplica(t, s, "old", 1, types.ReplicaStatusReady)
	old.Capacity.MaxConcurrency = 64
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, Serverless: true, MaxReplicas: 1}}}})
	ctx := context.Background()
	_, err := s.demand(ctx, endpoint.Spec.ID, "acquire", "request")
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
