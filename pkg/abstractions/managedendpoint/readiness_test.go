package managedendpoint

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vLLM's engine core registers before the API process binds its listener.
// Neither engine readiness nor HTTP readiness alone can admit a replica.
func TestHarnessReadinessRequiresServingHTTP(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	r := seedReplica(t, s, seedEndpoint(t, s))
	var healthy atomic.Bool
	var probes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "/health", req.URL.Path, "harness capacity must not be overwritten by a metrics scrape")
		probes.Add(1)
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	r.Address = strings.TrimPrefix(server.URL, "http://")
	require.NoError(t, s.containers.SetContainerState(r.ContainerID, &types.ContainerState{
		ContainerId: r.ContainerID, Status: types.ContainerStatusRunning,
	}))
	ctx := context.Background()
	healthy.Store(true)
	require.NoError(t, s.controller.syncReplica(ctx, r))
	assert.False(t, r.Serving(), "HTTP alone does not establish harness readiness")
	assert.Zero(t, probes.Load())

	healthy.Store(false)
	s.applyHeartbeat(r, &pb.HarnessHeartbeatRequest{Status: "ready", Capacity: &pb.ReplicaCapacity{MaxConcurrency: 32}})
	heartbeat := r.LastHeartbeat
	assert.False(t, r.Serving(), "engine registration is not HTTP readiness")
	assert.True(t, r.ReadyAt.IsZero())
	require.NoError(t, s.controller.syncReplica(ctx, r))
	assert.False(t, r.Serving())

	healthy.Store(true)
	require.NoError(t, s.controller.syncReplica(ctx, r))
	assert.True(t, r.Serving())
	assert.False(t, r.ReadyAt.IsZero())
	assert.Equal(t, heartbeat, r.LastHeartbeat, "HTTP must not hide a stale engine heartbeat")
	assert.EqualValues(t, 32, r.Capacity.MaxConcurrency)

	healthy.Store(false)
	require.NoError(t, s.controller.syncReplica(ctx, r))
	assert.False(t, r.Serving(), "HTTP failure removes the replica despite a live engine")
	s.applyHeartbeat(r, &pb.HarnessHeartbeatRequest{Status: "ready"})
	assert.False(t, r.Serving(), "another heartbeat cannot override a failed HTTP probe")

	healthy.Store(true)
	s.applyHeartbeat(r, &pb.HarnessHeartbeatRequest{Status: "loading"})
	require.NoError(t, s.controller.syncReplica(ctx, r))
	assert.False(t, r.Serving(), "an engine reload stays out of routing even while HTTP responds")
}

func TestFailedLastUpstreamIsNotReportedAsSaturation(t *testing.T) {
	s := newServiceForTest(t)
	e := seedEndpoint(t, s)
	replica := seedReplica(t, s, e)
	replica.Status, replica.Address = types.ReplicaStatusReady, "unreachable:8000"
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	started := time.Now()
	picked, err := newRouter(s).pick(context.Background(), &routeRequest{startedAt: started}, e, map[string]bool{replica.ID: true})
	assert.Nil(t, picked)
	require.NotNil(t, err)
	assert.Equal(t, http.StatusBadGateway, err.Status)
	assert.Equal(t, "upstream_unavailable", err.Code)
	assert.Less(t, time.Since(started), time.Second, "no alternative exists to wait for")
}
