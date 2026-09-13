package managedendpoint

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSlotService(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Service{ctx: ctx, drainCtx: ctx, rdb: rdb}, server
}

func TestHostedSlotAcquireIsAtomicIdempotentAndOwned(t *testing.T) {
	s, server := newSlotService(t)
	ctx := context.Background()
	now := time.Now()
	server.SetTime(now)
	for range 2 {
		ok, err := s.slot(ctx, "replica", "acquire", "request-a", 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	assert.EqualValues(t, 1, s.rdb.ZCard(ctx, slotKey("replica")).Val())
	ok, err := s.slot(ctx, "replica", "acquire", "request-b", 1)
	require.NoError(t, err)
	assert.False(t, ok)
	server.SetTime(now.Add(slotLeaseTTL + time.Second))
	ok, err = s.slot(ctx, "replica", "acquire", "request-b", 1)
	require.NoError(t, err)
	assert.True(t, ok, "expired request releases its own slot")
	ok, err = s.slot(ctx, "replica", "renew", "request-a", 1)
	require.ErrorIs(t, err, errSlotLeaseLost)
	assert.False(t, ok, "old request cannot resurrect its lease")
	ok, err = s.slot(ctx, "replica", "release", "request-a", 1)
	require.NoError(t, err)
	assert.False(t, ok, "late release cannot touch request-b")
	assert.Equal(t, []string{"request-b"}, s.rdb.ZRange(ctx, slotKey("replica"), 0, -1).Val())
	ok, err = s.slot(ctx, "replica", "release", "request-b", 1)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Zero(t, s.rdb.Exists(ctx, slotKey("replica")).Val())
}

func TestHostedSlotCrashExpiryIsIndependentOfOtherTraffic(t *testing.T) {
	s, server := newSlotService(t)
	a, b := newRouter(s), newRouter(s)
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "model"}}
	replica := &types.EndpointReplica{ID: "replica", Capacity: types.ReplicaCapacity{MaxConcurrency: 2}}
	ctx := context.Background()
	now := time.Now()
	server.SetTime(now)
	for _, request := range []struct {
		router *router
		id     string
	}{{a, "crashed"}, {b, "running"}} {
		ok, err := request.router.reserve(ctx, &routeRequest{requestID: request.id}, request.router.state("model"), replica)
		require.NoError(t, err)
		require.True(t, ok)
	}
	for _, elapsed := range []time.Duration{40 * time.Second, 70 * time.Second} {
		server.SetTime(now.Add(elapsed))
		ok, err := s.slot(ctx, replica.ID, "renew", "running", 0)
		require.NoError(t, err)
		require.True(t, ok)
	}
	pressure, err := b.state("model").Pressure(ctx, replica.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, pressure.ActiveStreams, "old soft pressure still contains the crashed request")
	selected, err := b.choose(ctx, &routeRequest{requestID: "new"}, endpoint, []*types.EndpointReplica{replica})
	require.NoError(t, err)
	require.NotNil(t, selected, "stale soft pressure cannot block an expired hard slot")
	assert.ElementsMatch(t, []string{"running", "new"}, s.rdb.ZRange(ctx, slotKey(replica.ID), 0, -1).Val())
}

func TestHostedSlotLongGenerationRenewsAcrossGateways(t *testing.T) {
	s, server := newSlotService(t)
	a, b := newRouter(s), newRouter(s)
	ctx := context.Background()
	now := time.Now()
	server.SetTime(now)
	replica := &types.EndpointReplica{ID: "replica", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}}
	requestA := &routeRequest{requestID: "long-generation"}
	ok, err := a.reserve(ctx, requestA, a.state("model"), replica)
	require.NoError(t, err)
	require.True(t, ok)
	for i := 1; i <= 40; i++ {
		server.SetTime(now.Add(time.Duration(i) * slotRenewInterval))
		server.FastForward(slotRenewInterval)
		ok, err = s.slot(ctx, replica.ID, "renew", requestA.requestID, 0)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = b.reserve(ctx, &routeRequest{requestID: fmt.Sprint("retry-", i)}, b.state("model"), replica)
		require.NoError(t, err)
		assert.False(t, ok, "a second gateway must not overlap a long active generation")
	}
	pressure, err := a.state("model").Pressure(ctx, replica.ID)
	require.NoError(t, err)
	assert.Zero(t, pressure.ActiveStreams, "generation outlived the old ten-minute soft counter")
	a.releaseReplica(requestA, a.state("model"), replica)
	ok, err = b.reserve(ctx, &routeRequest{requestID: "after-completion"}, b.state("model"), replica)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestHostedSlotRenewalCancelsLostLeaseAndStopsBeforeRelease(t *testing.T) {
	for _, failure := range []string{"expiry", "redis error"} {
		t.Run(failure, func(t *testing.T) {
			s, server := newSlotService(t)
			ctx := context.Background()
			now := time.Now()
			server.SetTime(now)
			ok, err := s.slot(ctx, "replica", "acquire", "old", 1)
			require.NoError(t, err)
			require.True(t, ok)
			ticks := make(chan time.Time, 1)
			attempt, stop := newRouter(s).renewSlotWithTicks(ctx, "replica", "old", ticks)
			defer stop()
			if failure == "expiry" {
				server.SetTime(now.Add(slotLeaseTTL + time.Second))
				ok, err := s.slot(ctx, "replica", "acquire", "new", 1)
				require.NoError(t, err)
				require.True(t, ok)
			} else {
				require.NoError(t, s.rdb.Del(ctx, slotKey("replica")).Err())
				require.NoError(t, s.rdb.Set(ctx, slotKey("replica"), "wrong-type", 0).Err())
			}
			ticks <- time.Now()
			select {
			case <-attempt.Done():
			case <-time.After(time.Second):
				t.Fatal("lost lease did not cancel the upstream context")
			}
			assert.ErrorIs(t, context.Cause(attempt), errSlotLeaseLost)
			stop()
			if failure == "expiry" {
				_, err := s.slot(ctx, "replica", "release", "old", 0)
				require.NoError(t, err)
				assert.Equal(t, []string{"new"}, s.rdb.ZRange(ctx, slotKey("replica"), 0, -1).Val())
			}
		})
	}
}

func TestHostedSlotRenewalSurvivesDrainAndEndsWithRequestOrService(t *testing.T) {
	for _, ending := range []string{"client", "service", "completed"} {
		t.Run(ending, func(t *testing.T) {
			s, server := newSlotService(t)
			service, cancelService := context.WithCancel(s.ctx)
			defer cancelService()
			s.ctx = service
			drain, cancelDrain := context.WithCancel(context.Background())
			s.drainCtx = drain
			client, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			now := time.Now()
			server.SetTime(now)
			ok, err := s.slot(client, "replica", "acquire", "request", 1)
			require.NoError(t, err)
			require.True(t, ok)
			ticks := make(chan time.Time, 1)
			attempt, stop := newRouter(s).renewSlotWithTicks(client, "replica", "request", ticks)
			defer stop()
			cancelDrain()
			server.SetTime(now.Add(20 * time.Second))
			ticks <- time.Now()
			require.Eventually(t, func() bool {
				return s.rdb.ZScore(context.Background(), slotKey("replica"), "request").Val() >= float64(now.Add(80*time.Second).UnixMilli())
			}, time.Second, time.Millisecond)
			assert.NoError(t, attempt.Err(), "readiness drain must not interrupt a generation")
			switch ending {
			case "client":
				cancelClient()
			case "service":
				cancelService()
			case "completed":
				stop()
			}
			select {
			case <-attempt.Done():
			case <-time.After(time.Second):
				t.Fatal("request lifecycle did not stop renewal")
			}
			stop()
			_, err = s.slot(context.Background(), "replica", "release", "request", 0)
			require.NoError(t, err)
			ticks <- time.Now()
			assert.Zero(t, s.rdb.Exists(context.Background(), slotKey("replica")).Val(), "stopped renewal cannot recreate a released slot")
		})
	}
}

func TestHostedSlotLossDuringJSONBodyReturns503(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Address = "test-engine"
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	started := make(chan struct{})
	releaseServer := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[`))
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-req.Context().Done():
		case <-releaseServer:
		}
	}))
	defer func() { close(releaseServer); upstream.Close() }()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	s.transports.Store(replica.Address, transport)
	ctx, rec := coldRouteContext()
	rq := &routeRequest{ctx: ctx, auth: ctx.AuthInfo, requestID: "lease-lost", app: endpoint, route: types.EndpointRouteChatCompletions, proto: protocols[types.EndpointRouteChatCompletions], startedAt: time.Now()}
	rq.charge = newCharge(rq.requestID, endpoint, ctx.AuthInfo.Workspace, "tok", rq.route, rq.startedAt)
	attempt, cancel := context.WithCancelCause(ctx.Request().Context())
	defer cancel(context.Canceled)
	ctx.SetRequest(ctx.Request().WithContext(attempt))
	done := make(chan error, 1)
	go func() { done <- newRouter(s).serveModel(rq, endpoint) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream was not reached")
	}
	cancel(errSlotLeaseLost)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("lost lease did not cancel upstream body")
	}
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), `"code":"registry_unavailable"`)
	assert.NotContains(t, rec.Body.String(), `"type":"rate_limit_error"`)
	assert.Zero(t, s.rdb.Exists(context.Background(), slotKey(replica.ID)).Val(), "canceled attempt releases its lease")
	stored := charge(t, s, rq.requestID)
	assert.Equal(t, http.StatusServiceUnavailable, stored.StatusCode, "lease loss is audited once as a 503")
	assert.Equal(t, types.ChargeVoid, stored.Status)
}
