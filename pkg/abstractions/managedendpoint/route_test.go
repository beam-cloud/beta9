package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostedResponsesKeepPlacementInternal(t *testing.T) {
	s := newServiceForTest(t)
	r := newRouter(s)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Locality = "internal-placement"
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	_, err := s.repo.SaveCharge(context.Background(), &types.Charge{
		ID: "req-placement", Status: types.ChargeSettled, AppID: endpoint.Spec.ID, WorkspaceID: "user-ws",
		ReplicaID: replica.ID, AcceptedAt: time.Now(), SettledAt: time.Now(), Work: types.Work{Requests: 1, PromptTokens: 10},
	})
	require.NoError(t, err)
	get := func(path string, handler echo.HandlerFunc) map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		var ctx echo.Context = echo.New().NewContext(httptest.NewRequest(http.MethodGet, path, nil), rec)
		if strings.HasPrefix(path, "/v1/generation") {
			ctx = &auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "user-ws"}, Token: &types.Token{}}}
		}
		require.NoError(t, handler(ctx), "the catalog needs no token")
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		return body
	}
	models := get("/v1/models", r.handleListModels)["data"].([]any)
	require.Len(t, models, 1)
	model := models[0].(map[string]any)
	assert.Equal(t, endpoint.Spec.ID, model["id"])
	assert.Equal(t, true, model["is_ready"])
	assert.Equal(t, "text->text", model["architecture"].(map[string]any)["modality"], "OpenRouter clients read the catalog fields")
	assert.Equal(t, "0.000001", model["pricing"].(map[string]any)["prompt"])
	assert.EqualValues(t, 32768, model["top_provider"].(map[string]any)["context_length"])
	one := get("/v1/models/acme/model", func(c echo.Context) error {
		c.SetParamNames("author", "slug")
		c.SetParamValues("acme", "model")
		return r.handleGetModel(c)
	})
	assert.Equal(t, model, one, "retrieving one model returns the same entry")
	generation := get("/v1/generation?id=req-placement", r.handleGeneration)["data"].(map[string]any)
	assert.Equal(t, "req-placement", generation["id"])
	assert.EqualValues(t, 10, generation["tokens_prompt"])
	for _, record := range []map[string]any{model, generation} {
		for _, field := range []string{"region", "regions", "locality", "datacenters"} {
			assert.NotContains(t, record, field)
		}
	}
}

func TestEndpointAccessIsIndependentOfCatalog(t *testing.T) {
	s := newServiceForTest(t)
	r := newRouter(s)
	endpoint := seedEndpoint(t, s)
	endpoint.Public = false
	user := &auth.AuthInfo{Token: &types.Token{}, Workspace: &types.Workspace{Id: 2, ExternalId: "user-ws", Name: "user"}}
	ctx := context.Background()
	assert.False(t, r.allowed(ctx, endpoint, user), "an endpoint is private without an explicit policy")
	endpoint.AllowedWorkspaces = []string{"user-ws"}
	assert.True(t, r.allowed(ctx, endpoint, user))
	endpoint.AllowedWorkspaces = nil
	endpoint.Public = true
	assert.True(t, r.allowed(ctx, endpoint, user))
	endpoint.Public = false
	user.Workspace.Id = 1
	assert.True(t, r.allowed(ctx, endpoint, user), "the owning admin workspace retains access")
	assert.False(t, r.allowed(ctx, endpoint, nil), "the public catalog hides private models from anonymous callers")
	endpoint.Public = true
	assert.True(t, r.allowed(ctx, endpoint, nil))
}

func TestChooseReservesInflightAtomically(t *testing.T) {
	r := newRouter(newServiceForTest(t))
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}}
	replicas := []*types.EndpointReplica{
		{ID: "replica-a", Address: "a:8000", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}},
		{ID: "replica-b", Address: "b:8000", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}},
	}
	rq := &routeRequest{proto: protocols["chat/completions"]}
	ctx := context.Background()

	// Concurrent selections over the same snapshot: each replica admits at
	// most MaxConcurrency requests, the rest see no capacity.
	var wg sync.WaitGroup
	type reservation struct {
		replica *types.EndpointReplica
		request *routeRequest
	}
	picked := make(chan reservation, 16)
	for i := 0; i < cap(picked); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request := &routeRequest{requestID: uuid.NewString(), proto: rq.proto}
			picked <- reservation{chooseForTest(t, r, ctx, request, endpoint, replicas), request}
		}()
	}
	wg.Wait()
	close(picked)
	counts := map[string]int{}
	owners := map[string]*routeRequest{}
	for held := range picked {
		if held.replica != nil {
			counts[held.replica.ID]++
			owners[held.replica.ID] = held.request
		}
	}
	assert.Equal(t, map[string]int{"replica-a": 1, "replica-b": 1}, counts)
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-a").Load())
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-b").Load())

	// Saturated until a slot is released; releasing frees exactly that replica.
	assert.Nil(t, chooseForTest(t, r, ctx, rq, endpoint, replicas))
	r.releaseReplica(owners[replicas[0].ID], r.state(endpoint.Spec.ID), replicas[0])
	got := chooseForTest(t, r, ctx, rq, endpoint, replicas)
	require.NotNil(t, got)
	assert.Equal(t, "replica-a", got.ID)
	assert.Nil(t, chooseForTest(t, r, ctx, rq, endpoint, replicas))

	// A failed reservation leaves the counter untouched.
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-a").Load())
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-b").Load())

	// Unlimited replicas are never refused.
	unlimited := []*types.EndpointReplica{{ID: "replica-c", Address: "c:8000"}}
	for i := 0; i < 5; i++ {
		require.NotNil(t, chooseForTest(t, r, ctx, rq, endpoint, unlimited))
	}
	assert.Equal(t, int64(5), counter(&r.inflight, "replica-c").Load())
}

// MaxConcurrency is a cluster-wide bound: two gateways sharing Redis cannot
// both take a replica's only slot, and a release on one frees it for the other.
func TestReplicaConcurrencyIsSharedAcrossGateways(t *testing.T) {
	s := newServiceForTest(t)
	a, b := newRouter(s), newRouter(s)
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}}
	replicas := []*types.EndpointReplica{{ID: "replica-a", Address: "a:8000", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}}}
	rq := &routeRequest{requestID: "gateway-a-request", proto: protocols["chat/completions"]}
	rqB := &routeRequest{requestID: "gateway-b-request", proto: rq.proto}
	ctx := context.Background()

	require.NotNil(t, chooseForTest(t, a, ctx, rq, endpoint, replicas))
	assert.Nil(t, chooseForTest(t, b, ctx, rqB, endpoint, replicas), "gateway B sees gateway A's reservation")
	assert.Equal(t, int64(0), counter(&b.inflight, "replica-a").Load(), "a refused reservation leaves B's counter untouched")
	pressure, err := a.state(endpoint.Spec.ID).Pressure(ctx, "replica-a")
	require.NoError(t, err)
	assert.EqualValues(t, 1, pressure.ActiveStreams)

	a.releaseReplica(rq, a.state(endpoint.Spec.ID), replicas[0])
	require.NotNil(t, chooseForTest(t, b, ctx, rqB, endpoint, replicas))
	pressure, _ = a.state(endpoint.Spec.ID).Pressure(ctx, "replica-a")
	assert.EqualValues(t, 1, pressure.ActiveStreams)
}

// TTFT is the time to the first generated output, not to the engine's first
// frame: role-only preambles, empty deltas and usage-only chunks do not count.
func TestGeneratesOutput(t *testing.T) {
	for line, want := range map[string]bool{
		`data: {"choices":[{"delta":{"role":"assistant","content":""}}]}`:        false,
		`data: {"choices":[{"delta":{"role":"assistant"}}]}`:                     false,
		`data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`: false,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`:                false,
		`data: [DONE]`: false,
		`data:`:        false,
		`data: {"choices":[{"delta":{"content":"Hel"}}]}`:                          true,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1"}]}}]}`: true,
		`data: {"choices":[{"text":"Hel","index":0}]}`:                             true,
		`data: {"event":"custom","payload":1}`:                                     false,
	} {
		assert.Equal(t, want, generatesOutput([]byte(line)), line)
	}
}

func chooseForTest(t *testing.T, r *router, ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replicas []*types.EndpointReplica) *types.EndpointReplica {
	t.Helper()
	if rq.requestID == "" {
		copy := *rq
		copy.requestID = uuid.NewString()
		rq = &copy
	}
	picked, err := r.choose(ctx, rq, endpoint, replicas)
	require.NoError(t, err)
	return picked
}

type routeReplicaRepository struct {
	repository.ManagedEndpointRepository
	err        error
	reads      atomic.Int64
	read       chan struct{}
	fleetErr   error
	fleetReads atomic.Int64
}

func (r *routeReplicaRepository) GetFleet(ctx context.Context) (*types.Fleet, error) {
	r.fleetReads.Add(1)
	if r.fleetErr != nil {
		return nil, r.fleetErr
	}
	return r.ManagedEndpointRepository.GetFleet(ctx)
}

func (r *routeReplicaRepository) ListReplicas(ctx context.Context, endpointID string) ([]*types.EndpointReplica, error) {
	r.reads.Add(1)
	if r.read != nil {
		select {
		case r.read <- struct{}{}:
		default:
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.ManagedEndpointRepository.ListReplicas(ctx, endpointID)
}

func coldRouteContext() (*auth.HttpAuthContext, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"acme/model","messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Content-Type", "application/json")
	return &auth.HttpAuthContext{
		Context: echo.New().NewContext(req, rec),
		AuthInfo: &auth.AuthInfo{
			Workspace: &types.Workspace{Id: 2, ExternalId: "user-ws", Name: "user"},
			Token:     &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "tok"},
		},
	}, rec
}

func TestOnDemandRouteRejectsThenServesReadyRetry(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
	}})
	repo := &routeReplicaRepository{ManagedEndpointRepository: s.repo}
	s.repo = repo
	r := newRouter(s)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"completion-1","choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	}))
	t.Cleanup(upstream.Close)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
	}}
	t.Cleanup(transport.CloseIdleConnections)
	s.transports.Store("test-replica", transport)
	ctx, rec := coldRouteContext()
	started := time.Now()
	require.NoError(t, r.handleRoute(ctx))
	assert.Less(t, time.Since(started), 200*time.Millisecond)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	demand, err := s.demand(context.Background(), endpoint.Spec.ID, demandRead, "", 0)
	require.NoError(t, err)
	assert.Zero(t, demand.active, "a rejected request releases its lease immediately")
	assert.True(t, demand.pending, "startup survives the rejected request")
	assert.True(t, demand.warm)
	replica := seedReplica(t, s, endpoint)
	replica.Address = "test-replica"
	replica.Status = types.ReplicaStatusReady
	replica.Capacity.MaxConcurrency = 1
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	ctx, rec = coldRouteContext()
	require.NoError(t, r.handleRoute(ctx))
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"content":"hello"`)
	assert.Contains(t, rec.Body.String(), `"id":"gen-`)
	assert.Equal(t, replica.ID, rec.Header().Get(headerReplicaServed))
	demand, err = s.demand(context.Background(), endpoint.Spec.ID, demandRead, "", 0)
	require.NoError(t, err)
	assert.Zero(t, demand.active, "completion releases the request lease")
	assert.LessOrEqual(t, repo.reads.Load(), int64(4), "rejected requests never poll the replica registry")
}

func TestRejectedRouteDoesNotCreateDemand(t *testing.T) {
	for _, reason := range []string{"unauthenticated", "private", "admission"} {
		t.Run(reason, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
				Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
			}})
			r := newRouter(s)
			ctx, rec := coldRouteContext()
			want := http.StatusUnauthorized
			switch reason {
			case "unauthenticated":
				ctx.AuthInfo.Token = nil
			case "private":
				endpoint.Public = false
				require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
				want = http.StatusForbidden
			case "admission":
				s.config.Routing.PerEndpointConcurrency = 1
				_, err := s.lease(context.Background(), admissionKey(endpoint.Spec.ID), leaseAcquire, "held-by-another-gateway", 1)
				require.NoError(t, err)
				want = http.StatusTooManyRequests
			}
			require.NoError(t, r.handleRoute(ctx))
			assert.Equal(t, want, rec.Code, rec.Body.String())
			keys, err := s.rdb.Exists(context.Background(), "managed_endpoint:demand:"+endpoint.Spec.ID).Result()
			require.NoError(t, err)
			assert.Zero(t, keys, "rejection must create neither an active lease nor an idle marker")
		})
	}
}

func TestOnDemandRouteSharedLimitReturns429(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
	}})
	for i := 0; i < serverlessAdmissionHeadroom; i++ {
		_, err := s.demand(context.Background(), endpoint.Spec.ID, leaseAcquire, "existing-"+strconv.Itoa(i), 0)
		require.NoError(t, err)
	}
	r := newRouter(s)
	s.config.Routing.PerEndpointConcurrency = 2
	ctx, rec := coldRouteContext()
	require.NoError(t, r.handleRoute(ctx))
	assert.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "rate_limit_exceeded")
	demand, err := s.demand(context.Background(), endpoint.Spec.ID, demandRead, "", 0)
	require.NoError(t, err)
	assert.EqualValues(t, serverlessAdmissionHeadroom, demand.active)
	assert.Zero(t, s.rdb.Exists(context.Background(), admissionKey(endpoint.Spec.ID)).Val(), "shared rejection releases the admission lease")
}

func TestReplicaLookupFailureIsNotColdCapacity(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, err: errors.New("registry read failed")}
	r := newRouter(s)
	ctx, _ := coldRouteContext()
	rq := &routeRequest{ctx: ctx, auth: ctx.AuthInfo, models: []string{endpoint.Spec.ID}, route: types.EndpointRouteChatCompletions, startedAt: time.Now(), serverless: true}
	resolved, rerr := r.resolveEndpoint(context.Background(), rq)
	assert.Nil(t, resolved)
	assert.Equal(t, errRegistry, rerr)
	started := time.Now()
	replica, rerr := r.pick(context.Background(), rq, endpoint, nil)
	assert.Nil(t, replica)
	assert.Equal(t, errRegistry, rerr)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestReplicaPickHonorsCancellationAndDrain(t *testing.T) {
	for _, reason := range []string{"client", "gateway"} {
		t.Run(reason, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			if reason == "gateway" {
				s.drainCtx = ctx
				ctx = context.Background()
			}
			cancel()
			_, rerr := newRouter(s).pick(ctx, &routeRequest{serverless: true}, endpoint, nil)
			require.NotNil(t, rerr)
			if reason == "client" {
				assert.Equal(t, "client_closed", rerr.Code)
			} else {
				assert.Equal(t, "gateway_draining", rerr.Code)
			}
			keys, err := s.rdb.Exists(context.Background(), "managed_endpoint:demand:"+endpoint.Spec.ID).Result()
			require.NoError(t, err)
			assert.Zero(t, keys)
		})
	}
}

func TestHotModelCapacityRejectionDoesNotWait(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	started := time.Now()
	replica, rerr := newRouter(s).pick(context.Background(), &routeRequest{startedAt: started}, endpoint, nil)
	assert.Nil(t, replica)
	require.NotNil(t, rerr)
	assert.Equal(t, http.StatusTooManyRequests, rerr.Status)
	assert.Equal(t, "rate_limit_exceeded", rerr.Code)
	assert.Less(t, time.Since(started), 200*time.Millisecond, "capacity rejections do not queue")
}

func TestModelCatalogKeepsPlacementModeInternal(t *testing.T) {
	for _, serverless := range []bool{false, true} {
		t.Run(strconv.FormatBool(serverless), func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
				Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: serverless}},
			}})
			repo := &routeReplicaRepository{ManagedEndpointRepository: s.repo}
			s.repo = repo
			ctx, rec := coldRouteContext()
			require.NoError(t, newRouter(s).handleListModels(ctx))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var body struct {
				Data []map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Data, 1)
			assert.Equal(t, false, body.Data[0]["is_ready"])
			assert.NotContains(t, body.Data[0], "serverless")
			assert.Zero(t, repo.fleetReads.Load(), "public catalog does not need placement policy")
		})
	}
}

func TestModelCatalogDoesNotDependOnPlacementConfig(t *testing.T) {
	s := newServiceForTest(t)
	seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, fleetErr: errors.New("fleet unavailable")}
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleListModels(ctx))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "serverless")
}

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
		ok, err := s.lease(ctx, slotKey("replica"), leaseAcquire, "request-a", 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	assert.EqualValues(t, 1, s.rdb.ZCard(ctx, slotKey("replica")).Val())
	ok, err := s.lease(ctx, slotKey("replica"), leaseAcquire, "request-b", 1)
	require.NoError(t, err)
	assert.False(t, ok)
	server.SetTime(now.Add(leaseTTL + time.Second))
	ok, err = s.lease(ctx, slotKey("replica"), leaseAcquire, "request-b", 1)
	require.NoError(t, err)
	assert.True(t, ok, "expired request releases its own slot")
	ok, err = s.lease(ctx, slotKey("replica"), leaseRenew, "request-a", 1)
	require.ErrorIs(t, err, errLeaseLost)
	assert.False(t, ok, "old request cannot resurrect its lease")
	ok, err = s.lease(ctx, slotKey("replica"), leaseRelease, "request-a", 1)
	require.NoError(t, err)
	assert.False(t, ok, "late release cannot touch request-b")
	assert.Equal(t, []string{"request-b"}, s.rdb.ZRange(ctx, slotKey("replica"), 0, -1).Val())
	ok, err = s.lease(ctx, slotKey("replica"), leaseRelease, "request-b", 1)
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
		ok, err := s.lease(ctx, slotKey(replica.ID), leaseRenew, "running", 0)
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
		server.SetTime(now.Add(time.Duration(i) * leaseRenewInterval))
		server.FastForward(leaseRenewInterval)
		ok, err = s.lease(ctx, slotKey(replica.ID), leaseRenew, requestA.requestID, 0)
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
			ok, err := s.lease(ctx, slotKey("replica"), leaseAcquire, "old", 1)
			require.NoError(t, err)
			require.True(t, ok)
			ticks := make(chan time.Time, 1)
			attempt, stop := newRouter(s).renewSlot(ctx, "replica", "old", ticks)
			defer stop()
			if failure == "expiry" {
				server.SetTime(now.Add(leaseTTL + time.Second))
				ok, err := s.lease(ctx, slotKey("replica"), leaseAcquire, "new", 1)
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
			assert.ErrorIs(t, context.Cause(attempt), errLeaseLost)
			stop()
			if failure == "expiry" {
				_, err := s.lease(ctx, slotKey("replica"), leaseRelease, "old", 0)
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
			ok, err := s.lease(client, slotKey("replica"), leaseAcquire, "request", 1)
			require.NoError(t, err)
			require.True(t, ok)
			ticks := make(chan time.Time, 1)
			attempt, stop := newRouter(s).renewSlot(client, "replica", "request", ticks)
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
			_, err = s.lease(context.Background(), slotKey("replica"), leaseRelease, "request", 0)
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
	rq.charge = &types.Charge{ID: rq.requestID, WorkspaceID: ctx.AuthInfo.Workspace.ExternalId, AppID: endpoint.Spec.ID, Pricing: endpoint.Pricing, AcceptedAt: rq.startedAt}
	attempt, cancel := context.WithCancelCause(ctx.Request().Context())
	defer cancel(context.Canceled)
	ctx.SetRequest(ctx.Request().WithContext(attempt))
	done := make(chan error, 1)
	go func() { done <- newRouter(s).serve(rq, endpoint) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream was not reached")
	}
	cancel(errLeaseLost)
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
