package managedendpoint

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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
	require.NoError(t, s.repo.SaveGeneration(context.Background(), &types.EventEndpointRouteSchema{
		RequestID: "req-placement", Model: endpoint.Spec.ID, WorkspaceID: "user-ws",
		Locality: replica.Locality, Timestamp: time.Now(), PromptTokens: 10,
	}, generationTTL))
	get := func(path string, handler echo.HandlerFunc) map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		ctx := &auth.HttpAuthContext{
			Context:  echo.New().NewContext(httptest.NewRequest(http.MethodGet, path, nil), rec),
			AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "user-ws"}, Token: &types.Token{}},
		}
		require.NoError(t, handler(ctx))
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
	endpoint.Spec.Public = false
	user := &auth.AuthInfo{Token: &types.Token{}, Workspace: &types.Workspace{Id: 2, ExternalId: "user-ws", Name: "user"}}
	ctx := context.Background()
	assert.False(t, r.allowed(ctx, endpoint, user), "an endpoint is private without an explicit policy")
	endpoint.Spec.AllowedWorkspaces = []string{"user-ws"}
	assert.True(t, r.allowed(ctx, endpoint, user))
	endpoint.Spec.AllowedWorkspaces = nil
	endpoint.Spec.Public = true
	assert.True(t, r.allowed(ctx, endpoint, user))
	endpoint.Spec.Public = false
	user.Workspace.Id = 1
	assert.True(t, r.allowed(ctx, endpoint, user), "the owning admin workspace retains access")
}

func TestChooseReservesInflightAtomically(t *testing.T) {
	r := newRouter(newServiceForTest(t))
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}}
	replicas := []*types.EndpointReplica{
		{ID: "replica-a", Address: "a:8000", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}},
		{ID: "replica-b", Address: "b:8000", Capacity: types.ReplicaCapacity{MaxConcurrency: 1}},
	}
	rq := &routeRequest{adapter: adapters[types.EndpointRouteChatCompletions]}
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
			request := &routeRequest{requestID: uuid.NewString(), adapter: rq.adapter}
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
	rq := &routeRequest{requestID: "gateway-a-request", adapter: adapters[types.EndpointRouteChatCompletions]}
	rqB := &routeRequest{requestID: "gateway-b-request", adapter: rq.adapter}
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
