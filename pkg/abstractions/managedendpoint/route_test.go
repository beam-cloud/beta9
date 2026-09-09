package managedendpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeCostMicroUSD(t *testing.T) {
	pricing := types.Pricing{
		PromptTokens:       "0.0000005",  // $0.50 / 1M
		CompletionTokens:   "0.0000015",  // $1.50 / 1M
		CachedPromptTokens: "0.00000025", // $0.25 / 1M
		Request:            "0.001",
	}
	cost, err := computeCostMicroUSD(pricing, Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000, CachedTokens: 400_000, Requests: 1, Found: true})
	require.NoError(t, err)
	// prompt: 600k uncached * 0.5 + 400k cached * 0.25 = 0.30 + 0.10; completion 1.50; request 0.001
	assert.Equal(t, int64(1_901_000), cost)

	// No cached rate: cached tokens bill at the prompt rate.
	cost, err = computeCostMicroUSD(types.Pricing{PromptTokens: "0.000001"}, Usage{PromptTokens: 100, CachedTokens: 50, Found: true})
	require.NoError(t, err)
	assert.Equal(t, int64(100), cost)

	cost, err = computeCostMicroUSD(types.Pricing{}, Usage{PromptTokens: 1e9})
	require.NoError(t, err)
	assert.Equal(t, int64(0), cost)

	// Rounding half-up at micro-dollar precision.
	cost, err = computeCostMicroUSD(types.Pricing{Image: "0.0000005"}, Usage{Images: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(1), cost)
}

func TestUsageExtraction(t *testing.T) {
	body := []byte(`{"id":"x","usage":{"prompt_tokens":12,"completion_tokens":30,"total_tokens":42,"prompt_tokens_details":{"cached_tokens":8}}}`)
	u := tokenUsage(body)
	assert.True(t, u.Found)
	assert.Equal(t, int64(12), u.PromptTokens)
	assert.Equal(t, int64(30), u.CompletionTokens)
	assert.Equal(t, int64(8), u.CachedTokens)

	assert.False(t, tokenUsage([]byte(`{"choices":[]}`)).Found)

	img := imageUsage([]byte(`{"data":[{"b64_json":"a"},{"b64_json":"b"}]}`))
	assert.True(t, img.Found)
	assert.Equal(t, int64(2), img.Images)

	u, ok := sseUsage([]byte(`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":7}}` + "\n"))
	assert.True(t, ok)
	assert.Equal(t, int64(7), u.CompletionTokens)
	_, ok = sseUsage([]byte("data: [DONE]\n"))
	assert.False(t, ok)
	_, ok = sseUsage([]byte(`data: {"choices":[{"delta":{"content":"hi"}}],"usage":null}` + "\n"))
	assert.False(t, ok)
}

func TestForceIncludeUsage(t *testing.T) {
	payload := map[string]any{"model": "m", "stream": true}
	assert.True(t, forceIncludeUsage(payload))
	assert.Equal(t, map[string]any{"include_usage": true}, payload["stream_options"])

	payload = map[string]any{"model": "m"}
	assert.False(t, forceIncludeUsage(payload))
	_, ok := payload["stream_options"]
	assert.False(t, ok)
}

func TestRouteFromPath(t *testing.T) {
	route, id, ok := routeFromPath("/v1", "/v1/chat/completions")
	require.True(t, ok)
	assert.Equal(t, types.EndpointRouteChatCompletions, route)
	assert.Empty(t, id)

	route, id, ok = routeFromPath("/v1", "/v1/models/acme/tool-v2/invoke")
	require.True(t, ok)
	assert.Equal(t, types.EndpointRouteInvoke, route)
	assert.Equal(t, "acme/tool-v2", id)

	_, _, ok = routeFromPath("/v1", "/v1/audio/speech")
	assert.False(t, ok)
}

func TestDecorateJSON(t *testing.T) {
	body := []byte(`{"id":"chatcmpl-upstream","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	out := decorateJSON(body, "gen-abc", Usage{Found: true, PromptTokens: 1, CompletionTokens: 1}, 2_500)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "gen-abc", payload["id"], "the engine's id is replaced by the gateway generation id")
	assert.Equal(t, providerName, payload["provider"])
	assert.InDelta(t, 0.0025, payload["usage"].(map[string]any)["cost"], 1e-9)

	// Non-object bodies pass through untouched.
	assert.Equal(t, []byte(`[1,2]`), decorateJSON([]byte(`[1,2]`), "gen", Usage{}, 0))
}

func TestChooseReservesInflightAtomically(t *testing.T) {
	r := &router{s: &Service{}, states: map[string]*llmroute.State{}}
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
	picked := make(chan *types.EndpointReplica, 16)
	for i := 0; i < cap(picked); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			picked <- r.choose(ctx, rq, endpoint, replicas)
		}()
	}
	wg.Wait()
	close(picked)
	counts := map[string]int{}
	for replica := range picked {
		if replica != nil {
			counts[replica.ID]++
		}
	}
	assert.Equal(t, map[string]int{"replica-a": 1, "replica-b": 1}, counts)
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-a").Load())
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-b").Load())

	// Saturated until a slot is released; releasing frees exactly that replica.
	assert.Nil(t, r.choose(ctx, rq, endpoint, replicas))
	r.releaseReplica(rq, r.state(endpoint.Spec.ID), replicas[0])
	got := r.choose(ctx, rq, endpoint, replicas)
	require.NotNil(t, got)
	assert.Equal(t, "replica-a", got.ID)
	assert.Nil(t, r.choose(ctx, rq, endpoint, replicas))

	// A failed reservation leaves the counter untouched.
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-a").Load())
	assert.Equal(t, int64(1), counter(&r.inflight, "replica-b").Load())

	// Unlimited replicas are never refused.
	unlimited := []*types.EndpointReplica{{ID: "replica-c", Address: "c:8000"}}
	for i := 0; i < 5; i++ {
		require.NotNil(t, r.choose(ctx, rq, endpoint, unlimited))
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
	rq := &routeRequest{adapter: adapters[types.EndpointRouteChatCompletions]}
	ctx := context.Background()

	require.NotNil(t, a.choose(ctx, rq, endpoint, replicas))
	assert.Nil(t, b.choose(ctx, rq, endpoint, replicas), "gateway B sees gateway A's reservation")
	assert.Equal(t, int64(0), counter(&b.inflight, "replica-a").Load(), "a refused reservation leaves B's counter untouched")
	pressure, err := a.state(endpoint.Spec.ID).Pressure(ctx, "replica-a")
	require.NoError(t, err)
	assert.EqualValues(t, 1, pressure.ActiveStreams)

	a.releaseReplica(rq, a.state(endpoint.Spec.ID), replicas[0])
	require.NotNil(t, b.choose(ctx, rq, endpoint, replicas))
	pressure, _ = a.state(endpoint.Spec.ID).Pressure(ctx, "replica-a")
	assert.EqualValues(t, 1, pressure.ActiveStreams)
}

func TestStampSSE(t *testing.T) {
	stamped := stampSSE([]byte("data: {\"id\":\"chatcmpl-1\",\"choices\":[]}\n"), "gen-abc")
	var chunk map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimPrefix(stamped, []byte("data: ")), &chunk))
	assert.Equal(t, "gen-abc", chunk["id"])
	assert.Equal(t, []any{}, chunk["choices"])
	assert.True(t, bytes.HasSuffix(stamped, []byte("\n")))

	// Chunks without an id, non-JSON payloads and [DONE] pass through untouched.
	for _, line := range []string{"data: {\"choices\":[]}\n", "data: [DONE]\n", "data: not json\n", "data:\n"} {
		assert.Equal(t, []byte(line), stampSSE([]byte(line), "gen-abc"), line)
	}
}

func TestUpstreamQueryDropsAuthToken(t *testing.T) {
	q := url.Values{"auth_token": {"secret"}, "foo": {"bar"}}
	assert.Equal(t, "foo=bar", upstreamQuery(q))
	assert.Empty(t, upstreamQuery(url.Values{"auth_token": {"secret"}}))
}

func TestSetModelRewritesUpstreamBody(t *testing.T) {
	rq := &routeRequest{body: []byte(`{"model":"acme/first","models":["acme/first","acme/second"],"stream":true}`)}
	rq.setModel("acme/second")
	var payload map[string]any
	require.NoError(t, json.Unmarshal(rq.body, &payload))
	assert.Equal(t, "acme/second", payload["model"])
	_, hasModels := payload["models"]
	assert.False(t, hasModels, "the fallback list never reaches the engine")
	assert.Equal(t, true, payload["stream"])

	// A body already naming the selected model is left byte-for-byte alone.
	original := []byte(`{"model": "acme/model", "n": 1}`)
	rq = &routeRequest{body: original}
	rq.setModel("acme/model")
	assert.Equal(t, original, rq.body)

	// A path model with a body that names none: the engine sees the endpoint.
	rq = &routeRequest{body: []byte(`{"input":"hi"}`)}
	rq.setModel("acme/embed")
	require.NoError(t, json.Unmarshal(rq.body, &payload))
	assert.Equal(t, "acme/embed", payload["model"])

	// Non-object bodies (multipart) are left alone.
	rq = &routeRequest{body: []byte("--boundary\r\n")}
	rq.setModel("acme/img")
	assert.Equal(t, []byte("--boundary\r\n"), rq.body)
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
		`data: {"event":"custom","payload":1}`:                                     true,
	} {
		assert.Equal(t, want, generatesOutput([]byte(line)), line)
	}
}
