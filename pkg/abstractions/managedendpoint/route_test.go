package managedendpoint

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

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
	body := []byte(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	out := decorateJSON(body, "gen-abc", Usage{Found: true, PromptTokens: 1, CompletionTokens: 1}, 2_500)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "gen-abc", payload["id"])
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
	r.releaseReplica(replicas[0])
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

func TestEnqueueUsageBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &router{s: &Service{ctx: ctx}, usageQueue: make(chan types.EventEndpointRouteSchema, 1)}
	r.usageQueue <- types.EventEndpointRouteSchema{RequestID: "first"}

	// A full queue waits for the drainer instead of dropping.
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.enqueueUsage(types.EventEndpointRouteSchema{RequestID: "second"})
	}()
	select {
	case <-done:
		t.Fatal("enqueue returned while the queue was full")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, "first", (<-r.usageQueue).RequestID)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue did not complete after the queue drained")
	}
	assert.Equal(t, "second", (<-r.usageQueue).RequestID)
	assert.Equal(t, int64(0), r.usageDropped.Load())

	// On shutdown nothing will drain the queue; drop promptly and count it.
	r.usageQueue <- types.EventEndpointRouteSchema{RequestID: "third"}
	cancel()
	started := time.Now()
	r.enqueueUsage(types.EventEndpointRouteSchema{RequestID: "fourth"})
	assert.Less(t, time.Since(started), usageEnqueueTimeout)
	assert.Equal(t, int64(1), r.usageDropped.Load())
}

func TestEvaluateRollout(t *testing.T) {
	ready := []*types.EndpointReplica{{Status: types.ReplicaStatusReady, Capacity: types.ReplicaCapacity{TPOTMs: 20, DecodeTokensPerSec: 1000}}}
	slow := []*types.EndpointReplica{{Status: types.ReplicaStatusReady, Capacity: types.ReplicaCapacity{TPOTMs: 40, DecodeTokensPerSec: 400}}}
	th := types.RolloutThresholds{}

	promote, reason := evaluateRollout(nil, nil, ready, nil, rolloutConfig(th))
	assert.False(t, promote, reason)

	promote, _ = evaluateRollout(nil, nil, ready, ready, rolloutConfig(th))
	assert.True(t, promote)

	active := &types.RouteMetrics{Requests: 200, Errors: 1, TTFTSumMs: 20_000, TTFTCount: 200}
	bad := &types.RouteMetrics{Requests: 50, Errors: 5, TTFTSumMs: 5_000, TTFTCount: 50}
	promote, reason = evaluateRollout(active, bad, ready, ready, rolloutConfig(th))
	assert.False(t, promote)
	assert.Contains(t, reason, "error rate")

	slowTTFT := &types.RouteMetrics{Requests: 50, TTFTSumMs: 10_000, TTFTCount: 50} // 200ms vs 100ms
	promote, reason = evaluateRollout(active, slowTTFT, ready, ready, rolloutConfig(th))
	assert.False(t, promote)
	assert.Contains(t, reason, "TTFT")

	same := &types.RouteMetrics{Requests: 50, TTFTSumMs: 5_000, TTFTCount: 50}
	promote, reason = evaluateRollout(active, same, ready, slow, rolloutConfig(th))
	assert.False(t, promote)
	assert.Contains(t, reason, "TPOT")

	promote, _ = evaluateRollout(active, same, ready, ready, rolloutConfig(th))
	assert.True(t, promote)
}
