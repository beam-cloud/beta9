package managedendpoint

import (
	"context"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
