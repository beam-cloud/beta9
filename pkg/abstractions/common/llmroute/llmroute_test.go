package llmroute

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestState(t *testing.T) *State {
	t.Helper()
	_, state := newTestStateServer(t)
	return state
}

func newTestStateServer(t *testing.T) (*miniredis.Miniredis, *State) {
	t.Helper()
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	return server, NewState(rdb, "pod:workspace:stub")
}

func inspect(t *testing.T, method, path, body string, headers map[string]string) (*RequestInfo, *http.Request) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	info, err := Inspect(req, path, InspectOptions{DefaultModel: "configured"})
	require.NoError(t, err)
	return info, req
}

func TestInspectParsesOpenAIChatAndRestoresBody(t *testing.T) {
	body := `{"model":"zai-org/GLM-4.5-Air-FP8","user":"session-1","stream":true,"max_tokens":64,"messages":[{"role":"system","content":"You are terse."},{"role":"user","content":[{"type":"text","text":"Explain prefix caching for LLM serving."}]}]}`
	info, req := inspect(t, http.MethodPost, "/v1/chat/completions", body, map[string]string{"X-Request-ID": "req-1"})
	require.Equal(t, "zai-org/GLM-4.5-Air-FP8", info.Model)
	require.Equal(t, "req-1", info.RequestID)
	require.True(t, info.Stream)
	require.EqualValues(t, 64, info.OutputTokens)
	require.Equal(t, "session-1", info.SessionKey)
	require.NotEmpty(t, info.SessionHash)
	require.Equal(t, info.SessionHash, info.AffinityKey)
	require.NotEmpty(t, info.PrefixHash)
	require.NotEmpty(t, info.PrefixBlocks)
	require.EqualValues(t, estimateTokens("You are terse.\nExplain prefix caching for LLM serving."), info.PromptTokens)
	require.Equal(t, info.PromptTokens+info.OutputTokens, info.TokenPressure)

	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(restored))
}

func TestInspectFallbacks(t *testing.T) {
	info, _ := inspect(t, http.MethodPost, "/v1/completions", `{"prompt":"hello"}`, map[string]string{"X-Beam-LLM-Session": "thread-123"})
	require.Equal(t, "configured", info.Model)
	require.Equal(t, "thread-123", info.SessionKey)
	require.EqualValues(t, defaultOutputTokens, info.OutputTokens)

	info, _ = inspect(t, http.MethodGet, "/v1/models", "", nil)
	require.EqualValues(t, 1, info.PromptTokens)
	require.EqualValues(t, 1+defaultOutputTokens, info.TokenPressure)

	// Oversized bodies are hashed from the inspected head and fully restored.
	big := strings.Repeat("x", int(maxInspectBytes)+100)
	info, req := inspect(t, http.MethodPost, "/v1/completions", big, nil)
	require.EqualValues(t, maxInspectBytes/4, info.PromptTokens)
	require.Equal(t, info.PrefixHash, info.AffinityKey)
	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, big, string(restored))
}

func TestPrefixBlockHashesShareLeadingBlocks(t *testing.T) {
	shared := strings.Repeat("shared system prompt and retrieval context ", 30)
	a := prefixBlockHashes("model", shared+"first question")
	b := prefixBlockHashes("model", shared+"second question")
	require.NotEmpty(t, a)
	require.NotEqual(t, a, b)
	require.Equal(t, a[0], b[0], "expected shared prefix blocks")
	require.NotEqual(t, prefixHash("model", shared+"first question"), prefixHash("model", shared+"second question"))
}

func TestSelectAffinityAndLoad(t *testing.T) {
	session := &RequestInfo{AffinityKey: "session", SessionHash: "session", PrefixHash: "session"}
	plain := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	busy := Pressure{ActiveStreams: 1, TokenPressure: 512}
	overloaded := Pressure{ActiveStreams: 10, TokenPressure: 4096}
	cases := []struct {
		name       string
		info       *RequestInfo
		candidates []Candidate
		affinity   Affinity
		wantID     string
		wantReason string
	}{
		{"exact prefix affinity when balanced", &RequestInfo{AffinityKey: "affinity", PrefixHash: "affinity"}, []Candidate{{ID: "a"}, {ID: "b"}}, Affinity{ExactID: "b"}, "b", "prefix_affinity"},
		{"session affinity", session, []Candidate{{ID: "a"}, {ID: "b"}}, Affinity{ExactID: "a", ExactIsSession: true}, "a", "session_affinity"},
		{"spills busy affinity target", session, []Candidate{{ID: "a", Pressure: busy}, {ID: "b"}}, Affinity{ExactID: "a", ExactIsSession: true}, "b", "least_pressure"},
		{"ignores affinity when imbalanced", session, []Candidate{{ID: "a", Pressure: overloaded}, {ID: "b"}}, Affinity{ExactID: "a", ExactIsSession: true}, "b", "load_imbalance"},
		{"prefix block affinity", plain, []Candidate{{ID: "a"}, {ID: "b"}}, Affinity{PrefixMatches: map[string]int{"b": 3}}, "b", "prefix_block_affinity"},
		{"engine metrics pressure", plain, []Candidate{
			{ID: "a", Engine: EngineMetrics{WaitingRequests: 3, TTFTMs: 700}},
			{ID: "b", Engine: EngineMetrics{DecodeTokensPerSecond: 300, PrefixCacheHitMilli: 700}},
		}, Affinity{}, "b", "power_of_two_load"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var selector Selector
			selection, ok := selector.Select(tc.candidates, tc.affinity, tc.info)
			require.True(t, ok)
			require.Equal(t, tc.wantID, selection.Candidate.ID)
			require.Equal(t, tc.wantReason, selection.Reason)
			require.Equal(t, tc.wantReason, tc.info.RouteReason)
			require.Equal(t, tc.affinity.PrefixMatches[tc.wantID], tc.info.PrefixCacheMatches)
		})
	}
}

func TestSelectSpreadTieBreakAndEdgeCases(t *testing.T) {
	var selector Selector
	info := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	for i := 0; ; i++ {
		require.Less(t, i, 1000, "no prefix prefers container-b")
		info.AffinityKey = "prefix-" + strconv.Itoa(i)
		info.PrefixHash = info.AffinityKey
		if spreadScore("container-b", info.AffinityKey) < spreadScore("container-a", info.AffinityKey) {
			break
		}
	}

	selection, ok := selector.Select([]Candidate{{ID: "container-a"}, {ID: "container-b"}}, Affinity{}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "least_pressure", selection.Reason)

	selection, ok = selector.Select([]Candidate{{ID: "container-a"}, {ID: "container-b", Connections: 1}}, Affinity{}, info)
	require.True(t, ok)
	require.Equal(t, "container-a", selection.Candidate.ID)

	_, ok = selector.Select(nil, Affinity{}, nil)
	require.False(t, ok)
	selection, ok = selector.Select([]Candidate{{ID: "only", Payload: 42}}, Affinity{}, nil)
	require.True(t, ok)
	require.Equal(t, "only", selection.Candidate.ID)
	require.Equal(t, 42, selection.Candidate.Payload)
}

func TestSelectWithoutRequestInfoRotatesEqualReplicas(t *testing.T) {
	var selector Selector
	candidates := []Candidate{{ID: "container-a"}, {ID: "container-b"}, {ID: "container-c"}}
	seen := map[string]int{}
	for i := 0; i < 300; i++ {
		selection, ok := selector.Select(candidates, Affinity{}, nil)
		require.True(t, ok)
		seen[selection.Candidate.ID]++
	}
	require.Len(t, seen, 3, "equal-load replicas should all be selected: %v", seen)
	for id, n := range seen {
		require.Greater(t, n, 30, "replica %s starved: %v", id, seen)
	}

	// Load still dominates the jitter.
	for i := 0; i < 50; i++ {
		selection, ok := selector.Select([]Candidate{{ID: "container-a", Connections: 1}, {ID: "container-b"}}, Affinity{}, nil)
		require.True(t, ok)
		require.Equal(t, "container-b", selection.Candidate.ID)
	}
}

func TestParsePrometheusSamplesIgnoresTimestampsAndLabelSpaces(t *testing.T) {
	body := "vllm:num_requests_running{model_name=\"qwen\"} 3 1725840000000\n" +
		"vllm:num_requests_waiting 2 1725840000000\n" +
		"vllm:generation_tokens_total{model_name=\"a b\",x=\"y\"} 40\n" +
		"vllm:generation_tokens_total{model_name=\"c\"} 2\n" +
		"vllm:time_to_first_token_seconds_bucket{le=\"+Inf\"} 7\n" +
		"malformed\n"
	samples := parsePrometheusSamples([]byte(body))
	require.EqualValues(t, 3, samples["vllm:num_requests_running"])
	require.EqualValues(t, 2, samples["vllm:num_requests_waiting"])
	require.EqualValues(t, 42, samples["vllm:generation_tokens_total"])
	require.EqualValues(t, 7, samples["vllm:time_to_first_token_seconds_bucket"])
	require.NotContains(t, samples, "malformed")

	got, found := engineMetricsFromPrometheus([]byte(body), EngineMetrics{}, time.Unix(100, 0))
	require.True(t, found)
	require.EqualValues(t, 3, got.RunningRequests)
	require.EqualValues(t, 2, got.WaitingRequests)
}

func TestEngineMetricsFromPrometheusIdleEngineIsStillData(t *testing.T) {
	now := time.Unix(100, 0)
	got, found := engineMetricsFromPrometheus([]byte("vllm:num_requests_running{model_name=\"qwen\"} 0\nvllm:num_requests_waiting{model_name=\"qwen\"} 0\n"), EngineMetrics{RunningRequests: 5}, now)
	require.True(t, found, "an all-zero scrape from an idle engine must replace stale data")
	require.EqualValues(t, 0, got.RunningRequests)

	_, found = engineMetricsFromPrometheus([]byte("# just comments\nprocess_cpu_seconds_total 12\n"), EngineMetrics{}, now)
	require.False(t, found)
	_, found = engineMetricsFromPrometheus(nil, EngineMetrics{}, now)
	require.False(t, found)
}

func TestEngineMetricsFromPrometheusParsesSGLang(t *testing.T) {
	now := time.Unix(100, 0)
	previous := EngineMetrics{
		GenerationTokensTotal: 100, PromptTokensTotal: 50,
		TTFTSumSeconds: 1.0, TTFTCount: 4, TPOTSumSeconds: 0.4, TPOTCount: 10,
		UpdatedAtUnixMs: now.Add(-5 * time.Second).UnixMilli(),
	}
	body := `# HELP sglang:num_running_reqs The number of running requests.
sglang:num_running_reqs{model_name="qwen"} 2.0
sglang:num_queue_reqs{model_name="qwen"} 1.0
sglang:token_usage{model_name="qwen"} 0.92
sglang:cache_hit_rate{model_name="qwen"} 0.8
sglang:prompt_tokens_total{model_name="qwen"} 110.0
sglang:generation_tokens_total{model_name="qwen"} 250.0
sglang:time_to_first_token_seconds_sum{model_name="qwen"} 1.6
sglang:time_to_first_token_seconds_count{model_name="qwen"} 6.0
sglang:inter_token_latency_seconds_sum{model_name="qwen"} 0.7
sglang:inter_token_latency_seconds_count{model_name="qwen"} 20.0
sglang:e2e_request_latency_seconds_sum{model_name="qwen"} 12.0
sglang:e2e_request_latency_seconds_count{model_name="qwen"} 6.0
`
	got, found := engineMetricsFromPrometheus([]byte(body), previous, now)
	require.True(t, found)
	require.EqualValues(t, 2, got.RunningRequests)
	require.EqualValues(t, 1, got.WaitingRequests)
	require.EqualValues(t, 920, got.GPUCacheUsageMilli)
	require.EqualValues(t, 800, got.PrefixCacheHitMilli)
	require.EqualValues(t, 300, got.TTFTMs)
	require.EqualValues(t, 30, got.TPOTMs)
	require.EqualValues(t, 30, got.DecodeTokensPerSecond)
	require.EqualValues(t, 12, got.PromptTokensPerSecond)

	// Older SGLang builds expose TPOT under the vLLM-style name.
	got, found = engineMetricsFromPrometheus([]byte("sglang:time_per_output_token_seconds_sum 0.5\nsglang:time_per_output_token_seconds_count 10\n"), EngineMetrics{}, now)
	require.True(t, found)
	require.EqualValues(t, 50, got.TPOTMs)
}

func TestEngineMetricsFromPrometheusUsesVLLMDeltas(t *testing.T) {
	now := time.Unix(100, 0)
	previous := EngineMetrics{
		GenerationTokensTotal: 100, PromptTokensTotal: 50,
		PrefixCacheHitsTotal: 25, PrefixCacheQueriesTotal: 50,
		TTFTSumSeconds: 1.0, TTFTCount: 4, TPOTSumSeconds: 0.4, TPOTCount: 10,
		UpdatedAtUnixMs: now.Add(-5 * time.Second).UnixMilli(),
	}
	body := `# HELP vllm:num_requests_running running
	vllm:num_requests_running{model_name="qwen"} 2
	vllm:num_requests_waiting{model_name="qwen"} 1
	vllm:kv_cache_usage_perc{model_name="qwen"} 0.92
	vllm:prefix_cache_hits_total{model_name="qwen"} 85
	vllm:prefix_cache_queries_total{model_name="qwen"} 125
	vllm:generation_tokens_total{model_name="qwen"} 250
	vllm:prompt_tokens_total{model_name="qwen"} 110
	vllm:time_to_first_token_seconds_sum{model_name="qwen"} 1.6
vllm:time_to_first_token_seconds_count{model_name="qwen"} 6
vllm:time_per_output_token_seconds_sum{model_name="qwen"} 0.7
vllm:time_per_output_token_seconds_count{model_name="qwen"} 20
`
	got, found := engineMetricsFromPrometheus([]byte(body), previous, now)
	require.True(t, found)
	require.EqualValues(t, 2, got.RunningRequests)
	require.EqualValues(t, 1, got.WaitingRequests)
	require.EqualValues(t, 920, got.GPUCacheUsageMilli)
	require.EqualValues(t, 800, got.PrefixCacheHitMilli)
	require.EqualValues(t, 300, got.TTFTMs)
	require.EqualValues(t, 30, got.TPOTMs)
	require.EqualValues(t, 30, got.DecodeTokensPerSecond)
	require.EqualValues(t, 12, got.PromptTokensPerSecond)
}

func TestStatePressureLifecycle(t *testing.T) {
	server, state := newTestStateServer(t)
	ctx := context.Background()

	require.NoError(t, state.AddPressure(ctx, "container-a", 1, 150))
	for _, target := range []string{PressureTargetTotal, "container-a"} {
		got, err := state.Pressure(ctx, target)
		require.NoError(t, err)
		require.Equal(t, Pressure{ActiveStreams: 1, TokenPressure: 150}, got)
	}

	require.NoError(t, state.AddPressure(ctx, "container-a", -1, -150))
	total, err := state.Pressure(ctx, PressureTargetTotal)
	require.NoError(t, err)
	require.Equal(t, Pressure{}, total)

	// A long stream keeps the lease alive: every increment and decrement
	// refreshes the TTL, so a concurrent stream's exit cannot expire it.
	key := state.key("pressure", "container-a")
	require.NoError(t, state.AddPressure(ctx, "container-a", 1, 100))
	require.NoError(t, state.AddPressure(ctx, "container-a", 1, 100))
	server.FastForward(pressureTTL / 2)
	require.NoError(t, state.AddPressure(ctx, "container-a", -1, -100))
	require.InDelta(t, pressureTTL, server.TTL(key), float64(time.Second), "decrement must refresh the lease")
	got, err := state.Pressure(ctx, "container-a")
	require.NoError(t, err)
	require.Equal(t, Pressure{ActiveStreams: 1, TokenPressure: 100}, got)

	// A stream that outlives the lease finds a fresh hash owned by newer
	// requests; its decrement floors at zero instead of corrupting their load.
	server.FastForward(pressureTTL + time.Second)
	require.False(t, server.Exists(key))
	require.NoError(t, state.AddPressure(ctx, "container-a", -1, -100))
	for _, target := range []string{PressureTargetTotal, "container-a"} {
		got, err := state.Pressure(ctx, target)
		require.NoError(t, err)
		require.Equal(t, Pressure{}, got, "stale decrement must not go negative")
	}
	require.NoError(t, state.AddPressure(ctx, "container-b", 1, 50))
	require.NoError(t, state.AddPressure(ctx, "container-a", -1, -100))
	total, err = state.Pressure(ctx, PressureTargetTotal)
	require.NoError(t, err)
	require.Equal(t, Pressure{}, total, "each field is floored at zero independently")

	var nilState *State
	require.NoError(t, nilState.AddPressure(ctx, "x", 1, 1))
	require.Empty(t, nilState.Affinity(ctx, &RequestInfo{AffinityKey: "k"}).ExactID)
	nilState.RecordAffinity(ctx, &RequestInfo{AffinityKey: "k"}, "x")
}

func TestStateAffinityRoundTrip(t *testing.T) {
	state := newTestState(t)
	ctx := context.Background()

	shared := strings.Repeat("shared system prompt and retrieval context ", 30)
	previous := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	previous.setAffinity(shared + "first question")
	state.RecordAffinity(ctx, previous, "container-b")

	info := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	info.setAffinity(shared + "second question")
	affinity := state.Affinity(ctx, info)
	require.Empty(t, affinity.ExactID)
	require.Greater(t, affinity.PrefixMatches["container-b"], 0)

	exact := state.Affinity(ctx, previous)
	require.Equal(t, "container-b", exact.ExactID)
	require.False(t, exact.ExactIsSession)

	session := &RequestInfo{Model: "model", SessionHash: "s", AffinityKey: "s"}
	state.RecordAffinity(ctx, session, "container-a")
	got := state.Affinity(ctx, session)
	require.Equal(t, "container-a", got.ExactID)
	require.True(t, got.ExactIsSession)
}

func TestProbes(t *testing.T) {
	defaults := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	require.Equal(t, defaults, ReadinessPaths("v1/models"))
	require.Equal(t, defaults, ReadinessPaths(" "))
	require.Equal(t, append(defaults, "/metrics"), ReadinessPaths("/metrics"))
	require.Equal(t, "/metrics", NormalizeMetricsPath(""))
	require.Equal(t, "/custom", NormalizeMetricsPath("custom"))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			_, _ = w.Write([]byte("vllm:num_requests_running 4\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	ctx := context.Background()

	require.True(t, CheckReady(ctx, server.Client(), server.URL, ReadinessPaths(""), time.Second))
	require.False(t, CheckReady(ctx, server.Client(), server.URL, []string{"/missing"}, time.Second))

	metrics, ok, err := FetchEngineMetrics(ctx, server.Client(), server.URL+"/metrics", EngineMetrics{})
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4, metrics.RunningRequests)

	_, ok, err = FetchEngineMetrics(ctx, server.Client(), server.URL+"/missing", EngineMetrics{})
	require.NoError(t, err)
	require.False(t, ok)
}
