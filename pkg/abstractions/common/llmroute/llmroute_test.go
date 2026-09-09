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
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	return NewState(rdb, "pod:workspace:stub")
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
		if spreadScore("container-b", info) < spreadScore("container-a", info) {
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
	got := engineMetricsFromPrometheus([]byte(body), previous, now)
	require.EqualValues(t, 2, got.RunningRequests)
	require.EqualValues(t, 1, got.WaitingRequests)
	require.EqualValues(t, 920, got.GPUCacheUsageMilli)
	require.EqualValues(t, 800, got.PrefixCacheHitMilli)
	require.EqualValues(t, 300, got.TTFTMs)
	require.EqualValues(t, 30, got.TPOTMs)
	require.EqualValues(t, 30, got.DecodeTokensPerSecond)
	require.EqualValues(t, 12, got.PromptTokensPerSecond)
	require.True(t, got.hasData())
	require.False(t, EngineMetrics{UpdatedAtUnixMs: 1}.hasData())
}

func TestStatePressureLifecycle(t *testing.T) {
	state := newTestState(t)
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
