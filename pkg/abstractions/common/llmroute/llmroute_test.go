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

func newTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	return rdb
}

func TestInspectParsesOpenAIChatAndRestoresBody(t *testing.T) {
	body := `{"model":"zai-org/GLM-4.5-Air-FP8","user":"session-1","stream":true,"max_tokens":64,"messages":[{"role":"system","content":"You are terse."},{"role":"user","content":"Explain prefix caching for LLM serving."}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))

	path, ok := NormalizePath(req.URL.Path)
	require.True(t, ok)
	info, err := Inspect(req, path, InspectOptions{DefaultModel: "fallback-model"})
	require.NoError(t, err)

	require.Equal(t, "zai-org/GLM-4.5-Air-FP8", info.Model)
	require.True(t, info.Stream)
	require.EqualValues(t, 64, info.OutputTokens)
	require.Equal(t, "session-1", info.SessionKey)
	require.NotEmpty(t, info.SessionHash)
	require.Equal(t, info.SessionHash, info.AffinityKey)
	require.NotEmpty(t, info.PrefixHash)
	require.NotEmpty(t, info.PrefixBlocks)
	require.Greater(t, info.PromptTokens, int64(0))
	require.Equal(t, info.PromptTokens+info.OutputTokens, info.TokenPressure)

	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(restored))
}

func TestInspectFallsBackToDefaultModelAndHeaderSession(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hello"}`))
	req.Header.Set("X-Beam-LLM-Session", "thread-123")

	info, err := Inspect(req, "/v1/completions", InspectOptions{DefaultModel: "configured"})
	require.NoError(t, err)
	require.Equal(t, "configured", info.Model)
	require.Equal(t, "thread-123", info.SessionKey)
	require.EqualValues(t, DefaultOutputTokens, info.OutputTokens)
}

func TestInspectGetRequestUsesMinimalPressure(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	info, err := Inspect(req, "/v1/models", InspectOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 1, info.PromptTokens)
	require.EqualValues(t, 1+DefaultOutputTokens, info.TokenPressure)
}

func TestNormalizePath(t *testing.T) {
	cases := map[string]struct {
		path string
		ok   bool
	}{
		"/v1/chat/completions":                        {"/v1/chat/completions", true},
		"/pod/public/stub-1/8000/v1/chat/completions": {"/v1/chat/completions", true},
		"v1/embeddings/":                              {"/v1/embeddings", true},
		"/v1/models":                                  {"/v1/models", true},
		"/health":                                     {"/health", false},
		"":                                            {"/", false},
	}
	for input, want := range cases {
		got, ok := NormalizePath(input)
		require.Equal(t, want.ok, ok, input)
		require.Equal(t, want.path, got, input)
	}
}

func TestReadAndRestoreBodyReportsOverflow(t *testing.T) {
	payload := strings.Repeat("x", 100)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	head, overflow, err := ReadAndRestoreBody(req, 10)
	require.NoError(t, err)
	require.True(t, overflow)
	require.Len(t, head, 10)

	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, payload, string(restored))
}

func TestPrefixBlockHashesShareLeadingBlocks(t *testing.T) {
	shared := strings.Repeat("shared system prompt and retrieval context ", 30)
	a := PrefixBlockHashes("model", shared+"first question")
	b := PrefixBlockHashes("model", shared+"second question")
	require.NotEmpty(t, a)
	require.NotEmpty(t, b)

	common := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			common++
		}
	}
	require.Greater(t, common, 0, "expected shared prefix blocks")
	require.NotEqual(t, PrefixHash("model", shared+"first question"), PrefixHash("model", shared+"second question"))
}

func TestSelectPrefersExactAffinityWhenBalanced(t *testing.T) {
	var selector Selector
	info := &RequestInfo{AffinityKey: "affinity", PrefixHash: "affinity"}
	candidates := []Candidate{{ID: "container-a"}, {ID: "container-b"}}

	selection, ok := selector.Select(candidates, Affinity{ExactID: "container-b"}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "prefix_affinity", selection.Reason)
	require.Equal(t, "prefix_affinity", info.RouteReason)
	require.Equal(t, 2, info.CandidateCount)
}

func TestSelectSessionAffinityReason(t *testing.T) {
	var selector Selector
	info := &RequestInfo{AffinityKey: "session", SessionHash: "session"}
	selection, ok := selector.Select([]Candidate{{ID: "a"}, {ID: "b"}}, Affinity{ExactID: "a", ExactIsSession: true}, info)
	require.True(t, ok)
	require.Equal(t, "a", selection.Candidate.ID)
	require.Equal(t, "session_affinity", selection.Reason)
}

func TestSelectSpillsBusyAffinityTarget(t *testing.T) {
	var selector Selector
	info := &RequestInfo{AffinityKey: "session", SessionHash: "session", PrefixHash: "session", PromptTokens: 128, OutputTokens: 128}
	candidates := []Candidate{
		{ID: "container-a", Pressure: Pressure{ActiveStreams: 1, TokenPressure: 512}},
		{ID: "container-b"},
	}
	selection, ok := selector.Select(candidates, Affinity{ExactID: "container-a", ExactIsSession: true}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "least_pressure", selection.Reason)
}

func TestSelectIgnoresAffinityWhenLoadIsImbalanced(t *testing.T) {
	var selector Selector
	info := &RequestInfo{AffinityKey: "session", SessionHash: "session", PrefixHash: "session"}
	candidates := []Candidate{
		{ID: "container-a", Pressure: Pressure{ActiveStreams: 10, TokenPressure: 4096}},
		{ID: "container-b"},
	}
	selection, ok := selector.Select(candidates, Affinity{ExactID: "container-a", ExactIsSession: true}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "load_imbalance", selection.Reason)
}

func TestSelectUsesPrefixBlockAffinity(t *testing.T) {
	var selector Selector
	info := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	candidates := []Candidate{{ID: "container-a"}, {ID: "container-b"}}
	selection, ok := selector.Select(candidates, Affinity{PrefixMatches: map[string]int{"container-b": 3}}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "prefix_block_affinity", selection.Reason)
	require.Equal(t, 3, selection.PrefixMatches)
}

func findSpreadPreference(t *testing.T, info *RequestInfo, prefix string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		info.AffinityKey = prefix + strconv.Itoa(i)
		info.PrefixHash = info.AffinityKey
		if spreadScore("container-b", info) < spreadScore("container-a", info) {
			return
		}
	}
	t.Fatal("test setup did not find a prefix that prefers container-b")
}

func TestSelectUsesSpreadTieBreak(t *testing.T) {
	var selector Selector
	info := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	findSpreadPreference(t, info, "prefix-spread-")

	selection, ok := selector.Select([]Candidate{{ID: "container-a"}, {ID: "container-b"}}, Affinity{}, info)
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
	require.Equal(t, "least_pressure", selection.Reason)
}

func TestSelectConnectionPressureBeatsSpread(t *testing.T) {
	var selector Selector
	info := &RequestInfo{Model: "model", Path: "/v1/chat/completions"}
	findSpreadPreference(t, info, "prefix-pressure-")

	selection, ok := selector.Select([]Candidate{{ID: "container-a"}, {ID: "container-b", Connections: 1}}, Affinity{}, info)
	require.True(t, ok)
	require.Equal(t, "container-a", selection.Candidate.ID)
}

func TestSelectUsesEngineMetricsPressure(t *testing.T) {
	var selector Selector
	candidates := []Candidate{
		{ID: "container-a", Engine: EngineMetrics{WaitingRequests: 3, TTFTMs: 700}},
		{ID: "container-b", Engine: EngineMetrics{DecodeTokensPerSecond: 300, PrefixCacheHitMilli: 700}},
	}
	selection, ok := selector.Select(candidates, Affinity{}, &RequestInfo{Model: "model", Path: "/v1/chat/completions"})
	require.True(t, ok)
	require.Equal(t, "container-b", selection.Candidate.ID)
}

func TestSelectEmptyAndSingle(t *testing.T) {
	var selector Selector
	_, ok := selector.Select(nil, Affinity{}, nil)
	require.False(t, ok)

	selection, ok := selector.Select([]Candidate{{ID: "only", Payload: 42}}, Affinity{}, nil)
	require.True(t, ok)
	require.Equal(t, "only", selection.Candidate.ID)
	require.Equal(t, 42, selection.Candidate.Payload)
}

func TestEngineMetricsFromPrometheusUsesVLLMDeltas(t *testing.T) {
	now := time.Unix(100, 0)
	previous := EngineMetrics{
		GenerationTokensTotal:   100,
		PromptTokensTotal:       50,
		PrefixCacheHitsTotal:    25,
		PrefixCacheQueriesTotal: 50,
		TTFTSumSeconds:          1.0,
		TTFTCount:               4,
		TPOTSumSeconds:          0.4,
		TPOTCount:               10,
		UpdatedAtUnixMs:         now.Add(-5 * time.Second).UnixMilli(),
	}
	body := `
	# HELP vllm:num_requests_running running
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

	got := EngineMetricsFromPrometheus([]byte(body), previous, now)
	require.EqualValues(t, 2, got.RunningRequests)
	require.EqualValues(t, 1, got.WaitingRequests)
	require.EqualValues(t, 920, got.GPUCacheUsageMilli)
	require.EqualValues(t, 800, got.PrefixCacheHitMilli)
	require.EqualValues(t, 300, got.TTFTMs)
	require.EqualValues(t, 30, got.TPOTMs)
	require.EqualValues(t, 30, got.DecodeTokensPerSecond)
	require.EqualValues(t, 12, got.PromptTokensPerSecond)
	require.True(t, got.HasData())
	require.False(t, got.Stale(now))
	require.True(t, got.Stale(now.Add(MetricsStaleAfter+time.Second)))
}

func TestStatePressureLifecycle(t *testing.T) {
	state := NewState(newTestRedis(t), "pod:workspace:stub")
	ctx := context.Background()

	require.NoError(t, state.AddPressure(ctx, "container-a", 1, 150))
	total, err := state.Pressure(ctx, PressureTargetTotal)
	require.NoError(t, err)
	require.Equal(t, Pressure{ActiveStreams: 1, TokenPressure: 150}, total)
	replica, err := state.Pressure(ctx, "container-a")
	require.NoError(t, err)
	require.Equal(t, Pressure{ActiveStreams: 1, TokenPressure: 150}, replica)

	require.NoError(t, state.AddPressure(ctx, "container-a", -1, -150))
	total, err = state.Pressure(ctx, PressureTargetTotal)
	require.NoError(t, err)
	require.Equal(t, Pressure{}, total)
}

func TestStateAffinityRoundTrip(t *testing.T) {
	state := NewState(newTestRedis(t), "pod:workspace:stub")
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

func TestStateEngineMetricsRoundTrip(t *testing.T) {
	state := NewState(newTestRedis(t), "pod:workspace:stub")
	ctx := context.Background()
	want := EngineMetrics{
		RunningRequests:         2,
		WaitingRequests:         1,
		TTFTMs:                  180,
		TPOTMs:                  25,
		DecodeTokensPerSecond:   220,
		GPUCacheUsageMilli:      700,
		PrefixCacheHitMilli:     850,
		GenerationTokensTotal:   1234,
		PrefixCacheHitsTotal:    700,
		PrefixCacheQueriesTotal: 1000,
		TTFTSumSeconds:          3.5,
		TTFTCount:               10,
		UpdatedAtUnixMs:         time.Now().UnixMilli(),
	}
	require.NoError(t, state.WriteEngineMetrics(ctx, "container-a", want))
	got, err := state.EngineMetrics(ctx, "container-a")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestNilStateIsNoop(t *testing.T) {
	var state *State
	ctx := context.Background()
	require.NoError(t, state.AddPressure(ctx, "x", 1, 1))
	p, err := state.Pressure(ctx, "x")
	require.NoError(t, err)
	require.Equal(t, Pressure{}, p)
	require.Empty(t, state.Affinity(ctx, &RequestInfo{AffinityKey: "k"}).ExactID)
}

func TestReadinessPathsDedupesMetricsPath(t *testing.T) {
	paths := ReadinessPaths("v1/models")
	require.Equal(t, []string{"/v1/models", "/health", "/server_info", "/get_model_info"}, paths)
	paths = ReadinessPaths("/metrics")
	require.Equal(t, "/metrics", paths[len(paths)-1])
	require.Equal(t, "/metrics", NormalizeMetricsPath(""))
	require.Equal(t, "/custom", NormalizeMetricsPath("custom"))
}

func TestCheckReadyAndFetchEngineMetrics(t *testing.T) {
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

	require.True(t, CheckReady(context.Background(), server.Client(), server.URL, ReadinessPaths(""), time.Second))
	require.False(t, CheckReady(context.Background(), server.Client(), server.URL, []string{"/missing"}, time.Second))

	metrics, ok, err := FetchEngineMetrics(context.Background(), server.Client(), server.URL+"/metrics", EngineMetrics{})
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4, metrics.RunningRequests)
}
