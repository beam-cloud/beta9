package llmroute

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

func TestProbes(t *testing.T) {
	require.Equal(t, "/metrics", NormalizeMetricsPath(" "))
	require.Equal(t, "/metrics", NormalizeMetricsPath("/metrics"))
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

	require.True(t, Ready(ctx, server.Client(), server.URL, " "), "one of the paths engines answer on")
	require.True(t, Ready(ctx, server.Client(), server.URL, "/health"))
	require.False(t, Ready(ctx, server.Client(), server.URL, "/missing"), "only the path it names counts")

	metrics, ok, err := FetchEngineMetrics(ctx, server.Client(), server.URL+"/metrics", EngineMetrics{})
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4, metrics.RunningRequests)

	_, ok, err = FetchEngineMetrics(ctx, server.Client(), server.URL+"/missing", EngineMetrics{})
	require.NoError(t, err)
	require.False(t, ok)
}
