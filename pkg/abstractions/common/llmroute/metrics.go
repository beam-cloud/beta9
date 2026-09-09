package llmroute

import (
	"bufio"
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	metricsBodyLimit int64 = 512 * 1024

	engineRunningWeight       = 1200
	engineWaitingWeight       = 2500
	engineTTFTWeight          = 2
	engineTPOTWeight          = 2
	engineCachePressureWeight = 4
	engineDecodeBonusCap      = 500
	engineCacheHitBonusCap    = 250
)

// Pressure is the router-observed load on a replica (or the whole deployment).
type Pressure struct {
	ActiveStreams int64
	TokenPressure int64
}

// EngineMetrics is a normalized snapshot of an inference engine's own metrics.
// Raw counters are kept so the next snapshot can derive rates from deltas.
type EngineMetrics struct {
	RunningRequests         int64
	WaitingRequests         int64
	TTFTMs                  int64
	TPOTMs                  int64
	DecodeTokensPerSecond   int64
	PromptTokensPerSecond   int64
	GPUCacheUsageMilli      int64
	PrefixCacheHitMilli     int64
	GenerationTokensTotal   float64
	PromptTokensTotal       float64
	PrefixCacheHitsTotal    float64
	PrefixCacheQueriesTotal float64
	TTFTSumSeconds          float64
	TTFTCount               float64
	TPOTSumSeconds          float64
	TPOTCount               float64
	UpdatedAtUnixMs         int64
}

// hasData reports whether any metric field (other than the timestamp) is set.
func (m EngineMetrics) hasData() bool {
	m.UpdatedAtUnixMs = 0
	return m != EngineMetrics{}
}

// score converts engine metrics into a load penalty; lower is better.
func (m EngineMetrics) score() int64 {
	score := m.RunningRequests*engineRunningWeight +
		m.WaitingRequests*engineWaitingWeight +
		m.TTFTMs*engineTTFTWeight +
		m.TPOTMs*engineTPOTWeight
	if m.GPUCacheUsageMilli > 850 {
		score += (m.GPUCacheUsageMilli - 850) * engineCachePressureWeight
	}
	score -= min(max(m.DecodeTokensPerSecond, 0), engineDecodeBonusCap)
	score -= min(max(m.PrefixCacheHitMilli, 0)/4, engineCacheHitBonusCap)
	return score
}

// engineMetricsFromPrometheus parses a vLLM/SGLang-style exposition, deriving rates from previous.
func engineMetricsFromPrometheus(body []byte, previous EngineMetrics, now time.Time) EngineMetrics {
	samples := parsePrometheusSamples(body)
	get := func(names ...string) float64 { return metricValue(samples, names...) }
	m := EngineMetrics{
		RunningRequests:         roundMetric(get("num_requests_running")),
		WaitingRequests:         roundMetric(get("num_requests_waiting")),
		GPUCacheUsageMilli:      ratioMilli(get("kv_cache_usage_perc", "gpu_cache_usage_perc", "gpu_cache_usage")),
		PrefixCacheHitMilli:     ratioMilli(get("gpu_prefix_cache_hit_rate", "prefix_cache_hit_rate")),
		GenerationTokensTotal:   get("generation_tokens_total"),
		PromptTokensTotal:       get("prompt_tokens_total", "prompt_tokens_processed_total"),
		PrefixCacheHitsTotal:    get("prefix_cache_hits_total", "gpu_prefix_cache_hits_total"),
		PrefixCacheQueriesTotal: get("prefix_cache_queries_total", "gpu_prefix_cache_queries_total"),
		TTFTSumSeconds:          get("time_to_first_token_seconds_sum"),
		TTFTCount:               get("time_to_first_token_seconds_count"),
		TPOTSumSeconds:          get("time_per_output_token_seconds_sum"),
		TPOTCount:               get("time_per_output_token_seconds_count"),
		UpdatedAtUnixMs:         now.UnixMilli(),
	}
	m.TTFTMs = histogramMeanMillis(m.TTFTSumSeconds, m.TTFTCount, previous.TTFTSumSeconds, previous.TTFTCount)
	m.TPOTMs = histogramMeanMillis(m.TPOTSumSeconds, m.TPOTCount, previous.TPOTSumSeconds, previous.TPOTCount)
	if m.PrefixCacheHitMilli == 0 && m.PrefixCacheHitsTotal > 0 && m.PrefixCacheQueriesTotal > 0 {
		hits, queries := m.PrefixCacheHitsTotal-previous.PrefixCacheHitsTotal, m.PrefixCacheQueriesTotal-previous.PrefixCacheQueriesTotal
		if queries <= 0 || hits < 0 {
			hits, queries = m.PrefixCacheHitsTotal, m.PrefixCacheQueriesTotal
		}
		m.PrefixCacheHitMilli = ratioMilli(hits / queries)
	}
	if elapsed := float64(m.UpdatedAtUnixMs-previous.UpdatedAtUnixMs) / 1000; elapsed > 0 {
		m.DecodeTokensPerSecond = roundMetric((m.GenerationTokensTotal - previous.GenerationTokensTotal) / elapsed)
		m.PromptTokensPerSecond = roundMetric((m.PromptTokensTotal - previous.PromptTokensTotal) / elapsed)
	}
	if m.DecodeTokensPerSecond == 0 && m.TPOTMs > 0 {
		m.DecodeTokensPerSecond = 1000 / m.TPOTMs
	}
	return m
}

// parsePrometheusSamples sums every sample by bare metric name (labels dropped).
func parsePrometheusSamples(body []byte) map[string]float64 {
	samples := map[string]float64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 4096), int(metricsBodyLimit))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		name, _, _ := strings.Cut(fields[0], "{")
		samples[name] += value
	}
	return samples
}

// metricValue returns the first sample present among names under either vllm prefix.
func metricValue(samples map[string]float64, names ...string) float64 {
	for _, name := range names {
		for _, prefix := range []string{"vllm:", "vllm_"} {
			if v, ok := samples[prefix+name]; ok {
				return v
			}
		}
	}
	return 0
}

// histogramMeanMillis prefers the mean since the previous snapshot over the lifetime mean.
func histogramMeanMillis(sum, count, prevSum, prevCount float64) int64 {
	if count-prevCount > 0 {
		return roundMetric((sum - prevSum) / (count - prevCount) * 1000)
	}
	if count > 0 {
		return roundMetric(sum / count * 1000)
	}
	return 0
}

// ratioMilli accepts ratios expressed either as 0..1 or as percentages.
func ratioMilli(value float64) int64 {
	if value <= 1.5 {
		return roundMetric(value * 1000)
	}
	return roundMetric(value * 10)
}

func roundMetric(value float64) int64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return int64(math.Round(value))
}
