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

// enginePrefixes are the metric namespaces of the supported engines.
var enginePrefixes = []string{"vllm:", "vllm_", "sglang:", "sglang_"}

// engineMetricNames maps each raw field to the bare metric names (vLLM first,
// then SGLang) that feed it; the first one present wins.
var engineMetricNames = struct {
	running, waiting, cacheUsage, cacheHitRate, generationTokens, promptTokens,
	prefixHits, prefixQueries, ttftSum, ttftCount, tpotSum, tpotCount []string
}{
	running:          []string{"num_requests_running", "num_running_reqs"},
	waiting:          []string{"num_requests_waiting", "num_queue_reqs"},
	cacheUsage:       []string{"kv_cache_usage_perc", "gpu_cache_usage_perc", "gpu_cache_usage", "token_usage"},
	cacheHitRate:     []string{"gpu_prefix_cache_hit_rate", "prefix_cache_hit_rate", "cache_hit_rate"},
	generationTokens: []string{"generation_tokens_total"},
	promptTokens:     []string{"prompt_tokens_total", "prompt_tokens_processed_total"},
	prefixHits:       []string{"prefix_cache_hits_total", "gpu_prefix_cache_hits_total"},
	prefixQueries:    []string{"prefix_cache_queries_total", "gpu_prefix_cache_queries_total"},
	ttftSum:          []string{"time_to_first_token_seconds_sum"},
	ttftCount:        []string{"time_to_first_token_seconds_count"},
	tpotSum:          []string{"time_per_output_token_seconds_sum", "inter_token_latency_seconds_sum"},
	tpotCount:        []string{"time_per_output_token_seconds_count", "inter_token_latency_seconds_count"},
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

// engineMetricsFromPrometheus parses a vLLM/SGLang exposition, deriving rates
// from previous. The boolean reports whether any recognized metric was
// present; an all-zero snapshot from an idle engine is still valid data.
func engineMetricsFromPrometheus(body []byte, previous EngineMetrics, now time.Time) (EngineMetrics, bool) {
	samples := parsePrometheusSamples(body)
	found := false
	get := func(names []string) float64 {
		v, ok := metricValue(samples, names)
		found = found || ok
		return v
	}
	names := engineMetricNames
	m := EngineMetrics{
		RunningRequests:         roundMetric(get(names.running)),
		WaitingRequests:         roundMetric(get(names.waiting)),
		GPUCacheUsageMilli:      ratioMilli(get(names.cacheUsage)),
		PrefixCacheHitMilli:     ratioMilli(get(names.cacheHitRate)),
		GenerationTokensTotal:   get(names.generationTokens),
		PromptTokensTotal:       get(names.promptTokens),
		PrefixCacheHitsTotal:    get(names.prefixHits),
		PrefixCacheQueriesTotal: get(names.prefixQueries),
		TTFTSumSeconds:          get(names.ttftSum),
		TTFTCount:               get(names.ttftCount),
		TPOTSumSeconds:          get(names.tpotSum),
		TPOTCount:               get(names.tpotCount),
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
	return m, found
}

// parsePrometheusSamples sums every sample by bare metric name (labels dropped).
// A line is `name[{labels}] value [timestamp]`; the optional timestamp is ignored.
func parsePrometheusSamples(body []byte) map[string]float64 {
	samples := map[string]float64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 4096), int(metricsBodyLimit))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var name, rest string
		if open := strings.IndexByte(line, '{'); open >= 0 {
			// Label values may contain spaces, so skip to the closing brace.
			end := strings.LastIndexByte(line, '}')
			if end < open {
				continue
			}
			name, rest = line[:open], line[end+1:]
		} else if i := strings.IndexAny(line, " \t"); i >= 0 {
			name, rest = line[:i], line[i:]
		} else {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		samples[name] += value
	}
	return samples
}

// metricValue returns the first sample present among names under any engine prefix.
func metricValue(samples map[string]float64, names []string) (float64, bool) {
	for _, name := range names {
		for _, prefix := range enginePrefixes {
			if v, ok := samples[prefix+name]; ok {
				return v, true
			}
		}
	}
	return 0, false
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
