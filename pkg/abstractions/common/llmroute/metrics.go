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
	MetricsBodyLimit  int64         = 512 * 1024
	MetricsStaleAfter time.Duration = 5 * time.Second

	engineRunningWeight       = 1200
	engineWaitingWeight       = 2500
	engineTTFTWeight          = 2
	engineTPOTWeight          = 2
	engineCachePressureWeight = 4
	engineDecodeBonusCap      = 500
	engineCacheHitBonusCap    = 250
)

var (
	metricRunningRequests    = vllmMetricAliases("num_requests_running")
	metricWaitingRequests    = vllmMetricAliases("num_requests_waiting")
	metricGPUCacheUsage      = vllmMetricAliases("kv_cache_usage_perc", "gpu_cache_usage_perc", "gpu_cache_usage")
	metricPrefixCacheHitRate = vllmMetricAliases("gpu_prefix_cache_hit_rate", "prefix_cache_hit_rate")
	metricGenerationTokens   = vllmMetricAliases("generation_tokens_total")
	metricPromptTokens       = vllmMetricAliases("prompt_tokens_total", "prompt_tokens_processed_total")
	metricPrefixCacheHits    = vllmMetricAliases("prefix_cache_hits_total", "gpu_prefix_cache_hits_total")
	metricPrefixCacheQueries = vllmMetricAliases("prefix_cache_queries_total", "gpu_prefix_cache_queries_total")
	metricTTFTSumSeconds     = vllmMetricAliases("time_to_first_token_seconds_sum")
	metricTTFTCount          = vllmMetricAliases("time_to_first_token_seconds_count")
	metricTPOTSumSeconds     = vllmMetricAliases("time_per_output_token_seconds_sum")
	metricTPOTCount          = vllmMetricAliases("time_per_output_token_seconds_count")
)

func vllmMetricAliases(names ...string) []string {
	aliases := make([]string, 0, len(names)*2)
	for _, name := range names {
		aliases = append(aliases, "vllm:"+name, "vllm_"+name)
	}
	return aliases
}

// Pressure is the router-observed load on a replica (or the whole deployment).
type Pressure struct {
	ActiveStreams int64
	TokenPressure int64
}

// EngineMetrics is a normalized snapshot of an inference engine's own metrics.
// Rates are derived from consecutive snapshots; counters are kept so the next
// snapshot can compute deltas.
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

// Stale reports whether the snapshot is older than MetricsStaleAfter.
func (m EngineMetrics) Stale(now time.Time) bool {
	if m.UpdatedAtUnixMs <= 0 {
		return true
	}
	return now.Sub(time.UnixMilli(m.UpdatedAtUnixMs)) > MetricsStaleAfter
}

// HasData reports whether any metric field is populated.
func (m EngineMetrics) HasData() bool {
	m.UpdatedAtUnixMs = 0
	return m != EngineMetrics{}
}

// Score converts engine metrics into a load penalty; lower is better.
func (m EngineMetrics) Score() int64 {
	score := m.RunningRequests*engineRunningWeight +
		m.WaitingRequests*engineWaitingWeight +
		m.TTFTMs*engineTTFTWeight +
		m.TPOTMs*engineTPOTWeight

	if m.GPUCacheUsageMilli > 850 {
		score += (m.GPUCacheUsageMilli - 850) * engineCachePressureWeight
	}
	if m.DecodeTokensPerSecond > 0 {
		score -= min(m.DecodeTokensPerSecond, engineDecodeBonusCap)
	}
	if m.PrefixCacheHitMilli > 0 {
		score -= min(m.PrefixCacheHitMilli/4, engineCacheHitBonusCap)
	}
	return score
}

// EngineMetricsFromPrometheus parses a vLLM/SGLang-style Prometheus exposition
// and derives rates relative to the previous snapshot.
func EngineMetricsFromPrometheus(body []byte, previous EngineMetrics, now time.Time) EngineMetrics {
	samples := ParsePrometheusSamples(body)
	snapshot := EngineMetrics{
		RunningRequests:         roundMetric(metricValue(samples, metricRunningRequests...)),
		WaitingRequests:         roundMetric(metricValue(samples, metricWaitingRequests...)),
		GPUCacheUsageMilli:      ratioMilli(metricValue(samples, metricGPUCacheUsage...)),
		PrefixCacheHitMilli:     ratioMilli(metricValue(samples, metricPrefixCacheHitRate...)),
		GenerationTokensTotal:   metricValue(samples, metricGenerationTokens...),
		PromptTokensTotal:       metricValue(samples, metricPromptTokens...),
		PrefixCacheHitsTotal:    metricValue(samples, metricPrefixCacheHits...),
		PrefixCacheQueriesTotal: metricValue(samples, metricPrefixCacheQueries...),
		TTFTSumSeconds:          metricValue(samples, metricTTFTSumSeconds...),
		TTFTCount:               metricValue(samples, metricTTFTCount...),
		TPOTSumSeconds:          metricValue(samples, metricTPOTSumSeconds...),
		TPOTCount:               metricValue(samples, metricTPOTCount...),
		UpdatedAtUnixMs:         now.UnixMilli(),
	}

	snapshot.TTFTMs = histogramMeanMillis(snapshot.TTFTSumSeconds, snapshot.TTFTCount, previous.TTFTSumSeconds, previous.TTFTCount)
	snapshot.TPOTMs = histogramMeanMillis(snapshot.TPOTSumSeconds, snapshot.TPOTCount, previous.TPOTSumSeconds, previous.TPOTCount)
	if snapshot.PrefixCacheHitMilli == 0 {
		snapshot.PrefixCacheHitMilli = ratioDeltaMilli(snapshot.PrefixCacheHitsTotal, snapshot.PrefixCacheQueriesTotal, previous.PrefixCacheHitsTotal, previous.PrefixCacheQueriesTotal)
	}
	elapsed := float64(snapshot.UpdatedAtUnixMs-previous.UpdatedAtUnixMs) / float64(time.Second/time.Millisecond)
	if elapsed > 0 {
		if delta := snapshot.GenerationTokensTotal - previous.GenerationTokensTotal; delta > 0 {
			snapshot.DecodeTokensPerSecond = roundMetric(delta / elapsed)
		}
		if delta := snapshot.PromptTokensTotal - previous.PromptTokensTotal; delta > 0 {
			snapshot.PromptTokensPerSecond = roundMetric(delta / elapsed)
		}
	}
	if snapshot.DecodeTokensPerSecond == 0 && snapshot.TPOTMs > 0 {
		snapshot.DecodeTokensPerSecond = 1000 / snapshot.TPOTMs
	}
	return snapshot
}

// ParsePrometheusSamples sums every sample by bare metric name (labels dropped).
func ParsePrometheusSamples(body []byte) map[string]float64 {
	samples := map[string]float64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 4096), int(MetricsBodyLimit))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		name := fields[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		if name == "" {
			continue
		}
		samples[name] += value
	}
	return samples
}

func metricValue(samples map[string]float64, names ...string) float64 {
	for _, name := range names {
		if value, ok := samples[name]; ok {
			return value
		}
	}
	return 0
}

func histogramMeanMillis(sum, count, previousSum, previousCount float64) int64 {
	deltaCount := count - previousCount
	if deltaCount > 0 {
		return roundMetric(((sum - previousSum) / deltaCount) * 1000)
	}
	if count > 0 {
		return roundMetric((sum / count) * 1000)
	}
	return 0
}

func ratioMilli(value float64) int64 {
	if value <= 0 {
		return 0
	}
	if value <= 1.5 {
		return roundMetric(value * 1000)
	}
	return roundMetric(value * 10)
}

func ratioDeltaMilli(numerator, denominator, previousNumerator, previousDenominator float64) int64 {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	numeratorDelta := numerator - previousNumerator
	denominatorDelta := denominator - previousDenominator
	if denominatorDelta > 0 && numeratorDelta >= 0 {
		return ratioMilli(numeratorDelta / denominatorDelta)
	}
	return ratioMilli(numerator / denominator)
}

func roundMetric(value float64) int64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return int64(math.Round(value))
}
