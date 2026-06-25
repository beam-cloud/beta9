package pod

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func llmMetricsPath(config *types.StubConfigV1) string {
	if config != nil && config.LLM != nil && strings.TrimSpace(config.LLM.MetricsPath) != "" {
		return "/" + strings.TrimPrefix(strings.TrimSpace(config.LLM.MetricsPath), "/")
	}
	return "/metrics"
}

func (s llmEngineMetricsSnapshot) Stale(now time.Time) bool {
	if s.UpdatedAtUnixMs <= 0 {
		return true
	}
	return now.Sub(time.UnixMilli(s.UpdatedAtUnixMs)) > llmMetricsStaleAfter
}

func (s llmEngineMetricsSnapshot) HasData() bool {
	return s.RunningRequests != 0 ||
		s.WaitingRequests != 0 ||
		s.TTFTMs != 0 ||
		s.TPOTMs != 0 ||
		s.DecodeTokensPerSecond != 0 ||
		s.PromptTokensPerSecond != 0 ||
		s.GPUCacheUsageMilli != 0 ||
		s.PrefixCacheHitMilli != 0 ||
		s.GenerationTokensTotal != 0 ||
		s.PromptTokensTotal != 0 ||
		s.PrefixCacheHitsTotal != 0 ||
		s.PrefixCacheQueriesTotal != 0 ||
		s.TTFTCount != 0 ||
		s.TPOTCount != 0
}

func (pb *PodProxyBuffer) llmEngineMetricsSnapshot(containerID string) (llmEngineMetricsSnapshot, error) {
	if pb.rdb == nil || pb.workspace == nil || containerID == "" {
		return llmEngineMetricsSnapshot{}, nil
	}
	return readPodLLMEngineMetrics(context.Background(), pb.rdb, pb.workspace.Name, pb.stubId, containerID)
}

func readPodLLMEngineMetrics(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerID string) (llmEngineMetricsSnapshot, error) {
	values, err := rdb.HMGet(ctx, Keys.podLLMEngineMetrics(workspaceName, stubId, containerID), llmEngineMetricRedisFields...).Result()
	if err != nil {
		return llmEngineMetricsSnapshot{}, err
	}

	return llmEngineMetricsSnapshot{
		RunningRequests:         redisFieldInt64(values[0]),
		WaitingRequests:         redisFieldInt64(values[1]),
		TTFTMs:                  redisFieldInt64(values[2]),
		TPOTMs:                  redisFieldInt64(values[3]),
		DecodeTokensPerSecond:   redisFieldInt64(values[4]),
		PromptTokensPerSecond:   redisFieldInt64(values[5]),
		GPUCacheUsageMilli:      redisFieldInt64(values[6]),
		PrefixCacheHitMilli:     redisFieldInt64(values[7]),
		GenerationTokensTotal:   redisFieldFloat64(values[8]),
		PromptTokensTotal:       redisFieldFloat64(values[9]),
		PrefixCacheHitsTotal:    redisFieldFloat64(values[10]),
		PrefixCacheQueriesTotal: redisFieldFloat64(values[11]),
		TTFTSumSeconds:          redisFieldFloat64(values[12]),
		TTFTCount:               redisFieldFloat64(values[13]),
		TPOTSumSeconds:          redisFieldFloat64(values[14]),
		TPOTCount:               redisFieldFloat64(values[15]),
		UpdatedAtUnixMs:         redisFieldInt64(values[16]),
	}, nil
}

func writePodLLMEngineMetrics(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerID string, snapshot llmEngineMetricsSnapshot) error {
	if rdb == nil || workspaceName == "" || stubId == "" || containerID == "" || !snapshot.HasData() {
		return nil
	}
	key := Keys.podLLMEngineMetrics(workspaceName, stubId, containerID)
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"running_requests":           snapshot.RunningRequests,
		"waiting_requests":           snapshot.WaitingRequests,
		"ttft_ms":                    snapshot.TTFTMs,
		"tpot_ms":                    snapshot.TPOTMs,
		"decode_tokens_per_second":   snapshot.DecodeTokensPerSecond,
		"prompt_tokens_per_second":   snapshot.PromptTokensPerSecond,
		"gpu_cache_usage_milli":      snapshot.GPUCacheUsageMilli,
		"prefix_cache_hit_milli":     snapshot.PrefixCacheHitMilli,
		"generation_tokens_total":    strconv.FormatFloat(snapshot.GenerationTokensTotal, 'f', -1, 64),
		"prompt_tokens_total":        strconv.FormatFloat(snapshot.PromptTokensTotal, 'f', -1, 64),
		"prefix_cache_hits_total":    strconv.FormatFloat(snapshot.PrefixCacheHitsTotal, 'f', -1, 64),
		"prefix_cache_queries_total": strconv.FormatFloat(snapshot.PrefixCacheQueriesTotal, 'f', -1, 64),
		"ttft_sum_seconds":           strconv.FormatFloat(snapshot.TTFTSumSeconds, 'f', -1, 64),
		"ttft_count":                 strconv.FormatFloat(snapshot.TTFTCount, 'f', -1, 64),
		"tpot_sum_seconds":           strconv.FormatFloat(snapshot.TPOTSumSeconds, 'f', -1, 64),
		"tpot_count":                 strconv.FormatFloat(snapshot.TPOTCount, 'f', -1, 64),
		"updated_at_ms":              snapshot.UpdatedAtUnixMs,
	})
	pipe.Expire(ctx, key, llmMetricsTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (pb *PodProxyBuffer) refreshLLMEngineMetricsAsync(containerID, address string) {
	if pb == nil || !llmEnabled(pb.stubConfig) || pb.rdb == nil || pb.workspace == nil || containerID == "" || address == "" {
		return
	}
	now := time.Now()
	if next, ok := pb.llmMetricsRefreshAfter.Load(containerID); ok {
		if nextUnix, ok := next.(int64); ok && now.UnixNano() < nextUnix {
			return
		}
	}
	pb.llmMetricsRefreshAfter.Store(containerID, now.Add(llmMetricsRefreshEvery).UnixNano())
	go func() {
		_ = pb.refreshLLMEngineMetrics(containerID, address)
	}()
}

func (pb *PodProxyBuffer) refreshLLMEngineMetrics(containerID, address string) error {
	if pb == nil || !llmEnabled(pb.stubConfig) || pb.rdb == nil || pb.workspace == nil || containerID == "" || address == "" {
		return nil
	}
	previous, _ := pb.llmEngineMetricsSnapshot(containerID)

	ctx, cancel := context.WithTimeout(pb.baseContext(), llmMetricsTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, podBackendURL("http", address, llmMetricsPath(pb.stubConfig), ""), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/plain")

	client := &http.Client{
		Transport: pb.backendTransport(address, llmMetricsTimeout),
		Timeout:   llmMetricsTimeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, llmReadinessBodyLimit))
		return nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, llmMetricsBodyLimit))
	if err != nil {
		return err
	}
	snapshot := llmEngineMetricsFromPrometheus(body, previous, time.Now())
	if !snapshot.HasData() {
		return nil
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return writePodLLMEngineMetrics(ctx, pb.rdb, pb.workspace.Name, pb.stubId, containerID, snapshot)
}

func llmEngineMetricsFromPrometheus(body []byte, previous llmEngineMetricsSnapshot, now time.Time) llmEngineMetricsSnapshot {
	samples := parsePrometheusSamples(body)
	snapshot := llmEngineMetricsSnapshot{
		RunningRequests:         roundMetric(metricValue(samples, llmMetricRunningRequests...)),
		WaitingRequests:         roundMetric(metricValue(samples, llmMetricWaitingRequests...)),
		GPUCacheUsageMilli:      ratioMilli(metricValue(samples, llmMetricGPUCacheUsage...)),
		PrefixCacheHitMilli:     ratioMilli(metricValue(samples, llmMetricPrefixCacheHitRate...)),
		GenerationTokensTotal:   metricValue(samples, llmMetricGenerationTokens...),
		PromptTokensTotal:       metricValue(samples, llmMetricPromptTokens...),
		PrefixCacheHitsTotal:    metricValue(samples, llmMetricPrefixCacheHits...),
		PrefixCacheQueriesTotal: metricValue(samples, llmMetricPrefixCacheQueries...),
		TTFTSumSeconds:          metricValue(samples, llmMetricTTFTSumSeconds...),
		TTFTCount:               metricValue(samples, llmMetricTTFTCount...),
		TPOTSumSeconds:          metricValue(samples, llmMetricTPOTSumSeconds...),
		TPOTCount:               metricValue(samples, llmMetricTPOTCount...),
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

func parsePrometheusSamples(body []byte) map[string]float64 {
	samples := map[string]float64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 4096), int(llmMetricsBodyLimit))
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

func llmEngineMetricsScore(snapshot llmEngineMetricsSnapshot) int64 {
	score := snapshot.RunningRequests*llmEngineRunningWeight +
		snapshot.WaitingRequests*llmEngineWaitingWeight +
		snapshot.TTFTMs*llmEngineTTFTWeight +
		snapshot.TPOTMs*llmEngineTPOTWeight

	if snapshot.GPUCacheUsageMilli > 850 {
		score += (snapshot.GPUCacheUsageMilli - 850) * llmEngineCachePressureWeight
	}
	if snapshot.DecodeTokensPerSecond > 0 {
		score -= minInt64(snapshot.DecodeTokensPerSecond, llmEngineDecodeBonusCap)
	}
	if snapshot.PrefixCacheHitMilli > 0 {
		score -= minInt64(snapshot.PrefixCacheHitMilli/4, llmEngineCacheHitBonusCap)
	}
	return score
}

func redisFieldInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case string:
		out, _ := strconv.ParseInt(v, 10, 64)
		return out
	case []byte:
		out, _ := strconv.ParseInt(string(v), 10, 64)
		return out
	default:
		return 0
	}
}

func redisFieldFloat64(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		out, _ := strconv.ParseFloat(v, 64)
		return out
	case []byte:
		out, _ := strconv.ParseFloat(string(v), 64)
		return out
	default:
		return 0
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
