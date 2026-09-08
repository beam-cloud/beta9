package llmroute

import (
	"context"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
)

const (
	// PressureTargetTotal is the pseudo-replica id that aggregates pressure
	// across every replica of a deployment.
	PressureTargetTotal = "total"

	PressureTTL time.Duration = 30 * time.Second
	AffinityTTL time.Duration = 10 * time.Minute
	MetricsTTL  time.Duration = 30 * time.Second

	stateOpTimeout = time.Second
)

var engineMetricFields = []string{
	"running_requests",
	"waiting_requests",
	"ttft_ms",
	"tpot_ms",
	"decode_tokens_per_second",
	"prompt_tokens_per_second",
	"gpu_cache_usage_milli",
	"prefix_cache_hit_milli",
	"generation_tokens_total",
	"prompt_tokens_total",
	"prefix_cache_hits_total",
	"prefix_cache_queries_total",
	"ttft_sum_seconds",
	"ttft_count",
	"tpot_sum_seconds",
	"tpot_count",
	"updated_at_ms",
}

// State stores routing state (pressure, affinity, engine metrics) in Redis
// beneath a caller-provided key prefix so that multiple deployments and
// gateway replicas share a consistent view.
type State struct {
	rdb    *common.RedisClient
	prefix string
}

// NewState returns a State namespaced by prefix (e.g. "pod:<ws>:<stub>" or
// "managed_endpoint:<id>"). A nil rdb yields a State whose operations are no-ops.
func NewState(rdb *common.RedisClient, prefix string) *State {
	return &State{rdb: rdb, prefix: prefix}
}

func (s *State) enabled() bool {
	return s != nil && s.rdb != nil && s.prefix != ""
}

func (s *State) pressureKey(target string) string {
	return s.prefix + ":llm_pressure:" + target
}

func (s *State) engineMetricsKey(replicaID string) string {
	return s.prefix + ":llm_engine_metrics:" + replicaID
}

func (s *State) affinityKey(key string) string {
	return s.prefix + ":llm_affinity:" + key
}

func (s *State) prefixAffinityKey(block string) string {
	return s.prefix + ":llm_prefix_affinity:" + block
}

// Pressure reads the pressure snapshot for a replica id or PressureTargetTotal.
func (s *State) Pressure(ctx context.Context, target string) (Pressure, error) {
	if !s.enabled() || target == "" {
		return Pressure{}, nil
	}
	values, err := s.rdb.HMGet(ctx, s.pressureKey(target), "active_streams", "token_pressure").Result()
	if err != nil {
		return Pressure{}, err
	}
	return Pressure{
		ActiveStreams: fieldInt64(values[0]),
		TokenPressure: fieldInt64(values[1]),
	}, nil
}

// AddPressure atomically adjusts pressure for the total aggregate and, when
// replicaID is non-empty, for that replica.
func (s *State) AddPressure(ctx context.Context, replicaID string, activeStreamsDelta, tokenPressureDelta int64) error {
	if !s.enabled() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, target := range []string{PressureTargetTotal, replicaID} {
		if target == "" {
			continue
		}
		key := s.pressureKey(target)
		pipe.HIncrBy(ctx, key, "active_streams", activeStreamsDelta)
		pipe.HIncrBy(ctx, key, "token_pressure", tokenPressureDelta)
		pipe.Expire(ctx, key, PressureTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Affinity resolves the exact and prefix-block affinity for a request.
func (s *State) Affinity(ctx context.Context, info *RequestInfo) Affinity {
	affinity := Affinity{PrefixMatches: map[string]int{}}
	if !s.enabled() || info == nil {
		return affinity
	}
	ctx, cancel := context.WithTimeout(ctx, stateOpTimeout)
	defer cancel()

	if info.AffinityKey != "" {
		if replicaID, err := s.rdb.Get(ctx, s.affinityKey(info.AffinityKey)).Result(); err == nil {
			affinity.ExactID = replicaID
		}
	}
	affinity.ExactIsSession = info.SessionHash != "" && info.AffinityKey == info.SessionHash

	if len(info.PrefixBlocks) == 0 {
		return affinity
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]interface{ Result() (string, error) }, 0, len(info.PrefixBlocks))
	for _, block := range info.PrefixBlocks {
		if block == "" {
			continue
		}
		cmds = append(cmds, pipe.Get(ctx, s.prefixAffinityKey(block)))
	}
	if len(cmds) == 0 {
		return affinity
	}
	_, _ = pipe.Exec(ctx)
	for _, cmd := range cmds {
		replicaID, err := cmd.Result()
		if err != nil || replicaID == "" {
			continue
		}
		affinity.PrefixMatches[replicaID]++
	}
	return affinity
}

// RecordAffinity remembers that replicaID served info's session/prefix.
func (s *State) RecordAffinity(ctx context.Context, info *RequestInfo, replicaID string) {
	if !s.enabled() || info == nil || replicaID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, stateOpTimeout)
	defer cancel()

	pipe := s.rdb.Pipeline()
	if info.AffinityKey != "" {
		pipe.Set(ctx, s.affinityKey(info.AffinityKey), replicaID, AffinityTTL)
	}
	for _, block := range info.PrefixBlocks {
		if block == "" {
			continue
		}
		pipe.Set(ctx, s.prefixAffinityKey(block), replicaID, AffinityTTL)
	}
	_, _ = pipe.Exec(ctx)
}

// EngineMetrics reads the last engine snapshot for a replica.
func (s *State) EngineMetrics(ctx context.Context, replicaID string) (EngineMetrics, error) {
	if !s.enabled() || replicaID == "" {
		return EngineMetrics{}, nil
	}
	values, err := s.rdb.HMGet(ctx, s.engineMetricsKey(replicaID), engineMetricFields...).Result()
	if err != nil {
		return EngineMetrics{}, err
	}
	return EngineMetrics{
		RunningRequests:         fieldInt64(values[0]),
		WaitingRequests:         fieldInt64(values[1]),
		TTFTMs:                  fieldInt64(values[2]),
		TPOTMs:                  fieldInt64(values[3]),
		DecodeTokensPerSecond:   fieldInt64(values[4]),
		PromptTokensPerSecond:   fieldInt64(values[5]),
		GPUCacheUsageMilli:      fieldInt64(values[6]),
		PrefixCacheHitMilli:     fieldInt64(values[7]),
		GenerationTokensTotal:   fieldFloat64(values[8]),
		PromptTokensTotal:       fieldFloat64(values[9]),
		PrefixCacheHitsTotal:    fieldFloat64(values[10]),
		PrefixCacheQueriesTotal: fieldFloat64(values[11]),
		TTFTSumSeconds:          fieldFloat64(values[12]),
		TTFTCount:               fieldFloat64(values[13]),
		TPOTSumSeconds:          fieldFloat64(values[14]),
		TPOTCount:               fieldFloat64(values[15]),
		UpdatedAtUnixMs:         fieldInt64(values[16]),
	}, nil
}

// WriteEngineMetrics stores a snapshot for a replica.
func (s *State) WriteEngineMetrics(ctx context.Context, replicaID string, snapshot EngineMetrics) error {
	if !s.enabled() || replicaID == "" || !snapshot.HasData() {
		return nil
	}
	key := s.engineMetricsKey(replicaID)
	pipe := s.rdb.Pipeline()
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
	pipe.Expire(ctx, key, MetricsTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func fieldInt64(value any) int64 {
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

func fieldFloat64(value any) float64 {
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
