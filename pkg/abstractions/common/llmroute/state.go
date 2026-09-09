package llmroute

import (
	"context"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/redis/go-redis/v9"
)

const (
	// PressureTargetTotal is the pseudo-replica id aggregating pressure across a deployment.
	PressureTargetTotal = "total"

	pressureTTL    = 30 * time.Second
	affinityTTL    = 10 * time.Minute
	stateOpTimeout = time.Second
)

// State stores pressure and affinity in Redis under a caller-provided key prefix.
type State struct {
	rdb    *common.RedisClient
	prefix string
}

// NewState returns a State namespaced by prefix. A nil rdb makes every operation a no-op.
func NewState(rdb *common.RedisClient, prefix string) *State {
	return &State{rdb: rdb, prefix: prefix}
}

func (s *State) enabled() bool { return s != nil && s.rdb != nil }

func (s *State) key(kind, id string) string { return s.prefix + ":llm_" + kind + ":" + id }

// Pressure reads the pressure snapshot for a replica id or PressureTargetTotal.
func (s *State) Pressure(ctx context.Context, target string) (Pressure, error) {
	if !s.enabled() || target == "" {
		return Pressure{}, nil
	}
	values, err := s.rdb.HMGet(ctx, s.key("pressure", target), "active_streams", "token_pressure").Result()
	if err != nil {
		return Pressure{}, err
	}
	return Pressure{ActiveStreams: fieldInt64(values[0]), TokenPressure: fieldInt64(values[1])}, nil
}

// AddPressure adjusts pressure for the total aggregate and, if non-empty, replicaID.
func (s *State) AddPressure(ctx context.Context, replicaID string, activeStreamsDelta, tokenPressureDelta int64) error {
	if !s.enabled() {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, target := range []string{PressureTargetTotal, replicaID} {
		if target == "" {
			continue
		}
		key := s.key("pressure", target)
		pipe.HIncrBy(ctx, key, "active_streams", activeStreamsDelta)
		pipe.HIncrBy(ctx, key, "token_pressure", tokenPressureDelta)
		pipe.Expire(ctx, key, pressureTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Affinity resolves the exact and prefix-block affinity for a request.
func (s *State) Affinity(ctx context.Context, info *RequestInfo) Affinity {
	affinity := Affinity{PrefixMatches: map[string]int{}}
	if !s.enabled() {
		return affinity
	}
	ctx, cancel := context.WithTimeout(ctx, stateOpTimeout)
	defer cancel()

	if info.AffinityKey != "" {
		if replicaID, err := s.rdb.Get(ctx, s.key("affinity", info.AffinityKey)).Result(); err == nil {
			affinity.ExactID = replicaID
		}
	}
	affinity.ExactIsSession = info.SessionHash != "" && info.AffinityKey == info.SessionHash

	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, 0, len(info.PrefixBlocks))
	for _, block := range info.PrefixBlocks {
		cmds = append(cmds, pipe.Get(ctx, s.key("prefix_affinity", block)))
	}
	_, _ = pipe.Exec(ctx)
	for _, cmd := range cmds {
		if replicaID, err := cmd.Result(); err == nil && replicaID != "" {
			affinity.PrefixMatches[replicaID]++
		}
	}
	return affinity
}

// RecordAffinity remembers that replicaID served info's session/prefix.
func (s *State) RecordAffinity(ctx context.Context, info *RequestInfo, replicaID string) {
	if !s.enabled() || replicaID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, stateOpTimeout)
	defer cancel()

	pipe := s.rdb.Pipeline()
	if info.AffinityKey != "" {
		pipe.Set(ctx, s.key("affinity", info.AffinityKey), replicaID, affinityTTL)
	}
	for _, block := range info.PrefixBlocks {
		pipe.Set(ctx, s.key("prefix_affinity", block), replicaID, affinityTTL)
	}
	_, _ = pipe.Exec(ctx)
}

// fieldInt64 parses an HMGET result, which is nil or a string.
func fieldInt64(value any) int64 {
	s, _ := value.(string)
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
