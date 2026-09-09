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

	// pressureTTL bounds how long a pressure hash outlives its last update. It
	// is refreshed on every increment and decrement, so it only needs to exceed
	// the longest single generation; pressure is a soft routing signal.
	pressureTTL    = 10 * time.Minute
	affinityTTL    = 10 * time.Minute
	stateOpTimeout = time.Second
)

// addPressureScript applies field deltas to a pressure hash, floors each
// field at zero (a decrement after the key expired must not go negative) and
// refreshes the lease. KEYS[1] is the hash; ARGV is ttl_ms, field, delta, ...
const addPressureScript = `
local key = KEYS[1]
for i = 2, #ARGV, 2 do
  if redis.call("HINCRBY", key, ARGV[i], ARGV[i + 1]) < 0 then
    redis.call("HSET", key, ARGV[i], 0)
  end
end
redis.call("PEXPIRE", key, ARGV[1])
return 1
`

// reserveStreamScript takes one active stream on a replica's pressure hash
// unless that would exceed the bound (ARGV[2], 0 for unbounded), in which case
// nothing changes. ARGV is ttl_ms, max_streams, token_delta.
const reserveStreamScript = `
local key = KEYS[1]
local n = redis.call("HINCRBY", key, "active_streams", 1)
local max = tonumber(ARGV[2])
if max > 0 and n > max then
  redis.call("HINCRBY", key, "active_streams", -1)
  redis.call("PEXPIRE", key, ARGV[1])
  return 0
end
redis.call("HINCRBY", key, "token_pressure", ARGV[3])
redis.call("PEXPIRE", key, ARGV[1])
return 1
`

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

// AddPressure adjusts pressure for the total aggregate and, if non-empty,
// replicaID. Counters never go below zero and every call renews the lease, so
// a stream that outlives pressureTTL cannot corrupt a recreated hash.
func (s *State) AddPressure(ctx context.Context, replicaID string, activeStreamsDelta, tokenPressureDelta int64) error {
	if !s.enabled() {
		return nil
	}
	// One EVAL per key: the targets hash to different slots in cluster mode.
	pipe := s.rdb.Pipeline()
	for _, target := range []string{PressureTargetTotal, replicaID} {
		if target == "" {
			continue
		}
		pipe.Eval(ctx, addPressureScript, []string{s.key("pressure", target)},
			pressureTTL.Milliseconds(), "active_streams", activeStreamsDelta, "token_pressure", tokenPressureDelta)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Reserve atomically takes one active stream on replicaID, refusing when the
// replica already has maxStreams in flight across every gateway (0 means no
// bound). It is the shared counterpart of a gateway-local inflight counter;
// release with AddPressure(replicaID, -1, -tokenPressure). A disabled state
// always admits.
func (s *State) Reserve(ctx context.Context, replicaID string, tokenPressure, maxStreams int64) (bool, error) {
	if !s.enabled() || replicaID == "" {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(ctx, stateOpTimeout)
	defer cancel()
	pipe := s.rdb.Pipeline()
	reserved := pipe.Eval(ctx, reserveStreamScript, []string{s.key("pressure", replicaID)}, pressureTTL.Milliseconds(), maxStreams, tokenPressure)
	pipe.Eval(ctx, addPressureScript, []string{s.key("pressure", PressureTargetTotal)}, pressureTTL.Milliseconds(), "active_streams", 1, "token_pressure", tokenPressure)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	if n, _ := reserved.Int64(); n == 1 {
		return true, nil
	}
	// Refused: the total was bumped optimistically alongside; take it back.
	_ = s.rdb.Eval(ctx, addPressureScript, []string{s.key("pressure", PressureTargetTotal)}, pressureTTL.Milliseconds(), "active_streams", -1, "token_pressure", -tokenPressure).Err()
	return false, nil
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
