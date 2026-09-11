package repository

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	managedEndpointPrefix        = "managed_endpoint"
	managedEndpointMetricsBucket = time.Minute
	managedEndpointMetricsRetain = 25 * time.Hour
	usageRetain                  = 95 * 24 * time.Hour
	usageMaxDays                 = 90
	metricsAggregateGPU          = "_all"
	metricsAggregateReplica      = "-"
)

// ManagedEndpointRedisRepository implements ManagedEndpointRepository on Redis. Keys (under managed_endpoint:):
//
//	endpoint:<id>, replica:<rid>                      JSON, indexed by the sets endpoints, replicas, replicas:<id>
//	replica_container:<cid> -> rid; replica_lock:<rid>, drain:<rid>, backoff:<id>:<gpu>
//	fleet, gitops, generation:<request_id>            JSON
//	config_events:<rid>                               pub/sub, replica config revision numbers
//	metrics:<id>:<gpu>:<replica[@revision]>:<minute>  HASH counters
//	usage:seen:<kind>:<request_id>                    request dedupe marker
//	usage:<spend|earned>:<workspace>:<day>            HASH counters, fields "<model>|<counter>"
//	meter:<kind>:<minute>, meter:buckets              HASH counters "<workspace>|<model>|<counter>" awaiting the billing flush, and their ZSET index
type ManagedEndpointRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewManagedEndpointRedisRepository(rdb *common.RedisClient) ManagedEndpointRepository {
	return &ManagedEndpointRedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

func meKey(parts ...string) string {
	return managedEndpointPrefix + ":" + strings.Join(parts, ":")
}

func meKeys(members []string, prefix ...string) []string {
	p := strings.Join(prefix, ":")
	keys := make([]string, 0, len(members))
	for _, m := range members {
		keys = append(keys, meKey(p, m))
	}
	return keys
}

func u64(n uint64) string { return strconv.FormatUint(n, 10) }

// getJSON returns nil, nil when the key does not exist.
func getJSON[T any](ctx context.Context, rdb *common.RedisClient, key string) (*T, error) {
	out, err := listJSON[T](ctx, rdb, []string{key})
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out[0], nil
}

// listIndexed MGETs meKey(keyPrefix..., member) for every member of the index set, sorted by less.
func listIndexed[T any](ctx context.Context, rdb *common.RedisClient, indexKey string, less func(a, b *T) bool, keyPrefix ...string) ([]*T, error) {
	members, err := rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	out, err := listJSON[T](ctx, rdb, meKeys(members, keyPrefix...))
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) setJSON(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, key, data, ttl).Err()
}

// saveIndexed writes the JSON value and adds member to the index set atomically; extra may add commands.
func (r *ManagedEndpointRedisRepository) saveIndexed(ctx context.Context, key, indexKey, member string, v any, extra func(redis.Pipeliner)) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, key, data, 0)
		pipe.SAdd(ctx, indexKey, member)
		if extra != nil {
			extra(pipe)
		}
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) hgetAll(ctx context.Context, keys []string) ([]map[string]string, error) {
	cmds := make([]*redis.MapStringStringCmd, 0, len(keys))
	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			cmds = append(cmds, pipe.HGetAll(ctx, key))
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]map[string]string, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, cmd.Val())
	}
	return out, nil
}

func (r *ManagedEndpointRedisRepository) SaveEndpoint(ctx context.Context, endpoint *types.ManagedEndpoint) error {
	if endpoint == nil || endpoint.Spec.ID == "" {
		return errors.New("endpoint id is required")
	}
	endpoint.UpdatedAt = time.Now()
	endpoint.CreatedAt = cmp.Or(endpoint.CreatedAt, endpoint.UpdatedAt)
	return r.saveIndexed(ctx, meKey("endpoint", endpoint.Spec.ID), meKey("endpoints"), endpoint.Spec.ID, endpoint, nil)
}

func (r *ManagedEndpointRedisRepository) GetEndpoint(ctx context.Context, endpointID string) (*types.ManagedEndpoint, error) {
	return getJSON[types.ManagedEndpoint](ctx, r.rdb, meKey("endpoint", endpointID))
}

func (r *ManagedEndpointRedisRepository) ListEndpoints(ctx context.Context) ([]*types.ManagedEndpoint, error) {
	return listIndexed(ctx, r.rdb, meKey("endpoints"), func(a, b *types.ManagedEndpoint) bool { return a.Spec.ID < b.Spec.ID }, "endpoint")
}

func (r *ManagedEndpointRedisRepository) SaveFleet(ctx context.Context, fleet *types.Fleet) error {
	if fleet == nil {
		return errors.New("fleet is required")
	}
	fleet.UpdatedAt = time.Now()
	return r.setJSON(ctx, meKey("fleet"), fleet, 0)
}

// GetFleet returns the stored fleet, or an empty one when none was applied yet.
func (r *ManagedEndpointRedisRepository) GetFleet(ctx context.Context) (*types.Fleet, error) {
	fleet, err := getJSON[types.Fleet](ctx, r.rdb, meKey("fleet"))
	if err != nil {
		return nil, err
	}
	if fleet == nil {
		fleet = &types.Fleet{}
	}
	if fleet.Endpoints == nil {
		fleet.Endpoints = map[string]types.FleetEndpoint{}
	}
	return fleet, nil
}

func (r *ManagedEndpointRedisRepository) SaveReplica(ctx context.Context, replica *types.EndpointReplica) error {
	if replica == nil || replica.ID == "" || replica.EndpointID == "" {
		return errors.New("replica id and endpoint id are required")
	}
	return r.saveIndexed(ctx, meKey("replica", replica.ID), meKey("replicas"), replica.ID, replica, func(pipe redis.Pipeliner) {
		pipe.SAdd(ctx, meKey("replicas", replica.EndpointID), replica.ID)
		if replica.ContainerID != "" {
			pipe.Set(ctx, meKey("replica_container", replica.ContainerID), replica.ID, 0)
		}
	})
}

func (r *ManagedEndpointRedisRepository) GetReplica(ctx context.Context, replicaID string) (*types.EndpointReplica, error) {
	return getJSON[types.EndpointReplica](ctx, r.rdb, meKey("replica", replicaID))
}

func (r *ManagedEndpointRedisRepository) GetReplicaByContainer(ctx context.Context, containerID string) (*types.EndpointReplica, error) {
	replicaID, err := r.rdb.Get(ctx, meKey("replica_container", containerID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return r.GetReplica(ctx, replicaID)
}

func (r *ManagedEndpointRedisRepository) ListReplicas(ctx context.Context, endpointID string) ([]*types.EndpointReplica, error) {
	return listIndexed(ctx, r.rdb, meKey("replicas", endpointID), func(a, b *types.EndpointReplica) bool { return a.ID < b.ID }, "replica")
}

func (r *ManagedEndpointRedisRepository) ListAllReplicas(ctx context.Context) ([]*types.EndpointReplica, error) {
	return listIndexed(ctx, r.rdb, meKey("replicas"), func(a, b *types.EndpointReplica) bool { return a.ID < b.ID }, "replica")
}

func (r *ManagedEndpointRedisRepository) DeleteReplica(ctx context.Context, replicaID string) error {
	replica, err := r.GetReplica(ctx, replicaID)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, meKey("replica", replicaID), meKey("drain", replicaID))
		pipe.SRem(ctx, meKey("replicas"), replicaID)
		if replica != nil {
			pipe.SRem(ctx, meKey("replicas", replica.EndpointID), replicaID)
			if replica.ContainerID != "" {
				pipe.Del(ctx, meKey("replica_container", replica.ContainerID))
			}
		}
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) WithReplicaLock(ctx context.Context, replicaID string, fn func(context.Context) error) error {
	opts := common.RedisLockOptions{TtlS: 10, Retries: 50, RetryInterval: 50 * time.Millisecond}
	return r.lock.WithLease(ctx, meKey("replica_lock", replicaID), opts, fn)
}

func (r *ManagedEndpointRedisRepository) RequestDrain(ctx context.Context, replicaID string, drainSeconds uint32) error {
	return r.rdb.Set(ctx, meKey("drain", replicaID), u64(uint64(drainSeconds)), 10*time.Minute).Err()
}

func (r *ManagedEndpointRedisRepository) DrainRequested(ctx context.Context, replicaID string) (bool, uint32, error) {
	value, err := r.rdb.Get(ctx, meKey("drain", replicaID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, 0, nil
		}
		return false, 0, err
	}
	seconds, _ := strconv.ParseUint(value, 10, 32)
	return true, uint32(seconds), nil
}

func (r *ManagedEndpointRedisRepository) SetScheduleBackoff(ctx context.Context, endpointID, gpu string, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	return r.rdb.Set(ctx, meKey("backoff", endpointID, gpu), "1", ttl).Err()
}

func (r *ManagedEndpointRedisRepository) InScheduleBackoff(ctx context.Context, endpointID, gpu string) (bool, error) {
	n, err := r.rdb.Exists(ctx, meKey("backoff", endpointID, gpu)).Result()
	return n > 0, err
}

func (r *ManagedEndpointRedisRepository) NotifyReplicaConfig(ctx context.Context, replicaID string, revision uint64) error {
	return r.rdb.Publish(ctx, meKey("config_events", replicaID), u64(revision)).Err()
}

func (r *ManagedEndpointRedisRepository) SubscribeReplicaConfig(ctx context.Context, replicaID string) (<-chan uint64, error) {
	messages, errs := r.rdb.Subscribe(ctx, meKey("config_events", replicaID))
	out := make(chan uint64, 16)
	go func() {
		defer close(out)
		for message := range messages {
			revision, err := strconv.ParseUint(message.Payload, 10, 64)
			if err != nil {
				continue
			}
			select {
			case out <- revision:
			case <-ctx.Done():
				return
			}
		}
		if err := <-errs; err != nil {
			log.Warn().Err(err).Str("replica_id", replicaID).Msg("managed endpoint config subscription error")
		}
	}()
	return out, nil
}

func (r *ManagedEndpointRedisRepository) SaveGitOpsState(ctx context.Context, state *types.GitOpsState) error {
	if state == nil {
		return errors.New("gitops state is required")
	}
	return r.setJSON(ctx, meKey("gitops"), state, 0)
}

func (r *ManagedEndpointRedisRepository) GetGitOpsState(ctx context.Context) (*types.GitOpsState, error) {
	state, err := getJSON[types.GitOpsState](ctx, r.rdb, meKey("gitops"))
	if state != nil && state.PerEndpoint == nil {
		state.PerEndpoint = map[string]types.GitOpsEndpointState{}
	}
	return state, err
}

func metricsKey(endpointID, gpu, replica string, bucket time.Time) string {
	return meKey("metrics", endpointID, gpu, replica, strconv.FormatInt(bucket.Unix(), 10))
}

func routeMetricsFields(m *types.RouteMetrics) map[string]*int64 {
	return map[string]*int64{
		"requests": &m.Requests, "errors": &m.Errors, "prompt_tokens": &m.PromptTokens, "completion_tokens": &m.CompletionTokens,
		"images": &m.Images, "cost_micro_usd": &m.CostMicroUSD, "duration_sum_ms": &m.DurationSumMs,
		"ttft_sum_ms": &m.TTFTSumMs, "ttft_count": &m.TTFTCount, "queue_wait_sum_ms": &m.QueueWaitSumMs,
	}
}

// RecordRouteSample increments the minute buckets for the endpoint, its GPU type and the replica.
func (r *ManagedEndpointRedisRepository) RecordRouteSample(ctx context.Context, sample types.RouteSample) error {
	if sample.EndpointID == "" {
		return errors.New("endpoint id is required")
	}
	bucket := cmp.Or(sample.At, time.Now()).Truncate(managedEndpointMetricsBucket)
	gpu := cmp.Or(sample.GPU, metricsAggregateGPU)
	keys := map[string]struct{}{
		metricsKey(sample.EndpointID, metricsAggregateGPU, metricsAggregateReplica, bucket): {},
		metricsKey(sample.EndpointID, gpu, metricsAggregateReplica, bucket):                 {},
	}
	if sample.ReplicaID != "" {
		keys[metricsKey(sample.EndpointID, gpu, sample.ReplicaID, bucket)] = struct{}{}
		if sample.ConfigRevision > 0 {
			keys[metricsKey(sample.EndpointID, gpu, replicaRevisionKey(sample.ReplicaID, sample.ConfigRevision), bucket)] = struct{}{}
		}
	}
	delta := types.RouteMetrics{
		Requests: 1, PromptTokens: sample.PromptTokens, CompletionTokens: sample.CompletionTokens, Images: sample.Images,
		CostMicroUSD: sample.CostMicroUSD, DurationSumMs: sample.Duration.Milliseconds(), TTFTSumMs: sample.TTFT.Milliseconds(), QueueWaitSumMs: sample.QueueWait.Milliseconds(),
	}
	if sample.Failed() {
		delta.Errors = 1
	}
	if sample.TTFT > 0 {
		delta.TTFTCount = 1
	}
	fields := routeMetricsFields(&delta)
	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key := range keys {
			for field, value := range fields {
				if *value > 0 {
					pipe.HIncrBy(ctx, key, field, *value)
				}
			}
			pipe.Expire(ctx, key, managedEndpointMetricsRetain)
		}
		return nil
	})
	return err
}

// replicaRevisionKey scopes a replica's metrics to one live config revision.
func replicaRevisionKey(replicaID string, revision uint64) string {
	return replicaID + "@" + strconv.FormatUint(revision, 10)
}

// GetRouteMetrics sums the window for the endpoint, one GPU type, one replica
// or one replica under one config revision.
func (r *ManagedEndpointRedisRepository) GetRouteMetrics(ctx context.Context, endpointID, gpu, replicaID string, configRevision uint64, window time.Duration) (*types.RouteMetrics, error) {
	if window <= 0 {
		window = 5 * time.Minute
	}
	window = min(window, managedEndpointMetricsRetain-time.Hour)
	if replicaID != "" && configRevision > 0 {
		replicaID = replicaRevisionKey(replicaID, configRevision)
	}
	gpu, replica := cmp.Or(gpu, metricsAggregateGPU), cmp.Or(replicaID, metricsAggregateReplica)
	now := time.Now()
	end := now.Truncate(managedEndpointMetricsBucket)
	var keys []string
	for bucket := now.Add(-window).Truncate(managedEndpointMetricsBucket); !bucket.After(end); bucket = bucket.Add(managedEndpointMetricsBucket) {
		keys = append(keys, metricsKey(endpointID, gpu, replica, bucket))
	}
	buckets, err := r.hgetAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	metrics := &types.RouteMetrics{EndpointID: endpointID, ReplicaID: strings.SplitN(replicaID, "@", 2)[0], ConfigRevision: configRevision, Window: window}
	if gpu != metricsAggregateGPU {
		metrics.GPU = gpu
	}
	fields := routeMetricsFields(metrics)
	for _, values := range buckets {
		for field, total := range fields {
			n, _ := strconv.ParseInt(values[field], 10, 64)
			*total += n
		}
	}
	return metrics, nil
}

func (r *ManagedEndpointRedisRepository) SaveGeneration(ctx context.Context, record *types.EventEndpointRouteSchema, ttl time.Duration) error {
	if record == nil || record.RequestID == "" {
		return errors.New("generation id is required")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	// A successful response is also the durable accounting outbox. Do not
	// expire it until spend and provider earnings have both been applied.
	pending := record.StatusCode >= 200 && record.StatusCode < 300
	return saveGenerationScript.Run(ctx, r.rdb, []string{meKey("generation", record.RequestID), meKey("accounting", "pending")}, string(data), record.RequestID, record.Timestamp.Unix(), int(ttl.Seconds()), pending).Err()
}

var saveGenerationScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local pendingType = redis.call('TYPE', KEYS[2]).ok
if ARGV[5] == '1' and pendingType ~= 'none' and pendingType ~= 'zset' then
	return redis.error_reply('invalid pending accounting index')
end
redis.call('SET', KEYS[1], ARGV[1])
if ARGV[5] == '1' then
	redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
else
	redis.call('EXPIRE', KEYS[1], ARGV[4])
end
return 1
`)

func (r *ManagedEndpointRedisRepository) ListPendingAccounting(ctx context.Context, limit int64) ([]types.EventEndpointRouteSchema, error) {
	ids, err := r.rdb.ZRange(ctx, meKey("accounting", "pending"), 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	records := make([]types.EventEndpointRouteSchema, 0, len(ids))
	for _, id := range ids {
		record, err := r.GetGeneration(ctx, id)
		if err != nil {
			return nil, err
		}
		if record == nil {
			return nil, fmt.Errorf("missing pending accounting record %s", id)
		}
		records = append(records, *record)
	}
	return records, nil
}

var completeAccountingScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return 1
`)

func (r *ManagedEndpointRedisRepository) CompleteAccounting(ctx context.Context, generationID string, ttl time.Duration) error {
	return completeAccountingScript.Run(ctx, r.rdb, []string{meKey("accounting", "pending"), meKey("generation", generationID)}, generationID, int(ttl.Seconds())).Err()
}

func (r *ManagedEndpointRedisRepository) GetGeneration(ctx context.Context, generationID string) (*types.EventEndpointRouteSchema, error) {
	return getJSON[types.EventEndpointRouteSchema](ctx, r.rdb, meKey("generation", generationID))
}

func usageFields(u *types.Usage) map[string]*int64 {
	return map[string]*int64{
		"requests": &u.Requests, "prompt_tokens": &u.PromptTokens, "completion_tokens": &u.CompletionTokens,
		"images": &u.Images, "micro_usd": &u.MicroUSD,
		"cached_tokens":    &u.CachedTokens,
		"prompt_micro_usd": &u.PromptMicroUSD, "completion_micro_usd": &u.CompletionMicroUSD,
		"cached_micro_usd": &u.CachedMicroUSD, "request_micro_usd": &u.RequestMicroUSD, "image_micro_usd": &u.ImageMicroUSD,
	}
}

// addUsageScript records one request atomically (seen marker, day counters,
// minute meter bucket) so a replay with the same request id is a no-op.
//
// KEYS[1] seen marker, KEYS[2] day bucket, KEYS[3] meter bucket index;
// ARGV[1] usage ttl seconds, ARGV[2] meter key prefix, ARGV[3] workspace,
// ARGV[4] model, then (field, value) pairs.
var addUsageScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local minute = math.floor(redis.call('TIME')[1] / 60) * 60
local meter = ARGV[2] .. ':' .. minute
-- Lua scripts do not roll back runtime errors. Validate all increments before
-- writing anything, including types and exact-integer bounds.
local indexType = redis.call('TYPE', KEYS[3]).ok
if indexType ~= 'none' and indexType ~= 'zset' then return redis.error_reply('invalid meter index') end
for _, key in ipairs({KEYS[2], meter}) do
	local kind = redis.call('TYPE', key).ok
	if kind ~= 'none' and kind ~= 'hash' then return redis.error_reply('invalid usage bucket') end
end
local function validIncrement(key, field, amount)
	local raw = redis.call('HGET', key, field)
	if raw and raw ~= '0' and not string.match(raw, '^[1-9][0-9]*$') then return false end
	local value = 0
	if raw then value = tonumber(raw) end
	return value and value >= 0 and value == math.floor(value) and value + amount <= 9007199254740991
end
for i = 5, #ARGV, 2 do
	local amount = tonumber(ARGV[i + 1])
	if not amount or amount < 0 or amount ~= math.floor(amount) or
		not validIncrement(KEYS[2], ARGV[i], amount) or
		not validIncrement(KEYS[2], ARGV[4] .. '|' .. ARGV[i], amount) or
		not validIncrement(meter, ARGV[3] .. '|' .. ARGV[4] .. '|' .. ARGV[i], amount) then
		return redis.error_reply('invalid usage counter or overflow')
	end
end
for i = 5, #ARGV, 2 do
	redis.call('HINCRBY', KEYS[2], ARGV[i], ARGV[i + 1])
	redis.call('HINCRBY', KEYS[2], ARGV[4] .. '|' .. ARGV[i], ARGV[i + 1])
	redis.call('HINCRBY', meter, ARGV[3] .. '|' .. ARGV[4] .. '|' .. ARGV[i], ARGV[i + 1])
end
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
redis.call('EXPIRE', KEYS[2], ARGV[1])
redis.call('PERSIST', meter)
redis.call('ZADD', KEYS[3], minute, meter)
return 1
`)

// AddUsage credits one request to a workspace's daily bucket for kind; a
// replayed request id is a no-op.
func (r *ManagedEndpointRedisRepository) AddUsage(ctx context.Context, kind types.UsageKind, workspaceID, model, requestID string, at time.Time, delta types.Usage) error {
	if workspaceID == "" || model == "" || requestID == "" {
		return errors.New("workspace id, model and request id are required")
	}
	if kind != types.UsageSpend && kind != types.UsageEarned {
		return errors.New("invalid usage kind")
	}
	if strings.ContainsAny(workspaceID+model, "|") {
		return errors.New("invalid usage identifier")
	}
	if delta.CachedTokens > delta.PromptTokens {
		return errors.New("cached tokens exceed prompt tokens")
	}
	args := []any{int(usageRetain.Seconds()), meKey("meter", string(kind)), workspaceID, model}
	for field, value := range usageFields(&delta) {
		if *value < 0 || *value > types.MaxUsageCounter {
			return errors.New("invalid usage counter")
		}
		if *value > 0 {
			args = append(args, field, *value)
		}
	}
	keys := []string{
		meKey("usage", "seen", string(kind), requestID),
		meKey("usage", string(kind), workspaceID, at.UTC().Format(time.DateOnly)),
		meKey("meter", "buckets"),
	}
	return addUsageScript.Run(ctx, r.rdb, keys, args...).Err()
}

// GetUsage folds the UTC days from..to (clamped to usageMaxDays) into a report.
func (r *ManagedEndpointRedisRepository) GetUsage(ctx context.Context, kind types.UsageKind, workspaceID string, from, to time.Time) (*types.UsageReport, error) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	first, last := from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if last.After(today) {
		last = today
	}
	if first.After(last) {
		first = last
	}
	if oldest := last.AddDate(0, 0, -(usageMaxDays - 1)); first.Before(oldest) {
		first = oldest
	}
	var keys, dates []string
	for day := first; !day.After(last); day = day.AddDate(0, 0, 1) {
		dates = append(dates, day.Format(time.DateOnly))
		keys = append(keys, meKey("usage", string(kind), workspaceID, dates[len(dates)-1]))
	}
	buckets, err := r.hgetAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	report := &types.UsageReport{PerModel: map[string]types.Usage{}, PerDay: map[string]types.Usage{}}
	for i, values := range buckets {
		var day types.Usage
		perModel := map[string]*types.Usage{}
		for field, raw := range values {
			n, _ := strconv.ParseInt(raw, 10, 64)
			target := &day
			if model, name, ok := strings.Cut(field, "|"); ok {
				if perModel[model] == nil {
					perModel[model] = &types.Usage{}
				}
				target, field = perModel[model], name
			}
			if slot := usageFields(target)[field]; slot != nil {
				*slot += n
			}
		}
		if day != (types.Usage{}) {
			report.PerDay[dates[i]] = day
			report.Total.Add(day)
		}
		for model, u := range perModel {
			total := report.PerModel[model]
			total.Add(*u)
			report.PerModel[model] = total
		}
	}
	return report, nil
}

// ListMeterBuckets returns every bucket that started before the cutoff, oldest first.
func (r *ManagedEndpointRedisRepository) ListMeterBuckets(ctx context.Context, before time.Time) ([]types.MeterBucket, error) {
	keys, err := r.rdb.ZRangeByScore(ctx, meKey("meter", "buckets"), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(before.Unix(), 10)}).Result()
	if err != nil {
		return nil, err
	}
	var out []types.MeterBucket
	for _, key := range keys {
		// Pending billing must not expire during an extended meter outage.
		if err := r.rdb.Persist(ctx, key).Err(); err != nil {
			return nil, err
		}
		parts := strings.Split(strings.TrimPrefix(key, meKey("meter")+":"), ":")
		if len(parts) != 2 {
			continue
		}
		minute, _ := strconv.ParseInt(parts[1], 10, 64)
		bucket := types.MeterBucket{Key: key, Kind: types.UsageKind(parts[0]), Start: time.Unix(minute, 0).UTC()}
		fields, err := r.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		rows := map[string]*types.MeterRow{}
		for field, raw := range fields {
			workspace, model, name, ok := splitMeterField(field)
			if !ok {
				continue
			}
			row := rows[workspace+"|"+model]
			if row == nil {
				row = &types.MeterRow{WorkspaceID: workspace, Model: model}
				rows[workspace+"|"+model] = row
			}
			if value, ok := usageFields(&row.Usage)[name]; ok {
				*value, _ = strconv.ParseInt(raw, 10, 64)
			}
		}
		for _, row := range rows {
			bucket.Rows = append(bucket.Rows, *row)
		}
		sort.Slice(bucket.Rows, func(i, j int) bool {
			return bucket.Rows[i].WorkspaceID+bucket.Rows[i].Model < bucket.Rows[j].WorkspaceID+bucket.Rows[j].Model
		})
		out = append(out, bucket)
	}
	return out, nil
}

// splitMeterField parses "<workspace>|<model>|<field>"; models contain no "|".
func splitMeterField(field string) (workspace, model, name string, ok bool) {
	i := strings.Index(field, "|")
	j := strings.LastIndex(field, "|")
	if i < 0 || j <= i {
		return "", "", "", false
	}
	return field[:i], field[i+1 : j], field[j+1:], true
}

// DeleteMeterBucket removes a delivered bucket.
func (r *ManagedEndpointRedisRepository) DeleteMeterBucket(ctx context.Context, key string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, meKey("meter", "buckets"), key)
		pipe.Del(ctx, key)
		return nil
	})
	return err
}
