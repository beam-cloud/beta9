package repository

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
//	metrics:<id>:<gpu>:<version>:<minute>             HASH counters
//	usage:<spend|earned>:<workspace>:<day>            HASH counters, fields "<model>|<counter>"
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

// deleteIndexed deletes keys and removes member from every index set atomically.
func (r *ManagedEndpointRedisRepository) deleteIndexed(ctx context.Context, keys []string, member string, indexKeys ...string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, keys...)
		for _, index := range indexKeys {
			pipe.SRem(ctx, index, member)
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

// --- registry ------------------------------------------------------------------

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

func (r *ManagedEndpointRedisRepository) DeleteEndpoint(ctx context.Context, endpointID string) error {
	return r.deleteIndexed(ctx, []string{meKey("endpoint", endpointID)}, endpointID, meKey("endpoints"))
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
	if fleet.Replicas == nil {
		fleet.Replicas = map[string]map[string]uint32{}
	}
	return fleet, nil
}

// --- replicas ------------------------------------------------------------------

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
	keys, indexes := []string{meKey("replica", replicaID), meKey("drain", replicaID)}, []string{meKey("replicas")}
	if replica != nil {
		indexes = append(indexes, meKey("replicas", replica.EndpointID))
	}
	if replica != nil && replica.ContainerID != "" {
		keys = append(keys, meKey("replica_container", replica.ContainerID))
	}
	return r.deleteIndexed(ctx, keys, replicaID, indexes...)
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

// NotifyReplicaConfig wakes the replica's WatchConfig stream after its
// config was saved with a new revision.
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

// --- gitops ------------------------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveGitOpsState(ctx context.Context, state *types.GitOpsState) error {
	if state == nil {
		return errors.New("gitops state is required")
	}
	state.UpdatedAt = time.Now()
	return r.setJSON(ctx, meKey("gitops"), state, 0)
}

func (r *ManagedEndpointRedisRepository) GetGitOpsState(ctx context.Context) (*types.GitOpsState, error) {
	state, err := getJSON[types.GitOpsState](ctx, r.rdb, meKey("gitops"))
	if state != nil && state.PerEndpoint == nil {
		state.PerEndpoint = map[string]types.GitOpsEndpointState{}
	}
	return state, err
}

// --- route metrics -----------------------------------------------------------

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

// RecordRouteSample increments the minute bucket for the endpoint, for its
// GPU type and for the exact replica, so tuning can read one replica's traffic
// apart from the fleet's.
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

// GetRouteMetrics sums the window for the endpoint (gpu and replica empty),
// one GPU type, or one replica (its gpu must be given).
func (r *ManagedEndpointRedisRepository) GetRouteMetrics(ctx context.Context, endpointID, gpu, replicaID string, window time.Duration) (*types.RouteMetrics, error) {
	if window <= 0 {
		window = 5 * time.Minute
	}
	window = min(window, managedEndpointMetricsRetain-time.Hour)
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
	metrics := &types.RouteMetrics{EndpointID: endpointID, ReplicaID: replicaID, Window: window}
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
	return r.setJSON(ctx, meKey("generation", record.RequestID), record, ttl)
}

func (r *ManagedEndpointRedisRepository) GetGeneration(ctx context.Context, generationID string) (*types.EventEndpointRouteSchema, error) {
	return getJSON[types.EventEndpointRouteSchema](ctx, r.rdb, meKey("generation", generationID))
}

// --- usage -------------------------------------------------------------------

func usageFields(u *types.Usage) map[string]*int64 {
	return map[string]*int64{
		"requests": &u.Requests, "prompt_tokens": &u.PromptTokens, "completion_tokens": &u.CompletionTokens,
		"images": &u.Images, "micro_usd": &u.MicroUSD,
	}
}

// AddUsage credits one request to a workspace's daily bucket for kind
// ("spend" for the caller, "earned" for the provider of the machine that
// served it), both in total and under the model.
func (r *ManagedEndpointRedisRepository) AddUsage(ctx context.Context, kind types.UsageKind, workspaceID, model, requestID string, at time.Time, delta types.Usage) error {
	if workspaceID == "" || model == "" || requestID == "" {
		return errors.New("workspace id, model and request id are required")
	}
	// One request is counted once per leg: a replayed request id is a no-op.
	fresh, err := r.rdb.SetNX(ctx, meKey("usage", "seen", string(kind), requestID), "1", usageRetain).Result()
	if err != nil || !fresh {
		return err
	}
	key := meKey("usage", string(kind), workspaceID, at.UTC().Format(time.DateOnly))
	_, err = r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for field, value := range usageFields(&delta) {
			if *value <= 0 {
				continue
			}
			pipe.HIncrBy(ctx, key, field, *value)
			pipe.HIncrBy(ctx, key, model+"|"+field, *value)
		}
		pipe.Expire(ctx, key, usageRetain)
		return nil
	})
	return err
}

// GetUsage folds the UTC days from..to (inclusive, clamped to today and to
// usageMaxDays) into totals, per model and per day.
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
