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
	managedEndpointRevisionKeep  = 200
	providerEarningsRetain       = 95 * 24 * time.Hour
	providerEarningsMaxDays      = 90
	metricsAggregateGPU          = "_all"
	metricsAggregateVersion      = "0"
)

// ManagedEndpointRedisRepository implements ManagedEndpointRepository on Redis. Keys (under managed_endpoint:):
//
//	endpoint:<id>, service:<name>, version:<id>:<n>, replica:<rid>   JSON, indexed by the sets endpoints, services, versions:<id>, replicas, replicas:<id>
//	replica_container:<cid> -> rid; replica_lock:<rid>, drain:<rid>, backoff:<id>:<target>
//	rollout:<id>, gitops, config_ack:<rid>:<rev>, generation:<request_id>   JSON
//	config_seq:<id> INCR; config:<id>:<rev> JSON; config_index:<id>:<scope>:<key> ZSET rev; config_events:<id> pub/sub
//	metrics:<id>:<gpu>:<version>:<minute>, earnings:<workspace>:<day>       HASH counters
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
	return r.deleteIndexed(ctx, []string{meKey("endpoint", endpointID), meKey("rollout", endpointID)}, endpointID, meKey("endpoints"))
}

func (r *ManagedEndpointRedisRepository) SaveService(ctx context.Context, service *types.ManagedService) error {
	if service == nil || service.Spec.Name == "" {
		return errors.New("service name is required")
	}
	service.UpdatedAt = time.Now()
	service.CreatedAt = cmp.Or(service.CreatedAt, service.UpdatedAt)
	return r.saveIndexed(ctx, meKey("service", service.Spec.Name), meKey("services"), service.Spec.Name, service, nil)
}

func (r *ManagedEndpointRedisRepository) GetService(ctx context.Context, name string) (*types.ManagedService, error) {
	return getJSON[types.ManagedService](ctx, r.rdb, meKey("service", name))
}

func (r *ManagedEndpointRedisRepository) ListServices(ctx context.Context) ([]*types.ManagedService, error) {
	return listIndexed(ctx, r.rdb, meKey("services"), func(a, b *types.ManagedService) bool { return a.Spec.Name < b.Spec.Name }, "service")
}

func (r *ManagedEndpointRedisRepository) DeleteService(ctx context.Context, name string) error {
	return r.deleteIndexed(ctx, []string{meKey("service", name)}, name, meKey("services"))
}

func (r *ManagedEndpointRedisRepository) SaveVersion(ctx context.Context, version *types.EndpointVersion) error {
	if version == nil || version.EndpointID == "" || version.Version == 0 {
		return errors.New("endpoint id and version are required")
	}
	version.UpdatedAt = time.Now()
	version.CreatedAt = cmp.Or(version.CreatedAt, version.UpdatedAt)
	return r.saveIndexed(ctx, meKey("version", version.EndpointID, u64(uint64(version.Version))), meKey("versions", version.EndpointID), u64(uint64(version.Version)), version, nil)
}

func (r *ManagedEndpointRedisRepository) ListVersions(ctx context.Context, endpointID string) ([]*types.EndpointVersion, error) {
	return listIndexed(ctx, r.rdb, meKey("versions", endpointID), func(a, b *types.EndpointVersion) bool { return a.Version < b.Version }, "version", endpointID)
}

func (r *ManagedEndpointRedisRepository) SaveRollout(ctx context.Context, rollout *types.RolloutState) error {
	if rollout == nil || rollout.EndpointID == "" {
		return errors.New("endpoint id is required")
	}
	rollout.UpdatedAt = time.Now()
	return r.setJSON(ctx, meKey("rollout", rollout.EndpointID), rollout, 0)
}

func (r *ManagedEndpointRedisRepository) GetRollout(ctx context.Context, endpointID string) (*types.RolloutState, error) {
	return getJSON[types.RolloutState](ctx, r.rdb, meKey("rollout", endpointID))
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

func (r *ManagedEndpointRedisRepository) SetScheduleBackoff(ctx context.Context, endpointID, targetKey string, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	return r.rdb.Set(ctx, meKey("backoff", endpointID, targetKey), "1", ttl).Err()
}

func (r *ManagedEndpointRedisRepository) InScheduleBackoff(ctx context.Context, endpointID, targetKey string) (bool, error) {
	n, err := r.rdb.Exists(ctx, meKey("backoff", endpointID, targetKey)).Result()
	return n > 0, err
}

func (r *ManagedEndpointRedisRepository) CreateConfigRevision(ctx context.Context, revision *types.EndpointConfigRevision) error {
	if revision == nil || revision.EndpointID == "" || revision.Scope == "" || revision.ScopeKey == "" {
		return errors.New("endpoint id, scope and scope key are required")
	}
	if revision.Config == nil {
		revision.Config = map[string]any{}
	}
	seq, err := r.rdb.Incr(ctx, meKey("config_seq", revision.EndpointID)).Result()
	if err != nil {
		return err
	}
	revision.Revision = uint64(seq)
	revision.CreatedAt = time.Now()
	data, err := json.Marshal(revision)
	if err != nil {
		return err
	}
	index := meKey("config_index", revision.EndpointID, string(revision.Scope), revision.ScopeKey)
	rev := u64(revision.Revision)
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("config", revision.EndpointID, rev), data, 0)
		pipe.ZAdd(ctx, index, redis.Z{Score: float64(revision.Revision), Member: rev})
		pipe.ZRemRangeByRank(ctx, index, 0, -managedEndpointRevisionKeep-1)
		pipe.Publish(ctx, meKey("config_events", revision.EndpointID), data)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetConfigRevision(ctx context.Context, endpointID string, revision uint64) (*types.EndpointConfigRevision, error) {
	return getJSON[types.EndpointConfigRevision](ctx, r.rdb, meKey("config", endpointID, u64(revision)))
}

func (r *ManagedEndpointRedisRepository) LatestConfigRevision(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string) (*types.EndpointConfigRevision, error) {
	revisions, err := r.ListConfigRevisions(ctx, endpointID, scope, scopeKey, 1)
	if err != nil || len(revisions) == 0 {
		return nil, err
	}
	return revisions[0], nil
}

// ListConfigRevisions returns up to limit revisions, newest first.
func (r *ManagedEndpointRedisRepository) ListConfigRevisions(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string, limit int) ([]*types.EndpointConfigRevision, error) {
	if limit <= 0 {
		limit = 20
	}
	members, err := r.rdb.ZRevRange(ctx, meKey("config_index", endpointID, string(scope), scopeKey), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out, err := listJSON[types.EndpointConfigRevision](ctx, r.rdb, meKeys(members, "config", endpointID))
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Revision > out[j].Revision })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) DeleteConfigRevisions(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string) error {
	index := meKey("config_index", endpointID, string(scope), scopeKey)
	members, err := r.rdb.ZRange(ctx, index, 0, -1).Result()
	if err != nil {
		return err
	}
	return r.rdb.Del(ctx, append(meKeys(members, "config", endpointID), index)...).Err()
}

func (r *ManagedEndpointRedisRepository) SubscribeConfigRevisions(ctx context.Context, endpointID string) (<-chan *types.EndpointConfigRevision, error) {
	messages, errs := r.rdb.Subscribe(ctx, meKey("config_events", endpointID))
	out := make(chan *types.EndpointConfigRevision, 16)
	go func() {
		defer close(out)
		for message := range messages {
			var revision types.EndpointConfigRevision
			if err := json.Unmarshal([]byte(message.Payload), &revision); err != nil {
				continue
			}
			select {
			case out <- &revision:
			case <-ctx.Done():
				return
			}
		}
		if err := <-errs; err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpointID).Msg("managed endpoint config subscription error")
		}
	}()
	return out, nil
}

func (r *ManagedEndpointRedisRepository) SaveConfigAck(ctx context.Context, ack *types.ConfigAck) error {
	if ack == nil || ack.ReplicaID == "" {
		return errors.New("replica id is required")
	}
	ack.At = cmp.Or(ack.At, time.Now())
	return r.setJSON(ctx, meKey("config_ack", ack.ReplicaID, u64(ack.Revision)), ack, 24*time.Hour)
}

func (r *ManagedEndpointRedisRepository) GetConfigAck(ctx context.Context, replicaID string, revision uint64) (*types.ConfigAck, error) {
	return getJSON[types.ConfigAck](ctx, r.rdb, meKey("config_ack", replicaID, u64(revision)))
}

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

func metricsKey(endpointID, gpu, version string, bucket time.Time) string {
	return meKey("metrics", endpointID, gpu, version, strconv.FormatInt(bucket.Unix(), 10))
}

func routeMetricsFields(m *types.RouteMetrics) map[string]*int64 {
	return map[string]*int64{
		"requests": &m.Requests, "errors": &m.Errors, "prompt_tokens": &m.PromptTokens, "completion_tokens": &m.CompletionTokens,
		"images": &m.Images, "cost_micro_usd": &m.CostMicroUSD, "duration_sum_ms": &m.DurationSumMs,
		"ttft_sum_ms": &m.TTFTSumMs, "ttft_count": &m.TTFTCount, "queue_wait_sum_ms": &m.QueueWaitSumMs,
	}
}

// RecordRouteSample increments the minute bucket for the exact (gpu, version) and for the aggregates.
func (r *ManagedEndpointRedisRepository) RecordRouteSample(ctx context.Context, sample types.RouteSample) error {
	if sample.EndpointID == "" {
		return errors.New("endpoint id is required")
	}
	bucket := cmp.Or(sample.At, time.Now()).Truncate(managedEndpointMetricsBucket)
	gpu, version := cmp.Or(sample.GPU, metricsAggregateGPU), u64(uint64(sample.Version))
	keys := map[string]struct{}{
		metricsKey(sample.EndpointID, metricsAggregateGPU, metricsAggregateVersion, bucket): {},
		metricsKey(sample.EndpointID, gpu, metricsAggregateVersion, bucket):                 {},
		metricsKey(sample.EndpointID, metricsAggregateGPU, version, bucket):                 {},
		metricsKey(sample.EndpointID, gpu, version, bucket):                                 {},
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

func (r *ManagedEndpointRedisRepository) GetRouteMetrics(ctx context.Context, endpointID, gpu string, version uint, window time.Duration) (*types.RouteMetrics, error) {
	if window <= 0 {
		window = 5 * time.Minute
	}
	window = min(window, managedEndpointMetricsRetain-time.Hour)
	gpu = cmp.Or(gpu, metricsAggregateGPU)
	now := time.Now()
	end := now.Truncate(managedEndpointMetricsBucket)
	var keys []string
	for bucket := now.Add(-window).Truncate(managedEndpointMetricsBucket); !bucket.After(end); bucket = bucket.Add(managedEndpointMetricsBucket) {
		keys = append(keys, metricsKey(endpointID, gpu, u64(uint64(version)), bucket))
	}
	buckets, err := r.hgetAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	metrics := &types.RouteMetrics{EndpointID: endpointID, Version: version, Window: window}
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

func providerEarningsFields(e *types.ProviderEarnings) map[string]*int64 {
	return map[string]*int64{
		"requests": &e.Requests, "prompt_tokens": &e.PromptTokens, "completion_tokens": &e.CompletionTokens,
		"images": &e.Images, "earnings_micro_usd": &e.EarningsMicroUSD,
	}
}

// AddProviderEarnings credits one served request to the provider workspace's
// daily bucket, both in total and under a "m:<machine>:" field prefix.
func (r *ManagedEndpointRedisRepository) AddProviderEarnings(ctx context.Context, workspaceID, machineID string, at time.Time, delta types.ProviderEarnings) error {
	if workspaceID == "" {
		return errors.New("provider workspace id is required")
	}
	key := meKey("earnings", workspaceID, at.UTC().Format(time.DateOnly))
	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for field, value := range providerEarningsFields(&delta) {
			if *value <= 0 {
				continue
			}
			pipe.HIncrBy(ctx, key, field, *value)
			if machineID != "" {
				pipe.HIncrBy(ctx, key, "m:"+machineID+":"+field, *value)
			}
		}
		pipe.Expire(ctx, key, providerEarningsRetain)
		return nil
	})
	return err
}

// GetProviderEarnings folds the trailing days (today included) into totals,
// per machine and per day.
func (r *ManagedEndpointRedisRepository) GetProviderEarnings(ctx context.Context, workspaceID string, days int) (*types.ProviderEarningsReport, error) {
	days = max(1, min(days, providerEarningsMaxDays))
	today := time.Now().UTC().Truncate(24 * time.Hour)
	keys, dates := make([]string, 0, days), make([]string, 0, days)
	for i := days - 1; i >= 0; i-- {
		day := today.AddDate(0, 0, -i).Format(time.DateOnly)
		dates = append(dates, day)
		keys = append(keys, meKey("earnings", workspaceID, day))
	}
	buckets, err := r.hgetAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	report := &types.ProviderEarningsReport{PerMachine: map[string]types.ProviderEarnings{}, PerDay: map[string]types.ProviderEarnings{}}
	for i, values := range buckets {
		var day types.ProviderEarnings
		perMachine := map[string]*types.ProviderEarnings{}
		for field, raw := range values {
			n, _ := strconv.ParseInt(raw, 10, 64)
			target := &day
			if machine, name, ok := strings.Cut(strings.TrimPrefix(field, "m:"), ":"); ok && strings.HasPrefix(field, "m:") {
				if perMachine[machine] == nil {
					perMachine[machine] = &types.ProviderEarnings{}
				}
				target, field = perMachine[machine], name
			}
			if slot := providerEarningsFields(target)[field]; slot != nil {
				*slot += n
			}
		}
		if day != (types.ProviderEarnings{}) {
			report.PerDay[dates[i]] = day
			report.Total.Add(day)
		}
		for machine, e := range perMachine {
			total := report.PerMachine[machine]
			total.Add(*e)
			report.PerMachine[machine] = total
		}
	}
	return report, nil
}
