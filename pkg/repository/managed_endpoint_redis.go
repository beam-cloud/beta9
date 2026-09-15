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
	metricsAggregateGPU          = "_all"
	metricsAggregateReplica      = "-"
)

// ManagedEndpointRedisRepository implements ManagedEndpointRepository on Redis. Keys (under managed_endpoint:):
//
//	app:<id>, replica:<rid>                           JSON, indexed by the sets apps, replicas, replicas:<id>
//	replica_container:<cid> -> rid; replica_lock:<rid>, drain:<rid>, backoff:<id>:<gpu>
//	fleet, gitops                                     JSON
//	generation:<charge_id>                            JSON charge; accounting:pending ZSET of ids not yet metered
//	config_events:<rid>                               pub/sub, replica config revision numbers
//	metrics:<id>:<gpu>:<replica[@revision]>:<minute>  HASH counters
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

func u64(n uint64) string { return strconv.FormatUint(n, 10) }

// getJSON returns nil, nil when the key does not exist.
func getJSON[T any](ctx context.Context, rdb *common.RedisClient, key string) (*T, error) {
	out, err := listJSON[T](ctx, rdb, []string{key})
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out[0], nil
}

// listIndexed MGETs meKey(keyPrefix, member) for every member of the index set, sorted by id.
func listIndexed[T any](ctx context.Context, rdb *common.RedisClient, indexKey, keyPrefix string, id func(*T) string) ([]*T, error) {
	members, err := rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(members))
	for _, m := range members {
		keys = append(keys, meKey(keyPrefix, m))
	}
	out, err := listJSON[T](ctx, rdb, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return id(out[i]) < id(out[j]) })
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

func (r *ManagedEndpointRedisRepository) SaveEndpoint(ctx context.Context, app *types.ManagedEndpoint) error {
	if app == nil || app.Spec.ID == "" {
		return errors.New("app id is required")
	}
	app.UpdatedAt = time.Now()
	app.CreatedAt = cmp.Or(app.CreatedAt, app.UpdatedAt)
	return r.saveIndexed(ctx, meKey("endpoint", app.Spec.ID), meKey("endpoints"), app.Spec.ID, app, nil)
}

func (r *ManagedEndpointRedisRepository) GetEndpoint(ctx context.Context, id string) (*types.ManagedEndpoint, error) {
	return getJSON[types.ManagedEndpoint](ctx, r.rdb, meKey("endpoint", id))
}

func (r *ManagedEndpointRedisRepository) ListEndpoints(ctx context.Context) ([]*types.ManagedEndpoint, error) {
	return listIndexed(ctx, r.rdb, meKey("endpoints"), "endpoint", func(e *types.ManagedEndpoint) string { return e.Spec.ID })
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
	return listIndexed(ctx, r.rdb, meKey("replicas", endpointID), "replica", func(a *types.EndpointReplica) string { return a.ID })
}

func (r *ManagedEndpointRedisRepository) ListAllReplicas(ctx context.Context) ([]*types.EndpointReplica, error) {
	return listIndexed(ctx, r.rdb, meKey("replicas"), "replica", func(a *types.EndpointReplica) string { return a.ID })
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

// replicaRevisionKey scopes a replica's metrics to one live config revision.
func replicaRevisionKey(replicaID string, revision uint64) string {
	return replicaID + "@" + strconv.FormatUint(revision, 10)
}

// RecordRouteSample folds one finished charge into the minute buckets for
// the app, its GPU type, the replica and the replica's config revision.
func (r *ManagedEndpointRedisRepository) RecordRouteSample(ctx context.Context, c *types.Charge) error {
	if c.AppID == "" {
		return errors.New("app id is required")
	}
	bucket := cmp.Or(c.SettledAt, time.Now()).Truncate(managedEndpointMetricsBucket)
	gpu := cmp.Or(c.GPU, metricsAggregateGPU)
	keys := map[string]struct{}{
		metricsKey(c.AppID, metricsAggregateGPU, metricsAggregateReplica, bucket): {},
		metricsKey(c.AppID, gpu, metricsAggregateReplica, bucket):                 {},
	}
	if c.ReplicaID != "" {
		keys[metricsKey(c.AppID, gpu, c.ReplicaID, bucket)] = struct{}{}
		if c.ConfigRevision > 0 {
			keys[metricsKey(c.AppID, gpu, replicaRevisionKey(c.ReplicaID, c.ConfigRevision), bucket)] = struct{}{}
		}
	}
	delta := types.RouteMetrics{
		Requests: 1, PromptTokens: c.Work.PromptTokens, CompletionTokens: c.Work.CompletionTokens,
		CostMicroUSD: c.Cost.MicroUSD, DurationSumMs: c.DurationMs, TTFTSumMs: c.TTFTMs, QueueWaitSumMs: c.QueueWaitMs,
	}
	if c.Status != types.ChargeSettled {
		delta.Errors = 1
	}
	if c.TTFTMs > 0 {
		delta.TTFTCount = 1
	}
	fields := delta.Fields()
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

// GetRouteMetrics sums the window for the app, one GPU type, one replica
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
	fields := metrics.Fields()
	for _, values := range buckets {
		for field, total := range fields {
			n, _ := strconv.ParseInt(values[field], 10, 64)
			*total += n
		}
	}
	return metrics, nil
}

// SaveCharge journals a charge and reports whether it was written. A charge
// is final once journaled, so a duplicate completion writes nothing. Written
// charges stay in the pending index until CompleteAccounting.
func (r *ManagedEndpointRedisRepository) SaveCharge(ctx context.Context, c *types.Charge) (bool, error) {
	if c == nil || c.ID == "" || c.WorkspaceID == "" || c.AppID == "" {
		return false, errors.New("charge id, caller and app are required")
	}
	data, err := json.Marshal(c)
	if err != nil {
		return false, err
	}
	due := cmp.Or(c.SettledAt, c.AcceptedAt) // accounting runs oldest first
	written, err := saveChargeScript.Run(ctx, r.rdb, []string{meKey("generation", c.ID), meKey("accounting", "pending")},
		string(data), c.ID, due.Unix()).Int()
	return written == 1, err
}

// KEYS[1] charge, KEYS[2] pending index; ARGV[1] json, ARGV[2] id, ARGV[3]
// score. Returns 1 when the charge was written, 0 when it already existed.
var saveChargeScript = redis.NewScript(`
if redis.call('SET', KEYS[1], ARGV[1], 'NX') == false then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1
`)

// GetCharge returns nil for an id with no charge, including a record written
// before charges existed.
func (r *ManagedEndpointRedisRepository) GetCharge(ctx context.Context, id string) (*types.Charge, error) {
	c, err := getJSON[types.Charge](ctx, r.rdb, meKey("generation", id))
	if err != nil || c == nil || c.ID == "" {
		return nil, err
	}
	return c, nil
}

// ListPendingCharges returns the charges due for accounting by now, oldest
// first. An index entry with no charge behind it is dropped with a warning
// rather than blocking every charge behind it.
func (r *ManagedEndpointRedisRepository) ListPendingCharges(ctx context.Context, now time.Time, limit int64) ([]*types.Charge, error) {
	ids, err := r.rdb.ZRangeByScore(ctx, meKey("accounting", "pending"), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.Unix(), 10), Count: limit}).Result()
	if err != nil {
		return nil, err
	}
	charges := make([]*types.Charge, 0, len(ids))
	for _, id := range ids {
		c, err := r.GetCharge(ctx, id)
		if err != nil {
			return nil, err
		}
		if c == nil {
			log.Warn().Str("charge_id", id).Msg("managed endpoints: dropping pending accounting entry without a charge")
			if err := r.rdb.ZRem(ctx, meKey("accounting", "pending"), id).Err(); err != nil {
				return nil, err
			}
			continue
		}
		charges = append(charges, c)
	}
	return charges, nil
}

var completeAccountingScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return 1
`)

// CompleteAccounting takes a charge out of the pending index and lets its journal entry expire.
func (r *ManagedEndpointRedisRepository) CompleteAccounting(ctx context.Context, id string, ttl time.Duration) error {
	return completeAccountingScript.Run(ctx, r.rdb, []string{meKey("accounting", "pending"), meKey("generation", id)}, id, int(ttl.Seconds())).Err()
}
