package repository

import (
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
	managedEndpointPrefix          = "managed_endpoint"
	managedEndpointReplicaLockTTLS = 10
	managedEndpointAckTTL          = 24 * time.Hour
	managedEndpointDrainTTL        = 10 * time.Minute
	managedEndpointMetricsBucket   = time.Minute
	managedEndpointMetricsRetain   = 25 * time.Hour
	managedEndpointRevisionKeep    = 200
)

// ManagedEndpointRedisRepository implements ManagedEndpointRepository on Redis.
//
// Key layout (all under managed_endpoint:):
//
//	endpoint:<id>                       JSON ManagedEndpoint       endpoints (set of ids)
//	service:<name>                      JSON ManagedService        services (set)
//	version:<id>:<n>                    JSON EndpointVersion       versions:<id> (set of n)
//	rollout:<id>                        JSON RolloutState
//	replica:<rid>                       JSON EndpointReplica       replicas (set), replicas:<id> (set)
//	replica_container:<cid>             rid
//	replica_lock:<rid>, drain:<rid>, backoff:<id>:<target>
//	config_seq:<id>                     INCR counter
//	config:<id>:<rev>                   JSON EndpointConfigRevision
//	config_index:<id>:<scope>:<key>     ZSET rev -> rev
//	config_events:<id>                  pub/sub channel (JSON revision)
//	config_ack:<rid>:<rev>              JSON ConfigAck
//	experiment:<eid>                    JSON Experiment            experiments:<id> (ZSET by start)
//	experiment_lock:<id>                eid
//	gitops                              JSON GitOpsState
//	metrics:<id>:<gpu>:<version>:<minute>  HASH counters
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

func (r *ManagedEndpointRedisRepository) getJSON(ctx context.Context, key string, out any) (bool, error) {
	data, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, json.Unmarshal(data, out)
}

func (r *ManagedEndpointRedisRepository) mgetJSON(ctx context.Context, keys []string, decode func([]byte) error) error {
	if len(keys) == 0 {
		return nil
	}
	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok {
			continue
		}
		if err := decode([]byte(raw)); err != nil {
			return err
		}
	}
	return nil
}

// --- Registry --------------------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveEndpoint(ctx context.Context, endpoint *types.ManagedEndpoint) error {
	if endpoint == nil || endpoint.Spec.ID == "" {
		return errors.New("endpoint id is required")
	}
	now := time.Now()
	if endpoint.CreatedAt.IsZero() {
		endpoint.CreatedAt = now
	}
	endpoint.UpdatedAt = now
	data, err := json.Marshal(endpoint)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("endpoint", endpoint.Spec.ID), data, 0)
		pipe.SAdd(ctx, meKey("endpoints"), endpoint.Spec.ID)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetEndpoint(ctx context.Context, endpointID string) (*types.ManagedEndpoint, error) {
	var endpoint types.ManagedEndpoint
	ok, err := r.getJSON(ctx, meKey("endpoint", endpointID), &endpoint)
	if err != nil || !ok {
		return nil, err
	}
	return &endpoint, nil
}

func (r *ManagedEndpointRedisRepository) ListEndpoints(ctx context.Context) ([]*types.ManagedEndpoint, error) {
	ids, err := r.rdb.SMembers(ctx, meKey("endpoints")).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, meKey("endpoint", id))
	}
	var out []*types.ManagedEndpoint
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var endpoint types.ManagedEndpoint
		if err := json.Unmarshal(raw, &endpoint); err != nil {
			return err
		}
		out = append(out, &endpoint)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.ID < out[j].Spec.ID })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) DeleteEndpoint(ctx context.Context, endpointID string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, meKey("endpoint", endpointID), meKey("rollout", endpointID))
		pipe.SRem(ctx, meKey("endpoints"), endpointID)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) SaveService(ctx context.Context, service *types.ManagedService) error {
	if service == nil || service.Spec.Name == "" {
		return errors.New("service name is required")
	}
	now := time.Now()
	if service.CreatedAt.IsZero() {
		service.CreatedAt = now
	}
	service.UpdatedAt = now
	data, err := json.Marshal(service)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("service", service.Spec.Name), data, 0)
		pipe.SAdd(ctx, meKey("services"), service.Spec.Name)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetService(ctx context.Context, name string) (*types.ManagedService, error) {
	var service types.ManagedService
	ok, err := r.getJSON(ctx, meKey("service", name), &service)
	if err != nil || !ok {
		return nil, err
	}
	return &service, nil
}

func (r *ManagedEndpointRedisRepository) ListServices(ctx context.Context) ([]*types.ManagedService, error) {
	names, err := r.rdb.SMembers(ctx, meKey("services")).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(names))
	for _, name := range names {
		keys = append(keys, meKey("service", name))
	}
	var out []*types.ManagedService
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var service types.ManagedService
		if err := json.Unmarshal(raw, &service); err != nil {
			return err
		}
		out = append(out, &service)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) DeleteService(ctx context.Context, name string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, meKey("service", name))
		pipe.SRem(ctx, meKey("services"), name)
		return nil
	})
	return err
}

// --- Versions and rollout --------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveVersion(ctx context.Context, version *types.EndpointVersion) error {
	if version == nil || version.EndpointID == "" || version.Version == 0 {
		return errors.New("endpoint id and version are required")
	}
	now := time.Now()
	if version.CreatedAt.IsZero() {
		version.CreatedAt = now
	}
	version.UpdatedAt = now
	data, err := json.Marshal(version)
	if err != nil {
		return err
	}
	n := strconv.FormatUint(uint64(version.Version), 10)
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("version", version.EndpointID, n), data, 0)
		pipe.SAdd(ctx, meKey("versions", version.EndpointID), n)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) ListVersions(ctx context.Context, endpointID string) ([]*types.EndpointVersion, error) {
	members, err := r.rdb.SMembers(ctx, meKey("versions", endpointID)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(members))
	for _, n := range members {
		keys = append(keys, meKey("version", endpointID, n))
	}
	var out []*types.EndpointVersion
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var version types.EndpointVersion
		if err := json.Unmarshal(raw, &version); err != nil {
			return err
		}
		out = append(out, &version)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) SaveRollout(ctx context.Context, rollout *types.RolloutState) error {
	if rollout == nil || rollout.EndpointID == "" {
		return errors.New("endpoint id is required")
	}
	rollout.UpdatedAt = time.Now()
	data, err := json.Marshal(rollout)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, meKey("rollout", rollout.EndpointID), data, 0).Err()
}

func (r *ManagedEndpointRedisRepository) GetRollout(ctx context.Context, endpointID string) (*types.RolloutState, error) {
	var rollout types.RolloutState
	ok, err := r.getJSON(ctx, meKey("rollout", endpointID), &rollout)
	if err != nil || !ok {
		return nil, err
	}
	return &rollout, nil
}

// --- Replicas --------------------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveReplica(ctx context.Context, replica *types.EndpointReplica) error {
	if replica == nil || replica.ID == "" || replica.EndpointID == "" {
		return errors.New("replica id and endpoint id are required")
	}
	data, err := json.Marshal(replica)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("replica", replica.ID), data, 0)
		pipe.SAdd(ctx, meKey("replicas"), replica.ID)
		pipe.SAdd(ctx, meKey("replicas", replica.EndpointID), replica.ID)
		if replica.ContainerID != "" {
			pipe.Set(ctx, meKey("replica_container", replica.ContainerID), replica.ID, 0)
		}
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetReplica(ctx context.Context, replicaID string) (*types.EndpointReplica, error) {
	var replica types.EndpointReplica
	ok, err := r.getJSON(ctx, meKey("replica", replicaID), &replica)
	if err != nil || !ok {
		return nil, err
	}
	return &replica, nil
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

func (r *ManagedEndpointRedisRepository) listReplicaIDs(ctx context.Context, indexKey string) ([]*types.EndpointReplica, error) {
	ids, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, meKey("replica", id))
	}
	var out []*types.EndpointReplica
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var replica types.EndpointReplica
		if err := json.Unmarshal(raw, &replica); err != nil {
			return err
		}
		out = append(out, &replica)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) ListReplicas(ctx context.Context, endpointID string) ([]*types.EndpointReplica, error) {
	return r.listReplicaIDs(ctx, meKey("replicas", endpointID))
}

func (r *ManagedEndpointRedisRepository) ListAllReplicas(ctx context.Context) ([]*types.EndpointReplica, error) {
	return r.listReplicaIDs(ctx, meKey("replicas"))
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
	return r.lock.WithLease(ctx, meKey("replica_lock", replicaID), common.RedisLockOptions{
		TtlS:          managedEndpointReplicaLockTTLS,
		Retries:       50,
		RetryInterval: 50 * time.Millisecond,
	}, fn)
}

func (r *ManagedEndpointRedisRepository) RequestDrain(ctx context.Context, replicaID string, drainSeconds uint32) error {
	return r.rdb.Set(ctx, meKey("drain", replicaID), strconv.FormatUint(uint64(drainSeconds), 10), managedEndpointDrainTTL).Err()
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
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// --- Config revisions ------------------------------------------------------

func configIndexKey(endpointID string, scope types.ConfigRevisionScope, scopeKey string) string {
	return meKey("config_index", endpointID, string(scope), scopeKey)
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
	index := configIndexKey(revision.EndpointID, revision.Scope, revision.ScopeKey)
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("config", revision.EndpointID, strconv.FormatUint(revision.Revision, 10)), data, 0)
		pipe.ZAdd(ctx, index, redis.Z{Score: float64(revision.Revision), Member: strconv.FormatUint(revision.Revision, 10)})
		pipe.ZRemRangeByRank(ctx, index, 0, -managedEndpointRevisionKeep-1)
		pipe.Publish(ctx, meKey("config_events", revision.EndpointID), data)
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetConfigRevision(ctx context.Context, endpointID string, revision uint64) (*types.EndpointConfigRevision, error) {
	var out types.EndpointConfigRevision
	ok, err := r.getJSON(ctx, meKey("config", endpointID, strconv.FormatUint(revision, 10)), &out)
	if err != nil || !ok {
		return nil, err
	}
	return &out, nil
}

func (r *ManagedEndpointRedisRepository) LatestConfigRevision(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string) (*types.EndpointConfigRevision, error) {
	members, err := r.rdb.ZRevRange(ctx, configIndexKey(endpointID, scope, scopeKey), 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	revision, err := strconv.ParseUint(members[0], 10, 64)
	if err != nil {
		return nil, err
	}
	return r.GetConfigRevision(ctx, endpointID, revision)
}

func (r *ManagedEndpointRedisRepository) ListConfigRevisions(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string, limit int) ([]*types.EndpointConfigRevision, error) {
	if limit <= 0 {
		limit = 20
	}
	members, err := r.rdb.ZRevRange(ctx, configIndexKey(endpointID, scope, scopeKey), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(members))
	for _, member := range members {
		keys = append(keys, meKey("config", endpointID, member))
	}
	var out []*types.EndpointConfigRevision
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var revision types.EndpointConfigRevision
		if err := json.Unmarshal(raw, &revision); err != nil {
			return err
		}
		out = append(out, &revision)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Revision > out[j].Revision })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) DeleteConfigRevisions(ctx context.Context, endpointID string, scope types.ConfigRevisionScope, scopeKey string) error {
	index := configIndexKey(endpointID, scope, scopeKey)
	members, err := r.rdb.ZRange(ctx, index, 0, -1).Result()
	if err != nil {
		return err
	}
	keys := []string{index}
	for _, member := range members {
		keys = append(keys, meKey("config", endpointID, member))
	}
	return r.rdb.Del(ctx, keys...).Err()
}

func (r *ManagedEndpointRedisRepository) SubscribeConfigRevisions(ctx context.Context, endpointID string) (<-chan *types.EndpointConfigRevision, error) {
	messages, errs := r.rdb.Subscribe(ctx, meKey("config_events", endpointID))
	out := make(chan *types.EndpointConfigRevision, 16)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errs:
				if ok && err != nil {
					log.Warn().Err(err).Str("endpoint_id", endpointID).Msg("managed endpoint config subscription error")
				}
				if !ok {
					return
				}
			case message, ok := <-messages:
				if !ok {
					return
				}
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
		}
	}()
	return out, nil
}

func (r *ManagedEndpointRedisRepository) SaveConfigAck(ctx context.Context, ack *types.ConfigAck) error {
	if ack == nil || ack.ReplicaID == "" {
		return errors.New("replica id is required")
	}
	if ack.At.IsZero() {
		ack.At = time.Now()
	}
	data, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, meKey("config_ack", ack.ReplicaID, strconv.FormatUint(ack.Revision, 10)), data, managedEndpointAckTTL).Err()
}

func (r *ManagedEndpointRedisRepository) GetConfigAck(ctx context.Context, replicaID string, revision uint64) (*types.ConfigAck, error) {
	var ack types.ConfigAck
	ok, err := r.getJSON(ctx, meKey("config_ack", replicaID, strconv.FormatUint(revision, 10)), &ack)
	if err != nil || !ok {
		return nil, err
	}
	return &ack, nil
}

// --- Experiments -----------------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveExperiment(ctx context.Context, experiment *types.Experiment, ttl time.Duration, keep int) error {
	if experiment == nil || experiment.ID == "" || experiment.EndpointID == "" {
		return errors.New("experiment id and endpoint id are required")
	}
	data, err := json.Marshal(experiment)
	if err != nil {
		return err
	}
	index := meKey("experiments", experiment.EndpointID)
	startedAt := experiment.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, meKey("experiment", experiment.ID), data, ttl)
		pipe.ZAdd(ctx, index, redis.Z{Score: float64(startedAt.UnixMilli()), Member: experiment.ID})
		if keep > 0 {
			pipe.ZRemRangeByRank(ctx, index, 0, int64(-keep-1))
		}
		if ttl > 0 {
			pipe.Expire(ctx, index, ttl)
		}
		return nil
	})
	return err
}

func (r *ManagedEndpointRedisRepository) GetExperiment(ctx context.Context, experimentID string) (*types.Experiment, error) {
	var experiment types.Experiment
	ok, err := r.getJSON(ctx, meKey("experiment", experimentID), &experiment)
	if err != nil || !ok {
		return nil, err
	}
	return &experiment, nil
}

func (r *ManagedEndpointRedisRepository) ListExperiments(ctx context.Context, endpointID string, limit int) ([]*types.Experiment, error) {
	if limit <= 0 {
		limit = 20
	}
	ids, err := r.rdb.ZRevRange(ctx, meKey("experiments", endpointID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, meKey("experiment", id))
	}
	var out []*types.Experiment
	err = r.mgetJSON(ctx, keys, func(raw []byte) error {
		var experiment types.Experiment
		if err := json.Unmarshal(raw, &experiment); err != nil {
			return err
		}
		out = append(out, &experiment)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
}

func (r *ManagedEndpointRedisRepository) AcquireExperimentLock(ctx context.Context, endpointID, experimentID string, ttl time.Duration) (bool, string, error) {
	key := meKey("experiment_lock", endpointID)
	ok, err := r.rdb.SetNX(ctx, key, experimentID, ttl).Result()
	if err != nil {
		return false, "", err
	}
	if ok {
		return true, experimentID, nil
	}
	holder, err := r.rdb.Get(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, "", err
	}
	if holder == experimentID {
		// Re-entrant: extend the lease.
		return true, holder, r.rdb.Expire(ctx, key, ttl).Err()
	}
	return false, holder, nil
}

var releaseExperimentLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (r *ManagedEndpointRedisRepository) ReleaseExperimentLock(ctx context.Context, endpointID, experimentID string) error {
	return releaseExperimentLockScript.Run(ctx, r.rdb, []string{meKey("experiment_lock", endpointID)}, experimentID).Err()
}

// --- GitOps ----------------------------------------------------------------

func (r *ManagedEndpointRedisRepository) SaveGitOpsState(ctx context.Context, state *types.GitOpsState) error {
	if state == nil {
		return errors.New("gitops state is required")
	}
	state.UpdatedAt = time.Now()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, meKey("gitops"), data, 0).Err()
}

func (r *ManagedEndpointRedisRepository) GetGitOpsState(ctx context.Context) (*types.GitOpsState, error) {
	var state types.GitOpsState
	ok, err := r.getJSON(ctx, meKey("gitops"), &state)
	if err != nil || !ok {
		return nil, err
	}
	if state.PerEndpoint == nil {
		state.PerEndpoint = map[string]types.GitOpsEndpointState{}
	}
	return &state, nil
}

// --- Route metrics ---------------------------------------------------------

const (
	metricsFieldRequests    = "requests"
	metricsFieldErrors      = "errors"
	metricsFieldPromptTok   = "prompt_tokens"
	metricsFieldCompleteTok = "completion_tokens"
	metricsFieldImages      = "images"
	metricsFieldCost        = "cost_micro_usd"
	metricsFieldDurationMs  = "duration_sum_ms"
	metricsFieldTTFTMs      = "ttft_sum_ms"
	metricsFieldTTFTCount   = "ttft_count"
	metricsFieldQueueWaitMs = "queue_wait_sum_ms"
	metricsAggregateGPU     = "_all"
	metricsAggregateVersion = "0"
)

func metricsKey(endpointID, gpu, version string, bucket time.Time) string {
	return meKey("metrics", endpointID, gpu, version, strconv.FormatInt(bucket.Unix(), 10))
}

// RecordRouteSample increments the per-minute buckets for the exact
// (gpu, version) as well as the (all gpus, all versions) aggregate so both
// rollout comparisons and endpoint overviews are one range read.
func (r *ManagedEndpointRedisRepository) RecordRouteSample(ctx context.Context, sample types.RouteSample) error {
	if sample.EndpointID == "" {
		return errors.New("endpoint id is required")
	}
	at := sample.At
	if at.IsZero() {
		at = time.Now()
	}
	bucket := at.Truncate(managedEndpointMetricsBucket)
	gpu := sample.GPU
	if gpu == "" {
		gpu = metricsAggregateGPU
	}
	version := strconv.FormatUint(uint64(sample.Version), 10)

	keys := map[string]struct{}{
		metricsKey(sample.EndpointID, metricsAggregateGPU, metricsAggregateVersion, bucket): {},
		metricsKey(sample.EndpointID, gpu, metricsAggregateVersion, bucket):                 {},
		metricsKey(sample.EndpointID, metricsAggregateGPU, version, bucket):                 {},
		metricsKey(sample.EndpointID, gpu, version, bucket):                                 {},
	}

	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key := range keys {
			pipe.HIncrBy(ctx, key, metricsFieldRequests, 1)
			if sample.Failed() {
				pipe.HIncrBy(ctx, key, metricsFieldErrors, 1)
			}
			if sample.PromptTokens > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldPromptTok, sample.PromptTokens)
			}
			if sample.CompletionTokens > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldCompleteTok, sample.CompletionTokens)
			}
			if sample.Images > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldImages, sample.Images)
			}
			if sample.CostMicroUSD > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldCost, sample.CostMicroUSD)
			}
			pipe.HIncrBy(ctx, key, metricsFieldDurationMs, sample.Duration.Milliseconds())
			if sample.TTFT > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldTTFTMs, sample.TTFT.Milliseconds())
				pipe.HIncrBy(ctx, key, metricsFieldTTFTCount, 1)
			}
			if sample.QueueWait > 0 {
				pipe.HIncrBy(ctx, key, metricsFieldQueueWaitMs, sample.QueueWait.Milliseconds())
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
	if window > managedEndpointMetricsRetain-time.Hour {
		window = managedEndpointMetricsRetain - time.Hour
	}
	if gpu == "" {
		gpu = metricsAggregateGPU
	}
	versionKey := strconv.FormatUint(uint64(version), 10)

	now := time.Now()
	end := now.Truncate(managedEndpointMetricsBucket)
	start := now.Add(-window).Truncate(managedEndpointMetricsBucket)
	cmds := make([]*redis.MapStringStringCmd, 0, int(window/managedEndpointMetricsBucket)+1)
	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for bucket := start; !bucket.After(end); bucket = bucket.Add(managedEndpointMetricsBucket) {
			cmds = append(cmds, pipe.HGetAll(ctx, metricsKey(endpointID, gpu, versionKey, bucket)))
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	metrics := &types.RouteMetrics{EndpointID: endpointID, Version: version, Window: window}
	if gpu != metricsAggregateGPU {
		metrics.GPU = gpu
	}
	for _, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil {
			continue
		}
		metrics.Requests += hashInt64(values, metricsFieldRequests)
		metrics.Errors += hashInt64(values, metricsFieldErrors)
		metrics.PromptTokens += hashInt64(values, metricsFieldPromptTok)
		metrics.CompletionTokens += hashInt64(values, metricsFieldCompleteTok)
		metrics.Images += hashInt64(values, metricsFieldImages)
		metrics.CostMicroUSD += hashInt64(values, metricsFieldCost)
		metrics.DurationSumMs += hashInt64(values, metricsFieldDurationMs)
		metrics.TTFTSumMs += hashInt64(values, metricsFieldTTFTMs)
		metrics.TTFTCount += hashInt64(values, metricsFieldTTFTCount)
		metrics.QueueWaitSumMs += hashInt64(values, metricsFieldQueueWaitMs)
	}
	return metrics, nil
}

func hashInt64(values map[string]string, field string) int64 {
	value, ok := values[field]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
