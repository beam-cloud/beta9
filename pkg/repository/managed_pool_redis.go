package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	managedPoolStateLockTTL = 30
)

type ManagedPoolRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewManagedPoolRedisRepository(rdb *common.RedisClient) ManagedPoolRepository {
	return &ManagedPoolRedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

func (r *ManagedPoolRedisRepository) WithManagedPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	return r.lock.WithLease(ctx, common.RedisKeys.ComputeManagedPoolStateLock(workspaceID, name), common.RedisLockOptions{
		TtlS:          managedPoolStateLockTTL,
		Retries:       100,
		RetryInterval: 50 * time.Millisecond,
	}, fn)
}

func (r *ManagedPoolRedisRepository) SaveManagedPoolState(ctx context.Context, workspaceID string, state *compute.PoolState) error {
	if state == nil {
		return errors.New("managed pool state is required")
	}

	persisted := *state
	persisted.WorkspaceID = workspaceID
	data, err := json.Marshal(&persisted)
	if err != nil {
		return err
	}

	stateKey := common.RedisKeys.ComputeManagedPoolState(workspaceID, state.Name)
	poolIndexKey := common.RedisKeys.ComputeManagedPoolIndex(workspaceID)
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, stateKey, data, 0)
		pipe.SAdd(ctx, poolIndexKey, state.Name)
		return nil
	})
	if err != nil {
		return err
	}
	r.publishUpdate(ctx, workspaceID, state.Name)
	return nil
}

func (r *ManagedPoolRedisRepository) GetManagedPoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeManagedPoolState(workspaceID, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var state compute.PoolState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	state.WorkspaceID = workspaceID
	return &state, nil
}

func (r *ManagedPoolRedisRepository) ListManagedPoolStates(ctx context.Context, workspaceID string, limit int) ([]*compute.PoolState, error) {
	names, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeManagedPoolIndex(workspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	keys := make([]string, 0, len(names))
	for _, name := range names {
		keys = append(keys, common.RedisKeys.ComputeManagedPoolState(workspaceID, name))
	}
	return r.managedPoolStates(ctx, workspaceID, keys)
}

func (r *ManagedPoolRedisRepository) DeleteManagedPoolState(ctx context.Context, workspaceID, name string) error {
	stateKey := common.RedisKeys.ComputeManagedPoolState(workspaceID, name)
	poolIndexKey := common.RedisKeys.ComputeManagedPoolIndex(workspaceID)
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, stateKey)
		pipe.SRem(ctx, poolIndexKey, name)
		return nil
	})
	if err != nil {
		return err
	}
	r.publishUpdate(ctx, workspaceID, name)
	return nil
}

func (r *ManagedPoolRedisRepository) managedPoolStates(ctx context.Context, workspaceID string, keys []string) ([]*compute.PoolState, error) {
	if len(keys) == 0 {
		return []*compute.PoolState{}, nil
	}
	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.PoolState, 0, len(values))
	for _, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}
		var state compute.PoolState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		state.WorkspaceID = workspaceID
		states = append(states, &state)
	}
	return states, nil
}

func (r *ManagedPoolRedisRepository) publishUpdate(ctx context.Context, workspaceID, name string) {
	payload, _ := json.Marshal(struct {
		WorkspaceID string `json:"workspace_id"`
		Name        string `json:"name"`
	}{WorkspaceID: workspaceID, Name: name})
	if err := r.rdb.Publish(ctx, common.RedisKeys.ComputeManagedPoolUpdates(), payload).Err(); err != nil {
		// State is already durable. Every gateway also reconciles periodically,
		// so a dropped invalidation only delays convergence.
		log.Warn().Err(err).Str("workspace_id", workspaceID).Str("pool_name", name).Msg("unable to publish managed pool update")
	}
}

var _ ManagedPoolRepository = (*ManagedPoolRedisRepository)(nil)
