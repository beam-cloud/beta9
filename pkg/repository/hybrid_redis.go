package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/hybrid"
	redis "github.com/redis/go-redis/v9"
)

const (
	hybridPoolKeyPrefix       = "hybrid:pool"
	hybridPoolIndexKeyPrefix  = "hybrid:pool_index"
	hybridJoinTokenKeyPrefix  = "hybrid:join_token"
	hybridAgentTokenKeyPrefix = "hybrid:agent_token"
	hybridMachineKeyPrefix    = "hybrid:machine"
)

type HybridRedisRepository struct {
	rdb *common.RedisClient
}

func NewHybridRedisRepository(rdb *common.RedisClient) HybridRepository {
	return &HybridRedisRepository{rdb: rdb}
}

func (r *HybridRedisRepository) SavePoolState(ctx context.Context, workspaceID string, state *hybrid.PoolState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, hybridPoolKey(workspaceID, state.Name), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, hybridPoolIndexKey(workspaceID), state.Name).Err()
}

func (r *HybridRedisRepository) GetPoolState(ctx context.Context, workspaceID, name string) (*hybrid.PoolState, error) {
	data, err := r.rdb.Get(ctx, hybridPoolKey(workspaceID, name)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state hybrid.PoolState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *HybridRedisRepository) ListPoolStates(ctx context.Context, workspaceID string, limit int) ([]*hybrid.PoolState, error) {
	names, err := r.rdb.SMembers(ctx, hybridPoolIndexKey(workspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	states := make([]*hybrid.PoolState, 0, len(names))
	for _, name := range names {
		state, err := r.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return nil, err
		}
		if state != nil {
			states = append(states, state)
		}
	}
	return states, nil
}

func (r *HybridRedisRepository) DeletePoolState(ctx context.Context, workspaceID, name string) error {
	if err := r.rdb.Del(ctx, hybridPoolKey(workspaceID, name)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, hybridPoolIndexKey(workspaceID), name).Err()
}

func (r *HybridRedisRepository) SaveJoinTokenState(ctx context.Context, state *hybrid.JoinTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Second
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, hybridJoinTokenKey(state.TokenHash), data, ttl).Err()
}

func (r *HybridRedisRepository) GetJoinTokenState(ctx context.Context, tokenHash string) (*hybrid.JoinTokenState, error) {
	data, err := r.rdb.Get(ctx, hybridJoinTokenKey(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state hybrid.JoinTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *HybridRedisRepository) SaveAgentTokenState(ctx context.Context, state *hybrid.AgentTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, hybridAgentTokenKey(state.TokenHash), data, ttl).Err(); err != nil {
		return err
	}
	return r.rdb.Set(ctx, hybridMachineKey(state.WorkspaceID, state.PoolName, state.MachineID), data, 0).Err()
}

func (r *HybridRedisRepository) GetAgentTokenState(ctx context.Context, tokenHash string) (*hybrid.AgentTokenState, error) {
	if tokenHash == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, hybridAgentTokenKey(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state hybrid.AgentTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func hybridPoolKey(workspaceID, name string) string {
	return fmt.Sprintf("%s:%s:%s", hybridPoolKeyPrefix, workspaceID, name)
}

func hybridPoolIndexKey(workspaceID string) string {
	return fmt.Sprintf("%s:%s", hybridPoolIndexKeyPrefix, workspaceID)
}

func hybridJoinTokenKey(tokenHash string) string {
	return fmt.Sprintf("%s:%s", hybridJoinTokenKeyPrefix, tokenHash)
}

func hybridAgentTokenKey(tokenHash string) string {
	return fmt.Sprintf("%s:%s", hybridAgentTokenKeyPrefix, tokenHash)
}

func hybridMachineKey(workspaceID, poolName, machineID string) string {
	return fmt.Sprintf("%s:%s:%s:%s", hybridMachineKeyPrefix, workspaceID, poolName, machineID)
}
