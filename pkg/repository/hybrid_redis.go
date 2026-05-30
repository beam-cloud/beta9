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
	if err := r.rdb.Set(ctx, common.RedisKeys.HybridPoolState(workspaceID, state.Name), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.HybridPoolIndex(workspaceID), state.Name).Err()
}

func (r *HybridRedisRepository) GetPoolState(ctx context.Context, workspaceID, name string) (*hybrid.PoolState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.HybridPoolState(workspaceID, name)).Bytes()
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
	names, err := r.rdb.SMembers(ctx, common.RedisKeys.HybridPoolIndex(workspaceID)).Result()
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
	if err := r.rdb.Del(ctx, common.RedisKeys.HybridPoolState(workspaceID, name)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.HybridPoolIndex(workspaceID), name).Err()
}

func (r *HybridRedisRepository) SaveJoinTokenState(ctx context.Context, state *hybrid.JoinTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Second
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, common.RedisKeys.HybridJoinToken(state.TokenHash), data, ttl).Err()
}

func (r *HybridRedisRepository) GetJoinTokenState(ctx context.Context, tokenHash string) (*hybrid.JoinTokenState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.HybridJoinToken(tokenHash)).Bytes()
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
	if err := r.rdb.Set(ctx, common.RedisKeys.HybridAgentToken(state.TokenHash), data, ttl).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.HybridAgentMachine(state.WorkspaceID, state.PoolName, state.MachineID), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.HybridAgentMachineIndex(state.WorkspaceID, state.PoolName), state.MachineID).Err()
}

func (r *HybridRedisRepository) GetAgentTokenState(ctx context.Context, tokenHash string) (*hybrid.AgentTokenState, error) {
	if tokenHash == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, common.RedisKeys.HybridAgentToken(tokenHash)).Bytes()
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

func (r *HybridRedisRepository) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*hybrid.AgentTokenState, error) {
	machineIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.HybridAgentMachineIndex(workspaceID, poolName)).Result()
	if err != nil {
		return nil, err
	}
	states := make([]*hybrid.AgentTokenState, 0, len(machineIDs))
	for _, machineID := range machineIDs {
		data, err := r.rdb.Get(ctx, common.RedisKeys.HybridAgentMachine(workspaceID, poolName, machineID)).Bytes()
		if err != nil {
			continue
		}
		var state hybrid.AgentTokenState
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}
		states = append(states, &state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].MachineID < states[j].MachineID
	})
	return states, nil
}

func (r *HybridRedisRepository) SaveAgentWorkerSlotState(ctx context.Context, state *hybrid.AgentWorkerSlotState) error {
	if state == nil || state.WorkerID == "" {
		return fmt.Errorf("worker slot id is required")
	}
	now := time.Now()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.HybridAgentSlot(state.WorkspaceID, state.PoolName, state.MachineID, state.WorkerID), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.HybridAgentSlotIndex(state.WorkspaceID, state.PoolName, state.MachineID), state.WorkerID).Err()
}

func (r *HybridRedisRepository) ListAgentWorkerSlotStates(ctx context.Context, workspaceID, poolName, machineID string) ([]*hybrid.AgentWorkerSlotState, error) {
	workerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.HybridAgentSlotIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return nil, err
	}
	states := make([]*hybrid.AgentWorkerSlotState, 0, len(workerIDs))
	for _, workerID := range workerIDs {
		data, err := r.rdb.Get(ctx, common.RedisKeys.HybridAgentSlot(workspaceID, poolName, machineID, workerID)).Bytes()
		if err != nil {
			continue
		}
		var state hybrid.AgentWorkerSlotState
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}
		states = append(states, &state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].WorkerID < states[j].WorkerID
	})
	return states, nil
}

func (r *HybridRedisRepository) DeleteAgentWorkerSlotState(ctx context.Context, workspaceID, poolName, machineID, workerID string) error {
	if err := r.rdb.Del(ctx, common.RedisKeys.HybridAgentSlot(workspaceID, poolName, machineID, workerID)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.HybridAgentSlotIndex(workspaceID, poolName, machineID), workerID).Err()
}
