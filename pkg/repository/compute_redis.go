package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	redis "github.com/redis/go-redis/v9"
)

type ComputeRedisRepository struct {
	rdb *common.RedisClient
}

func NewComputeRedisRepository(rdb *common.RedisClient) ComputeRepository {
	return &ComputeRedisRepository{rdb: rdb}
}

func (r *ComputeRedisRepository) SavePoolState(ctx context.Context, workspaceID string, state *compute.PoolState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputePoolState(workspaceID, state.Name), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputePoolIndex(workspaceID), state.Name).Err()
}

func (r *ComputeRedisRepository) GetPoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputePoolState(workspaceID, name)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.PoolState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) ListPoolStates(ctx context.Context, workspaceID string, limit int) ([]*compute.PoolState, error) {
	names, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputePoolIndex(workspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	keys := make([]string, 0, len(names))
	for _, name := range names {
		keys = append(keys, common.RedisKeys.ComputePoolState(workspaceID, name))
	}
	return r.poolStates(ctx, keys)
}

func (r *ComputeRedisRepository) DeletePoolState(ctx context.Context, workspaceID, name string) error {
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputePoolState(workspaceID, name)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputePoolIndex(workspaceID), name).Err()
}

func (r *ComputeRedisRepository) SaveJoinTokenState(ctx context.Context, state *compute.JoinTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Second
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, common.RedisKeys.ComputeJoinToken(state.TokenHash), data, ttl).Err()
}

func (r *ComputeRedisRepository) GetJoinTokenState(ctx context.Context, tokenHash string) (*compute.JoinTokenState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeJoinToken(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.JoinTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) SaveAgentTokenState(ctx context.Context, state *compute.AgentTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentToken(state.TokenHash), data, ttl).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentMachine(state.WorkspaceID, state.PoolName, state.MachineID), data, 0).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentMachinePool(state.WorkspaceID, state.MachineID), state.PoolName, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeAgentMachineIndex(state.WorkspaceID, state.PoolName), state.MachineID).Err()
}

func (r *ComputeRedisRepository) GetAgentTokenState(ctx context.Context, tokenHash string) (*compute.AgentTokenState, error) {
	if tokenHash == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentToken(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.AgentTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) GetAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) (*compute.AgentTokenState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.AgentTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) GetAgentMachineStateForWorkspace(ctx context.Context, workspaceID, machineID string) (*compute.AgentTokenState, error) {
	poolName, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentMachinePool(workspaceID, machineID)).Result()
	if err == nil && poolName != "" {
		return r.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	}
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return r.scanAgentMachineStateForWorkspace(ctx, workspaceID, machineID)
}

func (r *ComputeRedisRepository) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*compute.AgentTokenState, error) {
	machineIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(machineIDs))
	for _, machineID := range machineIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID))
	}
	states, err := r.machines(ctx, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].MachineID < states[j].MachineID
	})
	return states, nil
}

func (r *ComputeRedisRepository) DeleteAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) error {
	state, err := r.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	if err != nil {
		return err
	}

	workerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(workerIDs)+4)
	for _, workerID := range workerIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID))
	}
	keys = append(keys,
		common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID),
		common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID),
		common.RedisKeys.ComputeAgentMachinePool(workspaceID, machineID),
	)
	if state != nil && state.TokenHash != "" {
		keys = append(keys, common.RedisKeys.ComputeAgentToken(state.TokenHash))
	}

	if len(keys) > 0 {
		if err := r.rdb.Del(ctx, keys...).Err(); err != nil {
			return err
		}
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName), machineID).Err()
}

func (r *ComputeRedisRepository) scanAgentMachineStateForWorkspace(ctx context.Context, workspaceID, machineID string) (*compute.AgentTokenState, error) {
	pools, err := r.ListPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	for _, pool := range pools {
		if pool == nil || pool.Name == "" {
			continue
		}
		machine, err := r.GetAgentMachineState(ctx, workspaceID, pool.Name, machineID)
		if err != nil {
			return nil, err
		}
		if machine != nil {
			return machine, nil
		}
	}
	return nil, nil
}

func (r *ComputeRedisRepository) SaveAgentWorkerSlotState(ctx context.Context, state *compute.AgentWorkerSlotState) error {
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
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentSlot(state.WorkspaceID, state.PoolName, state.MachineID, state.WorkerID), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeAgentSlotIndex(state.WorkspaceID, state.PoolName, state.MachineID), state.WorkerID).Err()
}

func (r *ComputeRedisRepository) ListAgentWorkerSlotStates(ctx context.Context, workspaceID, poolName, machineID string) ([]*compute.AgentWorkerSlotState, error) {
	workerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(workerIDs))
	for _, workerID := range workerIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID))
	}
	states, err := r.slots(ctx, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].WorkerID < states[j].WorkerID
	})
	return states, nil
}

func (r *ComputeRedisRepository) DeleteAgentWorkerSlotState(ctx context.Context, workspaceID, poolName, machineID, workerID string) error {
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID), workerID).Err()
}

func (r *ComputeRedisRepository) poolStates(ctx context.Context, keys []string) ([]*compute.PoolState, error) {
	if len(keys) == 0 {
		return []*compute.PoolState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.PoolState, 0, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		data, ok := stateBytes(value)
		if !ok {
			return nil, fmt.Errorf("unexpected redis value type %T for %s", value, keys[i])
		}

		var state compute.PoolState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		states = append(states, &state)
	}
	return states, nil
}

func (r *ComputeRedisRepository) machines(ctx context.Context, keys []string) ([]*compute.AgentTokenState, error) {
	if len(keys) == 0 {
		return []*compute.AgentTokenState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.AgentTokenState, 0, len(values))
	for _, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}

		var state compute.AgentTokenState
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}
		states = append(states, &state)
	}
	return states, nil
}

func (r *ComputeRedisRepository) slots(ctx context.Context, keys []string) ([]*compute.AgentWorkerSlotState, error) {
	if len(keys) == 0 {
		return []*compute.AgentWorkerSlotState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.AgentWorkerSlotState, 0, len(values))
	for _, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}

		var state compute.AgentWorkerSlotState
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}
		states = append(states, &state)
	}
	return states, nil
}

func stateBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}
