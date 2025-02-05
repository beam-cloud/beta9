package repository

import (
	"context"
	"fmt"

	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	redis "github.com/redis/go-redis/v9"
)

type WorkerPoolRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewWorkerPoolRedisRepository(rdb *common.RedisClient) WorkerPoolRepository {
	return &WorkerPoolRedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

// GetWorkerPoolState retrieves a collection of health metrics for a worker pool
func (r *WorkerPoolRedisRepository) GetWorkerPoolState(ctx context.Context, poolName string) (*types.WorkerPoolState, error) {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.WorkerPoolStateLock(poolName), common.RedisLockOptions{TtlS: 10, Retries: 2})
	if err != nil {
		return nil, err
	}
	defer r.lock.Release(common.RedisKeys.WorkerPoolStateLock(poolName))

	// Retrieve worker pool state
	stateKey := common.RedisKeys.WorkerPoolState(poolName)
	res, err := r.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get worker pool state: %w", err)
	}

	if len(res) == 0 {
		return nil, &types.ErrWorkerPoolStateNotFound{PoolName: poolName}
	}

	state := &types.WorkerPoolState{}
	if err = common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize worker pool state <%v>: %v", stateKey, err)
	}

	return state, nil
}

// SetWorkerPoolState updates the worker pool state with some recent health metrics
func (r *WorkerPoolRedisRepository) SetWorkerPoolState(ctx context.Context, poolName string, state *types.WorkerPoolState) error {
	stateKey := common.RedisKeys.WorkerPoolState(poolName)

	err := r.lock.Acquire(context.TODO(), common.RedisKeys.WorkerPoolStateLock(poolName), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.WorkerPoolStateLock(poolName))

	err = r.rdb.HSet(
		context.TODO(), stateKey,
		"status", string(state.Status),
		"scheduling_latency", state.SchedulingLatency,
		"free_gpu", state.FreeGpu,
		"free_cpu", state.FreeCpu,
		"free_memory", state.FreeMemory,
		"pending_workers", state.PendingWorkers,
		"available_workers", state.AvailableWorkers,
		"pending_containers", state.PendingContainers,
		"running_containers", state.RunningContainers,
		"registered_machines", state.RegisteredMachines,
		"pending_machines", state.PendingMachines,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to set worker pool state <%v>: %w", stateKey, err)
	}

	return nil
}

func (r *WorkerPoolRedisRepository) SetWorkerPoolSizerLock(poolName string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.WorkerPoolSizerLock(poolName), common.RedisLockOptions{TtlS: 3, Retries: 0})
	if err != nil {
		return err
	}

	return nil
}

func (r *WorkerPoolRedisRepository) RemoveWorkerPoolSizerLock(poolName string) error {
	return r.lock.Release(common.RedisKeys.WorkerPoolSizerLock(poolName))
}
