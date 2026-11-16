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

const workerPoolSizerLockTTLSeconds = 30

func NewWorkerPoolRedisRepository(rdb *common.RedisClient) WorkerPoolRepository {
	return &WorkerPoolRedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

// GetWorkerPoolState retrieves a collection of health metrics for a worker pool
func (r *WorkerPoolRedisRepository) GetWorkerPoolState(ctx context.Context, poolName string) (*types.WorkerPoolState, error) {
	stateKey := common.RedisKeys.WorkerPoolState(poolName)
	res, err := r.rdb.HGetAll(ctx, stateKey).Result()
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

// DeleteWorkerPoolState removes the cached health snapshot for a pool. Pool
// configuration lives elsewhere, so this is safe to call while deleting a
// API-managed pool and prevents a later same-name pool from inheriting
// stale health or capacity data.
func (r *WorkerPoolRedisRepository) DeleteWorkerPoolState(ctx context.Context, poolName string) error {
	return r.rdb.Del(ctx, common.RedisKeys.WorkerPoolState(poolName)).Err()
}

// SetWorkerPoolState updates the worker pool state with some recent health metrics
func (r *WorkerPoolRedisRepository) SetWorkerPoolState(ctx context.Context, poolName string, state *types.WorkerPoolState) error {
	return r.rdb.HSet(
		ctx, common.RedisKeys.WorkerPoolState(poolName),
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
		"ready_machines", state.ReadyMachines,
	).Err()
}

// Three kinds of work are done for a pool by one scheduler replica at a time:
// sizing it, reporting its state and cleaning up after its workers. Each has
// a lock of its own, so none holds up another.

// WithWorkerPoolSizerLock runs size while this replica is the only one sizing
// the pool.
func (r *WorkerPoolRedisRepository) WithWorkerPoolSizerLock(poolName string, size func()) error {
	return r.withPoolLock(common.RedisKeys.WorkerPoolSizerLock(poolName), common.RedisLockOptions{TtlS: workerPoolSizerLockTTLSeconds, Retries: 0}, size)
}

// WithWorkerPoolStateLock runs report while this replica is the only one
// reporting the pool's state.
func (r *WorkerPoolRedisRepository) WithWorkerPoolStateLock(poolName string, report func()) error {
	return r.withPoolLock(common.RedisKeys.WorkerPoolStateLock(poolName), common.RedisLockOptions{TtlS: 10, Retries: 0}, report)
}

// WithWorkerPoolCleanerLock runs clean while this replica is the only one
// cleaning up after the pool's workers.
func (r *WorkerPoolRedisRepository) WithWorkerPoolCleanerLock(poolName string, clean func()) error {
	return r.withPoolLock(common.RedisKeys.WorkerPoolCleanerLock(poolName), common.RedisLockOptions{TtlS: 10, Retries: 3}, clean)
}

// withPoolLock runs work while this replica holds the lock. It returns
// redislock.ErrNotObtained without running it when another replica does. The
// lock is held for as long as work runs, however long that is, and expires
// on its own if the replica dies.
func (r *WorkerPoolRedisRepository) withPoolLock(key string, options common.RedisLockOptions, work func()) error {
	return r.lock.WithLease(context.TODO(), key, options, func(context.Context) error {
		work()
		return nil
	})
}
