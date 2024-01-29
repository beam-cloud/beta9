package repository

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type WorkerPoolRedisRepository struct {
	rdb         *common.RedisClient
	lock        *common.RedisLock
	lockOptions common.RedisLockOptions
}

func NewWorkerPoolRedisRepository(rdb *common.RedisClient) WorkerPoolRepository {
	lock := common.NewRedisLock(rdb)
	lockOptions := common.RedisLockOptions{TtlS: 10, Retries: 0}
	return &WorkerPoolRedisRepository{rdb: rdb, lock: lock, lockOptions: lockOptions}
}

// Get a pool without locking.
// Should only be called by public functions of this struct.
func (r *WorkerPoolRedisRepository) getPool(name string) (*types.WorkerPoolConfig, error) {
	bytes, err := r.rdb.Get(context.TODO(), common.RedisKeys.WorkerPoolState(name)).Bytes()
	if err != nil {
		return nil, err
	}

	p := &types.WorkerPoolConfig{}
	if err := json.Unmarshal(bytes, p); err != nil {
		return nil, err
	}

	return p, nil
}

func (r *WorkerPoolRedisRepository) GetPool(name string) (*types.WorkerPoolConfig, error) {
	lockKey := common.RedisKeys.WorkerPoolLock(name)
	if err := r.lock.Acquire(context.TODO(), lockKey, r.lockOptions); err != nil {
		return nil, err
	}
	defer r.lock.Release(lockKey)

	return r.getPool(name)
}

func (r *WorkerPoolRedisRepository) GetPools() ([]types.WorkerPoolConfig, error) {
	keys, err := r.rdb.Scan(context.TODO(), common.RedisKeys.WorkerPoolState("*"))
	if err != nil {
		return nil, err
	}

	pools := []types.WorkerPoolConfig{}
	for _, key := range keys {
		name := strings.Split(key, ":")[2]

		pool, err := r.getPool(name)
		if err != nil {
			return nil, err
		}

		pools = append(pools, *pool)
	}

	return pools, nil
}

func (r *WorkerPoolRedisRepository) SetPool(name string, pool types.WorkerPoolConfig) error {
	lockKey := common.RedisKeys.WorkerPoolLock(name)
	if err := r.lock.Acquire(context.TODO(), lockKey, r.lockOptions); err != nil {
		return err
	}
	defer r.lock.Release(lockKey)

	bytes, err := json.Marshal(pool)
	if err != nil {
		return err
	}

	return r.rdb.Set(context.TODO(), common.RedisKeys.WorkerPoolState(name), bytes, 0).Err()
}

func (r *WorkerPoolRedisRepository) RemovePool(name string) error {
	lockKey := common.RedisKeys.WorkerPoolLock(name)
	if err := r.lock.Acquire(context.TODO(), lockKey, r.lockOptions); err != nil {
		return err
	}
	defer r.lock.Release(lockKey)

	return r.rdb.Del(context.TODO(), common.RedisKeys.WorkerPoolState(name)).Err()
}

func (r *WorkerPoolRedisRepository) GetMachines(name string) error {
	pool, err := r.GetPool(name)
	if err != nil {
		return err
	}

	log.Printf("getting machines for pool: %+v\n", pool)
	return nil
}
