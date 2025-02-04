package repository

import (
	"context"

	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
)

type WorkerPoolRedisRepository struct {
	redis *common.RedisClient
}

func NewWorkerPoolRedisRepository(redis *common.RedisClient) WorkerPoolRepository {
	return &WorkerPoolRedisRepository{redis: redis}
}

func (r *WorkerPoolRedisRepository) GetWorkerPool(ctx context.Context, poolName string) (*types.WorkerPoolConfig, error) {
	pool := &types.WorkerPoolConfig{}
	return pool, nil
}
