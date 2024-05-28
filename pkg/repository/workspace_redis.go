package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type WorkspaceRedisRepository struct {
	rdb *common.RedisClient
}

func NewWorkspaceRedisRepository(r *common.RedisClient) WorkspaceRepository {
	return &WorkspaceRedisRepository{rdb: r}
}

func (wr *WorkspaceRedisRepository) GetConcurrencyLimitByWorkspaceId(workspaceId string) (*types.ConcurrencyLimit, error) {
	key := common.RedisKeys.WorkspaceConcurrencyLimit(workspaceId)
	res, err := wr.rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	limit := &types.ConcurrencyLimit{}
	if err = common.ToStruct(res, limit); err != nil {
		return nil, err
	}

	return limit, nil
}

var cachedConcurrencyLimitTtl = 600

func (wr *WorkspaceRedisRepository) SetConcurrencyLimitByWorkspaceId(workspaceId string, limit *types.ConcurrencyLimit) error {
	key := common.RedisKeys.WorkspaceConcurrencyLimit(workspaceId)
	err := wr.rdb.HSet(context.Background(), key, common.ToSlice(limit)).Err()
	if err != nil {
		return err
	}

	err = wr.rdb.Expire(context.Background(), key, time.Duration(cachedConcurrencyLimitTtl)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}
