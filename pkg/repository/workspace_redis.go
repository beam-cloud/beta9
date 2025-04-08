package repository

import (
	"context"
	"encoding/json"
	"errors"
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

const cachedTokenTTLS = 600 // 10 minutes

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

type AuthInfo struct {
	Workspace *types.Workspace
	Token     *types.Token
}

func (wr *WorkspaceRedisRepository) AuthorizeToken(token string) (*types.Token, *types.Workspace, error) {
	tokenKey := common.RedisKeys.WorkspaceAuthorizedToken(token)
	res, err := wr.rdb.Get(context.Background(), tokenKey).Result()
	if err != nil {
		return nil, nil, err
	}

	if res == "" {
		return nil, nil, errors.New("token not found")
	}

	info := &AuthInfo{}
	err = json.Unmarshal([]byte(res), &info)
	if err != nil {
		return nil, nil, err
	}

	if info.Token == nil || info.Workspace == nil {
		return nil, nil, errors.New("token not found")
	}

	return info.Token, info.Workspace, nil
}

func (wr *WorkspaceRedisRepository) RevokeToken(tokenKey string) error {
	err := wr.rdb.Del(context.Background(), common.RedisKeys.WorkspaceAuthorizedToken(tokenKey)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (wr *WorkspaceRedisRepository) SetAuthorizationToken(token *types.Token, workspace *types.Workspace) error {
	bytes, err := json.Marshal(AuthInfo{
		Workspace: workspace,
		Token:     token,
	})
	if err != nil {
		return err
	}

	tokenKey := common.RedisKeys.WorkspaceAuthorizedToken(token.Key)
	err = wr.rdb.Set(context.Background(), tokenKey, bytes, time.Duration(cachedTokenTTLS)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}
