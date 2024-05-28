package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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

type VolumePathTokenData struct {
	WorkspaceId string `redis:"workspace_id"`
	VolumePath  string `redis:"volume_path"`
}

func (wr *WorkspaceRedisRepository) ValidateWorkspaceVolumePathDownloadToken(workspaceId string, volumePath string, token string) error {
	key := common.RedisKeys.WorkspaceVolumePathDownloadToken(token)

	var data VolumePathTokenData
	res, err := wr.rdb.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return errors.New("invalid token")
	}

	err = common.ToStruct(res, &data)
	if err != nil {
		return err
	}

	if data.WorkspaceId != workspaceId || data.VolumePath != volumePath {
		return errors.New("invalid token")
	}

	return nil
}

func generateToken(length int) (string, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

const temporaryDownloadTokenLength = 32
const temporaryDownloadTokenExpiry = 60 // 1 minute

func (wr *WorkspaceRedisRepository) GenerateWorkspaceVolumePathDownloadToken(workspaceId string, volumePath string) (string, error) {
	token, err := generateToken(temporaryDownloadTokenLength)
	if err != nil {
		return "", err
	}

	key := common.RedisKeys.WorkspaceVolumePathDownloadToken(token)

	data := VolumePathTokenData{
		WorkspaceId: workspaceId,
		VolumePath:  volumePath,
	}

	err = wr.rdb.HMSet(context.TODO(), key, common.ToSlice(data)...).Err()
	if err != nil {
		return "", err
	}

	err = wr.rdb.Expire(context.TODO(), key, temporaryDownloadTokenExpiry*time.Second).Err()
	if err != nil {
		return "", err
	}

	return token, nil
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
