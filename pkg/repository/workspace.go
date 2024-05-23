package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
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
