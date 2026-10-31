package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type WorkerImageGrant struct {
	BuildSecrets      []string `json:"build_secrets"`
	GitToken          string   `json:"git_token"`
	WorkerID          string   `json:"worker_id"`
	ContainerID       string   `json:"container_id"`
	WorkspaceID       string   `json:"workspace_id"`
	StubID            string   `json:"stub_id"`
	ImageID           string   `json:"image_id"`
	OutputImageID     string   `json:"output_image_id"`
	Build             bool     `json:"build"`
	Export            bool     `json:"export"`
	SourceImage       string   `json:"source_image"`
	SourceCredentials string   `json:"source_credentials"`
	ImageCredentials  string   `json:"image_credentials"`
}

type WorkerImageGrantRepository struct {
	rdb *common.RedisClient
	key string
}

func NewWorkerImageGrantRepository(rdb *common.RedisClient, key string) *WorkerImageGrantRepository {
	return &WorkerImageGrantRepository{rdb: rdb, key: key}
}
func (r *WorkerImageGrantRepository) Save(ctx context.Context, workerID string, request *types.ContainerRequest) error {
	if r.rdb == nil {
		return fmt.Errorf("image grant storage is unavailable")
	}
	grant := WorkerImageGrant{BuildSecrets: append([]string(nil), request.BuildOptions.BuildSecrets...), WorkerID: workerID, ContainerID: request.ContainerId, WorkspaceID: request.WorkspaceId, StubID: request.StubId, ImageID: request.ImageId, OutputImageID: request.BuildOutputImageID, Build: request.IsBuildRequest(), Export: request.Stub.Type.IsSandbox(), SourceCredentials: request.BuildOptions.SourceImageCreds, ImageCredentials: request.ImageCredentials}
	if grant.OutputImageID == "" {
		grant.OutputImageID = grant.ImageID
	}
	if request.BuildOptions.GitSource != nil {
		grant.GitToken = request.BuildOptions.GitSource.Token
	}
	if request.BuildOptions.SourceImage != nil {
		grant.SourceImage = *request.BuildOptions.SourceImage
	}
	data, err := json.Marshal(grant)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(r.key, "sk_") {
		return fmt.Errorf("image grant encryption is unavailable")
	}
	key, err := common.ParseSecretKey(r.key)
	if err != nil {
		return err
	}
	wrapped, err := common.Encrypt(key, "worker-image-v1\x00"+workerID+"\x00"+request.ContainerId+"\x00"+string(data))
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, "worker-image-grant:"+request.ContainerId, wrapped, 24*time.Hour).Err()
}
func (r *WorkerImageGrantRepository) Get(ctx context.Context, workerID, containerID string) (*WorkerImageGrant, error) {
	if r.rdb == nil {
		return nil, fmt.Errorf("image grant storage is unavailable")
	}
	value, err := r.rdb.Get(ctx, "worker-image-grant:"+containerID).Result()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(r.key, "sk_") {
		return nil, fmt.Errorf("image grant encryption is unavailable")
	}
	key, err := common.ParseSecretKey(r.key)
	if err != nil {
		return nil, err
	}
	value, err = common.Decrypt(key, value)
	if err != nil {
		return nil, err
	}
	prefix := "worker-image-v1\x00" + workerID + "\x00" + containerID + "\x00"
	if !strings.HasPrefix(value, prefix) {
		return nil, fmt.Errorf("image grant does not match")
	}
	var grant WorkerImageGrant
	if err = json.Unmarshal([]byte(strings.TrimPrefix(value, prefix)), &grant); err != nil {
		return nil, err
	}
	if grant.WorkerID != workerID || grant.ContainerID != containerID {
		return nil, fmt.Errorf("image grant does not match")
	}
	return &grant, nil
}
