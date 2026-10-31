package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type WorkspaceRedisRepository struct {
	rdb           *common.RedisClient
	encryptionKey string
}

func NewWorkspaceRedisRepository(r *common.RedisClient, keys ...string) WorkspaceRepository {
	key := ""
	if len(keys) > 0 {
		key = keys[0]
	}
	return &WorkspaceRedisRepository{rdb: r, encryptionKey: key}
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

// Webhooks live in one hash per workspace: field = webhook id, value = JSON.
func (wr *WorkspaceRedisRepository) ListWebhooks(ctx context.Context, workspaceId string) ([]types.WorkspaceWebhook, error) {
	res, err := wr.rdb.HGetAll(ctx, common.RedisKeys.WorkspaceWebhooks(workspaceId)).Result()
	if err != nil {
		return nil, err
	}
	webhooks := make([]types.WorkspaceWebhook, 0, len(res))
	for _, raw := range res {
		var webhook types.WorkspaceWebhook
		if err := json.Unmarshal([]byte(raw), &webhook); err != nil {
			log.Warn().Err(err).Str("workspace_id", workspaceId).Msg("skipping unreadable webhook")
			continue
		}
		webhooks = append(webhooks, webhook)
	}
	sort.Slice(webhooks, func(i, j int) bool { return webhooks[i].CreatedAt.Before(webhooks[j].CreatedAt) })
	return webhooks, nil
}

func (wr *WorkspaceRedisRepository) SetWebhook(ctx context.Context, workspaceId string, webhook types.WorkspaceWebhook) error {
	raw, err := json.Marshal(webhook)
	if err != nil {
		return err
	}
	return wr.rdb.HSet(ctx, common.RedisKeys.WorkspaceWebhooks(workspaceId), webhook.ExternalId, raw).Err()
}

func (wr *WorkspaceRedisRepository) DeleteWebhook(ctx context.Context, workspaceId, webhookId string) error {
	return wr.rdb.HDel(ctx, common.RedisKeys.WorkspaceWebhooks(workspaceId), webhookId).Err()
}

type AuthInfo struct {
	WorkerID  string
	KeyHash   string
	Workspace *types.Workspace
	Token     *types.Token
}

func (wr *WorkspaceRedisRepository) AuthorizeToken(token string) (*types.Token, *types.Workspace, error) {
	tokenKey := common.RedisKeys.WorkspaceAuthorizedToken(tokenCacheDigest(token))
	res, err := wr.rdb.Get(context.Background(), tokenKey).Result()
	if err != nil {
		return nil, nil, err
	}

	if res == "" {
		return nil, nil, errors.New("token not found")
	}

	if !strings.HasPrefix(res, "enc:") || !strings.HasPrefix(wr.encryptionKey, "sk_") {
		return nil, nil, errors.New("authorization cache miss")
	}
	key, err := common.ParseSecretKey(wr.encryptionKey)
	if err != nil {
		return nil, nil, err
	}
	res, err = common.Decrypt(key, strings.TrimPrefix(res, "enc:"))
	if err != nil {
		return nil, nil, err
	}
	info := &AuthInfo{}
	err = json.Unmarshal([]byte(res), &info)
	if err != nil {
		return nil, nil, err
	}

	if info.Token == nil || info.Workspace == nil {
		return nil, nil, errors.New("token not found")
	}

	if !info.Token.ValidAt(time.Now()) {
		return nil, nil, errors.New("inactive token")
	}
	info.Token.Key = token
	info.Token.WorkerID = info.WorkerID
	info.Token.KeyHash = info.KeyHash
	return info.Token, info.Workspace, nil
}

func (wr *WorkspaceRedisRepository) RevokeToken(tokenKey string) error {
	err := wr.rdb.Del(context.Background(), common.RedisKeys.WorkspaceAuthorizedToken(tokenCacheDigest(tokenKey))).Err()
	if err != nil {
		return err
	}

	return nil
}

func (wr *WorkspaceRedisRepository) SetAuthorizationToken(token *types.Token, workspace *types.Workspace) error {
	if wr.encryptionKey == "" || !token.Reusable {
		return nil
	}
	cacheKey := tokenCacheDigest(token.Key)
	copied := *token
	copied.Key = ""
	token = &copied
	bytes, err := json.Marshal(AuthInfo{
		WorkerID: token.WorkerID, KeyHash: token.KeyHash,
		Workspace: workspace,
		Token:     token,
	})
	if err != nil {
		return err
	}

	key, err := common.ParseSecretKey(wr.encryptionKey)
	if err != nil {
		return err
	}
	encrypted, err := common.Encrypt(key, string(bytes))
	if err != nil {
		return err
	}
	if err := wr.rdb.Set(context.Background(), common.RedisKeys.WorkspaceAuthorizedToken(cacheKey), "enc:"+encrypted, time.Duration(cachedTokenTTLS)*time.Second).Err(); err != nil {
		return err
	}
	return nil
}

func tokenCacheDigest(key string) string {
	if strings.HasPrefix(key, "sha256:") {
		return strings.TrimPrefix(key, "sha256:")
	}
	return tokenDigest(key)
}
