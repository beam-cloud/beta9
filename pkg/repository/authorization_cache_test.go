package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAuthorizationCacheStorageAndEviction(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	key := "sk_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	repo := NewWorkspaceRedisRepository(rdb, key)
	expiry := time.Now().Add(time.Minute)
	token := &types.Token{Key: "presented-credential", KeyHash: tokenDigest("presented-credential"), WorkerID: "worker-one", Active: true, Reusable: true, ExpiresAt: &expiry}
	require.NoError(t, repo.SetAuthorizationToken(token, &types.Workspace{ExternalId: "workspace"}))
	for _, redisKey := range server.Keys() {
		require.NotContains(t, redisKey, token.Key)
		value, err := server.Get(redisKey)
		require.NoError(t, err)
		require.NotContains(t, value, token.Key)
		require.NotContains(t, value, "worker-one")
	}
	cached, _, err := repo.AuthorizeToken(token.Key)
	require.NoError(t, err)
	require.Equal(t, "worker-one", cached.WorkerID)
	require.Equal(t, token.Key, cached.Key)
	require.NoError(t, repo.RevokeToken(token.CacheKey()))
	_, _, err = repo.AuthorizeToken(token.Key)
	require.Error(t, err)
	require.NoError(t, repo.SetAuthorizationToken(token, &types.Workspace{}))
	server.FastForward(10 * time.Minute)
	_, _, err = repo.AuthorizeToken(token.Key)
	require.Error(t, err)
	// Records from the previous format are treated as cache misses.
	require.NoError(t, rdb.Set(context.Background(), common.RedisKeys.WorkspaceAuthorizedToken(tokenDigest(token.Key)), `{"Token":{"Key":"legacy"}}`, time.Minute).Err())
	_, _, err = repo.AuthorizeToken(token.Key)
	require.Error(t, err)
}
