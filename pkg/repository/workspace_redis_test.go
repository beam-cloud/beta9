package repository

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceAuthorizationCacheHidesCredentials(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkspaceRedisRepository(rdb, "sk_"+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	signingKey := "sk_workspace-signing-key"
	workspace := &types.Workspace{ExternalId: "workspace", SigningKey: &signingKey}

	token := &types.Token{Key: "token-key", TokenType: types.TokenTypeWorkspace, Active: true}
	require.NoError(t, repo.SetAuthorizationToken(token, workspace))
	keys, err := rdb.Keys(context.Background(), "*")
	require.NoError(t, err)
	require.Equal(t, []string{common.RedisKeys.WorkspaceAuthorizedToken(tokenKeyHash("token-key"))}, keys)
	raw, err := rdb.Get(context.Background(), keys[0]).Result()
	require.NoError(t, err)
	require.NotContains(t, raw, "token-key")
	require.NotContains(t, raw, signingKey)

	cachedToken, cachedWorkspace, err := repo.AuthorizeToken("token-key")
	require.NoError(t, err)
	require.Equal(t, "token-key", cachedToken.Key)
	require.Equal(t, signingKey, *cachedWorkspace.SigningKey)

	require.NoError(t, repo.RevokeToken("token-key"))
	_, _, err = repo.AuthorizeToken("token-key")
	require.Error(t, err)

	require.NoError(t, repo.SetAuthorizationToken(&types.Token{Key: "unbound", TokenType: types.TokenTypeWorker, Active: true}, workspace))
	_, _, err = repo.AuthorizeToken("unbound")
	require.Error(t, err, "unbound worker tokens are not cached")
	require.NoError(t, repo.SetAuthorizationToken(&types.Token{Key: "bound", TokenType: types.TokenTypeWorker, WorkerID: "worker", Active: true}, workspace))
	cachedToken, _, err = repo.AuthorizeToken("bound")
	require.NoError(t, err)
	require.Equal(t, "worker", cachedToken.WorkerID)
}
