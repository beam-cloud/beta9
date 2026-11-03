package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type tokensBackendRepo struct {
	repository.BackendRepository
	token    *types.Token
	err      error
	lookups  int
	disabled []string

	cache               *tokensWorkspaceRepo
	changed             bool
	evictedBeforeChange []string
}

func (r *tokensBackendRepo) change() {
	r.changed = true
	if r.cache != nil {
		r.evictedBeforeChange = slices.Clone(r.cache.evicted)
	}
}

func (r *tokensBackendRepo) AuthorizeToken(context.Context, string) (*types.Token, *types.Workspace, error) {
	r.lookups++
	if r.token == nil {
		return nil, nil, errors.New("token not found")
	}
	return r.token, &types.Workspace{}, nil
}

func (r *tokensBackendRepo) GetTokenByExternalId(context.Context, uint, string) (*types.Token, error) {
	return r.token, r.err
}

func (r *tokensBackendRepo) DeleteToken(context.Context, uint, string) error {
	r.change()
	return nil
}

func (r *tokensBackendRepo) ToggleToken(context.Context, uint, string) (types.Token, error) {
	r.change()
	return *r.token, nil
}

func (r *tokensBackendRepo) RevokeTokenByExternalId(context.Context, string) (string, error) {
	return r.token.Key, nil
}

func (r *tokensBackendRepo) SetWorkspaceTokensDisabled(context.Context, uint, bool) ([]string, error) {
	return r.disabled, nil
}

type tokensWorkspaceRepo struct {
	repository.WorkspaceRepository
	cached   *types.Token
	failures int
	evicted  []string
}

func (r *tokensWorkspaceRepo) AuthorizeToken(string) (*types.Token, *types.Workspace, error) {
	if r.cached == nil {
		return nil, nil, errors.New("not cached")
	}
	return r.cached, &types.Workspace{}, nil
}

func (r *tokensWorkspaceRepo) SetAuthorizationToken(token *types.Token, _ *types.Workspace) error {
	r.cached = token
	return nil
}

func (r *tokensWorkspaceRepo) RevokeToken(key string) error {
	if r.failures > 0 {
		r.failures--
		return errors.New("redis unavailable")
	}
	r.evicted = append(r.evicted, key)
	return nil
}

func TestAuthenticateCachesTheBackendAuthorization(t *testing.T) {
	backendRepo := &tokensBackendRepo{token: &types.Token{Key: "key", Active: true, TokenType: types.TokenTypeWorkspace}}
	workspaceRepo := &tokensWorkspaceRepo{}
	tokens := NewTokens(backendRepo, workspaceRepo)

	for range 2 {
		token, _, err := tokens.Authenticate(context.Background(), "key")
		require.NoError(t, err)
		require.Equal(t, "key", token.Key)
	}
	require.Equal(t, 1, backendRepo.lookups)
}

func TestAuthenticateRejectsInactiveTokens(t *testing.T) {
	for _, token := range []*types.Token{
		{Key: "key"},
		{Key: "key", Active: true, DisabledByClusterAdmin: true},
	} {
		tokens := NewTokens(&tokensBackendRepo{token: token}, &tokensWorkspaceRepo{})
		_, _, err := tokens.Authenticate(context.Background(), "key")
		require.ErrorIs(t, err, errInactiveToken)
	}
}

func TestAuthenticateRejectsRetiredTokenTypes(t *testing.T) {
	for _, tokenType := range []string{"machine", ""} {
		token := &types.Token{Key: "key", Active: true, TokenType: tokenType}
		for name, tokens := range map[string]Tokens{
			"database": NewTokens(&tokensBackendRepo{token: token}, &tokensWorkspaceRepo{}),
			"cache":    NewTokens(&tokensBackendRepo{}, &tokensWorkspaceRepo{cached: token}),
		} {
			_, _, err := tokens.Authenticate(context.Background(), "key")
			require.ErrorIs(t, err, errRetiredToken, name)
			_, _, err = tokens.AuthenticateUser(context.Background(), "key")
			require.ErrorIs(t, err, errRetiredToken, name)
		}
	}
}

func TestAuthenticateUserRejectsWorkerTokensBeforePreparingStorage(t *testing.T) {
	prepared := false
	previous := EnsureWorkspaceStorage
	EnsureWorkspaceStorage = func(context.Context, *types.Workspace) error {
		prepared = true
		return nil
	}
	defer func() { EnsureWorkspaceStorage = previous }()

	for _, tokenType := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate} {
		tokens := NewTokens(&tokensBackendRepo{token: &types.Token{Key: "key", Active: true, TokenType: tokenType}}, &tokensWorkspaceRepo{})
		_, _, err := tokens.AuthenticateUser(context.Background(), "key")
		require.ErrorIs(t, err, errWorkerToken, tokenType)
		require.False(t, prepared, tokenType)

		_, _, err = tokens.Authenticate(context.Background(), "key")
		require.NoError(t, err, tokenType)
		prepared = false
	}
}

func TestChangeChecksTheTokenAndEvictsItAroundTheChange(t *testing.T) {
	clusterAdmin := &types.Token{TokenType: types.TokenTypeClusterAdmin}
	for _, tc := range []struct {
		name   string
		token  *types.Token
		err    error
		caller *types.Token
		want   error
	}{
		{name: "workspace token", token: &types.Token{Key: "key", TokenType: types.TokenTypeWorkspace}},
		{name: "primary token", token: &types.Token{Key: "key", TokenType: types.TokenTypeWorkspacePrimary}, want: ErrPrimaryToken},
		{name: "primary token by cluster admin", token: &types.Token{Key: "key", TokenType: types.TokenTypeWorkspacePrimary}, caller: clusterAdmin},
		{name: "cluster admin token", token: &types.Token{Key: "key", TokenType: types.TokenTypeClusterAdmin}, caller: clusterAdmin, want: ErrProtectedToken},
		{name: "worker token", token: &types.Token{Key: "key", TokenType: types.TokenTypeWorker}, caller: clusterAdmin, want: ErrProtectedToken},
		{name: "private worker token", token: &types.Token{Key: "key", TokenType: types.TokenTypeWorkerPrivate}, caller: clusterAdmin, want: ErrProtectedToken},
		{name: "missing token", err: fmt.Errorf("lookup: %w", sql.ErrNoRows), want: ErrTokenNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := tc.caller
			if caller == nil {
				caller = &types.Token{TokenType: types.TokenTypeWorkspace}
			}
			for name, change := range map[string]func(Tokens) error{
				"delete": func(tokens Tokens) error { return tokens.Delete(context.Background(), caller, 1, "token") },
				"toggle": func(tokens Tokens) error {
					_, err := tokens.Toggle(context.Background(), caller, 1, "token")
					return err
				},
			} {
				workspaceRepo := &tokensWorkspaceRepo{}
				backendRepo := &tokensBackendRepo{token: tc.token, err: tc.err, cache: workspaceRepo}
				err := change(NewTokens(backendRepo, workspaceRepo))
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want, name)
					require.False(t, backendRepo.changed, name)
					require.Empty(t, workspaceRepo.evicted, name)
					continue
				}
				require.NoError(t, err, name)
				require.True(t, backendRepo.changed, name)
				require.Equal(t, []string{"key"}, backendRepo.evictedBeforeChange, name)
				require.Equal(t, []string{"key", "key"}, workspaceRepo.evicted, name)
			}
		})
	}
}

func TestChangeDoesNotHideBackendErrors(t *testing.T) {
	outage := errors.New("connection refused")
	tokens := NewTokens(&tokensBackendRepo{err: outage}, &tokensWorkspaceRepo{})
	err := tokens.Delete(context.Background(), &types.Token{}, 1, "token")
	require.ErrorIs(t, err, outage)
	var rejected TokenChangeError
	require.False(t, errors.As(err, &rejected))
}

func TestChangeIsNotAppliedWhenTheTokenCannotBeEvicted(t *testing.T) {
	backendRepo := &tokensBackendRepo{token: &types.Token{Key: "key", TokenType: types.TokenTypeWorkspace}}
	workspaceRepo := &tokensWorkspaceRepo{failures: tokenEvictionAttempts}
	require.Error(t, NewTokens(backendRepo, workspaceRepo).Delete(context.Background(), &types.Token{}, 1, "token"))
	require.False(t, backendRepo.changed)
}

func TestEvictionRetries(t *testing.T) {
	workspaceRepo := &tokensWorkspaceRepo{failures: tokenEvictionAttempts - 1}
	backendRepo := &tokensBackendRepo{token: &types.Token{Key: "key"}}
	require.NoError(t, NewTokens(backendRepo, workspaceRepo).Revoke(context.Background(), "token"))
	require.Equal(t, []string{"key"}, workspaceRepo.evicted)
}

func TestSetDisabledByClusterAdminEvictsEveryToken(t *testing.T) {
	workspaceRepo := &tokensWorkspaceRepo{}
	backendRepo := &tokensBackendRepo{disabled: []string{"key-1", "", "key-2"}}
	require.NoError(t, NewTokens(backendRepo, workspaceRepo).SetDisabledByClusterAdmin(context.Background(), 1, true))
	require.Equal(t, []string{"key-1", "key-2"}, workspaceRepo.evicted)
}
