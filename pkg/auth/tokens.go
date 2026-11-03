package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// TokenChangeError is why a caller can't toggle or delete a token.
type TokenChangeError string

func (e TokenChangeError) Error() string { return string(e) }

const (
	ErrTokenNotFound  TokenChangeError = "invalid token ID"
	ErrPrimaryToken   TokenChangeError = "only a cluster admin can change the primary token"
	ErrProtectedToken TokenChangeError = "worker and cluster admin tokens can't be changed"
)

var (
	errInactiveToken    = errors.New("token is inactive")
	errRetiredToken     = errors.New("token type is retired")
	errWorkerToken      = errors.New("worker tokens can't call this API")
	errWorkspaceStorage = errors.New("workspace storage is unavailable")
)

const tokenEvictionAttempts = 3

// Tokens authenticates API tokens and changes them. Every change also evicts
// the token from the authorization cache, so it applies to the next request
// rather than when the cached authorization expires.
type Tokens struct {
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
}

func NewTokens(backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository) Tokens {
	return Tokens{backendRepo: backendRepo, workspaceRepo: workspaceRepo}
}

// Authenticate returns the active token with this key and its workspace.
func (t Tokens) Authenticate(ctx context.Context, key string) (*types.Token, *types.Workspace, error) {
	return t.authenticate(ctx, key, true)
}

// AuthenticateUser is Authenticate for APIs that worker tokens can't call. It
// rejects them before preparing their workspace.
func (t Tokens) AuthenticateUser(ctx context.Context, key string) (*types.Token, *types.Workspace, error) {
	return t.authenticate(ctx, key, false)
}

func (t Tokens) authenticate(ctx context.Context, key string, workers bool) (*types.Token, *types.Workspace, error) {
	token, workspace, err := t.workspaceRepo.AuthorizeToken(key)
	if err != nil {
		if token, workspace, err = t.backendRepo.AuthorizeToken(ctx, key); err != nil {
			return nil, nil, err
		}
		_ = t.workspaceRepo.SetAuthorizationToken(token, workspace)
	}
	if !token.Active || token.DisabledByClusterAdmin {
		return nil, nil, errInactiveToken
	}
	if !types.IsTokenType(token.TokenType) {
		return nil, nil, errRetiredToken
	}
	if !workers && types.IsWorkerTokenType(token.TokenType) {
		return nil, nil, errWorkerToken
	}
	if !workspace.StorageAvailable() {
		if err := ensureWorkspaceStorage(ctx, workspace); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", errWorkspaceStorage, err)
		}
		_ = t.workspaceRepo.SetAuthorizationToken(token, workspace)
	}
	return token, workspace, nil
}

// Revoke deactivates a token by its external ID.
func (t Tokens) Revoke(ctx context.Context, externalID string) error {
	key, err := t.backendRepo.RevokeTokenByExternalId(ctx, externalID)
	if err != nil {
		return err
	}
	return t.evict(key)
}

// Toggle activates or deactivates a token the caller may change.
func (t Tokens) Toggle(ctx context.Context, caller *types.Token, workspaceID uint, externalID string) (types.Token, error) {
	var toggled types.Token
	err := t.change(ctx, caller, workspaceID, externalID, func() (err error) {
		toggled, err = t.backendRepo.ToggleToken(ctx, workspaceID, externalID)
		return err
	})
	return toggled, err
}

// Delete deletes a token the caller may change.
func (t Tokens) Delete(ctx context.Context, caller *types.Token, workspaceID uint, externalID string) error {
	return t.change(ctx, caller, workspaceID, externalID, func() error {
		return t.backendRepo.DeleteToken(ctx, workspaceID, externalID)
	})
}

// SetDisabledByClusterAdmin disables or re-enables every token in a workspace
// except cluster admin and cluster worker tokens.
func (t Tokens) SetDisabledByClusterAdmin(ctx context.Context, workspaceID uint, disabled bool) error {
	keys, err := t.backendRepo.SetWorkspaceTokensDisabled(ctx, workspaceID, disabled)
	if err != nil {
		return err
	}
	return t.evict(keys...)
}

// change applies a change to a token the caller may change. The token is
// evicted before the change too, so a failed eviction afterwards can only
// leave an authorization cached while the change ran.
func (t Tokens) change(ctx context.Context, caller *types.Token, workspaceID uint, externalID string, apply func() error) error {
	token, err := t.backendRepo.GetTokenByExternalId(ctx, workspaceID, externalID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTokenNotFound
	}
	if err != nil {
		return err
	}
	switch {
	case token.TokenType == types.TokenTypeClusterAdmin || types.IsWorkerTokenType(token.TokenType):
		return ErrProtectedToken
	case token.TokenType == types.TokenTypeWorkspacePrimary && (caller == nil || caller.TokenType != types.TokenTypeClusterAdmin):
		return ErrPrimaryToken
	}
	if err := t.evict(token.Key); err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	return t.evict(token.Key)
}

// evict drops authorizations from the cache, retrying briefly because a missed
// eviction keeps a changed token usable until its cache entry expires.
func (t Tokens) evict(keys ...string) error {
	for _, key := range keys {
		if key == "" {
			continue
		}
		var err error
		for attempt := 1; attempt <= tokenEvictionAttempts; attempt++ {
			if err = t.workspaceRepo.RevokeToken(key); err == nil {
				break
			}
			if attempt < tokenEvictionAttempts {
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
