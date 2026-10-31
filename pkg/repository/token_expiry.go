package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TokenExpiry(seconds int64) (time.Time, error) {
	if seconds == 0 {
		seconds = 90 * 24 * 60 * 60
	}
	if seconds < 3600 || seconds > 365*24*60*60 {
		return time.Time{}, fmt.Errorf("token lifetime must be between one hour and one year")
	}
	return time.Now().Add(time.Duration(seconds) * time.Second), nil
}

func CreateExpiringToken(ctx context.Context, backend BackendRepository, workspaceID uint, kind string, seconds int64) (types.Token, error) {
	if creator, ok := backend.(interface {
		CreatePublicTokenWithExpiry(context.Context, uint, string, bool, *time.Time) (types.Token, error)
	}); ok {
		if kind == types.TokenTypeWorkspacePrimary && seconds == 0 {
			return backend.CreateToken(ctx, workspaceID, kind, true)
		}
		expiry, err := TokenExpiry(seconds)
		if err != nil {
			return types.Token{}, err
		}
		return creator.CreatePublicTokenWithExpiry(ctx, workspaceID, kind, true, &expiry)
	}

	if seconds == 0 {
		return backend.CreateToken(ctx, workspaceID, kind, true)
	}
	expiry, err := TokenExpiry(seconds)
	if err != nil {
		return types.Token{}, err
	}
	creator, ok := backend.(interface {
		CreateTokenWithExpiry(context.Context, uint, string, bool, *time.Time) (types.Token, error)
	})
	if !ok {
		return types.Token{}, fmt.Errorf("token expiry is unavailable")
	}
	return creator.CreateTokenWithExpiry(ctx, workspaceID, kind, true, &expiry)
}

func RenewToken(ctx context.Context, backend BackendRepository, workspaceID uint, tokenID string, seconds int64) (types.Token, error) {
	expiry, err := TokenExpiry(seconds)
	if err != nil {
		return types.Token{}, err
	}
	renewer, ok := backend.(interface {
		RenewWorkspaceToken(context.Context, uint, string, time.Time) (types.Token, error)
	})
	if !ok {
		return types.Token{}, fmt.Errorf("token renewal is unavailable")
	}
	return renewer.RenewWorkspaceToken(ctx, workspaceID, tokenID, expiry)
}

func (r *PostgresBackendRepository) RenewWorkspaceToken(ctx context.Context, workspaceID uint, tokenID string, expiresAt time.Time) (types.Token, error) {
	var token types.Token
	err := r.client.GetContext(ctx, &token, `UPDATE token SET expires_at=$3,updated_at=NOW() WHERE workspace_id=$1 AND external_id=$2 AND token_type IN ('workspace','workspace_restricted') AND active=TRUE AND disabled_by_cluster_admin=FALSE RETURNING worker_id,expires_at,COALESCE(key_hash, encode(sha256(convert_to(key,'UTF8')),'hex')) AS key_hash,id,external_id,created_at,updated_at,active,disabled_by_cluster_admin,token_type,reusable,workspace_id`, workspaceID, tokenID, expiresAt)
	return token, err
}
