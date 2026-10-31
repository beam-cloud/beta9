package repository

import (
	"context"
	"fmt"
)

type workerTokenBinder interface {
	BindWorkerToken(context.Context, string, string) error
}

func BindWorkerToken(ctx context.Context, backend BackendRepository, tokenID, workerID string) error {
	binder, ok := backend.(workerTokenBinder)
	if !ok || tokenID == "" || workerID == "" {
		return fmt.Errorf("worker credential binding is unavailable")
	}
	return binder.BindWorkerToken(ctx, tokenID, workerID)
}

func (r *PostgresBackendRepository) BindWorkerToken(ctx context.Context, tokenID, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("worker is required")
	}
	result, err := r.client.ExecContext(ctx, `UPDATE token SET worker_id=$2,expires_at=COALESCE(expires_at,NOW()+INTERVAL '10 minutes') WHERE external_id=$1 AND token_type IN ('worker','worker_private') AND (worker_id='' OR worker_id=$2) AND active=TRUE AND disabled_by_cluster_admin=FALSE`, tokenID, workerID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("worker credential cannot be bound")
	}
	return nil
}

func (r *PostgresBackendRepository) RenewWorkerToken(ctx context.Context, tokenID, workerID string) error {
	result, err := r.client.ExecContext(ctx, `UPDATE token SET expires_at=NOW()+INTERVAL '10 minutes' WHERE external_id=$1 AND worker_id=$2 AND active=TRUE AND disabled_by_cluster_admin=FALSE AND (expires_at IS NULL OR expires_at>NOW())`, tokenID, workerID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("worker credential is no longer active")
	}
	return nil
}

func (r *PostgresBackendRepository) RevokeWorkerTokens(ctx context.Context, workerID string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE token SET active=FALSE WHERE worker_id=$1 AND token_type IN ('worker','worker_private')`, workerID)
	return err
}
