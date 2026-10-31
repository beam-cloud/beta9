package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const feedbackColumns = "id, external_id::text AS external_id, workspace_id, token_id, kind, summary, blocked, command, error_code, error_message, details, suggested_improvement, model, environment, caller, agent_session, idempotency_key, created_at"

// CreateFeedback stores a report. A report that repeats an earlier one's idempotency key
// returns the earlier report, and false.
func (r *PostgresBackendRepository) CreateFeedback(ctx context.Context, feedback types.Feedback) (types.Feedback, bool, error) {
	query := `
	INSERT INTO feedback (
		workspace_id, token_id, kind, summary, blocked, command, error_code, error_message,
		details, suggested_improvement, model, environment, caller, agent_session, idempotency_key
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	ON CONFLICT (workspace_id, idempotency_key) WHERE idempotency_key <> '' DO NOTHING
	RETURNING ` + feedbackColumns + `;`

	var created types.Feedback
	err := r.client.GetContext(ctx, &created, query,
		feedback.WorkspaceId, feedback.TokenId, feedback.Kind, feedback.Summary, feedback.Blocked,
		feedback.Command, feedback.ErrorCode, feedback.ErrorMessage, feedback.Details,
		feedback.SuggestedImprovement, feedback.Model, feedback.Environment, feedback.Caller,
		feedback.AgentSession, feedback.IdempotencyKey,
	)
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) || feedback.IdempotencyKey == "" {
		return types.Feedback{}, false, err
	}

	var existing types.Feedback
	query = `SELECT ` + feedbackColumns + ` FROM feedback WHERE workspace_id = $1 AND idempotency_key = $2;`
	if err := r.client.GetContext(ctx, &existing, query, feedback.WorkspaceId, feedback.IdempotencyKey); err != nil {
		return types.Feedback{}, false, err
	}
	return existing, false, nil
}

// ListRecentFeedback returns a workspace's reports created since `since`, newest first.
func (r *PostgresBackendRepository) ListRecentFeedback(ctx context.Context, workspaceId uint, since time.Time, limit int) ([]types.Feedback, error) {
	query := `SELECT ` + feedbackColumns + ` FROM feedback WHERE workspace_id = $1 AND created_at >= $2 ORDER BY created_at DESC LIMIT $3;`

	feedback := []types.Feedback{}
	if err := r.client.SelectContext(ctx, &feedback, query, workspaceId, since, limit); err != nil {
		return nil, err
	}
	return feedback, nil
}
