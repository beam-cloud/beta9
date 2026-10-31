package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func feedbackRows(externalId, idempotencyKey string, createdAt time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "external_id", "workspace_id", "token_id", "kind", "summary", "blocked", "command", "error_code",
		"error_message", "details", "suggested_improvement", "model", "environment", "caller", "agent_session",
		"idempotency_key", "created_at",
	}).AddRow(
		uint(1), externalId, uint(7), nil, types.FeedbackKindBug, "deploy ignored --port", true, "deploy", "ERROR",
		"", "", "", "", "", "cli/1 (cursor)", "run-1", idempotencyKey, createdAt,
	)
}

func TestCreateFeedbackReturnsTheReportAnIdempotencyKeyRepeats(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	now := time.Now().UTC()
	feedback := types.Feedback{WorkspaceId: 7, Kind: types.FeedbackKindBug, Summary: "deploy ignored --port", IdempotencyKey: "retry-1"}

	mock.ExpectQuery(`INSERT INTO feedback .* ON CONFLICT \(workspace_id, idempotency_key\) WHERE idempotency_key <> '' DO NOTHING`).
		WillReturnRows(feedbackRows("fb-1", "retry-1", now))
	created, isNew, err := repo.CreateFeedback(context.Background(), feedback)
	require.NoError(t, err)
	require.True(t, isNew)
	require.Equal(t, "fb-1", created.ExternalId)
	require.Nil(t, created.TokenId)

	mock.ExpectQuery(`INSERT INTO feedback`).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT .* FROM feedback WHERE workspace_id = \$1 AND idempotency_key = \$2`).
		WithArgs(uint(7), "retry-1").
		WillReturnRows(feedbackRows("fb-1", "retry-1", now))
	existing, isNew, err := repo.CreateFeedback(context.Background(), feedback)
	require.NoError(t, err)
	require.False(t, isNew)
	require.Equal(t, "fb-1", existing.ExternalId)

	feedback.IdempotencyKey = ""
	mock.ExpectQuery(`INSERT INTO feedback`).WillReturnError(sql.ErrNoRows)
	_, _, err = repo.CreateFeedback(context.Background(), feedback)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListRecentFeedback(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	since := time.Now().Add(-time.Hour)

	mock.ExpectQuery(`SELECT .* FROM feedback WHERE workspace_id = \$1 AND created_at >= \$2 ORDER BY created_at DESC LIMIT \$3`).
		WithArgs(uint(7), since, 500).
		WillReturnRows(feedbackRows("fb-1", "", time.Now()))
	recent, err := repo.ListRecentFeedback(context.Background(), 7, since, 500)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	require.Equal(t, "run-1", recent[0].AgentSession)
	require.NoError(t, mock.ExpectationsWereMet())
}
