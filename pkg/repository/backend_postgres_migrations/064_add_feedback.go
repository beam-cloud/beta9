package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddFeedback, downAddFeedback)
}

func upAddFeedback(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS feedback (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			token_id INT REFERENCES token(id) ON DELETE SET NULL,
			kind TEXT NOT NULL,
			summary TEXT NOT NULL,
			blocked BOOLEAN NOT NULL DEFAULT FALSE,
			command TEXT NOT NULL DEFAULT '',
			error_code TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			details TEXT NOT NULL DEFAULT '',
			suggested_improvement TEXT NOT NULL DEFAULT '',
			model TEXT NOT NULL DEFAULT '',
			environment TEXT NOT NULL DEFAULT '',
			caller TEXT NOT NULL DEFAULT '',
			agent_session TEXT NOT NULL DEFAULT '',
			idempotency_key TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_feedback_workspace_created ON feedback(workspace_id, created_at DESC);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_feedback_workspace_idempotency ON feedback(workspace_id, idempotency_key) WHERE idempotency_key <> '';`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downAddFeedback(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS feedback;`)
	return err
}
