package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upVMArtifactCreatedAt, downVMArtifactCreatedAt) }

// Artifact pages sort by creation time. The JSON timestamp cannot be indexed
// because its text-to-timestamptz cast is not immutable, so copy it to a column.
func upVMArtifactCreatedAt(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE vm_artifact ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
UPDATE vm_artifact SET created_at=(data->>'created_at')::timestamptz;
CREATE INDEX vm_artifact_page ON vm_artifact (workspace_id,kind,created_at DESC,id DESC);
CREATE INDEX vm_artifact_vm_page ON vm_artifact (vm_id,kind,created_at DESC,id DESC);`)
	return err
}

func downVMArtifactCreatedAt(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP INDEX vm_artifact_page; DROP INDEX vm_artifact_vm_page; ALTER TABLE vm_artifact DROP COLUMN created_at;`)
	return err
}
