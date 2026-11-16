package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upMirrors, downMirrors) }

// A generation's number is its parent's plus one and the pair (mirror,
// number) is the primary key, so two writers that start from the same parent
// cannot both commit. Databases that ran this as migration 065 already have
// the tables.
func upMirrors(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS mirror (
 id UUID PRIMARY KEY, workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 purged_at TIMESTAMPTZ, objects_deleted_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS mirror_name ON mirror (workspace_id, name) WHERE purged_at IS NULL;
CREATE TABLE IF NOT EXISTS mirror_generation (
 mirror_id UUID NOT NULL REFERENCES mirror(id), number BIGINT NOT NULL CHECK (number > 0),
 parent BIGINT NOT NULL, manifest_digest TEXT NOT NULL, manifest_size BIGINT NOT NULL,
 entries BIGINT NOT NULL, objects BIGINT NOT NULL, total_size BIGINT NOT NULL,
 token_id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 expired_at TIMESTAMPTZ,
 PRIMARY KEY (mirror_id, number)
);`)
	return err
}

func downMirrors(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE mirror_generation; DROP TABLE mirror;`)
	return err
}
