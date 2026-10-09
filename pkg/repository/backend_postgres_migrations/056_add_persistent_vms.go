package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upPersistentVMs, downPersistentVMs) }

func upPersistentVMs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE persistent_vm (
 id UUID PRIMARY KEY, workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 workspace_external_id TEXT NOT NULL, token_id TEXT NOT NULL,
 name TEXT NOT NULL, handle TEXT NOT NULL UNIQUE, data JSONB NOT NULL,
 last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX persistent_vm_name ON persistent_vm (workspace_id,name) WHERE data->>'desired_state' <> 'deleted';
CREATE TABLE vm_artifact (
 id UUID PRIMARY KEY, workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 name TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('snapshot','template')),
 vm_id UUID NOT NULL REFERENCES persistent_vm(id), data JSONB NOT NULL,
 UNIQUE (workspace_id, kind, name)
);`)
	return err
}

func downPersistentVMs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE vm_artifact; DROP TABLE persistent_vm;`)
	return err
}
