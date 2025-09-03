package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCheckpoint, downAddCheckpoint)
}

func upAddCheckpoint(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			id SERIAL PRIMARY KEY,
			checkpoint_id TEXT NOT NULL UNIQUE,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			source_container_id TEXT NOT NULL,
			container_ip TEXT NOT NULL,
			status TEXT NOT NULL,
			remote_key TEXT NOT NULL,
			workspace_id INT REFERENCES workspace(id),
			stub_id INT REFERENCES stub(id),
			stub_type TEXT NOT NULL,
			app_id INT REFERENCES app(id),
			exposed_ports INT[] DEFAULT '{}',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			last_restored_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`)
	return err
}

func downAddCheckpoint(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS checkpoint;`)
	return err
}
