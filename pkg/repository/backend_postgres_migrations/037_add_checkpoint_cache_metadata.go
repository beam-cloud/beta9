package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCheckpointCacheMetadata, downAddCheckpointCacheMetadata)
}

func upAddCheckpointCacheMetadata(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS cache_hash TEXT NOT NULL DEFAULT '';
		ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS cache_size_bytes BIGINT NOT NULL DEFAULT 0;
		ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS origin_key TEXT NOT NULL DEFAULT '';
		ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS locality TEXT NOT NULL DEFAULT '';
		ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS accelerator TEXT NOT NULL DEFAULT '';
	`)
	return err
}

func downAddCheckpointCacheMetadata(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE checkpoint DROP COLUMN IF EXISTS accelerator;
		ALTER TABLE checkpoint DROP COLUMN IF EXISTS locality;
		ALTER TABLE checkpoint DROP COLUMN IF EXISTS origin_key;
		ALTER TABLE checkpoint DROP COLUMN IF EXISTS cache_size_bytes;
		ALTER TABLE checkpoint DROP COLUMN IF EXISTS cache_hash;
	`)
	return err
}
