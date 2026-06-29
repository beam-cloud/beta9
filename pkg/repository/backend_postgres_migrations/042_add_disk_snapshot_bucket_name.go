package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddDiskSnapshotBucketName, downAddDiskSnapshotBucketName)
}

func upAddDiskSnapshotBucketName(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot ADD COLUMN IF NOT EXISTS bucket_name TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddDiskSnapshotBucketName(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot DROP COLUMN IF EXISTS bucket_name;`)
	return err
}
