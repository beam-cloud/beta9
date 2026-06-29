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
	// Ensure the disk_snapshot table exists before altering it. If a database
	// recorded migration 041 as applied without the table actually being
	// created (e.g. a partial/failed run that left goose_db_version ahead of the
	// real schema), this guard recreates it so the ALTER below cannot fail with
	// "relation \"disk_snapshot\" does not exist".
	for _, statement := range diskSnapshotSchemaStatements() {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot ADD COLUMN IF NOT EXISTS bucket_name TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddDiskSnapshotBucketName(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot DROP COLUMN IF EXISTS bucket_name;`)
	return err
}
