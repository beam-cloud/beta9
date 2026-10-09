package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upVMStubKind, downVMStubKind) }

func upVMStubKind(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TYPE stub_type ADD VALUE IF NOT EXISTS 'vm';
ALTER TABLE vm_artifact DROP CONSTRAINT vm_artifact_vm_id_fkey;
ALTER TABLE persistent_vm ALTER COLUMN id TYPE TEXT USING id::text;
ALTER TABLE vm_artifact ALTER COLUMN vm_id TYPE TEXT USING vm_id::text;
ALTER TABLE vm_artifact ADD CONSTRAINT vm_artifact_vm_id_fkey FOREIGN KEY (vm_id) REFERENCES persistent_vm(id);`)
	return err
}

func downVMStubKind(context.Context, *sql.Tx) error {
	// Existing short IDs cannot be converted back to UUIDs. PostgreSQL also
	// cannot remove an enum value without replacing its type.
	return nil
}
