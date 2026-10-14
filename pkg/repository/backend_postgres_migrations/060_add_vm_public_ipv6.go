package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upVMPublicIPv6, downVMPublicIPv6) }

func upVMPublicIPv6(ctx context.Context, tx *sql.Tx) error {
	// Deleted identities retain their reservation so stale DNS or connections
	// cannot reach another tenant. Addresses live outside snapshot/template specs.
	_, err := tx.ExecContext(ctx, `
ALTER TABLE persistent_vm ADD COLUMN public_ipv6 INET CHECK (public_ipv6 IS NULL OR (family(public_ipv6)=6 AND masklen(public_ipv6)=128));
CREATE UNIQUE INDEX persistent_vm_public_ipv6 ON persistent_vm (public_ipv6);`)
	return err
}

func downVMPublicIPv6(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE persistent_vm DROP COLUMN public_ipv6`)
	return err
}
