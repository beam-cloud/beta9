package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddPlatformDeployerTypes, downAddPlatformDeployerTypes)
}

func upAddPlatformDeployerTypes(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		"ALTER TYPE public.stub_type ADD VALUE IF NOT EXISTS 'platform_deployer'",
		"ALTER TYPE public.token_type ADD VALUE IF NOT EXISTS 'platform_deployer'",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downAddPlatformDeployerTypes(context.Context, *sql.Tx) error {
	// Enum values cannot be removed without rewriting tables. Leaving these
	// additive values permits a safe application rollback with existing records.
	return nil
}
