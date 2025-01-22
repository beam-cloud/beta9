package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddOnDeployStubIdToDeployment, downRemoveOnDeployStubIdFromDeployment)
}

func upAddOnDeployStubIdToDeployment(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE deployment ADD COLUMN on_deploy_stub_id INT REFERENCES stub(id);`)
	if err != nil {
		return fmt.Errorf("failed to add on_deploy_stub_id to deployment: %w", err)
	}

	return nil
}

func downRemoveOnDeployStubIdFromDeployment(ctx context.Context, tx *sql.Tx) error {
	// PostgreSQL doesn't support removing values from an ENUM directly
	_, err := tx.ExecContext(ctx, `ALTER TABLE deployment DROP COLUMN on_deploy_stub;`)
	if err != nil {
		return fmt.Errorf("failed to remove on_deploy_stub_id from deployment: %w", err)
	}
	return nil
}
