package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upCreateAppTable, downDropAppTable)
}

type StubForMigration struct {
	StubID         int64
	StubName       string
	DeploymentName string
	DeploymentID   int64
	WorkspaceId    uint
}

const (
	migrationAppName = "default"
)

func upCreateAppTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE app (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() NOT NULL,
			name VARCHAR(255) NOT NULL,
			description TEXT NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id),
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMP NULL,
			UNIQUE (name, workspace_id)
		);
	`)

	// Update stubs table to add app_id column
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE stub
		ADD COLUMN app_id INT NULL REFERENCES app(id);
	`)
	if err != nil {
		return err
	}

	// Update deployments table to add app_id column
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE deployment
		ADD COLUMN app_id INT NULL REFERENCES app(id);
	`)
	if err != nil {
		return err
	}

	allWorkspacesRows, err := tx.QueryContext(ctx, `
		SELECT id FROM workspace;
	`)
	if err != nil {
		return err
	}

	allWorkspaces := []uint{}
	for allWorkspacesRows.Next() {
		var workspaceID uint
		err = allWorkspacesRows.Scan(&workspaceID)
		if err != nil {
			return err
		}
		allWorkspaces = append(allWorkspaces, workspaceID)
	}

	allWorkspacesRows.Close()

	for _, workspaceID := range allWorkspaces {
		var appID uint
		err := tx.QueryRowContext(ctx, `
			INSERT INTO app (name, description, workspace_id)
			VALUES ($1, $2, $3) RETURNING id;
		`, migrationAppName, "", workspaceID).Scan(&appID)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE stub SET app_id = $1 WHERE workspace_id = $2;
		`, appID, workspaceID)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE deployment SET app_id = $1 WHERE workspace_id = $2;
		`, appID, workspaceID)
		if err != nil {
			return err
		}
	}

	// Set app_id not not nullable on deployments and stubs
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE deployment
		ALTER COLUMN app_id SET NOT NULL;
		ALTER TABLE stub
		ALTER COLUMN app_id SET NOT NULL;
	`)

	return err
}

func downDropAppTable(ctx context.Context, tx *sql.Tx) error {
	// Remove app_id column from deployments table
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE deployment
		DROP COLUMN app_id;
	`)
	if err != nil {
		return err
	}

	// Remove app_id column from stubs table
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE stub
		DROP COLUMN app_id;
	`)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `DROP TABLE IF EXISTS app;`)
	return err
}
