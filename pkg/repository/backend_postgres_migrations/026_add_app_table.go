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

	// Get all stubs that have deployments
	StubWithDeployment := []StubForMigration{}
	stubWithDeploymentRows, err := tx.QueryContext(ctx, `
		SELECT s.workspace_id AS workspace_id, s.id AS stub_id, s.name AS stub_name, d.id AS deployment_id, d.name AS deployment_name FROM stub s JOIN deployment d ON s.id = d.stub_id;
	`)
	if err != nil {
		return err
	}

	for stubWithDeploymentRows.Next() {
		row := StubForMigration{}
		err = stubWithDeploymentRows.Scan(&row.WorkspaceId, &row.StubID, &row.StubName, &row.DeploymentID, &row.DeploymentName)
		if err != nil {
			return err
		}
		StubWithDeployment = append(StubWithDeployment, row)
	}

	existingDeploymentNamesToAppID := make(map[string]int)
	for _, stub := range StubWithDeployment {
		// Check if the deployment name already exists in the map
		var appId uint
		if appID, exists := existingDeploymentNamesToAppID[stub.DeploymentName]; exists {
			appId = uint(appID)
		} else {
			// Insert a new app record
			err := tx.QueryRowContext(
				ctx,
				`INSERT INTO app (name, description, workspace_id)
				VALUES ($1, $2, $3)
				RETURNING id;
			`, stub.DeploymentName, "", stub.WorkspaceId).Scan(&appId)
			if err != nil {
				return err
			}
			existingDeploymentNamesToAppID[stub.DeploymentName] = int(appId)
		}

		_, err := tx.ExecContext(ctx, `
			UPDATE stub
			SET app_id = $1
			WHERE id = $2;
		`, appId, stub.StubID)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE deployment
			SET app_id = $1
			WHERE id = $2;
		`, appId, stub.DeploymentID)
		if err != nil {
			return err
		}
	}

	// Set app_id to NULL for stubs without deployments
	StubsWithoutDeployment := []StubForMigration{}
	stubWithoutDeploymentRows, err := tx.QueryContext(ctx, `
		SELECT s.workspace_id as workspace_id, s.id as stub_id, s.name as stub_name
		FROM stub s
		LEFT JOIN deployment d ON s.id = d.stub_id
		WHERE d.stub_id IS NULL;
	`)
	if err != nil {
		return err
	}

	for stubWithoutDeploymentRows.Next() {
		row := StubForMigration{}
		err = stubWithoutDeploymentRows.Scan(&row.WorkspaceId, &row.StubID, &row.StubName)
		if err != nil {
			return err
		}
		StubsWithoutDeployment = append(StubsWithoutDeployment, row)
	}

	stubNameToAppID := make(map[string]int)
	for _, stub := range StubsWithoutDeployment {
		// Check if the stub name already exists in the map
		var appId uint
		if appID, exists := stubNameToAppID[stub.StubName]; exists {
			appId = uint(appID)
		} else {
			// Insert a new app record
			err := tx.QueryRowContext(
				ctx,
				`INSERT INTO app (name, description, workspace_id)
				VALUES ($1, $2, $3)
				RETURNING id;
			`, stub.StubName, "", stub.WorkspaceId).Scan(&appId)
			if err != nil {
				return err
			}
			stubNameToAppID[stub.StubName] = int(appId)
		}

		_, err := tx.ExecContext(ctx, `
			UPDATE stub
			SET app_id = $1
			WHERE id = $2;
		`, appId, stub.StubID)
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
