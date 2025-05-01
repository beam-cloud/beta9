package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upCreateAppTable, downDropAppTable)
}

const (
	migrationAppName = "default"
)

func upCreateAppTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS app (
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
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name='stub' AND column_name='app_id'
			) THEN
				ALTER TABLE stub ADD COLUMN app_id INT NULL REFERENCES app(id);
			END IF;
		END
		$$;
	`)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name='deployment' AND column_name='app_id'
			) THEN
				ALTER TABLE deployment ADD COLUMN app_id INT NULL REFERENCES app(id);
			END IF;
		END
		$$;
	`)
	if err != nil {
		return err
	}

	// create apps based on the deployment name for each existing deployment
	_, err = tx.ExecContext(ctx, `
		INSERT INTO app (name, workspace_id, description, created_at, updated_at)
		SELECT DISTINCT d.name, d.workspace_id, '', NOW(), NOW()
		FROM deployment d
		LEFT JOIN app a
		  ON a.workspace_id = d.workspace_id
		 AND a.name = d.name
		WHERE d.app_id IS NULL
		  AND a.id IS NULL
		RETURNING id, workspace_id, name;
	`)
	if err != nil {
		return err
	}

	// update the deployments themselves to reference the new apps
	_, err = tx.ExecContext(ctx, `
		UPDATE deployment d
		SET app_id = a.id
		FROM app a
		WHERE d.name = a.name
		  AND d.workspace_id = a.workspace_id
		  AND d.app_id IS NULL;
	`)
	if err != nil {
		return err
	}

	// update the stubs themselves to also reference the new apps
	_, err = tx.ExecContext(ctx, `
		UPDATE stub s
		SET app_id = a.id
		FROM deployment d
		JOIN app a ON d.name = a.name AND d.workspace_id = a.workspace_id
		WHERE d.stub_id = s.id
		  AND s.app_id IS NULL
		  AND d.app_id IS NOT NULL;
	`)
	if err != nil {
		return err
	}

	//  insert "Misc." app for stubs without an app (i.e. serves, functions, shells)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO app (name, workspace_id, description, created_at, updated_at)
		SELECT DISTINCT 'Misc.', s.workspace_id, '', NOW(), NOW()
		FROM stub s
		LEFT JOIN app a
		  ON a.workspace_id = s.workspace_id
		 AND a.name = 'Misc.'
		WHERE s.app_id IS NULL
		  AND a.id IS NULL
		RETURNING id, workspace_id, name;
	`)
	if err != nil {
		return err
	}

	// update remaining stubs to reference the "Misc." app
	_, err = tx.ExecContext(ctx, `
		UPDATE stub s
		SET app_id = a.id
		FROM app a
		WHERE s.workspace_id = a.workspace_id
		  AND a.name = 'Misc.'
		  AND s.app_id IS NULL;
	`)
	if err != nil {
		return err
	}

	return nil
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
