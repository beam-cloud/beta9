package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upSubdomain, downSubdomain)
}

func upSubdomain(ctx context.Context, tx *sql.Tx) error {
	/*
		Update the deployment table to include a subdomain column
	*/
	createColumn := `
		ALTER TABLE deployment
		ADD COLUMN subdomain varchar(64),
		ADD CONSTRAINT deployment_subdomain_check CHECK (subdomain ~ '^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$');
	`
	if _, err := tx.ExecContext(ctx, createColumn); err != nil {
		return err
	}

	updateTable := `
		UPDATE deployment d
		SET subdomain = NULLIF(
			REGEXP_REPLACE(
				TRIM(BOTH '-' FROM LOWER(
					CONCAT(
						REGEXP_REPLACE(d.name, '[^a-zA-Z0-9]+', '-', 'g'),
						'-',
						LEFT(md5(d.name || s.type || s.workspace_id), 7)
					)
				)),
				'-+', '-', 'g'
			),
			''
		)
		FROM stub s
		WHERE d.stub_id = s.id;
	`
	if _, err := tx.ExecContext(ctx, updateTable); err != nil {
		return err
	}

	createIndex := `CREATE INDEX IF NOT EXISTS deployment_subdomain_idx ON deployment(subdomain);`
	if _, err := tx.ExecContext(ctx, createIndex); err != nil {
		return err
	}

	/*
		Remove the group column from the stub table
	*/
	dropIndex := `DROP INDEX IF EXISTS stub_group;`
	if _, err := tx.ExecContext(ctx, dropIndex); err != nil {
		return err
	}

	dropColumn := `ALTER TABLE stub DROP COLUMN IF EXISTS "group";`
	if _, err := tx.ExecContext(ctx, dropColumn); err != nil {
		return err
	}

	return nil
}

func downSubdomain(ctx context.Context, tx *sql.Tx) error {
	// No need to revert this migration
	return nil
}
