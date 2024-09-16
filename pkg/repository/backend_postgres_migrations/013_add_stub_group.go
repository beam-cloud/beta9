package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddStubGroup, downRemoveStubGroup)
}

func upAddStubGroup(ctx context.Context, tx *sql.Tx) error {
	addColumn := `ALTER TABLE stub ADD COLUMN "group" varchar(255);`
	if _, err := tx.ExecContext(ctx, addColumn); err != nil {
		return err
	}

	updateTable := `
		UPDATE stub
		SET "group" = CASE
			WHEN strpos(name, ':') = 0 THEN ''
			ELSE CONCAT(
				REPLACE(split_part(name, ':', 2), '_', '-'),
				'-',
				RIGHT(md5(name || type || workspace_id), 7)
			)
		END;
	`
	if _, err := tx.ExecContext(ctx, updateTable); err != nil {
		return err
	}

	addIndex := `CREATE INDEX stub_group ON stub("group");`
	if _, err := tx.ExecContext(ctx, addIndex); err != nil {
		return err
	}

	return nil
}

func downRemoveStubGroup(ctx context.Context, tx *sql.Tx) error {
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
