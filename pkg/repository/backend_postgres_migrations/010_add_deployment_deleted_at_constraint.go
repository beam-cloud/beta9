package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddDeploymentDeletedAtUniqueConstraint, dropDeploymentDeletedAtUniqueConstraint)
}

func upAddDeploymentDeletedAtUniqueConstraint(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE deployment DROP CONSTRAINT deployment_name_version_workspace_id_stub_type_key RESTRICT`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`ALTER TABLE deployment ADD CONSTRAINT unique_deploy UNIQUE (name, version, workspace_id, stub_type, deleted_at)`)
	if err != nil {
		return err
	}

	return err
}

func dropDeploymentDeletedAtUniqueConstraint(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE deployment DROP CONSTRAINT unique_deploy`)
	return err
}
