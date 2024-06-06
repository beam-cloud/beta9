package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upRenameConcurrencyLimitCpuCoreLimitField, downRenameConcurrencyLimitCpuCoreLimitField)
}

func upRenameConcurrencyLimitCpuCoreLimitField(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE concurrency_limit RENAME COLUMN cpu_core_limit TO cpu_millicore_limit;`)
	return err
}

func downRenameConcurrencyLimitCpuCoreLimitField(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE concurrency_limit RENAME COLUMN cpu_millicore_limit TO cpu_core_limit;`)
	return err
}
