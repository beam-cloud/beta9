package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upCreateConcurrencyLimitTable, downDropConcurrencyLimitTable)
}

func upCreateConcurrencyLimitTable(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS concurrency_limit (
		id SERIAL PRIMARY KEY,
		external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
		cpu_core_limit INT NOT NULL,
		gpu_limit INT NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`ALTER TABLE workspace ADD COLUMN concurrency_limit_id INT REFERENCES concurrency_limit(id);`)
	if err != nil {
		return err
	}

	return err
}

func downDropConcurrencyLimitTable(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE workspace DROP COLUMN IF EXISTS concurrency_limit_id;`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE IF EXISTS concurrency_limit;`)
	return err
}
