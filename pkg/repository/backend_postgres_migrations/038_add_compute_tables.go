package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddComputeTables, downAddComputeTables)
}

func upAddComputeTables(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS compute_pool (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT REFERENCES workspace(id),
			name VARCHAR(255) NOT NULL,
			selector VARCHAR(255) NOT NULL,
			config JSONB NOT NULL,
			status VARCHAR(64) NOT NULL DEFAULT 'active',
			source VARCHAR(64) NOT NULL DEFAULT 'autosolver',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP WITH TIME ZONE,
			UNIQUE (workspace_id, name)
		);`,
		`CREATE TABLE IF NOT EXISTS compute_capacity_request (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT REFERENCES workspace(id),
			pool_id INT REFERENCES compute_pool(id) ON DELETE CASCADE,
			stub_id INT REFERENCES stub(id),
			source VARCHAR(64) NOT NULL,
			max_spend_micros BIGINT NOT NULL DEFAULT 0,
			ttl_seconds BIGINT NOT NULL DEFAULT 0,
			status VARCHAR(64) NOT NULL DEFAULT 'active',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP WITH TIME ZONE
		);`,
		`CREATE TABLE IF NOT EXISTS compute_provider_instance (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			pool_id INT REFERENCES compute_pool(id) ON DELETE CASCADE,
			capacity_request_id INT REFERENCES compute_capacity_request(id) ON DELETE SET NULL,
			provider VARCHAR(64) NOT NULL,
			offer_id VARCHAR(255) NOT NULL,
			instance_type VARCHAR(255),
			instance_id VARCHAR(255),
			machine_id VARCHAR(255),
			gpu VARCHAR(128),
			gpu_count INT NOT NULL DEFAULT 0,
			cpu_millicores BIGINT NOT NULL DEFAULT 0,
			memory_mb BIGINT NOT NULL DEFAULT 0,
			hourly_cost_micros BIGINT NOT NULL DEFAULT 0,
			committed_micros BIGINT NOT NULL DEFAULT 0,
			source VARCHAR(64) NOT NULL,
			status VARCHAR(64) NOT NULL,
			raw JSONB,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP WITH TIME ZONE,
			billing_renewal_at TIMESTAMP WITH TIME ZONE
		);`,
		`CREATE TABLE IF NOT EXISTS compute_solver_run (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT REFERENCES workspace(id),
			pool_id INT REFERENCES compute_pool(id) ON DELETE CASCADE,
			input JSONB NOT NULL,
			output JSONB NOT NULL,
			feasible BOOLEAN NOT NULL,
			reason TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS compute_solver_decision (
			id SERIAL PRIMARY KEY,
			solver_run_id INT REFERENCES compute_solver_run(id) ON DELETE CASCADE,
			action VARCHAR(64) NOT NULL,
			provider VARCHAR(64),
			offer_id VARCHAR(255),
			reservation_id INT REFERENCES compute_provider_instance(id) ON DELETE SET NULL,
			count INT NOT NULL DEFAULT 0,
			cost_micros BIGINT NOT NULL DEFAULT 0,
			reason TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS compute_ledger (
			id SERIAL PRIMARY KEY,
			workspace_id INT REFERENCES workspace(id),
			pool_id INT REFERENCES compute_pool(id) ON DELETE SET NULL,
			reservation_id INT REFERENCES compute_provider_instance(id) ON DELETE SET NULL,
			source VARCHAR(64) NOT NULL,
			amount_micros BIGINT NOT NULL,
			started_at TIMESTAMP WITH TIME ZONE NOT NULL,
			ended_at TIMESTAMP WITH TIME ZONE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_compute_pool_workspace_name ON compute_pool(workspace_id, name);`,
		`CREATE INDEX IF NOT EXISTS idx_compute_provider_instance_pool ON compute_provider_instance(pool_id);`,
		`CREATE INDEX IF NOT EXISTS idx_compute_provider_instance_renewal ON compute_provider_instance(billing_renewal_at);`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func downAddComputeTables(tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE IF EXISTS compute_ledger;`,
		`DROP TABLE IF EXISTS compute_solver_decision;`,
		`DROP TABLE IF EXISTS compute_solver_run;`,
		`DROP TABLE IF EXISTS compute_provider_instance;`,
		`DROP TABLE IF EXISTS compute_capacity_request;`,
		`DROP TABLE IF EXISTS compute_pool;`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}
