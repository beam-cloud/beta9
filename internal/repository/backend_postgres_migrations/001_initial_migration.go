package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upCreateTables, downDropTables)
}

func upCreateTables(tx *sql.Tx) error {
	// Ensure UUID extension is available
	if _, err := tx.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`); err != nil {
		return err
	}

	createStatements := []string{
		`CREATE TYPE token_type AS ENUM ('admin', 'workspace', 'worker');`,
		`CREATE TYPE stub_type AS ENUM ('taskqueue', 'function', 'taskqueue/deployment', 'function/deployment', 'endpoint', 'endpoint/deployment', 'container');`,
		`CREATE TYPE task_status AS ENUM ('PENDING', 'RUNNING', 'CANCELLED', 'COMPLETE', 'ERROR', 'TIMEOUT', 'RETRY');`,

		`CREATE TABLE IF NOT EXISTS workspace (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );`,

		`CREATE TABLE IF NOT EXISTS token (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            key VARCHAR(255) NOT NULL,
            active BOOLEAN NOT NULL,
            reusable BOOLEAN DEFAULT true NOT NULL,
            workspace_id INT REFERENCES workspace(id),
            token_type token_type NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );`,

		`CREATE TABLE IF NOT EXISTS volume (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            workspace_id INT REFERENCES workspace(id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );`,

		`CREATE TABLE IF NOT EXISTS object (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() NOT NULL,
            hash VARCHAR(255) NOT NULL,
            size BIGINT NOT NULL,
            workspace_id INT REFERENCES workspace(id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (hash, workspace_id)
        );`,

		`CREATE TABLE IF NOT EXISTS stub (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            type stub_type NOT NULL,
            config JSON NOT NULL,
            config_version INT DEFAULT 1,
            object_id INT REFERENCES object(id),
            workspace_id INT REFERENCES workspace(id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );`,

		`CREATE TABLE IF NOT EXISTS deployment (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            active BOOLEAN NOT NULL DEFAULT true,
            workspace_id INT REFERENCES workspace(id),
            stub_type stub_type NOT NULL,
            stub_id INT REFERENCES stub(id),
            version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (name, version, workspace_id, stub_type)
        );`,

		`CREATE TABLE IF NOT EXISTS task (
            id SERIAL PRIMARY KEY,
            external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
            container_id VARCHAR(255) NOT NULL,
            status task_status NOT NULL DEFAULT 'PENDING',
            started_at TIMESTAMP WITH TIME ZONE,
            ended_at TIMESTAMP WITH TIME ZONE,
            workspace_id INT REFERENCES workspace(id),
            stub_id INT REFERENCES stub(id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );`,
	}

	for _, stmt := range createStatements {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downDropTables(tx *sql.Tx) error {
	dropStatements := []string{
		"DROP TABLE IF EXISTS stub;",
		"DROP TABLE IF EXISTS task;",
		"DROP TABLE IF EXISTS deployment;",
		"DROP TABLE IF EXISTS object;",
		"DROP TABLE IF EXISTS workspace;",
		"DROP TABLE IF EXISTS volume;",
		"DROP TABLE IF EXISTS token;",
		"DROP TYPE IF EXISTS stub_type;",
		"DROP TYPE IF EXISTS task_status;",
	}

	for i := len(dropStatements) - 1; i >= 0; i-- {
		if _, err := tx.Exec(dropStatements[i]); err != nil {
			return err
		}
	}

	return nil
}
