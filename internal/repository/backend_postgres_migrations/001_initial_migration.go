package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upCreateTables, downDropTables)
}

func upCreateTables(tx *sql.Tx) error {
	createStatements := []string{
		`CREATE TABLE IF NOT EXISTS identity (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			active BOOLEAN NOT NULL
		);`,

		`CREATE TABLE IF NOT EXISTS identity_token (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			key VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			active BOOLEAN NOT NULL,
			identity_id INT REFERENCES identity(id)
		);`,

		`CREATE TABLE IF NOT EXISTS task (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			started_at TIMESTAMP WITH TIME ZONE,
			ended_at TIMESTAMP WITH TIME ZONE,
			active BOOLEAN NOT NULL
		);`,

		`CREATE TABLE IF NOT EXISTS deployment (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			version INT NOT NULL,
			status VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,

		`CREATE TABLE IF NOT EXISTS object (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			hash VARCHAR(255) NOT NULL,
			size BIGINT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,

		`CREATE TABLE IF NOT EXISTS volume (
			id SERIAL PRIMARY KEY,
			external_id VARCHAR(255) UNIQUE NOT NULL,
			name VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
		"DROP TABLE IF EXISTS volume;",
		"DROP TABLE IF EXISTS object;",
		"DROP TABLE IF EXISTS deployment;",
		"DROP TABLE IF EXISTS task;",
		"DROP TABLE IF EXISTS identity_token;",
		"DROP TABLE IF EXISTS identity;",
	}

	// Run drop statements in reverse order of creation to handle dependencies.
	for i := len(dropStatements) - 1; i >= 0; i-- {
		if _, err := tx.Exec(dropStatements[i]); err != nil {
			return err
		}
	}

	return nil
}
