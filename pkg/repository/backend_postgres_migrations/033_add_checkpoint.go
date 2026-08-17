package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upStorageCutover033, downStorageCutover033)
}

// This historical version is intentionally empty. The state-volume cutover
// owns all writable machine state and migration 048 removes superseded storage
// tables on upgrade. Fresh installs must never create those intermediate schemas.
func upStorageCutover033(context.Context, *sql.Tx) error { return nil }

func downStorageCutover033(context.Context, *sql.Tx) error { return nil }
