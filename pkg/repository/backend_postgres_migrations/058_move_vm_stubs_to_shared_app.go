package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upMoveVMStubsToSharedApp, downMoveVMStubsToSharedApp)
}

// types.VMAppName when this migration was written.
const sharedVMAppName = "__beam_vms__"

type migrationStatement struct {
	query string
	args  []any
}

var moveVMStubsToSharedApp = []migrationStatement{
	{
		query: `INSERT INTO app (name, workspace_id, description)
SELECT DISTINCT CAST($1 AS VARCHAR(255)), s.workspace_id, ''
FROM stub s
WHERE s.type = 'vm'
  AND NOT EXISTS (SELECT 1 FROM app a WHERE a.workspace_id = s.workspace_id AND a.name = $1 AND a.deleted_at IS NULL);`,
		args: []any{sharedVMAppName},
	},
	{
		query: `UPDATE stub s SET app_id = a.id
FROM app a
WHERE s.type = 'vm'
  AND a.workspace_id = s.workspace_id
  AND a.name = $1
  AND a.deleted_at IS NULL
  AND s.app_id IS DISTINCT FROM a.id;`,
		args: []any{sharedVMAppName},
	},
	{
		query: `UPDATE checkpoint c SET app_id = s.app_id
FROM stub s
WHERE c.stub_id = s.id AND s.type = 'vm' AND c.app_id IS DISTINCT FROM s.app_id;`,
	},
	{
		query: `UPDATE task t SET app_id = s.app_id
FROM stub s
WHERE t.stub_id = s.id AND s.type = 'vm' AND t.app_id IS DISTINCT FROM s.app_id;`,
	},
	{
		query: `UPDATE app a SET deleted_at = NOW()
WHERE a.name LIKE 'vm/vm-%'
  AND a.deleted_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM stub s WHERE s.app_id = a.id)
  AND NOT EXISTS (SELECT 1 FROM deployment d WHERE d.app_id = a.id AND d.deleted_at IS NULL);`,
	},
}

func upMoveVMStubsToSharedApp(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range moveVMStubsToSharedApp {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return err
		}
	}
	return nil
}

func downMoveVMStubsToSharedApp(context.Context, *sql.Tx) error {
	// The per-VM apps held nothing but a name, and new VMs keep using the
	// shared app, so there is nothing to restore.
	return nil
}
