package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upVMLifecycleIndexes, downVMLifecycleIndexes) }

func upVMLifecycleIndexes(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE INDEX persistent_vm_lookup_name ON persistent_vm (workspace_id,name);
CREATE INDEX persistent_vm_pending_reconcile ON persistent_vm (id) WHERE
 data->>'desired_state' IS DISTINCT FROM data->>'status'
 OR data->>'desired_state'='running'
 OR (public_ipv6 IS NULL AND data->>'desired_state' IN ('stopped','paused'));`)
	return err
}

func downVMLifecycleIndexes(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP INDEX persistent_vm_lookup_name; DROP INDEX persistent_vm_pending_reconcile;`)
	return err
}
