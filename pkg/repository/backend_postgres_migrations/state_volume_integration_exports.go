//go:build statevolume_integration

package backend_postgres_migrations

import (
	"context"
	"database/sql"
)

// ApplyStateVolumeCutoverForIntegration exposes the exact immutable state
// migrations to the privileged repository integration suite. It is compiled
// only under the release-gate build tag and is not a production migration API.
func ApplyStateVolumeCutoverForIntegration(ctx context.Context, db *sql.DB) error {
	for _, migration := range []func(context.Context, *sql.Tx) error{
		upStateSnapshots,
		upStateVolumeAttachments,
		upStateCacheRetirement,
	} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := migration(ctx, tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
