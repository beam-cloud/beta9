package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddScheduleJob, downRemoveScheduleJob)
}

// upAddScheduleJob adds the scheduled_job table and the schedule and schedule/deployment stub types.
// The scheduled_job table relies on the cron.job table, so it should be run after the pg_cron extension is installed
func upAddScheduleJob(ctx context.Context, tx *sql.Tx) error {
	newStubTypes := []string{"schedule", "schedule/deployment"}

	for _, stubType := range newStubTypes {
		addEnumSQL := fmt.Sprintf(`
			DO $$
			BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_type t
				JOIN pg_enum e ON t.oid = e.enumtypid
				JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
				WHERE n.nspname = 'public' AND t.typname = 'stub_type' AND e.enumlabel = '%s'
			) THEN
				EXECUTE 'ALTER TYPE stub_type ADD VALUE ' || quote_literal('%s');
			END IF;
			END
			$$;`,
			stubType,
			stubType,
		)

		if _, err := tx.ExecContext(ctx, addEnumSQL); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scheduled_job (
			id SERIAL PRIMARY KEY,
			external_id uuid DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			job_id int NOT NULL,
			job_name varchar(255) CHECK (LENGTH(job_name) > 0) NOT NULL,
			job_schedule varchar(255) NOT NULL CHECK (
				COALESCE(array_length(string_to_array(job_schedule, ' '), 1), 0) = 5 OR job_schedule LIKE '@%'
			),
			job_payload jsonb,
			deployment_id int REFERENCES deployment(id) NOT NULL,
			stub_id int REFERENCES stub(id) NOT NULL,
			created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
			deleted_at timestamptz
		);
	`); err != nil {
		return err
	}

	return nil
}

func downRemoveScheduleJob(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.Exec(`DROP TABLE IF EXISTS scheduled_job;`); err != nil {
		return err
	}

	return nil
}
