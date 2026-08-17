//go:build statevolume_integration

package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

const stateMigrationAdminDSNEnv = "BETA9_STATE_MIGRATION_TEST_ADMIN_DSN"

func stateMigrationDatabase(t *testing.T) *sql.DB {
	t.Helper()
	adminDSN := strings.TrimSpace(os.Getenv(stateMigrationAdminDSNEnv))
	if adminDSN == "" {
		t.Fatalf("%s is required for the privileged migration suite", stateMigrationAdminDSNEnv)
	}
	parsed, err := url.Parse(adminDSN)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("%s must be a PostgreSQL URL: %v", stateMigrationAdminDSNEnv, err)
	}

	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect migration admin database: %v", err)
	}

	databaseName := "beta9_state_cutover_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedName := pq.QuoteIdentifier(databaseName)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quotedName); err != nil {
		t.Fatalf("create isolated migration database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quotedName+" WITH (FORCE)")
	})

	parsed.Path = "/" + databaseName
	testDB, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	if err := testDB.PingContext(ctx); err != nil {
		t.Fatalf("connect isolated migration database: %v", err)
	}
	return testDB
}

func runStateMigration(t *testing.T, db *sql.DB, migration func(context.Context, *sql.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := migration(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func createStateMigrationBaseSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE TABLE workspace (id SERIAL PRIMARY KEY, external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE stub (id SERIAL PRIMARY KEY, external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL, workspace_id INT REFERENCES workspace(id))`,
		`CREATE TABLE token (id SERIAL PRIMARY KEY, workspace_id INT REFERENCES workspace(id), key TEXT NOT NULL)`,
		`CREATE TABLE task (id SERIAL PRIMARY KEY, workspace_id INT REFERENCES workspace(id), amount_micros BIGINT NOT NULL DEFAULT 0)`,
		`CREATE TABLE compute_ledger (id SERIAL PRIMARY KEY, workspace_id INT REFERENCES workspace(id), amount_micros BIGINT NOT NULL)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func applyStateCutover(t *testing.T, db *sql.DB) {
	t.Helper()
	runStateMigration(t, db, upStateSnapshots)
	runStateMigration(t, db, upStateVolumeAttachments)
	runStateMigration(t, db, upStateCacheRetirement)
}

func relationExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func tableCount(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var count int64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", pq.QuoteIdentifier(name))).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestStateVolumeCutoverFreshInstallNeverCreatesSupersededStateTables(t *testing.T) {
	db := stateMigrationDatabase(t)
	createStateMigrationBaseSchema(t, db)

	for _, migration := range []func(context.Context, *sql.Tx) error{
		upStorageCutover033,
		upStorageCutover034,
		upStorageCutover037,
		upStorageCutover041,
		upStorageCutover042,
		upStorageCutover043,
		upStorageCutover046,
		upStorageCutover047,
	} {
		runStateMigration(t, db, migration)
	}
	applyStateCutover(t, db)

	for _, name := range []string{"checkpoint", "disk_snapshot", "disk"} {
		if relationExists(t, db, name) {
			t.Fatalf("fresh install created superseded table %q", name)
		}
	}
	for _, name := range []string{"state_volume", "volume_generation", "state_snapshot", "state_snapshot_generation", "state_volume_attachment_plan", "state_snapshot_member_plan", "state_volume_release_claim", "state_volume_release_claim_member", "state_snapshot_reference", "state_cache_retirement_outbox"} {
		if !relationExists(t, db, name) {
			t.Fatalf("fresh install is missing state-volume table %q", name)
		}
	}
}

func TestStateVolumeCutoverUpgrade47To50IsDestructiveAndPreservesAccountUsage(t *testing.T) {
	db := stateMigrationDatabase(t)
	createStateMigrationBaseSchema(t, db)
	_, err := db.Exec(`
		INSERT INTO workspace(id, name) VALUES (1, 'preserved');
		INSERT INTO stub(id, workspace_id) VALUES (1, 1);
		INSERT INTO token(workspace_id, key) VALUES (1, 'preserved-token');
		INSERT INTO task(workspace_id, amount_micros) VALUES (1, 17);
		INSERT INTO compute_ledger(workspace_id, amount_micros) VALUES (1, 23);
		CREATE TABLE checkpoint (id SERIAL PRIMARY KEY, workspace_id INT REFERENCES workspace(id), stub_id INT REFERENCES stub(id));
		CREATE TABLE disk (id SERIAL PRIMARY KEY, workspace_id INT REFERENCES workspace(id));
		CREATE TABLE disk_snapshot (id SERIAL PRIMARY KEY, disk_id INT REFERENCES disk(id), workspace_id INT REFERENCES workspace(id));
		INSERT INTO checkpoint(workspace_id, stub_id) VALUES (1, 1);
		INSERT INTO disk(workspace_id) VALUES (1);
		INSERT INTO disk_snapshot(disk_id, workspace_id) VALUES (1, 1);
	`)
	if err != nil {
		t.Fatal(err)
	}

	applyStateCutover(t, db)

	for _, name := range []string{"checkpoint", "disk_snapshot", "disk"} {
		if relationExists(t, db, name) {
			t.Fatalf("upgrade retained superseded table %q", name)
		}
	}
	for _, name := range []string{"workspace", "token", "task", "compute_ledger"} {
		if got := tableCount(t, db, name); got != 1 {
			t.Fatalf("upgrade changed %s count: got %d want 1", name, got)
		}
	}
}

func TestStateVolumeCutoverRollbackRemovesOnlyNewSchema(t *testing.T) {
	db := stateMigrationDatabase(t)
	createStateMigrationBaseSchema(t, db)
	if _, err := db.Exec(`
		INSERT INTO workspace(id, name) VALUES (1, 'preserved');
		INSERT INTO token(workspace_id, key) VALUES (1, 'preserved-token');
		INSERT INTO task(workspace_id, amount_micros) VALUES (1, 17);
		INSERT INTO compute_ledger(workspace_id, amount_micros) VALUES (1, 23);
	`); err != nil {
		t.Fatal(err)
	}
	applyStateCutover(t, db)

	runStateMigration(t, db, downStateCacheRetirement)
	runStateMigration(t, db, downStateVolumeAttachments)
	runStateMigration(t, db, downStateSnapshots)

	for _, name := range []string{"state_volume", "volume_generation", "state_snapshot", "state_snapshot_generation", "state_volume_attachment_plan", "state_snapshot_member_plan", "state_volume_release_claim", "state_volume_release_claim_member", "state_snapshot_reference", "state_cache_retirement_outbox"} {
		if relationExists(t, db, name) {
			t.Fatalf("rollback retained state-volume table %q", name)
		}
	}
	for _, name := range []string{"workspace", "token", "task", "compute_ledger"} {
		if got := tableCount(t, db, name); got != 1 {
			t.Fatalf("rollback changed %s count: got %d want 1", name, got)
		}
	}
	for _, name := range []string{"checkpoint", "disk_snapshot", "disk"} {
		if relationExists(t, db, name) {
			t.Fatalf("rollback recreated superseded table %q", name)
		}
	}
}
