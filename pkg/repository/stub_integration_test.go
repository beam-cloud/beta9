package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func postgresIntegrationSession(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// Shadow real tables on one session. No existing rows or sequences change.
	db.SetMaxOpenConns(1)
	return db
}

func TestStubIdentityIntegration(t *testing.T) {
	db := postgresIntegrationSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE app (LIKE public.app INCLUDING DEFAULTS);
		CREATE TEMP TABLE stub (LIKE public.stub INCLUDING DEFAULTS);
		CREATE TEMP SEQUENCE stub_identity_id;
		ALTER TABLE stub ALTER COLUMN id SET DEFAULT nextval('pg_temp.stub_identity_id');
		ALTER TABLE stub ADD CHECK (name <> 'rejected');
		INSERT INTO app (id,name,description,workspace_id,updated_at)
		VALUES (1,'one','',1,'2000-01-01'), (2,'two','',1,'2000-01-01');`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db}
	create := func(name string, app uint, force bool) (types.Stub, error) {
		return repo.GetOrCreateStub(ctx, name, types.StubTypeVM, types.StubConfigV1{}, 7, 1, force, app)
	}
	first, err := create("vm", 1, false)
	require.NoError(t, err)
	again, err := create("vm", 1, false)
	require.NoError(t, err)
	require.Equal(t, first.Id, again.Id)
	require.True(t, again.UpdatedAt.Time.After(first.UpdatedAt.Time))
	forced, err := create("vm", 1, true)
	require.NoError(t, err)
	require.NotEqual(t, first.Id, forced.Id)
	otherApp, err := create("vm", 2, false)
	require.NoError(t, err)
	require.NotEqual(t, first.Id, otherApp.Id)
	var touched time.Time
	require.NoError(t, db.GetContext(ctx, &touched, `SELECT updated_at FROM app WHERE id=1`))
	require.WithinDuration(t, time.Now(), touched, time.Second)
	// A failed stub write must roll back its app activity in the same statement.
	_, err = db.ExecContext(ctx, `UPDATE app SET updated_at='2000-01-01' WHERE id=1`)
	require.NoError(t, err)
	_, err = create("rejected", 1, false)
	require.Error(t, err)
	require.NoError(t, db.GetContext(ctx, &touched, `SELECT updated_at FROM app WHERE id=1`))
	require.Equal(t, 2000, touched.Year())
}

func TestCheckpointPruneSetIntegration(t *testing.T) {
	db := postgresIntegrationSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE workspace (id int, external_id uuid);
		CREATE TEMP TABLE stub (id int, external_id uuid, workspace_id int, updated_at timestamptz);
		CREATE TEMP TABLE persistent_vm (data jsonb);
		CREATE TEMP TABLE checkpoint (LIKE public.checkpoint INCLUDING DEFAULTS);
		INSERT INTO workspace VALUES (1,'00000000-0000-0000-0000-000000000001');
		INSERT INTO stub
		SELECT n,('00000000-0000-0000-0000-' || lpad(n::text,12,'0'))::uuid,1,'2000-01-01'
		FROM generate_series(1,3) n;
		INSERT INTO checkpoint (id,checkpoint_id,source_container_id,container_ip,status,remote_key,workspace_id,stub_id,stub_type,app_id)
		SELECT n,'checkpoint-'||n,'','','available','',1,n,'vm',1 FROM generate_series(1,3) n;
		INSERT INTO persistent_vm VALUES ('{"memory_checkpoint_id":"checkpoint-3","desired_state":"paused"}');`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db}
	active := "00000000-0000-0000-0000-000000000001|00000000-0000-0000-0000-000000000002"
	checkpoints, err := repo.ListStaleCheckpoints(ctx, []string{active, active}, time.Now())
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, "checkpoint-1", checkpoints[0].CheckpointId)
	// No active stubs still preserves the checkpoint paired with paused RAM.
	checkpoints, err = repo.ListStaleCheckpoints(ctx, nil, time.Now())
	require.NoError(t, err)
	require.Len(t, checkpoints, 2)
	// The final update must also recheck RAM ownership, even if its caller
	// supplies a checkpoint that the earlier selection excluded.
	pruned, err := repo.PruneCheckpoints(ctx, []string{"checkpoint-1", "checkpoint-2", "checkpoint-3"})
	require.NoError(t, err)
	require.Len(t, pruned, 2)
}
