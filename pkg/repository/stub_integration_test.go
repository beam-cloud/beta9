package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
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

func TestVMCreationTransactionIntegration(t *testing.T) {
	db := postgresIntegrationSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE app (id int PRIMARY KEY, updated_at timestamptz);
		CREATE TEMP TABLE object (id int PRIMARY KEY);
		CREATE TEMP TABLE disk (LIKE public.disk INCLUDING DEFAULTS);
		CREATE TEMP TABLE stub (LIKE public.stub INCLUDING DEFAULTS);
		CREATE TEMP TABLE persistent_vm (LIKE public.persistent_vm INCLUDING DEFAULTS);
		CREATE TEMP SEQUENCE vm_disk_id;
		CREATE TEMP SEQUENCE vm_stub_id;
		ALTER TABLE disk ALTER COLUMN id SET DEFAULT nextval('pg_temp.vm_disk_id');
		ALTER TABLE stub ALTER COLUMN id SET DEFAULT nextval('pg_temp.vm_stub_id');
		ALTER TABLE stub ADD FOREIGN KEY (app_id) REFERENCES app(id);
		ALTER TABLE stub ADD FOREIGN KEY (object_id) REFERENCES object(id);
		ALTER TABLE persistent_vm ADD CHECK (name <> 'rejected');
		CREATE UNIQUE INDEX ON disk(workspace_id,name) WHERE deleted_at IS NULL;
		CREATE UNIQUE INDEX ON persistent_vm(workspace_id,name) WHERE data->>'desired_state'<>'deleted';
		CREATE UNIQUE INDEX ON persistent_vm(public_ipv6) WHERE public_ipv6 IS NOT NULL;
		INSERT INTO app VALUES (1,'2000-01-01');
		INSERT INTO object VALUES (1);`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db}
	launch := func(name string) (*types.VM, types.Stub) {
		id, stubID := "vm-"+uuid.NewString(), uuid.NewString()
		config, err := json.Marshal(types.StubConfigV1{Disks: []*pb.DurableDisk{{Name: id, Size: "50Gi", Driver: "qcow", Filesystem: "ext4", MountPath: "/"}}})
		require.NoError(t, err)
		v := &types.VM{ID: id, WorkspaceID: 1, Name: name, Handle: uuid.NewString(),
			StubID: stubID, ContainerID: "vm-" + stubID + "-first", Generation: 1, LaunchAttempts: 1,
			DesiredState: "running", Status: "starting", LastActiveAt: time.Now().UTC()}
		stub := types.Stub{ExternalId: stubID, Name: "vm/" + id, Type: types.StubType(types.StubTypeVM),
			WorkspaceId: 1, ObjectId: 1, AppId: 1, Config: string(config)}
		return v, stub
	}
	first, stub := launch("created")
	first.PublicIPv6 = "2001:db8::1"
	require.NoError(t, repo.CreateVMWithStub(ctx, first, stub))
	stored, err := repo.GetVM(ctx, 1, first.ID)
	require.NoError(t, err)
	require.Equal(t, first.StubID, stored.StubID)
	require.Equal(t, first.ContainerID, stored.ContainerID)
	require.Equal(t, first.PublicIPv6, stored.PublicIPv6)
	var disk types.Disk
	require.NoError(t, db.GetContext(ctx, &disk, `SELECT `+diskColumns+` FROM disk WHERE name=$1`, first.ID))
	require.Equal(t, "50Gi", disk.Size)
	require.Equal(t, "qcow", disk.Driver)
	for _, failure := range []string{"VM constraint", "name collision", "address collision", "stub foreign key", "root collision"} {
		t.Run(failure, func(t *testing.T) {
			v, stub := launch(uuid.NewString())
			switch failure {
			case "VM constraint":
				v.Name = "rejected"
			case "name collision":
				v.Name = first.Name
			case "address collision":
				v.PublicIPv6 = first.PublicIPv6
			case "stub foreign key":
				stub.AppId = 999
			case "root collision":
				_, err = db.ExecContext(ctx, `INSERT INTO disk(workspace_id,name) VALUES (1,$1)`, v.ID)
				require.NoError(t, err)
			}
			_, err = db.ExecContext(ctx, `UPDATE app SET updated_at='2000-01-01'`)
			require.NoError(t, err)
			counts := func() []int {
				var counts []int
				require.NoError(t, db.SelectContext(ctx, &counts, `SELECT count(*) FROM disk UNION ALL SELECT count(*) FROM stub UNION ALL SELECT count(*) FROM persistent_vm`))
				return counts
			}
			before := counts()
			require.Error(t, repo.CreateVMWithStub(ctx, v, stub))
			require.Equal(t, before, counts(), "every creation write must roll back")
			var touched time.Time
			require.NoError(t, db.GetContext(ctx, &touched, `SELECT updated_at FROM app WHERE id=1`))
			require.Equal(t, 2000, touched.Year(), "app activity must roll back too")
		})
	}
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
