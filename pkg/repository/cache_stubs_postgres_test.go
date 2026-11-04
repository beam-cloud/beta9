package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestRetiredVMCacheStubKeysIgnoreMalformedKeys(t *testing.T) {
	r := &PostgresBackendRepository{} // No database access for unparseable keys.
	keys, err := r.ListRetiredVMCacheStubKeys(context.Background(), []string{"", "workspace|stub", uuid.NewString() + "|bad", "extra|parts|here"})
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestRetiredVMCacheStubKeysIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE workspace (id integer,external_id uuid);
CREATE TEMP TABLE stub (workspace_id integer,external_id uuid,type text);
CREATE TEMP TABLE persistent_vm (workspace_id integer,data jsonb);`)
	require.NoError(t, err)
	ws, other := uuid.NewString(), uuid.NewString()
	_, err = db.Exec(`INSERT INTO workspace VALUES (1,$1),(2,$2)`, ws, other)
	require.NoError(t, err)
	var keys, want []string
	for _, scenario := range []string{"deleted", "live", "shared", "orphan", "legacy", "non-vm", "other-workspace"} {
		id, workspace, wid, kind := uuid.NewString(), ws, 1, "vm"
		if scenario == "non-vm" {
			kind = "sandbox"
		}
		if scenario == "other-workspace" {
			workspace, wid = other, 2
			keys = append(keys, ws+"|"+id)
		}
		key := workspace + "|" + id
		keys = append(keys, key, key) // Duplicate index entries must not change classification.
		_, err = db.Exec(`INSERT INTO stub VALUES ($1,$2,$3)`, wid, id, kind)
		require.NoError(t, err)
		if scenario == "orphan" {
			continue
		}
		states := []string{"deleted"}
		switch scenario {
		case "live":
			states = []string{"running"}
		case "shared":
			states = []string{"deleted", "paused"}
		case "legacy":
			states = []string{""}
		}
		for _, state := range states {
			data := map[string]string{"stub_id": id}
			if state != "" {
				data["desired_state"] = state
			}
			body, err := json.Marshal(data)
			require.NoError(t, err)
			_, err = db.Exec(`INSERT INTO persistent_vm VALUES ($1,$2::jsonb)`, wid, string(body))
			require.NoError(t, err)
		}
		if scenario == "deleted" || scenario == "other-workspace" {
			want = append(want, key)
		}
	}
	keys = append(keys, "bad|uuid", uuid.NewString()+"|"+uuid.NewString())
	r := &PostgresBackendRepository{client: db}
	got, err := r.ListRetiredVMCacheStubKeys(context.Background(), keys)
	require.NoError(t, err)
	require.ElementsMatch(t, want, got, "retain live/shared/legacy/orphan/non-VM and cross-workspace requirements")
}
