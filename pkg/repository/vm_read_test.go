package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVMReadBatchKeepsWorkspaceAndLookupIdentity(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	keys := []vmReadKey{{WorkspaceID: 7, Name: "desktop"}, {WorkspaceID: 8, Name: "desktop"}, {WorkspaceID: 7, Name: "vm-a"}, {WorkspaceID: 7, Name: "missing"}}
	data, err := json.Marshal(keys)
	require.NoError(t, err)
	rows := sqlmock.NewRows([]string{"workspace_id", "workspace_external_id", "token_id", "data", "last_active_at", "public_ipv6", "lookup_name"})
	for _, key := range keys[:3] {
		vm := types.VM{ID: "vm-a", Name: "desktop", Metadata: map[string]string{"key": "original"}}
		value, err := json.Marshal(vm)
		require.NoError(t, err)
		rows.AddRow(key.WorkspaceID, "workspace", "token", value, time.Now(), nil, key.Name)
	}
	mock.ExpectQuery(`SELECT vm\.\*, request.name AS lookup_name`).WithArgs(data).WillReturnRows(rows)
	values, err := r.readVMs(context.Background(), keys)
	require.NoError(t, err)
	require.Len(t, values, 3)
	for _, key := range keys[:3] {
		row, exists := values[key]
		require.True(t, exists)
		first, err := row.vm()
		require.NoError(t, err)
		first.Metadata["key"] = "changed"
		second, err := row.vm()
		require.NoError(t, err)
		require.Equal(t, "original", second.Metadata["key"])
		require.Equal(t, key.WorkspaceID, second.WorkspaceID)
	}
	_, exists := values[keys[3]]
	require.False(t, exists)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetVMBatchDoesNotRetainMissingResults(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	for range 2 {
		mock.ExpectQuery(`SELECT vm\.\*, request.name AS lookup_name`).
			WithArgs([]byte(`[{"workspace_id":7,"name":"missing"}]`)).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "data", "lookup_name"}))
		_, err := r.GetVM(context.Background(), 7, "missing")
		require.ErrorIs(t, err, sql.ErrNoRows)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMReadBatchIntegration(t *testing.T) {
	db := postgresIntegrationSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE persistent_vm (LIKE public.persistent_vm INCLUDING DEFAULTS INCLUDING INDEXES);
		INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data)
		SELECT 'vm-'||n,1,'workspace','token','desktop-'||n,'handle-'||n,
		       jsonb_build_object('id','vm-'||n,'desired_state','running','metadata',jsonb_build_object('key','original'))
		FROM generate_series(1,100) n;
		INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data)
		VALUES ('old',1,'workspace','token','desktop-1','old','{"id":"old","desired_state":"deleted"}'),
		       ('foreign',2,'other','other','desktop-1','foreign','{"id":"foreign","desired_state":"running"}');`)
	require.NoError(t, err)
	r := &PostgresBackendRepository{client: db}
	var keys []vmReadKey
	for i := range 100 {
		keys = append(keys, vmReadKey{WorkspaceID: 1, Name: fmt.Sprintf("desktop-%d", i+1)})
	}
	started := time.Now()
	rows, err := r.readVMs(ctx, keys)
	require.NoError(t, err)
	require.Len(t, rows, 100)
	t.Logf("100 indexed VM lookups: %s", time.Since(started))
	first, err := r.GetVM(ctx, 1, "desktop-1")
	require.NoError(t, err)
	require.Equal(t, "vm-1", first.ID, "name reuse must prefer the active VM")
	first.Metadata["key"] = "changed by caller"
	again, err := r.GetVM(ctx, 1, "vm-1")
	require.NoError(t, err)
	require.Equal(t, "original", again.Metadata["key"])
	foreign, err := r.GetVM(ctx, 2, "desktop-1")
	require.NoError(t, err)
	require.Equal(t, "foreign", foreign.ID)
	_, err = r.GetVM(ctx, 2, "vm-1")
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = db.ExecContext(ctx, `UPDATE persistent_vm SET data=jsonb_set(data,'{status}','"paused"') WHERE id='vm-1'`)
	require.NoError(t, err)
	again, err = r.GetVM(ctx, 1, "vm-1")
	require.NoError(t, err)
	require.Equal(t, "paused", again.Status, "a completed read must not cache lifecycle state")
	_, err = r.GetVM(ctx, 1, "new")
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = db.ExecContext(ctx, `UPDATE persistent_vm SET name='new' WHERE id='vm-1'`)
	require.NoError(t, err)
	again, err = r.GetVM(ctx, 1, "new")
	require.NoError(t, err)
	require.Equal(t, "vm-1", again.ID, "a missing read must not cache absence")
}
