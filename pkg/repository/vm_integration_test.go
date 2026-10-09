package repository

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// Run against a migrated development database only. Rows use random names and
// are removed on completion; no existing workspace data is changed.
func TestVMRepositoryIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	ws, err := strconv.ParseUint(os.Getenv("BETA9_VM_TEST_WORKSPACE_ID"), 10, 32)
	require.NoError(t, err)
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := &PostgresBackendRepository{client: db}
	ctx := context.Background()
	id := uuid.NewString()
	now := time.Now().UTC().Add(-time.Hour)
	v := &types.VM{ID: id, WorkspaceID: uint(ws), WorkspaceExternalID: "test", TokenID: "test", Name: "vm-test-" + id[:8], Handle: "test-" + id, DesiredState: "running", Status: "running", CreatedAt: now, LastActiveAt: now}
	require.NoError(t, repo.CreateVM(ctx, v))
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM vm_artifact WHERE vm_id=$1`, id)
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, id)
	})

	unlock, err := repo.LockVM(ctx, id)
	require.NoError(t, err)
	_, err = repo.LockVM(ctx, id)
	require.ErrorContains(t, err, "operation already in progress")
	unlock()
	unlock, err = repo.LockVM(ctx, id)
	require.NoError(t, err)
	unlock()

	// Activity and lifecycle writes use different columns. A stale lifecycle
	// object must not overwrite a concurrent touch or trigger an idle stop.
	require.NoError(t, repo.TouchVM(ctx, id))
	v.Status = "starting"
	require.NoError(t, repo.SaveVM(ctx, v))
	loaded, err := repo.GetVM(ctx, uint(ws), id)
	require.NoError(t, err)
	require.True(t, loaded.LastActiveAt.After(now))
	claimed, err := repo.ClaimVMIdleStop(ctx, id, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	require.False(t, claimed)
	v.Spec.IdleAction = "pause"
	require.NoError(t, repo.SaveVM(ctx, v))
	claimed, err = repo.ClaimVMIdleStop(ctx, id, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)
	loaded, err = repo.GetVM(ctx, uint(ws), id)
	require.NoError(t, err)
	require.Equal(t, "paused", loaded.DesiredState)
	claimed, err = repo.ClaimVMIdleStop(ctx, id, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.False(t, claimed)

	a := &types.VMArtifact{ID: uuid.NewString(), VMID: id, Name: "template-" + id[:8], Kind: "template", RootSnapshotID: "committed-root", CreatedAt: time.Now()}
	require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a))
	require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a)) // crash retry
	v.DesiredState = "deleted"
	require.NoError(t, repo.SaveVM(ctx, v))
	child := *v
	child.ID, child.Handle, child.DesiredState = uuid.NewString(), "test-"+uuid.NewString(), "running"
	require.NoError(t, repo.CreateVM(ctx, &child)) // name reuse
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, child.ID) })
	loaded, err = repo.GetVM(ctx, uint(ws), v.Name)
	require.NoError(t, err)
	require.Equal(t, child.ID, loaded.ID)
	items, err := repo.ListVMArtifacts(ctx, uint(ws), "template")
	require.NoError(t, err)
	found := false
	for _, item := range items {
		if item.ID == a.ID {
			found = true
			require.Equal(t, a.RootSnapshotID, item.RootSnapshotID)
		}
	}
	require.True(t, found, "template must survive source deletion")
}
