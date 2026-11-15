package repository

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMirrorRepositoryIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	ws := uint(vmIntegrationWorkspace(t))
	ctx := context.Background()
	name := "mirror-test-" + uuid.NewString()[:8]
	vmID := uuid.NewString()
	artifactID := uuid.NewString()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM vm_artifact WHERE id=$1`, artifactID)
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, vmID)
		_, _ = db.Exec(`DELETE FROM mirror_generation WHERE mirror_id IN (SELECT id FROM mirror WHERE workspace_id=$1 AND name=$2)`, ws, name)
		_, _ = db.Exec(`DELETE FROM mirror WHERE workspace_id=$1 AND name=$2`, ws, name)
	})

	_, err := repo.GetMirror(ctx, ws, name)
	require.ErrorIs(t, err, sql.ErrNoRows)
	mirror, created, err := repo.EnsureMirror(ctx, ws, name)
	require.NoError(t, err)
	require.True(t, created)
	require.Nil(t, mirror.Latest)
	again, created, err := repo.EnsureMirror(ctx, ws, name)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, mirror.ID, again.ID)

	digest := func(seed string) string { return strings.Repeat(seed, 32) }
	commit := func(parent int64, seed string) (*types.MirrorGeneration, error) {
		generation := &types.MirrorGeneration{MirrorID: mirror.ID, Parent: parent, ManifestDigest: digest(seed), ManifestSize: 10, Entries: 2, Objects: 1, TotalSize: 100, TokenID: "token"}
		return generation, repo.CommitMirrorGeneration(ctx, generation)
	}
	_, err = commit(1, "aa")
	require.ErrorIs(t, err, ErrMirrorGenerationConflict, "a parent that does not exist")
	first, err := commit(0, "aa")
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Number)
	require.False(t, first.CreatedAt.IsZero())
	require.Empty(t, first.Home)
	_, err = commit(0, "bb")
	require.ErrorIs(t, err, ErrMirrorGenerationConflict, "a parent that already has a child")

	// Many writers on the same parent: exactly one wins.
	var wins, conflicts int
	var mu sync.Mutex
	var writers sync.WaitGroup
	for range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			_, err := commit(1, "cc")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
			} else if err == ErrMirrorGenerationConflict {
				conflicts++
			}
		}()
	}
	writers.Wait()
	require.Equal(t, 1, wins)
	require.Equal(t, 7, conflicts)
	third := &types.MirrorGeneration{MirrorID: mirror.ID, Parent: 2, ManifestDigest: digest("dd"), ManifestSize: 10, Entries: 2, Objects: 1, TotalSize: 100, TokenID: "token", Home: "/Users/luke"}
	require.NoError(t, repo.CommitMirrorGeneration(ctx, third))
	require.Equal(t, int64(3), third.Number)
	require.Equal(t, "/Users/luke", third.Home)

	latest, err := repo.GetMirrorGeneration(ctx, mirror.ID, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), latest.Number)
	require.Equal(t, "/Users/luke", latest.Home)
	loaded, err := repo.GetMirror(ctx, ws, name)
	require.NoError(t, err)
	require.Equal(t, int64(3), loaded.Latest.Number)
	mirrors, err := repo.ListMirrors(ctx, ws)
	require.NoError(t, err)
	require.Contains(t, mirrors, *loaded)
	ids, err := repo.ListMirrorIDs(ctx)
	require.NoError(t, err)
	found := false
	for _, listed := range ids {
		found = found || listed.ID == mirror.ID && listed.WorkspaceID == ws
	}
	require.True(t, found)

	// Pins on VMs and artifacts are references; deleted VMs are not.
	pin := func(generation int64) *types.VMMirror {
		return &types.VMMirror{Name: name, ID: mirror.ID, Generation: generation, ManifestDigest: digest("aa")}
	}
	now := time.Now().UTC()
	v := &types.VM{ID: vmID, WorkspaceID: ws, WorkspaceExternalID: "test", TokenID: "test", Name: "mirror-vm-" + vmID[:8], Handle: "mirror-" + vmID, DesiredState: "running", Status: "running", CreatedAt: now, LastActiveAt: now, Spec: types.VMSpec{Mirror: pin(1)}}
	require.NoError(t, repo.CreateVM(ctx, v))
	require.NoError(t, repo.CreateVMArtifact(ctx, ws, &types.VMArtifact{ID: artifactID, Name: "mirror-template-" + artifactID[:8], VMID: vmID, Kind: types.VMArtifactTemplate, Spec: types.VMSpec{Mirror: pin(2)}, CreatedAt: now}))
	references, err := repo.ListMirrorReferences(ctx, ws, mirror.ID)
	require.NoError(t, err)
	require.Equal(t, []types.MirrorReference{
		{Kind: "template", Name: "mirror-template-" + artifactID[:8], Generation: 2},
		{Kind: "vm", Name: v.Name, Generation: 1},
	}, references)
	other, err := repo.ListMirrorReferences(ctx, ws, uuid.NewString())
	require.NoError(t, err)
	require.Empty(t, other)
	v.DesiredState = types.VMStateDeleted
	require.NoError(t, repo.SaveVM(ctx, v))
	references, err = repo.ListMirrorReferences(ctx, ws, mirror.ID)
	require.NoError(t, err)
	require.Len(t, references, 1)

	// Expiry is reversible until the generation is deleted, and only
	// expired generations can be deleted.
	require.NoError(t, repo.SetMirrorGenerationsExpired(ctx, mirror.ID, []int64{1, 2}, true))
	generations, err := repo.ListMirrorGenerations(ctx, mirror.ID)
	require.NoError(t, err)
	require.Len(t, generations, 3)
	require.Equal(t, int64(3), generations[0].Number)
	require.Nil(t, generations[0].ExpiredAt)
	require.NotNil(t, generations[1].ExpiredAt)
	require.NoError(t, repo.SetMirrorGenerationsExpired(ctx, mirror.ID, []int64{2}, false))
	second, err := repo.GetMirrorGeneration(ctx, mirror.ID, 2)
	require.NoError(t, err)
	require.Nil(t, second.ExpiredAt)
	require.NoError(t, repo.DeleteMirrorGenerations(ctx, mirror.ID, []int64{1, 2, 3}))
	generations, err = repo.ListMirrorGenerations(ctx, mirror.ID)
	require.NoError(t, err)
	require.Len(t, generations, 2)
	fourth, err := commit(3, "ee")
	require.NoError(t, err)
	require.Equal(t, int64(4), fourth.Number)

	// A purged mirror is gone, takes no commits, frees its name, and is
	// listed for object deletion until that is recorded.
	require.NoError(t, repo.PurgeMirror(ctx, mirror.ID))
	_, err = repo.GetMirror(ctx, ws, name)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = commit(4, "ff")
	require.ErrorIs(t, err, ErrMirrorGenerationConflict)
	purged, err := repo.ListPurgedMirrors(ctx)
	require.NoError(t, err)
	pending := func(mirrors []types.Mirror) bool {
		for _, listed := range mirrors {
			if listed.ID == mirror.ID {
				return true
			}
		}
		return false
	}
	require.True(t, pending(purged))
	fresh, created, err := repo.EnsureMirror(ctx, ws, name)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEqual(t, mirror.ID, fresh.ID)
	require.Nil(t, fresh.Latest)
	require.NoError(t, repo.MarkMirrorObjectsDeleted(ctx, mirror.ID, time.Time{}))
	purged, err = repo.ListPurgedMirrors(ctx)
	require.NoError(t, err)
	require.True(t, pending(purged))
	require.NoError(t, repo.MarkMirrorObjectsDeleted(ctx, mirror.ID, time.Now().Add(time.Hour)))
	purged, err = repo.ListPurgedMirrors(ctx)
	require.NoError(t, err)
	require.False(t, pending(purged))
}
