package disk

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVhostDetachMarkerRoundTrip(t *testing.T) {
	manager, _ := newSpareTestManagerWithDevices(t, 0)
	manager.spareExport = ExportVhostUser
	manager.detachedStateMarker = true
	ctx := context.Background()
	defer manager.Close(ctx)
	require.NoError(t, manager.WarmSpares(ctx, testSpareSize))
	manager.mu.Lock()
	manager.spareBuilds[testSpareSize] = true // Keep optional refill out of this lifecycle proof.
	manager.mu.Unlock()
	spec := AttachSpec{Key: "vm-root", Owner: "first", VirtualSizeBytes: testSpareSize, Export: ExportVhostUser}
	volume, err := manager.Attach(ctx, spec, nil)
	require.NoError(t, err)
	before, err := os.ReadFile(filepath.Join(volume.dir, stateFileName))
	require.NoError(t, err)
	require.NoError(t, manager.DetachOwned(ctx, spec.Key, spec.Owner))
	require.FileExists(t, filepath.Join(volume.dir, detachedMarkerName))
	require.NoDirExists(t, manager.runtimeDir(spec.Key), "the daemon and runtime sockets must be gone before detach returns")
	after, err := os.ReadFile(filepath.Join(volume.dir, stateFileName))
	require.NoError(t, err)
	require.Equal(t, before, after)
	state, err := loadVolumeState(volume.dir)
	require.NoError(t, err)
	require.False(t, state.Attached)
	spec.Owner = "successor"
	volume, err = manager.Attach(ctx, spec, nil)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(volume.dir, detachedMarkerName))
	state, err = loadVolumeState(volume.dir)
	require.NoError(t, err)
	require.True(t, state.Attached)
	require.Equal(t, "successor", state.Owner)
}

func attachedMarkerTestState() *volumeState {
	return &volumeState{Key: "disk", Owner: "container-one", VirtualSizeBytes: 1 << 30,
		Attached: true, QSDPid: 12345, QMPSocket: "/run/disk/qmp", ExportSocket: "/run/disk/blk", Export: string(ExportVhostUser),
		HeadPath: "/disk/head", Chain: []stateLayer{{Path: "/disk/base", SnapshotID: "origin-one"}}, Pending: []stateLayer{{Path: "/disk/pending"}}}
}

func TestDetachedMarkerPreservesDurableManifestAndUnpublishedWrites(t *testing.T) {
	dir := t.TempDir()
	state := attachedMarkerTestState()
	require.NoError(t, saveVolumeState(dir, state))
	manifest := filepath.Join(dir, stateFileName)
	old := time.Now().Add(-24 * time.Hour)
	require.NoError(t, os.Chtimes(manifest, old, old))
	before, err := os.ReadFile(manifest)
	require.NoError(t, err)
	clearVhostAttachment(state) // The daemon has stopped; only attachment fields changed.
	require.NoError(t, saveDetachedVolumeMarker(dir, state))
	after, err := os.ReadFile(manifest)
	require.NoError(t, err)
	require.Equal(t, before, after, "shutdown hint must never replace the authoritative payload manifest")
	info, err := os.Stat(manifest)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), info.ModTime(), time.Second, "detach must refresh local-cache recency")
	loaded, err := loadVolumeState(dir)
	require.NoError(t, err)
	require.Equal(t, state, loaded)
	require.Len(t, loaded.Pending, 1, "unpublished layers must remain protected")
	// Loss of an unsynced hint after power loss exposes the original, complete
	// attached manifest. Normal recovery can then clean up its dead daemon.
	require.NoError(t, os.Remove(filepath.Join(dir, detachedMarkerName)))
	loaded, err = loadVolumeState(dir)
	require.NoError(t, err)
	require.True(t, loaded.Attached)
	require.Equal(t, state.Pending, loaded.Pending)
	require.Equal(t, state.Chain, loaded.Chain)
	require.Equal(t, state.HeadPath, loaded.HeadPath)
}

func TestDetachedMarkerAcceptsDrainedPendingSliceAfterPublication(t *testing.T) {
	dir := t.TempDir()
	state := attachedMarkerTestState()
	state.Pending = []stateLayer{} // MarkPublished drains the slice without making it nil.
	require.NoError(t, saveVolumeState(dir, state))
	before, err := os.ReadFile(filepath.Join(dir, stateFileName))
	require.NoError(t, err)
	clearVhostAttachment(state)
	require.NoError(t, saveDetachedVolumeMarker(dir, state))
	after, err := os.ReadFile(filepath.Join(dir, stateFileName))
	require.NoError(t, err)
	require.Equal(t, before, after)
	loaded, err := loadVolumeState(dir)
	require.NoError(t, err)
	require.False(t, loaded.Attached)
	require.Empty(t, loaded.Pending)
	require.Equal(t, state.Chain, loaded.Chain)
}

func TestDetachedMarkerRejectsPayloadChangesAndInvalidHints(t *testing.T) {
	for name, mutate := range map[string]func(*volumeState){
		"head":    func(s *volumeState) { s.HeadPath = "/disk/new-head" },
		"owner":   func(s *volumeState) { s.Owner = "new-container" },
		"chain":   func(s *volumeState) { s.Chain = nil },
		"pending": func(s *volumeState) { s.Pending = nil },
		"journal": func(s *volumeState) { s.Journal = true },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			state := attachedMarkerTestState()
			require.NoError(t, saveVolumeState(dir, state))
			clearVhostAttachment(state)
			mutate(state)
			require.ErrorContains(t, saveDetachedVolumeMarker(dir, state), "payload state changed")
		})
	}
	for _, hint := range []string{"", "{", `{"version":2,"state_sha256":"ignored"}`, `{"version":1,"state_sha256":"stale"}`} {
		dir := t.TempDir()
		state := attachedMarkerTestState()
		require.NoError(t, saveVolumeState(dir, state))
		require.NoError(t, os.WriteFile(filepath.Join(dir, detachedMarkerName), []byte(hint), 0600))
		loaded, err := loadVolumeState(dir)
		require.NoError(t, err)
		require.Equal(t, state, loaded, "untrusted hint must leave normal recovery in charge")
	}
}

func TestDetachedMarkerCannotSuppressReattachmentOrChangedManifest(t *testing.T) {
	dir := t.TempDir()
	state := attachedMarkerTestState()
	require.NoError(t, saveVolumeState(dir, state))
	clearVhostAttachment(state)
	require.NoError(t, saveDetachedVolumeMarker(dir, state))
	oldHint, err := os.ReadFile(filepath.Join(dir, detachedMarkerName))
	require.NoError(t, err)
	state.Attached, state.QSDPid, state.Owner = true, 23456, "container-two"
	require.NoError(t, saveVolumeState(dir, state))
	require.NoFileExists(t, filepath.Join(dir, detachedMarkerName))
	// Even if an old hint reappears after a crash, its digest cannot describe
	// the new owner, head, pending layers or daemon identity.
	require.NoError(t, os.WriteFile(filepath.Join(dir, detachedMarkerName), oldHint, 0600))
	loaded, err := loadVolumeState(dir)
	require.NoError(t, err)
	require.Equal(t, state, loaded)
	var durable volumeState
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &durable))
	require.Equal(t, state, &durable)
}
