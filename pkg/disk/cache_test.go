package disk

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPruneDetachedRetainsUnpublishedAndActiveVolumes(t *testing.T) {
	root := t.TempDir()
	m := NewManager(Config{Root: root})
	old := time.Now().Add(-2 * time.Hour)
	var probed []string
	m.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		path := args[len(args)-1]
		if args[0] == "info" {
			return json.Marshal(qcowInfo{VirtualSize: 4096, BackingFile: filepath.Join(filepath.Dir(path), "base.qcow2")})
		}
		probed = append(probed, path)
		if strings.Contains(path, "dirty") {
			return []byte(`[{"start":0,"length":4096,"depth":0,"present":true,"zero":true}]`), nil
		}
		return []byte(`[{"start":0,"length":4096,"depth":1,"present":true}]`), nil
	}
	for _, key := range []string{"clean", "fresh", "dirty", "pending", "journal", "active", "attaching", "unpublished", "readonly"} {
		dir := m.volumeDir(key)
		require.NoError(t, os.MkdirAll(dir, 0700))
		state := &volumeState{Key: key, VirtualSizeBytes: 4096,
			Chain:    []stateLayer{{SnapshotID: "published", Path: filepath.Join(dir, "base.qcow2")}},
			HeadPath: filepath.Join(dir, "head.qcow2")}
		switch key {
		case "pending":
			state.Pending = []stateLayer{{Path: state.HeadPath}}
		case "journal":
			state.Journal = true
		case "active":
			state.Attached = true
		case "attaching":
			m.volumes[key] = nil
		case "unpublished":
			state.Chain[0].SnapshotID = ""
		case "readonly":
			state.ReadOnly, state.HeadPath = true, ""
		}
		require.NoError(t, saveVolumeState(dir, state))
		if key != "fresh" {
			require.NoError(t, os.Chtimes(filepath.Join(dir, stateFileName), old, old))
		}
	}
	pruned, freed, err := m.PruneDetached(context.Background(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, pruned)
	require.Positive(t, freed)
	for _, key := range []string{"clean", "readonly"} {
		require.NoDirExists(t, m.volumeDir(key))
	}
	for _, key := range []string{"fresh", "dirty", "pending", "journal", "active", "attaching", "unpublished"} {
		require.DirExists(t, m.volumeDir(key))
	}
	require.Len(t, probed, 3) // clean, dirty, and an in-flight attach rechecked before removal
}

func TestPruneDetachedRemovesAdoptedSpareIndexAndRecoversDeletion(t *testing.T) {
	m := NewManager(Config{Root: t.TempDir()})
	dir := m.volumeDir(".spare-adopted")
	require.NoError(t, os.MkdirAll(dir, 0700))
	state := &volumeState{Key: "real-volume", ReadOnly: true, Chain: []stateLayer{{SnapshotID: "published", Path: filepath.Join(dir, "base")}}}
	require.NoError(t, saveVolumeState(dir, state))
	require.NoError(t, os.Symlink(filepath.Base(dir), m.volumeDir(state.Key)))
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(dir, stateFileName), old, old))
	leftover := filepath.Join(m.root, ".evict-volume-interrupted", "volume")
	require.NoError(t, os.MkdirAll(leftover, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(leftover, "layer"), []byte("old"), 0600))
	pruned, _, err := m.PruneDetached(context.Background(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	require.NoDirExists(t, dir)
	_, err = os.Lstat(m.volumeDir(state.Key))
	require.True(t, errors.Is(err, os.ErrNotExist))
	require.NoDirExists(t, filepath.Dir(leftover))
}

func TestPruneDetachedDoesNotRemoveStateChangedDuringProbe(t *testing.T) {
	m := NewManager(Config{Root: t.TempDir()})
	dir := m.volumeDir("volume")
	require.NoError(t, os.MkdirAll(dir, 0700))
	state := &volumeState{Key: "volume", VirtualSizeBytes: 4096, HeadPath: filepath.Join(dir, "head"),
		Chain: []stateLayer{{SnapshotID: "published", Path: filepath.Join(dir, "base")}}}
	require.NoError(t, saveVolumeState(dir, state))
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(dir, stateFileName), old, old))
	m.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "info" {
			return json.Marshal(qcowInfo{VirtualSize: 4096, BackingFile: state.Chain[0].Path})
		}
		// An attach+detach may complete while metadata is inspected. Its new
		// state inode must invalidate the scan even if the key is free again.
		require.NoError(t, saveVolumeState(dir, state))
		return []byte(`[{"start":0,"length":4096,"depth":1,"present":true}]`), nil
	}
	pruned, _, err := m.PruneDetached(context.Background(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.DirExists(t, dir)
}

func TestEmptyQcowHeadRequiresCompleteMapAndNoHeadSectors(t *testing.T) {
	for _, bad := range []string{
		`[]`, `null`, `{}`, `[{"start":0,"length":4096}]`,
		`[{"start":0,"length":4096,"depth":0,"present":true,"zero":true}]`,
		`[{"start":1,"length":4095,"depth":1,"present":true}]`,
		`[{"start":0,"length":2048,"depth":1,"present":true}]`,
		`[{"start":0,"length":8192,"depth":1,"present":true}]`,
	} {
		require.Error(t, emptyQcowHead([]byte(bad), 4096), bad)
	}
	require.NoError(t, emptyQcowHead([]byte(`[{"start":0,"length":4096,"depth":0,"present":false,"zero":true}]`), 4096))
}

func TestPruneDetachedWithRealQcowAllocationMaps(t *testing.T) {
	for _, binary := range []string{"qemu-img", "qemu-io"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Skip(binary + " is not installed")
		}
	}
	for _, policy := range []string{"ttl", "pressure", "ttl-marker", "pressure-marker"} {
		for _, mode := range []string{"clean", "dirty", "zeroed"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				m := NewManager(Config{Root: t.TempDir()})
				dir := m.volumeDir(mode)
				require.NoError(t, os.MkdirAll(dir, 0700))
				base, head := filepath.Join(dir, "base.qcow2"), filepath.Join(dir, "head.qcow2")
				const size = 16 << 20
				require.NoError(t, m.createQcowBase(ctx, base, size))
				_, err := execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", "write -P 0x5a 0 64k", base)
				require.NoError(t, err)
				require.NoError(t, m.createQcowOverlay(ctx, head, base, size))
				if mode != "clean" {
					write := "write -P 0x6b 0 64k"
					if mode == "zeroed" {
						write = "write -z 0 64k"
					}
					_, err := execRunner(ctx, "qemu-io", "-f", "qcow2", "-c", write, head)
					require.NoError(t, err)
				}
				state := &volumeState{Key: mode, VirtualSizeBytes: size, HeadPath: head,
					Chain: []stateLayer{{SnapshotID: "published", Path: base}}}
				if strings.HasSuffix(policy, "-marker") {
					state.Attached, state.Export, state.QSDPid = true, string(ExportVhostUser), 12345
				}
				require.NoError(t, saveVolumeState(dir, state))
				if strings.HasSuffix(policy, "-marker") {
					clearVhostAttachment(state)
					require.NoError(t, saveDetachedVolumeMarker(dir, state))
				}
				var pruned int
				if strings.HasPrefix(policy, "ttl") {
					old := time.Now().Add(-2 * time.Hour)
					require.NoError(t, os.Chtimes(filepath.Join(dir, stateFileName), old, old))
					pruned, _, err = m.PruneDetached(ctx, time.Now().Add(-time.Hour))
				} else {
					pruned, _, err = m.PruneDetachedWithBudget(ctx, time.Now().Add(-time.Hour), 1)
				}
				require.NoError(t, err)
				if mode == "clean" {
					require.Equal(t, 1, pruned)
					require.NoDirExists(t, dir)
				} else {
					require.Zero(t, pruned)
					require.FileExists(t, head, "unpublished writes, including optimized zeros, must survive cleanup")
				}
			})
		}
	}
}

func TestPruneDetachedPressureEvictsOldestRecoverableCopyAndStopsAtBudget(t *testing.T) {
	m := NewManager(Config{Root: t.TempDir()})
	now := time.Now()
	for i, key := range []string{"dirty", "active", "z-older", "a-newer"} {
		dir := m.volumeDir(key)
		require.NoError(t, os.MkdirAll(dir, 0700))
		state := &volumeState{Key: key, ReadOnly: true, Chain: []stateLayer{{SnapshotID: "published", Path: filepath.Join(dir, "base")}}}
		if key == "dirty" {
			state.ReadOnly = false
			state.HeadPath = filepath.Join(dir, "head")
		}
		if key == "active" {
			state.Attached = true
		}
		require.NoError(t, saveVolumeState(dir, state))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base"), []byte(strings.Repeat("x", 65536)), 0600))
		modified := now.Add(time.Duration(i-5) * time.Minute)
		require.NoError(t, os.Chtimes(filepath.Join(dir, stateFileName), modified, modified))
	}
	m.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("unpublished head is not disposable")
	}
	n, _, err := m.PruneDetached(context.Background(), now.Add(-time.Hour))
	require.NoError(t, err)
	require.Zero(t, n, "recent copies stay warm without disk pressure")
	n, freed, err := m.PruneDetachedWithBudget(context.Background(), now.Add(-time.Hour), 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Positive(t, freed)
	require.NoDirExists(t, m.volumeDir("z-older"))
	for _, key := range []string{"dirty", "active", "a-newer"} {
		require.DirExists(t, m.volumeDir(key))
	}
}
