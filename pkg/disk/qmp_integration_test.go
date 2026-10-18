package disk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompactReleasesQEMUOverlays(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	for i := 1; i <= 32; i++ {
		sealed, _, err := volume.Seal(ctx, true)
		require.NoError(t, err)
		require.Len(t, sealed, 1)
		require.NoError(t, volume.MarkPublished(sealed[0].Path, fmt.Sprintf("snap-%d", i)))
		if i%8 == 0 {
			require.NoError(t, volume.Compact(ctx))
			client, err := dialQMP(ctx, volume.state.QMPSocket)
			require.NoError(t, err)
			nodes, err := client.namedBlockNodes(ctx)
			client.Close()
			require.NoError(t, err)
			require.Len(t, nodes, 4, "only the base, head and their file children may remain after snapshot %d", i)
		}
	}
}

func TestSealRecoversExhaustedQEMUDaemon(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	client, err := dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	// Reproduce an older worker's already-unlinked, monitor-owned overlays.
	// Fill the daemon's real descriptor table without deepening its live chain.
	for i := 10_000; i < 10_200; i++ {
		path := headLayerPath(filepath.Join(volume.dir, layersSubdir), i)
		require.NoError(t, volume.manager.createQcowBase(ctx, path, 8<<20))
		name := fmtNodeName(i)
		err = client.addOverlay(ctx, name, qsdFileNodePrefix+name, path)
		require.NoError(t, os.Remove(path))
		if err != nil {
			break
		}
	}
	require.ErrorContains(t, err, "Too many open files")
	client.Close()

	sealed, _, err := volume.Seal(ctx, true)
	require.NoError(t, err, "recover without restarting the daemon or its VM")
	require.Len(t, sealed, 1)
	client, err = dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	nodes, err := client.namedBlockNodes(ctx)
	require.NoError(t, err)
	require.Len(t, nodes, 6, "only the base, original head, new head and their file nodes remain")
}

func newQEMUVolume(t *testing.T) (context.Context, *Volume) {
	t.Helper()
	qsd, err := exec.LookPath("qemu-storage-daemon")
	if err != nil {
		t.Skip("qemu-storage-daemon is not installed")
	}
	qemuImg, err := exec.LookPath("qemu-img")
	if err != nil {
		t.Skip("qemu-img is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	dir, err := os.MkdirTemp("", "qcow")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, layersSubdir), 0o700))
	head := headLayerPath(filepath.Join(dir, layersSubdir), 0)
	// No filesystem is mounted; only the daemon and block graph are real.
	manager := NewManager(Config{Root: dir, Binaries: Binaries{
		QemuImg: qemuImg, QSD: qsd, Fsfreeze: "true",
	}, Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == qsd {
			// Keep both regressions below the real process's soft limit.
			args = append([]string{"-c", `ulimit -n 128; exec "$@"`, "qsd", qsd}, args...)
			name = "sh"
		}
		return execRunner(ctx, name, args...)
	}})
	base := filepath.Join(dir, layersSubdir, "base.qcow2")
	require.NoError(t, manager.createQcowBase(ctx, base, 8<<20))
	require.NoError(t, manager.createQcowOverlay(ctx, head, base, 8<<20))
	proc, err := manager.startQSD(ctx, filepath.Join(dir, "run"), head, "fmt-0", false, ExportNBD)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.stopQSD(context.Background(), proc)) })
	return ctx, &Volume{manager: manager, dir: dir, fmtNode: "fmt-0", state: &volumeState{
		Key: "test", VirtualSizeBytes: 8 << 20, Attached: true, Formatted: true,
		HeadPath: head, QMPSocket: proc.qmpSocket,
		Chain: []stateLayer{{SnapshotID: "base", Path: base}},
	}}
}
