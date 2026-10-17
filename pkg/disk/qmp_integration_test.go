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
	qsd, err := exec.LookPath("qemu-storage-daemon")
	if err != nil {
		t.Skip("qemu-storage-daemon is not installed")
	}
	qemuImg, err := exec.LookPath("qemu-img")
	if err != nil {
		t.Skip("qemu-img is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir, err := os.MkdirTemp("", "qcow")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, layersSubdir), 0o700))
	head := headLayerPath(filepath.Join(dir, layersSubdir), 0)
	// No filesystem is mounted; only the daemon and block graph are real.
	manager := NewManager(Config{Root: dir, Binaries: Binaries{
		QemuImg: qemuImg, QSD: qsd, Fsfreeze: "true",
	}})
	base := filepath.Join(dir, layersSubdir, "base.qcow2")
	require.NoError(t, manager.createQcowBase(ctx, base, 8<<20))
	require.NoError(t, manager.createQcowOverlay(ctx, head, base, 8<<20))
	proc, err := manager.startQSD(ctx, filepath.Join(dir, "run"), head, "fmt-0", false, ExportNBD)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.stopQSD(context.Background(), proc)) })
	volume := &Volume{manager: manager, dir: dir, fmtNode: "fmt-0", state: &volumeState{
		Key: "test", VirtualSizeBytes: 8 << 20, Attached: true, Formatted: true,
		HeadPath: head, QMPSocket: proc.qmpSocket,
		Chain: []stateLayer{{SnapshotID: "base", Path: base}},
	}}
	for i := 1; i <= 32; i++ {
		sealed, _, err := volume.Seal(ctx, true)
		require.NoError(t, err)
		require.Len(t, sealed, 1)
		require.NoError(t, volume.MarkPublished(sealed[0].Path, fmt.Sprintf("snap-%d", i)))
		if i%8 == 0 {
			require.NoError(t, volume.Compact(ctx))
			client, err := dialQMP(ctx, proc.qmpSocket)
			require.NoError(t, err)
			nodes, err := client.namedBlockNodes(ctx)
			client.Close()
			require.NoError(t, err)
			require.Len(t, nodes, 4, "only the base, head and their file children may remain after snapshot %d", i)
		}
	}
}
