package disk

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInitializeVhostWriteBackWithRealQemu(t *testing.T) {
	for _, b := range []string{"qemu-img", "qemu-storage-daemon"} {
		if _, err := exec.LookPath(b); err != nil {
			t.Skip(b + " unavailable")
		}
	}
	dir, err := os.MkdirTemp("", "vwc")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	image := filepath.Join(dir, "disk")
	socket := filepath.Join(dir, "vhost.sock")
	out, err := exec.CommandContext(ctx, "qemu-img", "create", "-f", "qcow2", image, "16M").CombinedOutput()
	require.NoError(t, err, string(out))
	file, _ := json.Marshal(map[string]any{"driver": "file", "filename": image, "node-name": "file"})
	cmd := exec.CommandContext(ctx, "qemu-storage-daemon", "--blockdev", string(file), "--blockdev", "driver=qcow2,file=file,node-name=disk", "--export", "type=vhost-user-blk,id=vol,node-name=disk,addr.type=unix,addr.path="+socket+",writable=on")
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	require.NoError(t, waitForSocket(ctx, socket, time.Second))
	// A second connection exercises the same reconnect needed by Cloud
	// Hypervisor after initialization; no guest memory or queues are installed.
	require.NoError(t, initializeVhostWriteBack(ctx, socket))
	require.NoError(t, initializeVhostWriteBack(ctx, socket))
}
