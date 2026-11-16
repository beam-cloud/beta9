//go:build linux

package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMicroVMGuestInitReadOnlySharedMount(t *testing.T) {
	if os.Getenv("BETA9_MICROVM_REAL_INIT_MOUNT") != "1" {
		t.Skip("requires real privileged bind mounts")
	}
	root := t.TempDir()
	source := filepath.Join(root, "pinned-init")
	require.NoError(t, os.WriteFile(source, []byte("exact captured executable"), 0o755))
	original, err := os.Stat(source)
	require.NoError(t, err)
	for _, name := range []string{"fresh", "restored"} {
		inst := &microVMInstance{canvas: filepath.Join(root, name)}
		require.NoError(t, os.MkdirAll(filepath.Join(inst.canvas, microvm.CanvasDir), 0o700))
		target := filepath.Join(inst.canvas, microvm.InitPath)
		require.NoError(t, inst.installGuestBinary(source, microvm.InitPath))
		t.Cleanup(func() { require.NoError(t, unix.Unmount(target, unix.MNT_DETACH)) })
		installed, err := os.Stat(target)
		require.NoError(t, err)
		require.True(t, os.SameFile(original, installed))
		require.Equal(t, []string{target}, inst.submounts)
		require.ErrorIs(t, os.WriteFile(target, []byte("overwrite"), 0o755), unix.EROFS)
		data, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, "exact captured executable", string(data))
	}
	inst := &microVMInstance{canvas: filepath.Join(root, "missing")}
	require.NoError(t, os.MkdirAll(filepath.Join(inst.canvas, microvm.CanvasDir), 0o700))
	require.ErrorIs(t, inst.installGuestBinary(filepath.Join(root, "absent"), microvm.InitPath), os.ErrNotExist)
	require.Empty(t, inst.submounts)
}
