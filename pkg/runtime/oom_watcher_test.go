package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCgroupOOMWatcherReadsOOMKillCount(t *testing.T) {
	for _, test := range []struct {
		name, file, data string
		want             int64
	}{
		{"v2 kills", memoryEventsFile, "low 0\nhigh 0\nmax 1\noom 1\noom_kill 2\n", 2},
		{"v1 pressure", "memory.oom_control", "oom_kill_disable 0\nunder_oom 1\n", 0},
		{"v1 kills after pressure clears", "memory.oom_control", "oom_kill_disable 0\nunder_oom 0\noom_kill 2\n", 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			watcher := &CgroupOOMWatcher{cgroupRoot: t.TempDir()}
			require.NoError(t, os.WriteFile(filepath.Join(watcher.cgroupRoot, test.file), []byte(test.data), 0o644))
			count, err := watcher.readOOMKillCount()
			require.NoError(t, err)
			require.Equal(t, test.want, count)
		})
	}
}

func TestCgroupOOMWatcherRequiresNewKernelOOMKill(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, memoryEventsFile)
	// Old kills, pressure and allocation failures are not a new OOM victim.
	require.NoError(t, os.WriteFile(path, []byte("max 100\noom 10\noom_kill 4\n"), 0o644))
	watcher := NewCgroupOOMWatcher(t.Context(), "")
	watcher.cgroupRoot = root
	defer watcher.Stop()
	killed := make(chan struct{}, 1)
	require.NoError(t, watcher.Watch(func() { killed <- struct{}{} }))
	require.Never(t, func() bool { return len(killed) > 0 }, 600*time.Millisecond, 10*time.Millisecond)
	// Only the kernel's oom_kill increment identifies a host OOM.
	require.NoError(t, os.WriteFile(path, []byte("max 101\noom 11\noom_kill 5\n"), 0o644))
	receive(t, killed)
}
