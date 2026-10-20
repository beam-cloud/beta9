package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCgroupOOMWatcherReadsOOMKillCount(t *testing.T) {
	root := t.TempDir()
	cgroupPath := filepath.Join("containers", "container-id")
	require.NoError(t, os.MkdirAll(filepath.Join(root, cgroupPath), 0755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, cgroupPath, memoryEventsFile),
		[]byte("low 0\nhigh 0\nmax 1\noom 1\noom_kill 2\n"),
		0644,
	))

	watcher := &CgroupOOMWatcher{cgroupRoot: root, cgroupPath: cgroupPath}
	count, err := watcher.readOOMKillCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestCgroupOOMWatcherRequiresNewKernelOOMKill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	path := filepath.Join(root, "container", memoryEventsFile)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	// Old kills, pressure and allocation failures are not a new OOM victim.
	require.NoError(t, os.WriteFile(path, []byte("max 100\noom 10\noom_kill 4\n"), 0o644))
	watcher := NewCgroupOOMWatcher(ctx, "container")
	watcher.cgroupRoot = root
	defer watcher.Stop()
	killed := make(chan struct{}, 1)
	require.NoError(t, watcher.Watch(func() { killed <- struct{}{} }))
	select {
	case <-killed:
		t.Fatal("old OOM or memory pressure was reported as a new kill")
	case <-time.After(600 * time.Millisecond):
	}
	// Only the kernel's oom_kill increment identifies a host OOM.
	require.NoError(t, os.WriteFile(path, []byte("max 101\noom 11\noom_kill 5\n"), 0o644))
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("new kernel OOM kill was not reported")
	}
}

func TestCgroupV1OOMWatcherSeparatesPressureFromKills(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		want int64
	}{
		{name: "memory pressure without a kill", data: "oom_kill_disable 0\nunder_oom 1\n", want: 0},
		{name: "kill after pressure clears", data: "oom_kill_disable 0\nunder_oom 0\noom_kill 2\n", want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(root, "container"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "container", "memory.oom_control"), []byte(test.data), 0o644))
			watcher := &CgroupOOMWatcher{cgroupRoot: root, cgroupPath: "container"}
			count, err := watcher.readOOMKillCount()
			require.NoError(t, err)
			require.Equal(t, test.want, count)
		})
	}
}
