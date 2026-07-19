package runtime

import (
	"os"
	"path/filepath"
	"testing"

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
