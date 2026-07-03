package worker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTouchStubCodeReadyRefreshesMtime(t *testing.T) {
	root := t.TempDir()
	readyPath := filepath.Join(root, stubCodeReadyMarker)
	require.NoError(t, os.WriteFile(readyPath, []byte("ok"), 0644))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(readyPath, old, old))

	touchStubCodeReady(readyPath)

	info, err := os.Stat(readyPath)
	require.NoError(t, err)
	require.True(t, info.ModTime().After(old))
}

func TestPruneStubCodeCacheRemovesExpiredAndTempEntries(t *testing.T) {
	root := t.TempDir()
	oldReady := writeStubCodeCacheEntry(t, root, "old", time.Now().Add(-8*24*time.Hour))
	recentReady := writeStubCodeCacheEntry(t, root, "recent", time.Now())
	tmpDir := filepath.Join(root, "old.tmp.container")
	require.NoError(t, os.MkdirAll(tmpDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "file.py"), []byte("tmp"), 0644))

	pruned, freed := pruneStubCodeCache(root, 7*24*time.Hour)

	require.Equal(t, 2, pruned)
	require.Positive(t, freed)
	require.NoFileExists(t, oldReady)
	require.NoDirExists(t, tmpDir)
	require.FileExists(t, recentReady)
}

func writeStubCodeCacheEntry(t *testing.T, root, name string, readyTime time.Time) string {
	t.Helper()
	dir := filepath.Join(root, name)
	readyPath := filepath.Join(dir, stubCodeReadyMarker)
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.py"), []byte("print('ok')"), 0644))
	require.NoError(t, os.WriteFile(readyPath, []byte("ok"), 0644))
	require.NoError(t, os.Chtimes(readyPath, readyTime, readyTime))
	return readyPath
}
