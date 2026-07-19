package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadProcTreeIncludesNonLeaderChildren(t *testing.T) {
	root := t.TempDir()
	taskRoot := filepath.Join(root, "100", "task")
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "100"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "101"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "100", "children"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "101", "children"), []byte("200 201\n"), 0644))

	pids, err := readProcTree(root, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []int32{100, 200, 201}, pids)
}
