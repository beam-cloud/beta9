package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseProcessRSS(t *testing.T) {
	rss, err := parseProcessRSS([]byte("100 25 10 2 0 3 0\n"), 4096)
	require.NoError(t, err)
	require.Equal(t, uint64(25*4096), rss)
}

func TestParseProcessRSSRejectsInvalidData(t *testing.T) {
	_, err := parseProcessRSS([]byte("100 invalid\n"), 4096)
	require.Error(t, err)
}

func TestParseProcessChildren(t *testing.T) {
	children, err := parseProcessChildren([]byte("123 456 789\n"))
	require.NoError(t, err)
	require.Equal(t, []int32{123, 456, 789}, children)

	children, err = parseProcessChildren(nil)
	require.NoError(t, err)
	require.Empty(t, children)
}

func TestParseProcessChildrenRejectsInvalidPID(t *testing.T) {
	_, err := parseProcessChildren([]byte("123 nope\n"))
	require.Error(t, err)
}

func TestReadProcessChildrenIncludesChildrenForkedByNonLeaderThread(t *testing.T) {
	root := t.TempDir()
	taskRoot := filepath.Join(root, "100", "task")
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "100"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "101"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "100", "children"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "101", "children"), []byte("200 201\n"), 0644))

	children, err := readProcessChildrenAt(root, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []int32{200, 201}, children)
}
