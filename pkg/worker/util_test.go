package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForceSymlinkCreatesParentAndReplacesExistingLink(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	link := filepath.Join(root, "missing", "nested", "link")

	require.NoError(t, os.MkdirAll(source, 0755))
	require.NoError(t, forceSymlink(source, link))

	target, err := os.Readlink(link)
	require.NoError(t, err)
	require.Equal(t, source, target)

	nextSource := filepath.Join(root, "next-source")
	require.NoError(t, os.MkdirAll(nextSource, 0755))
	require.NoError(t, forceSymlink(nextSource, link))

	target, err = os.Readlink(link)
	require.NoError(t, err)
	require.Equal(t, nextSource, target)
}
