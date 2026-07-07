package worker

import (
	"net"
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

func TestCopyDirectorySkipsUnixSockets(t *testing.T) {
	src, err := os.MkdirTemp("/tmp", "copydir-src-")
	require.NoError(t, err)
	defer os.RemoveAll(src)
	dst, err := os.MkdirTemp("/tmp", "copydir-dst-")
	require.NoError(t, err)
	defer os.RemoveAll(dst)

	require.NoError(t, os.MkdirAll(filepath.Join(src, "tmp"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "tmp", "regular.txt"), []byte("ok"), 0644))
	require.NoError(t, os.Symlink("regular.txt", filepath.Join(src, "tmp", "regular-link")))

	socketPath := filepath.Join(src, "tmp", "runtime.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	require.NoError(t, err)
	defer listener.Close()

	require.NoError(t, copyDirectory(src, dst, nil))

	data, err := os.ReadFile(filepath.Join(dst, "tmp", "regular.txt"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(data))

	linkTarget, err := os.Readlink(filepath.Join(dst, "tmp", "regular-link"))
	require.NoError(t, err)
	require.Equal(t, "regular.txt", linkTarget)

	_, err = os.Lstat(filepath.Join(dst, "tmp", "runtime.sock"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCopyDirectoryExcludesOnlyRootPaths(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(src, "outputs"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "outputs", "root.txt"), []byte("drop"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(src, "workspace", "outputs"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "workspace", "outputs", "nested.txt"), []byte("keep"), 0644))

	require.NoError(t, copyDirectory(src, dst, []string{"outputs"}))

	_, err := os.Stat(filepath.Join(dst, "outputs", "root.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	data, err := os.ReadFile(filepath.Join(dst, "workspace", "outputs", "nested.txt"))
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
}

func TestCopyDirectoryNormalizesNestedExcludePaths(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(src, "workspace", "outputs"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "workspace", "outputs", "drop.txt"), []byte("drop"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(src, "workspace", "data"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "workspace", "data", "keep.txt"), []byte("keep"), 0644))

	require.NoError(t, copyDirectory(src, dst, []string{"./workspace/outputs/"}))

	_, err := os.Stat(filepath.Join(dst, "workspace", "outputs", "drop.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	data, err := os.ReadFile(filepath.Join(dst, "workspace", "data", "keep.txt"))
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
}

func TestCreateTarWithSHA256ReturnsArchiveHashAndSize(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "checkpoint")
	archivePath := filepath.Join(root, "checkpoint.tar")

	require.NoError(t, os.MkdirAll(filepath.Join(src, "nested"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "nested", "state.txt"), []byte("checkpoint payload"), 0644))

	hash, size, err := createTarWithSHA256(src, archivePath)
	require.NoError(t, err)
	require.NotZero(t, size)

	actualHash, actualSize, err := fileSHA256(archivePath)
	require.NoError(t, err)
	require.Equal(t, actualHash, hash)
	require.Equal(t, actualSize, size)
}
