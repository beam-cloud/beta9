package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDirectoryArchivePreservesOverlayWhiteoutWithoutMaterializingItInCache(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("overlay whiteouts are Linux character devices")
	}
	if os.Geteuid() != 0 {
		t.Skip("creating an overlay whiteout requires mknod")
	}

	root, err := os.MkdirTemp("/dev/shm", "beta9-whiteout-archive-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	src := filepath.Join(root, "upper")
	require.NoError(t, os.MkdirAll(filepath.Join(src, "etc"), 0755))
	whiteout := filepath.Join(src, "etc", ".wh.issue")
	if err := syscall.Mknod(whiteout, syscall.S_IFCHR|0600, 0); err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) {
			t.Skipf("creating an overlay whiteout is unsupported: %v", err)
		}
		require.NoError(t, err)
	}
	opaque := filepath.Join(src, "opaque")
	require.NoError(t, os.Mkdir(opaque, 0755))
	require.NoError(t, unix.Setxattr(opaque, "user.beta9.test", []byte("opaque"), 0))

	realTar, err := exec.LookPath("tar")
	require.NoError(t, err)
	fakeBin := t.TempDir()
	// Model the cache filesystem rejecting mknod during the legacy streaming
	// extract; nested archive creation and restore still use the real tar.
	wrapper := fmt.Sprintf("#!/bin/sh\ncase \"$*\" in *\"-xf - -C\"*) exit 1;; esac\nexec %q \"$@\"\n", realTar)
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "tar"), []byte(wrapper), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	request := &types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{Config: `{"_beta9_force_resource_limits":true}`}}}
	overlay := common.NewContainerOverlay(request, filepath.Join(root, "merged"), filepath.Join(root, "overlay-storage"))
	archivePath := filepath.Join(root, "cache", checkpointFsArchive)
	require.NoError(t, captureCheckpointFilesystem(context.Background(), &ContainerInstance{Request: request, Overlay: overlay}, filepath.Dir(archivePath), true))
	require.NoDirExists(t, filepath.Join(root, "cache", checkpointFsDir))
	info, err := os.Stat(archivePath)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.NoFileExists(t, filepath.Join(root, "cache", "etc", ".wh.issue"))

	dst := filepath.Join(root, "restored-upper")
	require.NoError(t, extractDirectoryArchiveContext(context.Background(), archivePath, dst))
	restored, err := os.Lstat(filepath.Join(dst, "etc", ".wh.issue"))
	require.NoError(t, err)
	require.NotZero(t, restored.Mode()&os.ModeCharDevice)
	value := make([]byte, len("opaque"))
	size, err := unix.Getxattr(filepath.Join(dst, "opaque"), "user.beta9.test", value)
	require.NoError(t, err)
	require.Equal(t, "opaque", string(value[:size]))
}

func TestDirectoryArchivePublishesOnlyACompleteFinalArchive(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, checkpointFsArchive)

	err := archiveDirectoryContext(context.Background(), filepath.Join(root, "missing-upper"), destination, nil)
	require.Error(t, err)
	require.NoFileExists(t, destination)
	matches, globErr := filepath.Glob(filepath.Join(root, "."+checkpointFsArchive+"-*"))
	require.NoError(t, globErr)
	require.Empty(t, matches)
}

func TestTarXattrArgsPreserveOverlayMetadataOnLinux(t *testing.T) {
	args := tarXattrArgs()
	require.Contains(t, args, "--xattrs")
	if runtime.GOOS == "linux" {
		require.Contains(t, args, "--xattrs-include=*")
	} else {
		require.NotContains(t, args, "--xattrs-include=*")
	}
}

func TestFileLockReleaseKeepsStableLockFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.lock")
	first := NewFileLock(path)
	require.NoError(t, first.Acquire())
	require.NoError(t, first.Release())

	_, err := os.Stat(path)
	require.NoError(t, err)

	second := NewFileLock(path)
	require.NoError(t, second.Acquire())
	t.Cleanup(func() { require.NoError(t, second.Release()) })

	contender := NewFileLock(path)
	require.Error(t, contender.Acquire())
}

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

func TestCopyDirectoryContextHonorsCancellation(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "dst")
	require.NoError(t, os.WriteFile(filepath.Join(src, "state.bin"), []byte("state"), 0644))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := copyDirectoryContext(ctx, src, dst, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCreateTarWithSHA256KeepsSparseFilesSparse(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("needs GNU tar")
	}
	root := t.TempDir()
	src := filepath.Join(root, "checkpoint")
	require.NoError(t, os.MkdirAll(src, 0755))
	disk, err := os.Create(filepath.Join(src, "root.img"))
	require.NoError(t, err)
	require.NoError(t, disk.Truncate(1<<30))
	_, err = disk.WriteAt([]byte("data"), 1<<29)
	require.NoError(t, err)
	require.NoError(t, disk.Close())

	_, size, err := createTarWithSHA256(src, filepath.Join(root, "checkpoint.tar"))
	require.NoError(t, err)
	require.Less(t, size, int64(1<<20), "a 1 GiB image holding 4 bytes archives to a few KiB")
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

func TestCreateTarWithSHA256ReportsProgress(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "checkpoint")
	archivePath := filepath.Join(root, "checkpoint.tar")

	require.NoError(t, os.MkdirAll(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "state.bin"), make([]byte, 2<<20), 0644))

	var updates []int64
	_, size, err := createTarWithSHA256Progress(context.Background(), src, archivePath, func(completed int64) {
		updates = append(updates, completed)
	})
	require.NoError(t, err)
	require.NotEmpty(t, updates)
	require.Equal(t, size, updates[len(updates)-1])
}

func TestCreateTarWithSHA256HonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	archivePath := filepath.Join(t.TempDir(), "checkpoint.tar")
	_, _, err := createTarWithSHA256Progress(ctx, t.TempDir(), archivePath, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoFileExists(t, archivePath)
}

// Spec files are copied into trees the container controls: a symlink it
// planted at the destination is replaced, and one at the source is refused.
func TestCopyFileDoesNotFollowPlantedSymlinks(t *testing.T) {
	hostFile := filepath.Join(t.TempDir(), "host.conf")
	require.NoError(t, os.WriteFile(hostFile, []byte("host"), 0o600))
	src := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(src, []byte(`{"ociVersion":"1.0.2"}`), 0o644))
	root := t.TempDir()

	dst := filepath.Join(root, "config.json")
	require.NoError(t, os.Symlink(hostFile, dst))
	require.NoError(t, copyFile(src, dst))
	data, err := os.ReadFile(hostFile)
	require.NoError(t, err)
	require.Equal(t, "host", string(data), "the copy must not write through the symlink")
	info, err := os.Lstat(dst)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())

	planted := filepath.Join(root, "initial_config.json")
	require.NoError(t, os.Symlink(hostFile, planted))
	require.Error(t, copyFile(planted, filepath.Join(t.TempDir(), "initial_config.json")), "a symlinked source would copy a host file into the archive")
}

func TestSpecFilesDoNotFollowPlantedSymlinks(t *testing.T) {
	hostFile := filepath.Join(t.TempDir(), "cron")
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.Symlink(hostFile, path))

	require.NoError(t, writeSpecFile(path, specs.Spec{Version: "1.0.2"}))
	_, err := os.Lstat(hostFile)
	require.True(t, os.IsNotExist(err), "the spec must not land on the host")
	data, err := readFileNoFollow(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "1.0.2")

	require.NoError(t, os.WriteFile(hostFile, []byte("{}"), 0o600))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink(hostFile, path))
	_, err = readFileNoFollow(path)
	require.Error(t, err, "reading a planted symlink would pull a host file into the spec")
}
