package runtime

import (
	"archive/tar"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
)

// The canvas is image content: a symlink where the host writes must be
// replaced, never followed onto the host.
func TestPrepareCanvasDoesNotFollowImageSymlinks(t *testing.T) {
	outside := t.TempDir()
	canvas := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(canvas, ".beam")))
	require.NoError(t, os.Symlink(outside, filepath.Join(canvas, "tmp")))
	require.NoError(t, os.WriteFile(filepath.Join(canvas, "proc"), []byte("not a dir"), 0o644))

	inst := &microVMInstance{canvas: canvas}
	for _, dir := range []string{"tmp", "proc"} {
		require.NoError(t, inst.canvasDir(dir))
	}
	require.NoError(t, removeCanvasEntry(filepath.Join(canvas, microvm.CanvasDir)))
	require.NoError(t, inst.canvasDir(microvm.CanvasDir))
	require.NoError(t, writeFileNoFollow(filepath.Join(canvas, microvm.SpecFile), []byte("{}"), 0o644))

	for _, rel := range []string{"tmp", "proc", ".beam"} {
		info, err := os.Lstat(filepath.Join(canvas, rel))
		require.NoError(t, err)
		require.True(t, info.IsDir(), "%s must be a real directory", rel)
	}
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing may land outside the canvas")

	// A symlink planted where a file goes is refused rather than written through.
	require.NoError(t, os.Remove(filepath.Join(canvas, microvm.SpecFile)))
	require.NoError(t, os.Symlink(filepath.Join(outside, "pwned"), filepath.Join(canvas, microvm.SpecFile)))
	require.Error(t, writeFileNoFollow(filepath.Join(canvas, microvm.SpecFile), []byte("{}"), 0o644))
	_, err = os.Stat(filepath.Join(outside, "pwned"))
	require.True(t, os.IsNotExist(err))
}

func TestLinkLocalFromMAC(t *testing.T) {
	mac, _ := net.ParseMAC("de:f0:d8:b8:88:fb")
	require.Equal(t, "fe80::dcf0:d8ff:feb8:88fb", linkLocalFromMAC(mac).String())
	require.Nil(t, linkLocalFromMAC(net.HardwareAddr{1, 2}))
}

type treeEntry struct {
	hdr  tar.Header
	body string
}

var treeModTime = time.Unix(1700000000, 0)

// guestTar builds a tar the way a guest could, owned by the test's user so
// extraction works unprivileged.
func guestTar(t *testing.T, entries ...treeEntry) *bytes.Buffer {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		hdr := entry.hdr
		hdr.Uid, hdr.Gid = os.Getuid(), os.Getgid()
		hdr.Size = int64(len(entry.body))
		hdr.ModTime = treeModTime
		if hdr.Mode == 0 {
			hdr.Mode = 0o755
		}
		require.NoError(t, tw.WriteHeader(&hdr))
		_, err := tw.Write([]byte(entry.body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return &buf
}

// The guest chooses every name in the export: an entry routed through an
// earlier symlink must fail instead of landing on the host.
func TestExtractTreeDoesNotResolveThroughSymlinks(t *testing.T) {
	cases := []struct {
		name    string
		entries func(outside string) []treeEntry
	}{
		{"file below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/cron.d/x", Typeflag: tar.TypeReg, Mode: 0o644}, body: "* * * * * root pwn\n"},
			}
		}},
		{"directory below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/sub/", Typeflag: tar.TypeDir}},
			}
		}},
		{"directory entry over a symlink", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "d", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o777}},
			}
		}},
		{"fifo below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/fifo", Typeflag: tar.TypeFifo, Mode: 0o600}},
			}
		}},
		{"hard link through a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "stolen", Typeflag: tar.TypeLink, Linkname: "a/secret"}},
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outside := t.TempDir()
			require.NoError(t, os.Chmod(outside, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("host"), 0o600))
			dst := t.TempDir()

			require.Error(t, extractTree(guestTar(t, tc.entries(outside)...), dst))

			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Len(t, entries, 1, "nothing may be created outside the export")
			info, err := os.Stat(outside)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), "the outside directory keeps its mode")
			data, err := os.ReadFile(filepath.Join(outside, "secret"))
			require.NoError(t, err)
			require.Equal(t, "host", string(data))
			_, err = os.Lstat(filepath.Join(dst, "stolen"))
			require.True(t, os.IsNotExist(err), "no hard link to a host file")
		})
	}
}

// A file entry where an earlier entry left a symlink replaces the symlink.
func TestExtractTreeReplacesASymlinkWithAFile(t *testing.T) {
	outside := t.TempDir()
	dst := t.TempDir()
	target := filepath.Join(outside, "target")

	require.NoError(t, extractTree(guestTar(t,
		treeEntry{hdr: tar.Header{Name: "f", Typeflag: tar.TypeSymlink, Linkname: target}},
		treeEntry{hdr: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644}, body: "guest"},
	), dst))

	_, err := os.Lstat(target)
	require.True(t, os.IsNotExist(err), "the write must not follow the symlink")
	info, err := os.Lstat(filepath.Join(dst, "f"))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
}

func TestExtractTreeReplaysTheGuestTree(t *testing.T) {
	dst := t.TempDir()
	require.NoError(t, extractTree(guestTar(t,
		treeEntry{hdr: tar.Header{Name: "etc/", Typeflag: tar.TypeDir}},
		treeEntry{hdr: tar.Header{Name: "etc/app.conf", Typeflag: tar.TypeReg, Mode: 0o640}, body: "key=value\n"},
		treeEntry{hdr: tar.Header{Name: "etc/current", Typeflag: tar.TypeSymlink, Linkname: "/etc/app.conf"}},
		treeEntry{hdr: tar.Header{Name: "etc/copy", Typeflag: tar.TypeLink, Linkname: "etc/app.conf", Mode: 0o640}},
		treeEntry{hdr: tar.Header{Name: "run/pipe", Typeflag: tar.TypeFifo, Mode: 0o620}},
	), dst))

	data, err := os.ReadFile(filepath.Join(dst, "etc/app.conf"))
	require.NoError(t, err)
	require.Equal(t, "key=value\n", string(data))
	file, err := os.Lstat(filepath.Join(dst, "etc/app.conf"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), file.Mode().Perm())
	require.True(t, file.ModTime().Equal(treeModTime))

	link, err := os.Readlink(filepath.Join(dst, "etc/current"))
	require.NoError(t, err)
	require.Equal(t, "/etc/app.conf", link, "symlinks are kept as written")
	copied, err := os.Lstat(filepath.Join(dst, "etc/copy"))
	require.NoError(t, err)
	require.True(t, os.SameFile(file, copied))

	pipe, err := os.Lstat(filepath.Join(dst, "run/pipe"))
	require.NoError(t, err)
	require.True(t, pipe.Mode()&os.ModeNamedPipe != 0)
	require.Equal(t, os.FileMode(0o620), pipe.Mode().Perm())

	dir, err := os.Stat(filepath.Join(dst, "etc"))
	require.NoError(t, err)
	require.True(t, dir.ModTime().Equal(treeModTime), "directory times are restored after their children")
}
