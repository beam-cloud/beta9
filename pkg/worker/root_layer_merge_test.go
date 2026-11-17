package worker

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
)

type cacheEntry struct {
	name     string
	kind     byte
	content  string
	mode     int64
	uid      int
	modTime  time.Time
	touched  bool
	linkname string
}

// writeCacheExportTar writes the tar a guest would, with whatever entries.
func writeCacheExportTar(t *testing.T, entries []cacheEntry) string {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), microvm.CacheExportFile))
	require.NoError(t, err)
	defer file.Close()
	writer := tar.NewWriter(file)
	for _, entry := range entries {
		// A test that is not root's can give files only to itself.
		gid := entry.uid
		if entry.uid == os.Getuid() {
			gid = os.Getgid()
		}
		header := &tar.Header{
			Name: entry.name, Typeflag: entry.kind, Mode: entry.mode, Linkname: entry.linkname,
			Uid: entry.uid, Gid: gid, ModTime: entry.modTime, Format: tar.FormatPAX,
		}
		if entry.kind == tar.TypeReg && !entry.touched {
			header.Size = int64(len(entry.content))
		}
		if entry.touched {
			header.PAXRecords = map[string]string{microvm.CacheExportTouched: "1"}
		}
		require.NoError(t, writer.WriteHeader(header))
		if header.Size > 0 {
			_, err := writer.Write([]byte(entry.content))
			require.NoError(t, err)
		}
	}
	require.NoError(t, writer.Close())
	return file.Name()
}

func cacheDirs(uid int, names ...string) []cacheEntry {
	entries := make([]cacheEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, cacheEntry{name: name + "/", kind: tar.TypeDir, mode: 0o755, uid: uid})
	}
	return entries
}

var cacheTestDirs = []string{"/home/runner/.cache/go-build", "/home/runner/go/pkg/mod"}

func TestMergeCacheExportAddsFilesBelowTheLayersDirectories(t *testing.T) {
	uid := os.Getuid()
	written := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "home/runner/.cache/go-build/ab"),
		cacheEntry{name: "home/runner/.cache/go-build/ab/abcd-d", kind: tar.TypeReg, content: "object", mode: 0o644, uid: uid, modTime: written},
		// Set-id bits never reach the layer.
		cacheEntry{name: "home/runner/.cache/go-build/ab/tool", kind: tar.TypeReg, content: "#!", mode: 0o6755, uid: uid, modTime: written},
	))
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 2, stats.files)
	require.EqualValues(t, len("object")+len("#!"), stats.bytes)
	require.Zero(t, stats.refused)

	object := filepath.Join(layer, "home/runner/.cache/go-build/ab/abcd-d")
	content, err := os.ReadFile(object)
	require.NoError(t, err)
	require.Equal(t, "object", string(content))
	info, err := os.Stat(object)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode())
	require.True(t, info.ModTime().Equal(written))

	info, err = os.Stat(filepath.Join(layer, "home/runner/.cache/go-build/ab/tool"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode(), "no set-id bit, and still a program")

	info, err = os.Stat(filepath.Join(layer, "home/runner/.cache"))
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|0o755, info.Mode())

	// Nothing is left of the names files are written under.
	leftovers, err := filepath.Glob(filepath.Join(layer, "home/runner/.cache/go-build/ab/*"+cacheMergeSuffix))
	require.NoError(t, err)
	require.Empty(t, leftovers)
}

func TestMergeCacheExportRefusesWhatIsNotACacheFile(t *testing.T) {
	uid := os.Getuid()
	outside := t.TempDir()
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "etc", "home/runner/.ssh"),
		// Not below one of the layer's directories.
		cacheEntry{name: "etc/passwd", kind: tar.TypeReg, content: "root::0:0", mode: 0o644, uid: uid},
		cacheEntry{name: "home/runner/.ssh/authorized_keys", kind: tar.TypeReg, content: "key", mode: 0o600, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build", kind: tar.TypeReg, content: "file", mode: 0o644, uid: uid},
		// Names that would leave the layer.
		cacheEntry{name: "home/runner/.cache/go-build/../../../../escape", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		cacheEntry{name: "../escape", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		// Anything that is not a file or a directory.
		cacheEntry{name: "home/runner/.cache/go-build/link", kind: tar.TypeSymlink, linkname: outside, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/hard", kind: tar.TypeLink, linkname: "etc/passwd", uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/device", kind: tar.TypeChar, mode: 0o666, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/pipe", kind: tar.TypeFifo, mode: 0o666, uid: uid},
		// A file whose directory the export did not bring.
		cacheEntry{name: "home/runner/.cache/go-build/missing/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
		cacheEntry{name: "home/runner/.cache/go-build/kept", kind: tar.TypeReg, content: "kept", mode: 0o644, uid: uid},
	))
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.files)
	require.Equal(t, 12, stats.refused)

	var found []string
	require.NoError(t, filepath.WalkDir(layer, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			found = append(found, path)
		}
		return err
	}))
	require.Equal(t, []string{filepath.Join(layer, "home/runner/.cache/go-build/kept")}, found)
	require.NoDirExists(t, filepath.Join(layer, "etc"))
	require.NoDirExists(t, filepath.Join(layer, "home/runner/.ssh"))
	escaped, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, escaped)
	require.NoFileExists(t, filepath.Join(filepath.Dir(layer), "escape"))
}

func TestMergeCacheExportBringsForwardTheTimeOfAFileThatWasUsed(t *testing.T) {
	uid := os.Getuid()
	layer := t.TempDir()
	dir := filepath.Join(layer, "home/runner/.cache/go-build/ab")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"used", "rewritten", "unused"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("before"), 0o644))
		require.NoError(t, os.Chtimes(filepath.Join(dir, name), old, old))
	}

	used := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build", "home/runner/.cache/go-build/ab"),
		cacheEntry{name: "home/runner/.cache/go-build/ab/used", kind: tar.TypeReg, touched: true, mode: 0o644, uid: uid, modTime: used},
		// The layer has no such file, so there is nothing to bring forward.
		cacheEntry{name: "home/runner/.cache/go-build/ab/gone", kind: tar.TypeReg, touched: true, mode: 0o644, uid: uid, modTime: used},
		cacheEntry{name: "home/runner/.cache/go-build/ab/rewritten", kind: tar.TypeReg, content: "after", mode: 0o644, uid: uid, modTime: used},
	))

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.touched)
	require.Equal(t, 1, stats.files)

	info, err := os.Stat(filepath.Join(dir, "used"))
	require.NoError(t, err)
	require.True(t, info.ModTime().Equal(used))
	content, err := os.ReadFile(filepath.Join(dir, "used"))
	require.NoError(t, err)
	require.Equal(t, "before", string(content), "a header carries no content")

	content, err = os.ReadFile(filepath.Join(dir, "rewritten"))
	require.NoError(t, err)
	require.Equal(t, "after", string(content))

	info, err = os.Stat(filepath.Join(dir, "unused"))
	require.NoError(t, err)
	require.True(t, info.ModTime().Equal(old))
	require.NoFileExists(t, filepath.Join(dir, "gone"))
}

func TestMergeCacheExportNeverFollowsALinkOutOfTheLayer(t *testing.T) {
	uid := os.Getuid()
	layer, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(layer, "home/runner/.cache"), 0o755))
	// No merge makes a link. One that is there anyway leads nowhere.
	require.NoError(t, os.Symlink(outside, filepath.Join(layer, "home/runner/.cache/go-build")))

	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build"),
		cacheEntry{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
	))

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.Zero(t, stats.files)
	if err == nil {
		require.NotZero(t, stats.refused)
	}
	escaped, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	require.Empty(t, escaped)
}

func TestMergeCacheExportStopsWhenTheLayerIsFull(t *testing.T) {
	uid := os.Getuid()
	export := writeCacheExportTar(t, append(
		cacheDirs(uid, "home", "home/runner", "home/runner/.cache", "home/runner/.cache/go-build"),
		cacheEntry{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: uid},
	))
	layer := t.TempDir()

	// More is to stay free than the filesystem has.
	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 1<<62)
	require.NoError(t, err)
	require.True(t, stats.full)
	require.Zero(t, stats.files)
	require.NoFileExists(t, filepath.Join(layer, "home/runner/.cache/go-build/entry"))
}

func TestMergeCacheExportRefusesADirectoryItsJobCouldNotWriteIn(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files to other users requires root")
	}
	const runner = 1001
	export := writeCacheExportTar(t, []cacheEntry{
		{name: "home/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		// Made with sudo: every later job would find a directory of root's
		// where it expects to make its own.
		{name: "home/runner/.cache/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/.cache/go-build/", kind: tar.TypeDir, mode: 0o755, uid: 0},
		{name: "home/runner/.cache/go-build/entry", kind: tar.TypeReg, content: "x", mode: 0o644, uid: 0},
		{name: "home/runner/go/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/mod/", kind: tar.TypeDir, mode: 0o755, uid: runner},
		{name: "home/runner/go/pkg/mod/module.zip", kind: tar.TypeReg, content: "zip", mode: 0o444, uid: runner},
	})
	layer := t.TempDir()

	stats, err := mergeCacheExport(export, layer, cacheTestDirs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stats.files)
	require.NoDirExists(t, filepath.Join(layer, "home/runner/.cache"))
	require.FileExists(t, filepath.Join(layer, "home/runner/go/pkg/mod/module.zip"))
}
