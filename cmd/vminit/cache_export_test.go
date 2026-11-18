//go:build linux

package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type exportedEntry struct {
	kind    byte
	content string
	touched bool
}

func readCacheExport(t *testing.T, file string) (names []string, entries map[string]exportedEntry) {
	t.Helper()
	in, err := os.Open(file)
	require.NoError(t, err)
	defer in.Close()
	entries = map[string]exportedEntry{}
	reader := tar.NewReader(in)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return names, entries
		}
		require.NoError(t, err)
		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		_, touched := header.PAXRecords[microvm.CacheExportTouched]
		names = append(names, header.Name)
		entries[header.Name] = exportedEntry{kind: header.Typeflag, content: string(content), touched: touched}
	}
}

// A root and the writable layer over it, as a guest has them: the layer holds
// what the workload changed, the root shows everything.
func cacheExportRoot(t *testing.T) (root, upper, file string) {
	t.Helper()
	root, upper = t.TempDir(), t.TempDir()
	for _, dir := range []string{root, upper} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "home/runner/.cache/go-build/ab"), 0o755))
	}
	return root, upper, filepath.Join(t.TempDir(), microvm.CacheExportFile)
}

func TestWriteCacheExportTakesTheFilesTheWorkloadWrote(t *testing.T) {
	root, upper, file := cacheExportRoot(t)
	cache := filepath.Join(upper, "home/runner/.cache/go-build")
	require.NoError(t, os.WriteFile(filepath.Join(cache, "ab/object"), []byte("object"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(cache, "trim.txt"), nil, 0o644))
	// Not files: a link and a pipe.
	require.NoError(t, os.Symlink("/etc/passwd", filepath.Join(cache, "ab/link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(cache, "ab/pipe"), 0o644))
	// Written elsewhere in the root: not a cache file.
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "home/runner/work"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "home/runner/work/secret"), []byte("secret"), 0o600))

	stats, err := writeCacheExport(file, root, upper, []string{"/home/runner/.cache/go-build", "/home/runner/.npm/_cacache"})
	require.NoError(t, err)
	require.Equal(t, 2, stats.files)
	require.EqualValues(t, len("object"), stats.bytes)

	names, entries := readCacheExport(t, file)
	// The directories that lead to a cache directory come before it, so that
	// whoever reads the export can make them in order.
	require.Equal(t, []string{
		"home/", "home/runner/", "home/runner/.cache/",
		"home/runner/.cache/go-build/", "home/runner/.cache/go-build/ab/", "home/runner/.cache/go-build/ab/object",
		"home/runner/.cache/go-build/trim.txt",
	}, names)
	require.Equal(t, exportedEntry{kind: tar.TypeReg, content: "object"}, entries["home/runner/.cache/go-build/ab/object"])
	require.Equal(t, byte(tar.TypeDir), entries["home/runner/.cache/"].kind)

	require.NoFileExists(t, file+".partial")
}

func TestWriteCacheExportSendsOnlyAHeaderForAFileThatWasUsed(t *testing.T) {
	root, upper, file := cacheExportRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(upper, "home/runner/.cache/go-build/ab/object"), []byte("object"), 0o644))
	used := filepath.Join(upper, "home/runner/.cache/go-build/ab/used")
	require.NoError(t, os.WriteFile(used, nil, 0o644))
	// overlayfs gives a file whose attributes changed the size of the one it
	// stands for, and none of its data.
	require.NoError(t, os.Truncate(used, 1<<20))
	if err := unix.Lsetxattr(used, overlayMetacopy, nil, 0); err != nil {
		t.Skip("requires trusted xattrs and CAP_SYS_ADMIN")
	}

	stats, err := writeCacheExport(file, root, upper, []string{"/home/runner/.cache/go-build"})
	require.NoError(t, err)
	require.Equal(t, 1, stats.touched)
	require.Equal(t, 1, stats.files)

	_, entries := readCacheExport(t, file)
	require.Equal(t, exportedEntry{kind: tar.TypeReg, touched: true}, entries["home/runner/.cache/go-build/ab/used"])
}

func TestWriteCacheExportHasItsNameOnlyWhenComplete(t *testing.T) {
	root, upper, _ := cacheExportRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(upper, "home/runner/.cache/go-build/ab/object"), []byte("object"), 0o644))

	// Nowhere to write it.
	missing := filepath.Join(t.TempDir(), "missing", microvm.CacheExportFile)
	_, err := writeCacheExport(missing, root, upper, []string{"/home/runner/.cache/go-build"})
	require.Error(t, err)
	require.NoFileExists(t, missing)

	// A workload that wrote to none of the directories leaves no export.
	file := filepath.Join(t.TempDir(), microvm.CacheExportFile)
	stats, err := writeCacheExport(file, root, upper, []string{"/home/runner/.npm/_cacache"})
	require.NoError(t, err)
	require.Zero(t, stats.files)
	require.NoFileExists(t, file)
	require.NoFileExists(t, file+".partial")
}
