package worker

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func readLayerEntries(t *testing.T, path string) map[string]*tar.Header {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	entries := map[string]*tar.Header{}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Typeflag == tar.TypeReg {
			body, err := io.ReadAll(tr)
			require.NoError(t, err)
			hdr.PAXRecords = map[string]string{"body": string(body)}
		}
		entries[hdr.Name] = hdr
	}
	return entries
}

func TestWriteOverlayLayerPacksUpperDirAsOCILayer(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "app", "data"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "app", "data", "file.bin"), []byte("payload"), 0o640))
	require.NoError(t, os.Symlink("data/file.bin", filepath.Join(upper, "app", "link")))
	require.NoError(t, os.Link(filepath.Join(upper, "app", "data", "file.bin"), filepath.Join(upper, "app", "hard")))
	root := os.Geteuid() == 0
	if root {
		// Overlay whiteout: a 0/0 character device; opaque directory marker xattr.
		require.NoError(t, unix.Mknod(filepath.Join(upper, "app", "gone"), unix.S_IFCHR|0o000, 0))
		require.NoError(t, unix.Lsetxattr(filepath.Join(upper, "app", "data"), overlayOpaqueXattr, []byte("y"), 0))
	}

	dir := t.TempDir()
	out, plain := filepath.Join(dir, "layer.tar.gz"), filepath.Join(dir, "layer.tar")
	packed, err := writeOverlayLayer(upper, out, plain)
	require.NoError(t, err)
	require.Positive(t, packed.compressedSize)
	tarBytes, err := os.ReadFile(plain)
	require.NoError(t, err)
	sum := sha256.Sum256(tarBytes)
	require.Equal(t, hex.EncodeToString(sum[:]), packed.tarHash)

	entries := readLayerEntries(t, out)
	require.Equal(t, byte(tar.TypeDir), entries["app/"].Typeflag)
	require.Equal(t, byte(tar.TypeDir), entries["app/data/"].Typeflag)
	require.Equal(t, "payload", entries["app/data/file.bin"].PAXRecords["body"])
	require.Equal(t, int64(0o640), entries["app/data/file.bin"].Mode)
	require.Equal(t, "data/file.bin", entries["app/link"].Linkname)
	require.Equal(t, byte(tar.TypeLink), entries["app/hard"].Typeflag)
	require.Equal(t, "app/data/file.bin", entries["app/hard"].Linkname)
	_, hasRoot := entries[""]
	require.False(t, hasRoot)
	if root {
		require.Equal(t, int64(0), entries["app/.wh.gone"].Size)
		require.Contains(t, entries, "app/data/.wh..wh..opq")
	}
}

func TestCopyFixedSizePadsShrunkenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("abc"), 0o644))
	var buf writerBuffer
	require.NoError(t, copyFixedSize(&buf, path, 5))
	require.Equal(t, []byte{'a', 'b', 'c', 0, 0}, buf.data)
	buf.data = nil
	require.NoError(t, copyFixedSize(&buf, path, 2))
	require.Equal(t, []byte("ab"), buf.data)
}

type writerBuffer struct{ data []byte }

func (w *writerBuffer) Write(p []byte) (int, error) {
	w.data = append(w.data, p...)
	return len(p), nil
}
