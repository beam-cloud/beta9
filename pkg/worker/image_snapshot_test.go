package worker

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
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

func TestPackOverlayLayersPacksUpperDirAsOCILayer(t *testing.T) {
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

	layers, err := packOverlayLayers(upper, t.TempDir(), ggcrtypes.DockerLayer)
	require.NoError(t, err)
	require.Len(t, layers, 1)
	require.Positive(t, layers[0].size)
	require.Equal(t, int64(len("payload")), layers[0].contentBytes)

	// Digests were computed inline; they must match what ggcr derives from the file.
	fromFile, err := tarball.LayerFromFile(layers[0].path)
	require.NoError(t, err)
	wantDigest, _ := fromFile.Digest()
	wantDiffID, _ := fromFile.DiffID()
	require.Equal(t, wantDigest, layers[0].digest)
	require.Equal(t, wantDiffID, layers[0].diffID)

	entries := readLayerEntries(t, layers[0].path)
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

func TestWriteSparseOCILayoutHoldsManifestConfigAndOnlyTheNewLayer(t *testing.T) {
	base, err := random.Image(1024, 2)
	require.NoError(t, err)
	dir := t.TempDir()
	upper := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(upper, "f"), []byte("x"), 0o644))
	layers, err := packOverlayLayers(upper, filepath.Join(dir, "layers"), ggcrtypes.DockerLayer)
	require.NoError(t, err)
	img, err := appendLayers(base, layers, "test")
	require.NoError(t, err)

	layoutDir := filepath.Join(dir, "layout")
	require.NoError(t, writeSparseOCILayout(layoutDir, img, layers))

	path, err := layout.FromPath(layoutDir)
	require.NoError(t, err)
	index, err := path.ImageIndex()
	require.NoError(t, err)
	manifest, err := index.IndexManifest()
	require.NoError(t, err)
	require.Len(t, manifest.Manifests, 1)
	got, err := path.Image(manifest.Manifests[0].Digest)
	require.NoError(t, err)
	wantDigest, _ := img.Digest()
	gotDigest, _ := got.Digest()
	require.Equal(t, wantDigest, gotDigest)
	gotLayers, err := got.Layers()
	require.NoError(t, err)
	require.Len(t, gotLayers, 3)
	_, err = gotLayers[2].Compressed() // the new layer's blob is present
	require.NoError(t, err)
	_, err = gotLayers[0].Compressed() // base layer blobs are not
	require.Error(t, err)
	entries, err := os.ReadDir(filepath.Join(layoutDir, "blobs", "sha256"))
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestPackOverlayLayersSplitsLargeDeltasAndKeepsLinksResolvable(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "lib"), 0o755))
	// 40 files of 1 MiB with a 4 MiB target -> about 10 layers.
	body := make([]byte, 1<<20)
	for i := range body {
		body[i] = byte(i)
	}
	for i := 0; i < 40; i++ {
		body[0] = byte(i)
		require.NoError(t, os.WriteFile(filepath.Join(upper, "lib", fmt.Sprintf("f%02d.so", i)), body, 0o644))
	}
	// Hard link to a file that lands in an earlier layer than the link's walk position.
	require.NoError(t, os.Link(filepath.Join(upper, "lib", "f00.so"), filepath.Join(upper, "lib", "zz-hard")))
	// Directory, symlink and small file: all pinned to layer 0.
	require.NoError(t, os.Symlink("lib/f01.so", filepath.Join(upper, "link")))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "small"), []byte("s"), 0o644))

	layers, err := packOverlayLayersWithTarget(upper, t.TempDir(), ggcrtypes.OCILayer, 4<<20)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(layers), 8)
	require.LessOrEqual(t, len(layers), 12)

	// Every entry appears exactly once across layers; the union has everything.
	seen := map[string]int{}
	var linkLayer, targetLayer int
	for i, layer := range layers {
		for name, hdr := range readLayerEntries(t, layer.path) {
			seen[name]++
			switch name {
			case "lib/zz-hard":
				linkLayer = i
				// Linked to its target within the same layer, else a full copy.
				if hdr.Typeflag == tar.TypeLink {
					require.Equal(t, "lib/f00.so", hdr.Linkname)
				} else {
					require.Equal(t, int64(1<<20), hdr.Size)
				}
			case "lib/f00.so":
				targetLayer = i
			}
		}
	}
	require.Equal(t, targetLayer, linkLayer, "hard-linked inodes stay in one layer")
	require.Equal(t, 1, seen["lib/"])
	require.Equal(t, 1, seen["link"])
	require.Equal(t, 1, seen["small"])
	for i := 0; i < 40; i++ {
		require.Equal(t, 1, seen[fmt.Sprintf("lib/f%02d.so", i)], "file %d", i)
	}
	require.Contains(t, readLayerEntries(t, layers[0].path), "lib/")
	require.Contains(t, readLayerEntries(t, layers[0].path), "link")

	var content int64
	for _, layer := range layers {
		content += layer.contentBytes
		require.Equal(t, ggcrtypes.OCILayer, layer.mediaType)
		fromFile, err := tarball.LayerFromFile(layer.path)
		require.NoError(t, err)
		d, _ := fromFile.Digest()
		require.Equal(t, d, layer.digest)
	}
	require.Equal(t, int64(40<<20+1), content, "the hard link is a link entry, not a copy")
}

func TestPackOverlayLayersOneLayerForSmallDelta(t *testing.T) {
	upper := t.TempDir()
	for i := 0; i < 5; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(upper, fmt.Sprintf("f%d", i)), make([]byte, 1000), 0o644))
	}
	layers, err := packOverlayLayers(upper, t.TempDir(), ggcrtypes.DockerLayer)
	require.NoError(t, err)
	require.Len(t, layers, 1)
}
