package worker

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
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
	require.NoError(t, copyFixedSize(&buf, strings.NewReader("abc"), 5))
	require.Equal(t, []byte{'a', 'b', 'c', 0, 0}, buf.data)
	buf.data = nil
	require.NoError(t, copyFixedSize(&buf, strings.NewReader("abc"), 2))
	require.Equal(t, []byte("ab"), buf.data)
}

// walkUpper collects the entries packOverlayLayers would, so a test can change
// the tree between the walk and the write the way a running sandbox does.
func walkUpper(t *testing.T, upper string) []overlayEntry {
	var entries []overlayEntry
	require.NoError(t, filepath.WalkDir(upper, func(path string, d os.DirEntry, err error) error {
		require.NoError(t, err)
		rel, _ := filepath.Rel(upper, path)
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		require.NoError(t, err)
		entry := overlayEntry{path: path, rel: filepath.ToSlash(rel), info: info}
		if info.Mode()&os.ModeSymlink != 0 {
			entry.link, err = os.Readlink(path)
			require.NoError(t, err)
		}
		if info.Mode().IsRegular() {
			entry.size = info.Size()
		}
		entries = append(entries, entry)
		return nil
	}))
	return entries
}

// A sandbox can swap a walked file, or a directory on the way to it, for a
// symlink before the file is read. The symlink must not be followed (it could
// point anywhere on the worker host); the entry is left out of the layer.
func TestWriteLayerTarDoesNotFollowSymlinksSwappedInAfterTheWalk(t *testing.T) {
	host := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(host, "secret"), []byte("host secret"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(host, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(host, "etc", "passwd"), []byte("host passwd"), 0o644))

	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "etc", "passwd"), []byte("sandbox passwd"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "swapped"), []byte("sandbox file"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "kept"), []byte("kept"), 0o644))
	entries := walkUpper(t, upper)

	// Final component swapped for a symlink to a host file.
	require.NoError(t, os.Remove(filepath.Join(upper, "swapped")))
	require.NoError(t, os.Symlink(filepath.Join(host, "secret"), filepath.Join(upper, "swapped")))
	// Directory component swapped: etc -> host's etc, which has a passwd too.
	require.NoError(t, os.RemoveAll(filepath.Join(upper, "etc")))
	require.NoError(t, os.Symlink(filepath.Join(host, "etc"), filepath.Join(upper, "etc")))

	layer, err := writeLayerTar(upper, entries, filepath.Join(t.TempDir(), "layer.tar.gz"), ggcrtypes.DockerLayer, 1)
	require.NoError(t, err)
	got := readLayerEntries(t, layer.path)
	require.Equal(t, "kept", got["kept"].PAXRecords["body"])
	require.NotContains(t, got, "swapped")
	require.NotContains(t, got, "etc/passwd")
	require.Equal(t, int64(len("kept")), layer.contentBytes)
	// The directory header itself was written from the walk's info, as a
	// directory; nothing under the swapped path was read.
	require.Equal(t, byte(tar.TypeDir), got["etc/"].Typeflag)
	for name, hdr := range got {
		require.NotContains(t, hdr.PAXRecords["body"], "host", name)
	}
}

// A file removed between the walk and the read is skipped rather than failing
// the snapshot, and no header is left behind for it.
func TestWriteLayerTarSkipsFilesDeletedAfterTheWalk(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "d", "gone"), []byte("gone"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "d", "kept"), []byte("kept"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "replaced"), []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "other"), []byte("new"), 0o644))
	// Hard links: the first link is removed, so the second must carry the content.
	require.NoError(t, os.WriteFile(filepath.Join(upper, "link-a"), []byte("linked"), 0o644))
	require.NoError(t, os.Link(filepath.Join(upper, "link-a"), filepath.Join(upper, "link-b")))
	entries := walkUpper(t, upper)

	require.NoError(t, os.Remove(filepath.Join(upper, "d", "gone")))
	require.NoError(t, os.Remove(filepath.Join(upper, "link-a")))
	// Another inode renamed over it: the name is there but it is not the
	// walked file (renaming, unlike remove+create, cannot reuse the inode).
	require.NoError(t, os.Rename(filepath.Join(upper, "other"), filepath.Join(upper, "replaced")))

	layer, err := writeLayerTar(upper, entries, filepath.Join(t.TempDir(), "layer.tar.gz"), ggcrtypes.DockerLayer, 1)
	require.NoError(t, err)
	got := readLayerEntries(t, layer.path)
	require.NotContains(t, got, "d/gone")
	require.NotContains(t, got, "link-a")
	require.NotContains(t, got, "replaced")
	require.NotContains(t, got, "other")
	require.Equal(t, "kept", got["d/kept"].PAXRecords["body"])
	require.Equal(t, byte(tar.TypeReg), got["link-b"].Typeflag)
	require.Equal(t, "linked", got["link-b"].PAXRecords["body"])
	require.Equal(t, int64(len("kept")+len("linked")), layer.contentBytes)

	// The tar is well formed: ggcr can hash it.
	fromFile, err := tarball.LayerFromFile(layer.path)
	require.NoError(t, err)
	d, _ := fromFile.Digest()
	require.Equal(t, d, layer.digest)
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
	// Directory and symlink are pinned to layer 0; the small file is a
	// regular file, so it is dealt out like any other (it lands in whichever
	// layer is being filled when the walk reaches it).
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

// An insecure build registry must not loosen the transport used to fetch a
// sandbox's base image from any other registry.
func TestParseImageReferenceAppliesInsecureOnlyToBuildRegistry(t *testing.T) {
	client := &ImageClient{config: types.AppConfig{ImageService: types.ImageServiceConfig{
		BuildRegistry:         "registry.internal:5000",
		BuildRegistryInsecure: true,
	}}}

	ref, err := client.parseImageReference("registry.internal:5000/beta9/images:snap-1")
	require.NoError(t, err)
	require.Equal(t, "http", ref.Context().Registry.Scheme())

	for _, external := range []string{
		"docker.io/library/python:3.11-slim",
		"python:3.11-slim",
		"ghcr.io/org/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"registry.internal:5001/beta9/images:snap-1", // different port, different registry
	} {
		ref, err := client.parseImageReference(external)
		require.NoError(t, err)
		require.Equal(t, "https", ref.Context().Registry.Scheme(), external)
	}

	client.config.ImageService.BuildRegistryInsecure = false
	ref, err = client.parseImageReference("registry.internal:5000/beta9/images:snap-1")
	require.NoError(t, err)
	require.Equal(t, "https", ref.Context().Registry.Scheme())
}
