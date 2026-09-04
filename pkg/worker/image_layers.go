package worker

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/pgzip"
	"golang.org/x/sys/unix"
)

const (
	overlayOpaqueXattr = "trusted.overlay.opaque"
	whiteoutPrefix     = ".wh."
	opaqueWhiteout     = ".wh..wh..opq"

	// layerSplitTargetBytes is how much file content one packed layer aims to
	// hold. Registries, the indexer and the content cache all move one layer
	// per stream, at well under a node's bandwidth: a single 3 GiB blob went
	// to ECR at 60-80 MiB/s while eight in flight together reached 345 MiB/s.
	// Splitting a large delta lets every stage run that wide.
	layerSplitTargetBytes = 512 << 20
	// layerSplitMaxLayers caps how many layers one delta becomes; beyond this
	// the target grows instead.
	layerSplitMaxLayers = 16
)

// packedLayer is a gzip'd OCI layer on local disk whose digests were computed
// while it was written, so ggcr never has to re-read (or re-inflate) it.
type packedLayer struct {
	path           string
	digest, diffID v1.Hash
	size           int64 // compressed
	contentBytes   int64 // file content packed, before tar framing and compression
	mediaType      ggcrtypes.MediaType
}

func (l *packedLayer) Digest() (v1.Hash, error)                { return l.digest, nil }
func (l *packedLayer) DiffID() (v1.Hash, error)                { return l.diffID, nil }
func (l *packedLayer) Size() (int64, error)                    { return l.size, nil }
func (l *packedLayer) MediaType() (ggcrtypes.MediaType, error) { return l.mediaType, nil }
func (l *packedLayer) Compressed() (io.ReadCloser, error)      { return os.Open(l.path) }

// Layer returns l as a v1.Layer for mutate/remote.
func (l *packedLayer) Layer() (v1.Layer, error) { return partial.CompressedToLayer(l) }

type overlayEntry struct {
	path string // absolute
	rel  string // relative to the upper dir, "/"-separated
	info os.FileInfo
	link string // symlink target
	size int64  // regular file content
}

// packOverlayLayers packs an overlayfs upper directory into one or more gzip'd
// OCI layers under dir. Overlay's whiteouts become the OCI ones: a 0/0
// character device turns into an empty ".wh.<name>" entry and an opaque
// directory gains a ".wh..wh..opq" child. Files are captured at the size seen
// when their header is written; a sandbox may still be running, so a file
// that shrinks meanwhile is zero-padded and one that grows is truncated.
//
// Directories, whiteouts and symlinks all go in the first layer; regular
// files are dealt across layers in walk order so each holds about
// layerSplitTargetBytes of content. A hard link whose target landed in another
// layer is written as a full copy, since tar links only reach within a layer.
// The layers are written concurrently.
func packOverlayLayers(upperDir, dir string, mediaType ggcrtypes.MediaType) ([]*packedLayer, error) {
	return packOverlayLayersWithTarget(upperDir, dir, mediaType, layerSplitTargetBytes)
}

func packOverlayLayersWithTarget(upperDir, dir string, mediaType ggcrtypes.MediaType, targetBytes int64) ([]*packedLayer, error) {
	var entries []overlayEntry
	var contentBytes int64
	err := filepath.WalkDir(upperDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(upperDir, path)
		if err != nil || rel == "." {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		entry := overlayEntry{path: path, rel: filepath.ToSlash(rel), info: info}
		if info.Mode()&os.ModeSymlink != 0 {
			if entry.link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		if info.Mode().IsRegular() {
			entry.size = info.Size()
			contentBytes += entry.size
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}

	target := targetBytes
	if minTarget := (contentBytes + layerSplitMaxLayers - 1) / layerSplitMaxLayers; minTarget > target {
		target = minTarget
	}
	// Assign each entry to a layer. Non-files pin to layer 0; files fill
	// layers greedily. Hard-linked inodes stay together with their first
	// occurrence so links resolve within the layer.
	type inode struct{ dev, ino uint64 }
	inodeLayer := map[inode]int{}
	assignment := make([]int, len(entries))
	current, filled := 0, int64(0)
	for i, entry := range entries {
		if !entry.info.Mode().IsRegular() {
			assignment[i] = 0
			continue
		}
		st, _ := entry.info.Sys().(*syscall.Stat_t)
		if st != nil && st.Nlink > 1 {
			key := inode{uint64(st.Dev), uint64(st.Ino)}
			if layer, seen := inodeLayer[key]; seen {
				assignment[i] = layer
				continue
			}
		}
		if filled > 0 && filled+entry.size > target {
			current++
			filled = 0
		}
		filled += entry.size
		assignment[i] = current
		if st != nil && st.Nlink > 1 {
			inodeLayer[inode{uint64(st.Dev), uint64(st.Ino)}] = current
		}
	}
	layerCount := current + 1

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	layers := make([]*packedLayer, layerCount)
	errs := make([]error, layerCount)
	var wg sync.WaitGroup
	for layer := 0; layer < layerCount; layer++ {
		wg.Add(1)
		go func(layer int) {
			defer wg.Done()
			var mine []overlayEntry
			for i, entry := range entries {
				if assignment[i] == layer {
					mine = append(mine, entry)
				}
			}
			layers[layer], errs[layer] = writeLayerTar(mine, filepath.Join(dir, fmt.Sprintf("layer-%d.tar.gz", layer)), mediaType, layerCount)
		}(layer)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return layers, nil
}

// writeLayerTar writes entries as one gzip'd layer tar, hashing the tar
// (diffID) and the gzip output (digest) as it goes.
func writeLayerTar(entries []overlayEntry, gzPath string, mediaType ggcrtypes.MediaType, concurrentLayers int) (*packedLayer, error) {
	out, err := os.Create(gzPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	compressedHash := sha256.New()
	gz, err := pgzip.NewWriterLevel(io.MultiWriter(out, compressedHash), pgzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	// pgzip parallelizes within a stream; when several layers already run
	// side by side, keep each one's worker count modest.
	workers := runtime.NumCPU() / concurrentLayers
	if workers < 2 {
		workers = 2
	}
	if err := gz.SetConcurrency(1<<20, workers); err != nil {
		return nil, err
	}
	diffHash := sha256.New()
	tw := tar.NewWriter(io.MultiWriter(gz, diffHash))

	type inode struct{ dev, ino uint64 }
	linked := map[inode]string{}
	var contentBytes int64

	for _, entry := range entries {
		info, st := entry.info, (*syscall.Stat_t)(nil)
		st, _ = info.Sys().(*syscall.Stat_t)

		if info.Mode()&os.ModeCharDevice != 0 && st != nil && st.Rdev == 0 {
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     filepath.ToSlash(filepath.Join(filepath.Dir(entry.rel), whiteoutPrefix+filepath.Base(entry.rel))),
				ModTime:  info.ModTime(),
			}); err != nil {
				return nil, err
			}
			continue
		}

		hdr, err := tar.FileInfoHeader(info, entry.link)
		if err != nil {
			return nil, err
		}
		hdr.Name = entry.rel
		hdr.Xattrs, hdr.PAXRecords = nil, nil
		if info.IsDir() {
			hdr.Name += "/"
		}
		if hdr.Typeflag == tar.TypeReg && st != nil && st.Nlink > 1 {
			key := inode{uint64(st.Dev), uint64(st.Ino)}
			if target, seen := linked[key]; seen {
				hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeLink, target, 0
			} else {
				linked[key] = entry.rel
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}

		switch {
		case hdr.Typeflag == tar.TypeReg && hdr.Size > 0:
			if err := copyFixedSize(tw, entry.path, hdr.Size); err != nil {
				return nil, err
			}
			contentBytes += hdr.Size
		case info.IsDir():
			if opaque, _ := readXattr(entry.path, overlayOpaqueXattr); opaque == "y" {
				if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: entry.rel + "/" + opaqueWhiteout, ModTime: info.ModTime()}); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	stat, err := out.Stat()
	if err != nil {
		return nil, err
	}
	return &packedLayer{
		path:         gzPath,
		digest:       v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(compressedHash.Sum(nil))},
		diffID:       v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(diffHash.Sum(nil))},
		size:         stat.Size(),
		contentBytes: contentBytes,
		mediaType:    mediaType,
	}, nil
}

// copyFixedSize writes exactly size bytes of path to w, zero-padding if the
// file has shrunk since it was measured.
func copyFixedSize(w io.Writer, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := io.CopyN(w, f, size)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < size {
		_, err = io.CopyN(w, zeroReader{}, size-n)
	}
	return err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func readXattr(path, attr string) (string, error) {
	buf := make([]byte, 64)
	n, err := unix.Lgetxattr(path, attr, buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// layerMediaTypeFor picks the layer media type matching an image manifest.
func layerMediaTypeFor(img v1.Image) ggcrtypes.MediaType {
	if mediaType, _ := img.MediaType(); mediaType == ggcrtypes.OCIManifestSchema1 {
		return ggcrtypes.OCILayer
	}
	return ggcrtypes.DockerLayer
}

// writeSparseOCILayout lays img out as an OCI image layout that carries only
// the manifest, the config and the given layers' compressed blobs; any other
// layer of img is left out, to be served from an index cache.
func writeSparseOCILayout(dir string, img v1.Image, layers []*packedLayer) error {
	blobs := filepath.Join(dir, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0o755); err != nil {
		return err
	}
	writeBlob := func(data []byte) (v1.Hash, error) {
		hash, _, err := v1.SHA256(bytes.NewReader(data))
		if err != nil {
			return hash, err
		}
		return hash, os.WriteFile(filepath.Join(blobs, hash.Hex), data, 0o644)
	}
	manifest, err := img.RawManifest()
	if err != nil {
		return err
	}
	manifestDigest, err := writeBlob(manifest)
	if err != nil {
		return err
	}
	config, err := img.RawConfigFile()
	if err != nil {
		return err
	}
	if _, err := writeBlob(config); err != nil {
		return err
	}
	for _, layer := range layers {
		if err := os.Link(layer.path, filepath.Join(blobs, layer.digest.Hex)); err != nil {
			return err
		}
	}
	mediaType, err := img.MediaType()
	if err != nil {
		return err
	}
	index, err := json.Marshal(v1.IndexManifest{
		SchemaVersion: 2,
		MediaType:     ggcrtypes.OCIImageIndex,
		Manifests: []v1.Descriptor{{
			MediaType: mediaType,
			Size:      int64(len(manifest)),
			Digest:    manifestDigest,
			Platform:  &v1.Platform{OS: "linux", Architecture: runtime.GOARCH},
		}},
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "index.json"), index, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644)
}

// layerStats summarizes packed layers for logging.
func layerStats(layers []*packedLayer) (compressed, content int64, sizes []int64) {
	for _, layer := range layers {
		compressed += layer.size
		content += layer.contentBytes
		sizes = append(sizes, layer.size)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] > sizes[j] })
	return compressed, content, sizes
}
