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
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/pgzip"
	"github.com/rs/zerolog/log"
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
// that shrinks meanwhile is zero-padded and one that grows is truncated. A
// file the sandbox removes or replaces between the walk and the read is left
// out of the layer (see upperOpener).
//
// Directories, whiteouts and symlinks all go in the first layer; regular
// files are dealt across layers in walk order so each holds about
// layerSplitTargetBytes of content. A hard link whose target landed in another
// layer is written as a full copy, since tar links only reach within a layer.
// The layers are written concurrently.
func packOverlayLayers(upperDir, dir string, mediaType ggcrtypes.MediaType) ([]*packedLayer, error) {
	return packOverlayLayersWithTarget(upperDir, dir, mediaType, layerSplitTargetBytes)
}

// inode identifies a file across its hard links.
type inode struct{ dev, ino uint64 }

// hardLinkInode returns the inode of a regular file that has other links, and
// false for anything else.
func hardLinkInode(info os.FileInfo) (inode, bool) {
	st, _ := info.Sys().(*syscall.Stat_t)
	if st == nil || st.Nlink <= 1 || !info.Mode().IsRegular() {
		return inode{}, false
	}
	return inode{uint64(st.Dev), uint64(st.Ino)}, true
}

func packOverlayLayersWithTarget(upperDir, dir string, mediaType ggcrtypes.MediaType, targetBytes int64) ([]*packedLayer, error) {
	var entries []overlayEntry
	var contentBytes int64
	seenInodes := map[inode]struct{}{}
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
			// Only the first link of an inode carries content; the others
			// become empty link entries.
			key, linked := hardLinkInode(info)
			if _, seen := seenInodes[key]; !linked || !seen {
				contentBytes += entry.size
			}
			if linked {
				seenInodes[key] = struct{}{}
			}
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Assign each entry to a layer. Non-files pin to layer 0; files fill
	// layers greedily. Hard-linked inodes stay together with their first
	// occurrence so links resolve within the layer. Greedy filling can leave
	// layers short (a file that does not fit starts the next one), so the
	// target grows until the layer cap holds.
	target := max(targetBytes, 1)
	if minTarget := (contentBytes + layerSplitMaxLayers - 1) / layerSplitMaxLayers; minTarget > target {
		target = minTarget
	}
	assignment, layerCount := assignLayers(entries, target)
	for layerCount > layerSplitMaxLayers {
		target *= 2
		assignment, layerCount = assignLayers(entries, target)
	}

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
			layers[layer], errs[layer] = writeLayerTar(upperDir, mine, filepath.Join(dir, fmt.Sprintf("layer-%d.tar.gz", layer)), mediaType, layerCount)
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

// assignLayers deals entries into layers of about target bytes of file
// content each, returning each entry's layer and the number of layers.
func assignLayers(entries []overlayEntry, target int64) ([]int, int) {
	inodeLayer := map[inode]int{}
	assignment := make([]int, len(entries))
	current, filled := 0, int64(0)
	for i, entry := range entries {
		if !entry.info.Mode().IsRegular() {
			assignment[i] = 0
			continue
		}
		key, linked := hardLinkInode(entry.info)
		if linked {
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
		if linked {
			inodeLayer[key] = current
		}
	}
	return assignment, current + 1
}

// writeLayerTar writes entries as one gzip'd layer tar, hashing the tar
// (diffID) and the gzip output (digest) as it goes. Regular files are opened
// through an upperOpener rooted at upperDir; an entry that has vanished or is
// no longer the file the walk saw is skipped, header and all.
func writeLayerTar(upperDir string, entries []overlayEntry, gzPath string, mediaType ggcrtypes.MediaType, concurrentLayers int) (*packedLayer, error) {
	opener, err := newUpperOpener(upperDir)
	if err != nil {
		return nil, err
	}
	defer opener.close()

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

	linked := map[inode]string{}
	var contentBytes int64

	for _, entry := range entries {
		info := entry.info
		st, _ := info.Sys().(*syscall.Stat_t)

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
		if info.IsDir() {
			hdr.Name += "/"
		}

		// A regular file is opened, and checked to still be the file the
		// walk saw, before its header goes out, so a skipped entry never
		// leaves a header without content. Only the first link of an inode
		// that could be opened carries the content: if the sandbox removed
		// that one, the next link becomes the carrier instead of pointing
		// at an entry the layer does not have.
		var content *os.File
		if hdr.Typeflag == tar.TypeReg {
			key, hardLinked := hardLinkInode(info)
			if target, seen := linked[key]; hardLinked && seen {
				hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeLink, target, 0
			} else {
				content, err = opener.open(entry.rel, st)
				if err != nil {
					log.Debug().Err(err).Str("path", entry.rel).Msg("upper dir entry changed since the walk, left out of the layer")
					continue
				}
				if hardLinked {
					linked[key] = entry.rel
				}
			}
		}
		if content != nil {
			hdr.PAXRecords = fdXattrRecords(int(content.Fd()))
		} else {
			hdr.PAXRecords = fileXattrRecords(entry.path)
		}
		if err := tw.WriteHeader(hdr); err != nil {
			content.Close()
			return nil, err
		}

		switch {
		case content != nil:
			err := copyFixedSize(tw, content, hdr.Size)
			content.Close()
			if err != nil {
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

// copyFixedSize writes exactly size bytes of r to w, zero-padding if the
// file has shrunk since it was measured.
func copyFixedSize(w io.Writer, r io.Reader, size int64) error {
	n, err := io.CopyN(w, r, size)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < size {
		_, err = io.CopyN(w, zeroReader{}, size-n)
	}
	return err
}

// errUpperEntryChanged reports that what is at a path is no longer the regular
// file the walk recorded there.
var errUpperEntryChanged = errors.New("entry is not the regular file seen by the walk")

// upperOpener opens regular files in a live sandbox's upper directory without
// trusting the path. The sandbox writes there while the layer is packed, so
// between the walk's lstat and the open it can swap a file, or any directory
// on the way to it, for a symlink; os.Open would follow that to wherever on
// the worker host it points and the snapshot would carry the target's bytes.
// Every path component is instead opened relative to its parent's descriptor
// with O_NOFOLLOW, and the opened file is fstat'd and required to be a
// regular file with the walk's device and inode. The directory descriptor of
// the most recent entry is kept, since the walk lists a directory's files
// together.
type upperOpener struct {
	rootFd int
	dirRel string
	dirFd  int
}

func newUpperOpener(upperDir string) (*upperOpener, error) {
	fd, err := unix.Open(upperDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: upperDir, Err: err}
	}
	return &upperOpener{rootFd: fd, dirFd: -1}, nil
}

func (o *upperOpener) close() {
	if o.dirFd >= 0 {
		unix.Close(o.dirFd)
	}
	unix.Close(o.rootFd)
}

// dir returns a descriptor for the upper-relative directory rel ("" is the
// root), opening one component at a time with O_NOFOLLOW.
func (o *upperOpener) dir(rel string) (int, error) {
	if rel == "" {
		return o.rootFd, nil
	}
	if rel == o.dirRel && o.dirFd >= 0 {
		return o.dirFd, nil
	}
	fd := o.rootFd
	for _, component := range strings.Split(rel, "/") {
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if fd != o.rootFd {
			unix.Close(fd)
		}
		if err != nil {
			return -1, &os.PathError{Op: "openat", Path: rel, Err: err}
		}
		fd = next
	}
	if o.dirFd >= 0 {
		unix.Close(o.dirFd)
	}
	o.dirRel, o.dirFd = rel, fd
	return fd, nil
}

// open opens the regular file at the upper-relative path rel and verifies it
// is still the inode the walk lstat'd as want.
func (o *upperOpener) open(rel string, want *syscall.Stat_t) (*os.File, error) {
	if want == nil {
		return nil, &os.PathError{Op: "open", Path: rel, Err: errUpperEntryChanged}
	}
	dirRel, base := path.Split(rel)
	dirFd, err := o.dir(strings.TrimSuffix(dirRel, "/"))
	if err != nil {
		return nil, err
	}
	// O_NONBLOCK so that a FIFO put in the file's place does not hang the
	// open; it is cleared once the file has passed the check.
	fd, err := unix.Openat(dirFd, base, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: rel, Err: err}
	}
	var got unix.Stat_t
	if err := unix.Fstat(fd, &got); err != nil {
		unix.Close(fd)
		return nil, &os.PathError{Op: "fstat", Path: rel, Err: err}
	}
	if got.Mode&unix.S_IFMT != unix.S_IFREG || got.Ino != uint64(want.Ino) || uint64(got.Dev) != uint64(want.Dev) {
		unix.Close(fd)
		return nil, &os.PathError{Op: "open", Path: rel, Err: errUpperEntryChanged}
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		unix.Close(fd)
		return nil, &os.PathError{Op: "open", Path: rel, Err: err}
	}
	return os.NewFile(uintptr(fd), rel), nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// xattrFuncs lists and reads the attributes of one file, by path (without
// following a final symlink) or by descriptor.
type xattrFuncs struct {
	list func(buf []byte) (int, error)
	get  func(attr string, buf []byte) (int, error)
}

func pathXattrs(path string) xattrFuncs {
	return xattrFuncs{
		list: func(buf []byte) (int, error) { return unix.Llistxattr(path, buf) },
		get:  func(attr string, buf []byte) (int, error) { return unix.Lgetxattr(path, attr, buf) },
	}
}

func fdXattrs(fd int) xattrFuncs {
	return xattrFuncs{
		list: func(buf []byte) (int, error) { return unix.Flistxattr(fd, buf) },
		get:  func(attr string, buf []byte) (int, error) { return unix.Fgetxattr(fd, attr, buf) },
	}
}

func readXattr(path, attr string) (string, error) {
	return pathXattrs(path).read(attr)
}

func (x xattrFuncs) read(attr string) (string, error) {
	buf := make([]byte, 64)
	for {
		n, err := x.get(attr, buf)
		if err == unix.ERANGE {
			buf = make([]byte, 2*len(buf))
			continue
		}
		if err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	}
}

// skippedXattrPrefixes are attributes that describe the overlay or the
// worker host rather than the file: overlayfs bookkeeping on the upper
// directory (opaque, redirect, origin, impure, nlink, metacopy, whiteout...)
// and the host's SELinux label.
var skippedXattrPrefixes = []string{"trusted.overlay.", "user.overlay.", "security.selinux"}

// fileXattrRecords returns the file's extended attributes as PAX records
// (SCHILY.xattr.<name>, the form Docker and containers/storage use), so file
// capabilities and the like survive into the image. Nothing is returned for
// files without attributes or on filesystems without them.
func fileXattrRecords(path string) map[string]string {
	return pathXattrs(path).records()
}

// fdXattrRecords is fileXattrRecords for an open file, so the attributes are
// read from the file that was verified rather than from whatever its path
// resolves to now.
func fdXattrRecords(fd int) map[string]string {
	return fdXattrs(fd).records()
}

func (x xattrFuncs) records() map[string]string {
	buf := make([]byte, 256)
	var n int
	for {
		var err error
		if n, err = x.list(buf); err == unix.ERANGE {
			buf = make([]byte, 2*len(buf))
			continue
		} else if err != nil || n == 0 {
			return nil
		}
		break
	}
	var records map[string]string
	for _, name := range bytes.Split(bytes.TrimRight(buf[:n], "\x00"), []byte{0}) {
		if len(name) == 0 || skippedXattr(string(name)) {
			continue
		}
		value, err := x.read(string(name))
		if err != nil {
			continue
		}
		if records == nil {
			records = map[string]string{}
		}
		records["SCHILY.xattr."+string(name)] = value
	}
	return records
}

func skippedXattr(name string) bool {
	for _, prefix := range skippedXattrPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
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
