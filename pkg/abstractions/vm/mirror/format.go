// Package mirror holds a laptop's files for VMs: the store format
// (content-addressed objects and the per-generation manifest that maps a
// home-relative file tree onto them), the gateway API that uploads and
// retires generations, and the read-only filesystem a worker mounts under a
// VM's home. docs/mirror/README.md is the specification; the vectors under
// testdata are shared with the Rust client.
package mirror

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	Magic   = "BMIRROR\x01"
	Version = 1

	CodecRaw  = 0
	CodecZstd = 1

	KindDir     = 1
	KindFile    = 2
	KindSymlink = 3

	MaxEntries      = 1 << 23
	MaxNameLen      = 255
	MaxPathLen      = 4095
	MaxTargetLen    = 4095
	MaxMetaLen      = 64 << 10
	MaxExtentLen    = 16 << 20
	MaxObjectSize   = 64 << 20
	MaxManifestSize = 1 << 30

	envelopeSize = 24
	headerSize   = 64
	objectSize   = 40
	entrySize    = 40
	extentSize   = 16
)

var ErrInvalidManifest = errors.New("invalid mirror manifest")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidManifest, fmt.Sprintf(format, args...))
}

// Digest is the SHA-256 of an object's stored bytes, which is also its name.
type Digest [sha256.Size]byte

func (d Digest) String() string { return hex.EncodeToString(d[:]) }

func ParseDigest(s string) (Digest, error) {
	var d Digest
	if len(s) != hex.EncodedLen(len(d)) || strings.ToLower(s) != s {
		return d, fmt.Errorf("invalid object digest %q", s)
	}
	if _, err := hex.Decode(d[:], []byte(s)); err != nil {
		return d, fmt.Errorf("invalid object digest %q", s)
	}
	return d, nil
}

func DigestOf(data []byte) Digest { return sha256.Sum256(data) }

type Object struct {
	Digest Digest
	Size   uint64
}

// Extent is one run of a file: StoredLen bytes at Offset of an object,
// holding RawLen bytes of the file. The run is compressed exactly when
// StoredLen < RawLen.
type Extent struct {
	Object    uint32
	Offset    uint32
	StoredLen uint32
	RawLen    uint32
}

func (e Extent) Compressed() bool { return e.StoredLen < e.RawLen }

// Entry is one node of the tree. First and Count address a directory's
// children in the entry table or a file's extents in the extent table.
type Entry struct {
	Kind    uint8
	Mode    uint32
	Size    uint64
	MtimeNs int64
	First   uint32
	Count   uint32
}

// Manifest is a validated generation manifest. It keeps the decoded body and
// reads records in place, so it costs the body's size and nothing per entry.
type Manifest struct {
	body        []byte
	digest      Digest
	storedSize  int
	compression uint32
	totalSize   uint64

	entries, objects, extents uint32
	meta                      []byte
	objectTable               []byte
	entryTable                []byte
	extentTable               []byte
	names                     []byte
}

var (
	// Decoding stops at the capacity of the buffer it is given, so a frame
	// costs no more memory than the size its manifest declares for it.
	zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(MaxManifestSize), zstd.WithDecodeAllCapLimit(true))
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
)

// decompress decodes one zstd frame that must hold exactly size bytes.
func decompress(stored []byte, size uint64) ([]byte, error) {
	decoded, err := zstdDecoder.DecodeAll(stored, make([]byte, 0, size))
	if err != nil {
		return nil, err
	}
	if uint64(len(decoded)) != size {
		return nil, fmt.Errorf("decoded to %d bytes, want %d", len(decoded), size)
	}
	return decoded, nil
}

// BodySize is the decoded size a stored manifest declares: what parsing it
// allocates and what a reader holds while it is mounted. Zero if stored is
// not a manifest.
func BodySize(stored []byte) uint64 {
	if len(stored) < envelopeSize || string(stored[:8]) != Magic {
		return 0
	}
	return binary.LittleEndian.Uint64(stored[16:])
}

// Parse decodes and fully validates a stored manifest. A manifest that
// parses can be served to a guest as is: every path stays inside the tree,
// every extent stays inside its object, and nothing but directories, regular
// files and in-tree symlinks is present.
func Parse(stored []byte) (*Manifest, error) {
	if len(stored) < envelopeSize || string(stored[:8]) != Magic {
		return nil, invalid("bad magic")
	}
	codec := binary.LittleEndian.Uint32(stored[8:])
	bodyLen := binary.LittleEndian.Uint64(stored[16:])
	if binary.LittleEndian.Uint32(stored[12:]) != 0 {
		return nil, invalid("reserved envelope bytes are set")
	}
	if bodyLen < headerSize || bodyLen > MaxManifestSize {
		return nil, invalid("body length %d out of range", bodyLen)
	}
	var body []byte
	switch codec {
	case CodecRaw:
		body = stored[envelopeSize:]
	case CodecZstd:
		decoded, err := decompress(stored[envelopeSize:], bodyLen)
		if err != nil {
			return nil, invalid("decompress body: %v", err)
		}
		body = decoded
	default:
		return nil, invalid("unknown envelope codec %d", codec)
	}
	if uint64(len(body)) != bodyLen {
		return nil, invalid("body is %d bytes, envelope says %d", len(body), bodyLen)
	}

	m := &Manifest{body: body, digest: DigestOf(stored), storedSize: len(stored)}
	if version := binary.LittleEndian.Uint32(body[0:]); version != Version {
		return nil, invalid("unsupported version %d", version)
	}
	m.compression = binary.LittleEndian.Uint32(body[4:])
	m.entries = binary.LittleEndian.Uint32(body[8:])
	m.objects = binary.LittleEndian.Uint32(body[12:])
	m.extents = binary.LittleEndian.Uint32(body[16:])
	metaLen := uint64(binary.LittleEndian.Uint32(body[20:]))
	namesLen := binary.LittleEndian.Uint64(body[24:])
	m.totalSize = binary.LittleEndian.Uint64(body[32:])
	for _, b := range body[40:headerSize] {
		if b != 0 {
			return nil, invalid("reserved header bytes are set")
		}
	}
	if m.compression != CodecZstd {
		return nil, invalid("unknown extent compression %d", m.compression)
	}
	if m.entries == 0 || m.entries > MaxEntries {
		return nil, invalid("entry count %d out of range", m.entries)
	}
	if metaLen > MaxMetaLen || namesLen > bodyLen {
		return nil, invalid("section length out of range")
	}
	want := headerSize + metaLen + uint64(m.objects)*objectSize + uint64(m.entries)*entrySize + uint64(m.extents)*extentSize + namesLen
	if want != bodyLen {
		return nil, invalid("sections cover %d bytes of a %d byte body", want, bodyLen)
	}
	rest := body[headerSize:]
	take := func(n uint64) []byte {
		section := rest[:n:n]
		rest = rest[n:]
		return section
	}
	m.meta = take(metaLen)
	m.objectTable = take(uint64(m.objects) * objectSize)
	m.entryTable = take(uint64(m.entries) * entrySize)
	m.extentTable = take(uint64(m.extents) * extentSize)
	m.names = take(namesLen)
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manifest) Digest() Digest    { return m.digest }
func (m *Manifest) StoredSize() int   { return m.storedSize }
func (m *Manifest) Entries() uint32   { return m.entries }
func (m *Manifest) Objects() uint32   { return m.objects }
func (m *Manifest) Extents() uint32   { return m.extents }
func (m *Manifest) TotalSize() uint64 { return m.totalSize }
func (m *Manifest) Meta() []byte      { return m.meta }
func (m *Manifest) IndexBytes() int   { return len(m.body) }
func (m *Manifest) Root() uint32      { return 0 }
func (m *Manifest) record(i uint32) []byte {
	return m.entryTable[uint64(i)*entrySize : uint64(i+1)*entrySize]
}

func (m *Manifest) Entry(i uint32) Entry {
	r := m.record(i)
	return Entry{
		Size:    binary.LittleEndian.Uint64(r[0:]),
		MtimeNs: int64(binary.LittleEndian.Uint64(r[8:])),
		First:   binary.LittleEndian.Uint32(r[16:]),
		Count:   binary.LittleEndian.Uint32(r[20:]),
		Kind:    r[30],
		Mode:    binary.LittleEndian.Uint32(r[32:]),
	}
}

func (m *Manifest) Kind(i uint32) uint8 { return m.record(i)[30] }

// Name returns the entry's name within its directory. It aliases the
// manifest and must not be modified.
func (m *Manifest) Name(i uint32) []byte {
	r := m.record(i)
	off := uint64(binary.LittleEndian.Uint32(r[24:]))
	return m.names[off : off+uint64(binary.LittleEndian.Uint16(r[28:]))]
}

// Target returns a symlink's target.
func (m *Manifest) Target(i uint32) []byte {
	entry := m.Entry(i)
	return m.names[uint64(entry.First) : uint64(entry.First)+uint64(entry.Count)]
}

func (m *Manifest) Object(i uint32) Object {
	r := m.objectTable[uint64(i)*objectSize:]
	var object Object
	copy(object.Digest[:], r[:sha256.Size])
	object.Size = binary.LittleEndian.Uint64(r[sha256.Size:])
	return object
}

func (m *Manifest) Extent(i uint32) Extent {
	r := m.extentTable[uint64(i)*extentSize:]
	return Extent{
		Object:    binary.LittleEndian.Uint32(r[0:]),
		Offset:    binary.LittleEndian.Uint32(r[4:]),
		StoredLen: binary.LittleEndian.Uint32(r[8:]),
		RawLen:    binary.LittleEndian.Uint32(r[12:]),
	}
}

// Lookup finds name among the children of directory dir.
func (m *Manifest) Lookup(dir uint32, name string) (uint32, bool) {
	entry := m.Entry(dir)
	if entry.Kind != KindDir {
		return 0, false
	}
	low, high := entry.First, entry.First+entry.Count
	for low < high {
		mid := low + (high-low)/2
		switch compareName(m.Name(mid), name) {
		case 0:
			return mid, true
		case -1:
			low = mid + 1
		default:
			high = mid
		}
	}
	return 0, false
}

func compareName(a []byte, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// Find resolves a slash-separated path relative to the root without
// following symlinks. The empty path is the root.
func (m *Manifest) Find(path string) (uint32, bool) {
	index := uint32(0)
	for _, name := range strings.Split(path, "/") {
		if name == "" {
			continue
		}
		child, ok := m.Lookup(index, name)
		if !ok {
			return 0, false
		}
		index = child
	}
	return index, true
}

// Walk visits every entry below the root, parents before children, with its
// path relative to the root.
func (m *Manifest) Walk(visit func(path string, index uint32, entry Entry) error) error {
	paths := make(map[uint32]string, 64)
	paths[0] = ""
	for dir := uint32(0); dir < m.entries; dir++ {
		entry := m.Entry(dir)
		if entry.Kind != KindDir {
			continue
		}
		prefix := paths[dir]
		delete(paths, dir)
		if prefix != "" {
			prefix += "/"
		}
		for child := entry.First; child < entry.First+entry.Count; child++ {
			path := prefix + string(m.Name(child))
			childEntry := m.Entry(child)
			if childEntry.Kind == KindDir {
				paths[child] = path
			}
			if err := visit(path, child, childEntry); err != nil {
				return err
			}
		}
	}
	return nil
}

func validName(name []byte) bool {
	if len(name) == 0 || len(name) > MaxNameLen || !utf8.Valid(name) {
		return false
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return false
	}
	return string(name) != "." && string(name) != ".."
}

// targetStaysInside reports whether a symlink in a directory depth levels
// below the root resolves, lexically, to a path inside the tree.
func targetStaysInside(target []byte, depth int) bool {
	if len(target) == 0 || len(target) > MaxTargetLen || target[0] == '/' {
		return false
	}
	if !utf8.Valid(target) || bytes.IndexByte(target, 0) >= 0 {
		return false
	}
	for _, part := range bytes.Split(target, []byte{'/'}) {
		switch string(part) {
		case "", ".":
		case "..":
			if depth--; depth < 0 {
				return false
			}
		default:
			depth++
		}
	}
	return true
}

func (m *Manifest) validate() error {
	var previous Digest
	for i := range m.objects {
		object := m.Object(i)
		if i > 0 && bytes.Compare(previous[:], object.Digest[:]) >= 0 {
			return invalid("object table is not sorted and unique at %d", i)
		}
		if object.Size == 0 || object.Size > MaxObjectSize {
			return invalid("object %s has size %d", object.Digest, object.Size)
		}
		previous = object.Digest
	}

	root := m.record(0)
	if root[30] != KindDir || binary.LittleEndian.Uint16(root[28:]) != 0 || binary.LittleEndian.Uint32(root[24:]) != 0 {
		return invalid("entry 0 is not an unnamed directory")
	}
	if err := m.validateRecord(0, m.Entry(0)); err != nil {
		return err
	}

	// A directory's children are the next unclaimed run of the entry table,
	// so the directories tile it exactly: every entry has one parent that
	// precedes it, and the table holds a tree and nothing else.
	type position struct{ pathLen, depth uint16 }
	positions := make([]position, m.entries)
	used := make([]bool, m.objects)
	nextEntry, nextExtent, nextName, total := uint32(1), uint32(0), uint64(0), uint64(0)
	for dir := range m.entries {
		entry := m.Entry(dir)
		if entry.Kind != KindDir {
			continue
		}
		if dir >= nextEntry {
			// Nothing before it claimed it, so the run it claims would
			// start at itself.
			return invalid("directory %d is claimed by no directory before it", dir)
		}
		if entry.First != nextEntry || entry.Count > m.entries-nextEntry {
			return invalid("directory %d does not claim the next run of entries", dir)
		}
		at := positions[dir]
		var previousName []byte
		for child := entry.First; child < entry.First+entry.Count; child++ {
			record := m.record(child)
			nameLen := uint64(binary.LittleEndian.Uint16(record[28:]))
			if uint64(binary.LittleEndian.Uint32(record[24:])) != nextName || nextName+nameLen > uint64(len(m.names)) {
				return invalid("entry %d name is out of place", child)
			}
			name := m.names[nextName : nextName+nameLen]
			nextName += nameLen
			if !validName(name) {
				return invalid("entry %d has an invalid name", child)
			}
			if previousName != nil && bytes.Compare(previousName, name) >= 0 {
				return invalid("children of directory %d are not sorted and unique", dir)
			}
			previousName = name
			pathLen := int(at.pathLen) + len(name)
			if at.pathLen > 0 {
				pathLen++
			}
			if pathLen > MaxPathLen {
				return invalid("path of entry %d is too long", child)
			}
			positions[child] = position{pathLen: uint16(pathLen), depth: at.depth + 1}

			childEntry := m.Entry(child)
			if err := m.validateRecord(child, childEntry); err != nil {
				return err
			}
			switch childEntry.Kind {
			case KindFile:
				if childEntry.Count == 0 {
					if childEntry.First != 0 || childEntry.Size != 0 {
						return invalid("empty file %d has extents or a size", child)
					}
					continue
				}
				if childEntry.First != nextExtent || childEntry.Count > m.extents-nextExtent {
					return invalid("file %d does not claim the next run of extents", child)
				}
				var size uint64
				for i := childEntry.First; i < childEntry.First+childEntry.Count; i++ {
					extent := m.Extent(i)
					if extent.Object >= m.objects {
						return invalid("extent %d names object %d of %d", i, extent.Object, m.objects)
					}
					if extent.RawLen == 0 || extent.RawLen > MaxExtentLen || extent.StoredLen == 0 || extent.StoredLen > extent.RawLen {
						return invalid("extent %d has lengths %d/%d", i, extent.StoredLen, extent.RawLen)
					}
					if uint64(extent.Offset)+uint64(extent.StoredLen) > m.Object(extent.Object).Size {
						return invalid("extent %d runs past the end of its object", i)
					}
					used[extent.Object] = true
					size += uint64(extent.RawLen)
				}
				if size != childEntry.Size {
					return invalid("file %d is %d bytes but its extents hold %d", child, childEntry.Size, size)
				}
				nextExtent += childEntry.Count
				total += size
			case KindSymlink:
				if uint64(childEntry.First) != nextName || nextName+uint64(childEntry.Count) > uint64(len(m.names)) || childEntry.Size != uint64(childEntry.Count) {
					return invalid("symlink %d target is out of place", child)
				}
				target := m.names[nextName : nextName+uint64(childEntry.Count)]
				nextName += uint64(childEntry.Count)
				if !targetStaysInside(target, int(at.depth)) {
					return invalid("symlink %d points outside the tree", child)
				}
			}
		}
		nextEntry += entry.Count
	}
	if nextEntry != m.entries {
		return invalid("%d entries are not reachable from the root", m.entries-nextEntry)
	}
	if nextExtent != m.extents {
		return invalid("%d extents belong to no file", m.extents-nextExtent)
	}
	if nextName != uint64(len(m.names)) {
		return invalid("name table has %d unused bytes", uint64(len(m.names))-nextName)
	}
	if total != m.totalSize {
		return invalid("files hold %d bytes, header says %d", total, m.totalSize)
	}
	for i, ok := range used {
		if !ok {
			return invalid("object %d is referenced by no extent", i)
		}
	}
	return nil
}

func (m *Manifest) validateRecord(index uint32, entry Entry) error {
	record := m.record(index)
	if record[31] != 0 || binary.LittleEndian.Uint32(record[36:]) != 0 {
		return invalid("entry %d has reserved bytes set", index)
	}
	switch entry.Kind {
	case KindDir:
		if entry.Size != 0 || entry.Mode > 0o777 {
			return invalid("directory %d has a size or mode bits beyond 0777", index)
		}
	case KindFile:
		if entry.Mode > 0o777 {
			return invalid("file %d has mode bits beyond 0777", index)
		}
	case KindSymlink:
		if entry.Mode != 0o777 {
			return invalid("symlink %d has mode %o", index, entry.Mode)
		}
	default:
		return invalid("entry %d has unknown kind %d", index, entry.Kind)
	}
	return nil
}

// homeName is a conservative account name. It cannot begin with a dot, so
// it is never . or ..
var homeName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,31}$`)

// ValidHome reports whether home is a home directory a tree may be presented
// at: /Users/<name> or /home/<name>, clean and absolute. A manifest's home is
// written by a client, so nothing else is taken.
func ValidHome(home string) bool {
	parent, name := path.Split(home)
	return (parent == "/Users/" || parent == "/home/") && homeName.MatchString(name) && path.Clean(home) == home
}

// SourceHome is the home directory meta records the files came from, or
// empty when meta records none that ValidHome accepts. Meta is the client's
// to fill: malformed meta has no home rather than an error.
func SourceHome(meta []byte) string {
	var decoded struct {
		Source struct {
			Home string `json:"home"`
		} `json:"source"`
	}
	if json.Unmarshal(meta, &decoded) != nil || !ValidHome(decoded.Source.Home) {
		return ""
	}
	return decoded.Source.Home
}

// FileExtent is an Extent that names its object directly, for building.
type FileExtent struct {
	Object    Object
	Offset    uint32
	StoredLen uint32
	RawLen    uint32
}

type buildNode struct {
	name     string
	kind     uint8
	mode     uint32
	mtimeNs  int64
	size     uint64
	target   string
	extents  []FileExtent
	children map[string]*buildNode
	index    uint32
}

// Builder assembles a tree and encodes it as a manifest. The encoding is
// canonical: the same tree always produces the same bytes, whichever writer
// produced it.
type Builder struct {
	root    *buildNode
	entries int
}

func NewBuilder() *Builder {
	return &Builder{
		root:    &buildNode{kind: KindDir, mode: 0o755, children: map[string]*buildNode{}},
		entries: 1,
	}
}

// SetRoot sets the mode and mtime of the root directory.
func (b *Builder) SetRoot(mode uint32, mtimeNs int64) {
	b.root.mode, b.root.mtimeNs = mode&0o777, mtimeNs
}

func (b *Builder) add(path string, node *buildNode) error {
	parts := strings.Split(path, "/")
	parent := b.root
	for _, part := range parts[:len(parts)-1] {
		next := parent.children[part]
		if next == nil || next.kind != KindDir {
			return fmt.Errorf("add %q: parent directory %q was not added", path, part)
		}
		parent = next
	}
	node.name = parts[len(parts)-1]
	if !validName([]byte(node.name)) {
		return fmt.Errorf("add %q: invalid name", path)
	}
	if _, exists := parent.children[node.name]; exists {
		return fmt.Errorf("add %q: already added", path)
	}
	if b.entries >= MaxEntries {
		return fmt.Errorf("add %q: manifest is full", path)
	}
	parent.children[node.name] = node
	b.entries++
	return nil
}

// AddDir adds a directory. Parents must be added before their children.
func (b *Builder) AddDir(path string, mode uint32, mtimeNs int64) error {
	return b.add(path, &buildNode{kind: KindDir, mode: mode & 0o777, mtimeNs: mtimeNs, children: map[string]*buildNode{}})
}

func (b *Builder) AddFile(path string, mode uint32, mtimeNs int64, extents []FileExtent) error {
	node := &buildNode{kind: KindFile, mode: mode & 0o777, mtimeNs: mtimeNs, extents: extents}
	for _, extent := range extents {
		node.size += uint64(extent.RawLen)
	}
	return b.add(path, node)
}

func (b *Builder) AddSymlink(path, target string, mtimeNs int64) error {
	return b.add(path, &buildNode{kind: KindSymlink, mode: 0o777, mtimeNs: mtimeNs, target: target, size: uint64(len(target))})
}

// Encode returns the stored manifest, compressed unless raw is set, and its
// digest. meta is carried opaquely. The result is validated.
func (b *Builder) Encode(meta []byte, raw bool) ([]byte, *Manifest, error) {
	// Breadth first, children sorted by name: a directory's children are the
	// next contiguous run of entries.
	order := make([]*buildNode, 0, b.entries)
	order = append(order, b.root)
	sizes := map[Digest]uint64{}
	var extentCount, namesLen, total uint64
	for at := 0; at < len(order); at++ {
		node := order[at]
		node.index = uint32(at)
		namesLen += uint64(len(node.name) + len(node.target))
		for _, extent := range node.extents {
			if known, ok := sizes[extent.Object.Digest]; ok && known != extent.Object.Size {
				return nil, nil, fmt.Errorf("object %s has two sizes", extent.Object.Digest)
			}
			sizes[extent.Object.Digest] = extent.Object.Size
			total += uint64(extent.RawLen)
		}
		extentCount += uint64(len(node.extents))
		if node.kind != KindDir {
			continue
		}
		names := make([]string, 0, len(node.children))
		for name := range node.children {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			order = append(order, node.children[name])
		}
	}
	if extentCount > 1<<32-1 || namesLen > 1<<32-1 {
		return nil, nil, fmt.Errorf("manifest is too large")
	}

	digests := make([]Digest, 0, len(sizes))
	for digest := range sizes {
		digests = append(digests, digest)
	}
	sort.Slice(digests, func(i, j int) bool { return string(digests[i][:]) < string(digests[j][:]) })
	objectIndex := make(map[Digest]uint32, len(digests))
	for i, digest := range digests {
		objectIndex[digest] = uint32(i)
	}

	bodyLen := headerSize + uint64(len(meta)) + uint64(len(digests))*objectSize + uint64(len(order))*entrySize + extentCount*extentSize + namesLen
	out := make([]byte, envelopeSize, envelopeSize+bodyLen)
	copy(out, Magic)
	binary.LittleEndian.PutUint64(out[16:], bodyLen)

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:], Version)
	binary.LittleEndian.PutUint32(header[4:], CodecZstd)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(order)))
	binary.LittleEndian.PutUint32(header[12:], uint32(len(digests)))
	binary.LittleEndian.PutUint32(header[16:], uint32(extentCount))
	binary.LittleEndian.PutUint32(header[20:], uint32(len(meta)))
	binary.LittleEndian.PutUint64(header[24:], namesLen)
	binary.LittleEndian.PutUint64(header[32:], total)
	out = append(out, header...)
	out = append(out, meta...)
	for _, digest := range digests {
		out = append(out, digest[:]...)
		out = binary.LittleEndian.AppendUint64(out, sizes[digest])
	}

	// Children indexes follow from the order: each directory claims the run
	// after the previous directory's.
	extents := make([]byte, 0, extentCount*extentSize)
	names := make([]byte, 0, namesLen)
	nextChild, nextExtent := uint32(1), uint32(0)
	for _, node := range order {
		var first, count uint32
		nameOffset := uint32(len(names))
		names = append(names, node.name...)
		switch node.kind {
		case KindDir:
			first, count = nextChild, uint32(len(node.children))
			nextChild += count
		case KindFile:
			if len(node.extents) > 0 {
				first, count = nextExtent, uint32(len(node.extents))
				nextExtent += count
			}
			for _, extent := range node.extents {
				extents = binary.LittleEndian.AppendUint32(extents, objectIndex[extent.Object.Digest])
				extents = binary.LittleEndian.AppendUint32(extents, extent.Offset)
				extents = binary.LittleEndian.AppendUint32(extents, extent.StoredLen)
				extents = binary.LittleEndian.AppendUint32(extents, extent.RawLen)
			}
		case KindSymlink:
			first, count = uint32(len(names)), uint32(len(node.target))
			names = append(names, node.target...)
		}
		out = binary.LittleEndian.AppendUint64(out, node.size)
		out = binary.LittleEndian.AppendUint64(out, uint64(node.mtimeNs))
		out = binary.LittleEndian.AppendUint32(out, first)
		out = binary.LittleEndian.AppendUint32(out, count)
		out = binary.LittleEndian.AppendUint32(out, nameOffset)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(node.name)))
		out = append(out, node.kind, 0)
		out = binary.LittleEndian.AppendUint32(out, node.mode)
		out = binary.LittleEndian.AppendUint32(out, 0)
	}
	out = append(out, extents...)
	out = append(out, names...)

	if !raw {
		compressed := zstdEncoder.EncodeAll(out[envelopeSize:], make([]byte, envelopeSize, envelopeSize+len(out)/4))
		copy(compressed, out[:envelopeSize])
		binary.LittleEndian.PutUint32(compressed[8:], CodecZstd)
		out = compressed
	}
	manifest, err := Parse(out)
	if err != nil {
		return nil, nil, err
	}
	return out, manifest, nil
}

// ChunkParams are the content-defined chunking bounds. Avg must be a power
// of two.
type ChunkParams struct {
	Min, Avg, Max int
}

const gearSeed = 0x9E3779B97F4A7C15

var gearTable = func() (table [256]uint64) {
	seed := uint64(gearSeed)
	for i := range table {
		seed = seed*6364136223846793005 + 1442695040888963407
		table[i] = seed
	}
	return table
}()

// Cut returns the length of the first chunk of data, which must hold either
// the rest of the input or at least Max bytes. The first Min bytes are never
// hashed; the chunk ends after the first later byte at which the gear hash
// has its low log2(Avg) bits clear, or at Max.
func (p ChunkParams) Cut(data []byte) int {
	if len(data) <= p.Min {
		return len(data)
	}
	end := min(len(data), p.Max)
	mask := uint64(p.Avg) - 1
	var hash uint64
	for i := p.Min; i < end; i++ {
		hash = hash<<1 + gearTable[data[i]]
		if hash&mask == 0 {
			return i + 1
		}
	}
	return end
}

// WriterOptions are writer policy. None of it is visible to readers.
type WriterOptions struct {
	Chunk ChunkParams
	// Files up to PackLimit bytes are batched into pack objects of about
	// PackTarget stored bytes; larger files are chunked into objects of
	// their own.
	PackLimit  int
	PackTarget int
	// Compress stores an extent zstd-compressed when that saves at least
	// one part in eight.
	Compress bool
}

// DefaultWriterOptions are the parameters fixed by the format spike. A
// reader fetches whole objects, so reading one small file costs one pack:
// the pack target is the smallest the spike measured, and nothing on the
// write side improved with a larger one.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		Chunk:      ChunkParams{Min: 256 << 10, Avg: 1 << 20, Max: 4 << 20},
		PackLimit:  256 << 10,
		PackTarget: 1 << 20,
		Compress:   true,
	}
}

type pendingExtent struct {
	node   *buildNode
	offset uint32
	stored uint32
	raw    uint32
}

// Writer is the reference writer: it turns file contents into objects and a
// manifest. Objects reach put as they are completed. It holds one pack and
// one file's read buffer in memory.
type Writer struct {
	*Builder
	opts    WriterOptions
	put     func(Object, []byte) error
	pack    []byte
	pending []pendingExtent
	seen    map[Digest]FileExtent // raw chunk -> the object holding it
	Objects int
	Stored  uint64
}

func NewWriter(opts WriterOptions, put func(Object, []byte) error) (*Writer, error) {
	chunk := opts.Chunk
	if chunk.Avg <= 0 || chunk.Avg&(chunk.Avg-1) != 0 || chunk.Min < 0 || chunk.Min > chunk.Max || chunk.Max > MaxExtentLen {
		return nil, fmt.Errorf("invalid chunk parameters %+v", chunk)
	}
	if opts.PackLimit > MaxExtentLen || opts.PackTarget+opts.PackLimit > MaxObjectSize {
		return nil, fmt.Errorf("invalid pack parameters")
	}
	return &Writer{Builder: NewBuilder(), opts: opts, put: put, seen: map[Digest]FileExtent{}}, nil
}

func (w *Writer) encode(raw []byte) []byte {
	if !w.opts.Compress {
		return raw
	}
	compressed := zstdEncoder.EncodeAll(raw, make([]byte, 0, len(raw)/2))
	// An extent as long stored as raw is raw to a reader, so a compressed
	// one is shorter whatever the eighth rounds to.
	if len(compressed) >= len(raw) || len(compressed) > len(raw)-len(raw)/8 {
		return raw
	}
	return compressed
}

func (w *Writer) emit(data []byte) (Object, error) {
	object := Object{Digest: DigestOf(data), Size: uint64(len(data))}
	w.Objects++
	w.Stored += object.Size
	return object, w.put(object, data)
}

func (w *Writer) flushPack() error {
	if len(w.pack) == 0 {
		return nil
	}
	object, err := w.emit(w.pack)
	if err != nil {
		return err
	}
	for _, pending := range w.pending {
		extent := FileExtent{Object: object, Offset: pending.offset, StoredLen: pending.stored, RawLen: pending.raw}
		pending.node.extents = []FileExtent{extent}
	}
	w.pack, w.pending = nil, nil
	return nil
}

// AddFile reads a file's content and adds it to the tree.
func (w *Writer) AddFile(path string, mode uint32, mtimeNs int64, content io.Reader) error {
	buffer := make([]byte, 0, w.opts.PackLimit+1)
	buffer, err := readFull(content, buffer, w.opts.PackLimit+1)
	if err != nil {
		return err
	}
	if len(buffer) <= w.opts.PackLimit {
		node := &buildNode{kind: KindFile, mode: mode & 0o777, mtimeNs: mtimeNs, size: uint64(len(buffer))}
		if err := w.add(path, node); err != nil {
			return err
		}
		if len(buffer) == 0 {
			return nil
		}
		stored := w.encode(buffer)
		w.pending = append(w.pending, pendingExtent{node: node, offset: uint32(len(w.pack)), stored: uint32(len(stored)), raw: uint32(len(buffer))})
		w.pack = append(w.pack, stored...)
		if len(w.pack) >= w.opts.PackTarget {
			return w.flushPack()
		}
		return nil
	}

	var extents []FileExtent
	for eof := false; ; {
		if !eof && len(buffer) < w.opts.Chunk.Max {
			if buffer, err = readFull(content, buffer, w.opts.Chunk.Max); err != nil {
				return err
			}
			eof = len(buffer) < w.opts.Chunk.Max
		}
		if len(buffer) == 0 {
			break
		}
		length := w.opts.Chunk.Cut(buffer)
		chunk := buffer[:length]
		key := DigestOf(chunk)
		extent, ok := w.seen[key]
		if !ok {
			stored := w.encode(chunk)
			object, err := w.emit(stored)
			if err != nil {
				return err
			}
			extent = FileExtent{Object: object, StoredLen: uint32(len(stored)), RawLen: uint32(length)}
			w.seen[key] = extent
		}
		extents = append(extents, extent)
		buffer = append(buffer[:0], buffer[length:]...)
	}
	return w.Builder.AddFile(path, mode, mtimeNs, extents)
}

// readFull appends to buffer until it holds limit bytes or content ends.
func readFull(content io.Reader, buffer []byte, limit int) ([]byte, error) {
	for len(buffer) < limit {
		if cap(buffer) < limit {
			grown := make([]byte, len(buffer), limit)
			copy(grown, buffer)
			buffer = grown
		}
		n, err := content.Read(buffer[len(buffer):limit])
		buffer = buffer[:len(buffer)+n]
		if err == io.EOF {
			break
		}
		if err != nil {
			return buffer, err
		}
	}
	return buffer, nil
}

// Finish flushes the last pack and returns the stored manifest, which the
// caller stores as an object under its digest like any other.
func (w *Writer) Finish(meta []byte) ([]byte, *Manifest, error) {
	if err := w.flushPack(); err != nil {
		return nil, nil, err
	}
	return w.Encode(meta, false)
}
