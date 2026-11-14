// Package mirror reads and writes the mirror store format: content-addressed
// objects and the per-generation manifest that maps a home-relative file tree
// onto them. docs/mirror/format.md is the specification; the vectors under
// testdata are shared with the Rust client.
package mirror

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
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
