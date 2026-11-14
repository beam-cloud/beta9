package mirror

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

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
