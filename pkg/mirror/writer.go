package mirror

import (
	"fmt"
	"io"
)

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
