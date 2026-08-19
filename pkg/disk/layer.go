package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Sealed qcow2 layers are shipped to object storage as sparse, content
// addressed chunks. Zero regions (holes and explicit zeros) are omitted; a
// restore recreates them by truncating the destination file to size.
const (
	// LayerChunkSize is the average chunk size. Boundaries are content
	// defined (gear hash), so unchanged content keeps its chunk hashes no
	// matter where a flatten relocated it in the file, which is what lets
	// publishes dedup against prior generations: fixed boundaries measured
	// 0% dedup between consecutive flattens, content-defined ~95%.
	LayerChunkSize = 4 << 20 // 4 MiB
	chunkMinSize   = 1 << 20
	chunkMeanMask  = LayerChunkSize - 1
	chunkMaxSize   = 8 << 20

	// LayerFileName is the single logical file inside a qcow.v1 manifest.
	LayerFileName = "layer.qcow2"

	uploadConcurrency = 16
	scanConcurrency   = 8
	// chunkFetchConcurrency bounds in-flight chunk fetches across every layer
	// of a chain, shared so one large layer gets full parallelism and a deep
	// chain does not multiply into hundreds of requests. Each in-flight chunk
	// buffers up to chunkMaxSize, capping restore memory (32 * 8 MiB = 256 MiB).
	chunkFetchConcurrency = 32
	layerFetchConcurrency = 4
)

// gearTable drives the boundary rolling hash. Changing it (or the size
// constants) re-cuts every future publish, costing each disk one full
// re-upload before dedup recovers.
var gearTable = func() [256]uint64 {
	var table [256]uint64
	seed := uint64(0x9E3779B97F4A7C15)
	for i := range table {
		seed = seed*6364136223846793005 + 1442695040888963407
		table[i] = seed
	}
	return table
}()

// ChunkSink stores a chunk body under a content-addressed object key.
type ChunkSink interface {
	WriteChunk(ctx context.Context, key string, data []byte) error
}

// ChunkSource retrieves a chunk body previously stored by a ChunkSink. The
// worker adapter backs this with the node-local content cache falling back to
// object storage, which is what makes restores and forks fast.
type ChunkSource interface {
	ReadChunk(ctx context.Context, chunk types.DiskSnapshotChunk, dest []byte) error
}

// ScanLayer splits a sealed layer file into content-addressed chunks, skipping
// holes and all-zero regions. Object keys are assigned by keyForDigest.
// Boundaries are found in one sequential pass, then chunks are read and
// hashed in parallel: this pass dominates publish latency on large images.
func ScanLayer(path string, keyForDigest func(digest string) string) (*types.DiskSnapshotFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	layer := &types.DiskSnapshotFile{
		Path:      LayerFileName,
		Type:      "file",
		Mode:      0o600,
		SizeBytes: fileSize,
	}

	spans, err := chunkSpans(file, fileSize)
	if err != nil {
		return nil, err
	}
	chunks := make([]*types.DiskSnapshotChunk, len(spans))
	group := errgroup.Group{}
	group.SetLimit(scanConcurrency)
	for i, span := range spans {
		group.Go(func() error {
			data := make([]byte, span.size)
			if _, err := file.ReadAt(data, span.offset); err != nil && err != io.EOF {
				return fmt.Errorf("read layer chunk at %d: %w", span.offset, err)
			}
			if isZero(data) {
				return nil
			}
			digestBytes := sha256.Sum256(data)
			digest := "sha256:" + hex.EncodeToString(digestBytes[:])
			chunks[i] = &types.DiskSnapshotChunk{
				OffsetBytes: span.offset,
				SizeBytes:   span.size,
				ObjectKey:   keyForDigest(digest),
				Digest:      digest,
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		if chunk == nil {
			continue
		}
		chunk.Index = int64(len(layer.Chunks))
		layer.Chunks = append(layer.Chunks, *chunk)
	}
	return layer, nil
}

// chunkSpan is one prospective chunk: a piece of a data extent.
type chunkSpan struct{ offset, size int64 }

// chunkSpans enumerates the file's data extents (SEEK_DATA/SEEK_HOLE; the
// whole file on filesystems without support) and splits each at content
// defined boundaries. Extents reset the boundary state, so sparse layouts
// chunk identically regardless of surrounding holes.
func chunkSpans(file *os.File, fileSize int64) ([]chunkSpan, error) {
	var spans []chunkSpan
	buffer := make([]byte, LayerChunkSize)
	for offset := int64(0); offset < fileSize; {
		dataStart, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_DATA)
		if err != nil {
			if err == unix.ENXIO {
				break // Nothing but holes remain.
			}
			// No SEEK_DATA support: treat the rest of the file as one extent.
			extent, err := splitExtent(file, buffer, offset, fileSize)
			if err != nil {
				return nil, err
			}
			return append(spans, extent...), nil
		}
		dataEnd, err := unix.Seek(int(file.Fd()), dataStart, unix.SEEK_HOLE)
		if err != nil {
			dataEnd = fileSize
		}
		extent, err := splitExtent(file, buffer, dataStart, min(dataEnd, fileSize))
		if err != nil {
			return nil, err
		}
		spans = append(spans, extent...)
		offset = dataEnd
	}
	return spans, nil
}

// splitExtent cuts one data extent at gear-hash boundaries: the hash rolls
// byte-wise (skipping the guaranteed minimum) and a chunk ends where the low
// bits clear, giving LayerChunkSize chunks on average within [min, max].
func splitExtent(file *os.File, buffer []byte, start, end int64) ([]chunkSpan, error) {
	var spans []chunkSpan
	for chunkStart := start; chunkStart < end; {
		if end-chunkStart <= chunkMinSize {
			spans = append(spans, chunkSpan{offset: chunkStart, size: end - chunkStart})
			break
		}
		windowEnd := min(end, chunkStart+chunkMaxSize)
		cut := windowEnd
		hash := uint64(0)
		for pos := chunkStart + chunkMinSize; pos < windowEnd && cut == windowEnd; {
			read, err := file.ReadAt(buffer[:min(int64(len(buffer)), windowEnd-pos)], pos)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("scan layer at %d: %w", pos, err)
			}
			if read == 0 {
				break
			}
			for i := range read {
				hash = hash<<1 + gearTable[buffer[i]]
				if hash&chunkMeanMask == 0 {
					cut = pos + int64(i) + 1
					break
				}
			}
			pos += int64(read)
		}
		spans = append(spans, chunkSpan{offset: chunkStart, size: cut - chunkStart})
		chunkStart = cut
	}
	return spans, nil
}

// UploadLayer ships every chunk of a scanned layer to the sink.
func UploadLayer(ctx context.Context, sink ChunkSink, path string, layer *types.DiskSnapshotFile) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(uploadConcurrency)
	for _, chunk := range layer.Chunks {
		group.Go(func() error {
			data := make([]byte, chunk.SizeBytes)
			if _, err := file.ReadAt(data, chunk.OffsetBytes); err != nil && err != io.EOF {
				return fmt.Errorf("read chunk %s: %w", chunk.Digest, err)
			}
			if err := sink.WriteChunk(groupCtx, chunk.ObjectKey, data); err != nil {
				return fmt.Errorf("upload chunk %s: %w", chunk.Digest, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// chunkGate bounds concurrent chunk fetches. A nil gate is unbounded.
type chunkGate chan struct{}

func newChunkGate(limit int) chunkGate {
	if limit <= 0 {
		return nil
	}
	return make(chunkGate, limit)
}

func (g chunkGate) acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g chunkGate) release() {
	if g != nil {
		<-g
	}
}

// fetchLayer reassembles a layer file from its chunks. The result is written
// to a temporary file and atomically renamed, so a crashed fetch never leaves
// a truncated layer behind. Chunk parallelism is bounded by gate, which the
// caller shares across all layers of a chain.
func fetchLayer(ctx context.Context, source ChunkSource, layer *types.DiskSnapshotFile, destPath string, gate chunkGate) error {
	tmpPath := destPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		os.Remove(tmpPath)
	}()
	if err := file.Truncate(layer.SizeBytes); err != nil {
		return err
	}

	if gate == nil {
		gate = newChunkGate(chunkFetchConcurrency)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	for _, chunk := range layer.Chunks {
		group.Go(func() error {
			if err := gate.acquire(groupCtx); err != nil {
				return err
			}
			defer gate.release()

			data := make([]byte, chunk.SizeBytes)
			if err := source.ReadChunk(groupCtx, chunk, data); err != nil {
				return fmt.Errorf("fetch chunk %s: %w", chunk.Digest, err)
			}
			digestBytes := sha256.Sum256(data)
			if digest := "sha256:" + hex.EncodeToString(digestBytes[:]); digest != chunk.Digest {
				return fmt.Errorf("chunk %s digest mismatch: got %s", chunk.ObjectKey, digest)
			}
			if _, err := file.WriteAt(data, chunk.OffsetBytes); err != nil {
				return fmt.Errorf("write chunk at %d: %w", chunk.OffsetBytes, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, destPath)
}

func isZero(data []byte) bool {
	// bytes.Count is vectorized; a zero chunk contains only zero bytes.
	return bytes.Count(data, []byte{0}) == len(data)
}

// StoredBytes sums the chunk sizes of a scanned layer.
func StoredBytes(layer *types.DiskSnapshotFile) int64 {
	var total int64
	for _, chunk := range layer.Chunks {
		total += chunk.SizeBytes
	}
	return total
}
