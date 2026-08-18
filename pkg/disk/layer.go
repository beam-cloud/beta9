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
	LayerChunkSize = 4 << 20 // 4 MiB

	// LayerFileName is the single logical file inside a qcow.v1 manifest.
	LayerFileName = "layer.qcow2"

	uploadConcurrency     = 16
	fetchConcurrency      = 8
	layerFetchConcurrency = 4
)

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
	buffer := make([]byte, LayerChunkSize)
	index := int64(0)
	for offset := int64(0); offset < fileSize; offset += LayerChunkSize {
		dataOffset, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_DATA)
		if err != nil {
			if err == unix.ENXIO {
				break // Nothing but holes remain.
			}
			// Filesystem without SEEK_DATA support: treat everything as data.
			dataOffset = offset
		}
		if dataOffset >= offset+LayerChunkSize {
			// Skip directly to the chunk containing the next data extent.
			offset = (dataOffset / LayerChunkSize) * LayerChunkSize
			offset -= LayerChunkSize // Compensate the loop increment.
			continue
		}

		chunkSize := min(LayerChunkSize, fileSize-offset)
		chunk := buffer[:chunkSize]
		if _, err := file.ReadAt(chunk, offset); err != nil && err != io.EOF {
			return nil, fmt.Errorf("read layer chunk at %d: %w", offset, err)
		}
		if isZero(chunk) {
			continue
		}
		digestBytes := sha256.Sum256(chunk)
		digest := "sha256:" + hex.EncodeToString(digestBytes[:])
		layer.Chunks = append(layer.Chunks, types.DiskSnapshotChunk{
			Index:       index,
			OffsetBytes: offset,
			SizeBytes:   chunkSize,
			ObjectKey:   keyForDigest(digest),
			Digest:      digest,
		})
		index++
	}
	return layer, nil
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

// fetchLayer reassembles a layer file from its chunks. The result is written
// to a temporary file and atomically renamed, so a crashed fetch never leaves
// a truncated layer behind.
func fetchLayer(ctx context.Context, source ChunkSource, layer *types.DiskSnapshotFile, destPath string) error {
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

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(fetchConcurrency)
	for _, chunk := range layer.Chunks {
		group.Go(func() error {
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
