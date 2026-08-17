package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	blockV1TransferConcurrency = 16
	// These are format implementation limits, not tenant quotas. They bound
	// allocation/truncate work before an authenticated scheduler size is bound
	// to a manifest. Raising them requires a restore/resource-budget review.
	BlockV1MaxVirtualSizeBytes   int64 = 64 << 40 // 64 TiB
	BlockV1MaxLayerFileSizeBytes int64 = 128 << 40
	BlockV1MaxChunks                   = 64 << 10
)

var blockV1DigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type BlockV1Manifest struct {
	Version                 string         `json:"version"`
	Format                  string         `json:"format"`
	VolumeID                string         `json:"volume_id"`
	GenerationID            string         `json:"generation_id"`
	Generation              int64          `json:"generation"`
	ParentGenerationID      string         `json:"parent_generation_id,omitempty"`
	CloneParentGenerationID string         `json:"clone_parent_generation_id,omitempty"`
	VirtualSizeBytes        int64          `json:"virtual_size_bytes"`
	LayerFileSizeBytes      int64          `json:"layer_file_size_bytes"`
	QCOW2ClusterSize        int64          `json:"qcow2_cluster_size"`
	QCOW2Compat             string         `json:"qcow2_compat"`
	QCOW2LazyRefcounts      bool           `json:"qcow2_lazy_refcounts"`
	ChunkSizeBytes          int64          `json:"chunk_size_bytes"`
	Depth                   int            `json:"depth"`
	Chunks                  []BlockV1Chunk `json:"chunks"`
}

type BlockV1Chunk struct {
	Index       int64  `json:"index"`
	OffsetBytes int64  `json:"offset_bytes"`
	SizeBytes   int64  `json:"size_bytes"`
	Digest      string `json:"digest"`
}

type BlockV1Metadata struct {
	VolumeID                string
	GenerationID            string
	ParentGenerationID      string
	CloneParentGenerationID string
	VirtualSizeBytes        int64
	Depth                   int
	Generation              int64
	BackingPath             string
}

type StateVolumeImageInfo struct {
	Format           string
	VirtualSizeBytes int64
	ClusterSizeBytes int64
	Compat           string
	LazyRefcounts    bool
	BackingPath      string
	BackingFormat    string
}

func validateStateVolumeImageInfo(info StateVolumeImageInfo, virtualSizeBytes int64, backingPath string) error {
	if info.Format != "qcow2" || info.VirtualSizeBytes != virtualSizeBytes ||
		info.ClusterSizeBytes != StateVolumeClusterSize || info.Compat != "1.1" || info.LazyRefcounts {
		return fmt.Errorf("qcow2 image settings do not match block.v1 contract")
	}
	if backingPath == "" {
		if info.BackingPath != "" || info.BackingFormat != "" {
			return fmt.Errorf("qcow2 image unexpectedly has backing %q", info.BackingPath)
		}
		return nil
	}
	expected, err := canonicalStateVolumePath(backingPath)
	if err != nil {
		return err
	}
	actual, err := canonicalStateVolumePath(info.BackingPath)
	if err != nil {
		return err
	}
	if actual != expected || info.BackingFormat != "qcow2" {
		return fmt.Errorf("qcow2 backing mismatch: got %q/%q, want %q/qcow2", actual, info.BackingFormat, expected)
	}
	return nil
}

func ValidateBlockV1Manifest(manifest BlockV1Manifest) error {
	if manifest.Version != BlockV1Format || manifest.Format != "qcow2" {
		return fmt.Errorf("unsupported block manifest %q/%q", manifest.Version, manifest.Format)
	}
	if manifest.VolumeID == "" || manifest.GenerationID == "" || manifest.Generation <= 0 {
		return fmt.Errorf("block manifest has no volume or generation ID")
	}
	if manifest.VirtualSizeBytes <= 0 || manifest.VirtualSizeBytes > BlockV1MaxVirtualSizeBytes ||
		manifest.LayerFileSizeBytes <= 0 || manifest.LayerFileSizeBytes > BlockV1MaxLayerFileSizeBytes {
		return fmt.Errorf("block manifest virtual or file size exceeds supported bounds")
	}
	if manifest.QCOW2ClusterSize != StateVolumeClusterSize {
		return fmt.Errorf("block manifest has unsupported qcow2 cluster size %d", manifest.QCOW2ClusterSize)
	}
	if manifest.QCOW2Compat != "1.1" || manifest.QCOW2LazyRefcounts {
		return fmt.Errorf("block manifest has unsupported qcow2 compatibility settings")
	}
	if manifest.ChunkSizeBytes != BlockV1ChunkSize {
		return fmt.Errorf("block manifest has unsupported chunk size %d", manifest.ChunkSizeBytes)
	}
	if manifest.Depth < 1 || manifest.Depth > StateVolumeMaxDepth {
		return fmt.Errorf("block manifest depth %d exceeds supported range", manifest.Depth)
	}
	if manifest.ParentGenerationID != "" && manifest.CloneParentGenerationID != "" {
		return fmt.Errorf("block manifest cannot have both parent and clone parent")
	}
	if manifest.CloneParentGenerationID != "" && manifest.Generation != 1 {
		return fmt.Errorf("cloned block manifest must start at generation 1")
	}
	if manifest.ParentGenerationID == "" && manifest.CloneParentGenerationID == "" && manifest.Depth != 1 {
		return fmt.Errorf("parentless block manifest must have depth 1")
	}
	if (manifest.ParentGenerationID != "" || manifest.CloneParentGenerationID != "") && manifest.Depth < 2 {
		return fmt.Errorf("child block manifest must have depth at least 2")
	}
	if len(manifest.Chunks) > BlockV1MaxChunks {
		return fmt.Errorf("block manifest chunk count %d exceeds supported maximum %d", len(manifest.Chunks), BlockV1MaxChunks)
	}
	maxChunkIndex := (manifest.LayerFileSizeBytes - 1) / manifest.ChunkSizeBytes
	previousEnd := int64(0)
	previousIndex := int64(-1)
	for i, chunk := range manifest.Chunks {
		// Bound the index before multiplying. This also proves the product and
		// all subsequent subtraction/addition stay within int64.
		if chunk.Index < 0 || chunk.Index > maxChunkIndex {
			return fmt.Errorf("block manifest chunk %d index is outside the layer", i)
		}
		expectedOffset := chunk.Index * manifest.ChunkSizeBytes
		if chunk.OffsetBytes != expectedOffset {
			return fmt.Errorf("block manifest chunk %d has inconsistent index and offset", i)
		}
		if chunk.Index <= previousIndex {
			return fmt.Errorf("block manifest chunks are not strictly ordered")
		}
		remaining := manifest.LayerFileSizeBytes - chunk.OffsetBytes
		expectedSize := min(manifest.ChunkSizeBytes, remaining)
		if chunk.SizeBytes <= 0 || chunk.SizeBytes != expectedSize || chunk.SizeBytes > remaining {
			return fmt.Errorf("block manifest chunk %d is outside the layer", i)
		}
		if chunk.OffsetBytes < previousEnd {
			return fmt.Errorf("block manifest chunks overlap")
		}
		if !blockV1DigestPattern.MatchString(chunk.Digest) {
			return fmt.Errorf("block manifest chunk %d has invalid digest", i)
		}
		previousEnd = chunk.OffsetBytes + chunk.SizeBytes
		previousIndex = chunk.Index
	}
	return nil
}

func CreateBlockV1Manifest(ctx context.Context, layerPath string, metadata BlockV1Metadata, images StateVolumeImageTool, cas BlockV1CAS) (BlockV1Manifest, error) {
	if cas == nil || images == nil {
		return BlockV1Manifest{}, fmt.Errorf("block.v1 CAS and image inspector are required")
	}
	imageInfo, err := images.Info(ctx, layerPath)
	if err != nil {
		return BlockV1Manifest{}, fmt.Errorf("inspect qcow2 layer %s: %w", layerPath, err)
	}
	if err := validateStateVolumeImageInfo(imageInfo, metadata.VirtualSizeBytes, metadata.BackingPath); err != nil {
		return BlockV1Manifest{}, fmt.Errorf("validate qcow2 layer %s: %w", layerPath, err)
	}
	if imageInfo.VirtualSizeBytes <= 0 || imageInfo.VirtualSizeBytes > BlockV1MaxVirtualSizeBytes {
		return BlockV1Manifest{}, fmt.Errorf("qcow2 layer %s virtual size exceeds block.v1 supported bounds", layerPath)
	}
	if err := images.Check(ctx, layerPath); err != nil {
		return BlockV1Manifest{}, fmt.Errorf("check immutable qcow2 layer %s: %w", layerPath, err)
	}
	file, err := os.Open(layerPath)
	if err != nil {
		return BlockV1Manifest{}, fmt.Errorf("open qcow2 layer %s: %w", layerPath, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return BlockV1Manifest{}, fmt.Errorf("stat qcow2 layer %s: %w", layerPath, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > BlockV1MaxLayerFileSizeBytes {
		return BlockV1Manifest{}, fmt.Errorf("qcow2 layer %s is not a non-empty regular file", layerPath)
	}
	indices, err := allocatedBlockV1ChunkIndices(file, info.Size(), BlockV1ChunkSize)
	if err != nil {
		return BlockV1Manifest{}, fmt.Errorf("scan sparse qcow2 layer %s: %w", layerPath, err)
	}

	manifest := BlockV1Manifest{
		Version:                 BlockV1Format,
		Format:                  "qcow2",
		VolumeID:                metadata.VolumeID,
		GenerationID:            metadata.GenerationID,
		Generation:              metadata.Generation,
		ParentGenerationID:      metadata.ParentGenerationID,
		CloneParentGenerationID: metadata.CloneParentGenerationID,
		VirtualSizeBytes:        imageInfo.VirtualSizeBytes,
		LayerFileSizeBytes:      info.Size(),
		QCOW2ClusterSize:        imageInfo.ClusterSizeBytes,
		QCOW2Compat:             imageInfo.Compat,
		QCOW2LazyRefcounts:      imageInfo.LazyRefcounts,
		ChunkSizeBytes:          BlockV1ChunkSize,
		Depth:                   metadata.Depth,
	}
	var chunksMu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(blockV1TransferConcurrency)
	for _, index := range indices {
		index := index
		group.Go(func() error {
			offset := index * BlockV1ChunkSize
			size := min(BlockV1ChunkSize, info.Size()-offset)
			data := make([]byte, size)
			if _, err := file.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("read qcow2 chunk %d: %w", index, err)
			}
			if isZeroBlockV1Chunk(data) {
				return nil
			}
			digestBytes := sha256.Sum256(data)
			digest := hex.EncodeToString(digestBytes[:])
			if err := cas.Put(groupCtx, digest, size, bytes.NewReader(data)); err != nil {
				return fmt.Errorf("upload qcow2 chunk %d: %w", index, err)
			}
			chunksMu.Lock()
			manifest.Chunks = append(manifest.Chunks, BlockV1Chunk{Index: index, OffsetBytes: offset, SizeBytes: size, Digest: digest})
			chunksMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return BlockV1Manifest{}, err
	}
	sort.Slice(manifest.Chunks, func(i, j int) bool { return manifest.Chunks[i].Index < manifest.Chunks[j].Index })
	if err := ValidateBlockV1Manifest(manifest); err != nil {
		return BlockV1Manifest{}, err
	}
	return manifest, nil
}

// EncodeBlockV1ManifestCanonical validates and encodes a deterministic
// manifest. Struct field order and sorted chunks make the digest stable across
// workers without relying on map ordering.
func EncodeBlockV1ManifestCanonical(manifest BlockV1Manifest) ([]byte, string, error) {
	manifest.Chunks = append([]BlockV1Chunk(nil), manifest.Chunks...)
	sort.Slice(manifest.Chunks, func(i, j int) bool { return manifest.Chunks[i].Index < manifest.Chunks[j].Index })
	if err := ValidateBlockV1Manifest(manifest); err != nil {
		return nil, "", err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", fmt.Errorf("encode block.v1 manifest: %w", err)
	}
	digestBytes := sha256.Sum256(data)
	return data, hex.EncodeToString(digestBytes[:]), nil
}

// PublishBlockV1Manifest writes the manifest only after CreateBlockV1Manifest
// has made every referenced chunk durable in CAS.
func PublishBlockV1Manifest(ctx context.Context, manifest BlockV1Manifest, cas BlockV1CAS) (string, error) {
	if cas == nil {
		return "", fmt.Errorf("block.v1 CAS is nil")
	}
	data, digest, err := EncodeBlockV1ManifestCanonical(manifest)
	if err != nil {
		return "", err
	}
	if err := cas.Put(ctx, digest, int64(len(data)), bytes.NewReader(data)); err != nil {
		return "", fmt.Errorf("publish block.v1 manifest: %w", err)
	}
	return digest, nil
}

func allocatedBlockV1ChunkIndices(file *os.File, fileSize, chunkSize int64) ([]int64, error) {
	if fileSize <= 0 || fileSize > BlockV1MaxLayerFileSizeBytes || chunkSize != BlockV1ChunkSize {
		return nil, fmt.Errorf("block.v1 sparse scan size is outside supported bounds")
	}
	indices := make(map[int64]struct{})
	offset := int64(0)
	for offset < fileSize {
		dataOffset, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
			return allBlockV1ChunkIndices(fileSize, chunkSize)
		}
		if err != nil {
			return nil, err
		}
		holeOffset, err := unix.Seek(int(file.Fd()), dataOffset, unix.SEEK_HOLE)
		if errors.Is(err, unix.ENXIO) {
			holeOffset = fileSize
		} else if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
			return allBlockV1ChunkIndices(fileSize, chunkSize)
		} else if err != nil {
			return nil, err
		}
		if holeOffset > fileSize {
			holeOffset = fileSize
		}
		for index := dataOffset / chunkSize; index <= (holeOffset-1)/chunkSize; index++ {
			if _, exists := indices[index]; !exists && len(indices) >= BlockV1MaxChunks {
				return nil, fmt.Errorf("block.v1 allocated chunk count exceeds supported maximum %d", BlockV1MaxChunks)
			}
			indices[index] = struct{}{}
		}
		if holeOffset <= offset {
			return nil, fmt.Errorf("sparse extent scan did not advance at offset %d", offset)
		}
		offset = holeOffset
	}
	result := make([]int64, 0, len(indices))
	for index := range indices {
		result = append(result, index)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func allBlockV1ChunkIndices(fileSize, chunkSize int64) ([]int64, error) {
	if fileSize <= 0 || chunkSize <= 0 {
		return nil, fmt.Errorf("invalid block.v1 chunk enumeration size")
	}
	// Division-first ceiling avoids fileSize+chunkSize-1 overflow.
	count := 1 + (fileSize-1)/chunkSize
	if count > BlockV1MaxChunks {
		return nil, fmt.Errorf("block.v1 chunk count %d exceeds supported maximum %d", count, BlockV1MaxChunks)
	}
	indices := make([]int64, count)
	for i := range indices {
		indices[i] = int64(i)
	}
	return indices, nil
}

func isZeroBlockV1Chunk(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func RestoreBlockV1Layer(ctx context.Context, destination string, manifest BlockV1Manifest, cas BlockV1CAS) error {
	return restoreBlockV1Layer(ctx, destination, manifest, 0, cas)
}

func restoreBlockV1Layer(ctx context.Context, destination string, manifest BlockV1Manifest, expectedVirtualSizeBytes int64, cas BlockV1CAS) error {
	if cas == nil {
		return fmt.Errorf("block.v1 CAS is nil")
	}
	if err := ValidateBlockV1Manifest(manifest); err != nil {
		return err
	}
	if expectedVirtualSizeBytes != 0 && manifest.VirtualSizeBytes != expectedVirtualSizeBytes {
		return fmt.Errorf("block manifest virtual size %d does not match requested size %d", manifest.VirtualSizeBytes, expectedVirtualSizeBytes)
	}
	if info, err := os.Lstat(destination); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refuse to replace non-regular block layer cache entry %s (%s)", destination, info.Mode())
		}
		if err := validateExistingBlockV1Layer(destination, manifest); err == nil {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	destinationDir := filepath.Dir(destination)
	if err := os.MkdirAll(destinationDir, 0700); err != nil {
		return fmt.Errorf("create block layer directory: %w", err)
	}
	destinationDirInfo, err := os.Lstat(destinationDir)
	if err != nil {
		return fmt.Errorf("inspect block layer directory: %w", err)
	}
	if destinationDirInfo.Mode()&os.ModeSymlink != 0 || !destinationDirInfo.IsDir() {
		return fmt.Errorf("refuse block layer cache directory %s with mode %s", destinationDir, destinationDirInfo.Mode())
	}
	tmp, err := os.CreateTemp(destinationDir, ".block-v1-*")
	if err != nil {
		return fmt.Errorf("create block layer temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Truncate(manifest.LayerFileSizeBytes); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("size restored block layer: %w", err)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(blockV1TransferConcurrency)
	for _, chunk := range manifest.Chunks {
		chunk := chunk
		group.Go(func() error {
			reader, err := cas.Get(groupCtx, chunk.Digest, chunk.SizeBytes)
			if err != nil {
				return fmt.Errorf("download qcow2 chunk %d: %w", chunk.Index, err)
			}
			defer reader.Close()
			data := make([]byte, chunk.SizeBytes)
			if _, err := io.ReadFull(reader, data); err != nil {
				return fmt.Errorf("read qcow2 chunk %d: %w", chunk.Index, err)
			}
			extra, err := io.ReadAll(io.LimitReader(reader, 2))
			if err != nil || len(extra) != 0 {
				return fmt.Errorf("qcow2 chunk %d has unexpected trailing data", chunk.Index)
			}
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) != chunk.Digest {
				return fmt.Errorf("qcow2 chunk %d digest mismatch", chunk.Index)
			}
			if _, err := tmp.WriteAt(data, chunk.OffsetBytes); err != nil {
				return fmt.Errorf("write qcow2 chunk %d: %w", chunk.Index, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync restored block layer: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close restored block layer: %w", err)
	}
	if err := os.Rename(tmpPath, destination); err != nil {
		return fmt.Errorf("publish restored block layer: %w", err)
	}
	return syncStateVolumeDirectory(destinationDir)
}

func validateExistingBlockV1Layer(path string, manifest BlockV1Manifest) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != manifest.LayerFileSizeBytes {
		return fmt.Errorf("cached block layer has the wrong type or size")
	}
	expected := make(map[int64]BlockV1Chunk, len(manifest.Chunks))
	indices := make(map[int64]struct{}, len(manifest.Chunks))
	for _, chunk := range manifest.Chunks {
		expected[chunk.Index] = chunk
		indices[chunk.Index] = struct{}{}
	}
	allocated, err := allocatedBlockV1ChunkIndices(file, info.Size(), manifest.ChunkSizeBytes)
	if err != nil {
		return err
	}
	for _, index := range allocated {
		indices[index] = struct{}{}
	}
	for index := range indices {
		offset := index * manifest.ChunkSizeBytes
		size := min(manifest.ChunkSizeBytes, info.Size()-offset)
		data := make([]byte, size)
		if _, err := file.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		chunk, present := expected[index]
		if !present {
			if !isZeroBlockV1Chunk(data) {
				return fmt.Errorf("cached block layer has unmanifested data at chunk %d", index)
			}
			continue
		}
		if chunk.SizeBytes != size {
			return fmt.Errorf("cached block layer chunk %d has the wrong size", index)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != chunk.Digest {
			return fmt.Errorf("cached block layer chunk %d digest mismatch", index)
		}
	}
	return nil
}

type StateVolumeImageTool interface {
	Create(ctx context.Context, path string, virtualSizeBytes int64, backingPath string) error
	Rebase(ctx context.Context, path, backingPath string) error
	Check(ctx context.Context, path string) error
	Flatten(ctx context.Context, sourcePath, destinationPath string) error
	Info(ctx context.Context, path string) (StateVolumeImageInfo, error)
}

type QEMUStateVolumeImageTool struct {
	Runner StateVolumeCommandRunner
}

func (t QEMUStateVolumeImageTool) runner() StateVolumeCommandRunner {
	if t.Runner == nil {
		return OSStateVolumeCommandRunner{}
	}
	return t.Runner
}

func (t QEMUStateVolumeImageTool) Create(ctx context.Context, path string, virtualSizeBytes int64, backingPath string) error {
	args := []string{"create", "-f", "qcow2", "-o", "compat=1.1,cluster_size=65536,lazy_refcounts=off"}
	if backingPath != "" {
		args = append(args, "-F", "qcow2", "-b", backingPath)
	}
	args = append(args, path, strconv.FormatInt(virtualSizeBytes, 10))
	_, err := t.runner().Run(ctx, "qemu-img", args...)
	return err
}

func (t QEMUStateVolumeImageTool) Rebase(ctx context.Context, path, backingPath string) error {
	args := []string{"rebase", "-u", "-f", "qcow2"}
	if backingPath == "" {
		args = append(args, "-b", "")
	} else {
		args = append(args, "-F", "qcow2", "-b", backingPath)
	}
	args = append(args, path)
	_, err := t.runner().Run(ctx, "qemu-img", args...)
	return err
}

func (t QEMUStateVolumeImageTool) Check(ctx context.Context, path string) error {
	_, err := t.runner().Run(ctx, "qemu-img", "check", "-q", "-f", "qcow2", path)
	return err
}

func (t QEMUStateVolumeImageTool) Info(ctx context.Context, path string) (StateVolumeImageInfo, error) {
	output, err := t.runner().Run(ctx, "qemu-img", "info", "--output=json", "-f", "qcow2", path)
	if err != nil {
		return StateVolumeImageInfo{}, err
	}
	var raw struct {
		Format              string `json:"format"`
		VirtualSize         int64  `json:"virtual-size"`
		ClusterSize         int64  `json:"cluster-size"`
		BackingFilename     string `json:"backing-filename"`
		FullBackingFilename string `json:"full-backing-filename"`
		BackingFormat       string `json:"backing-filename-format"`
		FormatSpecific      struct {
			Type string `json:"type"`
			Data struct {
				Compat        string `json:"compat"`
				LazyRefcounts bool   `json:"lazy-refcounts"`
			} `json:"data"`
		} `json:"format-specific"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		// qemu-img adds release-specific informational keys, so decode a
		// second time without unknown-field rejection while retaining typed
		// validation for every field in the block.v1 contract.
		if err := json.Unmarshal(output, &raw); err != nil {
			return StateVolumeImageInfo{}, fmt.Errorf("decode qemu-img info: %w", err)
		}
	}
	backing := raw.FullBackingFilename
	if backing == "" {
		backing = raw.BackingFilename
	}
	return StateVolumeImageInfo{
		Format: raw.Format, VirtualSizeBytes: raw.VirtualSize, ClusterSizeBytes: raw.ClusterSize,
		Compat: raw.FormatSpecific.Data.Compat, LazyRefcounts: raw.FormatSpecific.Data.LazyRefcounts,
		BackingPath: backing, BackingFormat: raw.BackingFormat,
	}, nil
}

func (t QEMUStateVolumeImageTool) Flatten(ctx context.Context, sourcePath, destinationPath string) error {
	_, err := t.runner().Run(ctx, "qemu-img", "convert", "-f", "qcow2", "-O", "qcow2", "-o", "compat=1.1,cluster_size=65536,lazy_refcounts=off", "-S", "4k", sourcePath, destinationPath)
	return err
}

func RestoreBlockV1Chain(ctx context.Context, generationID, destinationRoot string, resolver BlockV1ManifestResolver, cas BlockV1CAS, images StateVolumeImageTool) (string, BlockV1Manifest, error) {
	return RestoreBlockV1ChainForVolume(ctx, "", generationID, destinationRoot, resolver, cas, images)
}

func RestoreBlockV1ChainForVolume(ctx context.Context, volumeID, generationID, destinationRoot string, resolver BlockV1ManifestResolver, cas BlockV1CAS, images StateVolumeImageTool, expectedVirtualSizeBytes ...int64) (string, BlockV1Manifest, error) {
	if generationID == "" || resolver == nil || images == nil {
		return "", BlockV1Manifest{}, fmt.Errorf("restore block.v1 chain requires generation, resolver, and image tool")
	}
	if len(expectedVirtualSizeBytes) > 1 {
		return "", BlockV1Manifest{}, fmt.Errorf("restore block.v1 chain accepts at most one expected virtual size")
	}
	expectedSize := int64(0)
	if len(expectedVirtualSizeBytes) == 1 {
		expectedSize = expectedVirtualSizeBytes[0]
		if expectedSize <= 0 || expectedSize > BlockV1MaxVirtualSizeBytes {
			return "", BlockV1Manifest{}, fmt.Errorf("requested block.v1 virtual size %d exceeds supported bounds", expectedSize)
		}
	}
	if err := os.MkdirAll(destinationRoot, 0700); err != nil {
		return "", BlockV1Manifest{}, err
	}
	rootInfo, err := os.Lstat(destinationRoot)
	if err != nil {
		return "", BlockV1Manifest{}, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", BlockV1Manifest{}, fmt.Errorf("refuse block.v1 destination root %s with mode %s", destinationRoot, rootInfo.Mode())
	}
	visiting := make(map[string]bool)
	paths := make(map[string]string)
	var restore func(string, int, string) (string, BlockV1Manifest, error)
	restore = func(id string, traversed int, expectedVolumeID string) (string, BlockV1Manifest, error) {
		if traversed >= StateVolumeMaxDepth {
			return "", BlockV1Manifest{}, fmt.Errorf("block.v1 chain exceeds depth %d", StateVolumeMaxDepth)
		}
		if path := paths[id]; path != "" {
			manifest, err := resolver.ResolveBlockV1Manifest(ctx, id)
			if err == nil && expectedVolumeID != "" && manifest.VolumeID != expectedVolumeID {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q belongs to volume %q, not %q", id, manifest.VolumeID, expectedVolumeID)
			}
			return path, manifest, err
		}
		if visiting[id] {
			return "", BlockV1Manifest{}, fmt.Errorf("block.v1 chain cycle at generation %q", id)
		}
		visiting[id] = true
		defer delete(visiting, id)
		manifest, err := resolver.ResolveBlockV1Manifest(ctx, id)
		if err != nil {
			return "", BlockV1Manifest{}, fmt.Errorf("resolve block.v1 generation %q: %w", id, err)
		}
		if manifest.GenerationID != id {
			return "", BlockV1Manifest{}, fmt.Errorf("block.v1 resolver returned generation %q for %q", manifest.GenerationID, id)
		}
		if err := ValidateBlockV1Manifest(manifest); err != nil {
			return "", BlockV1Manifest{}, err
		}
		if expectedSize != 0 && manifest.VirtualSizeBytes != expectedSize {
			return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q virtual size %d does not match requested size %d", id, manifest.VirtualSizeBytes, expectedSize)
		}
		if expectedVolumeID != "" && manifest.VolumeID != expectedVolumeID {
			return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q belongs to volume %q, not %q", id, manifest.VolumeID, expectedVolumeID)
		}
		var parentPath string
		parentID := manifest.ParentGenerationID
		cloneEdge := false
		if parentID == "" {
			parentID = manifest.CloneParentGenerationID
			cloneEdge = parentID != ""
		}
		if parentID != "" {
			parentPreview, err := resolver.ResolveBlockV1Manifest(ctx, parentID)
			if err != nil {
				return "", BlockV1Manifest{}, fmt.Errorf("resolve block.v1 generation %q: %w", parentID, err)
			}
			if parentPreview.GenerationID != parentID {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 resolver returned generation %q for %q", parentPreview.GenerationID, parentID)
			}
			if err := ValidateBlockV1Manifest(parentPreview); err != nil {
				return "", BlockV1Manifest{}, err
			}
			if !cloneEdge && parentPreview.VolumeID != manifest.VolumeID {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q crosses volumes from %q to %q", id, parentPreview.VolumeID, manifest.VolumeID)
			}
			if cloneEdge && parentPreview.VolumeID == manifest.VolumeID {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 clone generation %q does not rekey its source volume", id)
			}
			if manifest.Depth != parentPreview.Depth+1 {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q depth %d does not follow parent depth %d", id, manifest.Depth, parentPreview.Depth)
			}
			var parentManifest BlockV1Manifest
			parentPath, parentManifest, err = restore(parentID, traversed+1, parentPreview.VolumeID)
			if err != nil {
				return "", BlockV1Manifest{}, err
			}
			if !cloneEdge && parentManifest.VolumeID != manifest.VolumeID {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q crosses volumes from %q to %q", id, parentManifest.VolumeID, manifest.VolumeID)
			}
			if manifest.Depth != parentManifest.Depth+1 {
				return "", BlockV1Manifest{}, fmt.Errorf("block.v1 generation %q depth %d does not follow parent depth %d", id, manifest.Depth, parentManifest.Depth)
			}
		}
		cacheDir := filepath.Join(destinationRoot, "cache", stateVolumeToken("generation-", id))
		pristinePath := filepath.Join(cacheDir, "layer.qcow2")
		if err := restoreBlockV1Layer(ctx, pristinePath, manifest, expectedSize, cas); err != nil {
			return "", BlockV1Manifest{}, err
		}
		graphKey := id + "\x00" + parentPath
		graphDir := filepath.Join(destinationRoot, "graph", stateVolumeToken("generation-", graphKey))
		graphPath := filepath.Join(graphDir, "layer.qcow2")
		_, manifestDigest, err := EncodeBlockV1ManifestCanonical(manifest)
		if err != nil {
			return "", BlockV1Manifest{}, err
		}
		if err := materializeBlockV1GraphLayer(ctx, pristinePath, graphPath, parentPath, manifest.VirtualSizeBytes, manifestDigest, images); err != nil {
			return "", BlockV1Manifest{}, fmt.Errorf("materialize block.v1 generation %q graph: %w", id, err)
		}
		paths[id] = graphPath
		return graphPath, manifest, nil
	}
	return restore(generationID, 0, volumeID)
}

func materializeBlockV1GraphLayer(ctx context.Context, pristinePath, destination, backingPath string, virtualSizeBytes int64, manifestDigest string, images StateVolumeImageTool) error {
	if !blockV1DigestPattern.MatchString(manifestDigest) {
		return fmt.Errorf("invalid graph manifest digest %q", manifestDigest)
	}
	dir := filepath.Dir(destination)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse block graph directory %s", dir)
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, ".graph.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
	identityPath := destination + ".identity.json"
	if info, err := os.Lstat(destination); err == nil {
		if info.Mode().IsRegular() && validateBlockV1GraphCache(ctx, destination, identityPath, backingPath, virtualSizeBytes, manifestDigest, images) == nil {
			return nil
		}
		quarantineToken := stateVolumeToken("corrupt-", destination+"\x00"+time.Now().UTC().String())
		if err := os.Rename(destination, filepath.Join(dir, quarantineToken+".qcow2")); err != nil {
			return fmt.Errorf("quarantine invalid block graph cache entry: %w", err)
		}
		if _, err := os.Lstat(identityPath); err == nil {
			if err := os.Rename(identityPath, filepath.Join(dir, quarantineToken+".identity.json")); err != nil {
				return fmt.Errorf("quarantine invalid block graph identity: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	source, err := os.Open(pristinePath)
	if err != nil {
		return err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("invalid pristine block layer %s", pristinePath)
	}
	tmp, err := os.CreateTemp(dir, ".block-v1-graph-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Truncate(info.Size()); err != nil {
		_ = tmp.Close()
		return err
	}
	for offset := int64(0); offset < info.Size(); {
		dataOffset, err := unix.Seek(int(source.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if err != nil {
			_ = tmp.Close()
			return err
		}
		holeOffset, err := unix.Seek(int(source.Fd()), dataOffset, unix.SEEK_HOLE)
		if errors.Is(err, unix.ENXIO) {
			holeOffset = info.Size()
		} else if err != nil {
			_ = tmp.Close()
			return err
		}
		if _, err := tmp.Seek(dataOffset, io.SeekStart); err != nil {
			_ = tmp.Close()
			return err
		}
		if _, err := io.CopyN(tmp, io.NewSectionReader(source, dataOffset, holeOffset-dataOffset), holeOffset-dataOffset); err != nil {
			_ = tmp.Close()
			return err
		}
		offset = holeOffset
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := images.Rebase(ctx, tmpPath, backingPath); err != nil {
		return err
	}
	if err := images.Check(ctx, tmpPath); err != nil {
		return err
	}
	imageInfo, err := images.Info(ctx, tmpPath)
	if err != nil {
		return err
	}
	if err := validateStateVolumeImageInfo(imageInfo, virtualSizeBytes, backingPath); err != nil {
		return err
	}
	graph, err := os.OpenFile(tmpPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := graph.Sync(); err != nil {
		_ = graph.Close()
		return err
	}
	_ = graph.Close()
	graphDigest, err := stateVolumeFileSHA256(tmpPath)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, destination); err != nil {
		return err
	}
	if err := publishBlockV1GraphIdentity(identityPath, blockV1GraphIdentity{
		ManifestDigest: manifestDigest, BackingPath: canonicalOptionalStateVolumePath(backingPath),
		VirtualSizeBytes: virtualSizeBytes, GraphDigest: graphDigest,
	}); err != nil {
		return err
	}
	return syncStateVolumeDirectory(dir)
}

type blockV1GraphIdentity struct {
	Version          string `json:"version"`
	ManifestDigest   string `json:"manifest_digest"`
	BackingPath      string `json:"backing_path,omitempty"`
	VirtualSizeBytes int64  `json:"virtual_size_bytes"`
	GraphDigest      string `json:"graph_digest"`
}

func canonicalOptionalStateVolumePath(path string) string {
	if path == "" {
		return ""
	}
	canonical, err := canonicalStateVolumePath(path)
	if err != nil {
		return ""
	}
	return canonical
}

func validateBlockV1GraphCache(ctx context.Context, graphPath, identityPath, backingPath string, virtualSizeBytes int64, manifestDigest string, images StateVolumeImageTool) error {
	if err := images.Check(ctx, graphPath); err != nil {
		return err
	}
	imageInfo, err := images.Info(ctx, graphPath)
	if err != nil {
		return err
	}
	if err := validateStateVolumeImageInfo(imageInfo, virtualSizeBytes, backingPath); err != nil {
		return err
	}
	identityInfo, err := os.Lstat(identityPath)
	if err != nil {
		return err
	}
	if !identityInfo.Mode().IsRegular() || identityInfo.Mode()&os.ModeSymlink != 0 || identityInfo.Size() > 4096 {
		return fmt.Errorf("invalid block graph identity file")
	}
	data, err := os.ReadFile(identityPath)
	if err != nil {
		return err
	}
	var identity blockV1GraphIdentity
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("block graph identity has trailing data")
	}
	if identity.Version != BlockV1Format || identity.ManifestDigest != manifestDigest ||
		identity.BackingPath != canonicalOptionalStateVolumePath(backingPath) ||
		identity.VirtualSizeBytes != virtualSizeBytes || !blockV1DigestPattern.MatchString(identity.GraphDigest) {
		return fmt.Errorf("block graph identity does not match requested manifest")
	}
	graphDigest, err := stateVolumeFileSHA256(graphPath)
	if err != nil {
		return err
	}
	if graphDigest != identity.GraphDigest {
		return fmt.Errorf("block graph cache byte digest mismatch")
	}
	return nil
}

func stateVolumeFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func publishBlockV1GraphIdentity(path string, identity blockV1GraphIdentity) error {
	identity.Version = BlockV1Format
	data, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".graph-identity-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncStateVolumeDirectory(dir)
}
