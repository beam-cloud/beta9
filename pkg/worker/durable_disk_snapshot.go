package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultDurableDiskSnapshotChunkSize int64 = 64 << 20

type durableDiskSnapshotStore interface {
	Exists(ctx context.Context, key string) (bool, error)
	Upload(ctx context.Context, key string, data []byte) error
	UploadWithReader(ctx context.Context, key string, data io.Reader) error
	DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error)
}

type durableDiskSnapshotCacheReader interface {
	GetContent(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]byte, error)
}

type durableDiskSnapshotBucketStore struct {
	client *clients.WorkspaceStorageClient
	bucket string
}

func newDurableDiskSnapshotWriteStore(ctx context.Context, request *types.ContainerRequest) (*durableDiskSnapshotBucketStore, error) {
	client, err := newDurableDiskSnapshotStorageClient(ctx, request)
	if err != nil {
		return nil, err
	}

	bucketName := client.BucketName()
	if err := client.EnsureLocalBucket(ctx); err != nil {
		return nil, fmt.Errorf("ensure durable disk snapshot bucket %s: %w", bucketName, err)
	}
	return &durableDiskSnapshotBucketStore{client: client, bucket: bucketName}, nil
}

func newDurableDiskSnapshotReadStore(ctx context.Context, request *types.ContainerRequest, bucketName string) (*durableDiskSnapshotBucketStore, error) {
	client, err := newDurableDiskSnapshotStorageClient(ctx, request)
	if err != nil {
		return nil, err
	}
	if bucketName == "" {
		bucketName = client.BucketName()
	}
	return &durableDiskSnapshotBucketStore{client: client, bucket: bucketName}, nil
}

func newDurableDiskSnapshotStorageClient(ctx context.Context, request *types.ContainerRequest) (*clients.WorkspaceStorageClient, error) {
	if request == nil || request.Workspace.Name == "" || !workspaceStorageDownloadAvailable(request.Workspace.Storage) {
		return nil, fmt.Errorf("workspace storage credentials are required for durable disk snapshots")
	}

	client, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, fmt.Errorf("create durable disk snapshot storage client: %w", err)
	}
	return client, nil
}

func (s *durableDiskSnapshotBucketStore) Exists(ctx context.Context, key string) (bool, error) {
	return s.client.StorageClient.Exists(ctx, key, s.bucket)
}

func (s *durableDiskSnapshotBucketStore) Upload(ctx context.Context, key string, data []byte) error {
	return s.client.StorageClient.UploadToBucket(ctx, key, data, s.bucket)
}

func (s *durableDiskSnapshotBucketStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	return s.client.StorageClient.UploadToBucketWithReader(ctx, key, data, s.bucket)
}

func (s *durableDiskSnapshotBucketStore) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.client.StorageClient.DownloadWithReader(ctx, key, s.bucket)
}

func uploadDurableDiskSnapshotManifest(ctx context.Context, store durableDiskSnapshotStore, manifestKey string, manifest *types.DiskSnapshotManifest) (string, int64, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", 0, fmt.Errorf("marshal durable disk snapshot manifest: %w", err)
	}
	sum := sha256.Sum256(data)
	if err := store.Upload(ctx, manifestKey, data); err != nil {
		return "", 0, fmt.Errorf("upload durable disk snapshot manifest %s: %w", manifestKey, err)
	}
	return "sha256:" + hex.EncodeToString(sum[:]), int64(len(data)), nil
}

func durableDiskSnapshotChunkPrefix(objectPrefix string) string {
	root := path.Dir(path.Dir(objectPrefix))
	if root == "." || root == "/" {
		return path.Join(objectPrefix, "chunks")
	}
	return path.Join(root, "chunks")
}

func durableDiskSnapshotObjectReader(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, key, digest string, sizeBytes int64) (io.ReadCloser, error) {
	hash := strings.TrimPrefix(digest, "sha256:")
	if cacheReader != nil && hash != "" && sizeBytes > 0 {
		data, err := cacheReader.GetContent(hash, 0, sizeBytes, struct{ RoutingKey string }{RoutingKey: hash})
		if err == nil && int64(len(data)) == sizeBytes {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}
	return store.DownloadWithReader(ctx, key)
}

func loadDurableDiskSnapshotManifest(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, snapshot *types.DiskSnapshot) (*types.DiskSnapshotManifest, error) {
	if snapshot == nil || snapshot.ManifestKey == "" {
		return nil, nil
	}
	reader, err := durableDiskSnapshotObjectReader(ctx, store, cacheReader, snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var manifest types.DiskSnapshotManifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func createDurableDiskDirectorySnapshot(ctx context.Context, store durableDiskSnapshotStore, sourceDir, objectPrefix string, snapshot types.DiskSnapshot, chunkSize int64, previous *types.DiskSnapshotManifest) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	if store == nil {
		return nil, nil, fmt.Errorf("durable disk snapshot store is nil")
	}
	objectPrefix = strings.Trim(strings.TrimSpace(objectPrefix), "/")
	if objectPrefix == "" {
		return nil, nil, fmt.Errorf("durable disk snapshot object prefix is required")
	}
	if chunkSize <= 0 {
		chunkSize = defaultDurableDiskSnapshotChunkSize
	}
	if chunkSize > int64(int(chunkSize)) {
		return nil, nil, fmt.Errorf("durable disk snapshot chunk size %d is too large", chunkSize)
	}

	manifest := &types.DiskSnapshotManifest{
		Version:          1,
		Format:           firstNonEmpty(snapshot.Format, types.DiskSnapshotFormatDirV1),
		DiskName:         snapshot.DiskName,
		Filesystem:       snapshot.Filesystem,
		Generation:       snapshot.Generation,
		ParentSnapshotId: snapshot.ParentSnapshotId,
		CreatedAt:        time.Now().UTC(),
	}

	previousFiles := durableDiskSnapshotFilesByPath(previous)
	chunkPrefix := durableDiskSnapshotChunkPrefix(objectPrefix)
	files := map[string]types.DiskSnapshotFile{}
	seen := durableDiskSnapshotSeenChunks(previous, chunkPrefix)
	buffer := make([]byte, int(chunkSize))

	if err := filepath.WalkDir(sourceDir, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if name == sourceDir {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(sourceDir, name)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == durableDiskMarkerFile {
			return nil
		}
		file := durableDiskSnapshotFile(rel, info)

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			file.Type = "symlink"
			file.LinkName, err = os.Readlink(name)
			if err != nil {
				return err
			}
		case info.IsDir():
			file.Type = "dir"
		case info.Mode().IsRegular():
			file.Type = "file"
			previousFile := previousFiles[file.Path]
			reusable, err := durableDiskSnapshotFileReusable(name, previousFile, file)
			if err != nil {
				return err
			}
			if reusable {
				file.Chunks = append([]types.DiskSnapshotChunk(nil), previousFile.Chunks...)
			} else if durableDiskSnapshotAppendOnlyFile(manifest.Format, file.Path) && durableDiskSnapshotFileAppendReusable(previousFile, file) {
				reusePrefix, err := durableDiskSnapshotFileChunksReusable(name, previousFile)
				if err != nil {
					return err
				}
				if reusePrefix {
					file.Chunks = append([]types.DiskSnapshotChunk(nil), previousFile.Chunks...)
				}
				if err := snapshotDurableDiskFile(ctx, store, name, chunkPrefix, buffer, seen, &file); err != nil {
					return err
				}
			} else if err := snapshotDurableDiskFile(ctx, store, name, chunkPrefix, buffer, seen, &file); err != nil {
				return err
			}
		default:
			return nil
		}

		files[file.Path] = file
		return nil
	}); err != nil {
		return nil, nil, err
	}

	manifest.Files = durableDiskSnapshotSortedFiles(files)
	logicalSizeBytes, storedSizeBytes, chunkCount := durableDiskSnapshotManifestStats(manifest.Files)
	manifest.LogicalSizeBytes = logicalSizeBytes
	manifest.StoredSizeBytes = storedSizeBytes

	manifestKey := path.Join(objectPrefix, "manifest.json")
	manifestDigest, manifestSizeBytes, err := uploadDurableDiskSnapshotManifest(ctx, store, manifestKey, manifest)
	if err != nil {
		return nil, nil, err
	}

	snapshot.Format = manifest.Format
	snapshot.Status = types.DiskSnapshotStatusAvailable
	snapshot.ObjectPrefix = objectPrefix
	snapshot.ManifestKey = manifestKey
	snapshot.ManifestDigest = manifestDigest
	snapshot.ManifestSizeBytes = manifestSizeBytes
	snapshot.ChunkCount = chunkCount
	snapshot.LogicalSizeBytes = manifest.LogicalSizeBytes
	snapshot.StoredSizeBytes = manifest.StoredSizeBytes
	return &snapshot, manifest, nil
}

func durableDiskSnapshotFilesByPath(manifest *types.DiskSnapshotManifest) map[string]types.DiskSnapshotFile {
	if manifest == nil || len(manifest.Files) == 0 {
		return nil
	}
	files := make(map[string]types.DiskSnapshotFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	return files
}

func durableDiskSnapshotSortedFiles(files map[string]types.DiskSnapshotFile) []types.DiskSnapshotFile {
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	result := make([]types.DiskSnapshotFile, 0, len(paths))
	for _, path := range paths {
		result = append(result, files[path])
	}
	return result
}

func durableDiskSnapshotManifestStats(files []types.DiskSnapshotFile) (logicalSizeBytes, storedSizeBytes, chunkCount int64) {
	for _, file := range files {
		if file.Type != "file" {
			continue
		}
		logicalSizeBytes += file.SizeBytes
		storedSizeBytes += durableDiskSnapshotFileStoredBytes(file.Chunks)
		chunkCount += int64(len(file.Chunks))
	}
	return logicalSizeBytes, storedSizeBytes, chunkCount
}

func durableDiskSnapshotFile(name string, info os.FileInfo) types.DiskSnapshotFile {
	file := types.DiskSnapshotFile{
		Path:            name,
		Mode:            int64(info.Mode()),
		SizeBytes:       info.Size(),
		ModTimeUnixNano: info.ModTime().UnixNano(),
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		file.Uid = int(stat.Uid)
		file.Gid = int(stat.Gid)
		file.ChangeUnixNano = durableDiskSnapshotChangeTimeUnixNano(stat)
		file.DeviceId = uint64(stat.Dev)
		file.Inode = uint64(stat.Ino)
	}
	return file
}

func durableDiskSnapshotFileReusable(filename string, previous, current types.DiskSnapshotFile) (bool, error) {
	metadataMatches := previous.Type == current.Type &&
		previous.Mode == current.Mode &&
		previous.Uid == current.Uid &&
		previous.Gid == current.Gid &&
		previous.SizeBytes == current.SizeBytes &&
		previous.ModTimeUnixNano == current.ModTimeUnixNano &&
		previous.ChangeUnixNano == current.ChangeUnixNano &&
		durableDiskSnapshotSameIdentity(previous, current) &&
		len(previous.Chunks) > 0
	if !metadataMatches {
		return false, nil
	}
	return durableDiskSnapshotFileChunksReusable(filename, previous)
}

func durableDiskSnapshotSameIdentity(previous, current types.DiskSnapshotFile) bool {
	return previous.DeviceId != 0 &&
		previous.Inode != 0 &&
		previous.DeviceId == current.DeviceId &&
		previous.Inode == current.Inode
}

func durableDiskSnapshotChangeTimeUnixNano(stat *syscall.Stat_t) int64 {
	if stat == nil {
		return 0
	}
	value := reflect.ValueOf(*stat)
	for _, name := range []string{"Ctim", "Ctimespec"} {
		field := value.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		sec := field.FieldByName("Sec")
		nsec := field.FieldByName("Nsec")
		if sec.IsValid() && nsec.IsValid() {
			return sec.Int()*int64(time.Second) + nsec.Int()
		}
	}
	return 0
}

func durableDiskSnapshotFileAppendReusable(previous, current types.DiskSnapshotFile) bool {
	if previous.Type != "file" || current.Type != "file" || previous.SizeBytes >= current.SizeBytes {
		return false
	}
	if previous.Mode != current.Mode || previous.Uid != current.Uid || previous.Gid != current.Gid {
		return false
	}
	var end int64
	for _, chunk := range previous.Chunks {
		if chunk.OffsetBytes != end || chunk.SizeBytes <= 0 {
			return false
		}
		end += chunk.SizeBytes
	}
	return end == previous.SizeBytes
}

func durableDiskSnapshotFileChunksReusable(filename string, previous types.DiskSnapshotFile) (bool, error) {
	in, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer in.Close()

	for _, chunk := range previous.Chunks {
		if chunk.SizeBytes <= 0 || chunk.Digest == "" {
			return false, nil
		}
		if _, err := in.Seek(chunk.OffsetBytes, io.SeekStart); err != nil {
			return false, err
		}

		sum := sha256.New()
		if n, err := io.CopyN(sum, in, chunk.SizeBytes); err != nil || n != chunk.SizeBytes {
			return false, nil
		}
		if "sha256:"+hex.EncodeToString(sum.Sum(nil)) != chunk.Digest {
			return false, nil
		}
	}
	return true, nil
}

func durableDiskSnapshotAppendOnlyFile(format, name string) bool {
	name = filepath.ToSlash(name)
	switch format {
	case types.DiskSnapshotFormatPostgresWalV1:
		return durableDiskSnapshotPostgresWALFile(name)
	case types.DiskSnapshotFormatRedisAOFV1:
		return strings.HasSuffix(path.Base(name), ".aof")
	default:
		return false
	}
}

func durableDiskSnapshotPostgresWALFile(name string) bool {
	name = filepath.ToSlash(name)
	return strings.HasPrefix(name, "pgdata/pg_wal/")
}

func durableDiskSnapshotSeenChunks(previous *types.DiskSnapshotManifest, chunkPrefix string) map[string]struct{} {
	seen := map[string]struct{}{}
	if previous == nil {
		return seen
	}
	for _, file := range previous.Files {
		for _, chunk := range file.Chunks {
			if chunk.ObjectKey != "" {
				seen[chunk.ObjectKey] = struct{}{}
			}
			if digest := strings.TrimPrefix(chunk.Digest, "sha256:"); digest != "" {
				seen[path.Join(chunkPrefix, digest)] = struct{}{}
			}
		}
	}
	return seen
}

func snapshotDurableDiskFile(ctx context.Context, store durableDiskSnapshotStore, filename, chunkPrefix string, buffer []byte, seen map[string]struct{}, file *types.DiskSnapshotFile) error {
	in, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer in.Close()

	var index, offset int64
	if len(file.Chunks) > 0 {
		last := file.Chunks[len(file.Chunks)-1]
		index = last.Index + 1
		offset = last.OffsetBytes + last.SizeBytes
		if _, err := in.Seek(offset, io.SeekStart); err != nil {
			return err
		}
	}

	for ; ; index++ {
		n, readErr := in.Read(buffer)
		if n > 0 {
			chunk := buffer[:n]
			sum := sha256.Sum256(chunk)
			hash := hex.EncodeToString(sum[:])
			key := path.Join(chunkPrefix, hash)
			if _, ok := seen[key]; !ok {
				exists, err := store.Exists(ctx, key)
				if err != nil {
					return fmt.Errorf("check durable disk snapshot chunk %s: %w", key, err)
				}
				if !exists {
					if err := store.UploadWithReader(ctx, key, bytes.NewReader(chunk)); err != nil {
						return fmt.Errorf("upload durable disk snapshot chunk %s: %w", key, err)
					}
				}
				seen[key] = struct{}{}
			}
			file.Chunks = append(file.Chunks, types.DiskSnapshotChunk{
				Index:       index,
				OffsetBytes: offset,
				SizeBytes:   int64(n),
				ObjectKey:   key,
				Digest:      "sha256:" + hash,
			})
			offset += int64(n)
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read durable disk snapshot file %s: %w", filename, readErr)
		}
	}
}

func durableDiskSnapshotFileStoredBytes(chunks []types.DiskSnapshotChunk) int64 {
	var n int64
	for _, chunk := range chunks {
		n += chunk.SizeBytes
	}
	return n
}

func restoreDurableDiskDirectorySnapshotWithCache(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, manifestKey, manifestDigest string, manifestSizeBytes int64, targetDir string) (*types.DiskSnapshotManifest, error) {
	manifest, err := loadDurableDiskSnapshotManifest(ctx, store, cacheReader, &types.DiskSnapshot{
		ManifestKey:       manifestKey,
		ManifestDigest:    manifestDigest,
		ManifestSizeBytes: manifestSizeBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("download durable disk snapshot manifest %s: %w", manifestKey, err)
	}
	return manifest, restoreDurableDiskDirectoryManifest(ctx, store, cacheReader, manifest, targetDir)
}

func restoreDurableDiskDirectoryManifest(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, manifest *types.DiskSnapshotManifest, targetDir string) error {
	if err := os.RemoveAll(targetDir); err != nil {
		return fmt.Errorf("clear durable disk restore directory %s: %w", targetDir, err)
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create durable disk restore directory %s: %w", targetDir, err)
	}

	for _, file := range manifest.Files {
		targetPath, err := durableDiskRestoreTarget(targetDir, file.Path)
		if err != nil {
			return err
		}
		mode := os.FileMode(file.Mode)
		switch file.Type {
		case "dir":
			if err := os.MkdirAll(targetPath, mode.Perm()); err != nil {
				return err
			}
		case "symlink":
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			}
			if err := os.Symlink(file.LinkName, targetPath); err != nil && !os.IsExist(err) {
				return err
			}
		case "file":
			if err := restoreDurableDiskManifestFile(ctx, store, cacheReader, file, targetPath, mode.Perm()); err != nil {
				return err
			}
		default:
			continue
		}
		_ = os.Chown(targetPath, file.Uid, file.Gid)
		if file.ModTimeUnixNano > 0 {
			modTime := time.Unix(0, file.ModTimeUnixNano)
			_ = os.Chtimes(targetPath, modTime, modTime)
		}
	}
	return nil
}

func restoreDurableDiskManifestFile(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, file types.DiskSnapshotFile, targetPath string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	for _, chunk := range file.Chunks {
		if _, err := out.Seek(chunk.OffsetBytes, io.SeekStart); err != nil {
			_ = out.Close()
			return err
		}
		reader, err := durableDiskSnapshotObjectReader(ctx, store, cacheReader, chunk.ObjectKey, chunk.Digest, chunk.SizeBytes)
		if err != nil {
			_ = out.Close()
			return fmt.Errorf("download durable disk snapshot chunk %s: %w", chunk.ObjectKey, err)
		}
		sum := sha256.New()
		n, copyErr := io.CopyN(out, io.TeeReader(reader, sum), chunk.SizeBytes)
		closeErr := reader.Close()
		if copyErr != nil {
			_ = out.Close()
			return fmt.Errorf("restore durable disk snapshot chunk %s: copied %d of %d bytes: %w", chunk.ObjectKey, n, chunk.SizeBytes, copyErr)
		}
		if closeErr != nil {
			_ = out.Close()
			return fmt.Errorf("close durable disk snapshot chunk %s: %w", chunk.ObjectKey, closeErr)
		}
		if digest := "sha256:" + hex.EncodeToString(sum.Sum(nil)); digest != chunk.Digest {
			_ = out.Close()
			return fmt.Errorf("durable disk snapshot chunk %s digest mismatch: got %s want %s", chunk.ObjectKey, digest, chunk.Digest)
		}
	}
	if err := out.Truncate(file.SizeBytes); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func durableDiskRestoreTarget(root, name string) (string, error) {
	name = filepath.Clean(name)
	if name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(os.PathSeparator)) || name == ".." {
		return "", fmt.Errorf("invalid durable disk snapshot path %q", name)
	}

	target := filepath.Join(root, name)
	if rel, err := filepath.Rel(root, target); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("durable disk snapshot path escapes target: %q", name)
	}
	return target, nil
}

func durableDiskSnapshotToProto(snapshot *types.DiskSnapshot) *pb.DiskSnapshot {
	if snapshot == nil {
		return nil
	}

	return &pb.DiskSnapshot{
		ExternalId:          snapshot.ExternalId,
		DiskName:            snapshot.DiskName,
		Format:              snapshot.Format,
		Status:              string(snapshot.Status),
		Reason:              snapshot.Reason,
		ParentSnapshotId:    snapshot.ParentSnapshotId,
		Generation:          snapshot.Generation,
		SizeBytes:           snapshot.SizeBytes,
		Filesystem:          snapshot.Filesystem,
		Driver:              snapshot.Driver,
		ManifestKey:         snapshot.ManifestKey,
		ManifestDigest:      snapshot.ManifestDigest,
		ManifestSizeBytes:   snapshot.ManifestSizeBytes,
		ChunkCount:          snapshot.ChunkCount,
		LogicalSizeBytes:    snapshot.LogicalSizeBytes,
		StoredSizeBytes:     snapshot.StoredSizeBytes,
		BucketName:          snapshot.BucketName,
		ObjectPrefix:        snapshot.ObjectPrefix,
		SourcePool:          snapshot.SourcePool,
		SourceWorkerId:      snapshot.SourceWorkerId,
		SourceStorageNodeId: snapshot.SourceStorageNodeId,
		CreatedAt:           timestamppb.New(snapshot.CreatedAt.Time),
		UpdatedAt:           timestamppb.New(snapshot.UpdatedAt.Time),
	}
}

func durableDiskSnapshotFromProto(in *pb.DiskSnapshot) *types.DiskSnapshot {
	if in == nil {
		return nil
	}

	return &types.DiskSnapshot{
		ExternalId:          in.ExternalId,
		DiskName:            in.DiskName,
		Format:              in.Format,
		Status:              types.DiskSnapshotStatus(in.Status),
		Reason:              in.Reason,
		ParentSnapshotId:    in.ParentSnapshotId,
		Generation:          in.Generation,
		SizeBytes:           in.SizeBytes,
		Filesystem:          in.Filesystem,
		Driver:              in.Driver,
		ManifestKey:         in.ManifestKey,
		ManifestDigest:      in.ManifestDigest,
		ManifestSizeBytes:   in.ManifestSizeBytes,
		ChunkCount:          in.ChunkCount,
		LogicalSizeBytes:    in.LogicalSizeBytes,
		StoredSizeBytes:     in.StoredSizeBytes,
		BucketName:          in.BucketName,
		ObjectPrefix:        in.ObjectPrefix,
		SourcePool:          in.SourcePool,
		SourceWorkerId:      in.SourceWorkerId,
		SourceStorageNodeId: in.SourceStorageNodeId,
	}
}
