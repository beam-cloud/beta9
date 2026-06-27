package worker

import (
	"archive/tar"
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
	"strings"
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

type durableDiskSnapshotBucketStore struct {
	client *clients.WorkspaceStorageClient
	bucket string
}

func newDurableDiskSnapshotBucketStore(ctx context.Context, request *types.ContainerRequest, diskName, bucketName string, create bool) (*durableDiskSnapshotBucketStore, error) {
	if request == nil || request.Workspace.Name == "" || !workspaceStorageDownloadAvailable(request.Workspace.Storage) {
		return nil, fmt.Errorf("workspace storage credentials are required for durable disk snapshots")
	}

	client, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, fmt.Errorf("create durable disk snapshot storage client: %w", err)
	}
	if bucketName == "" {
		bucketName = durableDiskSnapshotBucketName(client.BucketName(), diskName)
	}
	if create {
		if err := client.StorageClient.EnsureBucket(ctx, bucketName); err != nil {
			return nil, fmt.Errorf("ensure durable disk snapshot bucket %s: %w", bucketName, err)
		}
	}
	return &durableDiskSnapshotBucketStore{client: client, bucket: bucketName}, nil
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

func durableDiskSnapshotBucketName(workspaceBucket, diskName string) string {
	sum := sha256.Sum256([]byte(workspaceBucket + "/" + diskName))
	suffix := hex.EncodeToString(sum[:])[:12]
	prefix := durableDiskBucketPart(workspaceBucket + "-disk-" + diskName)
	if len(prefix) > 50 {
		prefix = strings.Trim(prefix[:50], "-")
	}
	if prefix == "" {
		prefix = "beta9-disk"
	}
	return strings.Trim(prefix+"-"+suffix, "-")
}

func durableDiskBucketPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := r >= 'a' && r <= 'z' || r >= '0' && r <= '9'
		if !ok {
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
			continue
		}
		b.WriteRune(r)
		lastDash = false
	}
	return strings.Trim(b.String(), "-")
}

func createDurableDiskSnapshot(ctx context.Context, store durableDiskSnapshotStore, backingPath, objectPrefix string, snapshot types.DiskSnapshot, chunkSize int64) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
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

	file, err := os.Open(backingPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open durable disk backing file %s: %w", backingPath, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat durable disk backing file %s: %w", backingPath, err)
	}
	if info.IsDir() {
		return nil, nil, fmt.Errorf("durable disk backing path %s is a directory; snapshot a block image or backing file", backingPath)
	}

	manifest := &types.DiskSnapshotManifest{
		Version:          1,
		Format:           firstNonEmpty(snapshot.Format, types.DiskSnapshotFormatBlockV1),
		DiskName:         snapshot.DiskName,
		Filesystem:       snapshot.Filesystem,
		Generation:       snapshot.Generation,
		ParentSnapshotId: snapshot.ParentSnapshotId,
		LogicalSizeBytes: info.Size(),
		CreatedAt:        time.Now().UTC(),
	}

	chunkPrefix := durableDiskSnapshotChunkPrefix(objectPrefix)
	buffer := make([]byte, int(chunkSize))
	seen := map[string]struct{}{}
	for index, offset := int64(0), int64(0); ; index++ {
		n, readErr := file.Read(buffer)
		if n > 0 {
			chunk := buffer[:n]
			sum := sha256.Sum256(chunk)
			digest := "sha256:" + hex.EncodeToString(sum[:])
			key := path.Join(chunkPrefix, hex.EncodeToString(sum[:]))

			if _, ok := seen[key]; !ok {
				exists, err := store.Exists(ctx, key)
				if err != nil {
					return nil, nil, fmt.Errorf("check durable disk snapshot chunk %s: %w", key, err)
				}
				if !exists {
					if err := store.UploadWithReader(ctx, key, bytes.NewReader(chunk)); err != nil {
						return nil, nil, fmt.Errorf("upload durable disk snapshot chunk %s: %w", key, err)
					}
				}
				seen[key] = struct{}{}
			}

			manifest.Chunks = append(manifest.Chunks, types.DiskSnapshotChunk{
				Index:       index,
				OffsetBytes: offset,
				SizeBytes:   int64(n),
				ObjectKey:   key,
				Digest:      digest,
			})
			manifest.StoredSizeBytes += int64(n)
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, nil, fmt.Errorf("read durable disk backing file %s: %w", backingPath, readErr)
		}
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal durable disk snapshot manifest: %w", err)
	}
	manifestSum := sha256.Sum256(data)
	manifestKey := path.Join(objectPrefix, "manifest.json")
	if err := store.Upload(ctx, manifestKey, data); err != nil {
		return nil, nil, fmt.Errorf("upload durable disk snapshot manifest %s: %w", manifestKey, err)
	}

	snapshot.Format = manifest.Format
	snapshot.Status = types.DiskSnapshotStatusAvailable
	snapshot.ObjectPrefix = objectPrefix
	snapshot.ManifestKey = manifestKey
	snapshot.ManifestDigest = "sha256:" + hex.EncodeToString(manifestSum[:])
	snapshot.ManifestSizeBytes = int64(len(data))
	snapshot.ChunkCount = int64(len(manifest.Chunks))
	snapshot.LogicalSizeBytes = manifest.LogicalSizeBytes
	snapshot.StoredSizeBytes = manifest.StoredSizeBytes
	return &snapshot, manifest, nil
}

func durableDiskSnapshotChunkPrefix(objectPrefix string) string {
	root := path.Dir(path.Dir(objectPrefix))
	if root == "." || root == "/" {
		return path.Join(objectPrefix, "chunks")
	}
	return path.Join(root, "chunks")
}

func restoreDurableDiskSnapshot(ctx context.Context, store durableDiskSnapshotStore, manifestKey, backingPath string) (*types.DiskSnapshotManifest, error) {
	if store == nil {
		return nil, fmt.Errorf("durable disk snapshot store is nil")
	}

	manifestReader, err := store.DownloadWithReader(ctx, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("download durable disk snapshot manifest %s: %w", manifestKey, err)
	}
	defer manifestReader.Close()

	var manifest types.DiskSnapshotManifest
	if err := json.NewDecoder(manifestReader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode durable disk snapshot manifest %s: %w", manifestKey, err)
	}

	if err := os.MkdirAll(filepath.Dir(backingPath), 0755); err != nil {
		return nil, fmt.Errorf("create durable disk restore directory %s: %w", filepath.Dir(backingPath), err)
	}
	tmpPath := backingPath + ".restore-tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("create durable disk restore file %s: %w", tmpPath, err)
	}
	defer os.Remove(tmpPath)

	for _, chunk := range manifest.Chunks {
		if _, err := out.Seek(chunk.OffsetBytes, io.SeekStart); err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("seek durable disk restore file %s: %w", tmpPath, err)
		}
		reader, err := store.DownloadWithReader(ctx, chunk.ObjectKey)
		if err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("download durable disk snapshot chunk %s: %w", chunk.ObjectKey, err)
		}
		sum := sha256.New()
		n, copyErr := io.CopyN(out, io.TeeReader(reader, sum), chunk.SizeBytes)
		closeErr := reader.Close()
		if copyErr != nil {
			_ = out.Close()
			return nil, fmt.Errorf("restore durable disk snapshot chunk %s: copied %d of %d bytes: %w", chunk.ObjectKey, n, chunk.SizeBytes, copyErr)
		}
		if closeErr != nil {
			_ = out.Close()
			return nil, fmt.Errorf("close durable disk snapshot chunk %s: %w", chunk.ObjectKey, closeErr)
		}
		if digest := "sha256:" + hex.EncodeToString(sum.Sum(nil)); digest != chunk.Digest {
			_ = out.Close()
			return nil, fmt.Errorf("durable disk snapshot chunk %s digest mismatch: got %s want %s", chunk.ObjectKey, digest, chunk.Digest)
		}
	}

	if err := out.Truncate(manifest.LogicalSizeBytes); err != nil {
		_ = out.Close()
		return nil, fmt.Errorf("truncate durable disk restore file %s: %w", tmpPath, err)
	}
	if err := out.Close(); err != nil {
		return nil, fmt.Errorf("close durable disk restore file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, backingPath); err != nil {
		return nil, fmt.Errorf("install durable disk restore file %s: %w", backingPath, err)
	}
	return &manifest, nil
}

func createDurableDiskDirectorySnapshot(ctx context.Context, store durableDiskSnapshotStore, sourceDir, objectPrefix string, snapshot types.DiskSnapshot, chunkSize int64) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	if !devDurableDiskHasPayload(sourceDir) {
		return nil, nil, fmt.Errorf("durable disk directory %s has no payload", sourceDir)
	}

	tmp, err := os.CreateTemp("", "beta9-disk-snapshot-*.tar")
	if err != nil {
		return nil, nil, fmt.Errorf("create durable disk snapshot tar: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := writeDurableDiskTar(tmp, sourceDir); err != nil {
		_ = tmp.Close()
		return nil, nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, nil, fmt.Errorf("close durable disk snapshot tar %s: %w", tmpPath, err)
	}

	snapshot.Format = types.DiskSnapshotFormatTarV1
	return createDurableDiskSnapshot(ctx, store, tmpPath, objectPrefix, snapshot, chunkSize)
}

func restoreDurableDiskDirectorySnapshot(ctx context.Context, store durableDiskSnapshotStore, manifestKey, targetDir string) (*types.DiskSnapshotManifest, error) {
	tmp, err := os.CreateTemp("", "beta9-disk-restore-*.tar")
	if err != nil {
		return nil, fmt.Errorf("create durable disk restore tar: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	manifest, err := restoreDurableDiskSnapshot(ctx, store, manifestKey, tmpPath)
	if err != nil {
		return nil, err
	}
	if err := os.RemoveAll(targetDir); err != nil {
		return nil, fmt.Errorf("clear durable disk restore directory %s: %w", targetDir, err)
	}
	if err := extractDurableDiskTar(tmpPath, targetDir); err != nil {
		return nil, err
	}
	return manifest, nil
}

func writeDurableDiskTar(out io.Writer, sourceDir string) error {
	tw := tar.NewWriter(out)
	defer tw.Close()

	return filepath.WalkDir(sourceDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if path == sourceDir {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}

		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		_, copyErr := io.Copy(tw, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func extractDurableDiskTar(tarPath, targetDir string) error {
	file, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("open durable disk restore tar %s: %w", tarPath, err)
	}
	defer file.Close()

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create durable disk restore directory %s: %w", targetDir, err)
	}

	tr := tar.NewReader(file)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read durable disk restore tar %s: %w", tarPath, err)
		}

		targetPath, err := durableDiskTarTarget(targetDir, header.Name)
		if err != nil {
			return err
		}
		mode := os.FileMode(header.Mode)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, mode.Perm()); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			}
			out, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(out, tr)
			closeErr := out.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return err
			}
		default:
			continue
		}
		_ = os.Chown(targetPath, header.Uid, header.Gid)
		_ = os.Chtimes(targetPath, header.ModTime, header.ModTime)
	}
}

func durableDiskTarTarget(root, name string) (string, error) {
	name = filepath.Clean(name)
	if name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(os.PathSeparator)) || name == ".." {
		return "", fmt.Errorf("invalid durable disk tar path %q", name)
	}

	target := filepath.Join(root, name)
	if rel, err := filepath.Rel(root, target); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("durable disk tar path escapes target: %q", name)
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
