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
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultDurableDiskSnapshotChunkSize  int64 = 16 << 20
	durableDiskSnapshotUploadConcurrency       = 16
	durableDiskRestoreConcurrency              = 8
	durableDiskSnapshotReadBufferSize          = 1 << 20
	durableDiskSnapshotUploadAttempts          = 3
	durableDiskSnapshotRetryDelay              = 250 * time.Millisecond
	// Chunks up to this size are kept in memory rather than spooled to a temp file.
	durableDiskSnapshotInlineChunkSize = 1 << 20
)

// Files read, hashed and uploaded at once. Bounded by memory, not cores: these wait on the store.
const durableDiskSnapshotFileConcurrency = 16

var durableDiskSnapshotChunkSlots = make(chan struct{}, durableDiskSnapshotUploadConcurrency)

// Chunk downloads in flight across every file being restored, so more files widen the pipe.
var durableDiskRestoreChunkSlots = make(chan struct{}, durableDiskRestoreConcurrency)

var errDurableDiskSnapshotInactive = errors.New("durable disk snapshot made no progress before the inactivity deadline")

func withDurableDiskInactivityWatchdog(ctx context.Context, timeout time.Duration) (context.Context, func()) {
	if timeout <= 0 {
		return ctx, func() {}
	}

	upstream, _ := ctx.Value(durableDiskProgressReporterKey{}).(func(durableDiskProgressEvent))
	watchCtx, cancel := context.WithCancelCause(ctx)
	progress := make(chan struct{}, 1)
	done := make(chan struct{})
	report := func(event durableDiskProgressEvent) {
		select {
		case progress <- struct{}{}:
		default:
		}
		if upstream != nil {
			upstream(event)
		}
	}
	watchCtx = withDurableDiskProgressReporter(watchCtx, report)

	go func() {
		defer close(done)
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-progress:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				// Prefer progress already queued at the deadline boundary.
				select {
				case <-progress:
					timer.Reset(timeout)
					continue
				default:
				}
				cancel(errDurableDiskSnapshotInactive)
				return
			case <-watchCtx.Done():
				return
			}
		}
	}()

	return watchCtx, func() {
		cancel(context.Canceled)
		<-done
	}
}

type durableDiskSnapshotStore interface {
	Upload(ctx context.Context, key string, data []byte) error
	UploadWithReader(ctx context.Context, key string, data io.Reader) error
	DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error)
}

type durableDiskSnapshotCacheReader interface {
	ReadContentInto(ctx context.Context, hash string, offset int64, dest []byte, opts cache.ClientOptions) (int64, error)
}

type durableDiskSnapshotBucketStore struct {
	client *clients.WorkspaceStorageClient
	bucket string
}

type durableDiskSnapshotURLStore struct {
	resolveURL  func(context.Context, *pb.GetDiskSnapshotDownloadURLRequest) (*pb.GetDiskSnapshotDownloadURLResponse, error)
	workspaceID string
	snapshotID  string
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

func newDurableDiskSnapshotReadStore(ctx context.Context, request *types.ContainerRequest, snapshot *types.DiskSnapshot, backendRepoClient pb.BackendRepositoryServiceClient) (durableDiskSnapshotStore, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("durable disk snapshot is required")
	}
	if snapshot.Public {
		if backendRepoClient == nil {
			return nil, fmt.Errorf("backend repository client is required for public disk snapshots")
		}
		return &durableDiskSnapshotURLStore{
			resolveURL: func(ctx context.Context, req *pb.GetDiskSnapshotDownloadURLRequest) (*pb.GetDiskSnapshotDownloadURLResponse, error) {
				return backendRepoClient.GetDiskSnapshotDownloadURL(ctx, req)
			},
			workspaceID: cacheRequestWorkspaceID(request),
			snapshotID:  snapshot.ExternalId,
		}, nil
	}

	client, err := newDurableDiskSnapshotStorageClient(ctx, request)
	if err != nil {
		return nil, err
	}
	bucketName := snapshot.BucketName
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

func (s *durableDiskSnapshotBucketStore) Upload(ctx context.Context, key string, data []byte) error {
	return s.client.StorageClient.UploadToBucket(ctx, key, data, s.bucket)
}

func (s *durableDiskSnapshotBucketStore) UploadWithReader(ctx context.Context, key string, data io.Reader) error {
	return s.client.StorageClient.UploadToBucketWithReader(ctx, key, data, s.bucket)
}

func (s *durableDiskSnapshotBucketStore) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.client.StorageClient.DownloadWithReader(ctx, key, s.bucket)
}

func (s *durableDiskSnapshotURLStore) Upload(context.Context, string, []byte) error {
	return fmt.Errorf("public disk snapshot store is read-only")
}

func (s *durableDiskSnapshotURLStore) UploadWithReader(context.Context, string, io.Reader) error {
	return fmt.Errorf("public disk snapshot store is read-only")
}

func (s *durableDiskSnapshotURLStore) DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := handleGRPCResponse(s.resolveURL(ctx, &pb.GetDiskSnapshotDownloadURLRequest{
		WorkspaceId: s.workspaceID,
		SnapshotId:  s.snapshotID,
		ObjectKey:   key,
	}))
	if err != nil {
		return nil, fmt.Errorf("resolve public disk snapshot object: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("create public disk snapshot request: %w", err)
	}
	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download public disk snapshot object: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		return nil, fmt.Errorf("download public disk snapshot object: status %d", httpResp.StatusCode)
	}
	return httpResp.Body, nil
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
	if index := strings.LastIndex(objectPrefix, "/snapshots/"); index >= 0 {
		return path.Join(objectPrefix[:index], "chunks")
	}
	root := path.Dir(path.Dir(objectPrefix))
	if root == "." || root == "/" {
		return path.Join(objectPrefix, "chunks")
	}
	return path.Join(root, "chunks")
}

func durableDiskSnapshotObjectReader(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, key, digest string, sizeBytes int64) (io.ReadCloser, error) {
	hash := strings.TrimPrefix(digest, "sha256:")
	if cacheReader != nil && hash != "" && sizeBytes > 0 {
		cacheCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		data := make([]byte, sizeBytes)
		n, err := cacheReader.ReadContentInto(cacheCtx, hash, 0, data, cache.ClientOptions{RoutingKey: hash})
		cancel()
		if err == nil && n == sizeBytes {
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

// createDurableDiskDirectorySnapshot returns nil only for an explicit live
// snapshot that elects to reuse an unchanged generation. Terminal cleanup
// disables skipUnchanged and therefore always publishes a new manifest.
func createDurableDiskDirectorySnapshot(ctx context.Context, store durableDiskSnapshotStore, sourceDir, objectPrefix string, snapshot types.DiskSnapshot, chunkSize int64, previous *types.DiskSnapshotManifest, skipUnchanged bool) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	if store == nil {
		return nil, nil, fmt.Errorf("durable disk snapshot store is nil")
	}
	objectPrefix = strings.Trim(strings.TrimSpace(objectPrefix), "/")
	if objectPrefix == "" {
		return nil, nil, fmt.Errorf("durable disk snapshot object prefix is required")
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
	seen := durableDiskSnapshotSeenChunks(previous, chunkPrefix)

	var filesMu sync.Mutex
	files := map[string]types.DiskSnapshotFile{}
	record := func(file types.DiskSnapshotFile) {
		filesMu.Lock()
		files[file.Path] = file
		filesMu.Unlock()
	}

	// On a pool: walking serially kept the upload pool one chunk deep, a round trip per file.
	content, contentCtx := errgroup.WithContext(ctx)
	content.SetLimit(durableDiskSnapshotFileConcurrency)

	walkErr := walkDurableDiskSnapshotTree(ctx, sourceDir, true, durableDiskProgressEvent{files: 1}, func(name string, file types.DiskSnapshotFile) error {
		if file.Type != "file" {
			record(file)
			return nil
		}

		// Metadata settles an unchanged file without reading it, for less than a worker costs.
		previousFile := previousFiles[file.Path]
		if durableDiskSnapshotFileReusable(previousFile, file) {
			file.Chunks = append([]types.DiskSnapshotChunk(nil), previousFile.Chunks...)
			record(file)
			return nil
		}

		reuseAppendPrefix := durableDiskSnapshotAppendOnlyFile(manifest.Format, file.Path) &&
			durableDiskSnapshotFileAppendReusable(previousFile, file)
		content.Go(func() error {
			if reuseAppendPrefix {
				reusePrefix, err := durableDiskSnapshotFileChunksReusable(contentCtx, name, previousFile)
				if err != nil {
					return err
				}
				if reusePrefix {
					file.Chunks = append([]types.DiskSnapshotChunk(nil), previousFile.Chunks...)
				}
			}
			if err := snapshotDurableDiskFile(contentCtx, store, name, chunkPrefix, chunkSize, seen, &file); err != nil {
				return err
			}
			record(file)
			return nil
		})
		// One file failing makes the rest of the walk pointless.
		return contentCtx.Err()
	})
	// Waited on first, so a walk that stopped reports the file rather than the cancellation.
	if err := content.Wait(); err != nil {
		return nil, nil, err
	}
	if walkErr != nil {
		return nil, nil, walkErr
	}

	manifest.Files = durableDiskSnapshotSortedFiles(files)
	logicalSizeBytes, storedSizeBytes, chunkCount := durableDiskSnapshotManifestStats(manifest.Files)
	manifest.LogicalSizeBytes = logicalSizeBytes
	manifest.StoredSizeBytes = storedSizeBytes
	if err := validateDurableDiskSnapshotTree(ctx, sourceDir, files); err != nil {
		return nil, nil, err
	}

	if skipUnchanged && durableDiskSnapshotContentsMatch(previous, manifest) {
		return nil, nil, nil
	}

	manifestKey := path.Join(objectPrefix, durableDiskManifestFileName)
	manifestDigest, manifestSizeBytes, err := uploadDurableDiskSnapshotManifest(ctx, store, manifestKey, manifest)
	if err != nil {
		return nil, nil, err
	}
	reportDurableDiskProgress(ctx, durableDiskProgressEvent{})

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

func durableDiskSnapshotContentsMatch(previous, current *types.DiskSnapshotManifest) bool {
	if previous == nil || current == nil {
		return false
	}
	if previous.Format != current.Format || len(previous.Files) != len(current.Files) {
		return false
	}

	for i, before := range previous.Files {
		after := current.Files[i]
		if before.Path != after.Path ||
			before.Type != after.Type ||
			before.Mode != after.Mode ||
			before.Uid != after.Uid ||
			before.Gid != after.Gid ||
			before.SizeBytes != after.SizeBytes ||
			before.ModTimeUnixNano != after.ModTimeUnixNano ||
			before.LinkName != after.LinkName ||
			!durableDiskSnapshotChunksMatch(before.Chunks, after.Chunks) {
			return false
		}
	}
	return true
}

// durableDiskSnapshotChunksMatch ignores storage object keys.
func durableDiskSnapshotChunksMatch(previous, current []types.DiskSnapshotChunk) bool {
	if len(previous) != len(current) {
		return false
	}
	for i, before := range previous {
		after := current[i]
		if before.Digest != after.Digest || before.OffsetBytes != after.OffsetBytes ||
			before.SizeBytes != after.SizeBytes {
			return false
		}
	}
	return true
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

func durableDiskSnapshotEntry(root, name string, entry os.DirEntry) (types.DiskSnapshotFile, bool, error) {
	if name == root {
		return types.DiskSnapshotFile{}, true, nil
	}
	info, err := entry.Info()
	if err != nil {
		return types.DiskSnapshotFile{}, false, err
	}
	rel, err := filepath.Rel(root, name)
	if err != nil {
		return types.DiskSnapshotFile{}, false, err
	}
	rel = filepath.ToSlash(rel)
	if rel == durableDiskMarkerFile {
		return types.DiskSnapshotFile{}, true, nil
	}

	file := durableDiskSnapshotFile(rel, info)
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		file.Type = "symlink"
		file.LinkName, err = os.Readlink(name)
	case info.IsDir():
		file.Type = "dir"
	case info.Mode().IsRegular():
		file.Type = "file"
	default:
		return types.DiskSnapshotFile{}, true, nil
	}
	return file, false, err
}

func walkDurableDiskSnapshotTree(ctx context.Context, root string, allowNotExist bool, progress durableDiskProgressEvent, visit func(string, types.DiskSnapshotFile) error) error {
	return filepath.WalkDir(root, func(name string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if allowNotExist && os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		file, skip, err := durableDiskSnapshotEntry(root, name, entry)
		if err != nil || skip {
			return err
		}
		reportDurableDiskProgress(ctx, progress)
		return visit(name, file)
	})
}

func validateDurableDiskSnapshotTree(ctx context.Context, root string, expected map[string]types.DiskSnapshotFile) error {
	seen := 0
	err := walkDurableDiskSnapshotTree(ctx, root, false, durableDiskProgressEvent{}, func(_ string, file types.DiskSnapshotFile) error {
		before, ok := expected[file.Path]
		if !ok || !durableDiskSnapshotEntryMetadataMatches(before, file) {
			return fmt.Errorf("durable disk changed while snapshotting: %s", file.Path)
		}
		seen++
		return nil
	})
	if err != nil {
		return err
	}
	if seen != len(expected) {
		return fmt.Errorf("durable disk changed while snapshotting: expected %d entries, found %d", len(expected), seen)
	}
	return nil
}

func durableDiskSnapshotEntryMetadataMatches(previous, current types.DiskSnapshotFile) bool {
	return previous.Path == current.Path &&
		previous.Type == current.Type &&
		previous.LinkName == current.LinkName &&
		previous.Mode == current.Mode &&
		previous.Uid == current.Uid &&
		previous.Gid == current.Gid &&
		previous.SizeBytes == current.SizeBytes &&
		previous.ModTimeUnixNano == current.ModTimeUnixNano &&
		previous.ChangeUnixNano == current.ChangeUnixNano &&
		previous.DeviceId == current.DeviceId &&
		previous.Inode == current.Inode
}

func durableDiskSnapshotFileReusable(previous, current types.DiskSnapshotFile) bool {
	return previous.Type == current.Type &&
		previous.Mode == current.Mode &&
		previous.Uid == current.Uid &&
		previous.Gid == current.Gid &&
		previous.SizeBytes == current.SizeBytes &&
		previous.ModTimeUnixNano == current.ModTimeUnixNano &&
		previous.ChangeUnixNano == current.ChangeUnixNano &&
		durableDiskSnapshotSameIdentity(previous, current) &&
		durableDiskSnapshotChunksCoverFile(previous)
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
	return durableDiskSnapshotChunksCoverFile(previous)
}

func durableDiskSnapshotChunksCoverFile(file types.DiskSnapshotFile) bool {
	if file.SizeBytes == 0 {
		return len(file.Chunks) == 0
	}
	var end int64
	for _, chunk := range file.Chunks {
		if chunk.OffsetBytes != end || chunk.SizeBytes <= 0 || chunk.Digest == "" || chunk.ObjectKey == "" {
			return false
		}
		end += chunk.SizeBytes
	}
	return end == file.SizeBytes
}

func durableDiskSnapshotFileChunksReusable(ctx context.Context, filename string, previous types.DiskSnapshotFile) (bool, error) {
	in, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer in.Close()

	buffer := make([]byte, durableDiskSnapshotReadBufferSize)
	for _, chunk := range previous.Chunks {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if chunk.SizeBytes <= 0 || chunk.Digest == "" {
			return false, nil
		}
		if _, err := in.Seek(chunk.OffsetBytes, io.SeekStart); err != nil {
			return false, err
		}

		sum := sha256.New()
		remaining := chunk.SizeBytes
		for remaining > 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			want := min(int64(len(buffer)), remaining)
			n, readErr := in.Read(buffer[:want])
			if n > 0 {
				_, _ = sum.Write(buffer[:n])
				remaining -= int64(n)
				reportDurableDiskProgress(ctx, durableDiskProgressEvent{logicalBytes: int64(n)})
			}
			if readErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return false, ctxErr
				}
				if errors.Is(readErr, io.EOF) && remaining == 0 {
					break
				}
				return false, nil
			}
			if n == 0 {
				return false, nil
			}
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

// durableDiskSnapshotChunkSet is every chunk the snapshot already has, claimed in one step
// because concurrent files holding the same bytes must produce one upload between them.
type durableDiskSnapshotChunkSet struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func newDurableDiskSnapshotChunkSet() *durableDiskSnapshotChunkSet {
	return &durableDiskSnapshotChunkSet{keys: map[string]struct{}{}}
}

// claim reports whether this caller is the one that has to upload the chunk.
func (s *durableDiskSnapshotChunkSet) claim(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; ok {
		return false
	}
	s.keys[key] = struct{}{}
	return true
}

func durableDiskSnapshotSeenChunks(previous *types.DiskSnapshotManifest, chunkPrefix string) *durableDiskSnapshotChunkSet {
	seen := newDurableDiskSnapshotChunkSet()
	if previous == nil {
		return seen
	}
	for _, file := range previous.Files {
		for _, chunk := range file.Chunks {
			if chunk.ObjectKey != "" {
				seen.claim(chunk.ObjectKey)
			}
			if digest := strings.TrimPrefix(chunk.Digest, "sha256:"); digest != "" {
				seen.claim(path.Join(chunkPrefix, digest))
			}
		}
	}
	return seen
}

func snapshotDurableDiskFile(ctx context.Context, store durableDiskSnapshotStore, filename, chunkPrefix string, chunkSize int64, seen *durableDiskSnapshotChunkSet, file *types.DiskSnapshotFile) error {
	in, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer in.Close()
	return snapshotDurableDiskReader(ctx, store, in, filename, chunkPrefix, chunkSize, seen, file)
}

func snapshotDurableDiskReader(ctx context.Context, store durableDiskSnapshotStore, source io.ReaderAt, sourceName, chunkPrefix string, chunkSize int64, seen *durableDiskSnapshotChunkSet, file *types.DiskSnapshotFile) error {
	if chunkSize <= 0 || chunkSize > defaultDurableDiskSnapshotChunkSize {
		chunkSize = defaultDurableDiskSnapshotChunkSize
	}
	var index, offset int64
	if len(file.Chunks) > 0 {
		last := file.Chunks[len(file.Chunks)-1]
		index = last.Index + 1
		offset = last.OffsetBytes + last.SizeBytes
	}

	uploadBaseCtx, cancelUploads := context.WithCancel(ctx)
	defer cancelUploads()
	uploads, uploadCtx := errgroup.WithContext(uploadBaseCtx)
	readBufferSize := min(chunkSize, int64(durableDiskSnapshotReadBufferSize))
	buffer := make([]byte, int(readBufferSize))
	var readErr error
	for ; ; index++ {
		if err := uploadCtx.Err(); err != nil {
			readErr = err
			break
		}
		select {
		case durableDiskSnapshotChunkSlots <- struct{}{}:
		case <-uploadCtx.Done():
			readErr = uploadCtx.Err()
			break
		}
		if readErr != nil {
			break
		}
		body, n, hash, err := spoolDurableDiskSnapshotChunk(uploadCtx, source, sourceName, offset, chunkSize, buffer)
		if err != nil {
			<-durableDiskSnapshotChunkSlots
			// A source read failure makes every sibling upload useless. Cancel
			// them before waiting so a slow object-store request cannot hold the
			// failed snapshot open until its own request timeout.
			if uploadCtx.Err() == nil {
				cancelUploads()
			}
			readErr = err
			break
		}
		if n == 0 {
			<-durableDiskSnapshotChunkSlots
			break
		}
		key := path.Join(chunkPrefix, hash)
		if seen.claim(key) {
			chunkSizeBytes := n
			uploads.Go(func() error {
				defer func() { <-durableDiskSnapshotChunkSlots }()
				defer body.release()
				if err := uploadDurableDiskSnapshotChunk(uploadCtx, store, key, body, chunkSizeBytes); err != nil {
					return fmt.Errorf("upload durable disk snapshot chunk %s: %w", key, err)
				}
				reportDurableDiskProgress(uploadCtx, durableDiskProgressEvent{chunks: 1})
				return nil
			})
		} else {
			body.release()
			<-durableDiskSnapshotChunkSlots
		}
		file.Chunks = append(file.Chunks, types.DiskSnapshotChunk{
			Index:       index,
			OffsetBytes: offset,
			SizeBytes:   n,
			ObjectKey:   key,
			Digest:      "sha256:" + hash,
		})
		offset += n
		if n < chunkSize {
			break
		}
	}
	uploadErr := uploads.Wait()
	if readErr != nil && !errors.Is(readErr, context.Canceled) && !errors.Is(readErr, context.DeadlineExceeded) {
		return readErr
	}
	if uploadErr != nil {
		return uploadErr
	}
	if readErr != nil {
		return readErr
	}
	return ctx.Err()
}

func uploadDurableDiskSnapshotChunk(ctx context.Context, store durableDiskSnapshotStore, key string, body *durableDiskSnapshotChunkBody, size int64) error {
	var err error
	for attempt := 0; attempt < durableDiskSnapshotUploadAttempts; attempt++ {
		err = store.UploadWithReader(ctx, key, &durableDiskSnapshotProgressReader{
			ctx:    ctx,
			reader: body.reader(size),
		})
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt+1 == durableDiskSnapshotUploadAttempts {
			break
		}
		if err := waitForRetry(ctx, durableDiskSnapshotRetryDelay*time.Duration(1<<attempt)); err != nil {
			return err
		}
	}
	return err
}

func spoolDurableDiskSnapshotChunk(ctx context.Context, source io.ReaderAt, sourceName string, offset, size int64, buffer []byte) (*durableDiskSnapshotChunkBody, int64, string, error) {
	body := &durableDiskSnapshotChunkBody{}

	sum := sha256.New()
	reader := io.NewSectionReader(&durableDiskSnapshotContextReaderAt{ctx: ctx, source: source}, offset, size)
	n, err := io.CopyBuffer(io.MultiWriter(body, sum, durableDiskSnapshotLogicalProgressWriter{ctx: ctx}), reader, buffer)
	if err != nil {
		body.release()
		return nil, 0, "", fmt.Errorf("read durable disk snapshot file %s: %w", sourceName, err)
	}
	if n == 0 {
		body.release()
		return nil, 0, "", nil
	}
	return body, n, hex.EncodeToString(sum.Sum(nil)), nil
}

// durableDiskSnapshotChunkBody is a chunk waiting to go up, re-readable because a failed
// upload rewinds. Small ones stay in memory; a temp file each was most of a small-file tree.
type durableDiskSnapshotChunkBody struct {
	data []byte
	file *os.File
}

func (b *durableDiskSnapshotChunkBody) Write(data []byte) (int, error) {
	if b.file == nil && len(b.data)+len(data) > durableDiskSnapshotInlineChunkSize {
		if err := b.spill(); err != nil {
			return 0, err
		}
	}
	if b.file != nil {
		return b.file.Write(data)
	}
	b.data = append(b.data, data...)
	return len(data), nil
}

func (b *durableDiskSnapshotChunkBody) spill() error {
	file, err := os.CreateTemp("", "beta9-durable-disk-snapshot-chunk-*")
	if err != nil {
		return fmt.Errorf("create durable disk snapshot chunk file: %w", err)
	}
	if _, err := file.Write(b.data); err != nil {
		removeDurableDiskSnapshotChunkFile(file)
		return fmt.Errorf("spool durable disk snapshot chunk: %w", err)
	}
	b.file = file
	b.data = nil
	return nil
}

// reader starts at the chunk's first byte, so a retried upload sends all of it, not the tail.
func (b *durableDiskSnapshotChunkBody) reader(size int64) durableDiskSnapshotChunkReader {
	if b.file != nil {
		return io.NewSectionReader(b.file, 0, size)
	}
	return bytes.NewReader(b.data)
}

func (b *durableDiskSnapshotChunkBody) release() {
	if b == nil {
		return
	}
	if b.file != nil {
		removeDurableDiskSnapshotChunkFile(b.file)
		b.file = nil
	}
	b.data = nil
}

type durableDiskSnapshotLogicalProgressWriter struct {
	ctx context.Context
}

func (w durableDiskSnapshotLogicalProgressWriter) Write(data []byte) (int, error) {
	reportDurableDiskProgress(w.ctx, durableDiskProgressEvent{logicalBytes: int64(len(data))})
	return len(data), nil
}

// durableDiskSnapshotChunkReader is what the S3 uploader slices in place; a plain reader is
// copied through a buffer sized for the largest part it might ever send, allocated per upload.
type durableDiskSnapshotChunkReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

// durableDiskSnapshotProgressReader passes that seekability through rather than hiding it.
type durableDiskSnapshotProgressReader struct {
	ctx    context.Context
	reader durableDiskSnapshotChunkReader
}

func (r *durableDiskSnapshotProgressReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.report(r.reader.Read(data))
}

func (r *durableDiskSnapshotProgressReader) ReadAt(data []byte, offset int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.report(r.reader.ReadAt(data, offset))
}

func (r *durableDiskSnapshotProgressReader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (r *durableDiskSnapshotProgressReader) report(n int, err error) (int, error) {
	if n > 0 {
		// Upload reads prove the request is still consuming bytes, but do not
		// double-count them as logical disk bytes or completed chunks.
		reportDurableDiskProgress(r.ctx, durableDiskProgressEvent{})
	}
	return n, err
}

func removeDurableDiskSnapshotChunkFile(chunkFile *os.File) {
	if chunkFile == nil {
		return
	}
	name := chunkFile.Name()
	_ = chunkFile.Close()
	_ = os.Remove(name)
}

type durableDiskSnapshotContextReaderAt struct {
	ctx    context.Context
	source io.ReaderAt
}

func (r *durableDiskSnapshotContextReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.ReadAt(p, offset)
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

	stagingDir, err := os.MkdirTemp(filepath.Dir(targetDir), "."+filepath.Base(targetDir)+".restore-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stagingDir)

	if err := restoreDurableDiskDirectoryManifest(ctx, store, cacheReader, manifest, stagingDir); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(targetDir); err != nil {
		return nil, err
	}
	if err := os.Rename(stagingDir, targetDir); err != nil {
		return nil, err
	}
	return manifest, nil
}

func restoreDurableDiskDirectoryManifest(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, manifest *types.DiskSnapshotManifest, targetDir string) error {
	if err := os.RemoveAll(targetDir); err != nil {
		return fmt.Errorf("clear durable disk restore directory %s: %w", targetDir, err)
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create durable disk restore directory %s: %w", targetDir, err)
	}

	// Contents on a pool, for the reason snapshotting is. Directories and symlinks stay here,
	// in sorted manifest order, so a parent exists and owns its mode before anything fills it.
	contents, contentsCtx := errgroup.WithContext(ctx)
	contents.SetLimit(durableDiskRestoreConcurrency)

	entriesErr := func() error {
		for _, file := range manifest.Files {
			if err := contentsCtx.Err(); err != nil {
				return err
			}
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
				contents.Go(func() error {
					if err := restoreDurableDiskManifestFile(contentsCtx, store, cacheReader, file, targetPath, mode.Perm()); err != nil {
						return err
					}
					applyDurableDiskRestoreMetadata(targetPath, file)
					return nil
				})
				continue
			default:
				continue
			}
			applyDurableDiskRestoreMetadata(targetPath, file)
		}
		return nil
	}()

	// Waited on either way: a pass that stopped early still has files writing into the directory.
	if contentsErr := contents.Wait(); contentsErr != nil {
		return contentsErr
	}
	return entriesErr
}

func applyDurableDiskRestoreMetadata(targetPath string, file types.DiskSnapshotFile) {
	_ = os.Chown(targetPath, file.Uid, file.Gid)
	if file.ModTimeUnixNano > 0 {
		modTime := time.Unix(0, file.ModTimeUnixNano)
		_ = os.Chtimes(targetPath, modTime, modTime)
	}
}

func restoreDurableDiskManifestFile(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, file types.DiskSnapshotFile, targetPath string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if err := out.Truncate(file.SizeBytes); err != nil {
		_ = out.Close()
		return err
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(durableDiskRestoreConcurrency)
	for _, chunk := range file.Chunks {
		chunk := chunk
		group.Go(func() error {
			// One pool across all files, so the bound does not multiply by the files open.
			select {
			case durableDiskRestoreChunkSlots <- struct{}{}:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
			defer func() { <-durableDiskRestoreChunkSlots }()
			return restoreDurableDiskManifestChunk(groupCtx, store, cacheReader, out, chunk)
		})
	}
	if err := group.Wait(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func restoreDurableDiskManifestChunk(ctx context.Context, store durableDiskSnapshotStore, cacheReader durableDiskSnapshotCacheReader, out *os.File, chunk types.DiskSnapshotChunk) error {
	reader, err := durableDiskSnapshotObjectReader(ctx, store, cacheReader, chunk.ObjectKey, chunk.Digest, chunk.SizeBytes)
	if err != nil {
		return fmt.Errorf("download durable disk snapshot chunk %s: %w", chunk.ObjectKey, err)
	}
	defer reader.Close()

	sum := sha256.New()
	n, err := io.CopyN(io.MultiWriter(io.NewOffsetWriter(out, chunk.OffsetBytes), sum), reader, chunk.SizeBytes)
	if err != nil {
		return fmt.Errorf("restore durable disk snapshot chunk %s: copied %d of %d bytes: %w", chunk.ObjectKey, n, chunk.SizeBytes, err)
	}
	if digest := "sha256:" + hex.EncodeToString(sum.Sum(nil)); digest != chunk.Digest {
		return fmt.Errorf("durable disk snapshot chunk %s digest mismatch: got %s want %s", chunk.ObjectKey, digest, chunk.Digest)
	}
	return nil
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
		Public:              snapshot.Public,
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
		Public:              in.Public,
	}
}
