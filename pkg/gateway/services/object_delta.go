package gatewayservices

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const objectDeltaSuffix = ".delta"

// objectDeltaKey is where the delta archive for objectID relative to
// baseObjectID is staged. The base is part of the key: two syncs can arrive
// at the same content (hence the same object) from different previous syncs,
// and their deltas are different archives. Under one key the later PUT would
// replace the earlier delta before its commit, and that commit would merge
// the wrong delta into its base and publish a bogus archive under the hash.
// Two syncs from the same base carry equivalent deltas, so sharing is fine.
func objectDeltaKey(objectID, baseObjectID string) string {
	return path.Join(types.DefaultObjectPrefix, objectID+objectDeltaSuffix+"."+baseObjectID)
}

// CreateObjectDelta registers a new object whose archive will be produced by
// merging a delta archive (added and modified files only) into the archive of
// base_object_id, and hands back a presigned URL for the delta upload.
func (gws *GatewayService) CreateObjectDelta(ctx context.Context, in *pb.CreateObjectDeltaRequest) (*pb.CreateObjectDeltaResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !authInfo.Workspace.StorageAvailable() {
		return &pb.CreateObjectDeltaResponse{Ok: false, ErrorMsg: "Workspace storage is unavailable"}, nil
	}
	if in.Hash == "" || in.BaseObjectId == "" {
		return &pb.CreateObjectDeltaResponse{Ok: false, ErrorMsg: "hash and base_object_id are required"}, nil
	}

	storageClient, err := clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage, gws.appConfig.Storage.WorkspaceStorage)
	if err != nil {
		return &pb.CreateObjectDeltaResponse{Ok: false, ErrorMsg: "Unable to create storage client"}, nil
	}

	// The base has to be an object of this workspace that is still in storage;
	// otherwise the client must upload everything.
	if _, err := gws.backendRepo.GetObjectByExternalId(ctx, in.BaseObjectId, authInfo.Workspace.Id); err != nil {
		return &pb.CreateObjectDeltaResponse{Ok: false, BaseMissing: true, ErrorMsg: "base object not found"}, nil
	}
	if exists, err := storageClient.Exists(ctx, path.Join(types.DefaultObjectPrefix, in.BaseObjectId)); err != nil || !exists {
		return &pb.CreateObjectDeltaResponse{Ok: false, BaseMissing: true, ErrorMsg: "base object archive not found"}, nil
	}

	object, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err != nil || object == nil {
		object, err = gws.backendRepo.CreateObject(ctx, in.Hash, in.Size, authInfo.Workspace.Id)
		if err != nil {
			return &pb.CreateObjectDeltaResponse{Ok: false, ErrorMsg: "Unable to create object"}, nil
		}
	}

	presignedURL, err := storageClient.GeneratePresignedPutURL(ctx, objectDeltaKey(object.ExternalId, in.BaseObjectId), defaultObjectPutExpirationS)
	if err != nil {
		return &pb.CreateObjectDeltaResponse{Ok: false, ErrorMsg: "Unable to generate presigned URL"}, nil
	}

	return &pb.CreateObjectDeltaResponse{Ok: true, ObjectId: object.ExternalId, PresignedUrl: presignedURL}, nil
}

// CommitObjectDelta merges the uploaded delta archive into the base archive
// and stores the result as the object's archive, tagged with the object hash
// so HeadObject treats it exactly like a directly uploaded archive.
func (gws *GatewayService) CommitObjectDelta(ctx context.Context, in *pb.CommitObjectDeltaRequest) (*pb.CommitObjectDeltaResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !authInfo.Workspace.StorageAvailable() {
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: "Workspace storage is unavailable"}, nil
	}

	object, err := gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	if err != nil {
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: "object not found"}, nil
	}
	if _, err := gws.backendRepo.GetObjectByExternalId(ctx, in.BaseObjectId, authInfo.Workspace.Id); err != nil {
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: "base object not found"}, nil
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: "Unable to create storage client"}, nil
	}

	baseKey := path.Join(types.DefaultObjectPrefix, in.BaseObjectId)
	deltaKey := objectDeltaKey(object.ExternalId, in.BaseObjectId)
	targetKey := path.Join(types.DefaultObjectPrefix, object.ExternalId)

	size, err := gws.mergeObjectDelta(ctx, storageClient, baseKey, deltaKey, targetKey, object.Hash, in.RemovedPaths)
	if err != nil {
		log.Error().Err(err).Str("object_id", object.ExternalId).Str("base_object_id", in.BaseObjectId).Msg("failed to merge object delta")
		_ = storageClient.Delete(ctx, deltaKey)
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: fmt.Sprintf("Unable to merge delta: %v", err)}, nil
	}
	_ = storageClient.Delete(ctx, deltaKey)

	if err := gws.backendRepo.UpdateObjectSizeByExternalId(ctx, object.ExternalId, int(size)); err != nil {
		return &pb.CommitObjectDeltaResponse{Ok: false, ErrorMsg: "Unable to record object size"}, nil
	}

	return &pb.CommitObjectDeltaResponse{Ok: true, ObjectId: object.ExternalId, Size: size}, nil
}

func (gws *GatewayService) mergeObjectDelta(ctx context.Context, storageClient *clients.WorkspaceStorageClient, baseKey, deltaKey, targetKey, hash string, removedPaths []string) (int64, error) {
	metadata := map[string]string{workspaceObjectHashMetadataKey: hash}

	// Fast path: assemble the merged archive from server-side range copies of
	// the two existing objects, so only the changed entries and the central
	// directory pass through the gateway.
	started := time.Now()
	store := &s3SpliceStore{client: storageClient.StorageClient.S3Client(), bucket: *storageClient.WorkspaceStorage.BucketName}
	size, err := spliceZipObjects(ctx, store, baseKey, deltaKey, targetKey, removedPaths, metadata, s3MinPartSize)
	if err == nil {
		log.Info().Str("target", targetKey).Int64("size", size).Dur("duration", time.Since(started)).Msg("spliced object delta")
		return size, nil
	}
	log.Warn().Err(err).Str("target", targetKey).Msg("object delta splice failed; merging archives locally")

	return gws.mergeObjectDeltaLocally(ctx, storageClient, baseKey, deltaKey, targetKey, metadata, removedPaths)
}

// mergeObjectDeltaLocally downloads both archives, merges them entry by entry
// and uploads the result. It handles any zip layout, at the cost of moving
// every byte of both archives through the gateway.
func (gws *GatewayService) mergeObjectDeltaLocally(ctx context.Context, storageClient *clients.WorkspaceStorageClient, baseKey, deltaKey, targetKey string, metadata map[string]string, removedPaths []string) (int64, error) {
	dir, err := os.MkdirTemp("", "object-delta-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	baseFile, err := downloadToFile(ctx, storageClient, baseKey, path.Join(dir, "base.zip"))
	if err != nil {
		return 0, fmt.Errorf("download base: %w", err)
	}
	defer baseFile.Close()
	deltaFile, err := downloadToFile(ctx, storageClient, deltaKey, path.Join(dir, "delta.zip"))
	if err != nil {
		return 0, fmt.Errorf("download delta: %w", err)
	}
	defer deltaFile.Close()

	merged, err := os.Create(path.Join(dir, "merged.zip"))
	if err != nil {
		return 0, err
	}
	defer merged.Close()

	size, err := mergeZipArchives(baseFile, deltaFile, removedPaths, merged)
	if err != nil {
		return 0, err
	}
	if _, err := merged.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	if err := storageClient.UploadWithReaderAndMetadata(ctx, targetKey, merged, metadata); err != nil {
		return 0, fmt.Errorf("upload merged archive: %w", err)
	}
	return size, nil
}

func downloadToFile(ctx context.Context, storageClient *clients.WorkspaceStorageClient, key, localPath string) (*os.File, error) {
	body, err := storageClient.DownloadWithReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(file, body); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

// mergeZipArchives writes a zip archive containing every entry of base that is
// neither in removedPaths nor present in delta, followed by every entry of
// delta. Entries are copied raw (no recompression). Returns the merged size.
func mergeZipArchives(base, delta *os.File, removedPaths []string, out io.Writer) (int64, error) {
	baseInfo, err := base.Stat()
	if err != nil {
		return 0, err
	}
	deltaInfo, err := delta.Stat()
	if err != nil {
		return 0, err
	}
	baseReader, err := zip.NewReader(base, baseInfo.Size())
	if err != nil {
		return 0, fmt.Errorf("base archive: %w", err)
	}
	deltaReader, err := zip.NewReader(delta, deltaInfo.Size())
	if err != nil {
		return 0, fmt.Errorf("delta archive: %w", err)
	}

	skip := make(map[string]struct{}, len(removedPaths)+len(deltaReader.File))
	for _, p := range removedPaths {
		skip[p] = struct{}{}
	}
	for _, f := range deltaReader.File {
		skip[f.Name] = struct{}{}
	}

	counter := &countingWriter{w: out}
	writer := zip.NewWriter(counter)
	for _, f := range baseReader.File {
		if _, drop := skip[f.Name]; drop {
			continue
		}
		if err := writer.Copy(f); err != nil {
			return 0, fmt.Errorf("copy %s from base: %w", f.Name, err)
		}
	}
	for _, f := range deltaReader.File {
		if err := writer.Copy(f); err != nil {
			return 0, fmt.Errorf("copy %s from delta: %w", f.Name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return 0, err
	}
	return counter.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
