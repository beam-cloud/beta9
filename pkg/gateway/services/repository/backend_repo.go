package repository_services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackendRepositoryService struct {
	ctx                     context.Context
	backendRepo             repository.BackendRepository
	diskSnapshotObjectMu    sync.Mutex
	diskSnapshotObjectCache map[string]diskSnapshotObjectCacheEntry
	diskSnapshotObjectGroup singleflight.Group
	pb.UnimplementedBackendRepositoryServiceServer
}

const diskSnapshotURLExpiry = 15 * time.Minute

type diskSnapshotObjectCacheEntry struct {
	digest    string
	expiresAt time.Time
	keys      map[string]struct{}
}

func NewBackendRepositoryService(ctx context.Context, backendRepo repository.BackendRepository) *BackendRepositoryService {
	return &BackendRepositoryService{
		ctx:                     ctx,
		backendRepo:             backendRepo,
		diskSnapshotObjectCache: make(map[string]diskSnapshotObjectCacheEntry),
	}
}

func (s *BackendRepositoryService) GetCheckpointById(ctx context.Context, req *pb.GetCheckpointByIdRequest) (*pb.GetCheckpointByIdResponse, error) {
	checkpoint, err := s.backendRepo.GetCheckpointById(ctx, req.CheckpointId)
	if err != nil {
		return &pb.GetCheckpointByIdResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetCheckpointByIdResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}

func (s *BackendRepositoryService) GetLatestCheckpointByStubId(ctx context.Context, req *pb.GetLatestCheckpointByStubIdRequest) (*pb.GetLatestCheckpointByStubIdResponse, error) {
	checkpoint, err := s.backendRepo.GetLatestCheckpointByStubId(ctx, req.StubId)
	if err != nil {
		return &pb.GetLatestCheckpointByStubIdResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetLatestCheckpointByStubIdResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}

func (s *BackendRepositoryService) ListCheckpoints(ctx context.Context, req *pb.ListCheckpointsRequest) (*pb.ListCheckpointsResponse, error) {
	checkpoints, err := s.backendRepo.ListCheckpoints(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListCheckpointsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	checkpointsProto := []*pb.Checkpoint{}
	for _, checkpoint := range checkpoints {
		checkpointsProto = append(checkpointsProto, checkpoint.ToProto())
	}

	return &pb.ListCheckpointsResponse{Ok: true, Checkpoints: checkpointsProto}, nil
}

func (s *BackendRepositoryService) CreateCheckpoint(ctx context.Context, req *pb.CreateCheckpointRequest) (*pb.CreateCheckpointResponse, error) {
	stub, err := s.backendRepo.GetStubByExternalId(ctx, req.StubId)
	if err != nil {
		return &pb.CreateCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	checkpoint, err := s.backendRepo.CreateCheckpoint(ctx, &types.Checkpoint{
		CheckpointId:      req.CheckpointId,
		SourceContainerId: req.SourceContainerId,
		ContainerIp:       req.ContainerIp,
		Status:            req.Status,
		RemoteKey:         req.RemoteKey,
		WorkspaceId:       stub.WorkspaceId,
		StubId:            stub.Id,
		StubType:          string(stub.Type),
		AppId:             stub.AppId,
		ExposedPorts:      req.ExposedPorts,
		CacheHash:         req.CacheHash,
		CacheSizeBytes:    req.CacheSizeBytes,
		OriginKey:         req.OriginKey,
		Locality:          req.Locality,
		Accelerator:       req.Accelerator,
	})
	if err != nil {
		return &pb.CreateCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.CreateCheckpointResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}

func (s *BackendRepositoryService) UpdateCheckpoint(ctx context.Context, req *pb.UpdateCheckpointRequest) (*pb.UpdateCheckpointResponse, error) {
	lastRestoredAt := types.Time{}
	if req.LastRestoredAt != nil {
		lastRestoredAt = types.Time{Time: req.LastRestoredAt.AsTime()}
	}

	checkpoint, err := s.backendRepo.UpdateCheckpoint(ctx, &types.Checkpoint{
		CheckpointId:   req.CheckpointId,
		Status:         req.Status,
		LastRestoredAt: lastRestoredAt,
	})
	if err != nil {
		return &pb.UpdateCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateCheckpointResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}

func (s *BackendRepositoryService) CreateDiskSnapshot(ctx context.Context, req *pb.CreateDiskSnapshotRequest) (*pb.CreateDiskSnapshotResponse, error) {
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.CreateDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	snapshot := diskSnapshotFromProto(req.Snapshot)
	if snapshot == nil {
		return &pb.CreateDiskSnapshotResponse{Ok: false, ErrorMsg: "disk snapshot is required"}, nil
	}
	snapshot.WorkspaceId = workspace.Id

	if req.StubId != "" {
		stub, err := s.backendRepo.GetStubByExternalId(ctx, req.StubId)
		if err != nil {
			return &pb.CreateDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		if stub.WorkspaceId != workspace.Id {
			return &pb.CreateDiskSnapshotResponse{Ok: false, ErrorMsg: "snapshot stub does not belong to workspace"}, nil
		}
		snapshot.StubId = stub.Id
	}

	created, err := s.backendRepo.CreateDiskSnapshot(ctx, snapshot)
	if err != nil {
		return &pb.CreateDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.CreateDiskSnapshotResponse{Ok: true, Snapshot: diskSnapshotToProto(created)}, nil
}

func (s *BackendRepositoryService) GetLatestDiskSnapshot(ctx context.Context, req *pb.GetLatestDiskSnapshotRequest) (*pb.GetLatestDiskSnapshotResponse, error) {
	snapshot, err := s.findDiskSnapshot(ctx, req.WorkspaceId, func(ctx context.Context, workspaceID uint) (*types.DiskSnapshot, error) {
		return s.backendRepo.GetLatestDiskSnapshot(ctx, workspaceID, req.DiskName)
	})
	if err != nil {
		return &pb.GetLatestDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.GetLatestDiskSnapshotResponse{Ok: true, Snapshot: diskSnapshotToProto(snapshot)}, nil
}

func (s *BackendRepositoryService) GetDiskSnapshot(ctx context.Context, req *pb.GetDiskSnapshotRequest) (*pb.GetDiskSnapshotResponse, error) {
	snapshot, err := s.findDiskSnapshot(ctx, req.WorkspaceId, func(ctx context.Context, workspaceID uint) (*types.DiskSnapshot, error) {
		return s.backendRepo.GetDiskSnapshot(ctx, workspaceID, req.SnapshotId)
	})
	if err != nil {
		return &pb.GetDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.GetDiskSnapshotResponse{Ok: true, Snapshot: diskSnapshotToProto(snapshot)}, nil
}

func (s *BackendRepositoryService) GetDiskSnapshotDownloadURL(ctx context.Context, req *pb.GetDiskSnapshotDownloadURLRequest) (*pb.GetDiskSnapshotDownloadURLResponse, error) {
	url, err := s.diskSnapshotDownloadURL(ctx, req)
	if err != nil {
		return &pb.GetDiskSnapshotDownloadURLResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.GetDiskSnapshotDownloadURLResponse{Ok: true, Url: url}, nil
}

func (s *BackendRepositoryService) diskSnapshotDownloadURL(ctx context.Context, req *pb.GetDiskSnapshotDownloadURLRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("request is required")
	}
	if err := authorizeDiskSnapshotWorkspace(ctx, req.WorkspaceId); err != nil {
		return "", err
	}

	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return "", err
	}
	snapshot, err := s.backendRepo.GetDiskSnapshot(ctx, workspace.Id, req.SnapshotId)
	if err != nil {
		return "", err
	}
	if snapshot.Status != types.DiskSnapshotStatusAvailable {
		return "", fmt.Errorf("disk snapshot object is unavailable")
	}

	source, err := s.backendRepo.GetWorkspace(ctx, snapshot.WorkspaceId)
	if err != nil {
		return "", err
	}
	if !source.StorageAvailable() || snapshot.BucketName == "" {
		return "", fmt.Errorf("disk snapshot storage is unavailable")
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, source.Name, source.Storage)
	if err != nil {
		return "", err
	}
	allowed, err := s.diskSnapshotObjectAllowed(ctx, snapshot, storageClient, req.ObjectKey)
	if err != nil {
		return "", err
	}
	if !allowed {
		return "", fmt.Errorf("disk snapshot object is unavailable")
	}
	return storageClient.StorageClient.GeneratePresignedGetURL(ctx, req.ObjectKey, int64(diskSnapshotURLExpiry.Seconds()), snapshot.BucketName)
}

func authorizeDiskSnapshotWorkspace(ctx context.Context, workspaceID string) error {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil {
		return fmt.Errorf("authorization is required")
	}
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin || authInfo.Token.TokenType == types.TokenTypeWorker {
		return nil
	}
	if authInfo.Workspace == nil || authInfo.Workspace.ExternalId != workspaceID {
		return fmt.Errorf("token cannot access workspace %q", workspaceID)
	}
	return nil
}

func (s *BackendRepositoryService) diskSnapshotObjectAllowed(ctx context.Context, snapshot *types.DiskSnapshot, storageClient *clients.WorkspaceStorageClient, objectKey string) (bool, error) {
	if !diskSnapshotObjectKeyValid(snapshot, objectKey) {
		return false, nil
	}
	if objectKey == snapshot.ManifestKey {
		return true, nil
	}

	keys, err := s.diskSnapshotChunkKeys(ctx, snapshot, storageClient)
	if err != nil {
		return false, err
	}
	return diskSnapshotObjectKeyAllowed(snapshot, objectKey, keys), nil
}

func (s *BackendRepositoryService) diskSnapshotChunkKeys(ctx context.Context, snapshot *types.DiskSnapshot, storageClient *clients.WorkspaceStorageClient) (map[string]struct{}, error) {
	now := time.Now()
	if keys, ok := s.getCachedDiskSnapshotChunkKeys(snapshot, now); ok {
		return keys, nil
	}

	cacheKey := snapshot.ExternalId + ":" + snapshot.ManifestDigest
	value, err, _ := s.diskSnapshotObjectGroup.Do(cacheKey, func() (any, error) {
		now := time.Now()
		if keys, ok := s.getCachedDiskSnapshotChunkKeys(snapshot, now); ok {
			return keys, nil
		}

		data, err := storageClient.StorageClient.Download(ctx, snapshot.ManifestKey, snapshot.BucketName)
		if err != nil {
			return nil, fmt.Errorf("read disk snapshot manifest: %w", err)
		}
		keys, err := diskSnapshotManifestChunkKeys(data, snapshot.ManifestDigest)
		if err != nil {
			return nil, err
		}

		s.diskSnapshotObjectMu.Lock()
		if len(s.diskSnapshotObjectCache) >= 256 {
			for id, cached := range s.diskSnapshotObjectCache {
				if !now.Before(cached.expiresAt) {
					delete(s.diskSnapshotObjectCache, id)
				}
			}
		}
		if len(s.diskSnapshotObjectCache) >= 256 {
			for id := range s.diskSnapshotObjectCache {
				delete(s.diskSnapshotObjectCache, id)
				break
			}
		}
		s.diskSnapshotObjectCache[snapshot.ExternalId] = diskSnapshotObjectCacheEntry{
			digest:    snapshot.ManifestDigest,
			expiresAt: now.Add(diskSnapshotURLExpiry),
			keys:      keys,
		}
		s.diskSnapshotObjectMu.Unlock()
		return keys, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(map[string]struct{}), nil
}

func (s *BackendRepositoryService) getCachedDiskSnapshotChunkKeys(snapshot *types.DiskSnapshot, now time.Time) (map[string]struct{}, bool) {
	s.diskSnapshotObjectMu.Lock()
	defer s.diskSnapshotObjectMu.Unlock()

	entry, ok := s.diskSnapshotObjectCache[snapshot.ExternalId]
	if !ok || entry.digest != snapshot.ManifestDigest || !now.Before(entry.expiresAt) {
		return nil, false
	}
	return entry.keys, true
}

func diskSnapshotObjectKeyValid(snapshot *types.DiskSnapshot, objectKey string) bool {
	return snapshot != nil && objectKey != "" && strings.TrimSpace(objectKey) == objectKey && !strings.HasPrefix(objectKey, "/") && path.Clean(objectKey) == objectKey
}

func diskSnapshotObjectKeyAllowed(snapshot *types.DiskSnapshot, objectKey string, chunkKeys map[string]struct{}) bool {
	if !diskSnapshotObjectKeyValid(snapshot, objectKey) {
		return false
	}
	if objectKey == snapshot.ManifestKey {
		return true
	}
	_, ok := chunkKeys[objectKey]
	return ok
}

func diskSnapshotManifestChunkKeys(data []byte, expectedDigest string) (map[string]struct{}, error) {
	if expectedDigest != "" {
		digest := sha256.Sum256(data)
		if "sha256:"+hex.EncodeToString(digest[:]) != expectedDigest {
			return nil, fmt.Errorf("disk snapshot manifest digest mismatch")
		}
	}
	var manifest types.DiskSnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode disk snapshot manifest: %w", err)
	}
	keys := make(map[string]struct{})
	for _, file := range manifest.Files {
		for _, chunk := range file.Chunks {
			keys[chunk.ObjectKey] = struct{}{}
		}
	}
	return keys, nil
}

func (s *BackendRepositoryService) findDiskSnapshot(
	ctx context.Context,
	workspaceExternalID string,
	lookup func(context.Context, uint) (*types.DiskSnapshot, error),
) (*types.DiskSnapshot, error) {
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, workspaceExternalID)
	if err != nil {
		return nil, err
	}

	snapshot, err := lookup(ctx, workspace.Id)
	if err != nil {
		var notFound *types.ErrDiskSnapshotNotFound
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, err
	}

	return snapshot, nil
}

func diskSnapshotToProto(snapshot *types.DiskSnapshot) *pb.DiskSnapshot {
	if snapshot == nil {
		return nil
	}

	var completedAt *timestamppb.Timestamp
	if snapshot.CompletedAt.Valid {
		completedAt = timestamppb.New(snapshot.CompletedAt.Time)
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
		CompletedAt:         completedAt,
		Public:              snapshot.Public,
	}
}

func diskSnapshotFromProto(in *pb.DiskSnapshot) *types.DiskSnapshot {
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
