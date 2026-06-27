package repository_services

import (
	"context"
	"errors"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackendRepositoryService struct {
	ctx         context.Context
	backendRepo repository.BackendRepository
	pb.UnimplementedBackendRepositoryServiceServer
}

func NewBackendRepositoryService(ctx context.Context, backendRepo repository.BackendRepository) *BackendRepositoryService {
	return &BackendRepositoryService{ctx: ctx, backendRepo: backendRepo}
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
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.GetLatestDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	snapshot, err := s.backendRepo.GetLatestDiskSnapshot(ctx, workspace.Id, req.DiskName)
	if err != nil {
		var notFound *types.ErrDiskSnapshotNotFound
		if errors.As(err, &notFound) {
			return &pb.GetLatestDiskSnapshotResponse{Ok: true}, nil
		}
		return &pb.GetLatestDiskSnapshotResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.GetLatestDiskSnapshotResponse{Ok: true, Snapshot: diskSnapshotToProto(snapshot)}, nil
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
	}
}
