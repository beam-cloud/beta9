package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
	})
	if err != nil {
		return &pb.CreateCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.CreateCheckpointResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}

func (s *BackendRepositoryService) UpdateCheckpoint(ctx context.Context, req *pb.UpdateCheckpointRequest) (*pb.UpdateCheckpointResponse, error) {
	checkpoint, err := s.backendRepo.UpdateCheckpoint(ctx, &types.Checkpoint{
		CheckpointId: req.CheckpointId,
		ContainerIp:  req.ContainerIp,
		Status:       req.Status,
		ExposedPorts: req.ExposedPorts,
	})
	if err != nil {
		return &pb.UpdateCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateCheckpointResponse{Ok: true, Checkpoint: checkpoint.ToProto()}, nil
}
