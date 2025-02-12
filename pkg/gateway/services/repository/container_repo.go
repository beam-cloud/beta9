package repository_services

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerRepositoryService struct {
	ctx           context.Context
	containerRepo repository.ContainerRepository
	pb.UnimplementedContainerRepositoryServiceServer
}

func NewContainerRepositoryService(ctx context.Context, containerRepo repository.ContainerRepository) *ContainerRepositoryService {
	return &ContainerRepositoryService{ctx: ctx, containerRepo: containerRepo}
}

func (s *ContainerRepositoryService) GetContainerState(ctx context.Context, req *pb.GetContainerStateRequest) (*pb.GetContainerStateResponse, error) {
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return &pb.GetContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetContainerStateResponse{
		Ok:          true,
		ContainerId: req.ContainerId,
		State: &pb.ContainerState{
			Status:      string(state.Status),
			ContainerId: state.ContainerId,
			StubId:      state.StubId,
			ScheduledAt: state.ScheduledAt,
			StartedAt:   state.StartedAt,
			WorkspaceId: state.WorkspaceId,
			Gpu:         state.Gpu,
			GpuCount:    state.GpuCount,
			Cpu:         state.Cpu,
			Memory:      state.Memory,
		}}, nil
}

func (s *ContainerRepositoryService) DeleteContainerState(ctx context.Context, req *pb.DeleteContainerStateRequest) (*pb.DeleteContainerStateResponse, error) {
	err := s.containerRepo.DeleteContainerState(req.ContainerId)
	if err != nil {
		return &pb.DeleteContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.DeleteContainerStateResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) UpdateContainerStatus(ctx context.Context, req *pb.UpdateContainerStatusRequest) (*pb.UpdateContainerStatusResponse, error) {
	err := s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatus(req.Status), req.ExpirySeconds)
	if err != nil {
		return &pb.UpdateContainerStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateContainerStatusResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) SetContainerExitCode(ctx context.Context, req *pb.SetContainerExitCodeRequest) (*pb.SetContainerExitCodeResponse, error) {
	err := s.containerRepo.SetContainerExitCode(req.ContainerId, int(req.ExitCode))
	if err != nil {
		return &pb.SetContainerExitCodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerExitCodeResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) SetContainerAddress(ctx context.Context, req *pb.SetContainerAddressRequest) (*pb.SetContainerAddressResponse, error) {
	err := s.containerRepo.SetContainerAddress(req.ContainerId, req.Address)
	if err != nil {
		return &pb.SetContainerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerAddressResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) SetWorkerAddress(ctx context.Context, req *pb.SetWorkerAddressRequest) (*pb.SetWorkerAddressResponse, error) {
	err := s.containerRepo.SetWorkerAddress(req.ContainerId, req.Address)
	if err != nil {
		return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerAddressResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) UpdateCheckpointState(ctx context.Context, req *pb.UpdateCheckpointStateRequest) (*pb.UpdateCheckpointStateResponse, error) {
	log.Info().Str("container_id", req.ContainerId).Interface("request", req).Msg("updating checkpoint state")
	checkpointState := types.CheckpointState{
		StubId:      req.CheckpointState.StubId,
		ContainerId: req.CheckpointState.ContainerId,
		Status:      types.CheckpointStatus(req.CheckpointState.Status),
		RemoteKey:   req.CheckpointState.RemoteKey,
	}
	err := s.containerRepo.UpdateCheckpointState(req.WorkspaceName, req.CheckpointId, &checkpointState)
	if err != nil {
		return &pb.UpdateCheckpointStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	log.Info().Interface("checkpoint_state", checkpointState).Msg("updated checkpoint state")

	return &pb.UpdateCheckpointStateResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) GetCheckpointState(ctx context.Context, req *pb.GetCheckpointStateRequest) (*pb.GetCheckpointStateResponse, error) {
	log.Info().Str("workspace_name", req.WorkspaceName).Str("checkpoint_id", req.CheckpointId).Interface("request", req).Msg("getting checkpoint state")
	checkpointState, err := s.containerRepo.GetCheckpointState(req.WorkspaceName, req.CheckpointId)
	if err != nil {
		return &pb.GetCheckpointStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	log.Info().Interface("checkpoint_state", checkpointState).Msg("got checkpoint state")

	return &pb.GetCheckpointStateResponse{Ok: true, CheckpointState: &pb.CheckpointState{
		Status:      string(checkpointState.Status),
		ContainerId: checkpointState.ContainerId,
		StubId:      checkpointState.StubId,
		RemoteKey:   checkpointState.RemoteKey,
	}}, nil
}
