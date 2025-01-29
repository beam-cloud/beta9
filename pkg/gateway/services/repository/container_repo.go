package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerRepositoryService struct {
	containerRepo repository.ContainerRepository
	pb.UnimplementedContainerRepositoryServiceServer
}

func NewContainerRepositoryService(containerRepo repository.ContainerRepository) *ContainerRepositoryService {
	return &ContainerRepositoryService{containerRepo: containerRepo}
}

func (s *ContainerRepositoryService) GetContainerState(ctx context.Context, req *pb.GetContainerStateRequest) (*pb.GetContainerStateResponse, error) {
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return nil, err
	}

	return &pb.GetContainerStateResponse{ContainerId: req.ContainerId, State: &pb.ContainerState{
		Status:      string(state.Status),
		ContainerId: state.ContainerId,
		StubId:      state.StubId,
		ScheduledAt: state.ScheduledAt,
		WorkspaceId: state.WorkspaceId,
		Gpu:         state.Gpu,
		GpuCount:    state.GpuCount,
		Cpu:         state.Cpu,
		Memory:      state.Memory,
	}}, nil
}
