package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	container_svc "github.com/beam-cloud/beta9/proto/goa/gen/container_repository"
)

type ContainerService struct {
	containerRepo repository.ContainerRepository
}

func NewContainerRepositoryService(containerRepo repository.ContainerRepository) container_svc.Service {
	return &ContainerService{containerRepo: containerRepo}
}

func (s *ContainerService) GetContainerState(ctx context.Context, payload *container_svc.GetContainerStatePayload) error {
	_, err := s.containerRepo.GetContainerState(payload.ContainerID)
	return err
}

func (s *ContainerService) SetContainerState(ctx context.Context, payload *container_svc.SetContainerStatePayload) error {
	containerState := &types.ContainerState{}
	return s.containerRepo.SetContainerState(*payload.ContainerID, containerState)
}
