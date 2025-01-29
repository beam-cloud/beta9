package repository_services

import (
	"github.com/beam-cloud/beta9/pkg/repository"
)

type ContainerService struct {
	containerRepo repository.ContainerRepository
}

func NewContainerRepositoryService(containerRepo repository.ContainerRepository) *ContainerService {
	return &ContainerService{containerRepo: containerRepo}
}
