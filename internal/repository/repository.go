package repository

import (
	"github.com/beam-cloud/beta9/internal/common"
)

type RepositoryManager struct {
	Backend    BackendRepository
	Container  ContainerRepository
	Worker     WorkerRepository
	WorkerPool WorkerPoolRepository
}

func NewRepositoryManager(s *SQLClient, r *common.RedisClient) (*RepositoryManager, error) {
	return &RepositoryManager{
		Backend:    NewBackendPostgresRepository(s),
		Container:  NewContainerRedisRepository(r),
		Worker:     NewWorkerRedisRepository(r),
		WorkerPool: NewWorkerPoolRedisRepository(r),
	}, nil
}
