package repository

import (
	"github.com/beam-cloud/beta9/internal/common"
)

type RepositoryManager struct {
	PostgresClient *SQLClient
	RedisClient    *common.RedisClient

	Backend    BackendRepository
	Container  ContainerRepository
	Worker     WorkerRepository
	WorkerPool WorkerPoolRepository
}

func NewRepositoryManager(s *SQLClient, r *common.RedisClient) (*RepositoryManager, error) {
	return &RepositoryManager{
		PostgresClient: s,
		RedisClient:    r,

		Backend:    NewBackendPostgresRepository(s),
		Container:  NewContainerRedisRepository(r),
		Worker:     NewWorkerRedisRepository(r),
		WorkerPool: NewWorkerPoolRedisRepository(r),
	}, nil
}
