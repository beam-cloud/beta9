package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
)

type WorkerPoolRepositoryService struct {
	ctx            context.Context
	workerPoolRepo repository.WorkerPoolRepository
	pb.UnimplementedWorkerPoolRepositoryServiceServer
}

func NewWorkerPoolRepositoryService(ctx context.Context, workerPoolRepo repository.WorkerPoolRepository) *WorkerPoolRepositoryService {
	return &WorkerPoolRepositoryService{ctx: ctx, workerPoolRepo: workerPoolRepo}
}
