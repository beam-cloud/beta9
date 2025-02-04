package repository_services

import (
	"context"
	"log"

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

func (s *WorkerPoolRepositoryService) GetWorkerPool(ctx context.Context, req *pb.GetWorkerPoolRequest) (*pb.GetWorkerPoolResponse, error) {
	workerPool, err := s.workerPoolRepo.GetWorkerPool(ctx, req.PoolName)
	if err != nil {
		return nil, err
	}

	log.Printf("workerPool: %+v", workerPool)

	return &pb.GetWorkerPoolResponse{
		Ok: true,
	}, nil
}
