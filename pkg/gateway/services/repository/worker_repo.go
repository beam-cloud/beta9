package repository_services

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
)

type WorkerRepositoryService struct {
	workerRepo repository.WorkerRepository
	pb.UnimplementedWorkerRepositoryServiceServer
}

func NewWorkerRepositoryService(workerRepo repository.WorkerRepository) *WorkerRepositoryService {
	return &WorkerRepositoryService{workerRepo: workerRepo}
}
