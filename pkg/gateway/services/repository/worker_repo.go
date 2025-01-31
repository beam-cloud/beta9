package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerRepositoryService struct {
	workerRepo repository.WorkerRepository
	pb.UnimplementedWorkerRepositoryServiceServer
}

func NewWorkerRepositoryService(workerRepo repository.WorkerRepository) *WorkerRepositoryService {
	return &WorkerRepositoryService{workerRepo: workerRepo}
}

func (s *WorkerRepositoryService) GetNextContainerRequest(ctx context.Context, req *pb.GetNextContainerRequestRequest) (*pb.GetNextContainerRequestResponse, error) {
	request, err := s.workerRepo.GetNextContainerRequest(req.WorkerId)
	if err != nil {
		return &pb.GetNextContainerRequestResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.GetNextContainerRequestResponse{
		Ok: true,
		ContainerRequest: &pb.ContainerRequest{
			ContainerId: request.ContainerId,
			ImageId:     request.ImageId,
			Env:         request.Env,
			EntryPoint:  request.EntryPoint,
			Cpu:         request.Cpu,
			Memory:      request.Memory,
			Gpu:         request.Gpu,
			GpuRequest:  request.GpuRequest,
			GpuCount:    request.GpuCount,
			StubId:      request.StubId,
			WorkspaceId: request.WorkspaceId,
			Workspace: &pb.Workspace{
				Id:                 uint32(request.Workspace.Id),
				Name:               request.Workspace.Name,
				CreatedAt:          timestamppb.New(request.Workspace.CreatedAt),
				UpdatedAt:          timestamppb.New(request.Workspace.UpdatedAt),
				SigningKey:         *request.Workspace.SigningKey,
				VolumeCacheEnabled: request.Workspace.VolumeCacheEnabled,
				MultiGpuEnabled:    request.Workspace.MultiGpuEnabled,
				ConcurrencyLimitId: uint32(*request.Workspace.ConcurrencyLimitId),
				ConcurrencyLimit: &pb.ConcurrencyLimit{
					Id:                uint32(*request.Workspace.ConcurrencyLimitId),
					ExternalId:        request.Workspace.ConcurrencyLimit.ExternalId,
					GpuLimit:          request.Workspace.ConcurrencyLimit.GPULimit,
					CpuMillicoreLimit: request.Workspace.ConcurrencyLimit.CPUMillicoreLimit,
				},
			},
			Stub: &pb.StubWithRelated{
				Stub: &pb.Stub{
					Id:            uint32(request.Stub.Id),
					Name:          request.Stub.Name,
					Type:          string(request.Stub.Type),
					Config:        request.Stub.Config,
					ConfigVersion: uint32(request.Stub.ConfigVersion),
					ObjectId:      uint32(request.Stub.ObjectId),
					WorkspaceId:   uint32(request.Stub.WorkspaceId),
					CreatedAt:     timestamppb.New(request.Stub.CreatedAt),
				},
				Object: &pb.Object{
					Id:          uint32(request.Stub.ObjectId),
					Hash:        request.Stub.Object.Hash,
					Size:        request.Stub.Object.Size,
					WorkspaceId: uint32(request.Stub.WorkspaceId),
					CreatedAt:   timestamppb.New(request.Stub.Object.CreatedAt),
				},
			},
		},
		ReceivedRequest: true,
	}, nil
}

func (s *WorkerRepositoryService) SetImagePullLock(ctx context.Context, req *pb.SetImagePullLockRequest) (*pb.SetImagePullLockResponse, error) {
	err := s.workerRepo.SetImagePullLock(req.WorkerId, req.ImageId)
	if err != nil {
		return &pb.SetImagePullLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetImagePullLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveImagePullLock(ctx context.Context, req *pb.RemoveImagePullLockRequest) (*pb.RemoveImagePullLockResponse, error) {
	err := s.workerRepo.RemoveImagePullLock(req.WorkerId, req.ImageId)
	if err != nil {
		return &pb.RemoveImagePullLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveImagePullLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) AddContainerToWorker(ctx context.Context, req *pb.AddContainerToWorkerRequest) (*pb.AddContainerToWorkerResponse, error) {
	err := s.workerRepo.AddContainerToWorker(req.WorkerId, req.ContainerId)
	if err != nil {
		return &pb.AddContainerToWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.AddContainerToWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveContainerFromWorker(ctx context.Context, req *pb.RemoveContainerFromWorkerRequest) (*pb.RemoveContainerFromWorkerResponse, error) {
	err := s.workerRepo.RemoveContainerFromWorker(req.WorkerId, req.ContainerId)
	if err != nil {
		return &pb.RemoveContainerFromWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveContainerFromWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) GetWorkerById(ctx context.Context, req *pb.GetWorkerByIdRequest) (*pb.GetWorkerByIdResponse, error) {
	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.GetWorkerByIdResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetWorkerByIdResponse{Ok: true, Worker: &pb.Worker{
		Id:            worker.Id,
		Status:        string(worker.Status),
		TotalCpu:      worker.TotalCpu,
		TotalMemory:   worker.TotalMemory,
		TotalGpuCount: worker.TotalGpuCount,
		FreeCpu:       worker.FreeCpu,
		FreeMemory:    worker.FreeMemory,
		FreeGpuCount:  worker.FreeGpuCount,
		Gpu:           worker.Gpu,
		PoolName:      worker.PoolName,
		MachineId:     worker.MachineId,
		Priority:      worker.Priority,
		BuildVersion:  worker.BuildVersion,
	}}, nil
}
