package repository_services

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type WorkerRepositoryService struct {
	workerRepo repository.WorkerRepository
	pb.UnimplementedWorkerRepositoryServiceServer
}

const (
	containerRequestPollingInterval time.Duration = 100 * time.Millisecond
)

func NewWorkerRepositoryService(workerRepo repository.WorkerRepository) *WorkerRepositoryService {
	return &WorkerRepositoryService{workerRepo: workerRepo}
}

func (s *WorkerRepositoryService) GetNextContainerRequest(req *pb.GetNextContainerRequestRequest, stream pb.WorkerRepositoryService_GetNextContainerRequestServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			request, err := s.workerRepo.GetNextContainerRequest(req.WorkerId)
			if err != nil {
				return stream.Send(&pb.GetNextContainerRequestResponse{
					Ok:       false,
					ErrorMsg: err.Error(),
				})
			}

			var containerRequest *pb.ContainerRequest = nil
			if request != nil {
				containerRequest = request.ToProto()
			}

			err = stream.Send(&pb.GetNextContainerRequestResponse{
				Ok:               true,
				ContainerRequest: containerRequest,
			})
			if err != nil {
				return err
			}

			time.Sleep(containerRequestPollingInterval)
		}
	}
}

func (s *WorkerRepositoryService) SetImagePullLock(ctx context.Context, req *pb.SetImagePullLockRequest) (*pb.SetImagePullLockResponse, error) {
	token, err := s.workerRepo.SetImagePullLock(req.WorkerId, req.ImageId)
	if err != nil {
		return &pb.SetImagePullLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetImagePullLockResponse{Ok: true, Token: token}, nil
}

func (s *WorkerRepositoryService) RemoveImagePullLock(ctx context.Context, req *pb.RemoveImagePullLockRequest) (*pb.RemoveImagePullLockResponse, error) {
	err := s.workerRepo.RemoveImagePullLock(req.WorkerId, req.ImageId, req.Token)
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

	return &pb.GetWorkerByIdResponse{Ok: true, Worker: worker.ToProto()}, nil
}

func (s *WorkerRepositoryService) ToggleWorkerAvailable(ctx context.Context, req *pb.ToggleWorkerAvailableRequest) (*pb.ToggleWorkerAvailableResponse, error) {
	err := s.workerRepo.ToggleWorkerAvailable(req.WorkerId)
	if err != nil {
		return &pb.ToggleWorkerAvailableResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ToggleWorkerAvailableResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) UpdateWorkerCapacity(ctx context.Context, req *pb.UpdateWorkerCapacityRequest) (*pb.UpdateWorkerCapacityResponse, error) {
	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.UpdateWorkerCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	err = s.workerRepo.UpdateWorkerCapacity(worker, types.NewContainerRequestFromProto(req.ContainerRequest), types.CapacityUpdateType(req.CapacityChange))
	if err != nil {
		return &pb.UpdateWorkerCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateWorkerCapacityResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveWorker(ctx context.Context, req *pb.RemoveWorkerRequest) (*pb.RemoveWorkerResponse, error) {
	err := s.workerRepo.RemoveWorker(req.WorkerId)
	if err != nil {
		return &pb.RemoveWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetWorkerKeepAlive(ctx context.Context, req *pb.SetWorkerKeepAliveRequest) (*pb.SetWorkerKeepAliveResponse, error) {
	err := s.workerRepo.SetWorkerKeepAlive(req.WorkerId)
	if err != nil {
		return &pb.SetWorkerKeepAliveResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerKeepAliveResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetNetworkLock(ctx context.Context, req *pb.SetNetworkLockRequest) (*pb.SetNetworkLockResponse, error) {
	token, err := s.workerRepo.SetNetworkLock(req.NetworkPrefix, int(req.Ttl), int(req.Retries))
	if err != nil {
		return &pb.SetNetworkLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetNetworkLockResponse{Ok: true, Token: token}, nil
}

func (s *WorkerRepositoryService) RemoveNetworkLock(ctx context.Context, req *pb.RemoveNetworkLockRequest) (*pb.RemoveNetworkLockResponse, error) {
	err := s.workerRepo.RemoveNetworkLock(req.NetworkPrefix, req.Token)
	if err != nil {
		return &pb.RemoveNetworkLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveNetworkLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetContainerIp(ctx context.Context, req *pb.SetContainerIpRequest) (*pb.SetContainerIpResponse, error) {
	err := s.workerRepo.SetContainerIp(req.NetworkPrefix, req.ContainerId, req.IpAddress)
	if err != nil {
		return &pb.SetContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerIpResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) GetContainerIp(ctx context.Context, req *pb.GetContainerIpRequest) (*pb.GetContainerIpResponse, error) {
	ip, err := s.workerRepo.GetContainerIp(req.NetworkPrefix, req.ContainerId)
	if err != nil {
		return &pb.GetContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetContainerIpResponse{Ok: true, IpAddress: ip}, nil
}

func (s *WorkerRepositoryService) GetContainerIps(ctx context.Context, req *pb.GetContainerIpsRequest) (*pb.GetContainerIpsResponse, error) {
	ips, err := s.workerRepo.GetContainerIps(req.NetworkPrefix)
	if err != nil {
		return &pb.GetContainerIpsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetContainerIpsResponse{Ok: true, Ips: ips}, nil
}

func (s *WorkerRepositoryService) RemoveContainerIp(ctx context.Context, req *pb.RemoveContainerIpRequest) (*pb.RemoveContainerIpResponse, error) {
	err := s.workerRepo.RemoveContainerIp(req.NetworkPrefix, req.ContainerId)
	if err != nil {
		return &pb.RemoveContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveContainerIpResponse{Ok: true}, nil
}
