package repository_services

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

type WorkerRepositoryService struct {
	ctx                   context.Context
	cacheCoordinator      *cache.Coordinator
	cacheCoordinatorToken string
	cacheMetadata         cache.CacheMetadataStore
	workerEvents          *workerEventBroker
	workerRepo            repository.WorkerRepository
	containerRepo         repository.ContainerRepository
	backendRepo           repository.BackendRepository
	computeRepo           repository.ComputeRepository
	eventRepo             repository.EventRepository
	appConfig             types.AppConfig
	pb.UnimplementedWorkerRepositoryServiceServer
}

const (
	containerRequestPollingInterval   = 100 * time.Millisecond
	containerRequestHeartbeatInterval = 30 * time.Second
	containerRequestBatchSize         = 128
)

func NewWorkerRepositoryService(ctx context.Context, workerRepo repository.WorkerRepository, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, computeRepo repository.ComputeRepository, eventRepo repository.EventRepository, rdb *common.RedisClient, appConfig types.AppConfig, cacheCoordinatorToken string) *WorkerRepositoryService {
	service := &WorkerRepositoryService{
		ctx:                   ctx,
		workerRepo:            workerRepo,
		containerRepo:         containerRepo,
		backendRepo:           backendRepo,
		computeRepo:           computeRepo,
		eventRepo:             eventRepo,
		appConfig:             appConfig,
		cacheCoordinatorToken: configuredCacheCoordinatorToken(cacheCoordinatorToken),
	}
	if rdb != nil {
		service.cacheCoordinator = cache.NewCoordinator(repository.NewCacheRedisRepository(rdb))
		service.cacheMetadata = cache.NewRedisCacheMetadataStoreWithClient(cache.GlobalConfig{}, cache.ServerConfig{}, rdb.UniversalClient)
		service.workerEvents = newWorkerEventBroker(ctx, rdb)
	}
	return service
}

func (s *WorkerRepositoryService) authorizeWorkerDeliveryProcess(ctx context.Context, workerId, workerInstanceId, storageNodeId string) error {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return errWorkerIdentityUnauthorized
	}
	worker, err := s.workerRepo.GetWorkerById(workerId)
	if err != nil {
		return err
	}
	if worker == nil || worker.Id != workerId || worker.InstanceId == "" || worker.InstanceId != workerInstanceId ||
		worker.MachineId == "" || worker.MachineId != storageNodeId {
		return errWorkerIdentityUnauthorized
	}
	return authorizeRegisteredWorkerToken(ctx, authInfo, worker, s.computeRepo)
}

func (s *WorkerRepositoryService) GetNextContainerRequest(req *pb.GetNextContainerRequestRequest, stream pb.WorkerRepositoryService_GetNextContainerRequestServer) error {
	if req == nil {
		return errWorkerIdentityUnauthorized
	}
	if err := s.authorizeWorkerDeliveryProcess(stream.Context(), req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return err
	}
	if err := s.workerRepo.RecoverPendingContainerRequestsForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return err
	}
	var requestsReady <-chan struct{}
	if s.workerEvents != nil {
		sinkID, ready := s.workerEvents.registerRequests(req.WorkerId)
		defer s.workerEvents.unregister(sinkID)
		requestsReady = ready
	}
	if err := s.workerRepo.ToggleWorkerAvailableForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, ""); err != nil {
		return err
	}
	poll := time.NewTicker(containerRequestPollingInterval)
	defer poll.Stop()
	heartbeatAt := time.Now().Add(containerRequestHeartbeatInterval)
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			requests, err := s.workerRepo.GetNextContainerRequestsForProcess(req.WorkerId, req.WorkerInstanceId,
				req.StorageNodeId, containerRequestBatchSize)
			if err != nil {
				return stream.Send(&pb.GetNextContainerRequestResponse{
					Ok:       false,
					ErrorMsg: err.Error(),
				})
			}

			for i, request := range requests {
				if err := stream.Send(&pb.GetNextContainerRequestResponse{
					Ok:                  true,
					ContainerRequest:    request.ToProto(),
					DeliveryToken:       request.DeliveryToken,
					StateVolumePlanId:   request.StateVolumePlanId,
					StateVolumePlanHash: request.StateVolumePlanHash,
				}); err != nil {
					if requeueErr := s.workerRepo.RequeueContainerRequestsForProcess(req.WorkerId, req.WorkerInstanceId,
						req.StorageNodeId, requests[i:]); requeueErr != nil {
						log.Error().Err(requeueErr).Str("worker_id", req.WorkerId).Msg("failed to requeue undelivered container requests")
					}
					return err
				}
			}

			if len(requests) > 0 {
				continue
			}
			if time.Now().After(heartbeatAt) {
				if err := stream.Send(&pb.GetNextContainerRequestResponse{Ok: true}); err != nil {
					return err
				}
				heartbeatAt = time.Now().Add(containerRequestHeartbeatInterval)
			}

			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-requestsReady:
			case <-poll.C:
			}
		}
	}
}

func (s *WorkerRepositoryService) SetImagePullLock(ctx context.Context, req *pb.SetImagePullLockRequest) (*pb.SetImagePullLockResponse, error) {
	if req == nil {
		return &pb.SetImagePullLockResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.SetImagePullLockResponse{ErrorMsg: err.Error()}, nil
	}
	token, err := s.workerRepo.SetImagePullLockForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.ImageId)
	if err != nil {
		return &pb.SetImagePullLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetImagePullLockResponse{Ok: true, Token: token}, nil
}

func (s *WorkerRepositoryService) RemoveImagePullLock(ctx context.Context, req *pb.RemoveImagePullLockRequest) (*pb.RemoveImagePullLockResponse, error) {
	if req == nil {
		return &pb.RemoveImagePullLockResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.RemoveImagePullLockResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.RemoveImagePullLockForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.ImageId, req.Token)
	if err != nil {
		return &pb.RemoveImagePullLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveImagePullLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) AddContainerToWorker(ctx context.Context, req *pb.AddContainerToWorkerRequest) (*pb.AddContainerToWorkerResponse, error) {
	if req == nil {
		return &pb.AddContainerToWorkerResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.AddContainerToWorkerResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.AddContainerToWorkerForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.ContainerId, req.DeliveryToken,
		req.StateVolumePlanId, req.StateVolumePlanHash)
	if err != nil {
		log.Error().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("failed to add container to worker")
		return &pb.AddContainerToWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	log.Info().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("container added to worker")
	return &pb.AddContainerToWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveContainerFromWorker(ctx context.Context, req *pb.RemoveContainerFromWorkerRequest) (*pb.RemoveContainerFromWorkerResponse, error) {
	if req == nil {
		return &pb.RemoveContainerFromWorkerResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.RemoveContainerFromWorkerResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.RemoveContainerFromWorkerForProcess(req.WorkerId, req.WorkerInstanceId,
		req.StorageNodeId, req.ContainerId)
	if err != nil {
		log.Error().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("failed to remove container from worker")
		return &pb.RemoveContainerFromWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	log.Info().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("container removed from worker")
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
	if req == nil {
		return &pb.ToggleWorkerAvailableResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.ToggleWorkerAvailableResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.ToggleWorkerAvailableForProcess(req.WorkerId, req.WorkerInstanceId,
		req.StorageNodeId, req.Generation)
	if err != nil {
		return &pb.ToggleWorkerAvailableResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ToggleWorkerAvailableResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) DisableWorker(ctx context.Context, req *pb.DisableWorkerRequest) (*pb.DisableWorkerResponse, error) {
	if req == nil {
		return &pb.DisableWorkerResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.DisableWorkerResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.UpdateWorkerStatusForProcess(req.WorkerId, req.WorkerInstanceId,
		req.StorageNodeId, types.WorkerStatusDisabled)
	if err != nil {
		return &pb.DisableWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.DisableWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) UpdateWorkerCapacity(ctx context.Context, req *pb.UpdateWorkerCapacityRequest) (*pb.UpdateWorkerCapacityResponse, error) {
	if req == nil {
		return &pb.UpdateWorkerCapacityResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.UpdateWorkerCapacityResponse{ErrorMsg: err.Error()}, nil
	}
	worker := &types.Worker{Id: req.WorkerId}
	err := s.workerRepo.UpdateWorkerCapacityForProcess(worker, req.WorkerInstanceId, req.StorageNodeId,
		types.NewContainerRequestFromProto(req.ContainerRequest), types.CapacityUpdateType(req.CapacityChange))
	if err != nil {
		return &pb.UpdateWorkerCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateWorkerCapacityResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) RemoveWorker(ctx context.Context, req *pb.RemoveWorkerRequest) (*pb.RemoveWorkerResponse, error) {
	if req == nil {
		return &pb.RemoveWorkerResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.RemoveWorkerResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.RemoveWorkerForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId)
	if err != nil {
		return &pb.RemoveWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetWorkerKeepAlive(ctx context.Context, req *pb.SetWorkerKeepAliveRequest) (*pb.SetWorkerKeepAliveResponse, error) {
	if req == nil {
		return &pb.SetWorkerKeepAliveResponse{Ok: false, ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.MachineId); err != nil {
		return &pb.SetWorkerKeepAliveResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.SetWorkerKeepAlive(req.WorkerId, types.WorkerKeepAlive{
		MachineId: req.MachineId, InstanceId: req.WorkerInstanceId,
	})
	if err != nil {
		return &pb.SetWorkerKeepAliveResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerKeepAliveResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetWorkerStateVolumeCapacity(ctx context.Context, req *pb.SetWorkerStateVolumeCapacityRequest) (*pb.SetWorkerStateVolumeCapacityResponse, error) {
	if req == nil || req.WorkerId == "" || req.WorkerInstanceId == "" || req.MachineId == "" {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: "worker, process instance, and machine ids are required"}, nil
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if worker.InstanceId != req.WorkerInstanceId || worker.MachineId != req.MachineId {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: "state-volume capacity caller does not match the registered worker process"}, nil
	}
	if err := authorizeRegisteredWorkerToken(ctx, authInfo, worker, s.computeRepo); err != nil {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	err = s.workerRepo.SetWorkerStateVolumeCapacityForProcess(req.WorkerId, req.WorkerInstanceId,
		req.MachineId, req.TotalNbdDevices, req.FreeNbdDevices)
	if err != nil {
		return &pb.SetWorkerStateVolumeCapacityResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerStateVolumeCapacityResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetNetworkLock(ctx context.Context, req *pb.SetNetworkLockRequest) (*pb.SetNetworkLockResponse, error) {
	if req == nil {
		return &pb.SetNetworkLockResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.SetNetworkLockResponse{ErrorMsg: err.Error()}, nil
	}
	token, err := s.workerRepo.SetNetworkLockForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.NetworkPrefix, int(req.Ttl), int(req.Retries))
	if err != nil {
		return &pb.SetNetworkLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetNetworkLockResponse{Ok: true, Token: token}, nil
}

func (s *WorkerRepositoryService) RemoveNetworkLock(ctx context.Context, req *pb.RemoveNetworkLockRequest) (*pb.RemoveNetworkLockResponse, error) {
	if req == nil {
		return &pb.RemoveNetworkLockResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.RemoveNetworkLockResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.RemoveNetworkLockForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.NetworkPrefix, req.Token)
	if err != nil {
		return &pb.RemoveNetworkLockResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveNetworkLockResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) SetContainerIp(ctx context.Context, req *pb.SetContainerIpRequest) (*pb.SetContainerIpResponse, error) {
	if req == nil {
		return &pb.SetContainerIpResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.SetContainerIpResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.SetContainerIpForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.NetworkPrefix, req.ContainerId, req.IpAddress)
	if err != nil {
		return &pb.SetContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerIpResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) MoveContainerIp(ctx context.Context, req *pb.MoveContainerIpRequest) (*pb.MoveContainerIpResponse, error) {
	if req == nil {
		return &pb.MoveContainerIpResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.MoveContainerIpResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.MoveContainerIpForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.NetworkPrefix, req.FromContainerId, req.ToContainerId, req.IpAddress)
	if err != nil {
		return &pb.MoveContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.MoveContainerIpResponse{Ok: true}, nil
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

func (s *WorkerRepositoryService) GetContainerIpAssignments(ctx context.Context, req *pb.GetContainerIpAssignmentsRequest) (*pb.GetContainerIpAssignmentsResponse, error) {
	assignments, err := s.workerRepo.GetContainerIpAssignments(req.NetworkPrefix)
	if err != nil {
		return &pb.GetContainerIpAssignmentsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	responseAssignments := make([]*pb.ContainerIpAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		responseAssignments = append(responseAssignments, &pb.ContainerIpAssignment{
			ContainerId: assignment.ContainerID,
			IpAddress:   assignment.IPAddress,
		})
	}

	return &pb.GetContainerIpAssignmentsResponse{Ok: true, Assignments: responseAssignments}, nil
}

func (s *WorkerRepositoryService) RemoveContainerIp(ctx context.Context, req *pb.RemoveContainerIpRequest) (*pb.RemoveContainerIpResponse, error) {
	if req == nil {
		return &pb.RemoveContainerIpResponse{ErrorMsg: errWorkerIdentityUnauthorized.Error()}, nil
	}
	if err := s.authorizeWorkerDeliveryProcess(ctx, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId); err != nil {
		return &pb.RemoveContainerIpResponse{ErrorMsg: err.Error()}, nil
	}
	err := s.workerRepo.RemoveContainerIpForProcess(req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.NetworkPrefix, req.ContainerId)
	if err != nil {
		return &pb.RemoveContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveContainerIpResponse{Ok: true}, nil
}
