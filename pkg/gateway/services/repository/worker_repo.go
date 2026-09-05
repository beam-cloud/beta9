package repository_services

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *WorkerRepositoryService) GetNextContainerRequest(req *pb.GetNextContainerRequestRequest, stream pb.WorkerRepositoryService_GetNextContainerRequestServer) error {
	if err := s.workerRepo.RecoverPendingContainerRequests(req.WorkerId); err != nil {
		return err
	}
	var requestsReady <-chan struct{}
	if s.workerEvents != nil {
		sinkID, ready := s.workerEvents.registerRequests(req.WorkerId)
		defer s.workerEvents.unregister(sinkID)
		requestsReady = ready
	}
	if err := s.workerRepo.ToggleWorkerAvailable(req.WorkerId, ""); err != nil {
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
			requests, err := s.workerRepo.GetNextContainerRequests(req.WorkerId, containerRequestBatchSize)
			if err != nil {
				return stream.Send(&pb.GetNextContainerRequestResponse{
					Ok:       false,
					ErrorMsg: err.Error(),
				})
			}

			for i, request := range requests {
				if err := stream.Send(&pb.GetNextContainerRequestResponse{
					Ok:               true,
					ContainerRequest: request.ToProto(),
					DeliveryToken:    request.DeliveryToken,
				}); err != nil {
					if requeueErr := s.workerRepo.RequeueContainerRequests(req.WorkerId, requests[i:]); requeueErr != nil {
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
	err := s.workerRepo.AddContainerToWorker(req.WorkerId, req.ContainerId, req.DeliveryToken)
	if err != nil {
		log.Error().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("failed to add container to worker")
		return &pb.AddContainerToWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	log.Info().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Msg("container added to worker")
	return &pb.AddContainerToWorkerResponse{Ok: true}, nil
}

// ClaimContainer is the worker's single pre-start round trip: it acknowledges
// the delivery (the same atomic script as AddContainerToWorker), refreshes the
// pending lease, and vends runtime credentials against the state it just read.
// Every step after the acknowledgement reports claimed=true so the worker knows
// it owns the container's failure reporting.
func (s *WorkerRepositoryService) ClaimContainer(ctx context.Context, req *pb.ClaimContainerRequest) (*pb.ClaimContainerResponse, error) {
	logger := log.With().Str("pool_name", req.PoolName).Str("hostname", req.PodHostname).Str("worker_id", req.WorkerId).Str("container_id", req.ContainerId).Logger()

	if err := s.workerRepo.AddContainerToWorker(req.WorkerId, req.ContainerId, req.DeliveryToken); err != nil {
		logger.Warn().Err(err).Msg("container claim rejected")
		return &pb.ClaimContainerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	resp := &pb.ClaimContainerResponse{Claimed: true}

	// The claim is idempotent for its delivery token, so a transient
	// repository failure after it is answered with UNAVAILABLE and the worker
	// retries. A missing state is authoritative: the container is gone.
	//
	// The scheduler wrote the pending lease when it queued the request; renew
	// it now so a long backlog wait cannot expire the state mid-startup. The
	// worker's status heartbeat takes over from here. The renewal is a no-op
	// when the container has already moved on (a STOPPING that raced the
	// claim is refused, not overwritten), so the state is read after it: the
	// worker acts on the persisted status, never on a stale snapshot.
	if err := s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatusPending, int64(types.ContainerStateTtlSWhilePending)); err != nil {
		if (&types.ErrContainerStateNotFound{}).From(err) {
			resp.ErrorMsg = err.Error()
			return resp, nil
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		if (&types.ErrContainerStateNotFound{}).From(err) {
			resp.ErrorMsg = err.Error()
			return resp, nil
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resp.State = containerStateToProto(state)

	if req.Credentials != nil {
		resp.Credentials = s.vendRuntimeCredentials(ctx, req.Credentials, state)
		if !resp.Credentials.Ok {
			resp.ErrorMsg = resp.Credentials.ErrorMsg
			return resp, nil
		}
	}

	logger.Info().Msg("container claimed by worker")
	resp.Ok = true
	return resp, nil
}

func (s *WorkerRepositoryService) RemoveContainerFromWorker(ctx context.Context, req *pb.RemoveContainerFromWorkerRequest) (*pb.RemoveContainerFromWorkerResponse, error) {
	err := s.workerRepo.RemoveContainerFromWorker(req.WorkerId, req.ContainerId)
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
	err := s.workerRepo.ToggleWorkerAvailable(req.WorkerId, req.Generation)
	if err != nil {
		return &pb.ToggleWorkerAvailableResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ToggleWorkerAvailableResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) DisableWorker(ctx context.Context, req *pb.DisableWorkerRequest) (*pb.DisableWorkerResponse, error) {
	err := s.workerRepo.UpdateWorkerStatus(req.WorkerId, types.WorkerStatusDisabled)
	if err != nil {
		return &pb.DisableWorkerResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.DisableWorkerResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) UpdateWorkerCapacity(ctx context.Context, req *pb.UpdateWorkerCapacityRequest) (*pb.UpdateWorkerCapacityResponse, error) {
	worker := &types.Worker{Id: req.WorkerId}
	err := s.workerRepo.UpdateWorkerCapacity(worker, types.NewContainerRequestFromProto(req.ContainerRequest), types.CapacityUpdateType(req.CapacityChange))
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
	err := s.workerRepo.SetWorkerKeepAlive(req.WorkerId, types.WorkerKeepAlive{
		MachineId: req.MachineId,
	})
	if err != nil {
		return &pb.SetWorkerKeepAliveResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Tell an idle worker whether it is the pool's headroom, so it does not
	// idle out and leave the pool cold until the sizer's replacement boots.
	// Only idle workers can spin down, so only they pay for the pool scan; a
	// lookup failure answers "headroom" so the worker stays until a good read.
	headroom := false
	if req.Idle {
		headroom = true
		if worker, err := s.workerRepo.GetWorkerById(req.WorkerId); err == nil {
			headroom = scheduler.WorkerHoldsPoolHeadroom(s.workerRepo, s.appConfig, worker)
		}
	}

	return &pb.SetWorkerKeepAliveResponse{Ok: true, PoolHeadroom: headroom}, nil
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

func (s *WorkerRepositoryService) MoveContainerIp(ctx context.Context, req *pb.MoveContainerIpRequest) (*pb.MoveContainerIpResponse, error) {
	err := s.workerRepo.MoveContainerIp(req.NetworkPrefix, req.FromContainerId, req.ToContainerId, req.IpAddress)
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
	err := s.workerRepo.RemoveContainerIp(req.NetworkPrefix, req.ContainerId)
	if err != nil {
		return &pb.RemoveContainerIpResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RemoveContainerIpResponse{Ok: true}, nil
}
