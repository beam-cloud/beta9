package repository_services

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ContainerRepositoryService struct {
	ctx           context.Context
	containerRepo repository.ContainerRepository
	pb.UnimplementedContainerRepositoryServiceServer
}

var _ auth.RequestAuthorizer = (*ContainerRepositoryService)(nil)

type containerRepositoryRequest interface {
	GetContainerId() string
}

func NewContainerRepositoryService(ctx context.Context, containerRepo repository.ContainerRepository) *ContainerRepositoryService {
	return &ContainerRepositoryService{ctx: ctx, containerRepo: containerRepo}
}

// AuthorizeRequest restricts private workers to containers in their workspace.
// The auth interceptor calls it before dispatching a repository RPC.
func (s *ContainerRepositoryService) AuthorizeRequest(ctx context.Context, req interface{}) error {
	authInfo, err := workerRepositoryAuth(ctx)
	if err != nil {
		return err
	}
	if authInfo.Token.TokenType != types.TokenTypeWorkerPrivate {
		return nil
	}
	request, ok := req.(containerRepositoryRequest)
	if !ok {
		return status.Error(codes.PermissionDenied, "container is required")
	}
	state, err := containerStateForWorkspace(s.containerRepo, request.GetContainerId(), authInfo.Workspace.ExternalId)
	if err != nil {
		return err
	}
	var routes []*pb.BackendRoute
	switch req := req.(type) {
	case *pb.SetContainerAddressRequest:
		routes = []*pb.BackendRoute{req.Route}
	case *pb.SetContainerAddressMapRequest:
		routes = req.Routes
	case *pb.SetWorkerAddressRequest:
		routes = []*pb.BackendRoute{req.Route}
	}
	for _, route := range routes {
		if err := authorizeBackendRoute(state, route); err != nil {
			return err
		}
	}
	return nil
}

func (s *ContainerRepositoryService) GetContainerState(ctx context.Context, req *pb.GetContainerStateRequest) (*pb.GetContainerStateResponse, error) {
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return &pb.GetContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetContainerStateResponse{Ok: true, ContainerId: req.ContainerId, State: containerStateToProto(state)}, nil
}

func containerStateForWorkspace(repo repository.ContainerRepository, containerID, workspaceID string) (*types.ContainerState, error) {
	if containerID == "" {
		return nil, status.Error(codes.PermissionDenied, "container is required")
	}
	state, err := repo.GetContainerState(containerID)
	if err != nil || state == nil || state.WorkspaceId != workspaceID {
		return nil, status.Error(codes.PermissionDenied, "container is not in worker workspace")
	}
	return state, nil
}

func containerStateToProto(state *types.ContainerState) *pb.ContainerState {
	return &pb.ContainerState{
		Status:       string(state.Status),
		ContainerId:  state.ContainerId,
		StubId:       state.StubId,
		ScheduledAt:  state.ScheduledAt,
		StartedAt:    state.StartedAt,
		WorkspaceId:  state.WorkspaceId,
		Gpu:          state.Gpu,
		GpuCount:     state.GpuCount,
		Cpu:          state.Cpu,
		Memory:       state.Memory,
		WorkerId:     state.WorkerId,
		MachineId:    state.MachineId,
		Evictable:    state.Evictable,
		Evicting:     state.Evicting,
		DrainSeconds: state.DrainSeconds,
		EvictOrder:   state.EvictOrder,
	}
}

func (s *ContainerRepositoryService) DeleteContainerState(ctx context.Context, req *pb.DeleteContainerStateRequest) (*pb.DeleteContainerStateResponse, error) {
	err := s.containerRepo.DeleteContainerState(req.ContainerId)
	if err != nil {
		return &pb.DeleteContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.DeleteContainerStateResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) UpdateContainerStatus(ctx context.Context, req *pb.UpdateContainerStatusRequest) (*pb.UpdateContainerStatusResponse, error) {
	// A non-positive expiry would make the repository expire the state on the
	// spot; refuse it before anything is written.
	if req.ExpirySeconds <= 0 {
		return &pb.UpdateContainerStatusResponse{Ok: false, ErrorMsg: fmt.Sprintf("expiry_seconds must be positive, got %d", req.ExpirySeconds)}, nil
	}

	err := s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatus(req.Status), req.ExpirySeconds)
	if err != nil {
		return &pb.UpdateContainerStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// A refused transition (STOPPING is terminal) returns nil above; report the
	// persisted status so the worker can react without a second round trip.
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return &pb.UpdateContainerStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.UpdateContainerStatusResponse{Ok: true, Status: string(state.Status)}, nil
}

func (s *ContainerRepositoryService) SetContainerExitCode(ctx context.Context, req *pb.SetContainerExitCodeRequest) (*pb.SetContainerExitCodeResponse, error) {
	err := s.containerRepo.SetContainerExitCode(req.ContainerId, int(req.ExitCode))
	if err != nil {
		return &pb.SetContainerExitCodeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerExitCodeResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) SetContainerAddress(ctx context.Context, req *pb.SetContainerAddressRequest) (*pb.SetContainerAddressResponse, error) {
	address := req.Address
	if req.Route != nil {
		routeAddress, err := s.registerBackendRoute(ctx, req.ContainerId, req.Route, req.Address)
		if err != nil {
			return &pb.SetContainerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		address = routeAddress
	}

	err := s.containerRepo.SetContainerAddress(req.ContainerId, address)
	if err != nil {
		return &pb.SetContainerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetContainerAddressResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) SetContainerAddressMap(ctx context.Context, req *pb.SetContainerAddressMapRequest) (*pb.SetContainerAddressMapResponse, error) {
	addressMap := make(map[int32]string)
	for k, v := range req.AddressMap {
		addressMap[int32(k)] = v
	}
	routes := make([]types.BackendRoute, 0, len(req.Routes))
	for _, routeProto := range req.Routes {
		if routeProto == nil {
			return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: "backend route is required"}, nil
		}
		route, routeAddress, err := backendRoute(req.ContainerId, routeProto, addressMap[routeProto.Port])
		if err != nil {
			return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		routes = append(routes, route)
		addressMap[routeProto.Port] = routeAddress
	}
	var primaryAddress string
	if req.PrimaryPort != 0 {
		primaryAddress = addressMap[req.PrimaryPort]
		if primaryAddress == "" {
			return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: fmt.Sprintf("primary port %d has no address", req.PrimaryPort)}, nil
		}
	}
	if err := s.containerRepo.SetBackendRoutes(ctx, routes); err != nil {
		return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	err := s.containerRepo.SetContainerAddressMap(req.ContainerId, addressMap)
	if err != nil {
		return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if primaryAddress != "" {
		if err := s.containerRepo.SetContainerAddress(req.ContainerId, primaryAddress); err != nil {
			return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	return &pb.SetContainerAddressMapResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) GetContainerAddressMap(ctx context.Context, req *pb.GetContainerAddressMapRequest) (*pb.GetContainerAddressMapResponse, error) {
	addressMap, err := s.containerRepo.GetContainerAddressMap(req.ContainerId)
	if err != nil {
		return &pb.GetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	protoMap := make(map[int32]string)
	for k, v := range addressMap {
		protoMap[int32(k)] = v
	}

	return &pb.GetContainerAddressMapResponse{Ok: true, AddressMap: protoMap}, nil
}

func (s *ContainerRepositoryService) SetWorkerAddress(ctx context.Context, req *pb.SetWorkerAddressRequest) (*pb.SetWorkerAddressResponse, error) {
	err := publishWorkerAddress(ctx, s.containerRepo, req)
	if err != nil {
		return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.SetWorkerAddressResponse{Ok: true}, nil
}

func publishWorkerAddress(ctx context.Context, repo repository.ContainerRepository, req *pb.SetWorkerAddressRequest) error {
	address := req.Address
	if req.Route != nil {
		route, routeAddress, err := backendRoute(req.ContainerId, req.Route, req.Address)
		if err != nil {
			return err
		}
		if err := repo.SetBackendRoute(ctx, route); err != nil {
			return err
		}
		address = routeAddress
	}
	return repo.SetWorkerAddress(req.ContainerId, address)
}

func (s *ContainerRepositoryService) registerBackendRoute(ctx context.Context, containerID string, routeProto *pb.BackendRoute, defaultLocalTarget string) (string, error) {
	if routeProto == nil {
		return "", fmt.Errorf("backend route is required")
	}
	route, address, err := backendRoute(containerID, routeProto, defaultLocalTarget)
	if err != nil {
		return "", err
	}
	if err := s.containerRepo.SetBackendRoute(ctx, route); err != nil {
		return "", err
	}
	return address, nil
}

func authorizeBackendRoute(state *types.ContainerState, route *pb.BackendRoute) error {
	if route == nil {
		return nil
	}
	if route.WorkspaceId != state.WorkspaceId || route.WorkerId != state.WorkerId || route.MachineId != state.MachineId {
		return status.Error(codes.PermissionDenied, "route does not belong to container")
	}
	containerID := state.ContainerId
	if route.Kind == types.BackendRouteKindWorker {
		containerID = ""
	} else if route.Kind != types.BackendRouteKindContainer {
		return status.Error(codes.PermissionDenied, "invalid route kind")
	}
	if route.ContainerId != containerID || route.RouteId != types.BackendRouteID(state.MachineId, state.WorkerId, containerID, route.Kind, route.Port) {
		return status.Error(codes.PermissionDenied, "invalid route identity")
	}
	return nil
}

func backendRoute(containerID string, routeProto *pb.BackendRoute, defaultLocalTarget string) (types.BackendRoute, string, error) {
	route := backendRouteFromProto(routeProto)
	route.ContainerID = registeredRouteContainerID(containerID, route)
	if route.LocalTarget == "" {
		route.LocalTarget = defaultLocalTarget
	}
	if route.LocalTarget == "" {
		return types.BackendRoute{}, "", fmt.Errorf("backend route local target is required for port %d", route.Port)
	}
	return route, types.BackendRouteAddress(route.RouteID), nil
}

func backendRouteFromProto(in *pb.BackendRoute) types.BackendRoute {
	if in == nil {
		return types.BackendRoute{}
	}
	route := types.BackendRoute{
		RouteID:     in.RouteId,
		WorkspaceID: in.WorkspaceId,
		PoolName:    in.PoolName,
		MachineID:   in.MachineId,
		WorkerID:    in.WorkerId,
		ContainerID: in.ContainerId,
		Kind:        in.Kind,
		Port:        in.Port,
		Protocol:    in.Protocol,
		Transport:   in.Transport,
		LocalTarget: in.LocalTarget,
		ProxyTarget: in.ProxyTarget,
		State:       in.State,
		Error:       in.Error,
		UpdatedAt:   in.UpdatedAt,
	}
	if route.Protocol == "" {
		route.Protocol = types.BackendRouteProtocolTCP
	}
	if route.Transport == "" {
		route.Transport = types.BackendRouteTransportDirect
	}
	if route.State == "" {
		route.State = types.BackendRouteStateOpening
	}
	return route
}

func registeredRouteContainerID(containerID string, route types.BackendRoute) string {
	if route.Kind == types.BackendRouteKindWorker && route.ContainerID == "" {
		return ""
	}
	return containerID
}
