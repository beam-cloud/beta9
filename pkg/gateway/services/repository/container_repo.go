package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerRepositoryService struct {
	ctx           context.Context
	containerRepo repository.ContainerRepository
	pb.UnimplementedContainerRepositoryServiceServer
}

func NewContainerRepositoryService(ctx context.Context, containerRepo repository.ContainerRepository) *ContainerRepositoryService {
	return &ContainerRepositoryService{ctx: ctx, containerRepo: containerRepo}
}

func (s *ContainerRepositoryService) GetContainerState(ctx context.Context, req *pb.GetContainerStateRequest) (*pb.GetContainerStateResponse, error) {
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return &pb.GetContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.GetContainerStateResponse{
		Ok:          true,
		ContainerId: req.ContainerId,
		State: &pb.ContainerState{
			Status:      string(state.Status),
			ContainerId: state.ContainerId,
			StubId:      state.StubId,
			ScheduledAt: state.ScheduledAt,
			StartedAt:   state.StartedAt,
			WorkspaceId: state.WorkspaceId,
			Gpu:         state.Gpu,
			GpuCount:    state.GpuCount,
			Cpu:         state.Cpu,
			Memory:      state.Memory,
		}}, nil
}

func (s *ContainerRepositoryService) DeleteContainerState(ctx context.Context, req *pb.DeleteContainerStateRequest) (*pb.DeleteContainerStateResponse, error) {
	err := s.containerRepo.DeleteContainerState(req.ContainerId)
	if err != nil {
		return &pb.DeleteContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.DeleteContainerStateResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) UpdateContainerStatus(ctx context.Context, req *pb.UpdateContainerStatusRequest) (*pb.UpdateContainerStatusResponse, error) {
	err := s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatus(req.Status), req.ExpirySeconds)
	if err != nil {
		return &pb.UpdateContainerStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.UpdateContainerStatusResponse{Ok: true}, nil
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
		route := backendRouteFromProto(req.Route)
		route.ContainerID = req.ContainerId
		if route.LocalTarget == "" {
			route.LocalTarget = req.Address
		}
		if err := s.containerRepo.SetBackendRoute(ctx, route); err != nil {
			return &pb.SetContainerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		address = types.BackendRouteAddress(route.RouteID)
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
	for _, routeProto := range req.Routes {
		route := backendRouteFromProto(routeProto)
		route.ContainerID = req.ContainerId
		if route.LocalTarget == "" {
			route.LocalTarget = addressMap[route.Port]
		}
		if err := s.containerRepo.SetBackendRoute(ctx, route); err != nil {
			return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		addressMap[route.Port] = types.BackendRouteAddress(route.RouteID)
	}

	err := s.containerRepo.SetContainerAddressMap(req.ContainerId, addressMap)
	if err != nil {
		return &pb.SetContainerAddressMapResponse{Ok: false, ErrorMsg: err.Error()}, nil
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
	address := req.Address
	if req.Route != nil {
		route := backendRouteFromProto(req.Route)
		route.ContainerID = req.ContainerId
		if route.LocalTarget == "" {
			route.LocalTarget = req.Address
		}
		if err := s.containerRepo.SetBackendRoute(ctx, route); err != nil {
			return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		address = types.BackendRouteAddress(route.RouteID)
	}

	err := s.containerRepo.SetWorkerAddress(req.ContainerId, address)
	if err != nil {
		return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerAddressResponse{Ok: true}, nil
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
