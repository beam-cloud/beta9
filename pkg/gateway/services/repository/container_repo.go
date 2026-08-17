package repository_services

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ContainerRepositoryService struct {
	ctx           context.Context
	containerRepo repository.ContainerRepository
	backendRepo   repository.BackendRepository
	workerRepo    repository.WorkerRepository
	computeRepo   repository.ComputeRepository
	pb.UnimplementedContainerRepositoryServiceServer
}

func NewContainerRepositoryService(ctx context.Context, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, workerRepo repository.WorkerRepository, computeRepo repository.ComputeRepository) *ContainerRepositoryService {
	return &ContainerRepositoryService{ctx: ctx, containerRepo: containerRepo, backendRepo: backendRepo, workerRepo: workerRepo, computeRepo: computeRepo}
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
			Status:          string(state.Status),
			ContainerId:     state.ContainerId,
			StubId:          state.StubId,
			ScheduledAt:     state.ScheduledAt,
			StartedAt:       state.StartedAt,
			WorkspaceId:     state.WorkspaceId,
			Gpu:             state.Gpu,
			GpuCount:        state.GpuCount,
			NbdDevices:      state.NbdDevices,
			Cpu:             state.Cpu,
			Memory:          state.Memory,
			StateSnapshotId: state.StateSnapshotId,
			StateFork:       state.StateFork,
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

func (s *ContainerRepositoryService) SetStateRestoreReceipt(ctx context.Context, req *pb.SetStateRestoreReceiptRequest) (*pb.SetStateRestoreReceiptResponse, error) {
	if req == nil || req.Receipt == nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: "state restore receipt is required"}, nil
	}
	state, err := s.authorizeStateRestoreReceiptWorker(ctx, req)
	if err != nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	receipt := &types.StateRestoreReceipt{
		StateSnapshotId: strings.TrimSpace(req.Receipt.StateSnapshotId),
		RestoreMode:     strings.TrimSpace(req.Receipt.RestoreMode),
		FallbackReason:  strings.TrimSpace(req.Receipt.FallbackReason),
		Generations:     make([]types.StateGeneration, 0, len(req.Receipt.Generations)),
	}
	for _, generation := range req.Receipt.Generations {
		if generation != nil {
			receipt.Generations = append(receipt.Generations, types.StateGeneration{
				VolumeId: generation.VolumeId, GenerationId: generation.GenerationId, Name: generation.Name,
				MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
				Generation: generation.Generation, ParentGenerationId: generation.ParentGenerationId,
				CloneParentGenerationId: generation.CloneParentGenerationId,
			})
		}
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, state.WorkspaceId)
	if err != nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	snapshot, err := s.backendRepo.GetStateSnapshot(ctx, workspace.Id, receipt.StateSnapshotId)
	if err != nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	if err := validateStateRestoreReceipt(state, snapshot, receipt); err != nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	expectedAssignment := &types.ContainerState{
		WorkerId: req.WorkerId, MachineId: req.StorageNodeId, StateSnapshotId: receipt.StateSnapshotId,
		AssignmentId: req.DeliveryToken, StateVolumePlanId: req.StateVolumePlanId,
		StateVolumePlanHash: req.StateVolumePlanHash,
	}
	if err := s.containerRepo.SetStateRestoreReceipt(req.ContainerId, req.WorkerInstanceId, receipt, expectedAssignment); err != nil {
		return &pb.SetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.SetStateRestoreReceiptResponse{Ok: true}, nil
}

func (s *ContainerRepositoryService) authorizeStateRestoreReceiptWorker(ctx context.Context, req *pb.SetStateRestoreReceiptRequest) (*types.ContainerState, error) {
	if s.containerRepo == nil || s.backendRepo == nil || s.workerRepo == nil || strings.TrimSpace(req.ContainerId) == "" ||
		strings.TrimSpace(req.WorkerId) == "" || strings.TrimSpace(req.WorkerInstanceId) == "" || strings.TrimSpace(req.StorageNodeId) == "" ||
		strings.TrimSpace(req.DeliveryToken) == "" || strings.TrimSpace(req.StateVolumePlanId) == "" ||
		strings.TrimSpace(req.StateVolumePlanHash) == "" {
		return nil, fmt.Errorf("state restore receipt authority is unavailable")
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return nil, fmt.Errorf("state restore receipt requires an authenticated worker")
	}
	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil || worker == nil || worker.Id != req.WorkerId || worker.InstanceId != req.WorkerInstanceId || worker.MachineId != req.StorageNodeId {
		return nil, fmt.Errorf("state restore receipt caller does not match a registered worker and storage node")
	}
	if err := authorizeRegisteredWorkerToken(ctx, authInfo, worker, s.computeRepo); err != nil {
		return nil, err
	}
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return nil, err
	}
	if state.WorkerId != req.WorkerId || state.MachineId != req.StorageNodeId {
		return nil, fmt.Errorf("state restore receipt caller does not own the current container assignment")
	}
	if state.AssignmentId != req.DeliveryToken || state.StateVolumePlanId != req.StateVolumePlanId ||
		state.StateVolumePlanHash != req.StateVolumePlanHash {
		return nil, fmt.Errorf("state restore receipt caller does not match the current delivery epoch and state-volume plan")
	}
	if authInfo.Token.TokenType == types.TokenTypeWorkerPrivate &&
		(authInfo.Workspace == nil || authInfo.Workspace.ExternalId != state.WorkspaceId) {
		return nil, fmt.Errorf("state restore receipt worker workspace does not match the container")
	}
	return state, nil
}

func validateStateRestoreReceipt(state *types.ContainerState, snapshot *types.StateSnapshot, receipt *types.StateRestoreReceipt) error {
	if state == nil || snapshot == nil || receipt == nil {
		return fmt.Errorf("state restore receipt validation requires container and snapshot state")
	}
	if state.StateSnapshotId == "" || receipt.StateSnapshotId != state.StateSnapshotId || snapshot.ExternalId != state.StateSnapshotId {
		return fmt.Errorf("state restore receipt does not match the container's requested state snapshot")
	}
	if snapshot.Status != types.StateSnapshotStatusAvailable {
		return fmt.Errorf("state restore receipt snapshot is not available")
	}
	if !exactStateGenerations(snapshot.Generations, receipt.Generations) {
		return fmt.Errorf("state restore receipt generations do not match the authoritative snapshot membership")
	}
	hasMemory := strings.TrimSpace(snapshot.CheckpointId) != ""
	switch receipt.RestoreMode {
	case "memory":
		if !hasMemory || state.StateFork || state.StubId != snapshot.SourceStubExternalId || snapshot.Mode != "terminal" {
			return fmt.Errorf("memory restore receipt is not valid for this container and state snapshot")
		}
		if receipt.FallbackReason != "" {
			return fmt.Errorf("memory restore receipt cannot include a fallback reason")
		}
	case "cold_state":
		if hasMemory {
			if receipt.FallbackReason == "" {
				return fmt.Errorf("cold state receipt requires a fallback reason when memory was available")
			}
		} else if receipt.FallbackReason != strings.TrimSpace(snapshot.FallbackReason) {
			return fmt.Errorf("cold state receipt fallback does not match the snapshot outcome")
		}
	default:
		return fmt.Errorf("invalid state restore mode %q", receipt.RestoreMode)
	}
	return nil
}

func exactStateGenerations(authoritative, reported []types.StateGeneration) bool {
	if len(authoritative) == 0 || len(authoritative) != len(reported) {
		return false
	}
	left := append([]types.StateGeneration(nil), authoritative...)
	right := append([]types.StateGeneration(nil), reported...)
	less := func(values []types.StateGeneration, i, j int) bool {
		if values[i].Root != values[j].Root {
			return values[i].Root
		}
		if values[i].VolumeId != values[j].VolumeId {
			return values[i].VolumeId < values[j].VolumeId
		}
		return values[i].GenerationId < values[j].GenerationId
	}
	sort.Slice(left, func(i, j int) bool { return less(left, i, j) })
	sort.Slice(right, func(i, j int) bool { return less(right, i, j) })
	for index := range left {
		if left[index] != right[index] || (index > 0 && left[index-1].VolumeId == left[index].VolumeId) {
			return false
		}
	}
	return true
}

func (s *ContainerRepositoryService) GetStateRestoreReceipt(ctx context.Context, req *pb.GetStateRestoreReceiptRequest) (*pb.GetStateRestoreReceiptResponse, error) {
	if req == nil || strings.TrimSpace(req.ContainerId) == "" {
		return &pb.GetStateRestoreReceiptResponse{ErrorMsg: "state restore receipt read is unauthorized"}, nil
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || authInfo.Workspace == nil ||
		types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return &pb.GetStateRestoreReceiptResponse{ErrorMsg: "state restore receipt read is unauthorized"}, nil
	}
	state, err := s.containerRepo.GetContainerState(req.ContainerId)
	if err != nil {
		return &pb.GetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	if state == nil || strings.TrimSpace(state.WorkspaceId) == "" || state.WorkspaceId != authInfo.Workspace.ExternalId {
		return &pb.GetStateRestoreReceiptResponse{ErrorMsg: "state restore receipt read is unauthorized"}, nil
	}
	receipt, err := s.containerRepo.GetStateRestoreReceipt(req.ContainerId)
	if err != nil {
		return &pb.GetStateRestoreReceiptResponse{ErrorMsg: err.Error()}, nil
	}
	generations := make([]*pb.StateGeneration, 0, len(receipt.Generations))
	for _, generation := range receipt.Generations {
		generations = append(generations, &pb.StateGeneration{
			VolumeId: generation.VolumeId, GenerationId: generation.GenerationId, Name: generation.Name,
			MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
			Generation: generation.Generation, ParentGenerationId: generation.ParentGenerationId,
			CloneParentGenerationId: generation.CloneParentGenerationId,
		})
	}
	return &pb.GetStateRestoreReceiptResponse{Ok: true, Receipt: &pb.StateRestoreReceipt{
		StateSnapshotId: receipt.StateSnapshotId, RestoreMode: receipt.RestoreMode,
		FallbackReason: receipt.FallbackReason, Generations: generations,
	}}, nil
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
	address := req.Address
	if req.Route != nil {
		routeAddress, err := s.registerBackendRoute(ctx, req.ContainerId, req.Route, req.Address)
		if err != nil {
			return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		address = routeAddress
	}

	err := s.containerRepo.SetWorkerAddress(req.ContainerId, address)
	if err != nil {
		return &pb.SetWorkerAddressResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.SetWorkerAddressResponse{Ok: true}, nil
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
