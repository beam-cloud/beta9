package compute

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	defaultPrivateTransport  = types.BackendRouteTransportTSNet
	defaultPrivateFallback   = types.PrivatePoolFallbackInternal
	defaultPrivatePriority   = int32(1000)
	defaultPrivateExecutor   = types.DefaultAgentWorkerContainerMode
	defaultPrivateJoinTTL    = 30 * time.Minute
	agentStreamRefresh       = 30 * time.Second
	agentStreamEventCoalesce = 25 * time.Millisecond
)

func (s *Service) ListPrivatePools(ctx context.Context, in *pb.ListPrivatePoolsRequest) (*pb.ListPrivatePoolsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	states, err := s.listPrivatePoolStates(ctx, workspaceID, 0)
	if err != nil {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if limit := int(in.Limit); limit > 0 && len(states) > limit {
		states = states[:limit]
	}
	out := make([]*pb.PrivatePool, 0, len(states))
	for _, state := range states {
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
		if err != nil {
			return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		pool := privatePoolStateToProtoWithMachines(state, machines)
		pool.ReadyMachineCount = s.readyAgentMachineCount(machines)
		out = append(out, pool)
	}
	return &pb.ListPrivatePoolsResponse{Ok: true, Pools: out}, nil
}

func (s *Service) CreatePool(ctx context.Context, in *pb.CreatePoolRequest) (*pb.CreatePoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	if in.Pool == nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: "pool config is required"}, nil
	}
	config := normalizePoolConfig(in.Pool)
	if config.Name == "" {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	if err := model.ValidatePoolName(config.Name); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if _, err := computePoolFromProto(config, false); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := s.getPrivatePoolState(ctx, workspaceID, config.Name)
	if err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: "pool already exists in this workspace"}, nil
	}

	now := time.Now()
	state := &model.PoolState{
		Name:             config.Name,
		Selector:         config.Selector,
		Config:           config,
		Status:           "active",
		Source:           model.SourceAttached,
		Mode:             config.Mode,
		Transport:        config.Transport,
		Fallback:         config.Fallback,
		Priority:         config.Priority,
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if existing != nil {
		state.Reservations = existing.Reservations
		state.ReservedGPUs = existing.ReservedGPUs
		state.CommittedSpendMicros = existing.CommittedSpendMicros
		state.Source = existing.Source
		state.CreatedByTokenID = existing.CreatedByTokenID
		state.CreatedAt = existing.CreatedAt
		state.ExpiresAt = existing.ExpiresAt
	}
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if s.scheduler != nil {
		if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreated, ""))
	return &pb.CreatePoolResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (s *Service) DeletePool(ctx context.Context, in *pb.DeletePoolRequest) (*pb.DeletePoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	vendors := s.computeVendors()
	for _, reservation := range state.Reservations {
		if reservation.Source.IsAttached() || reservation.Source == model.SourceAutosolver {
			continue
		}
		vendor := vendors[reservation.Provider]
		if vendor == nil {
			continue
		}
		_ = vendor.DeleteReservation(ctx, computeReservationInstanceID(reservation))
	}

	if err := s.deletePrivatePoolState(ctx, workspaceID, in.Name); err != nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if s.scheduler != nil {
		s.scheduler.DeleteAgentPool(firstNonEmpty(state.Selector, state.Name))
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolDeleted, "deleted"))
	return &pb.DeletePoolResponse{Ok: true}, nil
}

func (s *Service) ExtendPoolCapacity(ctx context.Context, in *pb.ExtendPoolCapacityRequest) (*pb.ExtendPoolCapacityResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if in.Ttl != "" {
		state.Config.Ttl = in.Ttl
		ttl, err := model.ParseTTL(in.Ttl)
		if err != nil {
			return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		state.ExpiresAt = time.Now().Add(ttl)
	}
	if in.MaxSpend > 0 {
		state.Config.MaxSpend = in.MaxSpend
	}
	state.UpdatedAt = time.Now()
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolExtended, ""))
	return &pb.ExtendPoolCapacityResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (s *Service) ListPoolMachines(ctx context.Context, in *pb.ListPoolMachinesRequest) (*pb.ListPoolMachinesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := s.getPrivatePoolState(ctx, workspaceID, in.PoolName)
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if limit := int(in.Limit); limit > 0 && len(machines) > limit {
		machines = machines[:limit]
	}

	out := make([]*pb.Machine, 0, len(machines))
	for _, machine := range machines {
		out = append(out, s.agentMachineToProto(machine))
	}
	return &pb.ListPoolMachinesResponse{Ok: true, Machines: out}, nil
}

func (s *Service) DeletePoolMachine(ctx context.Context, in *pb.DeleteMachineRequest) (bool, *pb.DeleteMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" || in.MachineId == "" {
		return false, nil, nil
	}

	machine, handled, err := s.privateMachineForDelete(ctx, authInfo, workspaceID, in.PoolName, in.MachineId)
	if err != nil {
		return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if machine == nil {
		if handled {
			return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: "machine not found"}, nil
		}
		return false, nil, nil
	}

	if err := s.releasePrivateMachine(ctx, machine); err != nil {
		return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return true, &pb.DeleteMachineResponse{Ok: true}, nil
}

func (s *Service) privateMachineForDelete(ctx context.Context, authInfo *auth.AuthInfo, workspaceID, poolName, machineID string) (*model.AgentTokenState, bool, error) {
	if poolName != "" {
		pool, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
		if err != nil || pool == nil {
			return nil, pool != nil, err
		}
		if !computePoolCreatedByAuth(pool, authInfo) {
			return nil, true, nil
		}
		machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
		return machine, true, err
	}
	machine, err := s.computeRepo.GetAgentMachineStateForWorkspace(ctx, workspaceID, machineID)
	if err != nil || machine == nil {
		return machine, false, err
	}
	pool, err := s.getOwnedPrivatePoolState(ctx, authInfo, machine.PoolName)
	if err != nil || pool == nil {
		return nil, true, err
	}
	return machine, true, nil
}

func (s *Service) readyAgentMachineCount(machines []*model.AgentTokenState) uint32 {
	ready := uint32(0)
	now := time.Now()
	for _, machine := range machines {
		if agentMachineStatus(machine, s.agentMachineStatusWorker(machine), now) == types.MachineStatusAvailable {
			ready++
		}
	}
	return ready
}
