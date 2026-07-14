package compute

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	agentStreamHeartbeat     = 10 * time.Second
	agentStreamEventCoalesce = 25 * time.Millisecond
)

func (s *Service) ListPrivatePools(ctx context.Context, in *pb.ListPrivatePoolsRequest) (*pb.ListPrivatePoolsResponse, error) {
	if in == nil {
		in = &pb.ListPrivatePoolsRequest{}
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	states, err := s.listPrivatePoolStates(ctx, workspaceID, 0)
	if err != nil {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	limit := int(in.Limit)
	out := make([]*pb.PrivatePool, 0, len(states))
	for _, state := range states {
		if !poolStateIsPrivate(state) {
			continue
		}
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
		if err != nil {
			return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		machines, deleted, err := s.reconcileBYOCPoolMachinesForRead(ctx, workspaceID, state, machines, time.Now().UTC())
		if err != nil {
			return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if deleted {
			continue
		}
		pool := s.privatePoolStateToProtoWithMachines(state, machines)
		out = append(out, pool)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return &pb.ListPrivatePoolsResponse{Ok: true, Pools: out}, nil
}

func poolStateIsPrivate(state *model.PoolState) bool {
	if state == nil {
		return false
	}
	return state.Mode == "" || state.Mode == string(types.PoolModePrivate)
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
	pool, err := computePoolFromProto(config, 0, false)
	if err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	config.Gpu = pool.GPUs
	for _, name := range []string{config.Name, config.Selector} {
		if _, exists := s.appConfig.Worker.Pools[name]; exists {
			return &pb.CreatePoolResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q conflicts with a configured worker pool", name)}, nil
		}
	}

	var state *model.PoolState
	err = s.withPoolStateLock(ctx, workspaceID, config.Name, func(lockCtx context.Context) error {
		existing, err := s.getPrivatePoolState(lockCtx, workspaceID, config.Name)
		if err != nil {
			return err
		}
		if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
			return fmt.Errorf("pool already exists in this workspace")
		}

		now := time.Now()
		state = &model.PoolState{
			Name:             config.Name,
			Selector:         config.Selector,
			Config:           config,
			Status:           types.ComputePoolStatusActive,
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
			state.ReservedNodes = existing.ReservedNodes
			state.CommittedSpendMicros = existing.CommittedSpendMicros
			state.Source = existing.Source
			state.CreatedByTokenID = existing.CreatedByTokenID
			state.CreatedAt = existing.CreatedAt
			state.ExpiresAt = existing.ExpiresAt
		}
		if err := s.savePrivatePoolState(lockCtx, workspaceID, state); err != nil {
			return err
		}
		if s.scheduler == nil {
			return nil
		}
		if err := s.scheduler.EnsureAgentPool(workspaceID, state); err != nil {
			return s.rollbackPoolState(lockCtx, workspaceID, state.Name, existing, err)
		}
		if existing != nil && firstNonEmpty(existing.Selector, existing.Name) != firstNonEmpty(state.Selector, state.Name) {
			s.scheduler.DeleteAgentPool(workspaceID, existing)
		}
		return nil
	})
	if err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreated, ""))
	return &pb.CreatePoolResponse{Ok: true, Pool: s.privatePoolStateToProto(state)}, nil
}

func (s *Service) DeletePool(ctx context.Context, in *pb.DeletePoolRequest) (*pb.DeletePoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	var state *model.PoolState
	err := s.withPoolStateLock(ctx, workspaceID, in.Name, func(lockCtx context.Context) error {
		var err error
		state, err = s.getOwnedPrivatePoolState(lockCtx, authInfo, in.Name)
		if err != nil || state == nil {
			return err
		}
		if err := s.releasePrivatePoolMachinesLocked(lockCtx, workspaceID, state); err != nil {
			return err
		}
		if err := s.revokeBYOCBootstrapJoinTokens(lockCtx, state); err != nil {
			return err
		}

		vendors := s.computeVendors()
		for _, reservation := range state.Reservations {
			if reservation.Source.IsAttached() || reservation.Source == model.SourceAutosolver {
				continue
			}
			if vendor := vendors[reservation.Provider]; vendor != nil {
				_ = vendor.DeleteReservation(lockCtx, computeReservationInstanceID(reservation))
			}
		}
		if err := s.deletePrivatePoolState(lockCtx, workspaceID, in.Name); err != nil {
			return err
		}
		if s.scheduler != nil {
			s.scheduler.DeleteAgentPool(workspaceID, state)
		}
		return nil
	})
	if err != nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.DeletePoolResponse{Ok: true}, nil
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolDeleted, "deleted"))
	return &pb.DeletePoolResponse{Ok: true}, nil
}

func (s *Service) releasePrivatePoolMachinesLocked(ctx context.Context, workspaceID string, state *model.PoolState) error {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return err
	}
	for _, machine := range machines {
		if releaseErr := s.releaseBYOCProviderMachine(ctx, machine); releaseErr != nil {
			if !byocReleaseErrorAllowsLocalReconcile(releaseErr) && !errors.Is(releaseErr, errBYOCDirectScaleUnavailable) {
				return releaseErr
			}

			current, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, state.Name, machine.MachineID)
			if err != nil {
				return err
			}
			if current != nil {
				machine = current
			}
			if model.AgentMachineConnected(machine, time.Now().UTC()) {
				return releaseErr
			}
		}
		reservation, err := managedReservationForMachineRelease(state, machine.MachineID)
		if err != nil {
			return err
		}
		if reservation != nil {
			if err := s.releaseManagedReservation(ctx, workspaceID, state, reservation); err != nil {
				return err
			}
		}
		if err := s.removePrivateMachine(ctx, machine); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ExtendPoolCapacity(ctx context.Context, in *pb.ExtendPoolCapacityRequest) (*pb.ExtendPoolCapacityResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	var response *pb.ExtendPoolCapacityResponse
	lockErr := s.withPoolStateLock(ctx, workspaceID, in.Name, func(lockCtx context.Context) error {
		state, err := s.getOwnedPrivatePoolState(lockCtx, authInfo, in.Name)
		if err != nil {
			response = &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		if state == nil {
			response = &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "pool not found"}
			return nil
		}
		if in.Ttl != "" {
			state.Config.Ttl = in.Ttl
			ttl, err := model.ParseTTL(in.Ttl)
			if err != nil {
				response = &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}
				return nil
			}
			state.ExpiresAt = time.Now().Add(ttl)
			s.extendManagedReservations(lockCtx, state, state.ExpiresAt)
		}
		if in.MaxSpend > 0 {
			state.Config.MaxSpend = in.MaxSpend
		}
		state.UpdatedAt = time.Now()
		if err := s.savePrivatePoolState(lockCtx, workspaceID, state); err != nil {
			response = &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolExtended, ""))
		response = &pb.ExtendPoolCapacityResponse{Ok: true, Pool: s.privatePoolStateToProto(state)}
		return nil
	})
	if lockErr != nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: lockErr.Error()}, nil
	}
	return response, nil
}

// extendManagedReservations pushes a new expiry to the pool's open managed
// reservations and their provider-side auto-delete thresholds, so extending a
// pool actually extends the instances backing it.
func (s *Service) extendManagedReservations(ctx context.Context, state *model.PoolState, expiresAt time.Time) {
	vendors := s.computeVendors()
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservationClosed(reservation.Status) || reservation.Status == model.ReservationTerminating {
			continue
		}
		if !reservation.ExpiresAt.IsZero() && !expiresAt.After(reservation.ExpiresAt) {
			continue
		}
		vendor := vendors[reservation.Provider]
		if vendor == nil {
			reservation.LastError = fmt.Sprintf("vendor %q is not configured", reservation.Provider)
			continue
		}
		if err := vendor.ExtendReservation(ctx, computeReservationInstanceID(*reservation), expiresAt); err != nil {
			reservation.LastError = fmt.Sprintf("extension failed: %v", err)
			continue
		}
		additional := expiresAt.Sub(reservation.ExpiresAt)
		reservation.ExpiresAt = expiresAt
		reservation.CommittedMicros += reservation.HourlyCostMicros * model.WholeHours(additional)
		reservation.LastError = ""
		state.CommittedSpendMicros += s.billableMicros(reservation.HourlyCostMicros * model.WholeHours(additional))
	}
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
	if state == nil || !poolStateIsPrivate(state) {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	machines, deleted, err := s.reconcileBYOCPoolMachinesForRead(ctx, workspaceID, state, machines, time.Now().UTC())
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if deleted {
		return &pb.ListPoolMachinesResponse{Ok: true, Machines: nil}, nil
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

// ListMachineContainers lists the active containers scheduled on one machine.
// Works for private pools and marketplace listing pools alike — both are
// stored under the owning workspace, so sellers can see what runs on their
// hardware.
func (s *Service) ListMachineContainers(ctx context.Context, in *pb.ListMachineContainersRequest) (*pb.ListMachineContainersResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	poolName := strings.TrimSpace(in.PoolName)
	machineID := strings.TrimSpace(in.MachineId)
	if poolName == "" || machineID == "" {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: "pool name and machine id are required"}, nil
	}

	ownerWorkspaceID := workspaceID
	poolFound := false
	if requireClusterAdmin(authInfo) == nil && s.managedPoolRepo != nil {
		// Serverless (managed) pools are not private pool states — they live
		// in the managed pool store under the admin workspace. Cluster admins
		// can inspect their machines from any workspace.
		adminID, err := s.adminWorkspaceID(ctx)
		if err != nil {
			return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		managed, err := s.managedPoolRepo.GetManagedPoolState(ctx, adminID, poolName)
		if err != nil {
			return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if managed != nil {
			ownerWorkspaceID = adminID
			poolFound = true
		}
	}
	if !poolFound {
		state, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
		if err != nil {
			return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		poolFound = state != nil
	}
	if !poolFound {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, ownerWorkspaceID, poolName, machineID)
	if err != nil {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if machine == nil {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: "machine not found"}, nil
	}
	if s.containerRepo == nil {
		return &pb.ListMachineContainersResponse{Ok: true}, nil
	}

	containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(machine.MachineID))
	if err != nil {
		return &pb.ListMachineContainersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := make([]*pb.MachineContainer, 0, len(containers))
	for _, container := range containers {
		out = append(out, &pb.MachineContainer{
			ContainerId: container.ContainerId,
			StubId:      container.StubId,
			Status:      string(container.Status),
			WorkspaceId: container.WorkspaceId,
			Gpu:         container.Gpu,
			GpuCount:    container.GpuCount,
			Cpu:         container.Cpu,
			Memory:      container.Memory,
			ScheduledAt: container.ScheduledAt,
			StartedAt:   container.StartedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ScheduledAt > out[j].ScheduledAt })
	return &pb.ListMachineContainersResponse{Ok: true, Containers: out}, nil
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
			reconciled, err := s.reconcileMissingBYOCMachineForDelete(ctx, authInfo, workspaceID, in.PoolName)
			if err != nil {
				return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
			}
			if reconciled {
				return true, &pb.DeleteMachineResponse{Ok: true}, nil
			}
			released, err := s.releasePrivateReservationForDelete(ctx, authInfo, workspaceID, in.PoolName, in.MachineId)
			if err != nil {
				return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
			}
			if released {
				return true, &pb.DeleteMachineResponse{Ok: true}, nil
			}
			deleted, err := s.missingOwnedPrivateMachineDeleted(ctx, authInfo, in.PoolName)
			if err != nil {
				return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
			}
			if deleted {
				return true, &pb.DeleteMachineResponse{Ok: true}, nil
			}
			return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: "machine not found"}, nil
		}
		return false, nil, nil
	}

	if err := s.releasePrivateMachine(ctx, machine); err != nil {
		return true, &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return true, &pb.DeleteMachineResponse{Ok: true}, nil
}

func (s *Service) missingOwnedPrivateMachineDeleted(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (bool, error) {
	if poolName == "" {
		return false, nil
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, poolName)
	return state != nil, err
}

func (s *Service) reconcileMissingBYOCMachineForDelete(ctx context.Context, authInfo *auth.AuthInfo, workspaceID, poolName string) (bool, error) {
	if poolName == "" {
		return false, nil
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, poolName)
	if err != nil || state == nil || state.BYOC == nil {
		return false, err
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return false, err
	}
	if _, _, err := s.reconcileBYOCPoolMachinesForRead(ctx, workspaceID, state, machines, time.Now().UTC()); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) releasePrivateReservationForDelete(ctx context.Context, authInfo *auth.AuthInfo, workspaceID, poolName, targetID string) (bool, error) {
	if poolName == "" || targetID == "" {
		return false, nil
	}

	found := false
	machineID := ""
	err := s.withPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		state, err := s.getOwnedPrivatePoolState(lockCtx, authInfo, poolName)
		if err != nil || state == nil {
			return err
		}
		reservationIndex := managedReservationIndexForDeleteTarget(state, targetID)
		if reservationIndex < 0 {
			return nil
		}
		reservation := &state.Reservations[reservationIndex]
		found = true
		machineID = reservation.MachineID
		if reservation.Status == model.ReservationFailed {
			state.Reservations = append(state.Reservations[:reservationIndex], state.Reservations[reservationIndex+1:]...)
			now := time.Now().UTC()
			state.ReservedNodes = activeReservationNodes(state.Reservations, now)
			state.UpdatedAt = now
			return s.savePrivatePoolState(lockCtx, workspaceID, state)
		}
		return s.releaseManagedReservation(lockCtx, workspaceID, state, reservation)
	})
	if err != nil || !found {
		return found, err
	}
	if machineID == "" {
		return true, nil
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	if err != nil || machine == nil {
		return true, err
	}
	return true, s.removePrivateMachine(ctx, machine)
}

func (s *Service) privateMachineForDelete(ctx context.Context, authInfo *auth.AuthInfo, workspaceID, poolName, machineID string) (*model.AgentTokenState, bool, error) {
	if poolName != "" {
		pool, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
		if err != nil || pool == nil || !poolStateIsPrivate(pool) {
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
