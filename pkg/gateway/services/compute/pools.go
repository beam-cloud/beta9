package compute

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
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
		machines, deleted, err := s.poolMachines(ctx, workspaceID, state)
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
	return state != nil && state.Mode == string(types.PoolModePrivate)
}

// poolMachines lists a pool's machines for a call that reads them, and
// reports whether the pool was deleted instead. A BYOC pool is brought up to
// date first: one whose cloud resource is gone is cleaned up, and the records
// of disconnected machines beyond the number of nodes it wants are removed.
func (s *Service) poolMachines(ctx context.Context, workspaceID string, state *model.PoolState) ([]*model.AgentTokenState, bool, error) {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return nil, false, err
	}
	return s.reconcileBYOCPoolMachinesForRead(ctx, workspaceID, state, machines, time.Now().UTC())
}

// nextPoolState is the record a call saves for a pool. It starts from the
// pool as it is, so that nothing the call does not set is lost, or from a new
// pool when there is none, and it takes the pool's settings from config.
func nextPoolState(existing *model.PoolState, config *pb.PoolConfig, actorTokenID string, now time.Time) *model.PoolState {
	next := model.PoolState{Name: config.Name, CreatedByTokenID: actorTokenID, CreatedAt: now}
	if existing != nil {
		next = *existing
	}
	next.Selector = config.Selector
	next.Config = config
	next.Status = types.ComputePoolStatusActive
	next.Mode = config.Mode
	next.Transport = config.Transport
	next.Fallback = config.Fallback
	next.Priority = config.Priority
	next.UpdatedAt = now
	return &next
}

func (s *Service) CreatePool(ctx context.Context, in *pb.CreatePoolRequest) (*pb.CreatePoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	actorTokenID := computeActorTokenID(authInfo)
	if workspaceID == "" || actorTokenID == "" {
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
		if existing != nil && !poolStateIsPrivate(existing) {
			return fmt.Errorf("pool already exists in this workspace")
		}

		state = nextPoolState(existing, config, actorTokenID, time.Now())
		if existing == nil {
			state.Source = model.SourceAttached
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
	if !auth.HasPermission(authInfo) {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "Unauthorized Access"}, nil
	}
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	var state *model.PoolState
	err := s.withPoolStateLock(ctx, workspaceID, in.Name, func(lockCtx context.Context) error {
		var err error
		state, err = s.getWorkspacePrivatePoolState(lockCtx, authInfo, in.Name)
		if err != nil || state == nil {
			return err
		}
		if err := s.releasePrivatePoolMachinesLocked(lockCtx, workspaceID, state); err != nil {
			return err
		}
		if err := s.revokeBYOCBootstrapJoinTokens(lockCtx, state); err != nil {
			return err
		}

		if err := s.deletePoolInstances(lockCtx, workspaceID, state); err != nil {
			return err
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

// deletePoolInstances deletes every machine Beam bought for the pool at its
// provider. The pool's record is the only record of those machines, so when
// one cannot be deleted the record is kept, with that reservation left
// terminating for the reconciler to retry, and the pool is not deleted.
func (s *Service) deletePoolInstances(ctx context.Context, workspaceID string, state *model.PoolState) error {
	var failures []string
	now := time.Now().UTC()
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() {
			continue
		}
		if reservation.MachineID != "" {
			if err := s.computeRepo.DeleteMachineSSHState(ctx, workspaceID, state.Name, reservation.MachineID); err != nil {
				return err
			}
		}
		switch reservation.Status {
		case model.ReservationDeleted, model.ReservationInterrupted:
		case model.ReservationFailed:
			// A reservation recorded as failed before failed machines were
			// deleted at the provider may still have one.
			if err := s.deleteInstance(ctx, reservation, now); err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", reservation.Name, err))
			}
		default:
			s.terminateReservation(ctx, workspaceID, state, reservation, reconcileReasonMachineReleased, machineReleasedMessage, now)
			if reservation.Status == model.ReservationTerminating {
				failures = append(failures, fmt.Sprintf("%s: %s", reservation.Name, reservation.LastError))
			}
		}
	}
	if len(failures) == 0 {
		return nil
	}
	state.UpdatedAt = now
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return err
	}
	return fmt.Errorf("pool %q was not deleted because its machines could not be deleted at the provider (%s); deleting them is being retried, delete the pool again once they are gone", state.Name, strings.Join(failures, "; "))
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
		state, err := s.getWorkspacePrivatePoolState(lockCtx, authInfo, in.Name)
		if err != nil {
			return err
		}
		if state == nil {
			return errors.New("pool not found")
		}
		if in.Ttl != "" {
			ttl, err := model.ParseTTL(in.Ttl)
			if err != nil {
				return err
			}
			// The pool is to end this long from now. A time that is not
			// ahead keeps no machine for longer, and would only show the
			// pool as having ended.
			if ttl <= 0 {
				return errors.New("ttl must be positive")
			}
			state.Config.Ttl = in.Ttl
			state.ExpiresAt = time.Now().Add(ttl)
			s.extendManagedReservations(lockCtx, workspaceID, state, state.ExpiresAt)
		}
		if in.MaxSpend > 0 {
			state.Config.MaxSpend = in.MaxSpend
		}
		state.UpdatedAt = time.Now()
		if err := s.savePrivatePoolState(lockCtx, workspaceID, state); err != nil {
			return err
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
func (s *Service) extendManagedReservations(ctx context.Context, workspaceID string, state *model.PoolState, expiresAt time.Time) {
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || !reservation.Open() {
			continue
		}
		if !reservation.ExpiresAt.IsZero() && !expiresAt.After(reservation.ExpiresAt) {
			continue
		}
		if err := s.extendInstance(ctx, *reservation, expiresAt); err != nil {
			reservationFailed(workspaceID, state.Name, reservation, "extend", fmt.Errorf("extension failed: %w", err))
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
	machines, err := s.ListPrivateMachines(ctx, authInfo, in.PoolName, int(in.Limit))
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.ListPoolMachinesResponse{Ok: true, Machines: machines}, nil
}

// ListPrivateMachines lists private and BYOC inventory in one workspace.
func (s *Service) ListPrivateMachines(ctx context.Context, authInfo *auth.AuthInfo, poolName string, limit int) ([]*pb.Machine, error) {
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return nil, errors.New("missing workspace auth")
	}

	var states []*model.PoolState
	if poolName != "" {
		state, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
		if err != nil {
			return nil, err
		}
		if state == nil || !poolStateIsPrivate(state) {
			return nil, errors.New("pool not found")
		}
		states = []*model.PoolState{state}
	} else {
		var err error
		states, err = s.listPrivatePoolStates(ctx, workspaceID, 0)
		if err != nil {
			return nil, err
		}
	}

	showSSH := auth.HasInteractivePermission(authInfo) && s.appConfig.ManagedCompute.SSH.Enabled
	out := []*pb.Machine{}
	for _, state := range states {
		if !poolStateIsPrivate(state) {
			continue
		}
		machines, deleted, err := s.poolMachines(ctx, workspaceID, state)
		if err != nil {
			return nil, err
		}
		if deleted {
			continue
		}
		for _, machine := range machines {
			protoMachine := s.agentMachineToProto(machine)
			if showSSH {
				sshState, err := s.computeRepo.GetMachineSSHState(ctx, workspaceID, state.Name, machine.MachineID)
				if err != nil {
					return nil, err
				}
				if sshState != nil {
					protoMachine.Ssh = machineSSHAccessToProto(sshState)
				}
			}
			out = append(out, protoMachine)
			if limit > 0 && len(out) == limit {
				return out, nil
			}
		}
	}
	return out, nil
}

// ListMachineContainers lists the active containers scheduled on one machine.
// Works for private and provider pools alike — both are stored under the
// owning workspace, so providers can see what runs on their
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

func (s *Service) DeletePrivateMachine(ctx context.Context, in *pb.DeleteMachineRequest) (*pb.DeleteMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	if strings.TrimSpace(in.MachineId) == "" {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "machine id is required"}, nil
	}

	machine, pool, poolName, err := s.privateMachineForDelete(ctx, workspaceID, in.PoolName, in.MachineId)
	if err != nil {
		return deleteMachineFailed(err), nil
	}
	if machine != nil {
		if pool != nil && pool.Mode == string(types.PoolModeProvider) {
			// The provider is pulling hardware that serves other workspaces'
			// endpoint replicas. Stop them through the scheduler first so the
			// endpoint controller records the loss and replaces them, instead
			// of leaving orphaned containers running on a machine we no longer
			// route to.
			s.stopWorkerContainers(ctx, model.AgentMachineWorkerID(machine.MachineID), reconcileReasonMachineReleased)
		}
		if err := s.releasePrivateMachine(ctx, machine); err != nil {
			return deleteMachineFailed(err), nil
		}
		return &pb.DeleteMachineResponse{Ok: true}, nil
	}

	if poolName == "" {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "machine not found"}, nil
	}
	reconciled, err := s.reconcileMissingBYOCMachineForDelete(ctx, workspaceID, poolName)
	if err != nil {
		return deleteMachineFailed(err), nil
	}
	if reconciled {
		return &pb.DeleteMachineResponse{Ok: true}, nil
	}
	if err := s.releasePrivateReservationForDelete(ctx, workspaceID, poolName, in.MachineId); err != nil {
		return deleteMachineFailed(err), nil
	}
	return &pb.DeleteMachineResponse{Ok: true}, nil
}

// deleteMachineFailed answers a release that failed. A pool is locked while
// the reconciler checks its machines with their providers, which can outlast
// a release's wait for the lock; that is worth trying again.
func deleteMachineFailed(err error) *pb.DeleteMachineResponse {
	if common.IsRedisLockNotObtained(err) {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "the pool is busy with another change; try again in a few seconds"}
	}
	return &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}
}

func (s *Service) reconcileMissingBYOCMachineForDelete(ctx context.Context, workspaceID, poolName string) (bool, error) {
	state, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
	if err != nil || state == nil || !poolStateIsPrivate(state) || state.BYOC == nil {
		return false, err
	}
	if _, _, err := s.poolMachines(ctx, workspaceID, state); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) releasePrivateReservationForDelete(ctx context.Context, workspaceID, poolName, targetID string) error {
	machineID := ""
	err := s.withPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		state, err := s.getPrivatePoolState(lockCtx, workspaceID, poolName)
		if err != nil || state == nil || !poolStateIsPrivate(state) {
			return err
		}
		reservationIndex := managedReservationIndexForDeleteTarget(state, targetID)
		if reservationIndex < 0 {
			return nil
		}
		reservation := &state.Reservations[reservationIndex]
		machineID = reservation.MachineID
		if reservation.Dismissable() {
			state.Reservations = append(state.Reservations[:reservationIndex], state.Reservations[reservationIndex+1:]...)
			now := time.Now().UTC()
			state.ReservedNodes = activeReservationNodes(state.Reservations, now)
			state.UpdatedAt = now
			return s.savePrivatePoolState(lockCtx, workspaceID, state)
		}
		return s.releaseManagedReservation(lockCtx, workspaceID, state, reservation)
	})
	if err != nil || machineID == "" {
		return err
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	if err != nil || machine == nil {
		return err
	}
	return s.removePrivateMachine(ctx, machine)
}

// privateMachineForDelete resolves a machine the workspace may delete: one in
// its private pools, or one it contributes to a provider pool. The pool is
// returned so the caller can tell the two apart.
func (s *Service) privateMachineForDelete(ctx context.Context, workspaceID, poolName, machineID string) (*model.AgentTokenState, *model.PoolState, string, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		machine, err := s.computeRepo.GetAgentMachineStateForWorkspace(ctx, workspaceID, machineID)
		if err != nil || machine == nil {
			return machine, nil, "", err
		}
		poolName = machine.PoolName
	}

	pool, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
	if err != nil || pool == nil || !(poolStateIsPrivate(pool) || pool.Mode == string(types.PoolModeProvider)) {
		return nil, nil, "", err
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	return machine, pool, poolName, err
}
