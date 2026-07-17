package compute

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *Service) JoinAgent(ctx context.Context, in *pb.JoinAgentRequest) (*pb.JoinAgentResponse, error) {
	if strings.TrimSpace(in.JoinToken) == "" {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}

	tokenState, err := s.getComputeJoinTokenState(ctx, in.JoinToken)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if tokenState == nil || tokenState.Revoked || joinTokenExpired(tokenState, time.Now()) {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}
	if err := s.bindJoinTokenFingerprint(ctx, tokenState, in.MachineFingerprint); err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	var poolState *model.PoolState
	if tokenState.ManagedPoolInstanceID != "" {
		if activationErr := s.requireManagedPoolRepository(); activationErr != nil {
			err = activationErr
		} else {
			poolState, err = s.managedPoolRepo.GetManagedPoolState(ctx, tokenState.WorkspaceID, tokenState.PoolName)
			if err == nil {
				poolState, err = s.activeManagedPoolState(poolState)
			}
		}
	}
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if tokenState.ManagedPoolInstanceID != "" {
		if poolState == nil || poolState.ManagementSource == "" ||
			poolState.Mode != string(types.PoolModeExternal) ||
			poolState.ManagedInstanceID == "" ||
			tokenState.ManagedPoolInstanceID != poolState.ManagedInstanceID {
			return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
		}
	}

	agentToken, err := generateComputeToken()
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	machineID := strings.TrimSpace(tokenState.MachineID)
	if machineID == "" {
		machineID = model.AgentMachineID(tokenState.WorkspaceID, tokenState.PoolName, in.MachineFingerprint)
	}
	existing, err := s.computeRepo.GetAgentMachineState(ctx, tokenState.WorkspaceID, tokenState.PoolName, machineID)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	workerImageOverride := strings.TrimSpace(in.WorkerImage)
	if workerImageOverride == agentWorkerImage(s.appConfig) {
		workerImageOverride = ""
	}
	now := time.Now()
	agentState := &model.AgentTokenState{
		TokenHash:                 hashComputeToken(agentToken),
		WorkspaceID:               tokenState.WorkspaceID,
		PoolName:                  tokenState.PoolName,
		Mode:                      tokenState.Mode,
		MarketplaceListingID:      tokenState.MarketplaceListingID,
		SellerWorkspaceID:         tokenState.SellerWorkspaceID,
		ManagedPoolInstanceID:     tokenState.ManagedPoolInstanceID,
		MachineID:                 machineID,
		MachineFingerprint:        in.MachineFingerprint,
		Hostname:                  in.Hostname,
		OS:                        in.Os,
		Arch:                      in.Arch,
		CPUCount:                  in.CpuCount,
		CPUMillicores:             firstNonZeroInt64(in.CpuMillicores, int64(in.CpuCount)*1000),
		MemoryMB:                  in.MemoryMb,
		GPUs:                      in.Gpu,
		GPUIDs:                    in.GpuIds,
		GPUCount:                  in.GpuCount,
		Executor:                  firstNonEmpty(in.Executor, defaultPrivateExecutor),
		NetworkSlotPoolSize:       in.NetworkSlotPoolSize,
		ContainerStartConcurrency: in.ContainerStartConcurrency,
		WorkerImageOverride:       workerImageOverride,
		Schedulable:               in.Schedulable,
		Preflight:                 preflightChecksFromProto(in.Preflight),
		CreatedAt:                 now,
		LastJoinAt:                now,
		LastHeartbeatAt:           now,
	}
	if existing != nil {
		agentState.Cordoned = existing.Cordoned
		if !existing.CreatedAt.IsZero() {
			agentState.CreatedAt = existing.CreatedAt
		}
	}
	if poolState != nil && poolState.ManagementSource != "" && poolState.WorkerConfig != nil {
		if poolState.WorkerConfig.NetworkSlotPoolSize > 0 {
			agentState.NetworkSlotPoolSize = uint32(poolState.WorkerConfig.NetworkSlotPoolSize)
		}
		if poolState.WorkerConfig.ContainerStartConcurrency > 0 {
			agentState.ContainerStartConcurrency = uint32(poolState.WorkerConfig.ContainerStartConcurrency)
		}
	}
	var bootstrap *pb.AgentBootstrapConfig
	if agentState.ManagedPoolInstanceID == "" {
		poolState, bootstrap, err = s.commitPrivateAgentJoin(ctx, tokenState, agentState)
	} else {
		if err = s.enforcePoolGPUType(ctx, poolState, agentState); err == nil {
			bootstrap, err = s.agentBootstrapConfig(ctx, tokenState.WorkspaceID, poolState)
		}
		if err == nil {
			bootstrap.Executor = agentState.Executor
			err = s.saveJoinedAgentState(ctx, poolState, agentState)
		}
		if err == nil {
			err = s.ensureAgentMachine(ctx, agentState, poolState)
		}
		if err != nil {
			_ = s.removePrivateMachineWorker(agentState)
			_ = s.computeRepo.DeleteAgentMachineState(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
		}
	}
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineID:   machineID,
		Action:      types.EventComputeActionMachineJoined,
		Status:      string(agentMachineStatus(agentState, s.agentMachineStatusWorker(agentState), now)),
		Transport:   normalizePoolConfig(poolState.Config).Transport,
		Executor:    agentState.Executor,
		Hostname:    agentState.Hostname,
		OS:          agentState.OS,
		Arch:        agentState.Arch,
		CPUCount:    agentState.CPUCount,
		MemoryMB:    agentState.MemoryMB,
		GPUCount:    agentState.GPUCount,
		GPUs:        agentState.GPUs,
		Schedulable: boolPtr(agentState.Schedulable),
		Message:     preflightSummary(agentState.Preflight),
		Attrs: map[string]string{
			"cpu_millicores": strconv.FormatInt(agentState.CPUMillicores, 10),
			"gpu_ids":        strings.Join(agentState.GPUIDs, ","),
		},
	})

	return &pb.JoinAgentResponse{
		Ok:          true,
		WorkspaceId: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineId:   machineID,
		AgentToken:  agentToken,
		Bootstrap:   bootstrap,
	}, nil
}

func (s *Service) commitPrivateAgentJoin(ctx context.Context, token *model.JoinTokenState, agent *model.AgentTokenState) (*model.PoolState, *pb.AgentBootstrapConfig, error) {
	var pool *model.PoolState
	var bootstrap *pb.AgentBootstrapConfig
	err := s.withPoolStateLock(ctx, agent.WorkspaceID, agent.PoolName, func(lockCtx context.Context) error {
		var err error
		pool, err = s.getPrivatePoolState(lockCtx, agent.WorkspaceID, agent.PoolName)
		if err != nil {
			return err
		}
		if pool == nil {
			return errors.New("pool no longer exists")
		}
		if pool.CreatedAt.IsZero() || token.PoolCreatedAt.IsZero() || !pool.CreatedAt.Equal(token.PoolCreatedAt) {
			return errors.New("join token is invalid or expired")
		}
		agent.Mode = firstNonEmpty(token.Mode, pool.Mode)
		agent.MarketplaceListingID = firstNonEmpty(token.MarketplaceListingID, pool.MarketplaceListingID)
		agent.SellerWorkspaceID = firstNonEmpty(token.SellerWorkspaceID, pool.SellerWorkspaceID)
		if err := s.enforcePoolGPUType(lockCtx, pool, agent); err != nil {
			return err
		}
		bootstrap, err = s.agentBootstrapConfig(lockCtx, agent.WorkspaceID, pool)
		if err != nil {
			return err
		}
		bootstrap.Executor = agent.Executor
		if err := s.saveComputeAgentTokenState(lockCtx, agent); err != nil {
			return err
		}
		if err := s.assignManagedReservationToMachineLocked(lockCtx, pool, token, agent); err != nil {
			return errors.Join(err, s.computeRepo.DeleteAgentMachineState(lockCtx, agent.WorkspaceID, agent.PoolName, agent.MachineID))
		}
		if err := s.ensureAgentMachine(lockCtx, agent, pool); err != nil {
			return errors.Join(err, s.removePrivateMachineWorker(agent), s.computeRepo.DeleteAgentMachineState(lockCtx, agent.WorkspaceID, agent.PoolName, agent.MachineID))
		}
		return nil
	})
	return pool, bootstrap, err
}

func (s *Service) saveJoinedAgentState(ctx context.Context, poolState *model.PoolState, agentState *model.AgentTokenState) error {
	if agentState == nil {
		return errors.New("agent state is unavailable")
	}
	if agentState.ManagedPoolInstanceID == "" {
		return s.saveComputeAgentTokenState(ctx, agentState)
	}
	return s.withManagedPoolStateLock(ctx, agentState.WorkspaceID, agentState.PoolName, func(lockCtx context.Context) error {
		persisted, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, agentState.WorkspaceID, agentState.PoolName)
		if err != nil {
			return err
		}
		current, err := s.activeManagedPoolState(persisted)
		if err != nil {
			return err
		}
		if current == nil || current.ManagedInstanceID != agentState.ManagedPoolInstanceID {
			return errors.New("managed pool is no longer active")
		}
		if current.ManagedInstanceID != poolState.ManagedInstanceID {
			return errors.New("managed pool changed while the machine was joining; retry")
		}
		return s.saveComputeAgentTokenState(lockCtx, agentState)
	})
}

func (s *Service) ensureAgentMachine(ctx context.Context, agentState *model.AgentTokenState, poolState *model.PoolState) error {
	if s.scheduler == nil {
		return nil
	}
	if poolState == nil {
		var err error
		poolState, err = s.getAgentPoolState(ctx, agentState)
		if err != nil {
			return err
		}
		if poolState == nil {
			if agentState.ManagedPoolInstanceID != "" {
				return nil
			}
			return errors.New("pool no longer exists")
		}
	}
	if agentState.ManagedPoolInstanceID != "" {
		return s.ensureManagedAgentMachine(ctx, poolState, agentState.MachineID)
	}
	return s.scheduler.EnsureAgentMachine(agentState.WorkspaceID, poolState, agentState.MachineID)
}

// bindJoinTokenFingerprint pins machine-specific join tokens to the first
// fingerprint that uses them; pool-wide tokens stay reusable.
func (s *Service) bindJoinTokenFingerprint(ctx context.Context, tokenState *model.JoinTokenState, fingerprint string) error {
	if tokenState.MachineID == "" {
		return nil
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return nil
	}
	if tokenState.BoundFingerprint == "" {
		tokenState.BoundFingerprint = fingerprint
		return s.saveComputeJoinTokenState(ctx, tokenState, joinTokenStateTTL(tokenState))
	}
	if tokenState.BoundFingerprint != fingerprint {
		return fmt.Errorf("join token is already bound to another machine")
	}
	return nil
}

// requireAgentState resolves an agent token to its current machine state,
// returning a user-facing error message when the token is invalid or stale.
// Shared by every agent-token-authenticated RPC in this file.
func (s *Service) requireAgentState(ctx context.Context, token string) (*model.AgentTokenState, string) {
	agentState, err := s.getCurrentComputeAgentTokenState(ctx, token)
	if err != nil {
		return nil, err.Error()
	}
	if agentState == nil {
		return nil, "invalid agent token"
	}
	if agentState.ManagedPoolInstanceID != "" {
		if err := s.requireManagedPoolRepository(); err != nil {
			return nil, err.Error()
		}
		state, err := s.managedPoolRepo.GetManagedPoolState(ctx, agentState.WorkspaceID, agentState.PoolName)
		if err != nil {
			return nil, err.Error()
		}
		if state == nil || state.Name != agentState.PoolName || state.WorkspaceID != agentState.WorkspaceID ||
			state.ManagementSource == "" || state.Mode != string(types.PoolModeExternal) ||
			agentState.ManagedPoolInstanceID == "" || agentState.ManagedPoolInstanceID != state.ManagedInstanceID {
			return nil, "managed pool no longer exists"
		}
	}
	return agentState, ""
}

func (s *Service) getAgentPoolState(ctx context.Context, agentState *model.AgentTokenState) (*model.PoolState, error) {
	if agentState == nil {
		return nil, nil
	}
	if agentState.ManagedPoolInstanceID != "" && s.managedPoolRepo != nil {
		state, err := s.managedPoolRepo.GetManagedPoolState(ctx, agentState.WorkspaceID, agentState.PoolName)
		if err != nil {
			return nil, err
		}
		return s.activeManagedPoolState(state)
	}
	return s.getPrivatePoolState(ctx, agentState.WorkspaceID, agentState.PoolName)
}

func (s *Service) RequestAgentTransportCredential(ctx context.Context, in *pb.RequestAgentTransportCredentialRequest) (*pb.RequestAgentTransportCredentialResponse, error) {
	agentState, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: errMsg}, nil
	}

	transport := types.NormalizeBackendRouteTransport(in.Transport)
	if err := s.validateAgentTransportConfig(transport); err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emitComputeEvent(types.EventComputeTransport, types.EventComputeSchema{
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionTransportCredentialVended,
		Status:      "ready",
		Transport:   transport,
	})

	return &pb.RequestAgentTransportCredentialResponse{
		Ok:         true,
		AuthKey:    s.appConfig.Tailscale.AgentAuthKey,
		ControlUrl: s.appConfig.Tailscale.ControlURL,
		Hostname:   types.AgentTailnetHostnamePrefix + agentState.MachineID,
		Ephemeral:  true,
	}, nil
}

func (s *Service) StreamAgent(in *pb.StreamAgentRequest, stream pb.GatewayService_StreamAgentServer) error {
	ctx := stream.Context()
	agentState, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: errMsg})
	}
	agentState, err := s.touchAgentHeartbeat(ctx, agentState)
	if err != nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
	}
	if err := s.ensureAgentMachine(ctx, agentState, nil); err != nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
	}
	// Record disconnect on any stream exit; recordAgentDisconnect no-ops
	// while the heartbeat is still fresh.
	defer func() {
		s.recordAgentDisconnect(context.Background(), agentState)
	}()

	sendSnapshot := func() error {
		current, err := s.currentComputeAgentState(ctx, agentState)
		if err != nil {
			return err
		}
		if current == nil {
			msg := "agent token is no longer current"
			if err := stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: msg}); err != nil {
				return err
			}
			return fmt.Errorf("agent token is no longer current")
		}
		agentState = current
		routes, err := s.agentRoutesForMachine(ctx, agentState)
		if err != nil {
			return err
		}
		slots, err := s.agentSlotsForMachine(ctx, agentState)
		if err != nil {
			return err
		}
		return stream.Send(&pb.StreamAgentResponse{Ok: true, Routes: routes, Slots: slots})
	}

	for {
		if err := sendSnapshot(); err != nil {
			if !isAgentSnapshotTransient(err) {
				return err
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		break
	}

	events := make(chan common.KeyEvent, 32)
	if s.keyEventManager != nil {
		revisionKey := agentSnapshotRevisionKey(agentState)
		if err := s.keyEventManager.ListenForPublishedPattern(ctx, revisionKey, events); err != nil {
			return err
		}
	}

	ticker := time.NewTicker(agentStreamRefresh)
	defer ticker.Stop()

	// Refresh the heartbeat from this stream too, so a brief telemetry stream
	// outage does not mark the machine disconnected.
	heartbeat := time.NewTicker(agentStreamHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-events:
			coalesceAgentStreamEvents(ctx, events)
			if err := sendSnapshot(); err != nil {
				if isAgentSnapshotTransient(err) {
					continue
				}
				return err
			}
		case <-heartbeat.C:
			current, err := s.touchAgentHeartbeat(ctx, agentState)
			if err != nil {
				return err
			}
			agentState = current
			worker := s.agentMachineStatusWorker(agentState)
			if worker == nil || worker.Status == types.WorkerStatusDisabled {
				if err := s.ensureAgentMachine(ctx, agentState, nil); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := sendSnapshot(); err != nil {
				if isAgentSnapshotTransient(err) {
					continue
				}
				return err
			}
		}
	}
}

func isAgentSnapshotTransient(err error) bool {
	return common.IsRedisLockNotObtained(err)
}

// touchAgentHeartbeat refreshes the machine's liveness timestamp without
// touching telemetry-reported metrics.
func (s *Service) touchAgentHeartbeat(ctx context.Context, agentState *model.AgentTokenState) (*model.AgentTokenState, error) {
	current, err := s.currentComputeAgentState(ctx, agentState)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, errors.New("agent token is no longer current")
	}
	now := time.Now().UTC()
	if current.LastHeartbeatAt.After(now) {
		return current, nil
	}
	current.LastHeartbeatAt = now
	current.LastDisconnectAt = time.Time{}
	if err := s.saveComputeAgentTokenState(ctx, current); err != nil {
		return nil, err
	}
	return current, nil
}

func coalesceAgentStreamEvents(ctx context.Context, events <-chan common.KeyEvent) {
	timer := time.NewTimer(agentStreamEventCoalesce)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-events:
			continue
		case <-timer.C:
			return
		}
	}
}

func (s *Service) agentRoutesForMachine(ctx context.Context, agentState *model.AgentTokenState) ([]*pb.AgentRoute, error) {
	routes, err := s.listAgentRoutesForMachine(ctx, agentState)
	if err != nil {
		return nil, err
	}

	out := make([]*pb.AgentRoute, 0, len(routes))
	for _, route := range routes {
		if !agentCanManageRoute(agentState, route) {
			continue
		}
		if route.State == types.BackendRouteStateClosing {
			continue
		}
		out = append(out, agentRouteToProto(route))
	}
	return out, nil
}

func (s *Service) listAgentRoutesForMachine(ctx context.Context, agentState *model.AgentTokenState) ([]types.BackendRoute, error) {
	return s.containerRepo.ListBackendRoutesByMachineID(ctx, agentState.MachineID)
}

func agentSnapshotRevisionKey(agentState *model.AgentTokenState) string {
	return common.RedisKeys.SchedulerBackendRouteMachineIDRevision(agentState.MachineID)
}

func (s *Service) notifyAgentPool(ctx context.Context, workspaceID, poolName string) error {
	if s.redisClient == nil || s.computeRepo == nil {
		return nil
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, poolName)
	if err != nil {
		return err
	}
	var errs []error
	for _, machine := range machines {
		if machine == nil || machine.MachineID == "" {
			continue
		}
		err := s.redisClient.Publish(ctx, agentSnapshotRevisionKey(machine), common.KeyOperationSet).Err()
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func agentCanManageRoute(agentState *model.AgentTokenState, route types.BackendRoute) bool {
	if agentState == nil {
		return false
	}
	return route.PoolName == agentState.PoolName &&
		route.MachineID == agentState.MachineID
}

func (s *Service) agentSlotsForMachine(ctx context.Context, agentState *model.AgentTokenState) ([]*pb.AgentWorkerSlot, error) {
	poolState, err := s.getAgentPoolState(ctx, agentState)
	if err != nil {
		return nil, err
	}
	var poolConfig types.WorkerPoolConfig
	if poolState != nil && poolState.WorkerConfig != nil {
		poolConfig = *poolState.WorkerConfig
	}
	slots, err := s.computeRepo.ListAgentWorkerSlotStates(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return nil, err
	}
	worker, err := s.agentMachineWorker(agentState)
	if err != nil {
		return nil, err
	}
	if worker == nil {
		if err := s.pruneAgentWorkerSlots(ctx, agentState, "", slots); err != nil {
			return nil, err
		}
		return nil, nil
	}

	slot, workerToken, err := s.ensureAgentWorkerSlot(ctx, agentState, worker, poolConfig, slots)
	if err != nil {
		return nil, err
	}
	if err := s.pruneAgentWorkerSlots(ctx, agentState, worker.Id, slots); err != nil {
		return nil, err
	}
	return []*pb.AgentWorkerSlot{agentWorkerSlotToProto(slot, workerToken)}, nil
}

func (s *Service) agentMachineWorker(agentState *model.AgentTokenState) (*types.Worker, error) {
	worker, err := s.workerRepo.GetWorkerById(model.AgentMachineWorkerID(agentState.MachineID))
	if err != nil {
		notFoundErr := &types.ErrWorkerNotFound{}
		if notFoundErr.From(err) {
			return nil, nil
		}
		return nil, err
	}
	if worker.MachineId != agentState.MachineID || worker.PoolName != agentState.PoolName {
		return nil, nil
	}
	if worker.Status == types.WorkerStatusDisabled && !agentState.Cordoned && worker.RolloutGeneration == "" {
		return nil, nil
	}
	return worker, nil
}

func (s *Service) ensureAgentWorkerSlot(ctx context.Context, agentState *model.AgentTokenState, worker *types.Worker, poolConfig types.WorkerPoolConfig, slots []*model.AgentWorkerSlotState) (*model.AgentWorkerSlotState, string, error) {
	var existing *model.AgentWorkerSlotState
	for _, slot := range slots {
		if slot != nil && slot.WorkerID == worker.Id {
			existing = slot
			break
		}
	}

	workspace, tokenType, err := s.workerTokenWorkspaceAndType(ctx, agentState)
	if err != nil {
		return nil, "", err
	}
	token, tokenID, tokenHash, err := s.agentWorkerToken(ctx, workspace.Id, tokenType, existing)
	if err != nil {
		return nil, "", err
	}

	slot := agentWorkerSlotState(s.appConfig, agentState, worker, poolConfig, tokenID, tokenHash)
	if err := setAgentWorkerSlotGeneration(slot); err != nil {
		return nil, "", err
	}
	if existing != nil {
		slot.CreatedAt = existing.CreatedAt
	}
	if existing != nil && existing.Generation == slot.Generation {
		return existing, token, nil
	}
	if existing != nil {
		prepared, err := s.workerRepo.PrepareWorkerRollout(worker.Id, slot.Generation)
		if err != nil {
			return nil, "", err
		}
		if !prepared {
			return existing, token, nil
		}
	}
	if err := s.computeRepo.SaveAgentWorkerSlotState(ctx, slot); err != nil {
		return nil, "", err
	}
	if existing == nil {
		s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			WorkerID:    worker.Id,
			Action:      types.EventComputeActionWorkerSlotCreated,
			Status:      string(worker.Status),
			CPUCount:    agentState.CPUCount,
			MemoryMB:    uint64(worker.TotalMemory),
			GPUCount:    worker.TotalGpuCount,
			GPUs:        agentState.GPUs,
			Attrs: map[string]string{
				"cpu_millicores": fmt.Sprintf("%d", worker.TotalCpu),
				"gpu_ids":        strings.Join(agentState.GPUIDs, ","),
			},
		})
	}
	return slot, token, nil
}

func (s *Service) workerTokenWorkspaceAndType(ctx context.Context, agentState *model.AgentTokenState) (*types.Workspace, string, error) {
	if agentState == nil {
		return nil, "", fmt.Errorf("agent state is unavailable")
	}
	if agentState.Mode == string(types.PoolModeMarketplace) {
		workspace, err := s.backendRepo.GetAdminWorkspace(ctx)
		return workspace, types.TokenTypeWorker, err
	}
	tokenType := types.TokenTypeWorkerPrivate
	if agentState.Mode == string(types.PoolModeExternal) {
		tokenType = types.TokenTypeWorker
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, agentState.WorkspaceID)
	if err != nil {
		return nil, tokenType, err
	}
	return &workspace, tokenType, nil
}

// agentWorkerToken mints (or reuses) the worker token for an agent worker
// slot. Private-pool workers use workspace-scoped TokenTypeWorkerPrivate;
// marketplace and managed external workers use trusted worker tokens from
// the admin workspace because they serve workloads from many workspaces.
func (s *Service) agentWorkerToken(ctx context.Context, workspaceID uint, tokenType string, existing *model.AgentWorkerSlotState) (string, string, string, error) {
	if existing != nil && existing.WorkerTokenID != "" {
		token, err := s.backendRepo.GetTokenByExternalId(ctx, workspaceID, existing.WorkerTokenID)
		if err == nil && token != nil && token.Active && !token.DisabledByClusterAdmin && token.TokenType == tokenType {
			tokenHash := hashComputeToken(token.Key)
			if existing.WorkerTokenHash == "" || existing.WorkerTokenHash == tokenHash {
				return token.Key, token.ExternalId, tokenHash, nil
			}
		}
	}

	createdToken, err := s.backendRepo.CreateToken(ctx, workspaceID, tokenType, true)
	if err != nil {
		return "", "", "", err
	}
	return createdToken.Key, createdToken.ExternalId, hashComputeToken(createdToken.Key), nil
}

func (s *Service) pruneAgentWorkerSlots(ctx context.Context, agentState *model.AgentTokenState, keepWorkerID string, slots []*model.AgentWorkerSlotState) error {
	for _, slot := range slots {
		if slot == nil || slot.WorkerID == "" || slot.WorkerID == keepWorkerID {
			continue
		}
		if err := s.computeRepo.DeleteAgentWorkerSlotState(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID, slot.WorkerID); err != nil {
			return err
		}
		s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			WorkerID:    slot.WorkerID,
			Action:      types.EventComputeActionWorkerSlotPruned,
			Status:      "stale",
			Message:     "removed stale agent worker slot because the scheduler worker no longer owns this machine",
		})
	}
	return nil
}

func agentWorkerSlotState(config types.AppConfig, agentState *model.AgentTokenState, worker *types.Worker, poolConfig types.WorkerPoolConfig, tokenID, tokenHash string) *model.AgentWorkerSlotState {
	runtimeName := agentWorkerRuntime(agentState, worker)
	networkSlots := agentState.NetworkSlotPoolSize
	startConcurrency := agentState.ContainerStartConcurrency
	requiresPoolSelector := worker.RequiresPoolSelector
	priority := worker.Priority
	preemptable := worker.Preemptable
	var cpuAffinityEnforced *bool

	// Managed pool configuration is authoritative and may change while the
	// machine remains connected. Machine capacity still comes from the worker.
	if agentState.ManagedPoolInstanceID != "" {
		runtimeName = firstNonEmpty(poolConfig.ContainerRuntime, types.ContainerRuntimeRunc.String())
		if poolConfig.NetworkSlotPoolSize > 0 {
			networkSlots = uint32(poolConfig.NetworkSlotPoolSize)
		}
		if poolConfig.ContainerStartConcurrency > 0 {
			startConcurrency = uint32(poolConfig.ContainerStartConcurrency)
		}
		requiresPoolSelector = poolConfig.RequiresPoolSelector
		priority = poolConfig.Priority
		preemptable = poolConfig.Preemptable
		cpuAffinityEnforced = &poolConfig.CPUAffinityEnforced
	}
	return &model.AgentWorkerSlotState{
		WorkerID:                  worker.Id,
		WorkerTokenID:             tokenID,
		WorkerTokenHash:           tokenHash,
		WorkspaceID:               agentState.WorkspaceID,
		PoolName:                  agentState.PoolName,
		MachineID:                 agentState.MachineID,
		Mode:                      firstNonEmpty(agentState.Mode, string(types.PoolModePrivate)),
		ContainerRuntime:          runtimeName,
		ContainerRuntimeConfig:    poolConfig.ContainerRuntimeConfig.WithDefaults(runtimeName),
		CPUAffinityEnforced:       cpuAffinityEnforced,
		MarketplaceListingID:      agentState.MarketplaceListingID,
		SellerWorkspaceID:         agentState.SellerWorkspaceID,
		CPU:                       worker.TotalCpu,
		Memory:                    worker.TotalMemory,
		GPU:                       worker.Gpu,
		GPUCount:                  worker.TotalGpuCount,
		GPUAssignment:             strings.Join(agentState.GPUIDs, ","),
		NetworkPrefix:             common.WorkerNetworkPrefix(config.ClusterName, agentState.MachineID),
		WorkerImage:               firstNonEmpty(agentState.WorkerImageOverride, agentWorkerImage(config)),
		NetworkSlotPoolSize:       networkSlots,
		ContainerStartConcurrency: startConcurrency,
		RequiresPoolSelector:      requiresPoolSelector,
		Priority:                  priority,
		Preemptable:               preemptable,
	}
}

func setAgentWorkerSlotGeneration(slot *model.AgentWorkerSlotState) error {
	if slot == nil {
		return fmt.Errorf("agent worker slot is unavailable")
	}
	copy := *slot
	copy.Generation = ""
	copy.CreatedAt = time.Time{}
	copy.UpdatedAt = time.Time{}
	data, err := json.Marshal(copy)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	slot.Generation = fmt.Sprintf("%x", sum[:])
	return nil
}

// agentWorkerRuntime picks the container runtime for an agent worker slot.
// Marketplace machines prefer gVisor, but GPU families without gVisor CUDA
// support fall back to runc and are advertised as such.
func agentWorkerRuntime(agentState *model.AgentTokenState, worker *types.Worker) string {
	if worker != nil && worker.Runtime != "" {
		return worker.Runtime
	}
	if agentState != nil && agentState.Mode == string(types.PoolModeMarketplace) {
		return types.MarketplaceContainerRuntimeForGPU(agentWorkerGPU(agentState, worker))
	}
	return types.ContainerRuntimeRunc.String()
}

func agentWorkerGPU(agentState *model.AgentTokenState, worker *types.Worker) string {
	if worker != nil && worker.Gpu != "" {
		return worker.Gpu
	}
	if agentState != nil && len(agentState.GPUs) > 0 {
		return agentState.GPUs[0]
	}
	return ""
}

func agentWorkerImage(config types.AppConfig) string {
	return agentWorkerImageWithTag(config, config.Worker.ImageTag)
}

func agentWorkerImageWithTag(config types.AppConfig, tag string) string {
	image := strings.TrimSuffix(config.Worker.ImageRegistry, "/")
	if image != "" {
		image += "/"
	}
	image += config.Worker.ImageName
	if tag != "" {
		image += ":" + tag
	}
	return image
}

func agentWorkerSlotToProto(slot *model.AgentWorkerSlotState, workerToken string) *pb.AgentWorkerSlot {
	return &pb.AgentWorkerSlot{
		Generation:                slot.Generation,
		WorkerId:                  slot.WorkerID,
		WorkerToken:               workerToken,
		PoolName:                  slot.PoolName,
		MachineId:                 slot.MachineID,
		Mode:                      slot.Mode,
		ContainerRuntime:          slot.ContainerRuntime,
		GvisorPlatform:            slot.ContainerRuntimeConfig.GVisorPlatform,
		GvisorRoot:                slot.ContainerRuntimeConfig.GVisorRoot,
		GvisorExtraArgs:           append([]string(nil), slot.ContainerRuntimeConfig.GVisorExtraArgs...),
		MarketplaceListingId:      slot.MarketplaceListingID,
		SellerWorkspaceId:         slot.SellerWorkspaceID,
		Cpu:                       slot.CPU,
		Memory:                    slot.Memory,
		Gpu:                       slot.GPU,
		GpuCount:                  slot.GPUCount,
		GpuAssignment:             slot.GPUAssignment,
		NetworkPrefix:             slot.NetworkPrefix,
		WorkerImage:               slot.WorkerImage,
		NetworkSlotPoolSize:       slot.NetworkSlotPoolSize,
		ContainerStartConcurrency: slot.ContainerStartConcurrency,
		CpuAffinityEnforced:       slot.CPUAffinityEnforced != nil && *slot.CPUAffinityEnforced,
		RequiresPoolSelector:      slot.RequiresPoolSelector,
		Priority:                  slot.Priority,
		PrioritySet:               true,
		Preemptable:               slot.Preemptable,
	}
}

func (s *Service) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	agentState, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: errMsg}, nil
	}

	route, err := s.containerRepo.GetBackendRoute(ctx, in.RouteId)
	if err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if !agentCanManageRoute(agentState, *route) {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: "route does not belong to this agent"}, nil
	}

	previousState := route.State
	previousProxyTarget := route.ProxyTarget
	previousError := route.Error
	route.State = firstNonEmpty(in.State, route.State)
	route.ProxyTarget = firstNonEmpty(in.ProxyTarget, route.ProxyTarget)
	route.Error = in.Error
	route.UpdatedAt = time.Now().Unix()
	if err := s.containerRepo.SetBackendRoute(ctx, *route); err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if route.State == types.BackendRouteStateReady &&
		(previousState != types.BackendRouteStateReady || previousProxyTarget != route.ProxyTarget) {
		s.prewarmRoute(*route, agentState)
	}
	if previousState != route.State || previousProxyTarget != route.ProxyTarget || previousError != route.Error {
		attrs := map[string]string{
			"kind":     route.Kind,
			"port":     fmt.Sprintf("%d", route.Port),
			"protocol": route.Protocol,
		}
		for key, value := range in.Attrs {
			if strings.TrimSpace(key) != "" && value != "" {
				attrs[key] = value
			}
		}
		s.emitComputeEvent(types.EventComputeRoute, types.EventComputeSchema{
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			WorkerID:    route.WorkerID,
			ContainerID: route.ContainerID,
			RouteID:     route.RouteID,
			Action:      types.EventComputeActionRouteStatusUpdated,
			Status:      route.State,
			Transport:   route.Transport,
			Message:     route.Error,
			Attrs:       attrs,
		})
	}
	return &pb.UpdateAgentRouteStatusResponse{Ok: true}, nil
}

func (s *Service) UpdateAgentAvailability(ctx context.Context, in *pb.UpdateAgentAvailabilityRequest) (*pb.UpdateAgentAvailabilityResponse, error) {
	// requireAgentState already resolves to the current machine state.
	current, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return &pb.UpdateAgentAvailabilityResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	if in.Schedulable && model.AgentPreflightFailed(current.Preflight) {
		return &pb.UpdateAgentAvailabilityResponse{Ok: false, ErrMsg: "machine has failed preflight checks"}, nil
	}

	observedAt := time.Now().UTC()
	if in.ObservedAtUnixNano > 0 {
		observedAt = time.Unix(0, in.ObservedAtUnixNano).UTC()
	}
	current.Schedulable = in.Schedulable
	current.AvailabilityReason = strings.TrimSpace(in.Reason)
	current.AvailabilityUpdatedAt = observedAt
	if current.Schedulable {
		current.LastDisconnectAt = time.Time{}
		current.LastHeartbeatAt = time.Now().UTC()
	}
	if err := s.saveComputeAgentTokenState(ctx, current); err != nil {
		return &pb.UpdateAgentAvailabilityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if !current.Schedulable {
		s.disableMachineWorker(ctx, current, reconcileReasonExternalAvailability)
	} else if s.scheduler != nil {
		if err := s.ensureAgentMachine(ctx, current, nil); err != nil {
			current.Schedulable = false
			rollbackErr := s.saveComputeAgentTokenState(ctx, current)
			s.disableMachineWorker(ctx, current, reconcileReasonExternalAvailability)
			return &pb.UpdateAgentAvailabilityResponse{Ok: false, ErrMsg: errors.Join(err, rollbackErr).Error()}, nil
		}
	}

	status := types.AgentMachineStatusUnschedulable
	if current.Schedulable {
		status = types.AgentMachineStatusSchedulable
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   observedAt,
		WorkspaceID: current.WorkspaceID,
		PoolName:    current.PoolName,
		MachineID:   current.MachineID,
		Action:      types.EventComputeActionMachineAvailabilityUpdated,
		Status:      status,
		Schedulable: boolPtr(current.Schedulable),
		Message:     "agent availability updated: " + firstNonEmpty(current.AvailabilityReason, "unspecified"),
	})

	return &pb.UpdateAgentAvailabilityResponse{Ok: true}, nil
}

func (s *Service) agentBootstrapConfig(ctx context.Context, workspaceID string, poolState *model.PoolState) (*pb.AgentBootstrapConfig, error) {
	config := normalizePoolConfig(poolState.Config)
	telemetryConfig, err := s.agentTelemetryConfig(ctx, workspaceID, poolState.ManagementSource != "")
	if err != nil {
		return nil, err
	}
	return &pb.AgentBootstrapConfig{
		GatewayHttpUrl:         s.appConfig.GatewayService.HTTP.GetExternalURL(),
		GatewayGrpcHost:        s.appConfig.GatewayService.GRPC.ExternalHost,
		GatewayGrpcPort:        int32(s.appConfig.GatewayService.GRPC.ExternalPort),
		GatewayGrpcTls:         s.appConfig.GatewayService.GRPC.TLS,
		WorkspaceId:            workspaceID,
		PoolName:               poolState.Name,
		Transport:              config.Transport,
		Executor:               defaultPrivateExecutor,
		Fallback:               config.Fallback,
		ImageRegistryStore:     s.appConfig.ImageService.RegistryStore,
		ImageClipVersion:       s.appConfig.ImageService.ClipVersion,
		ImageLocalCacheEnabled: s.appConfig.ImageService.LocalCacheEnabled,
		Telemetry:              telemetryConfig,
		Billing:                s.agentBillingConfig(poolState),
		DisabledServices: []string{
			"redis",
			"postgres",
			"juicefs",
			"fluent-bit",
			"alluxio",
			"configman",
			"k3s",
		},
	}, nil
}

// agentBillingConfig ships the marketplace usage endpoint (and container cost
// hook) to workers on seller machines so they can meter buyer usage. Private
// pools never receive billing credentials.
func (s *Service) agentBillingConfig(poolState *model.PoolState) *pb.AgentBillingConfig {
	if poolState == nil || poolState.Mode != string(types.PoolModeMarketplace) {
		return nil
	}
	billing := s.appConfig.ManagedCompute.Billing
	if strings.TrimSpace(billing.Endpoint) == "" {
		return nil
	}
	return &pb.AgentBillingConfig{
		UsageEndpoint:     billing.Endpoint,
		UsageToken:        billing.AuthToken,
		CostHookEndpoint:  s.appConfig.Monitoring.ContainerCostHookConfig.Endpoint,
		CostHookToken:     s.appConfig.Monitoring.ContainerCostHookConfig.Token,
		BillableMarginPct: s.appConfig.ManagedCompute.BillableMarginPctOrDefault(),
	}
}

func (s *Service) validateAgentTransportConfig(transport string) error {
	transport = strings.ReplaceAll(firstNonEmpty(transport, types.BackendRouteTransportTSNet), "-", "_")
	switch transport {
	case types.BackendRouteTransportTSNet:
		if !s.appConfig.Tailscale.Enabled {
			return fmt.Errorf("tailscale is not enabled")
		}
		if s.appConfig.Tailscale.AuthKey == "" {
			return fmt.Errorf("gateway tailscale auth key is not configured")
		}
		if s.appConfig.Tailscale.AgentAuthKey == "" {
			return fmt.Errorf("agent tailscale auth key is not configured")
		}
		return nil
	default:
		return fmt.Errorf("unsupported agent transport %q", transport)
	}
}

func preflightChecksFromProto(in []*pb.AgentPreflightCheck) []model.PreflightCheckState {
	checks := make([]model.PreflightCheckState, 0, len(in))
	for _, check := range in {
		if check == nil {
			continue
		}
		checks = append(checks, model.PreflightCheckState{
			Name:     check.Name,
			OK:       check.Ok,
			Message:  check.Message,
			Severity: check.Severity,
		})
	}
	return checks
}

func boolPtr(value bool) *bool {
	return &value
}

func preflightSummary(checks []model.PreflightCheckState) string {
	failed := make([]string, 0)
	for _, check := range checks {
		if check.Severity == types.AgentPreflightSeverityError && !check.OK {
			failed = append(failed, check.Name)
		}
	}
	if len(failed) == 0 {
		return "all required preflight checks passed"
	}
	return "failed preflight checks: " + strings.Join(failed, ", ")
}
