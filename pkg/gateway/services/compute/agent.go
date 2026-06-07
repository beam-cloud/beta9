package compute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	if tokenState == nil || tokenState.Revoked || time.Now().After(tokenState.ExpiresAt) {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	poolState, err := s.getPrivatePoolState(ctx, tokenState.WorkspaceID, tokenState.PoolName)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if poolState == nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if poolState.CreatedByTokenID == "" || poolState.CreatedByTokenID != tokenState.CreatedByTokenID {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	agentToken, err := generateComputeToken()
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	machineID := computeMachineID(tokenState.WorkspaceID, tokenState.PoolName, in.MachineFingerprint)
	now := time.Now()
	agentState := &model.AgentTokenState{
		TokenHash:                 hashComputeToken(agentToken),
		WorkspaceID:               tokenState.WorkspaceID,
		PoolName:                  tokenState.PoolName,
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
		Schedulable:               in.Schedulable,
		Preflight:                 preflightChecksFromProto(in.Preflight),
		CreatedAt:                 now,
		LastJoinAt:                now,
		LastHeartbeatAt:           now,
	}
	if err := s.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if s.scheduler != nil {
		if err := s.scheduler.RegisterAgentPool(tokenState.WorkspaceID, poolState); err != nil {
			return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineID:   machineID,
		Action:      types.EventComputeActionMachineJoined,
		Status:      computeMachineStatus(agentState),
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

	bootstrap := s.agentBootstrapConfig(tokenState.WorkspaceID, poolState)
	bootstrap.Executor = agentState.Executor

	return &pb.JoinAgentResponse{
		Ok:          true,
		WorkspaceId: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineId:   machineID,
		AgentToken:  agentToken,
		Bootstrap:   bootstrap,
	}, nil
}

func (s *Service) RequestAgentTransportCredential(ctx context.Context, in *pb.RequestAgentTransportCredentialRequest) (*pb.RequestAgentTransportCredentialResponse, error) {
	agentState, err := s.getCurrentComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	transport := firstNonEmpty(in.Transport, types.BackendRouteTransportTSNet)
	transport = strings.ReplaceAll(transport, "-", "_")
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
		Hostname:   "beam-agent-" + agentState.MachineID,
		Ephemeral:  true,
	}, nil
}

func (s *Service) ListAgentRoutes(ctx context.Context, in *pb.ListAgentRoutesRequest) (*pb.ListAgentRoutesResponse, error) {
	agentState, err := s.getCurrentComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	out, err := s.agentRoutesForMachine(ctx, agentState)
	if err != nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.ListAgentRoutesResponse{Ok: true, Routes: out}, nil
}

func (s *Service) StreamAgent(in *pb.StreamAgentRequest, stream pb.GatewayService_StreamAgentServer) error {
	ctx := stream.Context()
	agentState, err := s.getCurrentComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
	}
	if agentState == nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: "invalid agent token"})
	}

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
		if s.scheduler != nil {
			if poolState, err := s.getPrivatePoolState(ctx, agentState.WorkspaceID, agentState.PoolName); err == nil && poolState != nil {
				if err := s.scheduler.RegisterAgentPool(agentState.WorkspaceID, poolState); err != nil {
					return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
				}
			}
		}
		routes, err := s.agentRoutesForMachine(ctx, agentState)
		if err != nil {
			return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
		}
		slots, err := s.agentSlotsForMachine(ctx, agentState)
		if err != nil {
			return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
		}
		return stream.Send(&pb.StreamAgentResponse{Ok: true, Routes: routes, Slots: slots})
	}

	if err := sendSnapshot(); err != nil {
		return err
	}

	events := make(chan common.KeyEvent, 32)
	if s.keyEventManager != nil {
		routeRevisionKey := common.RedisKeys.SchedulerBackendRouteMachineRevision(agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
		if err := s.keyEventManager.ListenForPublishedPattern(ctx, routeRevisionKey, events); err != nil {
			return err
		}
	}

	ticker := time.NewTicker(agentStreamRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-events:
			coalesceAgentStreamEvents(ctx, events)
			if err := sendSnapshot(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := sendSnapshot(); err != nil {
				return err
			}
		}
	}
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
	routes, err := s.containerRepo.ListBackendRoutesByMachine(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return nil, err
	}

	out := make([]*pb.AgentRoute, 0, len(routes))
	for _, route := range routes {
		if route.State == types.BackendRouteStateClosing {
			continue
		}
		out = append(out, agentRouteToProto(route))
	}
	return out, nil
}

func (s *Service) agentSlotsForMachine(ctx context.Context, agentState *model.AgentTokenState) ([]*pb.AgentWorkerSlot, error) {
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

	slot, workerToken, err := s.ensureAgentWorkerSlot(ctx, agentState, worker, slots)
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
	if worker.MachineId != agentState.MachineID || worker.PoolName != agentState.PoolName || worker.Status == types.WorkerStatusDisabled {
		return nil, nil
	}
	return worker, nil
}

func (s *Service) ensureAgentWorkerSlot(ctx context.Context, agentState *model.AgentTokenState, worker *types.Worker, slots []*model.AgentWorkerSlotState) (*model.AgentWorkerSlotState, string, error) {
	var existing *model.AgentWorkerSlotState
	for _, slot := range slots {
		if slot != nil && slot.WorkerID == worker.Id {
			existing = slot
			break
		}
	}

	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, agentState.WorkspaceID)
	if err != nil {
		return nil, "", err
	}
	token, tokenID, tokenHash, err := s.agentWorkerToken(ctx, workspace.Id, existing)
	if err != nil {
		return nil, "", err
	}

	slot := agentWorkerSlotState(s.appConfig, agentState, worker, tokenID, tokenHash)
	if existing != nil {
		slot.CreatedAt = existing.CreatedAt
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

func (s *Service) agentWorkerToken(ctx context.Context, workspaceID uint, existing *model.AgentWorkerSlotState) (string, string, string, error) {
	if existing != nil && existing.WorkerTokenID != "" {
		token, err := s.backendRepo.GetTokenByExternalId(ctx, workspaceID, existing.WorkerTokenID)
		if err == nil && token != nil && token.Active && !token.DisabledByClusterAdmin && token.TokenType == types.TokenTypeWorker {
			tokenHash := hashComputeToken(token.Key)
			if existing.WorkerTokenHash == "" || existing.WorkerTokenHash == tokenHash {
				return token.Key, token.ExternalId, tokenHash, nil
			}
		}
	}

	createdToken, err := s.backendRepo.CreateToken(ctx, workspaceID, types.TokenTypeWorker, true)
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

func agentWorkerSlotState(config types.AppConfig, agentState *model.AgentTokenState, worker *types.Worker, tokenID, tokenHash string) *model.AgentWorkerSlotState {
	return &model.AgentWorkerSlotState{
		WorkerID:                  worker.Id,
		WorkerTokenID:             tokenID,
		WorkerTokenHash:           tokenHash,
		WorkspaceID:               agentState.WorkspaceID,
		PoolName:                  agentState.PoolName,
		MachineID:                 agentState.MachineID,
		CPU:                       worker.TotalCpu,
		Memory:                    worker.TotalMemory,
		GPU:                       worker.Gpu,
		GPUCount:                  worker.TotalGpuCount,
		GPUAssignment:             strings.Join(agentState.GPUIDs, ","),
		NetworkPrefix:             common.WorkerNetworkPrefix(config.ClusterName, agentState.MachineID),
		WorkerImage:               agentWorkerImage(config),
		NetworkSlotPoolSize:       agentState.NetworkSlotPoolSize,
		ContainerStartConcurrency: agentState.ContainerStartConcurrency,
	}
}

func agentWorkerImage(config types.AppConfig) string {
	image := strings.TrimSuffix(config.Worker.ImageRegistry, "/")
	if image != "" {
		image += "/"
	}
	image += config.Worker.ImageName
	if config.Worker.ImageTag != "" {
		image += ":" + config.Worker.ImageTag
	}
	return image
}

func agentWorkerSlotToProto(slot *model.AgentWorkerSlotState, workerToken string) *pb.AgentWorkerSlot {
	return &pb.AgentWorkerSlot{
		WorkerId:                  slot.WorkerID,
		WorkerToken:               workerToken,
		PoolName:                  slot.PoolName,
		MachineId:                 slot.MachineID,
		Cpu:                       slot.CPU,
		Memory:                    slot.Memory,
		Gpu:                       slot.GPU,
		GpuCount:                  slot.GPUCount,
		GpuAssignment:             slot.GPUAssignment,
		NetworkPrefix:             slot.NetworkPrefix,
		WorkerImage:               slot.WorkerImage,
		NetworkSlotPoolSize:       slot.NetworkSlotPoolSize,
		ContainerStartConcurrency: slot.ContainerStartConcurrency,
	}
}

func (s *Service) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	agentState, err := s.getCurrentComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	route, err := s.containerRepo.GetBackendRoute(ctx, in.RouteId)
	if err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if route.WorkspaceID != agentState.WorkspaceID || route.PoolName != agentState.PoolName || route.MachineID != agentState.MachineID {
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
	if previousState != route.State || previousProxyTarget != route.ProxyTarget || previousError != route.Error {
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
			Attrs: map[string]string{
				"kind":     route.Kind,
				"port":     fmt.Sprintf("%d", route.Port),
				"protocol": route.Protocol,
			},
		})
	}
	return &pb.UpdateAgentRouteStatusResponse{Ok: true}, nil
}

func (s *Service) agentBootstrapConfig(workspaceID string, poolState *model.PoolState) *pb.AgentBootstrapConfig {
	config := normalizePoolConfig(poolState.Config)
	return &pb.AgentBootstrapConfig{
		GatewayHttpUrl:  s.appConfig.GatewayService.HTTP.GetExternalURL(),
		GatewayGrpcHost: s.appConfig.GatewayService.GRPC.ExternalHost,
		GatewayGrpcPort: int32(s.appConfig.GatewayService.GRPC.ExternalPort),
		GatewayGrpcTls:  s.appConfig.GatewayService.GRPC.TLS,
		WorkspaceId:     workspaceID,
		PoolName:        poolState.Name,
		Transport:       config.Transport,
		Executor:        defaultPrivateExecutor,
		Fallback:        config.Fallback,
		DisabledServices: []string{
			"redis",
			"postgres",
			"juicefs",
			"fluent-bit",
			"alluxio",
			"configman",
			"k3s",
		},
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

func computeMachineID(workspaceID, poolName, fingerprint string) string {
	seed := fingerprint
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + poolName + "\x00" + seed))
	return "machine-" + hex.EncodeToString(sum[:])[:20]
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

func computeMachineStatus(state *model.AgentTokenState) string {
	return model.AgentMachineStatus(state, time.Now())
}

func boolPtr(value bool) *bool {
	return &value
}

func preflightSummary(checks []model.PreflightCheckState) string {
	failed := make([]string, 0)
	for _, check := range checks {
		if check.Severity == "error" && !check.OK {
			failed = append(failed, check.Name)
		}
	}
	if len(failed) == 0 {
		return "all required preflight checks passed"
	}
	return "failed preflight checks: " + strings.Join(failed, ", ")
}
