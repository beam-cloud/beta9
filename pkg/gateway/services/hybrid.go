package gatewayservices

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/hybrid/shadeform"
	"github.com/beam-cloud/beta9/pkg/hybrid/solver"
	"github.com/beam-cloud/beta9/pkg/hybrid/vast"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultHybridTransport = "tsnet_restricted"
	defaultHybridFallback  = "internal"
	defaultHybridPriority  = int32(1000)
	defaultHybridExecutor  = "worker-container"
	defaultHybridJoinTTL   = 30 * time.Minute
)

func (gws *GatewayService) ListHybridOffers(ctx context.Context, in *pb.ListHybridOffersRequest) (*pb.ListHybridOffersResponse, error) {
	pool, err := hybridPoolFromProto(normalizeHybridPoolConfig(in.Pool), false)
	if err != nil {
		return &pb.ListHybridOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	offers, err := gws.collectHybridOffers(ctx, pool)
	if err != nil {
		return &pb.ListHybridOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	sort.SliceStable(offers, func(i, j int) bool {
		return offers[i].CostPerGPU() < offers[j].CostPerGPU()
	})

	out := make([]*pb.HybridOffer, 0, len(offers))
	for _, offer := range offers {
		out = append(out, hybridOfferToProto(offer))
	}
	return &pb.ListHybridOffersResponse{Ok: true, Offers: out}, nil
}

func (gws *GatewayService) ReserveHybridPool(ctx context.Context, in *pb.ReserveHybridPoolRequest) (*pb.ReserveHybridPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := hybridWorkspaceID(authInfo)
	ownerTokenID := hybridOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	config := normalizeHybridPoolConfig(in.Pool)
	pool, err := hybridPoolFromProto(config, true)
	if err != nil {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if pool.Name == "" {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	if pool.Selector == "" {
		pool.Selector = pool.Name
	}

	offers, err := gws.collectHybridOffers(ctx, pool)
	if err != nil {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := gws.getHybridPoolState(ctx, workspaceID, pool.Name)
	if err != nil {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !hybridPoolCreatedByAuth(existing, authInfo) {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	reservations := []hybrid.Reservation{}
	if existing != nil {
		reservations = existing.Reservations
	}

	plan := solver.New().Solve(hybrid.SolveInput{
		Demand: hybrid.Demand{
			PoolName:       pool.Name,
			Selector:       pool.Selector,
			GPUs:           pool.GPUs,
			TotalGPUs:      pool.TotalGPUs,
			TTL:            pool.TTL,
			MaxSpendMicros: pool.MaxSpendMicros,
			Providers:      pool.Providers,
			Regions:        pool.Regions,
			MinReliability: pool.MinReliability,
		},
		Offers:       offers,
		Reservations: reservations,
		Now:          time.Now(),
	})
	if !plan.Feasible {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: plan.Reason}, nil
	}

	vendors := gws.hybridVendors()
	newReservations := append([]hybrid.Reservation{}, reservations...)
	for _, action := range plan.Actions {
		if action.Type != hybrid.ActionCreate {
			continue
		}
		vendor := vendors[action.Offer.Provider]
		if vendor == nil {
			return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: fmt.Sprintf("vendor %q is not configured", action.Offer.Provider)}, nil
		}
		for i := uint32(0); i < action.Count; i++ {
			reservation, err := vendor.CreateReservation(ctx, hybrid.ReservationRequest{
				PoolName:       pool.Name,
				Selector:       pool.Selector,
				Offer:          action.Offer,
				Count:          1,
				TTL:            pool.TTL,
				MaxSpendMicros: pool.MaxSpendMicros,
				Source:         hybrid.SourceCLIReservation,
			})
			if err != nil {
				return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
			}
			newReservations = append(newReservations, *reservation)
		}
	}

	now := time.Now()
	state := &hybrid.PoolState{
		Name:                 pool.Name,
		Selector:             pool.Selector,
		Config:               config,
		Reservations:         newReservations,
		ReservedGPUs:         plan.TotalGPUs,
		CommittedSpendMicros: plan.CommittedCostMicros,
		Status:               "active",
		Source:               hybrid.SourceCLIReservation,
		Mode:                 config.Mode,
		Transport:            config.Transport,
		Fallback:             config.Fallback,
		Priority:             config.Priority,
		CreatedByTokenID:     ownerTokenID,
		CreatedAt:            now,
		UpdatedAt:            now,
		ExpiresAt:            now.Add(pool.TTL),
	}
	if existing != nil && !existing.CreatedAt.IsZero() {
		state.CreatedAt = existing.CreatedAt
		state.CreatedByTokenID = existing.CreatedByTokenID
	}
	if err := gws.saveHybridPoolState(ctx, workspaceID, state); err != nil {
		return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return &pb.ReserveHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitHybridEvent(types.EventHybridPool, hybridPoolEvent(workspaceID, state, types.EventHybridActionPoolReserved, ""))

	return &pb.ReserveHybridPoolResponse{Ok: true, Pool: hybridPoolStateToProto(state)}, nil
}

func (gws *GatewayService) ListHybridPools(ctx context.Context, in *pb.ListHybridPoolsRequest) (*pb.ListHybridPoolsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := hybridWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListHybridPoolsResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	states, err := gws.listHybridPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return &pb.ListHybridPoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	states = filterHybridPoolsCreatedByAuth(states, authInfo)
	if limit := int(in.Limit); limit > 0 && len(states) > limit {
		states = states[:limit]
	}
	out := make([]*pb.HybridPool, 0, len(states))
	for _, state := range states {
		out = append(out, hybridPoolStateToProto(state))
	}
	return &pb.ListHybridPoolsResponse{Ok: true, Pools: out}, nil
}

func (gws *GatewayService) UpsertHybridPool(ctx context.Context, in *pb.UpsertHybridPoolRequest) (*pb.UpsertHybridPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := hybridWorkspaceID(authInfo)
	ownerTokenID := hybridOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	if in.Pool == nil {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: "pool config is required"}, nil
	}
	config := normalizeHybridPoolConfig(in.Pool)
	if config.Name == "" {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	if err := hybrid.ValidatePoolName(config.Name); err != nil {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if _, err := hybridPoolFromProto(config, false); err != nil {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := gws.getHybridPoolState(ctx, workspaceID, config.Name)
	if err != nil {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !hybridPoolCreatedByAuth(existing, authInfo) {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	now := time.Now()
	state := &hybrid.PoolState{
		Name:             config.Name,
		Selector:         config.Selector,
		Config:           config,
		Status:           "active",
		Source:           hybrid.SourceManual,
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
	if err := gws.saveHybridPoolState(ctx, workspaceID, state); err != nil {
		return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return &pb.UpsertHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitHybridEvent(types.EventHybridPool, hybridPoolEvent(workspaceID, state, types.EventHybridActionPoolUpserted, ""))
	return &pb.UpsertHybridPoolResponse{Ok: true, Pool: hybridPoolStateToProto(state)}, nil
}

func (gws *GatewayService) DeleteHybridPool(ctx context.Context, in *pb.DeleteHybridPoolRequest) (*pb.DeleteHybridPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := hybridWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeleteHybridPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := gws.getOwnedHybridPoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.DeleteHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.DeleteHybridPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	vendors := gws.hybridVendors()
	for _, reservation := range state.Reservations {
		if reservation.Source == hybrid.SourceManual || reservation.Source == hybrid.SourceAutosolver {
			continue
		}
		vendor := vendors[reservation.Provider]
		if vendor == nil {
			continue
		}
		instanceID := reservation.InstanceID
		if instanceID == "" {
			instanceID = reservation.ID
		}
		_ = vendor.DeleteReservation(ctx, instanceID)
	}

	if err := gws.deleteHybridPoolState(ctx, workspaceID, in.Name); err != nil {
		return &pb.DeleteHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		gws.scheduler.DeleteAgentPool(firstNonEmpty(state.Selector, state.Name))
	}
	gws.emitHybridEvent(types.EventHybridPool, hybridPoolEvent(workspaceID, state, types.EventHybridActionPoolDeleted, "deleted"))
	return &pb.DeleteHybridPoolResponse{Ok: true}, nil
}

func (gws *GatewayService) ExtendHybridPool(ctx context.Context, in *pb.ExtendHybridPoolRequest) (*pb.ExtendHybridPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := hybridWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ExtendHybridPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := gws.getOwnedHybridPoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.ExtendHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.ExtendHybridPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if in.Ttl != "" {
		state.Config.Ttl = in.Ttl
		ttl, err := hybrid.ParseTTL(in.Ttl)
		if err != nil {
			return &pb.ExtendHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		state.ExpiresAt = time.Now().Add(ttl)
	}
	if in.MaxSpend > 0 {
		state.Config.MaxSpend = in.MaxSpend
	}
	state.UpdatedAt = time.Now()
	if err := gws.saveHybridPoolState(ctx, workspaceID, state); err != nil {
		return &pb.ExtendHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitHybridEvent(types.EventHybridPool, hybridPoolEvent(workspaceID, state, types.EventHybridActionPoolExtended, ""))
	return &pb.ExtendHybridPoolResponse{Ok: true, Pool: hybridPoolStateToProto(state)}, nil
}

func (gws *GatewayService) AttachHybridPool(ctx context.Context, in *pb.AttachHybridPoolRequest) (*pb.AttachHybridPoolResponse, error) {
	if in.Name == "" {
		return &pb.AttachHybridPoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	command, _, _, err := gws.createHybridPoolJoinCommand(ctx, in.Name, "")
	if err != nil {
		return &pb.AttachHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.AttachHybridPoolResponse{
		Ok:      true,
		Command: command,
	}, nil
}

func (gws *GatewayService) CreateHybridPoolJoinToken(ctx context.Context, in *pb.CreateHybridPoolJoinTokenRequest) (*pb.CreateHybridPoolJoinTokenResponse, error) {
	token, expiresAt, err := gws.createHybridPoolJoinToken(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.CreateHybridPoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateHybridPoolJoinTokenResponse{
		Ok:        true,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (gws *GatewayService) RevokeHybridPoolJoinToken(ctx context.Context, in *pb.RevokeHybridPoolJoinTokenRequest) (*pb.RevokeHybridPoolJoinTokenResponse, error) {
	if in.Token == "" {
		return &pb.RevokeHybridPoolJoinTokenResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}
	state, err := gws.getHybridJoinTokenState(ctx, in.Token)
	if err != nil {
		return &pb.RevokeHybridPoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.RevokeHybridPoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if state.WorkspaceID != hybridWorkspaceID(authInfo) || state.CreatedByTokenID != hybridOwnerTokenID(authInfo) {
		return &pb.RevokeHybridPoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	state.Revoked = true
	if err := gws.saveHybridJoinTokenState(ctx, state, time.Until(state.ExpiresAt)); err != nil {
		return &pb.RevokeHybridPoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitHybridEvent(types.EventHybridJoinToken, types.EventHybridSchema{
		WorkspaceID: state.WorkspaceID,
		PoolName:    state.PoolName,
		Action:      types.EventHybridActionJoinTokenRevoked,
		Status:      "revoked",
	})
	return &pb.RevokeHybridPoolJoinTokenResponse{Ok: true}, nil
}

func (gws *GatewayService) GetHybridPoolJoinCommand(ctx context.Context, in *pb.GetHybridPoolJoinCommandRequest) (*pb.GetHybridPoolJoinCommandResponse, error) {
	command, token, expiresAt, err := gws.createHybridPoolJoinCommand(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.GetHybridPoolJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetHybridPoolJoinCommandResponse{
		Ok:        true,
		Command:   command,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (gws *GatewayService) JoinAgent(ctx context.Context, in *pb.JoinAgentRequest) (*pb.JoinAgentResponse, error) {
	if strings.TrimSpace(in.JoinToken) == "" {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}

	tokenState, err := gws.getHybridJoinTokenState(ctx, in.JoinToken)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if tokenState == nil || tokenState.Revoked || time.Now().After(tokenState.ExpiresAt) {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	poolState, err := gws.getHybridPoolState(ctx, tokenState.WorkspaceID, tokenState.PoolName)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if poolState == nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if poolState.CreatedByTokenID == "" || poolState.CreatedByTokenID != tokenState.CreatedByTokenID {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	agentToken, err := generateHybridToken()
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	machineID := hybridMachineID(tokenState.WorkspaceID, tokenState.PoolName, in.MachineFingerprint)
	now := time.Now()
	agentState := &hybrid.AgentTokenState{
		TokenHash:          hashHybridToken(agentToken),
		WorkspaceID:        tokenState.WorkspaceID,
		PoolName:           tokenState.PoolName,
		MachineID:          machineID,
		MachineFingerprint: in.MachineFingerprint,
		Hostname:           in.Hostname,
		OS:                 in.Os,
		Arch:               in.Arch,
		CPUCount:           in.CpuCount,
		MemoryMB:           in.MemoryMb,
		GPUs:               in.Gpu,
		GPUCount:           in.GpuCount,
		Executor:           firstNonEmpty(in.Executor, defaultHybridExecutor),
		Schedulable:        in.Schedulable,
		Preflight:          hybridPreflightChecksFromProto(in.Preflight),
		CreatedAt:          now,
		LastJoinAt:         now,
	}
	if err := gws.saveHybridAgentTokenState(ctx, agentState); err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(tokenState.WorkspaceID, poolState); err != nil {
			return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitHybridEvent(types.EventHybridMachine, types.EventHybridSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineID:   machineID,
		Action:      types.EventHybridActionMachineJoined,
		Status:      hybridMachineStatus(agentState),
		Transport:   normalizeHybridPoolConfig(poolState.Config).Transport,
		Executor:    agentState.Executor,
		Hostname:    agentState.Hostname,
		OS:          agentState.OS,
		Arch:        agentState.Arch,
		CPUCount:    agentState.CPUCount,
		MemoryMB:    agentState.MemoryMB,
		GPUCount:    agentState.GPUCount,
		GPUs:        agentState.GPUs,
		Schedulable: boolPtr(agentState.Schedulable),
		Message:     hybridPreflightSummary(agentState.Preflight),
	})

	bootstrap := gws.agentBootstrapConfig(tokenState.WorkspaceID, poolState)
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

func (gws *GatewayService) RequestAgentTransportCredential(ctx context.Context, in *pb.RequestAgentTransportCredentialRequest) (*pb.RequestAgentTransportCredentialResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	transport := firstNonEmpty(in.Transport, types.BackendRouteTransportTSNet)
	transport = strings.ReplaceAll(transport, "-", "_")
	if err := gws.validateHybridTransportConfig(transport); err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitHybridEvent(types.EventHybridTransport, types.EventHybridSchema{
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventHybridActionTransportCredentialVended,
		Status:      "ready",
		Transport:   transport,
	})

	return &pb.RequestAgentTransportCredentialResponse{
		Ok:         true,
		AuthKey:    gws.appConfig.Tailscale.HybridWorkerAuthKey,
		ControlUrl: gws.appConfig.Tailscale.ControlURL,
		Hostname:   "beam-agent-" + agentState.MachineID,
		Ephemeral:  true,
	}, nil
}

func (gws *GatewayService) ListAgentRoutes(ctx context.Context, in *pb.ListAgentRoutesRequest) (*pb.ListAgentRoutesResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	out, err := gws.agentRoutesForMachine(ctx, agentState)
	if err != nil {
		return &pb.ListAgentRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.ListAgentRoutesResponse{Ok: true, Routes: out}, nil
}

func (gws *GatewayService) StreamAgent(in *pb.StreamAgentRequest, stream pb.GatewayService_StreamAgentServer) error {
	ctx := stream.Context()
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
	}
	if agentState == nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: "invalid agent token"})
	}

	sendSnapshot := func() error {
		routes, err := gws.agentRoutesForMachine(ctx, agentState)
		if err != nil {
			return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
		}
		slots, err := gws.agentSlotsForMachine(ctx, agentState)
		if err != nil {
			return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
		}
		return stream.Send(&pb.StreamAgentResponse{Ok: true, Routes: routes, Slots: slots})
	}

	if err := sendSnapshot(); err != nil {
		return err
	}

	events := make(chan common.KeyEvent, 32)
	if gws.keyEventManager != nil {
		if err := gws.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerBackendRoute(""), events); err != nil {
			return err
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-events:
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

func (gws *GatewayService) agentRoutesForMachine(ctx context.Context, agentState *hybrid.AgentTokenState) ([]*pb.AgentRoute, error) {
	routes, err := gws.containerRepo.ListBackendRoutesByMachine(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
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

func (gws *GatewayService) agentSlotsForMachine(ctx context.Context, agentState *hybrid.AgentTokenState) ([]*pb.AgentWorkerSlot, error) {
	slots, err := gws.hybridRepo.ListAgentWorkerSlotStates(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.AgentWorkerSlot, 0, len(slots))
	for _, slot := range slots {
		if slot == nil || slot.WorkerID == "" {
			continue
		}
		if _, err := gws.workerRepo.GetWorkerById(slot.WorkerID); err != nil {
			notFoundErr := &types.ErrWorkerNotFound{}
			if notFoundErr.From(err) {
				if deleteErr := gws.hybridRepo.DeleteAgentWorkerSlotState(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID, slot.WorkerID); deleteErr != nil {
					return nil, deleteErr
				}
				gws.emitHybridEvent(types.EventHybridMachine, types.EventHybridSchema{
					WorkspaceID: slot.WorkspaceID,
					PoolName:    slot.PoolName,
					MachineID:   slot.MachineID,
					WorkerID:    slot.WorkerID,
					Action:      types.EventHybridActionWorkerSlotPruned,
					Status:      "stale",
					Message:     "removed stale agent worker slot because the scheduler worker no longer exists",
				})
				continue
			}
			return nil, err
		}
		out = append(out, agentWorkerSlotToProto(slot))
	}
	return out, nil
}

func agentWorkerSlotToProto(slot *hybrid.AgentWorkerSlotState) *pb.AgentWorkerSlot {
	return &pb.AgentWorkerSlot{
		WorkerId:      slot.WorkerID,
		WorkerToken:   slot.WorkerToken,
		PoolName:      slot.PoolName,
		MachineId:     slot.MachineID,
		Cpu:           slot.CPU,
		Memory:        slot.Memory,
		Gpu:           slot.GPU,
		GpuCount:      slot.GPUCount,
		GpuAssignment: slot.GPUAssignment,
		NetworkPrefix: slot.NetworkPrefix,
		WorkerImage:   slot.WorkerImage,
	}
}

func (gws *GatewayService) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	route, err := gws.containerRepo.GetBackendRoute(ctx, in.RouteId)
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
	if err := gws.containerRepo.SetBackendRoute(ctx, *route); err != nil {
		return &pb.UpdateAgentRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if previousState != route.State || previousProxyTarget != route.ProxyTarget || previousError != route.Error {
		gws.emitHybridEvent(types.EventHybridRoute, types.EventHybridSchema{
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			WorkerID:    route.WorkerID,
			ContainerID: route.ContainerID,
			RouteID:     route.RouteID,
			Action:      types.EventHybridActionRouteStatusUpdated,
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

func (gws *GatewayService) hybridVendors() map[string]hybrid.Vendor {
	vendors := map[string]hybrid.Vendor{}
	if gws.appConfig.Providers.Vast.ApiKey != "" {
		vendors["vast"] = vast.New(vast.Config{
			APIKey:  gws.appConfig.Providers.Vast.ApiKey,
			BaseURL: gws.appConfig.Providers.Vast.BaseURL,
		})
	}
	if gws.appConfig.Providers.Shadeform.ApiKey != "" {
		vendors["shadeform"] = shadeform.New(shadeform.Config{
			APIKey:  gws.appConfig.Providers.Shadeform.ApiKey,
			BaseURL: gws.appConfig.Providers.Shadeform.BaseURL,
		})
	}
	return vendors
}

func (gws *GatewayService) collectHybridOffers(ctx context.Context, pool hybrid.Pool) ([]hybrid.Offer, error) {
	vendors := gws.hybridVendors()
	if len(vendors) == 0 {
		return nil, fmt.Errorf("no hybrid GPU vendors are configured")
	}

	request := hybrid.OfferRequest{
		GPUs:           pool.GPUs,
		TotalGPUs:      pool.TotalGPUs,
		Providers:      pool.Providers,
		Regions:        pool.Regions,
		MinReliability: pool.MinReliability,
	}
	offers := []hybrid.Offer{}
	for name, vendor := range vendors {
		if len(pool.Providers) > 0 {
			found := false
			for _, provider := range pool.Providers {
				if provider == name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		vendorOffers, err := vendor.ListOffers(ctx, request)
		if err != nil {
			return nil, err
		}
		for _, offer := range vendorOffers {
			if pool.MatchesOffer(offer) {
				offers = append(offers, offer)
			}
		}
	}
	return offers, nil
}

func normalizeHybridPoolConfig(in *pb.HybridPoolConfig) *pb.HybridPoolConfig {
	if in == nil {
		return nil
	}
	out := *in
	out.Gpu = append([]string(nil), in.Gpu...)
	out.Providers = append([]string(nil), in.Providers...)
	out.Regions = append([]string(nil), in.Regions...)
	if out.Selector == "" {
		out.Selector = out.Name
	}
	if out.Mode == "" {
		out.Mode = "hybrid"
	}
	if out.Transport == "" {
		out.Transport = defaultHybridTransport
	}
	out.Transport = strings.ReplaceAll(out.Transport, "-", "_")
	if out.Fallback == "" {
		out.Fallback = defaultHybridFallback
	}
	if out.Priority == 0 {
		out.Priority = defaultHybridPriority
	}
	return &out
}

func hybridPoolFromProto(in *pb.HybridPoolConfig, requireReservation bool) (hybrid.Pool, error) {
	if in == nil {
		return hybrid.Pool{}, fmt.Errorf("pool config is required")
	}
	if in.Mode != "" && in.Mode != "hybrid" {
		return hybrid.Pool{}, fmt.Errorf("hybrid pool mode must be %q", "hybrid")
	}
	switch in.Transport {
	case "", defaultHybridTransport:
	default:
		return hybrid.Pool{}, fmt.Errorf("unsupported hybrid transport %q", in.Transport)
	}
	switch in.Fallback {
	case "", "internal", "wait", "fail":
	default:
		return hybrid.Pool{}, fmt.Errorf("unsupported hybrid fallback %q", in.Fallback)
	}
	ttl, err := hybrid.ParseTTL(in.Ttl)
	if err != nil {
		return hybrid.Pool{}, err
	}
	pool := hybrid.Pool{
		Name:           in.Name,
		Selector:       in.Selector,
		GPUs:           in.Gpu,
		TotalGPUs:      in.Gpus,
		TTL:            ttl,
		MaxSpendMicros: hybrid.DollarsToMicros(in.MaxSpend),
		Providers:      in.Providers,
		Regions:        in.Regions,
		MinReliability: in.MinReliability,
	}
	if requireReservation {
		if err := pool.Validate(); err != nil {
			return hybrid.Pool{}, err
		}
	} else if pool.MinReliability < 0 || pool.MinReliability > 1 {
		return hybrid.Pool{}, fmt.Errorf("min_reliability must be between 0 and 1")
	}
	return pool, nil
}

func hybridOfferToProto(offer hybrid.Offer) *pb.HybridOffer {
	return &pb.HybridOffer{
		Id:               offer.ID,
		Provider:         offer.Provider,
		InstanceType:     offer.InstanceType,
		Region:           offer.Region,
		Gpu:              offer.GPU,
		GpuCount:         offer.GPUCount,
		CpuMillicores:    offer.CPUMillicores,
		MemoryMb:         offer.MemoryMB,
		HourlyCostMicros: offer.HourlyCostMicros,
		Reliability:      offer.Reliability,
		Available:        offer.Available,
	}
}

func hybridReservationToProto(reservation hybrid.Reservation) *pb.HybridReservation {
	return &pb.HybridReservation{
		Id:               reservation.ID,
		PoolName:         reservation.PoolName,
		Provider:         reservation.Provider,
		OfferId:          reservation.OfferID,
		Status:           string(reservation.Status),
		GpuCount:         reservation.GPUCount,
		HourlyCostMicros: reservation.HourlyCostMicros,
		Source:           string(reservation.Source),
		CreatedAt:        timestampOrNil(reservation.CreatedAt),
		ExpiresAt:        timestampOrNil(reservation.ExpiresAt),
		BillingRenewalAt: timestampOrNil(reservation.BillingRenewalAt),
	}
}

func agentRouteToProto(route types.BackendRoute) *pb.AgentRoute {
	return &pb.AgentRoute{
		RouteId:     route.RouteID,
		WorkspaceId: route.WorkspaceID,
		PoolName:    route.PoolName,
		MachineId:   route.MachineID,
		WorkerId:    route.WorkerID,
		ContainerId: route.ContainerID,
		Kind:        route.Kind,
		Port:        route.Port,
		Protocol:    route.Protocol,
		Transport:   route.Transport,
		LocalTarget: route.LocalTarget,
		ProxyTarget: route.ProxyTarget,
		State:       route.State,
		Error:       route.Error,
		UpdatedAt:   route.UpdatedAt,
	}
}

func hybridPoolStateToProto(state *hybrid.PoolState) *pb.HybridPool {
	reservations := make([]*pb.HybridReservation, 0, len(state.Reservations))
	for _, reservation := range state.Reservations {
		reservations = append(reservations, hybridReservationToProto(reservation))
	}
	config := normalizeHybridPoolConfig(state.Config)
	return &pb.HybridPool{
		Name:                 state.Name,
		Selector:             state.Selector,
		Config:               config,
		Reservations:         reservations,
		ReservedGpus:         state.ReservedGPUs,
		CommittedSpendMicros: state.CommittedSpendMicros,
		Status:               state.Status,
		Source:               string(state.Source),
		CreatedAt:            timestampOrNil(state.CreatedAt),
		ExpiresAt:            timestampOrNil(state.ExpiresAt),
	}
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func (gws *GatewayService) createHybridPoolJoinCommand(ctx context.Context, poolName, ttlValue string) (string, string, time.Time, error) {
	gatewayURL := strings.TrimRight(gws.appConfig.GatewayService.HTTP.GetExternalURL(), "/")

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	devMode := isLocalGatewayURL(gatewayURL)
	if authInfo != nil && authInfo.Workspace != nil {
		if state, err := gws.getOwnedHybridPoolState(ctx, authInfo, poolName); err == nil && state != nil {
			config := normalizeHybridPoolConfig(state.Config)
			if err := gws.validateHybridTransportConfig(config.Transport); err != nil {
				return "", "", time.Time{}, err
			}
		}
	}

	token, expiresAt, err := gws.createHybridPoolJoinToken(ctx, poolName, ttlValue)
	if err != nil {
		return "", "", time.Time{}, err
	}

	command := fmt.Sprintf("curl -fsSL %s/install/agent | sudo bash -s -- --gateway %s --join-token %s", gatewayURL, gatewayURL, token)
	if devMode {
		command = fmt.Sprintf("curl -fsSL %s/install/agent | bash -s -- --gateway %s --join-token %s --dev", gatewayURL, gatewayURL, token)
	}
	return command, token, expiresAt, nil
}

func (gws *GatewayService) createHybridPoolJoinToken(ctx context.Context, poolName, ttlValue string) (string, time.Time, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", time.Time{}, fmt.Errorf("pool name is required")
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	ownerTokenID := hybridOwnerTokenID(authInfo)
	if ownerTokenID == "" {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := gws.getOwnedHybridPoolState(ctx, authInfo, poolName)
	if err != nil {
		return "", time.Time{}, err
	}
	if state == nil {
		return "", time.Time{}, fmt.Errorf("pool not found")
	}

	ttl, err := hybrid.ParseTTL(ttlValue)
	if err != nil {
		return "", time.Time{}, err
	}
	if ttl == 0 {
		ttl = defaultHybridJoinTTL
	}
	if ttl <= 0 {
		return "", time.Time{}, fmt.Errorf("join token ttl must be positive")
	}

	token, err := generateHybridToken()
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now()
	tokenState := &hybrid.JoinTokenState{
		TokenHash:        hashHybridToken(token),
		WorkspaceID:      authInfo.Workspace.ExternalId,
		PoolName:         poolName,
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		ExpiresAt:        now.Add(ttl),
	}
	if err := gws.saveHybridJoinTokenState(ctx, tokenState, ttl); err != nil {
		return "", time.Time{}, err
	}
	gws.emitHybridEvent(types.EventHybridJoinToken, types.EventHybridSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		Action:      types.EventHybridActionJoinTokenCreated,
		Status:      "active",
		Attrs: map[string]string{
			"expires_at":  tokenState.ExpiresAt.UTC().Format(time.RFC3339),
			"ttl_seconds": fmt.Sprintf("%.0f", ttl.Seconds()),
		},
	})
	return token, tokenState.ExpiresAt, nil
}

func (gws *GatewayService) agentBootstrapConfig(workspaceID string, poolState *hybrid.PoolState) *pb.AgentBootstrapConfig {
	config := normalizeHybridPoolConfig(poolState.Config)
	return &pb.AgentBootstrapConfig{
		GatewayHttpUrl:  gws.appConfig.GatewayService.HTTP.GetExternalURL(),
		GatewayGrpcHost: gws.appConfig.GatewayService.GRPC.ExternalHost,
		GatewayGrpcPort: int32(gws.appConfig.GatewayService.GRPC.ExternalPort),
		GatewayGrpcTls:  gws.appConfig.GatewayService.GRPC.TLS,
		WorkspaceId:     workspaceID,
		PoolName:        poolState.Name,
		Transport:       config.Transport,
		Executor:        defaultHybridExecutor,
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

func (gws *GatewayService) validateHybridTransportConfig(transport string) error {
	transport = strings.ReplaceAll(firstNonEmpty(transport, types.BackendRouteTransportTSNet), "-", "_")
	switch transport {
	case types.BackendRouteTransportTSNet:
		if !gws.appConfig.Tailscale.Enabled {
			return fmt.Errorf("tailscale is not enabled")
		}
		if gws.appConfig.Tailscale.AuthKey == "" {
			return fmt.Errorf("gateway tailscale auth key is not configured")
		}
		if gws.appConfig.Tailscale.HybridWorkerAuthKey == "" {
			return fmt.Errorf("agent tailscale auth key is not configured")
		}
		return nil
	default:
		return fmt.Errorf("unsupported hybrid transport %q", transport)
	}
}

func isLocalGatewayURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		host = strings.TrimSpace(rawURL)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func generateHybridToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func hashHybridToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func hybridMachineID(workspaceID, poolName, fingerprint string) string {
	seed := fingerprint
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + poolName + "\x00" + seed))
	return "machine-" + hex.EncodeToString(sum[:])[:20]
}

func hybridPreflightChecksFromProto(in []*pb.AgentPreflightCheck) []hybrid.PreflightCheckState {
	checks := make([]hybrid.PreflightCheckState, 0, len(in))
	for _, check := range in {
		if check == nil {
			continue
		}
		checks = append(checks, hybrid.PreflightCheckState{
			Name:     check.Name,
			OK:       check.Ok,
			Message:  check.Message,
			Severity: check.Severity,
		})
	}
	return checks
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (gws *GatewayService) saveHybridPoolState(ctx context.Context, workspaceID string, state *hybrid.PoolState) error {
	return gws.hybridRepo.SavePoolState(ctx, workspaceID, state)
}

func (gws *GatewayService) getHybridPoolState(ctx context.Context, workspaceID, name string) (*hybrid.PoolState, error) {
	return gws.hybridRepo.GetPoolState(ctx, workspaceID, name)
}

func (gws *GatewayService) listHybridPoolStates(ctx context.Context, workspaceID string, limit int) ([]*hybrid.PoolState, error) {
	return gws.hybridRepo.ListPoolStates(ctx, workspaceID, limit)
}

func (gws *GatewayService) deleteHybridPoolState(ctx context.Context, workspaceID, name string) error {
	return gws.hybridRepo.DeletePoolState(ctx, workspaceID, name)
}

func (gws *GatewayService) saveHybridJoinTokenState(ctx context.Context, state *hybrid.JoinTokenState, ttl time.Duration) error {
	return gws.hybridRepo.SaveJoinTokenState(ctx, state, ttl)
}

func (gws *GatewayService) getHybridJoinTokenState(ctx context.Context, token string) (*hybrid.JoinTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return gws.hybridRepo.GetJoinTokenState(ctx, hashHybridToken(token))
}

func (gws *GatewayService) saveHybridAgentTokenState(ctx context.Context, state *hybrid.AgentTokenState) error {
	return gws.hybridRepo.SaveAgentTokenState(ctx, state, 24*time.Hour)
}

func (gws *GatewayService) getHybridAgentTokenState(ctx context.Context, token string) (*hybrid.AgentTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return gws.hybridRepo.GetAgentTokenState(ctx, hashHybridToken(token))
}

func (gws *GatewayService) getOwnedHybridPoolState(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*hybrid.PoolState, error) {
	workspaceID := hybridWorkspaceID(authInfo)
	if workspaceID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	state, err := gws.getHybridPoolState(ctx, workspaceID, poolName)
	if err != nil {
		return nil, err
	}
	if state == nil || !hybridPoolCreatedByAuth(state, authInfo) {
		return nil, nil
	}
	return state, nil
}

func (gws *GatewayService) emitHybridEvent(eventType string, event types.EventHybridSchema) {
	if gws.eventRepo == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	gws.eventRepo.PushHybridEvent(eventType, event)
}

func hybridPoolEvent(workspaceID string, state *hybrid.PoolState, action, status string) types.EventHybridSchema {
	if state == nil {
		return types.EventHybridSchema{}
	}
	if status == "" {
		status = state.Status
	}
	event := types.EventHybridSchema{
		WorkspaceID: workspaceID,
		PoolName:    state.Name,
		Action:      action,
		Status:      status,
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		Source:      string(state.Source),
		GPUCount:    state.ReservedGPUs,
		Attrs: map[string]string{
			"selector":               state.Selector,
			"mode":                   state.Mode,
			"priority":               fmt.Sprintf("%d", state.Priority),
			"reservation_count":      fmt.Sprintf("%d", len(state.Reservations)),
			"committed_spend_micros": fmt.Sprintf("%d", state.CommittedSpendMicros),
		},
	}
	if state.Config != nil {
		event.Attrs["ttl"] = state.Config.Ttl
		event.Attrs["max_spend"] = fmt.Sprintf("%g", state.Config.MaxSpend)
	}
	if !state.ExpiresAt.IsZero() {
		event.Attrs["expires_at"] = state.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return event
}

func hybridMachineStatus(state *hybrid.AgentTokenState) string {
	if state != nil && state.Schedulable {
		return "schedulable"
	}
	return "preflight_failed"
}

func boolPtr(value bool) *bool {
	return &value
}

func hybridPreflightSummary(checks []hybrid.PreflightCheckState) string {
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

func filterHybridPoolsCreatedByAuth(states []*hybrid.PoolState, authInfo *auth.AuthInfo) []*hybrid.PoolState {
	if len(states) == 0 {
		return states
	}

	filtered := make([]*hybrid.PoolState, 0, len(states))
	for _, state := range states {
		if hybridPoolCreatedByAuth(state, authInfo) {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func hybridPoolCreatedByAuth(state *hybrid.PoolState, authInfo *auth.AuthInfo) bool {
	if state == nil || state.CreatedByTokenID == "" {
		return false
	}
	return state.CreatedByTokenID == hybridOwnerTokenID(authInfo)
}

func hybridOwnerTokenID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Token == nil {
		return ""
	}
	return authInfo.Token.ExternalId
}

func hybridWorkspaceID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Workspace == nil {
		return ""
	}
	return authInfo.Workspace.ExternalId
}
