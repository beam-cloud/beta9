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
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/compute/shadeform"
	"github.com/beam-cloud/beta9/pkg/compute/solver"
	"github.com/beam-cloud/beta9/pkg/compute/vast"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPrivateTransport = types.BackendRouteTransportTSNet
	defaultPrivateFallback  = types.PrivatePoolFallbackInternal
	defaultPrivatePriority  = int32(1000)
	defaultPrivateExecutor  = types.DefaultAgentWorkerContainerMode
	defaultPrivateJoinTTL   = 30 * time.Minute
	agentStreamRefresh      = 30 * time.Second
)

func (gws *GatewayService) ListPoolOffers(ctx context.Context, in *pb.ListPoolOffersRequest) (*pb.ListPoolOffersResponse, error) {
	pool, err := computePoolFromProto(normalizePoolConfig(in.Pool), false)
	if err != nil {
		return &pb.ListPoolOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	offers, err := gws.collectPoolOffers(ctx, pool)
	if err != nil {
		return &pb.ListPoolOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	sort.SliceStable(offers, func(i, j int) bool {
		return offers[i].CostPerGPU() < offers[j].CostPerGPU()
	})

	out := make([]*pb.PoolOffer, 0, len(offers))
	for _, offer := range offers {
		out = append(out, poolOfferToProto(offer))
	}
	return &pb.ListPoolOffersResponse{Ok: true, Offers: out}, nil
}

func (gws *GatewayService) LaunchPoolCapacity(ctx context.Context, in *pb.LaunchPoolCapacityRequest) (*pb.LaunchPoolCapacityResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	config := normalizePoolConfig(in.Pool)
	pool, err := computePoolFromProto(config, true)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if pool.Name == "" {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	if pool.Selector == "" {
		pool.Selector = pool.Name
	}

	offers, err := gws.collectPoolOffers(ctx, pool)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := gws.getPrivatePoolState(ctx, workspaceID, pool.Name)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	reservations := []compute.Reservation{}
	if existing != nil {
		reservations = existing.Reservations
	}

	plan := solver.New().Solve(compute.SolveInput{
		Demand: compute.Demand{
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
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: plan.Reason}, nil
	}

	vendors := gws.computeVendors()
	newReservations := append([]compute.Reservation{}, reservations...)
	createdReservations := []compute.Reservation{}
	for _, action := range plan.Actions {
		if action.Type != compute.ActionCreate {
			continue
		}
		vendor := vendors[action.Offer.Provider]
		if vendor == nil {
			return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: fmt.Sprintf("vendor %q is not configured", action.Offer.Provider)}, nil
		}
		for i := uint32(0); i < action.Count; i++ {
			reservation, err := vendor.CreateReservation(ctx, compute.ReservationRequest{
				PoolName:       pool.Name,
				Selector:       pool.Selector,
				Offer:          action.Offer,
				Count:          1,
				TTL:            pool.TTL,
				MaxSpendMicros: pool.MaxSpendMicros,
				Source:         compute.SourceCLIReservation,
			})
			if err != nil {
				return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
			}
			if reservation == nil {
				return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "vendor returned empty reservation"}, nil
			}
			createdReservations = append(createdReservations, *reservation)
			newReservations = append(newReservations, *reservation)
		}
	}

	now := time.Now()
	state := &compute.PoolState{
		Name:                 pool.Name,
		Selector:             pool.Selector,
		Config:               config,
		Reservations:         newReservations,
		ReservedGPUs:         plan.TotalGPUs,
		CommittedSpendMicros: plan.CommittedCostMicros,
		Status:               "active",
		Source:               compute.SourceCLIReservation,
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
	if err := gws.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		err = gws.compensatePoolLaunchFailure(ctx, workspaceID, pool.Name, existing, vendors, createdReservations, err)
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			err = gws.compensatePoolLaunchFailure(ctx, workspaceID, pool.Name, existing, vendors, createdReservations, err)
			return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolReserved, ""))

	return &pb.LaunchPoolCapacityResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (gws *GatewayService) compensatePoolLaunchFailure(ctx context.Context, workspaceID, poolName string, previous *compute.PoolState, vendors map[string]compute.Vendor, reservations []compute.Reservation, cause error) error {
	failures := []string{}
	for _, reservation := range reservations {
		vendor := vendors[reservation.Provider]
		if vendor == nil {
			failures = append(failures, fmt.Sprintf("vendor %q is not configured", reservation.Provider))
			continue
		}
		instanceID := computeReservationInstanceID(reservation)
		if err := vendor.DeleteReservation(ctx, instanceID); err != nil {
			failures = append(failures, fmt.Sprintf("delete reservation %q: %v", instanceID, err))
		}
	}

	if previous != nil {
		if err := gws.savePrivatePoolState(ctx, workspaceID, previous); err != nil {
			failures = append(failures, fmt.Sprintf("restore previous pool state: %v", err))
		}
	} else if err := gws.deletePrivatePoolState(ctx, workspaceID, poolName); err != nil {
		failures = append(failures, fmt.Sprintf("delete partial pool state: %v", err))
	}

	if len(failures) > 0 {
		return fmt.Errorf("%w; cleanup failed: %s", cause, strings.Join(failures, "; "))
	}
	return cause
}

func computeReservationInstanceID(reservation compute.Reservation) string {
	return firstNonEmpty(reservation.InstanceID, reservation.ID)
}

func (gws *GatewayService) ListPrivatePools(ctx context.Context, in *pb.ListPrivatePoolsRequest) (*pb.ListPrivatePoolsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	states, err := gws.listPrivatePoolStates(ctx, workspaceID, 0)
	if err != nil {
		return &pb.ListPrivatePoolsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	states = filterPrivatePoolsCreatedByAuth(states, authInfo)
	if limit := int(in.Limit); limit > 0 && len(states) > limit {
		states = states[:limit]
	}
	out := make([]*pb.PrivatePool, 0, len(states))
	for _, state := range states {
		out = append(out, privatePoolStateToProto(state))
	}
	return &pb.ListPrivatePoolsResponse{Ok: true, Pools: out}, nil
}

func (gws *GatewayService) CreatePool(ctx context.Context, in *pb.CreatePoolRequest) (*pb.CreatePoolResponse, error) {
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
	if err := compute.ValidatePoolName(config.Name); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if _, err := computePoolFromProto(config, false); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := gws.getPrivatePoolState(ctx, workspaceID, config.Name)
	if err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	now := time.Now()
	state := &compute.PoolState{
		Name:             config.Name,
		Selector:         config.Selector,
		Config:           config,
		Status:           "active",
		Source:           compute.SourceManual,
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
	if err := gws.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return &pb.CreatePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreated, ""))
	return &pb.CreatePoolResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (gws *GatewayService) DeletePool(ctx context.Context, in *pb.DeletePoolRequest) (*pb.DeletePoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := gws.getOwnedPrivatePoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	vendors := gws.computeVendors()
	for _, reservation := range state.Reservations {
		if reservation.Source == compute.SourceManual || reservation.Source == compute.SourceAutosolver {
			continue
		}
		vendor := vendors[reservation.Provider]
		if vendor == nil {
			continue
		}
		_ = vendor.DeleteReservation(ctx, computeReservationInstanceID(reservation))
	}

	if err := gws.deletePrivatePoolState(ctx, workspaceID, in.Name); err != nil {
		return &pb.DeletePoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		gws.scheduler.DeleteAgentPool(firstNonEmpty(state.Selector, state.Name))
	}
	gws.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolDeleted, "deleted"))
	return &pb.DeletePoolResponse{Ok: true}, nil
}

func (gws *GatewayService) ExtendPoolCapacity(ctx context.Context, in *pb.ExtendPoolCapacityRequest) (*pb.ExtendPoolCapacityResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := gws.getOwnedPrivatePoolState(ctx, authInfo, in.Name)
	if err != nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if in.Ttl != "" {
		state.Config.Ttl = in.Ttl
		ttl, err := compute.ParseTTL(in.Ttl)
		if err != nil {
			return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		state.ExpiresAt = time.Now().Add(ttl)
	}
	if in.MaxSpend > 0 {
		state.Config.MaxSpend = in.MaxSpend
	}
	state.UpdatedAt = time.Now()
	if err := gws.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return &pb.ExtendPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolExtended, ""))
	return &pb.ExtendPoolCapacityResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (gws *GatewayService) CreatePoolJoinToken(ctx context.Context, in *pb.CreatePoolJoinTokenRequest) (*pb.CreatePoolJoinTokenResponse, error) {
	token, expiresAt, err := gws.createPrivatePoolJoinToken(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.CreatePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreatePoolJoinTokenResponse{
		Ok:        true,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (gws *GatewayService) RevokePoolJoinToken(ctx context.Context, in *pb.RevokePoolJoinTokenRequest) (*pb.RevokePoolJoinTokenResponse, error) {
	if in.Token == "" {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}
	state, err := gws.getComputeJoinTokenState(ctx, in.Token)
	if err != nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if state.WorkspaceID != computeWorkspaceID(authInfo) || state.CreatedByTokenID != computeOwnerTokenID(authInfo) {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	state.Revoked = true
	if err := gws.saveComputeJoinTokenState(ctx, state, time.Until(state.ExpiresAt)); err != nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitComputeEvent(types.EventComputeJoinToken, types.EventComputeSchema{
		WorkspaceID: state.WorkspaceID,
		PoolName:    state.PoolName,
		Action:      types.EventComputeActionJoinTokenRevoked,
		Status:      "revoked",
	})
	return &pb.RevokePoolJoinTokenResponse{Ok: true}, nil
}

func (gws *GatewayService) GetPoolJoinCommand(ctx context.Context, in *pb.GetPoolJoinCommandRequest) (*pb.GetPoolJoinCommandResponse, error) {
	command, token, expiresAt, err := gws.createPrivatePoolJoinCommand(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.GetPoolJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetPoolJoinCommandResponse{
		Ok:        true,
		Command:   command,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (gws *GatewayService) ListPoolMachines(ctx context.Context, in *pb.ListPoolMachinesRequest) (*pb.ListPoolMachinesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := gws.getOwnedPrivatePoolState(ctx, authInfo, in.PoolName)
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := gws.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.ListPoolMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if limit := int(in.Limit); limit > 0 && len(machines) > limit {
		machines = machines[:limit]
	}

	out := make([]*pb.Machine, 0, len(machines))
	for _, machine := range machines {
		out = append(out, agentMachineToProto(machine))
	}
	return &pb.ListPoolMachinesResponse{Ok: true, Machines: out}, nil
}

func (gws *GatewayService) JoinAgent(ctx context.Context, in *pb.JoinAgentRequest) (*pb.JoinAgentResponse, error) {
	if strings.TrimSpace(in.JoinToken) == "" {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}

	tokenState, err := gws.getComputeJoinTokenState(ctx, in.JoinToken)
	if err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if tokenState == nil || tokenState.Revoked || time.Now().After(tokenState.ExpiresAt) {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	poolState, err := gws.getPrivatePoolState(ctx, tokenState.WorkspaceID, tokenState.PoolName)
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
	agentState := &compute.AgentTokenState{
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
	}
	if err := gws.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if gws.scheduler != nil {
		if err := gws.scheduler.RegisterAgentPool(tokenState.WorkspaceID, poolState); err != nil {
			return &pb.JoinAgentResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	gws.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
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
	agentState, err := gws.getComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	transport := firstNonEmpty(in.Transport, types.BackendRouteTransportTSNet)
	transport = strings.ReplaceAll(transport, "-", "_")
	if err := gws.validateAgentTransportConfig(transport); err != nil {
		return &pb.RequestAgentTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gws.emitComputeEvent(types.EventComputeTransport, types.EventComputeSchema{
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionTransportCredentialVended,
		Status:      "ready",
		Transport:   transport,
	})

	return &pb.RequestAgentTransportCredentialResponse{
		Ok:         true,
		AuthKey:    gws.appConfig.Tailscale.AgentAuthKey,
		ControlUrl: gws.appConfig.Tailscale.ControlURL,
		Hostname:   "beam-agent-" + agentState.MachineID,
		Ephemeral:  true,
	}, nil
}

func (gws *GatewayService) ListAgentRoutes(ctx context.Context, in *pb.ListAgentRoutesRequest) (*pb.ListAgentRoutesResponse, error) {
	agentState, err := gws.getComputeAgentTokenState(ctx, in.AgentToken)
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
	agentState, err := gws.getComputeAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
	}
	if agentState == nil {
		return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: "invalid agent token"})
	}

	sendSnapshot := func() error {
		if gws.scheduler != nil {
			if poolState, err := gws.getPrivatePoolState(ctx, agentState.WorkspaceID, agentState.PoolName); err == nil && poolState != nil {
				if err := gws.scheduler.RegisterAgentPool(agentState.WorkspaceID, poolState); err != nil {
					return stream.Send(&pb.StreamAgentResponse{Ok: false, ErrMsg: err.Error()})
				}
			}
		}
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
		routeRevisionKey := common.RedisKeys.SchedulerBackendRouteMachineRevision(agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
		if err := gws.keyEventManager.ListenForPattern(ctx, routeRevisionKey, events); err != nil {
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

func (gws *GatewayService) agentRoutesForMachine(ctx context.Context, agentState *compute.AgentTokenState) ([]*pb.AgentRoute, error) {
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

func (gws *GatewayService) agentSlotsForMachine(ctx context.Context, agentState *compute.AgentTokenState) ([]*pb.AgentWorkerSlot, error) {
	slots, err := gws.computeRepo.ListAgentWorkerSlotStates(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return nil, err
	}
	worker, err := gws.agentMachineWorker(agentState)
	if err != nil {
		return nil, err
	}
	if worker == nil {
		if err := gws.pruneAgentWorkerSlots(ctx, agentState, "", slots); err != nil {
			return nil, err
		}
		return nil, nil
	}

	slot, workerToken, err := gws.ensureAgentWorkerSlot(ctx, agentState, worker, slots)
	if err != nil {
		return nil, err
	}
	if err := gws.pruneAgentWorkerSlots(ctx, agentState, worker.Id, slots); err != nil {
		return nil, err
	}
	return []*pb.AgentWorkerSlot{agentWorkerSlotToProto(slot, workerToken)}, nil
}

func (gws *GatewayService) agentMachineWorker(agentState *compute.AgentTokenState) (*types.Worker, error) {
	worker, err := gws.workerRepo.GetWorkerById(compute.AgentMachineWorkerID(agentState.MachineID))
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

func (gws *GatewayService) ensureAgentWorkerSlot(ctx context.Context, agentState *compute.AgentTokenState, worker *types.Worker, slots []*compute.AgentWorkerSlotState) (*compute.AgentWorkerSlotState, string, error) {
	var existing *compute.AgentWorkerSlotState
	for _, slot := range slots {
		if slot != nil && slot.WorkerID == worker.Id {
			existing = slot
			break
		}
	}

	workspace, err := gws.backendRepo.GetWorkspaceByExternalId(ctx, agentState.WorkspaceID)
	if err != nil {
		return nil, "", err
	}
	token, tokenID, tokenHash, err := gws.agentWorkerToken(ctx, workspace.Id, existing)
	if err != nil {
		return nil, "", err
	}

	slot := agentWorkerSlotState(gws.appConfig, agentState, worker, tokenID, tokenHash)
	if existing != nil {
		slot.CreatedAt = existing.CreatedAt
	}
	if err := gws.computeRepo.SaveAgentWorkerSlotState(ctx, slot); err != nil {
		return nil, "", err
	}
	if existing == nil {
		gws.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
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

func (gws *GatewayService) agentWorkerToken(ctx context.Context, workspaceID uint, existing *compute.AgentWorkerSlotState) (string, string, string, error) {
	if existing != nil && existing.WorkerTokenID != "" {
		token, err := gws.backendRepo.GetTokenByExternalId(ctx, workspaceID, existing.WorkerTokenID)
		if err == nil && token != nil && token.Active && !token.DisabledByClusterAdmin && token.TokenType == types.TokenTypeWorker {
			tokenHash := hashComputeToken(token.Key)
			if existing.WorkerTokenHash == "" || existing.WorkerTokenHash == tokenHash {
				return token.Key, token.ExternalId, tokenHash, nil
			}
		}
	}

	createdToken, err := gws.backendRepo.CreateToken(ctx, workspaceID, types.TokenTypeWorker, true)
	if err != nil {
		return "", "", "", err
	}
	return createdToken.Key, createdToken.ExternalId, hashComputeToken(createdToken.Key), nil
}

func (gws *GatewayService) pruneAgentWorkerSlots(ctx context.Context, agentState *compute.AgentTokenState, keepWorkerID string, slots []*compute.AgentWorkerSlotState) error {
	for _, slot := range slots {
		if slot == nil || slot.WorkerID == "" || slot.WorkerID == keepWorkerID {
			continue
		}
		if err := gws.computeRepo.DeleteAgentWorkerSlotState(ctx, slot.WorkspaceID, slot.PoolName, slot.MachineID, slot.WorkerID); err != nil {
			return err
		}
		gws.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
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

func agentWorkerSlotState(config types.AppConfig, agentState *compute.AgentTokenState, worker *types.Worker, tokenID, tokenHash string) *compute.AgentWorkerSlotState {
	return &compute.AgentWorkerSlotState{
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

func agentWorkerSlotToProto(slot *compute.AgentWorkerSlotState, workerToken string) *pb.AgentWorkerSlot {
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

func (gws *GatewayService) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	agentState, err := gws.getComputeAgentTokenState(ctx, in.AgentToken)
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
		gws.emitComputeEvent(types.EventComputeRoute, types.EventComputeSchema{
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

func (gws *GatewayService) computeVendors() map[string]compute.Vendor {
	vendors := map[string]compute.Vendor{}
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

func (gws *GatewayService) collectPoolOffers(ctx context.Context, pool compute.Pool) ([]compute.Offer, error) {
	vendors := gws.computeVendors()
	if len(vendors) == 0 {
		return nil, fmt.Errorf("no compute GPU vendors are configured")
	}

	request := compute.OfferRequest{
		GPUs:           pool.GPUs,
		TotalGPUs:      pool.TotalGPUs,
		Providers:      pool.Providers,
		Regions:        pool.Regions,
		MinReliability: pool.MinReliability,
	}
	offers := []compute.Offer{}
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

func normalizePoolConfig(in *pb.PoolConfig) *pb.PoolConfig {
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
		out.Mode = string(types.PoolModePrivate)
	}
	if out.Transport == "" {
		out.Transport = defaultPrivateTransport
	}
	out.Transport = strings.ReplaceAll(out.Transport, "-", "_")
	if out.Fallback == "" {
		out.Fallback = defaultPrivateFallback
	}
	if out.Priority == 0 {
		out.Priority = defaultPrivatePriority
	}
	return &out
}

func computePoolFromProto(in *pb.PoolConfig, requireReservation bool) (compute.Pool, error) {
	if in == nil {
		return compute.Pool{}, fmt.Errorf("pool config is required")
	}
	if in.Mode != "" && in.Mode != string(types.PoolModePrivate) {
		return compute.Pool{}, fmt.Errorf("private pool mode must be %q", types.PoolModePrivate)
	}
	switch in.Transport {
	case "", defaultPrivateTransport:
	default:
		return compute.Pool{}, fmt.Errorf("unsupported agent transport %q", in.Transport)
	}
	switch in.Fallback {
	case "", types.PrivatePoolFallbackInternal, types.PrivatePoolFallbackWait, types.PrivatePoolFallbackFail:
	default:
		return compute.Pool{}, fmt.Errorf("unsupported private pool fallback %q", in.Fallback)
	}
	ttl, err := compute.ParseTTL(in.Ttl)
	if err != nil {
		return compute.Pool{}, err
	}
	pool := compute.Pool{
		Name:           in.Name,
		Selector:       in.Selector,
		GPUs:           in.Gpu,
		TotalGPUs:      in.Gpus,
		TTL:            ttl,
		MaxSpendMicros: compute.DollarsToMicros(in.MaxSpend),
		Providers:      in.Providers,
		Regions:        in.Regions,
		MinReliability: in.MinReliability,
	}
	if requireReservation {
		if err := pool.Validate(); err != nil {
			return compute.Pool{}, err
		}
	} else if pool.MinReliability < 0 || pool.MinReliability > 1 {
		return compute.Pool{}, fmt.Errorf("min_reliability must be between 0 and 1")
	}
	return pool, nil
}

func poolOfferToProto(offer compute.Offer) *pb.PoolOffer {
	return &pb.PoolOffer{
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

func providerInstanceToProto(reservation compute.Reservation) *pb.ProviderInstance {
	return &pb.ProviderInstance{
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

func privatePoolStateToProto(state *compute.PoolState) *pb.PrivatePool {
	reservations := make([]*pb.ProviderInstance, 0, len(state.Reservations))
	for _, reservation := range state.Reservations {
		reservations = append(reservations, providerInstanceToProto(reservation))
	}
	config := normalizePoolConfig(state.Config)
	return &pb.PrivatePool{
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

func agentMachineToProto(state *compute.AgentTokenState) *pb.Machine {
	if state == nil {
		return &pb.Machine{}
	}
	gpu := ""
	if len(state.GPUs) > 0 {
		gpu = strings.Join(state.GPUs, ",")
	}
	return &pb.Machine{
		Id:            state.MachineID,
		Cpu:           state.CPUMillicores,
		Memory:        int64(state.MemoryMB),
		Gpu:           gpu,
		GpuCount:      state.GPUCount,
		Status:        computeMachineStatus(state),
		PoolName:      state.PoolName,
		ProviderName:  types.DefaultAgentName,
		Created:       formatComputeTime(state.CreatedAt),
		LastKeepalive: formatComputeTime(state.LastJoinAt),
	}
}

func formatComputeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func (gws *GatewayService) createPrivatePoolJoinCommand(ctx context.Context, poolName, ttlValue string) (string, string, time.Time, error) {
	gatewayURL := strings.TrimRight(gws.appConfig.GatewayService.HTTP.GetExternalURL(), "/")

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	devMode := isLocalGatewayURL(gatewayURL)
	if authInfo != nil && authInfo.Workspace != nil {
		if state, err := gws.getOwnedPrivatePoolState(ctx, authInfo, poolName); err == nil && state != nil {
			config := normalizePoolConfig(state.Config)
			if err := gws.validateAgentTransportConfig(config.Transport); err != nil {
				return "", "", time.Time{}, err
			}
		}
	}

	token, expiresAt, err := gws.createPrivatePoolJoinToken(ctx, poolName, ttlValue)
	if err != nil {
		return "", "", time.Time{}, err
	}

	command := fmt.Sprintf("curl -fsSL %s/install/agent | sudo bash -s -- --gateway %s --join-token %s", gatewayURL, gatewayURL, token)
	if devMode {
		command = fmt.Sprintf("curl -fsSL %s/install/agent | bash -s -- --gateway %s --join-token %s --dev", gatewayURL, gatewayURL, token)
	}
	return command, token, expiresAt, nil
}

func (gws *GatewayService) createPrivatePoolJoinToken(ctx context.Context, poolName, ttlValue string) (string, time.Time, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", time.Time{}, fmt.Errorf("pool name is required")
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	ownerTokenID := computeOwnerTokenID(authInfo)
	if ownerTokenID == "" {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := gws.getOwnedPrivatePoolState(ctx, authInfo, poolName)
	if err != nil {
		return "", time.Time{}, err
	}
	if state == nil {
		return "", time.Time{}, fmt.Errorf("pool not found")
	}

	ttl, err := compute.ParseTTL(ttlValue)
	if err != nil {
		return "", time.Time{}, err
	}
	if ttl == 0 {
		ttl = defaultPrivateJoinTTL
	}
	if ttl <= 0 {
		return "", time.Time{}, fmt.Errorf("join token ttl must be positive")
	}

	token, err := generateComputeToken()
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now()
	tokenState := &compute.JoinTokenState{
		TokenHash:        hashComputeToken(token),
		WorkspaceID:      authInfo.Workspace.ExternalId,
		PoolName:         poolName,
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		ExpiresAt:        now.Add(ttl),
	}
	if err := gws.saveComputeJoinTokenState(ctx, tokenState, ttl); err != nil {
		return "", time.Time{}, err
	}
	gws.emitComputeEvent(types.EventComputeJoinToken, types.EventComputeSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		Action:      types.EventComputeActionJoinTokenCreated,
		Status:      "active",
		Attrs: map[string]string{
			"expires_at":  tokenState.ExpiresAt.UTC().Format(time.RFC3339),
			"ttl_seconds": fmt.Sprintf("%.0f", ttl.Seconds()),
		},
	})
	return token, tokenState.ExpiresAt, nil
}

func (gws *GatewayService) agentBootstrapConfig(workspaceID string, poolState *compute.PoolState) *pb.AgentBootstrapConfig {
	config := normalizePoolConfig(poolState.Config)
	return &pb.AgentBootstrapConfig{
		GatewayHttpUrl:  gws.appConfig.GatewayService.HTTP.GetExternalURL(),
		GatewayGrpcHost: gws.appConfig.GatewayService.GRPC.ExternalHost,
		GatewayGrpcPort: int32(gws.appConfig.GatewayService.GRPC.ExternalPort),
		GatewayGrpcTls:  gws.appConfig.GatewayService.GRPC.TLS,
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

func (gws *GatewayService) validateAgentTransportConfig(transport string) error {
	transport = strings.ReplaceAll(firstNonEmpty(transport, types.BackendRouteTransportTSNet), "-", "_")
	switch transport {
	case types.BackendRouteTransportTSNet:
		if !gws.appConfig.Tailscale.Enabled {
			return fmt.Errorf("tailscale is not enabled")
		}
		if gws.appConfig.Tailscale.AuthKey == "" {
			return fmt.Errorf("gateway tailscale auth key is not configured")
		}
		if gws.appConfig.Tailscale.AgentAuthKey == "" {
			return fmt.Errorf("agent tailscale auth key is not configured")
		}
		return nil
	default:
		return fmt.Errorf("unsupported agent transport %q", transport)
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

func generateComputeToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func hashComputeToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func computeMachineID(workspaceID, poolName, fingerprint string) string {
	seed := fingerprint
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + poolName + "\x00" + seed))
	return "machine-" + hex.EncodeToString(sum[:])[:20]
}

func preflightChecksFromProto(in []*pb.AgentPreflightCheck) []compute.PreflightCheckState {
	checks := make([]compute.PreflightCheckState, 0, len(in))
	for _, check := range in {
		if check == nil {
			continue
		}
		checks = append(checks, compute.PreflightCheckState{
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

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func (gws *GatewayService) savePrivatePoolState(ctx context.Context, workspaceID string, state *compute.PoolState) error {
	return gws.computeRepo.SavePoolState(ctx, workspaceID, state)
}

func (gws *GatewayService) getPrivatePoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	return gws.computeRepo.GetPoolState(ctx, workspaceID, name)
}

func (gws *GatewayService) listPrivatePoolStates(ctx context.Context, workspaceID string, limit int) ([]*compute.PoolState, error) {
	return gws.computeRepo.ListPoolStates(ctx, workspaceID, limit)
}

func (gws *GatewayService) deletePrivatePoolState(ctx context.Context, workspaceID, name string) error {
	return gws.computeRepo.DeletePoolState(ctx, workspaceID, name)
}

func (gws *GatewayService) saveComputeJoinTokenState(ctx context.Context, state *compute.JoinTokenState, ttl time.Duration) error {
	return gws.computeRepo.SaveJoinTokenState(ctx, state, ttl)
}

func (gws *GatewayService) getComputeJoinTokenState(ctx context.Context, token string) (*compute.JoinTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return gws.computeRepo.GetJoinTokenState(ctx, hashComputeToken(token))
}

func (gws *GatewayService) saveComputeAgentTokenState(ctx context.Context, state *compute.AgentTokenState) error {
	return gws.computeRepo.SaveAgentTokenState(ctx, state, 24*time.Hour)
}

func (gws *GatewayService) getComputeAgentTokenState(ctx context.Context, token string) (*compute.AgentTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return gws.computeRepo.GetAgentTokenState(ctx, hashComputeToken(token))
}

func (gws *GatewayService) getOwnedPrivatePoolState(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*compute.PoolState, error) {
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	state, err := gws.getPrivatePoolState(ctx, workspaceID, poolName)
	if err != nil {
		return nil, err
	}
	if state == nil || !computePoolCreatedByAuth(state, authInfo) {
		return nil, nil
	}
	return state, nil
}

func (gws *GatewayService) emitComputeEvent(eventType string, event types.EventComputeSchema) {
	if gws.eventRepo == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	gws.eventRepo.PushComputeEvent(eventType, event)
}

func computePoolEvent(workspaceID string, state *compute.PoolState, action, status string) types.EventComputeSchema {
	if state == nil {
		return types.EventComputeSchema{}
	}
	if status == "" {
		status = state.Status
	}
	event := types.EventComputeSchema{
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

func computeMachineStatus(state *compute.AgentTokenState) string {
	if state != nil && state.Schedulable {
		return "schedulable"
	}
	return "preflight_failed"
}

func boolPtr(value bool) *bool {
	return &value
}

func preflightSummary(checks []compute.PreflightCheckState) string {
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

func filterPrivatePoolsCreatedByAuth(states []*compute.PoolState, authInfo *auth.AuthInfo) []*compute.PoolState {
	if len(states) == 0 {
		return states
	}

	filtered := make([]*compute.PoolState, 0, len(states))
	for _, state := range states {
		if computePoolCreatedByAuth(state, authInfo) {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func computePoolCreatedByAuth(state *compute.PoolState, authInfo *auth.AuthInfo) bool {
	if state == nil || state.CreatedByTokenID == "" {
		return false
	}
	return state.CreatedByTokenID == computeOwnerTokenID(authInfo)
}

func computeOwnerTokenID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Token == nil {
		return ""
	}
	return authInfo.Token.ExternalId
}

func computeWorkspaceID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Workspace == nil {
		return ""
	}
	return authInfo.Workspace.ExternalId
}
