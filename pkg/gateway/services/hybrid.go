package gatewayservices

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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

func (gws *GatewayService) JoinHybridPool(ctx context.Context, in *pb.JoinHybridPoolRequest) (*pb.JoinHybridPoolResponse, error) {
	if strings.TrimSpace(in.JoinToken) == "" {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}

	tokenState, err := gws.getHybridJoinTokenState(ctx, in.JoinToken)
	if err != nil {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if tokenState == nil || tokenState.Revoked || time.Now().After(tokenState.ExpiresAt) {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	poolState, err := gws.getHybridPoolState(ctx, tokenState.WorkspaceID, tokenState.PoolName)
	if err != nil {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if poolState == nil {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	if poolState.CreatedByTokenID == "" || poolState.CreatedByTokenID != tokenState.CreatedByTokenID {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: "join token is invalid or expired"}, nil
	}

	agentToken, err := generateHybridToken()
	if err != nil {
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
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
		return &pb.JoinHybridPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	return &pb.JoinHybridPoolResponse{
		Ok:          true,
		WorkspaceId: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		MachineId:   machineID,
		AgentToken:  agentToken,
		Bootstrap:   gws.hybridWorkerBootstrapConfig(tokenState.WorkspaceID, poolState),
	}, nil
}

func (gws *GatewayService) RequestHybridTransportCredential(ctx context.Context, in *pb.RequestHybridTransportCredentialRequest) (*pb.RequestHybridTransportCredentialResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	transport := firstNonEmpty(in.Transport, types.BackendRouteTransportTSNet)
	transport = strings.ReplaceAll(transport, "-", "_")
	if transport == types.BackendRouteTransportLocalDirect {
		return &pb.RequestHybridTransportCredentialResponse{Ok: true}, nil
	}
	if transport != types.BackendRouteTransportTSNet {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: fmt.Sprintf("unsupported transport %q", transport)}, nil
	}
	if !gws.appConfig.Tailscale.Enabled {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: "tailscale is not enabled"}, nil
	}
	if gws.appConfig.Tailscale.AuthKey == "" {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: "gateway tailscale auth key is not configured"}, nil
	}
	if gws.appConfig.Tailscale.HybridWorkerAuthKey == "" {
		return &pb.RequestHybridTransportCredentialResponse{Ok: false, ErrMsg: "hybrid worker tailscale auth key is not configured"}, nil
	}

	return &pb.RequestHybridTransportCredentialResponse{
		Ok:         true,
		AuthKey:    gws.appConfig.Tailscale.HybridWorkerAuthKey,
		ControlUrl: gws.appConfig.Tailscale.ControlURL,
		Hostname:   "beam-agent-" + agentState.MachineID,
		Ephemeral:  true,
	}, nil
}

func (gws *GatewayService) ListHybridRoutes(ctx context.Context, in *pb.ListHybridRoutesRequest) (*pb.ListHybridRoutesResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.ListHybridRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.ListHybridRoutesResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	routes, err := gws.containerRepo.ListBackendRoutesByMachine(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return &pb.ListHybridRoutesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	out := make([]*pb.HybridBackendRoute, 0, len(routes))
	for _, route := range routes {
		if route.State == types.BackendRouteStateClosing {
			continue
		}
		out = append(out, hybridBackendRouteToProto(route))
	}
	return &pb.ListHybridRoutesResponse{Ok: true, Routes: out}, nil
}

func (gws *GatewayService) UpdateHybridRouteStatus(ctx context.Context, in *pb.UpdateHybridRouteStatusRequest) (*pb.UpdateHybridRouteStatusResponse, error) {
	agentState, err := gws.getHybridAgentTokenState(ctx, in.AgentToken)
	if err != nil {
		return &pb.UpdateHybridRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if agentState == nil {
		return &pb.UpdateHybridRouteStatusResponse{Ok: false, ErrMsg: "invalid agent token"}, nil
	}

	route, err := gws.containerRepo.GetBackendRoute(ctx, in.RouteId)
	if err != nil {
		return &pb.UpdateHybridRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if route.WorkspaceID != agentState.WorkspaceID || route.PoolName != agentState.PoolName || route.MachineID != agentState.MachineID {
		return &pb.UpdateHybridRouteStatusResponse{Ok: false, ErrMsg: "route does not belong to this agent"}, nil
	}

	route.State = firstNonEmpty(in.State, route.State)
	route.ProxyTarget = firstNonEmpty(in.ProxyTarget, route.ProxyTarget)
	route.Error = in.Error
	route.UpdatedAt = time.Now().Unix()
	if err := gws.containerRepo.SetBackendRoute(ctx, *route); err != nil {
		return &pb.UpdateHybridRouteStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.UpdateHybridRouteStatusResponse{Ok: true}, nil
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
	case "", defaultHybridTransport, types.BackendRouteTransportLocalDirect:
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

func hybridBackendRouteToProto(route types.BackendRoute) *pb.HybridBackendRoute {
	return &pb.HybridBackendRoute{
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
	token, expiresAt, err := gws.createHybridPoolJoinToken(ctx, poolName, ttlValue)
	if err != nil {
		return "", "", time.Time{}, err
	}
	gatewayURL := strings.TrimRight(gws.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	command := fmt.Sprintf("curl -fsSL %s/install/hybrid-worker | sudo bash -s -- --gateway %s --join-token %s", gatewayURL, gatewayURL, token)
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo != nil && authInfo.Workspace != nil {
		if state, err := gws.getOwnedHybridPoolState(ctx, authInfo, poolName); err == nil && state != nil {
			config := normalizeHybridPoolConfig(state.Config)
			if config.Transport == types.BackendRouteTransportLocalDirect {
				command = fmt.Sprintf("curl -fsSL %s/install/hybrid-worker | bash -s -- --gateway %s --join-token %s --dev", gatewayURL, gatewayURL, token)
			}
		}
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
	return token, tokenState.ExpiresAt, nil
}

func (gws *GatewayService) hybridWorkerBootstrapConfig(workspaceID string, poolState *hybrid.PoolState) *pb.HybridWorkerBootstrapConfig {
	config := normalizeHybridPoolConfig(poolState.Config)
	return &pb.HybridWorkerBootstrapConfig{
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

func hybridPreflightChecksFromProto(in []*pb.HybridPreflightCheck) []hybrid.PreflightCheckState {
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
