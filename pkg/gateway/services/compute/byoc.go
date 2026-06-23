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
	byocBootstrapJoinTokenHashLabel            = "join_token_hash"
	byocBootstrapJoinTokenHashesLabel          = "join_token_hashes"
	byocDeletedPoolRetention                   = 5 * time.Minute
	byocProviderControlUnavailableCleanupGrace = 30 * time.Minute
)

var (
	errBYOCProviderControlUnavailable = errors.New("BYOC provider control plane is unavailable")
	errBYOCProviderResourceNotFound   = errors.New("BYOC provider resource not found")
)

// byocProvider owns provider-specific validation and setup links; the service
// owns auth, pool state, join tokens, events, and scheduler registration.
type byocProvider interface {
	Source() model.CapacitySource
	NormalizePoolRequest(byocRawPoolRequest) (byocPoolRequest, error)
	ValidateSetupConfig(types.ManagedComputeConfig) error
	PoolState(workspaceID string, req byocPoolRequest, config types.ManagedComputeConfig) *model.BYOCProviderState
	Setup(context.Context, byocProviderSetupInput) (*byocProviderSetupResult, error)
	Resource(workspaceID string, state *model.PoolState, config types.ManagedComputeConfig) (*byocProviderResource, error)
	Scale(context.Context, byocProviderScaleInput) error
	ResourceDeleted(context.Context, byocProviderResourceInput) (bool, error)
	ReleaseMachine(context.Context, byocProviderReleaseMachineInput) error
	UpdateScaleState(*model.PoolState, byocPoolRequest, types.ManagedComputeConfig) error
	ValidateExistingPool(*model.PoolState, byocPoolRequest) error
}

type byocRawPoolRequest struct {
	PoolName     string
	Region       string
	InstanceType string
	DesiredNodes uint32
	MaxNodes     uint32
	AccountID    string
	GPU          string
	GPUCount     uint32
}

type byocPoolRequest struct {
	poolName     string
	region       string
	instanceType string
	desiredNodes uint32
	maxNodes     uint32
	accountID    string
	gpu          string
	gpuCount     uint32
}

type byocProviderSetupInput struct {
	WorkspaceID  string
	GatewayURL   string
	JoinToken    string
	WorkerImage  string
	Config       types.ManagedComputeConfig
	Request      byocPoolRequest
	Pool         *model.PoolState
	ProviderData *model.BYOCProviderState
}

type byocProviderSetupResult struct {
	SetupURL     string
	ResourceName string
	ResourceURL  string
	EventAttrs   map[string]string
}

type byocProviderScaleInput struct {
	WorkspaceID  string
	Config       types.ManagedComputeConfig
	Request      byocPoolRequest
	Pool         *model.PoolState
	ProviderData *model.BYOCProviderState
}

type byocProviderResourceInput struct {
	WorkspaceID  string
	Pool         *model.PoolState
	ProviderData *model.BYOCProviderState
}

type byocProviderReleaseMachineInput struct {
	WorkspaceID  string
	Pool         *model.PoolState
	ProviderData *model.BYOCProviderState
	Machine      *model.AgentTokenState
}

type byocProviderResource struct {
	Provider          string
	AccountID         string
	Region            string
	ResourceName      string
	ResourceURL       string
	DestroyURL        string
	InstanceType      string
	GPU               string
	GPUCount          uint32
	DesiredNodes      uint32
	MaxNodes          uint32
	TargetSandboxes   uint32
	SandboxesPerNode  uint32
	HourlyCostMicros  int64
	TotalHourlyMicros int64
	DirectScale       bool
}

type byocPoolSetupResult struct {
	Pool         *model.PoolState
	SetupURL     string
	ResourceName string
	ResourceURL  string
}

func (s *Service) CreateBYOCPool(ctx context.Context, in *pb.CreateBYOCPoolRequest) (*pb.CreateBYOCPoolResponse, error) {
	if in == nil {
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	provider, err := byocProviderForName(in.Provider)
	if err != nil {
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	result, err := s.prepareBYOCPoolSetup(ctx, provider, byocRawPoolRequest{
		PoolName:     in.PoolName,
		Region:       in.Region,
		InstanceType: in.InstanceType,
		DesiredNodes: in.DesiredNodes,
		MaxNodes:     in.MaxNodes,
		AccountID:    in.AccountId,
		GPU:          in.Gpu,
		GPUCount:     in.GpuCount,
	})
	if err != nil {
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	pool := s.privatePoolStateToProto(result.Pool)
	return &pb.CreateBYOCPoolResponse{
		Ok:           true,
		Pool:         pool,
		SetupUrl:     result.SetupURL,
		ResourceName: result.ResourceName,
		ResourceUrl:  result.ResourceURL,
		Byoc:         pool.GetByoc(),
	}, nil
}

func (s *Service) prepareBYOCPoolSetup(ctx context.Context, provider byocProvider, raw byocRawPoolRequest) (*byocPoolSetupResult, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	req, err := provider.NormalizePoolRequest(raw)
	if err != nil {
		return nil, err
	}

	var state *model.PoolState
	lockErr := s.withPoolStateLock(ctx, workspaceID, req.poolName, func() error {
		next, err := s.createOrUpdateBYOCPool(ctx, authInfo, provider, req)
		if err != nil {
			return err
		}
		state = next
		return nil
	})
	if lockErr != nil {
		return nil, lockErr
	}

	joinToken, tokenState, err := s.createPersistentPrivatePoolJoinTokenForOwner(
		ctx,
		workspaceID,
		ownerTokenID,
		req.poolName,
		"",
	)
	if err != nil {
		return nil, err
	}
	if state.BYOC != nil {
		recordBYOCBootstrapJoinTokenHash(state.BYOC, tokenState.TokenHash)
		if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
			_ = s.revokeComputeJoinTokenHash(ctx, tokenState.TokenHash)
			return nil, err
		}
	}

	setup, err := provider.Setup(ctx, byocProviderSetupInput{
		WorkspaceID:  workspaceID,
		GatewayURL:   strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/"),
		JoinToken:    joinToken,
		WorkerImage:  agentWorkerImage(s.appConfig),
		Config:       s.appConfig.ManagedCompute,
		Request:      req,
		Pool:         state,
		ProviderData: state.BYOC,
	})
	if err != nil {
		return nil, err
	}

	attrs := map[string]string{
		"provider": string(provider.Source()),
	}
	for key, value := range setup.EventAttrs {
		attrs[key] = value
	}
	s.emitComputeEvent(types.EventComputePool, types.EventComputeSchema{
		WorkspaceID: workspaceID,
		PoolName:    state.Name,
		Action:      types.EventComputeActionPoolCreated,
		Status:      "byoc_pool_configured",
		Source:      string(provider.Source()),
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		NodeCount:   req.desiredNodes,
		Attrs:       attrs,
	})

	return &byocPoolSetupResult{
		Pool:         state,
		SetupURL:     setup.SetupURL,
		ResourceName: setup.ResourceName,
		ResourceURL:  setup.ResourceURL,
	}, nil
}

func (s *Service) GetBYOCPool(ctx context.Context, in *pb.GetBYOCPoolRequest) (*pb.GetBYOCPoolResponse, error) {
	if in == nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, strings.TrimSpace(in.GetPoolName()))
	if err != nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	machines, deleted, err := s.reconcileBYOCPoolMachinesForRead(ctx, workspaceID, state, machines, time.Now().UTC())
	if err != nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if deleted {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	pool := s.privatePoolStateToProtoWithMachines(state, machines)
	if pool.GetByoc() == nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool is not a BYOC pool"}, nil
	}
	ready := pool.GetByoc().Phase == "ready"
	return &pb.GetBYOCPoolResponse{
		Ok:                true,
		Pool:              pool,
		Ready:             ready,
		ReadyMachineCount: pool.ReadyMachineCount,
		MachineCount:      pool.MachineCount,
		Byoc:              pool.GetByoc(),
	}, nil
}

func (s *Service) ScaleBYOCPool(ctx context.Context, in *pb.ScaleBYOCPoolRequest) (*pb.ScaleBYOCPoolResponse, error) {
	if in == nil {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	poolName := strings.TrimSpace(in.GetPoolName())
	if poolName == "" {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}

	var response *pb.ScaleBYOCPoolResponse
	lockErr := s.withPoolStateLock(ctx, workspaceID, poolName, func() error {
		state, err := s.getOwnedPrivatePoolState(ctx, authInfo, poolName)
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		if state == nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "pool not found"}
			return nil
		}
		provider, err := byocProviderForState(state)
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		resource, err := provider.Resource(workspaceID, state, s.appConfig.ManagedCompute)
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		req, err := provider.NormalizePoolRequest(byocRawPoolRequest{
			PoolName:     state.Name,
			Region:       resource.Region,
			InstanceType: resource.InstanceType,
			DesiredNodes: in.GetDesiredNodes(),
			MaxNodes:     firstNonZeroUint32(in.GetMaxNodes(), in.GetDesiredNodes()),
			AccountID:    resource.AccountID,
			GPU:          resource.GPU,
			GPUCount:     resource.GPUCount,
		})
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		if err := provider.Scale(ctx, byocProviderScaleInput{
			WorkspaceID:  workspaceID,
			Config:       s.appConfig.ManagedCompute,
			Request:      req,
			Pool:         state,
			ProviderData: state.BYOC,
		}); err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		if err := provider.UpdateScaleState(state, req, s.appConfig.ManagedCompute); err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		state.UpdatedAt = time.Now().UTC()
		if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		machines, err = s.pruneBYOCPoolMachinesAboveDesired(ctx, workspaceID, state, machines, time.Now().UTC())
		if err != nil {
			response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}
			return nil
		}
		pool := s.privatePoolStateToProtoWithMachines(state, machines)
		s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolExtended, "byoc_scaled"))
		response = &pb.ScaleBYOCPoolResponse{
			Ok:   true,
			Pool: pool,
			Byoc: pool.GetByoc(),
		}
		return nil
	})
	if lockErr != nil {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: lockErr.Error()}, nil
	}
	if response == nil {
		response = &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "failed to scale BYOC pool"}
	}
	return response, nil
}

func byocPoolStateToProto(state *model.PoolState, pool *pb.PrivatePool, config types.ManagedComputeConfig) *pb.BYOCPoolState {
	if state == nil || state.BYOC == nil {
		return nil
	}
	provider, err := byocProviderForState(state)
	if err != nil {
		return nil
	}
	resource, err := provider.Resource(state.WorkspaceID, state, config)
	if err != nil {
		return nil
	}
	phase, message := byocPoolPhase(resource, pool)
	return &pb.BYOCPoolState{
		Provider:              resource.Provider,
		AccountId:             resource.AccountID,
		Region:                resource.Region,
		ResourceName:          resource.ResourceName,
		ResourceUrl:           resource.ResourceURL,
		DestroyUrl:            resource.DestroyURL,
		Phase:                 phase,
		DesiredNodes:          resource.DesiredNodes,
		MaxNodes:              resource.MaxNodes,
		TargetSandboxes:       resource.TargetSandboxes,
		SandboxesPerNode:      resource.SandboxesPerNode,
		InstanceType:          resource.InstanceType,
		Gpu:                   resource.GPU,
		GpuCount:              resource.GPUCount,
		MachineCount:          pool.MachineCount,
		ReadyMachineCount:     pool.ReadyMachineCount,
		Message:               message,
		HourlyCostMicros:      resource.HourlyCostMicros,
		TotalHourlyCostMicros: resource.TotalHourlyMicros,
		DirectScaleEnabled:    resource.DirectScale,
	}
}

func byocPoolGPUs(req byocPoolRequest) []string {
	if req.gpu == "" || req.gpuCount == 0 {
		return nil
	}
	return []string{req.gpu}
}

func (s *Service) cleanupDeletedBYOCPoolIfNeeded(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) (bool, error) {
	if !s.byocPoolDeletedForCleanup(ctx, workspaceID, state, machines, now) && !byocPoolLooksDeleted(state, machines, now) {
		return false, nil
	}

	deleted := false
	err := s.withPoolStateLock(ctx, workspaceID, state.Name, func() error {
		fresh, err := s.getPrivatePoolState(ctx, workspaceID, state.Name)
		if err != nil || fresh == nil {
			return err
		}
		freshMachines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, fresh.Name)
		if err != nil {
			return err
		}
		if !s.byocPoolDeletedForCleanup(ctx, workspaceID, fresh, freshMachines, now) && !byocPoolLooksDeleted(fresh, freshMachines, now) {
			return nil
		}
		deleted = true
		return s.deleteBYOCPoolLocalState(ctx, workspaceID, fresh, freshMachines, "cloud_resource_deleted")
	})
	return deleted, err
}

func (s *Service) reconcileBYOCPoolMachinesForRead(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) ([]*model.AgentTokenState, bool, error) {
	deleted, err := s.cleanupDeletedBYOCPoolIfNeeded(ctx, workspaceID, state, machines, now)
	if err != nil || deleted {
		return nil, deleted, err
	}
	machines, err = s.pruneBYOCPoolMachinesAboveDesired(ctx, workspaceID, state, machines, now)
	return machines, false, err
}

func (s *Service) pruneBYOCPoolMachinesAboveDesired(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) ([]*model.AgentTokenState, error) {
	if state == nil || state.BYOC == nil || len(machines) == 0 {
		return machines, nil
	}
	provider, err := byocProviderForState(state)
	if err != nil {
		return machines, err
	}
	resource, err := provider.Resource(workspaceID, state, s.appConfig.ManagedCompute)
	if err != nil {
		return machines, err
	}
	desiredNodes := int(resource.DesiredNodes)
	if desiredNodes < 1 || len(machines) <= desiredNodes {
		return machines, nil
	}

	stale := staleBYOCMachinesAboveDesired(machines, desiredNodes, now)
	if len(stale) == 0 {
		return machines, nil
	}
	remove := map[string]bool{}
	for _, machine := range stale {
		if err := s.removePrivateMachine(ctx, machine); err != nil {
			return machines, err
		}
		remove[machine.MachineID] = true
	}
	kept := machines[:0]
	for _, machine := range machines {
		if machine == nil || remove[machine.MachineID] {
			continue
		}
		kept = append(kept, machine)
	}
	return kept, nil
}

func staleBYOCMachinesAboveDesired(machines []*model.AgentTokenState, desiredNodes int, now time.Time) []*model.AgentTokenState {
	excess := len(machines) - desiredNodes
	if excess <= 0 {
		return nil
	}
	candidates := make([]*model.AgentTokenState, 0, excess)
	for _, machine := range machines {
		if machine == nil || model.AgentMachineConnected(machine, now) {
			continue
		}
		candidates = append(candidates, machine)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := model.AgentMachineLastSeen(candidates[i])
		right := model.AgentMachineLastSeen(candidates[j])
		if left.Equal(right) {
			return candidates[i].MachineID < candidates[j].MachineID
		}
		if left.IsZero() {
			return false
		}
		if right.IsZero() {
			return true
		}
		return left.Before(right)
	})
	if len(candidates) > excess {
		candidates = candidates[:excess]
	}
	return candidates
}

func (s *Service) byocPoolDeletedForCleanup(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	resourceDeleted, err := s.byocProviderResourceDeleted(ctx, workspaceID, state)
	if resourceDeleted {
		return true
	}
	if !errors.Is(err, errBYOCProviderControlUnavailable) && !errors.Is(err, errBYOCProviderResourceNotFound) {
		return false
	}
	return byocProviderResourceUnavailableLooksDeleted(state, machines, now)
}

func byocPoolInProvisioningGrace(state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	if state == nil || state.BYOC == nil || len(machines) > 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if state.CreatedAt.IsZero() {
		return false
	}
	return now.Sub(state.CreatedAt) < byocProviderControlUnavailableCleanupGrace
}

func (s *Service) byocProviderResourceDeleted(ctx context.Context, workspaceID string, state *model.PoolState) (bool, error) {
	if state == nil || state.BYOC == nil {
		return false, nil
	}
	provider, err := byocProviderForState(state)
	if err != nil {
		return false, err
	}
	return provider.ResourceDeleted(ctx, byocProviderResourceInput{
		WorkspaceID:  workspaceID,
		Pool:         state,
		ProviderData: state.BYOC,
	})
}

func byocProviderResourceUnavailableLooksDeleted(state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	if byocPoolMachinesAllDisconnected(machines, now) {
		return true
	}
	if byocPoolLooksDeleted(state, machines, now) {
		return true
	}
	if byocPoolInProvisioningGrace(state, machines, now) || state == nil || state.BYOC == nil || len(machines) > 0 || state.CreatedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.Sub(state.CreatedAt) >= byocProviderControlUnavailableCleanupGrace
}

func byocPoolMachinesAllDisconnected(machines []*model.AgentTokenState, now time.Time) bool {
	if len(machines) == 0 {
		return false
	}
	for _, machine := range machines {
		if machine == nil || model.AgentMachineConnected(machine, now) || machine.LastDisconnectAt.IsZero() {
			return false
		}
	}
	return true
}

// Without provider credentials, external stack deletion is observable as all
// previously joined BYOC machines disconnecting and staying gone.
func byocPoolLooksDeleted(state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	if state == nil || state.BYOC == nil || len(machines) == 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	latestSeen := time.Time{}
	for _, machine := range machines {
		if model.AgentMachineConnected(machine, now) {
			return false
		}
		lastSeen := model.AgentMachineLastSeen(machine)
		if lastSeen.IsZero() {
			return false
		}
		if lastSeen.After(latestSeen) {
			latestSeen = lastSeen
		}
	}
	return now.Sub(latestSeen) >= byocDeletedPoolRetention
}

func (s *Service) deleteBYOCPoolLocalState(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, status string) error {
	for _, machine := range machines {
		if err := s.removePrivateMachine(ctx, machine); err != nil {
			return err
		}
	}
	if err := s.revokeBYOCBootstrapJoinTokens(ctx, state); err != nil {
		return err
	}
	if err := s.deletePrivatePoolState(ctx, workspaceID, state.Name); err != nil {
		return err
	}
	if s.scheduler != nil {
		s.scheduler.DeleteAgentPool(firstNonEmpty(state.Selector, state.Name))
	}
	event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolDeleted, status)
	event.MachineCount = uint32(len(machines))
	event.Message = "BYOC cloud resource is gone; cleaned up local pool state"
	s.emitComputeEvent(types.EventComputePool, event)
	return nil
}

func (s *Service) createOrUpdateBYOCPool(ctx context.Context, authInfo *auth.AuthInfo, provider byocProvider, req byocPoolRequest) (*model.PoolState, error) {
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	existing, err := s.getPrivatePoolState(ctx, workspaceID, req.poolName)
	if err != nil {
		return nil, err
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return nil, fmt.Errorf("pool already exists in this workspace")
	}
	if existing != nil && existing.Source.Canonical() != provider.Source() {
		return nil, fmt.Errorf("pool already exists with source %s", existing.Source)
	}
	if err := provider.ValidateExistingPool(existing, req); err != nil {
		return nil, err
	}
	if err := provider.ValidateSetupConfig(s.appConfig.ManagedCompute); err != nil {
		return nil, err
	}

	config := normalizePoolConfig(&pb.PoolConfig{
		Name:      req.poolName,
		Regions:   []string{req.region},
		Gpu:       byocPoolGPUs(req),
		Mode:      string(types.PoolModePrivate),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  defaultPrivatePriority,
	})
	if _, err := computePoolFromProto(config, 0, false); err != nil {
		return nil, err
	}

	now := time.Now()
	byocState := provider.PoolState(workspaceID, req, s.appConfig.ManagedCompute)
	if existing != nil {
		for _, label := range []string{byocBootstrapJoinTokenHashLabel, byocBootstrapJoinTokenHashesLabel} {
			value := byocLabelValue(existing.BYOC, label)
			if value == "" {
				continue
			}
			if byocState.Labels == nil {
				byocState.Labels = map[string]string{}
			}
			byocState.Labels[label] = value
		}
	}
	state := &model.PoolState{
		Name:             config.Name,
		Selector:         config.Selector,
		Config:           config,
		Status:           "active",
		Source:           provider.Source(),
		Mode:             config.Mode,
		Transport:        config.Transport,
		Fallback:         config.Fallback,
		Priority:         config.Priority,
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		UpdatedAt:        now,
		BYOC:             byocState,
	}
	if existing != nil {
		state.Reservations = existing.Reservations
		state.ReservedNodes = existing.ReservedNodes
		state.CommittedSpendMicros = existing.CommittedSpendMicros
		state.CreatedByTokenID = existing.CreatedByTokenID
		state.CreatedAt = existing.CreatedAt
		state.ExpiresAt = existing.ExpiresAt
	}
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return nil, err
	}
	if s.scheduler != nil {
		if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (s *Service) revokeBYOCBootstrapJoinTokens(ctx context.Context, state *model.PoolState) error {
	for _, tokenHash := range byocBootstrapJoinTokenHashes(state) {
		if err := s.revokeComputeJoinTokenHash(ctx, tokenHash); err != nil {
			return err
		}
	}
	return nil
}

func recordBYOCBootstrapJoinTokenHash(state *model.BYOCProviderState, tokenHash string) {
	tokenHash = strings.TrimSpace(tokenHash)
	if state == nil || tokenHash == "" {
		return
	}
	if state.Labels == nil {
		state.Labels = map[string]string{}
	}
	hashes := appendUniqueTokenHash(byocBootstrapJoinTokenHashesFromLabels(state.Labels), tokenHash)
	state.Labels[byocBootstrapJoinTokenHashLabel] = tokenHash
	state.Labels[byocBootstrapJoinTokenHashesLabel] = strings.Join(hashes, ",")
}

func byocBootstrapJoinTokenHashes(state *model.PoolState) []string {
	if state == nil || state.BYOC == nil {
		return nil
	}
	return byocBootstrapJoinTokenHashesFromLabels(state.BYOC.Labels)
}

func byocBootstrapJoinTokenHashesFromLabels(labels map[string]string) []string {
	if labels == nil {
		return nil
	}
	hashes := make([]string, 0, 2)
	for _, tokenHash := range strings.Split(labels[byocBootstrapJoinTokenHashesLabel], ",") {
		hashes = appendUniqueTokenHash(hashes, tokenHash)
	}
	return appendUniqueTokenHash(hashes, labels[byocBootstrapJoinTokenHashLabel])
}

func appendUniqueTokenHash(hashes []string, tokenHash string) []string {
	tokenHash = strings.TrimSpace(tokenHash)
	if tokenHash == "" {
		return hashes
	}
	for _, existing := range hashes {
		if existing == tokenHash {
			return hashes
		}
	}
	return append(hashes, tokenHash)
}

func byocLabelValue(state *model.BYOCProviderState, key string) string {
	if state == nil || state.Labels == nil {
		return ""
	}
	return strings.TrimSpace(state.Labels[key])
}

func byocProviderForState(state *model.PoolState) (byocProvider, error) {
	if state == nil || state.BYOC == nil {
		return nil, fmt.Errorf("missing BYOC provider metadata")
	}
	return byocProviderForName(byocProviderNameForState(state))
}

func byocProviderNameForState(state *model.PoolState) string {
	if state == nil || state.BYOC == nil {
		return ""
	}
	name := strings.TrimSpace(state.BYOC.Provider)
	if name != "" {
		return name
	}
	return string(state.Source.Canonical())
}

func byocPoolPhase(resource *byocProviderResource, pool *pb.PrivatePool) (string, string) {
	if resource == nil || pool == nil {
		return "unknown", "Waiting for provider status"
	}
	desiredNodes := resource.DesiredNodes
	if desiredNodes == 0 {
		desiredNodes = 1
	}
	switch {
	case pool.ReadyMachineCount >= desiredNodes:
		return "ready", "Pool is ready to run sandboxes"
	case pool.ReadyMachineCount > 0:
		return "partially_ready", fmt.Sprintf("%d/%d nodes connected", pool.ReadyMachineCount, desiredNodes)
	case pool.MachineCount > 0:
		return "nodes_booting", fmt.Sprintf("%d node%s detected", pool.MachineCount, pluralSuffix(pool.MachineCount))
	default:
		return "waiting_for_provider", "Waiting for cloud resources to launch nodes"
	}
}

func pluralSuffix(count uint32) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func byocProviderForName(name string) (byocProvider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return nil, fmt.Errorf("provider is required")
	case string(model.SourceAWS):
		return byocAWSProvider{}, nil
	default:
		return nil, fmt.Errorf("unsupported BYOC provider %q", name)
	}
}
