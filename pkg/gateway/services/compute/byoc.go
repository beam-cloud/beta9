package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const byocJoinTokenTTL = 30 * 24 * time.Hour

// byocProvider owns provider-specific validation and setup links; the service
// owns auth, pool state, join tokens, events, and scheduler registration.
type byocProvider interface {
	Source() model.CapacitySource
	NormalizeOnboarding(byocRawOnboardingRequest) (byocPoolOnboardingRequest, error)
	PoolState(workspaceID string, req byocPoolOnboardingRequest) *model.BYOCProviderState
	Setup(context.Context, byocProviderSetupInput) (*byocProviderSetupResult, error)
	Resource(workspaceID string, state *model.PoolState) (*byocProviderResource, error)
	ValidateExistingPool(*model.PoolState) error
}

type byocRawOnboardingRequest struct {
	PoolName     string
	Region       string
	InstanceType string
	DesiredNodes uint32
	MaxNodes     uint32
	AccountID    string
}

type byocPoolOnboardingRequest struct {
	poolName     string
	region       string
	instanceType string
	desiredNodes uint32
	maxNodes     uint32
	accountID    string
}

type byocProviderSetupInput struct {
	WorkspaceID  string
	GatewayURL   string
	JoinToken    string
	WorkerImage  string
	Request      byocPoolOnboardingRequest
	Pool         *model.PoolState
	ProviderData *model.BYOCProviderState
}

type byocProviderSetupResult struct {
	SetupURL     string
	ResourceName string
	ResourceURL  string
	EventAttrs   map[string]string
}

type byocProviderResource struct {
	Provider     string
	AccountID    string
	Region       string
	ResourceName string
	ResourceURL  string
	DestroyURL   string
}

type byocOnboardingResult struct {
	Pool         *model.PoolState
	SetupURL     string
	ResourceName string
	ResourceURL  string
}

func (s *Service) createBYOCPoolOnboarding(ctx context.Context, provider byocProvider, raw byocRawOnboardingRequest) (*byocOnboardingResult, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	req, err := provider.NormalizeOnboarding(raw)
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

	joinToken, _, err := s.createPrivatePoolJoinTokenForOwner(
		ctx,
		workspaceID,
		ownerTokenID,
		req.poolName,
		byocJoinTokenTTL.String(),
		"",
	)
	if err != nil {
		return nil, err
	}

	setup, err := provider.Setup(ctx, byocProviderSetupInput{
		WorkspaceID:  workspaceID,
		GatewayURL:   strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/"),
		JoinToken:    joinToken,
		WorkerImage:  agentWorkerImage(s.appConfig),
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
		Status:      "byoc_onboarding",
		Source:      string(provider.Source()),
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		NodeCount:   req.desiredNodes,
		Attrs:       attrs,
	})

	return &byocOnboardingResult{
		Pool:         state,
		SetupURL:     setup.SetupURL,
		ResourceName: setup.ResourceName,
		ResourceURL:  setup.ResourceURL,
	}, nil
}

func (s *Service) GetBYOCPoolOnboardingStatus(ctx context.Context, in *pb.GetBYOCPoolOnboardingStatusRequest) (*pb.GetBYOCPoolOnboardingStatusResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.GetBYOCPoolOnboardingStatusResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, strings.TrimSpace(in.GetPoolName()))
	if err != nil {
		return &pb.GetBYOCPoolOnboardingStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.GetBYOCPoolOnboardingStatusResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.GetBYOCPoolOnboardingStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	pool := s.privatePoolStateToProtoWithMachines(state, machines)
	return &pb.GetBYOCPoolOnboardingStatusResponse{
		Ok:                true,
		Pool:              pool,
		Ready:             pool.ReadyMachineCount > 0,
		ReadyMachineCount: pool.ReadyMachineCount,
		MachineCount:      pool.MachineCount,
	}, nil
}

func (s *Service) getBYOCProviderResource(ctx context.Context, authInfo *auth.AuthInfo, provider byocProvider, poolName string) (*byocProviderResource, error) {
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, strings.TrimSpace(poolName))
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("pool not found")
	}
	if state.Source.Canonical() != provider.Source() {
		return nil, fmt.Errorf("pool is not a %s BYOC pool", provider.Source())
	}
	return provider.Resource(workspaceID, state)
}

func (s *Service) createOrUpdateBYOCPool(ctx context.Context, authInfo *auth.AuthInfo, provider byocProvider, req byocPoolOnboardingRequest) (*model.PoolState, error) {
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	existing, err := s.getPrivatePoolState(ctx, workspaceID, req.poolName)
	if err != nil {
		return nil, err
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return nil, fmt.Errorf("pool already exists in this workspace")
	}
	if existing != nil && existing.Source != "" && existing.Source.Canonical() != provider.Source() {
		return nil, fmt.Errorf("pool already exists with source %s", existing.Source)
	}
	if err := provider.ValidateExistingPool(existing); err != nil {
		return nil, err
	}

	config := normalizePoolConfig(&pb.PoolConfig{
		Name:      req.poolName,
		Regions:   []string{req.region},
		Mode:      string(types.PoolModePrivate),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  defaultPrivatePriority,
	})
	if _, err := computePoolFromProto(config, 0, false); err != nil {
		return nil, err
	}

	now := time.Now()
	byocState := provider.PoolState(workspaceID, req)
	if existing != nil {
		mergeBYOCProviderState(byocState, existing.BYOC)
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

func mergeBYOCProviderState(next, existing *model.BYOCProviderState) {
	if next == nil || existing == nil {
		return
	}
	if next.AccountID == "" {
		next.AccountID = existing.AccountID
	}
	if next.Region == "" {
		next.Region = existing.Region
	}
	if next.ResourceName == "" {
		next.ResourceName = existing.ResourceName
	}
	if next.ResourceURL == "" {
		next.ResourceURL = existing.ResourceURL
	}
	if next.DestroyURL == "" {
		next.DestroyURL = existing.DestroyURL
	}
	if len(next.Labels) == 0 {
		next.Labels = existing.Labels
	}
}
