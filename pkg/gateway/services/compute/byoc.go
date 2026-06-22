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
	ValidateSetupConfig(types.ManagedComputeConfig) error
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
	Config       types.ManagedComputeConfig
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
	Provider         string
	AccountID        string
	Region           string
	ResourceName     string
	ResourceURL      string
	DestroyURL       string
	InstanceType     string
	DesiredNodes     uint32
	MaxNodes         uint32
	TargetSandboxes  uint32
	SandboxesPerNode uint32
}

type byocOnboardingResult struct {
	Pool         *model.PoolState
	SetupURL     string
	ResourceName string
	ResourceURL  string
}

func (s *Service) CreateBYOCPoolOnboarding(ctx context.Context, in *pb.CreateBYOCPoolOnboardingRequest) (*pb.CreateBYOCPoolOnboardingResponse, error) {
	if in == nil {
		return &pb.CreateBYOCPoolOnboardingResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	provider, err := byocProviderForName(in.Provider)
	if err != nil {
		return &pb.CreateBYOCPoolOnboardingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	result, err := s.createBYOCPoolOnboarding(ctx, provider, byocRawOnboardingRequest{
		PoolName:     in.PoolName,
		Region:       in.Region,
		InstanceType: in.InstanceType,
		DesiredNodes: in.DesiredNodes,
		MaxNodes:     in.MaxNodes,
		AccountID:    in.AccountId,
	})
	if err != nil {
		return &pb.CreateBYOCPoolOnboardingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateBYOCPoolOnboardingResponse{
		Ok:           true,
		Pool:         s.privatePoolStateToProto(result.Pool),
		SetupUrl:     result.SetupURL,
		ResourceName: result.ResourceName,
		ResourceUrl:  result.ResourceURL,
	}, nil
}

func (s *Service) GetBYOCPoolResource(ctx context.Context, in *pb.GetBYOCPoolResourceRequest) (*pb.GetBYOCPoolResourceResponse, error) {
	if in == nil {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, strings.TrimSpace(in.GetPoolName()))
	if err != nil {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	provider, err := byocProviderForName(string(state.Source.Canonical()))
	if err != nil {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	resource, err := provider.Resource(workspaceID, state)
	if err != nil {
		return &pb.GetBYOCPoolResourceResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetBYOCPoolResourceResponse{
		Ok:               true,
		Provider:         resource.Provider,
		AccountId:        resource.AccountID,
		Region:           resource.Region,
		ResourceName:     resource.ResourceName,
		ResourceUrl:      resource.ResourceURL,
		DestroyUrl:       resource.DestroyURL,
		InstanceType:     resource.InstanceType,
		DesiredNodes:     resource.DesiredNodes,
		MaxNodes:         resource.MaxNodes,
		TargetSandboxes:  resource.TargetSandboxes,
		SandboxesPerNode: resource.SandboxesPerNode,
	}, nil
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
	if in == nil {
		return &pb.GetBYOCPoolOnboardingStatusResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
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
	if existing != nil && existing.Source.Canonical() != provider.Source() {
		return nil, fmt.Errorf("pool already exists with source %s", existing.Source)
	}
	if err := provider.ValidateExistingPool(existing); err != nil {
		return nil, err
	}
	if err := provider.ValidateSetupConfig(s.appConfig.ManagedCompute); err != nil {
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
