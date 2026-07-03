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

const (
	defaultMarketplacePriority    = int32(100)
	defaultMarketplaceSource      = "operator"
	marketplaceErrMissingAuth     = "missing workspace auth"
	marketplaceErrListingNotFound = "listing not found"
)

var marketplaceListingStatuses = map[string]struct{}{
	model.MarketplaceListingStatusActive:   {},
	model.MarketplaceListingStatusInactive: {},
}

type marketplaceAuthContext struct {
	workspaceID  string
	ownerTokenID string
}

// marketplaceOfferStats aggregates the live agent machine states behind a
// listing into buyer-facing offer fields.
type marketplaceOfferStats struct {
	total        uint32
	ready        uint32
	cpuCores     uint32 // max across connected machines
	memoryMB     uint64 // max across connected machines
	diskGB       uint64 // max across connected machines
	freeGPUCount uint32 // sum across connected machines
	reliability  float32
}

func (s *Service) CreateMarketplaceListing(ctx context.Context, in *pb.CreateMarketplaceListingRequest) (*pb.CreateMarketplaceListingResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.CreateMarketplaceListingResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}

	listing, err := newMarketplaceListingState(authCtx.workspaceID, in)
	if err != nil {
		return &pb.CreateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	now := time.Now().UTC()
	listing.ID = model.MarketplaceListingID(authCtx.workspaceID, listing.DisplayName)
	listing.PoolName = model.MarketplacePoolName(firstNonEmpty(in.GetPoolName(), listing.GPU))
	listing.CreatedAt = now
	listing.UpdatedAt = now
	if err := s.validateMarketplacePoolAssignment(ctx, listing); err != nil {
		return &pb.CreateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if err := s.saveMarketplaceListing(ctx, listing, authCtx.ownerTokenID); err != nil {
		return &pb.CreateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateMarketplaceListingResponse{Ok: true, Listing: s.marketplaceListingToProto(ctx, listing)}, nil
}

func (s *Service) UpdateMarketplaceListing(ctx context.Context, in *pb.UpdateMarketplaceListingRequest) (*pb.UpdateMarketplaceListingResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.UpdateMarketplaceListingResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	listing, errMsg := s.marketplaceListing(ctx, authCtx.workspaceID, in.GetListingId())
	if errMsg != "" {
		return &pb.UpdateMarketplaceListingResponse{Ok: false, ErrMsg: errMsg}, nil
	}

	if err := applyMarketplaceListingUpdate(listing, in); err != nil {
		return &pb.UpdateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	// Updates can change the GPU type, so shared-pool invariants must hold
	// here just as on create.
	if err := s.validateMarketplacePoolAssignment(ctx, listing); err != nil {
		return &pb.UpdateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if err := s.saveMarketplaceListing(ctx, listing, authCtx.ownerTokenID); err != nil {
		return &pb.UpdateMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.UpdateMarketplaceListingResponse{Ok: true, Listing: s.marketplaceListingToProto(ctx, listing)}, nil
}

func (s *Service) DeleteMarketplaceListing(ctx context.Context, in *pb.DeleteMarketplaceListingRequest) (*pb.DeleteMarketplaceListingResponse, error) {
	// Deleting releases machines and tears down the pool, so it requires the
	// same owner-scoped auth as create/update.
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	listingID := strings.TrimSpace(in.GetListingId())
	listing, err := s.computeRepo.GetMarketplaceListing(ctx, authCtx.workspaceID, listingID)
	if err != nil {
		return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if listing == nil {
		return &pb.DeleteMarketplaceListingResponse{Ok: true}, nil
	}
	// A pool can back several listings (shared machine caches); only tear it
	// down with the last listing that references it.
	others, err := s.otherListingsSharingPool(ctx, listing)
	if err != nil {
		return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	poolShared := len(others) > 0
	if !poolShared {
		if err := s.releasePrivatePoolMachines(ctx, authCtx.workspaceID, listing.PoolName); err != nil {
			return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if err := s.deletePrivatePoolState(ctx, authCtx.workspaceID, listing.PoolName); err != nil {
			return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}
	if err := s.computeRepo.DeleteMarketplaceListing(ctx, authCtx.workspaceID, listing.ID); err != nil {
		return &pb.DeleteMarketplaceListingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if !poolShared && s.scheduler != nil {
		s.scheduler.DeleteAgentPool(listing.PoolName)
	}
	return &pb.DeleteMarketplaceListingResponse{Ok: true}, nil
}

// marketplaceListing resolves a listing owned by the workspace, returning a
// user-facing error message on failure. Shared by every listing-scoped RPC.
func (s *Service) marketplaceListing(ctx context.Context, workspaceID, rawListingID string) (*model.MarketplaceListingState, string) {
	listing, err := s.computeRepo.GetMarketplaceListing(ctx, workspaceID, strings.TrimSpace(rawListingID))
	if err != nil {
		return nil, err.Error()
	}
	if listing == nil {
		return nil, marketplaceErrListingNotFound
	}
	return listing, ""
}

// otherListingsSharingPool returns the workspace's other listings that point at
// the same pool as this one.
func (s *Service) otherListingsSharingPool(ctx context.Context, listing *model.MarketplaceListingState) ([]*model.MarketplaceListingState, error) {
	listings, err := s.computeRepo.ListMarketplaceListings(ctx, listing.SellerWorkspaceID, 0)
	if err != nil {
		return nil, err
	}
	shared := listings[:0]
	for _, other := range listings {
		if other != nil && other.ID != listing.ID && other.PoolName == listing.PoolName {
			shared = append(shared, other)
		}
	}
	return shared, nil
}

// validateMarketplacePoolAssignment guards the listing's pool choice: listings
// sharing a pool must sell the same GPU type (the pool schedules as one GPU
// class), and marketplace listings can never take over a non-marketplace pool.
func (s *Service) validateMarketplacePoolAssignment(ctx context.Context, listing *model.MarketplaceListingState) error {
	if listing.PoolName == "" {
		return fmt.Errorf("pool name is required")
	}
	poolState, err := s.computeRepo.GetPoolState(ctx, listing.SellerWorkspaceID, listing.PoolName)
	if err != nil {
		return err
	}
	if poolState != nil && poolState.Mode != string(types.PoolModeMarketplace) {
		return fmt.Errorf("pool %q is already used by a non-marketplace pool", listing.PoolName)
	}
	others, err := s.otherListingsSharingPool(ctx, listing)
	if err != nil {
		return err
	}
	for _, other := range others {
		if other.GPU != listing.GPU {
			return fmt.Errorf("pool %q hosts %s listings; pick a different pool for %s hardware", listing.PoolName, other.GPU, listing.GPU)
		}
	}
	return nil
}

func (s *Service) ListMarketplaceListings(ctx context.Context, in *pb.ListMarketplaceListingsRequest) (*pb.ListMarketplaceListingsResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasWorkspace() {
		return &pb.ListMarketplaceListingsResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	listings, err := s.computeRepo.ListMarketplaceListings(ctx, authCtx.workspaceID, int(in.GetLimit()))
	if err != nil {
		return &pb.ListMarketplaceListingsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := make([]*pb.MarketplaceListing, 0, len(listings))
	for _, listing := range listings {
		out = append(out, s.marketplaceListingToProto(ctx, listing))
	}
	return &pb.ListMarketplaceListingsResponse{Ok: true, Listings: out}, nil
}

func (s *Service) GetMarketplaceJoinCommand(ctx context.Context, in *pb.GetMarketplaceJoinCommandRequest) (*pb.GetMarketplaceJoinCommandResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.GetMarketplaceJoinCommandResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	listing, errMsg := s.marketplaceListing(ctx, authCtx.workspaceID, in.GetListingId())
	if errMsg != "" {
		return &pb.GetMarketplaceJoinCommandResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	if err := s.ensureMarketplacePoolState(ctx, listing, authCtx.ownerTokenID); err != nil {
		return &pb.GetMarketplaceJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	poolState, err := s.computeRepo.GetPoolState(ctx, listing.SellerWorkspaceID, listing.PoolName)
	if err != nil {
		return &pb.GetMarketplaceJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	ownerTokenID := authCtx.ownerTokenID
	if poolState != nil {
		ownerTokenID = firstNonEmpty(poolState.CreatedByTokenID, ownerTokenID)
	}
	token, tokenState, err := s.createMarketplaceJoinTokenState(ctx, listing, ownerTokenID, in.GetTtl())
	if err != nil {
		return &pb.GetMarketplaceJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	command := agentInstallCommand(gatewayURL, token, isLocalGatewayURL(gatewayURL), agentWorkerImage(s.appConfig))
	return &pb.GetMarketplaceJoinCommandResponse{
		Ok:        true,
		Command:   command,
		Token:     token,
		ExpiresAt: timestampOrNil(tokenState.ExpiresAt),
	}, nil
}

// ListMarketplaceOffers is the marketplace search: only public, active
// listings with ready machines are discoverable here. Unlisted listings are
// served exclusively through GetMarketplaceOffer (direct share links).
func (s *Service) ListMarketplaceOffers(ctx context.Context, in *pb.ListMarketplaceOffersRequest) (*pb.ListMarketplaceOffersResponse, error) {
	if in == nil {
		in = &pb.ListMarketplaceOffersRequest{}
	}
	listings, err := s.computeRepo.ListAllMarketplaceListings(ctx, int(in.GetLimit()))
	if err != nil {
		return &pb.ListMarketplaceOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	requestGPU := normalizeMarketplaceGPU(in.GetGpu())
	out := make([]*pb.MarketplaceOffer, 0, len(listings))
	for _, listing := range listings {
		if listing == nil || !listing.Public || listing.Status != model.MarketplaceListingStatusActive {
			continue
		}
		if requestGPU != "" && listing.GPU != requestGPU {
			continue
		}
		stats := s.marketplaceOfferStats(ctx, listing)
		if stats.ready == 0 {
			continue
		}
		out = append(out, marketplaceOfferProto(listing, stats))
	}
	return &pb.ListMarketplaceOffersResponse{Ok: true, Offers: out}, nil
}

// GetMarketplaceOffer resolves one active listing by id — the direct share
// link. Unlisted (non-public) listings are intentionally served here, like an
// unlisted video: anyone with the link can view it, but it never shows up in
// search. Offers without ready machines are still returned so a shared link
// can show the listing's current state.
func (s *Service) GetMarketplaceOffer(ctx context.Context, in *pb.GetMarketplaceOfferRequest) (*pb.GetMarketplaceOfferResponse, error) {
	listingID := strings.TrimSpace(in.GetListingId())
	if listingID == "" {
		return &pb.GetMarketplaceOfferResponse{Ok: false, ErrMsg: "listing id is required"}, nil
	}
	listing, err := s.computeRepo.GetMarketplaceListingByID(ctx, listingID)
	if err != nil {
		return &pb.GetMarketplaceOfferResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if listing == nil || listing.Status != model.MarketplaceListingStatusActive {
		return &pb.GetMarketplaceOfferResponse{Ok: false, ErrMsg: marketplaceErrListingNotFound}, nil
	}
	stats := s.marketplaceOfferStats(ctx, listing)
	return &pb.GetMarketplaceOfferResponse{Ok: true, Offer: marketplaceOfferProto(listing, stats)}, nil
}

func marketplaceOfferProto(listing *model.MarketplaceListingState, stats marketplaceOfferStats) *pb.MarketplaceOffer {
	return &pb.MarketplaceOffer{
		ListingId:            listing.ID,
		SellerWorkspaceId:    listing.SellerWorkspaceID,
		DisplayName:          listing.DisplayName,
		Gpu:                  listing.GPU,
		GpuCount:             listing.GPUCount,
		Source:               listing.Source,
		Preemptible:          listing.Preemptible,
		Public:               listing.Public,
		MachineCount:         stats.total,
		ReadyMachineCount:    stats.ready,
		Runtime:              marketplaceListingRuntime(listing),
		Region:               listing.Region,
		CpuCores:             stats.cpuCores,
		MemoryMb:             stats.memoryMB,
		DiskGb:               stats.diskGB,
		FreeGpuCount:         stats.freeGPUCount,
		Reliability:          stats.reliability,
		CreatedAt:            timestampOrNil(listing.CreatedAt),
		PricePerGpuHourCents: listing.PricePerGPUHourCents,
	}
}

func (s *Service) ListMarketplaceMachines(ctx context.Context, in *pb.ListMarketplaceMachinesRequest) (*pb.ListMarketplaceMachinesResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasWorkspace() {
		return &pb.ListMarketplaceMachinesResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	listing, errMsg := s.marketplaceListing(ctx, authCtx.workspaceID, in.GetListingId())
	if errMsg != "" {
		return &pb.ListMarketplaceMachinesResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, authCtx.workspaceID, listing.PoolName)
	if err != nil {
		return &pb.ListMarketplaceMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if limit := int(in.GetLimit()); limit > 0 && len(machines) > limit {
		machines = machines[:limit]
	}
	out := make([]*pb.Machine, 0, len(machines))
	for _, machine := range machines {
		out = append(out, s.agentMachineToProto(machine))
	}
	return &pb.ListMarketplaceMachinesResponse{Ok: true, Machines: out}, nil
}

func newMarketplaceListingState(workspaceID string, in *pb.CreateMarketplaceListingRequest) (*model.MarketplaceListingState, error) {
	if in == nil {
		in = &pb.CreateMarketplaceListingRequest{}
	}
	listing := &model.MarketplaceListingState{
		SellerWorkspaceID:    workspaceID,
		DisplayName:          strings.TrimSpace(in.GetDisplayName()),
		GPU:                  normalizeMarketplaceGPU(in.GetGpu()),
		GPUCount:             firstNonZeroUint32(in.GetGpuCount(), 1),
		Source:               firstNonEmpty(strings.TrimSpace(in.GetSource()), defaultMarketplaceSource),
		Preemptible:          in.GetPreemptible(),
		Public:               in.GetPublic(),
		Region:               strings.TrimSpace(in.GetRegion()),
		PricePerGPUHourCents: in.GetPricePerGpuHourCents(),
		Status:               model.MarketplaceListingStatusActive,
	}
	if err := validateMarketplaceListing(listing); err != nil {
		return nil, err
	}
	return listing, nil
}

func marketplaceAuthFromContext(ctx context.Context) marketplaceAuthContext {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	return marketplaceAuthContext{
		workspaceID:  computeWorkspaceID(authInfo),
		ownerTokenID: computeOwnerTokenID(authInfo),
	}
}

func (a marketplaceAuthContext) hasWorkspace() bool {
	return a.workspaceID != ""
}

func (a marketplaceAuthContext) hasOwner() bool {
	return a.workspaceID != "" && a.ownerTokenID != ""
}

func applyMarketplaceListingUpdate(listing *model.MarketplaceListingState, in *pb.UpdateMarketplaceListingRequest) error {
	if listing == nil {
		return fmt.Errorf("listing is required")
	}
	if in == nil {
		return fmt.Errorf("listing update is required")
	}
	if displayName := strings.TrimSpace(in.GetDisplayName()); displayName != "" {
		listing.DisplayName = displayName
	}
	if gpu := normalizeMarketplaceGPU(in.GetGpu()); gpu != "" {
		listing.GPU = gpu
	}
	if in.GetGpuCount() > 0 {
		listing.GPUCount = in.GetGpuCount()
	}
	if source := strings.TrimSpace(in.GetSource()); source != "" {
		listing.Source = source
	}
	if region := strings.TrimSpace(in.GetRegion()); region != "" {
		listing.Region = region
	}
	if status := strings.TrimSpace(in.GetStatus()); status != "" {
		if _, ok := marketplaceListingStatuses[status]; !ok {
			return fmt.Errorf("unsupported listing status")
		}
		listing.Status = status
	}
	if in.Preemptible != nil {
		listing.Preemptible = in.GetPreemptible()
	}
	if in.Public != nil {
		listing.Public = in.GetPublic()
	}
	if in.PricePerGpuHourCents != nil {
		listing.PricePerGPUHourCents = in.GetPricePerGpuHourCents()
	}
	listing.UpdatedAt = time.Now().UTC()
	return validateMarketplaceListing(listing)
}

func validateMarketplaceListing(listing *model.MarketplaceListingState) error {
	if listing == nil {
		return fmt.Errorf("listing is required")
	}
	if listing.SellerWorkspaceID == "" {
		return fmt.Errorf("seller workspace is required")
	}
	if listing.DisplayName == "" {
		return fmt.Errorf("display name is required")
	}
	if listing.GPU == "" || listing.GPU == string(types.NO_GPU) || listing.GPU == string(types.GPU_ANY) {
		return fmt.Errorf("gpu type is required")
	}
	if listing.GPUCount == 0 {
		return fmt.Errorf("gpu count must be greater than 0")
	}
	if listing.Source == "" {
		listing.Source = defaultMarketplaceSource
	}
	if listing.Status == "" {
		listing.Status = model.MarketplaceListingStatusActive
	}
	if _, ok := marketplaceListingStatuses[listing.Status]; !ok {
		return fmt.Errorf("unsupported listing status")
	}
	if listing.PoolName == "" && listing.ID != "" {
		listing.PoolName = model.MarketplacePoolName(firstNonEmpty(listing.GPU, listing.ID))
	}
	return nil
}

func normalizeMarketplaceGPU(value string) string {
	return string(types.NormalizeGPUType(strings.TrimSpace(value)))
}

func (s *Service) saveMarketplaceListing(ctx context.Context, listing *model.MarketplaceListingState, ownerTokenID string) error {
	if err := validateMarketplaceListing(listing); err != nil {
		return err
	}
	if err := s.computeRepo.SaveMarketplaceListing(ctx, listing); err != nil {
		return err
	}
	return s.ensureMarketplacePoolState(ctx, listing, ownerTokenID)
}

func (s *Service) ensureMarketplacePoolState(ctx context.Context, listing *model.MarketplaceListingState, ownerTokenID string) error {
	if listing == nil || listing.PoolName == "" {
		return fmt.Errorf("listing pool name is required")
	}
	now := time.Now().UTC()
	existing, err := s.computeRepo.GetPoolState(ctx, listing.SellerWorkspaceID, listing.PoolName)
	if err != nil {
		return err
	}
	createdAt := now
	createdBy := ownerTokenID
	if existing != nil {
		createdAt = existing.CreatedAt
		createdBy = firstNonEmpty(existing.CreatedByTokenID, ownerTokenID)
	}
	state := marketplacePoolState(listing, createdBy, createdAt, now)
	if existing != nil {
		state.Reservations = existing.Reservations
		state.ReservedNodes = existing.ReservedNodes
		state.CommittedSpendMicros = existing.CommittedSpendMicros
	}
	if err := s.computeRepo.SavePoolState(ctx, listing.SellerWorkspaceID, state); err != nil {
		return err
	}
	if s.scheduler != nil {
		return s.scheduler.RegisterAgentPool(listing.SellerWorkspaceID, state)
	}
	return nil
}

func marketplacePoolState(listing *model.MarketplaceListingState, createdBy string, createdAt, updatedAt time.Time) *model.PoolState {
	config := marketplacePoolConfig(listing)
	return &model.PoolState{
		Name:                 listing.PoolName,
		Selector:             listing.PoolName,
		Config:               config,
		Status:               listing.Status,
		Source:               model.SourceAttached,
		Mode:                 config.Mode,
		Transport:            config.Transport,
		Fallback:             config.Fallback,
		Priority:             config.Priority,
		Preemptible:          listing.Preemptible,
		MarketplaceListingID: listing.ID,
		SellerWorkspaceID:    listing.SellerWorkspaceID,
		CreatedByTokenID:     createdBy,
		CreatedAt:            createdAt,
		UpdatedAt:            updatedAt,
	}
}

func marketplacePoolConfig(listing *model.MarketplaceListingState) *pb.PoolConfig {
	return &pb.PoolConfig{
		Name:      listing.PoolName,
		Selector:  listing.PoolName,
		Gpu:       []string{listing.GPU},
		Mode:      string(types.PoolModeMarketplace),
		Transport: defaultPrivateTransport,
		Fallback:  types.PrivatePoolFallbackFail,
		Priority:  defaultMarketplacePriority,
	}
}

func marketplaceListingRuntime(listing *model.MarketplaceListingState) string {
	if listing == nil {
		return types.ContainerRuntimeGvisor.String()
	}
	return types.MarketplaceContainerRuntimeForGPU(listing.GPU)
}

func (s *Service) createMarketplaceJoinTokenState(ctx context.Context, listing *model.MarketplaceListingState, ownerTokenID, ttlValue string) (string, *model.JoinTokenState, error) {
	ttl, err := model.ParseTTL(ttlValue)
	if err != nil {
		return "", nil, err
	}
	if ttl == 0 {
		ttl = defaultPrivateJoinTTL
	}
	if ttl <= 0 {
		return "", nil, fmt.Errorf("join token ttl must be positive")
	}
	token, err := generateComputeToken()
	if err != nil {
		return "", nil, err
	}
	now := time.Now().UTC()
	tokenState := &model.JoinTokenState{
		TokenHash:            hashComputeToken(token),
		WorkspaceID:          listing.SellerWorkspaceID,
		PoolName:             listing.PoolName,
		CreatedByTokenID:     ownerTokenID,
		CreatedAt:            now,
		ExpiresAt:            now.Add(ttl),
		Mode:                 string(types.PoolModeMarketplace),
		MarketplaceListingID: listing.ID,
		SellerWorkspaceID:    listing.SellerWorkspaceID,
	}
	if err := s.saveComputeJoinTokenState(ctx, tokenState, ttl); err != nil {
		return "", nil, err
	}
	return token, tokenState, nil
}

func (s *Service) marketplaceListingToProto(ctx context.Context, listing *model.MarketplaceListingState) *pb.MarketplaceListing {
	if listing == nil {
		return nil
	}
	stats := s.marketplaceOfferStats(ctx, listing)
	return &pb.MarketplaceListing{
		Id:                   listing.ID,
		SellerWorkspaceId:    listing.SellerWorkspaceID,
		DisplayName:          listing.DisplayName,
		Gpu:                  listing.GPU,
		GpuCount:             listing.GPUCount,
		Source:               listing.Source,
		Preemptible:          listing.Preemptible,
		Public:               listing.Public,
		Status:               listing.Status,
		PoolName:             listing.PoolName,
		Region:               listing.Region,
		Runtime:              marketplaceListingRuntime(listing),
		CreatedAt:            timestampOrNil(listing.CreatedAt),
		UpdatedAt:            timestampOrNil(listing.UpdatedAt),
		MachineCount:         stats.total,
		ReadyMachineCount:    stats.ready,
		PricePerGpuHourCents: listing.PricePerGPUHourCents,
	}
}

func (s *Service) marketplaceOfferStats(ctx context.Context, listing *model.MarketplaceListingState) marketplaceOfferStats {
	stats := marketplaceOfferStats{}
	if s == nil || s.computeRepo == nil || listing == nil {
		return stats
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, listing.SellerWorkspaceID, listing.PoolName)
	if err != nil {
		return stats
	}
	stats.total = uint32(len(machines))
	now := time.Now()
	reliabilitySum := float64(0)
	readyMachineIDs := map[string]struct{}{}
	metricFreeGPUCount := uint32(0)
	for _, machine := range machines {
		if machine == nil {
			continue
		}
		reliabilitySum += marketplaceMachineReliability(machine, now)
		if !model.AgentMachineConnected(machine, now) {
			continue
		}
		stats.ready++
		readyMachineIDs[machine.MachineID] = struct{}{}
		if machine.CPUCount > stats.cpuCores {
			stats.cpuCores = machine.CPUCount
		}
		if machine.MemoryMB > stats.memoryMB {
			stats.memoryMB = machine.MemoryMB
		}
		if diskGB := machine.Metrics.DiskTotalMB / 1024; diskGB > stats.diskGB {
			stats.diskGB = diskGB
		}
		metricFreeGPUCount += machine.Metrics.FreeGPUCount
	}
	if freeGPUCount, ok := s.marketplaceWorkerFreeGPUCount(listing.PoolName, readyMachineIDs); ok {
		stats.freeGPUCount = freeGPUCount
	} else {
		stats.freeGPUCount = metricFreeGPUCount
	}
	if stats.total > 0 {
		stats.reliability = float32(reliabilitySum / float64(stats.total))
	}
	return stats
}

func (s *Service) marketplaceWorkerFreeGPUCount(poolName string, readyMachineIDs map[string]struct{}) (uint32, bool) {
	if s == nil || s.workerRepo == nil || poolName == "" || len(readyMachineIDs) == 0 {
		return 0, false
	}
	workers, err := s.workerRepo.GetAllWorkersInPool(poolName)
	if err != nil {
		return 0, false
	}
	var freeGPUCount uint32
	matched := false
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		if _, ok := readyMachineIDs[worker.MachineId]; !ok {
			continue
		}
		if worker.Status != "" && worker.Status != types.WorkerStatusAvailable {
			continue
		}
		matched = true
		freeGPUCount += worker.FreeGpuCount
	}
	return freeGPUCount, matched
}

// marketplaceMachineReliability scores a machine's current health in 0..1.
// This is a point-in-time signal, not historical uptime: connected machines
// score by heartbeat freshness (1.0 when fresh, decaying linearly toward 0.5
// as the heartbeat approaches the timeout) and disconnected machines score 0.
func marketplaceMachineReliability(machine *model.AgentTokenState, now time.Time) float64 {
	if !model.AgentMachineConnected(machine, now) {
		return 0
	}
	lastSeen := model.AgentMachineLastSeen(machine)
	age := now.Sub(lastSeen)
	if age <= 0 {
		return 1
	}
	freshness := 1 - age.Seconds()/model.AgentHeartbeatTimeout.Seconds()
	if freshness < 0 {
		freshness = 0
	}
	return 0.5 + 0.5*freshness
}
