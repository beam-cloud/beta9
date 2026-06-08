package compute

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/compute/solver"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *Service) ListPoolOffers(ctx context.Context, in *pb.ListPoolOffersRequest) (*pb.ListPoolOffersResponse, error) {
	pool, err := computePoolFromProto(normalizePoolConfig(in.Pool), false)
	if err != nil {
		return &pb.ListPoolOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	offers, err := s.collectPoolOffers(ctx, pool)
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

func (s *Service) LaunchPoolCapacity(ctx context.Context, in *pb.LaunchPoolCapacityRequest) (*pb.LaunchPoolCapacityResponse, error) {
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
	if err := s.validateAgentTransportConfig(config.Transport); err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	offers, err := s.collectPoolOffers(ctx, pool)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	existing, err := s.getPrivatePoolState(ctx, workspaceID, pool.Name)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	reservations := []model.Reservation{}
	if existing != nil {
		reservations = existing.Reservations
	}

	plan := solver.New().Solve(model.SolveInput{
		Demand: model.Demand{
			PoolName:       pool.Name,
			Selector:       pool.Selector,
			GPUs:           pool.GPUs,
			TotalGPUs:      pool.TotalGPUs,
			OfferID:        pool.OfferID,
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
		return launchPoolError("no_compatible_capacity", plan.Reason, billingDecision{}), nil
	}

	if planRequiresManagedCreate(plan.Actions) {
		decision, err := s.checkManagedLaunchCredit(ctx, workspaceID, pool.Name, plan)
		if err != nil {
			return launchPoolError(launchErrorBillingUnavailable, err.Error(), decision), nil
		}
		if !decision.OK {
			code := firstNonEmpty(decision.ErrorCode, launchErrorInsufficientCredit)
			msg := firstNonEmpty(decision.Message, "not enough credits to launch managed compute")
			return launchPoolError(code, msg, decision), nil
		}
	}

	bootstrapCommand, registrationToken, _, err := s.createPrivatePoolJoinCommandForOwner(ctx, workspaceID, ownerTokenID, pool.Name, "")
	if err != nil {
		return launchPoolError("bootstrap_failed", err.Error(), billingDecision{}), nil
	}

	vendors := s.computeVendors()
	newReservations := append([]model.Reservation{}, reservations...)
	createdReservations := []model.Reservation{}
	for _, action := range plan.Actions {
		if action.Type != model.ActionCreate {
			continue
		}
		vendor := vendors[action.Offer.Provider]
		if vendor == nil {
			return launchPoolError("provider_unavailable", fmt.Sprintf("vendor %q is not configured", action.Offer.Provider), billingDecision{}), nil
		}
		for i := uint32(0); i < action.Count; i++ {
			reservation, err := vendor.CreateReservation(ctx, model.ReservationRequest{
				PoolName:          pool.Name,
				Selector:          pool.Selector,
				Offer:             action.Offer,
				Count:             1,
				TTL:               pool.TTL,
				MaxSpendMicros:    pool.MaxSpendMicros,
				Source:            model.SourceCLIReservation,
				RegistrationToken: registrationToken,
				BootstrapCommand:  bootstrapCommand,
			})
			if err != nil {
				return launchPoolError("provider_failure", err.Error(), billingDecision{}), nil
			}
			if reservation == nil {
				return launchPoolError("provider_failure", "vendor returned empty reservation", billingDecision{}), nil
			}
			createdReservations = append(createdReservations, *reservation)
			newReservations = append(newReservations, *reservation)
		}
	}

	now := time.Now()
	state := &model.PoolState{
		Name:                 pool.Name,
		Selector:             pool.Selector,
		Config:               config,
		Reservations:         newReservations,
		ReservedGPUs:         plan.TotalGPUs,
		CommittedSpendMicros: plan.CommittedCostMicros,
		Status:               "active",
		Source:               model.SourceCLIReservation,
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
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		err = s.compensatePoolLaunchFailure(ctx, workspaceID, pool.Name, existing, vendors, createdReservations, err)
		return launchPoolError("store_failed", err.Error(), billingDecision{}), nil
	}
	if s.scheduler != nil {
		if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			err = s.compensatePoolLaunchFailure(ctx, workspaceID, pool.Name, existing, vendors, createdReservations, err)
			return launchPoolError("scheduler_failed", err.Error(), billingDecision{}), nil
		}
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolReserved, ""))

	return &pb.LaunchPoolCapacityResponse{Ok: true, Pool: privatePoolStateToProto(state)}, nil
}

func (s *Service) checkManagedLaunchCredit(ctx context.Context, workspaceID, poolName string, plan model.SolvePlan) (billingDecision, error) {
	if s.billing == nil {
		return billingDecision{}, errManagedBillingUnavailable
	}
	quantity := uint32(0)
	for _, action := range plan.Actions {
		if action.Type == model.ActionCreate {
			quantity += action.Count
		}
	}
	return s.billing.CheckLaunchCredit(ctx, billingCreditRequest{
		WorkspaceID:               workspaceID,
		PoolName:                  poolName,
		RequiredCents:             s.appConfig.ManagedCompute.Billing.MinimumCreditCentsOrDefault(),
		Quantity:                  quantity,
		EstimatedHourlyCostMicros: managedHourlyCostMicros(plan.Actions),
		EstimatedCommittedMicros:  plan.CommittedCostMicros,
	})
}

func planRequiresManagedCreate(actions []model.SolveAction) bool {
	for _, action := range actions {
		if action.Type == model.ActionCreate {
			return true
		}
	}
	return false
}

func managedHourlyCostMicros(actions []model.SolveAction) int64 {
	var total int64
	for _, action := range actions {
		if action.Type == model.ActionCreate {
			total += action.Offer.HourlyCostMicros * int64(action.Count)
		}
	}
	return total
}

func launchPoolError(code, msg string, decision billingDecision) *pb.LaunchPoolCapacityResponse {
	return &pb.LaunchPoolCapacityResponse{
		Ok:             false,
		ErrMsg:         msg,
		ErrorCode:      code,
		RequiredCents:  decision.RequiredCents,
		AvailableCents: decision.AvailableCents,
	}
}

func (s *Service) compensatePoolLaunchFailure(ctx context.Context, workspaceID, poolName string, previous *model.PoolState, vendors map[string]model.Vendor, reservations []model.Reservation, cause error) error {
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
		if err := s.savePrivatePoolState(ctx, workspaceID, previous); err != nil {
			failures = append(failures, fmt.Sprintf("restore previous pool state: %v", err))
		}
	} else if err := s.deletePrivatePoolState(ctx, workspaceID, poolName); err != nil {
		failures = append(failures, fmt.Sprintf("delete partial pool state: %v", err))
	}

	if len(failures) > 0 {
		return fmt.Errorf("%w; cleanup failed: %s", cause, strings.Join(failures, "; "))
	}
	return cause
}

func computeReservationInstanceID(reservation model.Reservation) string {
	return firstNonEmpty(reservation.InstanceID, reservation.ID)
}

func (s *Service) computeVendors() map[string]model.Vendor {
	vendors := map[string]model.Vendor{}
	if s.appConfig.Providers.Vast.ApiKey != "" {
		vendors["vast"] = model.NewVast(model.VastConfig{
			APIKey:  s.appConfig.Providers.Vast.ApiKey,
			BaseURL: s.appConfig.Providers.Vast.BaseURL,
		})
	}
	if s.appConfig.Providers.Shadeform.ApiKey != "" {
		vendors["shadeform"] = model.NewShadeform(model.ShadeformConfig{
			APIKey:  s.appConfig.Providers.Shadeform.ApiKey,
			BaseURL: s.appConfig.Providers.Shadeform.BaseURL,
		})
	}
	return vendors
}

func (s *Service) collectPoolOffers(ctx context.Context, pool model.Pool) ([]model.Offer, error) {
	vendors := s.computeVendors()
	if len(vendors) == 0 {
		return nil, fmt.Errorf("no compute GPU vendors are configured")
	}

	request := model.OfferRequest{
		GPUs:           pool.GPUs,
		TotalGPUs:      pool.TotalGPUs,
		OfferID:        pool.OfferID,
		Providers:      pool.Providers,
		Regions:        pool.Regions,
		MinReliability: pool.MinReliability,
	}
	offers := []model.Offer{}
	for _, name := range orderedComputeVendorNames(vendors, pool.Providers) {
		vendor := vendors[name]
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

func orderedComputeVendorNames(vendors map[string]model.Vendor, providers []string) []string {
	if len(providers) > 0 {
		names := make([]string, 0, len(providers))
		for _, provider := range providers {
			if _, ok := vendors[provider]; ok {
				names = append(names, provider)
			}
		}
		return names
	}

	names := make([]string, 0, len(vendors))
	for name := range vendors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
