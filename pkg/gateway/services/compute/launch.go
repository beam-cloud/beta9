package compute

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

func (s *Service) ListPoolOffers(ctx context.Context, in *pb.ListPoolOffersRequest) (*pb.ListPoolOffersResponse, error) {
	pool, err := computePoolFromProto(normalizePoolConfig(in.Pool), 0, false)
	if err != nil {
		return &pb.ListPoolOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	offers, err := s.collectPoolOffers(ctx, pool)
	if err != nil {
		return &pb.ListPoolOffersResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	sort.SliceStable(offers, func(i, j int) bool {
		return offers[i].HourlyCostMicros < offers[j].HourlyCostMicros
	})

	out := make([]*pb.PoolOffer, 0, len(offers))
	for _, offer := range offers {
		out = append(out, poolOfferToProto(s.billableOffer(offer)))
	}
	return &pb.ListPoolOffersResponse{Ok: true, Offers: out}, nil
}

func (s *Service) LaunchPoolCapacity(ctx context.Context, in *pb.LaunchPoolCapacityRequest) (*pb.LaunchPoolCapacityResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	actorTokenID := computeActorTokenID(authInfo)
	if workspaceID == "" || actorTokenID == "" {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	config := normalizePoolConfig(in.Pool)
	pool, err := computePoolFromProto(config, in.Nodes, true)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	config.Gpu = pool.GPUs
	config.Nodes = pool.Nodes
	if pool.Name == "" {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}
	if err := s.validateAgentTransportConfig(config.Transport); err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	for _, name := range []string{pool.Name, pool.Selector} {
		if _, exists := s.appConfig.Worker.Pools[name]; exists {
			return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q conflicts with a configured worker pool", name)}, nil
		}
	}

	offers, err := s.collectPoolOffers(ctx, pool)
	if err != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	// Serialize against the reconciler and release paths.
	var response *pb.LaunchPoolCapacityResponse
	lockErr := s.withPoolStateLock(ctx, workspaceID, pool.Name, func(lockCtx context.Context) error {
		response = s.launchPoolCapacityLocked(lockCtx, workspaceID, actorTokenID, config, pool, offers)
		return nil
	})
	if lockErr != nil {
		return &pb.LaunchPoolCapacityResponse{Ok: false, ErrMsg: lockErr.Error()}, nil
	}
	return response, nil
}

// launchPoolCapacityLocked buys the machines a launch asks for and records
// them in the pool, which it makes if there is none. A launch it cannot
// finish it undoes, once, and says why in its reply. The caller holds the
// pool lock.
func (s *Service) launchPoolCapacityLocked(ctx context.Context, workspaceID, actorTokenID string, config *pb.PoolConfig, pool model.Pool, offers []providers.Offer) *pb.LaunchPoolCapacityResponse {
	existing, refusal := s.poolForLaunch(ctx, workspaceID, pool)
	if refusal != nil {
		return refusal
	}
	if existing != nil {
		// Adding capacity to an existing pool must not rewrite the pool's
		// identity. The launch request only describes the new machine(s); the
		// region lives on the reservation, and the pool config accumulates.
		config = mergePoolConfigForLaunch(existing.Config, config)
	}
	plan, refusal := s.planLaunch(ctx, workspaceID, pool, offers, existing)
	if refusal != nil {
		return refusal
	}

	// The pool's record as the launch will save it, before what it buys is
	// added. The machines join with a token tied to when the pool was created.
	started := time.Now()
	state := nextPoolState(existing, config, actorTokenID, started)
	state.Source = model.SourceCLIReservation
	state.CreatedByTokenID = firstNonEmpty(state.CreatedByTokenID, actorTokenID)
	if state.CreatedAt.IsZero() {
		state.CreatedAt = started
	}

	bought, code, err := s.buyAndRecord(ctx, workspaceID, existing, state, pool, plan)
	if err != nil {
		err = s.compensatePoolLaunchFailure(ctx, workspaceID, existing, state, bought, err)
		return launchPoolError(code, err.Error(), billingDecision{})
	}
	s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolReserved, ""))
	return &pb.LaunchPoolCapacityResponse{Ok: true, Pool: s.privatePoolStateToProto(state)}
}

// poolForLaunch loads the pool a launch adds machines to, which is nil when
// the launch makes the pool, and refuses a pool that cannot take them.
func (s *Service) poolForLaunch(ctx context.Context, workspaceID string, pool model.Pool) (*model.PoolState, *pb.LaunchPoolCapacityResponse) {
	existing, err := s.getPrivatePoolState(ctx, workspaceID, pool.Name)
	if err != nil {
		return nil, launchPoolError("", err.Error(), billingDecision{})
	}
	if existing == nil {
		return nil, nil
	}
	if !poolStateIsPrivate(existing) {
		return nil, launchPoolError("", "pool already exists in this workspace", billingDecision{})
	}
	if existing.BYOC != nil {
		// Such a pool is scaled, billed and cleaned up as the tenant's cloud
		// resource. Machines Beam buys are looked after another way.
		return nil, launchPoolError("", fmt.Sprintf("pool %q runs on machines from your own cloud account, so machines cannot be reserved for it; reserve them in another pool", existing.Name), billingDecision{})
	}
	if err := validatePoolLaunchCompatible(existing, pool); err != nil {
		return nil, launchPoolError(launchErrorPoolGPUMismatch, err.Error(), billingDecision{})
	}
	return existing, nil
}

// planLaunch chooses the machines to buy. It refuses the launch when the
// offers do not add up to them, when they cost more than the pool may spend,
// and when the workspace does not have the credit for them.
func (s *Service) planLaunch(ctx context.Context, workspaceID string, pool model.Pool, offers []providers.Offer, existing *model.PoolState) (model.Plan, *pb.LaunchPoolCapacityResponse) {
	// The machines are chosen within what may be spent at the provider: the
	// tenant's limit less Beam's margin.
	budget := pool
	budget.MaxSpendMicros = s.providerBudgetMicros(pool.MaxSpendMicros)
	plan, err := model.Solve(budget, offers)
	if err != nil {
		return plan, launchPoolError(launchErrorNoCompatibleCapacity, err.Error(), billingDecision{})
	}
	if len(plan.Purchases) == 0 {
		return plan, nil
	}

	decision, err := s.checkManagedLaunchCredit(ctx, workspaceID, pool.Name, plan, existing)
	if err != nil {
		return plan, launchPoolError(launchErrorBillingUnavailable, err.Error(), decision)
	}
	if !decision.OK {
		code := firstNonEmpty(decision.ErrorCode, launchErrorInsufficientCredit)
		msg := firstNonEmpty(decision.Message, "not enough credits to launch managed compute")
		return plan, launchPoolError(code, msg, decision)
	}
	return plan, nil
}

// buyAndRecord buys the machines of a plan, adds them to the pool's record,
// saves it and registers the pool with the scheduler. When a step fails it
// returns the machines bought so far and the code of the step, for the caller
// to undo the launch.
func (s *Service) buyAndRecord(ctx context.Context, workspaceID string, existing, state *model.PoolState, pool model.Pool, plan model.Plan) ([]model.Reservation, string, error) {
	bought, code, err := s.createPlanReservations(ctx, workspaceID, pool, state.CreatedAt, plan)
	if err != nil {
		return bought, code, err
	}
	for i := range bought {
		if err := s.createMachineSSHState(ctx, workspaceID, pool.Name, bought[i].MachineID, &bought[i]); err != nil {
			return bought, launchErrorSSHBootstrapFailed, err
		}
	}

	now := time.Now()
	state.Reservations = append(append([]model.Reservation{}, state.Reservations...), bought...)
	state.ReservedNodes = activeReservationNodes(state.Reservations, now)
	state.CommittedSpendMicros = existingCommittedSpendMicros(existing) + s.billableMicros(plan.CostMicros)
	state.UpdatedAt = now
	// Adding a machine never shortens the pool's life.
	if expiresAt := now.Add(pool.TTL); expiresAt.After(state.ExpiresAt) {
		state.ExpiresAt = expiresAt
	}
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return bought, launchErrorStoreFailed, err
	}
	if s.scheduler != nil {
		if err := s.scheduler.EnsureAgentPool(workspaceID, state); err != nil {
			return bought, launchErrorSchedulerFailed, err
		}
	}
	return bought, "", nil
}

// machineTag is the tag a bought instance carries at its provider: the ID of
// the machine it was bought to be.
const machineTag = "beam-machine"

// reservationDeletionGrace is how far past a reservation's term its provider
// is told the machine is wanted. A provider that deletes machines by itself
// does so at the time it is told. Beam ends a reservation's machine itself:
// the provider's timer is for a machine Beam has lost track of, and must not
// fire while a renewal is late or a billing grace period holds the machine
// past its term.
const reservationDeletionGrace = 7 * 24 * time.Hour

// The codes a failed launch is reported with. Callers of the API read them,
// so their values stay as they are. billing.go has the two for credit.
const (
	launchErrorPoolGPUMismatch      = "pool_gpu_mismatch"
	launchErrorNoCompatibleCapacity = "no_compatible_capacity"
	launchErrorProviderUnavailable  = "provider_unavailable"
	launchErrorBootstrapFailed      = "bootstrap_failed"
	launchErrorProviderFailure      = "provider_failure"
	launchErrorSSHBootstrapFailed   = "ssh_bootstrap_failed"
	launchErrorStoreFailed          = "store_failed"
	launchErrorSchedulerFailed      = "scheduler_failed"
)

// createPlanReservations buys the machines a plan calls for. On failure it
// returns the reservations created so far, for the caller to give back. The
// last of them has no ID when the provider did not say whether it made the
// machine.
func (s *Service) createPlanReservations(ctx context.Context, workspaceID string, pool model.Pool, poolCreatedAt time.Time, plan model.Plan) ([]model.Reservation, string, error) {
	created := []model.Reservation{}
	for _, purchase := range plan.Purchases {
		provider, err := s.provider(purchase.Offer.Provider)
		if err != nil {
			return created, launchErrorProviderUnavailable, err
		}
		for i := uint32(0); i < purchase.Count; i++ {
			machineSeed, err := generateComputeToken()
			if err != nil {
				return created, launchErrorBootstrapFailed, err
			}
			machineID := model.ManagedMachineID(workspaceID, pool.Name, machineSeed)
			nodeName := model.ManagedNodeName(workspaceID, pool.Name, machineID)

			// The machine joins with a token that does not lapse: a provider
			// can take longer to boot a machine than any fixed time.
			bootstrapCommand, registrationToken, err := s.createPersistentPrivatePoolJoinCommandForWorkspace(ctx, workspaceID, pool.Name, poolCreatedAt, machineID)
			if err != nil {
				return created, launchErrorBootstrapFailed, err
			}
			now := time.Now()
			instance, err := provider.Create(ctx, providers.CreateRequest{
				Offer:         purchase.Offer,
				Name:          nodeName,
				StartupScript: bootstrapCommand,
				Tags:          map[string]string{machineTag: machineID},
				Until:         now.Add(pool.TTL).Add(reservationDeletionGrace),
			})
			if err != nil {
				_ = s.revokeComputeJoinTokenHash(ctx, hashComputeToken(registrationToken))
				if errors.Is(err, providers.ErrOutOfStock) || errors.Is(err, providers.ErrQuota) {
					return created, launchErrorProviderFailure, err
				}
				// Anything but a clear refusal may be an answer that was lost
				// on the way, with the machine made all the same. It is passed
				// on without an ID, for the cleanup to look for by its tag.
				instance = providers.Instance{}
			}
			reservation := newReservation(pool, machineID, nodeName, purchase.Offer, instance, now)
			reservation.RegistrationTokenHash = hashComputeToken(registrationToken)
			created = append(created, reservation)
			if err != nil {
				return created, launchErrorProviderFailure, err
			}
		}
	}
	return created, "", nil
}

// newReservation records a machine bought for a pool. What was bought comes
// from the offer, its state from the instance, and the terms from the launch.
func newReservation(pool model.Pool, machineID, name string, offer providers.Offer, instance providers.Instance, now time.Time) model.Reservation {
	status := model.ReservationPending
	if instance.Status == providers.StatusActive {
		status = model.ReservationActive
	}
	return model.Reservation{
		ID:                instance.ID,
		PoolName:          pool.Name,
		Name:              name,
		Selector:          pool.Selector,
		Provider:          offer.Provider,
		Cloud:             offer.Cloud,
		Region:            offer.Region,
		OfferID:           offer.ID,
		InstanceType:      offer.InstanceType,
		InstanceID:        instance.ID,
		MachineID:         machineID,
		GPU:               offer.GPU,
		GPUCount:          offer.GPUCount,
		NodeCount:         1,
		CPUMillicores:     offer.CPUMillicores,
		MemoryMB:          offer.MemoryMB,
		StorageMB:         offer.StorageMB,
		HourlyCostMicros:  offer.HourlyCostMicros,
		CommittedMicros:   offer.HourlyCostMicros * model.WholeHours(pool.TTL),
		Source:            model.SourceCLIReservation,
		Status:            status,
		CreatedAt:         now,
		ExpiresAt:         now.Add(pool.TTL),
		BillingRenewalAt:  now.Add(time.Hour),
		LastStatusMessage: instance.StatusDetail,
		PublicIP:          instance.PublicIP,
		SSHHost:           instance.SSHHost,
		SSHPort:           instance.SSHPort,
	}
}

// provider returns the provider configured under name, which is how a
// reservation and an offer say where a machine is bought.
func (s *Service) provider(name string) (providers.Provider, error) {
	provider, ok := s.providers.Get(name)
	if !ok {
		return nil, fmt.Errorf("provider %q is not configured", name)
	}
	return provider, nil
}

// deleteInstance deletes a reservation's machine at its provider. It succeeds
// when the machine is already gone.
func (s *Service) deleteInstance(ctx context.Context, reservation model.Reservation) error {
	provider, err := s.provider(reservation.Provider)
	if err != nil {
		return err
	}
	return provider.Delete(ctx, computeReservationInstanceID(reservation))
}

// instanceOf asks a reservation's provider how its machine is doing. A machine
// the provider no longer has is reported as deleted.
func (s *Service) instanceOf(ctx context.Context, reservation model.Reservation) (providers.Instance, error) {
	provider, err := s.provider(reservation.Provider)
	if err != nil {
		return providers.Instance{}, err
	}
	instance, err := provider.Get(ctx, computeReservationInstanceID(reservation))
	if errors.Is(err, providers.ErrNotFound) {
		return providers.Instance{Status: providers.StatusDeleted}, nil
	}
	return instance, err
}

// deleteLaunchedInstances deletes the machines of a launch that failed at
// their provider. It returns the machines it could not delete, marked as
// terminating for the reconciler to go on with, and why each could not be
// deleted.
func (s *Service) deleteLaunchedInstances(ctx context.Context, workspaceID, poolName string, reservations []model.Reservation) ([]model.Reservation, []string) {
	left := []model.Reservation{}
	failures := []string{}
	for _, reservation := range reservations {
		if reservation.ID == "" {
			instanceID, found := s.unansweredInstance(ctx, workspaceID, poolName, reservation)
			if !found {
				continue
			}
			reservation.ID, reservation.InstanceID = instanceID, instanceID
		}
		if err := s.deleteInstance(ctx, reservation); err != nil {
			reservationFailed(workspaceID, poolName, &reservation, "delete machine", err)
			markReservationTerminating(&reservation, reconcileReasonLaunchFailed, launchFailedMessage)
			left = append(left, reservation)
			failures = append(failures, fmt.Sprintf("delete reservation %q: %v", computeReservationInstanceID(reservation), err))
		}
	}
	return left, failures
}

// unansweredInstance looks once for the machine of a create that ended
// without an answer, which the provider may have made all the same. Such a
// machine is known only by its tag.
func (s *Service) unansweredInstance(ctx context.Context, workspaceID, poolName string, reservation model.Reservation) (string, bool) {
	logger := log.With().
		Str("workspace_id", workspaceID).
		Str("pool_name", poolName).
		Str("provider", reservation.Provider).
		Str("machine_id", reservation.MachineID).
		Logger()
	provider, ok := s.providers.Get(reservation.Provider)
	if !ok {
		return "", false
	}
	instances, err := provider.List(ctx)
	if err != nil {
		logger.Error().Err(err).Msgf("launch: could not look for a machine whose purchase went unanswered; if the provider made it, it carries the tag %s and has no record", machineTag)
		return "", false
	}
	for _, instance := range instances {
		if instance.Tags[machineTag] == reservation.MachineID {
			logger.Warn().Str("instance_id", instance.ID).Msg("launch: the provider made a machine whose purchase went unanswered; giving it back")
			return instance.ID, true
		}
	}
	logger.Info().Msg("launch: the provider made no machine for a purchase that went unanswered")
	return "", false
}

// validatePoolLaunchCompatible rejects launches that would change the node
// hardware profile; a pool should not mix CPU-only and GPU-attributed nodes.
func validatePoolLaunchCompatible(existing *model.PoolState, pool model.Pool) error {
	existingConfig := normalizePoolConfig(existing.Config)
	if existingConfig == nil {
		return nil
	}
	if existing.ReservedNodes > 0 && !existingPoolHasGPU(existing) && len(pool.GPUs) > 0 {
		return fmt.Errorf("pool %q is configured for CPU-only nodes; create a separate pool for nodes with GPU attributes", existing.Name)
	}
	if existingPoolHasGPU(existing) && pool.Nodes > 0 && len(pool.GPUs) == 0 {
		return fmt.Errorf("pool %q is configured for nodes with GPU attributes; create a separate pool for CPU-only nodes", existing.Name)
	}
	if len(existingConfig.Gpu) == 0 || len(pool.GPUs) == 0 {
		return nil
	}
	for _, gpu := range pool.GPUs {
		if !slices.Contains(existingConfig.Gpu, gpu) {
			return fmt.Errorf("pool %q is configured for %s node attributes; create a new pool for %s", existing.Name, strings.Join(existingConfig.Gpu, ", "), gpu)
		}
	}
	return nil
}

func existingPoolHasGPU(existing *model.PoolState) bool {
	config := normalizePoolConfig(existing.Config)
	if config != nil && len(config.Gpu) > 0 {
		return true
	}
	for _, reservation := range existing.Reservations {
		if reservation.GPUCount > 0 || reservation.GPU != "" {
			return true
		}
	}
	return false
}

// mergePoolConfigForLaunch combines an add-capacity request into the existing
// pool config: identity fields (name, gpu, transport, ...) are kept, regions
// and providers accumulate, and the budget grows by the request's budget.
func mergePoolConfigForLaunch(existing, request *pb.PoolConfig) *pb.PoolConfig {
	merged := normalizePoolConfig(existing)
	if merged == nil {
		return request
	}
	merged.Regions = unionStrings(merged.Regions, request.Regions)
	merged.Providers = unionStrings(merged.Providers, request.Providers)
	merged.MaxSpend += request.MaxSpend
	merged.Nodes += request.Nodes
	if merged.Ttl == "" {
		merged.Ttl = request.Ttl
	}
	return merged
}

func unionStrings(a, b []string) []string {
	out := append([]string(nil), a...)
	for _, value := range b {
		if !slices.Contains(out, value) {
			out = append(out, value)
		}
	}
	return out
}

func activeReservationNodes(reservations []model.Reservation, now time.Time) uint32 {
	var total uint32
	for _, reservation := range reservations {
		if reservation.ActiveAt(now) {
			if reservation.NodeCount > 0 {
				total += reservation.NodeCount
			} else {
				total++
			}
		}
	}
	return total
}

func existingCommittedSpendMicros(state *model.PoolState) int64 {
	if state == nil || state.CommittedSpendMicros < 0 {
		return 0
	}
	return state.CommittedSpendMicros
}

func (s *Service) checkManagedLaunchCredit(ctx context.Context, workspaceID, poolName string, plan model.Plan, existing *model.PoolState) (billingDecision, error) {
	if s.billing == nil {
		return billingDecision{}, errManagedBillingUnavailable
	}
	// Include outstanding commitments so back-to-back launches cannot each
	// pass the same balance.
	committedMicros := s.billableMicros(plan.CostMicros) + s.billableMicros(outstandingCommittedMicros(existing, time.Now().UTC()))
	return s.billing.CheckLaunchCredit(ctx, billingCreditRequest{
		WorkspaceID:               workspaceID,
		PoolName:                  poolName,
		RequiredCents:             s.appConfig.ManagedCompute.Billing.MinimumCreditCentsOrDefault(),
		Quantity:                  plan.Machines(),
		EstimatedHourlyCostMicros: s.billableMicros(plan.HourlyCostMicros()),
		EstimatedCommittedMicros:  committedMicros,
	})
}

// outstandingCommittedMicros estimates the provider cost still owed for the
// pool's open managed reservations.
func outstandingCommittedMicros(state *model.PoolState, now time.Time) int64 {
	if state == nil {
		return 0
	}
	var total int64
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || !reservation.ActiveAt(now) {
			continue
		}
		if reservation.ExpiresAt.IsZero() || reservation.HourlyCostMicros <= 0 {
			continue
		}
		total += reservation.HourlyCostMicros * model.WholeHours(reservation.ExpiresAt.Sub(now))
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

// launchCleanupTimeout bounds the calls to providers that clean up after a
// launch that failed. They run on a context of their own, because the caller
// may be gone, and must end while the pool lock is still held: it is leased
// for five minutes, and no longer renewed once the caller's context is done.
const launchCleanupTimeout = 2 * time.Minute

const launchFailedMessage = "the launch this machine was bought in failed"

// compensatePoolLaunchFailure undoes a launch that failed: it gives back the
// machines that were bought and puts the pool's record back as it was. A
// machine that cannot be given back stays in the record as terminating,
// where the reconciler finds it and goes on trying. next is the record the
// launch was going to save.
func (s *Service) compensatePoolLaunchFailure(ctx context.Context, workspaceID string, previous, next *model.PoolState, reservations []model.Reservation, cause error) error {
	// The caller's context may be why the launch failed. The cleanup is done
	// all the same.
	ctx = context.WithoutCancel(ctx)
	providerCtx, cancel := context.WithTimeout(ctx, launchCleanupTimeout)
	defer cancel()

	failures := []string{}
	for i := range reservations {
		reservation := &reservations[i]
		// Rolled-back reservations must not leave live registration tokens.
		s.revokeReservationJoinToken(ctx, reservation)
		if err := s.computeRepo.DeleteMachineSSHState(ctx, workspaceID, next.Name, reservation.MachineID); err != nil {
			failures = append(failures, fmt.Sprintf("delete SSH state for %q: %v", reservation.MachineID, err))
		}
	}
	left, undeleted := s.deleteLaunchedInstances(providerCtx, workspaceID, next.Name, reservations)
	failures = append(failures, undeleted...)

	restore := previous
	if len(left) > 0 {
		restore = poolWithLeftovers(previous, next, left)
	}
	if err := s.rollbackPoolState(ctx, workspaceID, next.Name, restore, nil); err != nil {
		failures = append(failures, fmt.Sprintf("restore pool state: %v", err))
	}

	if len(failures) > 0 {
		return fmt.Errorf("%w; cleanup failed: %s", cause, strings.Join(failures, "; "))
	}
	return cause
}

// poolWithLeftovers is the record of a pool after a launch that failed and
// could not give back every machine: the pool as it was, or else the pool the
// launch was making with nothing reserved, holding the machines that are left.
func poolWithLeftovers(previous, next *model.PoolState, left []model.Reservation) *model.PoolState {
	pool := *next
	pool.Reservations, pool.ReservedNodes, pool.CommittedSpendMicros = nil, 0, 0
	if previous != nil {
		pool = *previous
	}
	pool.Reservations = append(append([]model.Reservation{}, pool.Reservations...), left...)
	pool.UpdatedAt = time.Now()
	return &pool
}

func computeReservationInstanceID(reservation model.Reservation) string {
	return firstNonEmpty(reservation.InstanceID, reservation.ID)
}

// collectPoolOffers asks the pool's providers, or every provider when it
// names none, what they sell that the pool can use. A reservation is for a
// term, so it is never a spot machine, which its provider can take back.
//
// A provider that cannot be asked is passed over, so that it does not hide
// what the others sell. It is an error only when none of them answered.
func (s *Service) collectPoolOffers(ctx context.Context, pool model.Pool) ([]providers.Offer, error) {
	if len(s.providers.Names()) == 0 {
		return nil, fmt.Errorf("no compute providers are configured")
	}
	names := pool.Providers
	if len(names) == 0 {
		names = s.providers.Names()
	}

	filter := providers.OfferFilter{GPUs: pool.GPUs, Regions: pool.Regions}
	offers := []providers.Offer{}
	answered := 0
	failures := []string{}
	for _, name := range names {
		provider, ok := s.providers.Get(name)
		if !ok {
			continue
		}
		listed, err := provider.Offers(ctx, filter)
		if err != nil {
			log.Warn().Err(err).Str("provider", name).Msg("could not list what a provider sells; its offers are left out")
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		answered++
		for _, offer := range listed {
			if !offer.Spot && pool.MatchesOffer(offer) {
				offers = append(offers, offer)
			}
		}
	}
	if answered == 0 && len(failures) > 0 {
		return nil, fmt.Errorf("no compute provider could be asked what it sells: %s", strings.Join(failures, "; "))
	}
	return offers, nil
}
