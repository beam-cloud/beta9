package compute

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// onDemandPoolPrefix names the managed pool that holds failover hardware
	// for one GPU type, e.g. "ondemand-a10g".
	onDemandPoolPrefix = "ondemand-"

	// onDemandPoolCreator marks pools the failover system owns, distinguishing
	// them from pools an operator created through the admin API.
	onDemandPoolCreator = "scheduling.failover"

	// onDemandIdleReason is the termination reason recorded on scale-down.
	onDemandIdleReason = "ondemand_idle"

	// onDemandReservationTTL is the vendor-side ceiling on a machine's life, a
	// backstop against leaking hardware if this loop stops running. Idle
	// scale-down normally reclaims capacity long before it.
	onDemandReservationTTL = 24 * time.Hour

	// defaultOnDemandScaleDownAfterIdle is used when no idle window is
	// configured; short enough to stop paying for nothing, long enough to
	// absorb gaps between bursts.
	defaultOnDemandScaleDownAfterIdle = 10 * time.Minute
)

// reconcileOnDemandFailover keeps the control plane's on-demand failover
// capacity in step with demand the scheduler recorded. One tick of the existing
// managed-compute reconcile loop owns the whole lifecycle: account for spend,
// terminate idle machines, then reserve new ones for GPU types whose serverless
// capacity is exhausted, within the cluster budget.
func (s *Service) reconcileOnDemandFailover(ctx context.Context, now time.Time) error {
	failover := s.appConfig.Scheduling.Failover
	if !failover.Enabled || s.computeRepo == nil || s.managedPoolRepo == nil {
		return nil
	}
	gpus := onDemandGPUs(failover)
	if len(gpus) == 0 {
		return nil
	}

	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return err
	}

	demand, err := s.computeRepo.ListFailoverDemand(ctx)
	if err != nil {
		return err
	}
	demandByGPU := map[string]*model.FailoverDemand{}
	for _, record := range demand {
		if record != nil {
			demandByGPU[strings.ToUpper(record.GPU)] = record
		}
	}

	var errs []error
	for _, gpu := range gpus {
		if err := s.reconcileOnDemandPool(ctx, workspaceID, gpu, failover, demandByGPU[strings.ToUpper(gpu)], now); err != nil {
			errs = append(errs, fmt.Errorf("on-demand pool for %s: %w", gpu, err))
		}
	}
	return errors.Join(errs...)
}

func onDemandGPUs(failover types.FailoverConfig) []string {
	gpus := make([]string, 0, len(failover.Chains))
	for gpu, chain := range failover.Chains {
		if chain.OnDemand != nil {
			gpus = append(gpus, gpu)
		}
	}
	sort.Strings(gpus)
	return gpus
}

func onDemandPoolName(gpu string) string {
	return onDemandPoolPrefix + strings.ToLower(gpu)
}

// reconcileOnDemandPool runs the full lifecycle for one GPU type's pool under
// the managed pool lock, which makes it single-writer and safe to run on every
// gateway replica.
func (s *Service) reconcileOnDemandPool(ctx context.Context, workspaceID, gpu string, failover types.FailoverConfig, demand *model.FailoverDemand, now time.Time) error {
	step := failover.Chains[gpu].OnDemand
	if step == nil {
		return nil
	}
	poolName := onDemandPoolName(gpu)

	return s.withManagedPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		state, err := s.onDemandPoolState(lockCtx, workspaceID, gpu, poolName, now)
		if err != nil || state == nil {
			return err
		}

		changed := s.recordOnDemandSpend(lockCtx, state, now)
		changed = s.reclaimOnDemandMachines(lockCtx, workspaceID, state, failover, demand != nil, now) || changed

		if demand != nil {
			launched, err := s.launchOnDemandCapacity(lockCtx, workspaceID, state, gpu, step, failover, now)
			changed = launched || changed
			if err != nil {
				log.Warn().Err(err).Str("pool_name", poolName).Msg("failed to launch on-demand failover capacity")
			}
			if launched {
				// Demand is satisfied for now; the scheduler rewrites the
				// record if requests are still waiting after the machine boots.
				if err := s.computeRepo.DeleteFailoverDemand(lockCtx, demand.GPU); err != nil {
					log.Warn().Err(err).Str("gpu", demand.GPU).Msg("failed to clear on-demand demand record")
				}
			}
		}

		if !changed {
			return nil
		}
		state.Reservations = pruneClosedOnDemandReservations(state.Reservations)
		state.ReservedNodes = activeReservationNodes(state.Reservations, now)
		state.UpdatedAt = now
		return s.managedPoolRepo.SaveManagedPoolState(lockCtx, workspaceID, state)
	})
}

// onDemandPoolState returns the managed pool that holds this GPU type's
// failover hardware, creating it on first use. It is an ordinary
// control-plane-managed pool, so its machines are schedulable by every
// workspace exactly like configured serverless capacity.
func (s *Service) onDemandPoolState(ctx context.Context, workspaceID, gpu, poolName string, now time.Time) (*model.PoolState, error) {
	existing, err := s.managedPoolRepo.GetManagedPoolState(ctx, workspaceID, poolName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.ManagementSource != types.WorkerPoolManagementSourceAPI {
			return nil, fmt.Errorf("pool %q is not control-plane managed", poolName)
		}
		return existing, nil
	}
	if _, conflict := s.appConfig.Worker.Pools[poolName]; conflict {
		return nil, fmt.Errorf("pool %q conflicts with a configured worker pool", poolName)
	}

	config, err := normalizeManagedPoolConfig(types.WorkerPoolConfig{
		GPUType: gpu,
		Mode:    types.PoolModeExternal,
	})
	if err != nil {
		return nil, err
	}
	state := newManagedPoolState(workspaceID, poolName, types.WorkerPoolManagementSourceAPI, onDemandPoolCreator, config, now)
	if err := s.managedPoolRepo.SaveManagedPoolState(ctx, workspaceID, state); err != nil {
		return nil, err
	}
	return state, nil
}

// launchOnDemandCapacity reserves one machine when the budget allows it. One
// machine per tick keeps the loop conservative: capacity arrives within a
// minute of demand persisting, and a burst never overshoots by more than the
// reconcile interval.
func (s *Service) launchOnDemandCapacity(ctx context.Context, workspaceID string, state *model.PoolState, gpu string, step *types.FailoverOnDemandStep, failover types.FailoverConfig, now time.Time) (bool, error) {
	active := activeOnDemandReservations(state, now)
	if step.MaxNodes > 0 && len(active) >= step.MaxNodes {
		return false, nil
	}

	headroomCents, ok, err := s.onDemandBudgetHeadroom(ctx, state, failover, now)
	if err != nil || !ok {
		return false, err
	}
	// The headroom becomes the solver's and the vendor's spend ceiling, so an
	// offer the budget cannot afford is never chosen in the first place.
	headroomMicros := types.CentsToMicros(headroomCents)

	pool := model.Pool{
		Name:           state.Name,
		Selector:       state.Name,
		GPUs:           onDemandStepGPUs(gpu, step),
		Nodes:          1,
		TTL:            onDemandReservationTTL,
		Providers:      onDemandStepProviders(step),
		MaxSpendMicros: headroomMicros,
	}
	offers, err := s.collectPoolOffers(ctx, pool)
	if err != nil {
		return false, err
	}

	plan := model.NewSolver().Solve(model.SolveInput{
		Demand: model.Demand{
			PoolName:       pool.Name,
			Selector:       pool.Selector,
			GPUs:           pool.GPUs,
			Nodes:          pool.Nodes,
			TTL:            pool.TTL,
			Providers:      pool.Providers,
			MaxSpendMicros: pool.MaxSpendMicros,
		},
		Offers: offers,
		Now:    now,
	})
	if !plan.Feasible {
		return false, fmt.Errorf("no compatible on-demand capacity: %s", plan.Reason)
	}

	created, code, err := s.createPlanReservations(ctx, workspaceID, plan, s.computeVendors(), poolLaunchSpec{
		poolName:       state.Name,
		selector:       state.Name,
		ttl:            pool.TTL,
		maxSpendMicros: headroomMicros,
		bootstrap: func(ctx context.Context, machineID string) (string, string, error) {
			return s.onDemandJoinCommand(ctx, state, machineID)
		},
	})
	if err != nil {
		if failures := s.releaseReservations(ctx, s.computeVendors(), created); len(failures) > 0 {
			return false, fmt.Errorf("%s: %w; cleanup failed: %s", code, err, strings.Join(failures, "; "))
		}
		return false, fmt.Errorf("%s: %w", code, err)
	}
	if len(created) == 0 {
		return false, nil
	}

	for i := range created {
		created[i].BillingCursorAt = now
	}
	state.Reservations = append(state.Reservations, created...)

	for _, reservation := range created {
		s.emitOnDemandEvent(workspaceID, state, types.EventComputeActionOnDemandCreated, now, map[string]string{
			types.EventComputeAttrGPU:             gpu,
			types.EventComputeAttrProvider:        reservation.Provider,
			types.EventComputeAttrNodeCount:       fmt.Sprintf("%d", max(reservation.NodeCount, 1)),
			types.EventComputeAttrHourlyCostCents: fmt.Sprintf("%d", types.MicrosToCents(reservation.HourlyCostMicros)),
		})
	}
	return true, nil
}

// onDemandJoinCommand mints the same persistent managed-pool join token an
// operator would receive for a manually installed machine, so a vendor-created
// node bootstraps through the identical agent install path.
func (s *Service) onDemandJoinCommand(ctx context.Context, state *model.PoolState, machineID string) (string, string, error) {
	if state.ManagedInstanceID == "" {
		return "", "", fmt.Errorf("pool %q has no instance identity", state.Name)
	}
	token, tokenState, err := newPoolJoinToken(state.WorkspaceID, state.Name, state.CreatedAt, 0, machineID)
	if err != nil {
		return "", "", err
	}
	tokenState.Mode = string(types.PoolModeExternal)
	tokenState.ManagedPoolInstanceID = state.ManagedInstanceID
	if err := s.savePoolJoinToken(ctx, tokenState, 0); err != nil {
		return "", "", err
	}
	return s.joinCommandForToken(token), token, nil
}

// onDemandBudgetHeadroom reports how much hourly spend the cluster budget still
// allows, in cents, where 0 means unlimited. It returns false once either
// ceiling is reached: the concurrent burn rate across live failover hardware, or
// the rolling 24h spend. Operators declare budgets in cents; reservation costs
// are micro-dollars, converted at this boundary.
func (s *Service) onDemandBudgetHeadroom(ctx context.Context, state *model.PoolState, failover types.FailoverConfig, now time.Time) (int64, bool, error) {
	budget := failover.OnDemand.Budget

	if budget.MaxDailyCents > 0 {
		spentCents, err := s.computeRepo.OnDemandSpendCents(ctx, 24*time.Hour)
		if err != nil {
			return 0, false, err
		}
		if int64(spentCents) >= budget.MaxDailyCents {
			s.emitOnDemandEvent(state.WorkspaceID, state, types.EventComputeActionOnDemandBudgetExhausted, now, map[string]string{
				types.EventComputeAttrBudgetDailyCents:    fmt.Sprintf("%.0f", spentCents),
				types.EventComputeAttrBudgetDailyMaxCents: fmt.Sprintf("%d", budget.MaxDailyCents),
			})
			return 0, false, nil
		}
	}

	if budget.MaxHourlyCents <= 0 {
		return 0, true, nil
	}

	clusterHourlyCents, err := s.onDemandHourlyCents(ctx, now)
	if err != nil {
		return 0, false, err
	}
	headroomCents := budget.MaxHourlyCents - clusterHourlyCents
	if headroomCents <= 0 {
		s.emitOnDemandEvent(state.WorkspaceID, state, types.EventComputeActionOnDemandBudgetExhausted, now, map[string]string{
			types.EventComputeAttrBudgetHourlyCents:    fmt.Sprintf("%d", clusterHourlyCents),
			types.EventComputeAttrBudgetHourlyMaxCents: fmt.Sprintf("%d", budget.MaxHourlyCents),
		})
		return 0, false, nil
	}
	return headroomCents, true, nil
}

// onDemandHourlyCents is the committed burn rate across every on-demand pool,
// so one GPU type's chain cannot spend the whole cluster budget twice.
func (s *Service) onDemandHourlyCents(ctx context.Context, now time.Time) (int64, error) {
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return 0, err
	}
	states, err := s.managedPoolRepo.ListManagedPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return 0, err
	}

	micros := int64(0)
	for _, state := range states {
		if state == nil || !strings.HasPrefix(state.Name, onDemandPoolPrefix) {
			continue
		}
		for _, reservation := range activeOnDemandReservations(state, now) {
			micros += reservation.HourlyCostMicros
		}
	}
	return types.MicrosToCents(micros), nil
}

// recordOnDemandSpend accumulates what the platform has actually spent on
// failover hardware into the rolling hourly buckets the budget reads.
func (s *Service) recordOnDemandSpend(ctx context.Context, state *model.PoolState, now time.Time) bool {
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.HourlyCostMicros <= 0 {
			continue
		}
		start := reservation.BillingCursorAt
		if start.IsZero() {
			start = reservation.CreatedAt
		}
		end := now
		if !reservation.ActiveAt(now) {
			// A closed reservation stops accruing at its expiry.
			if reservation.ExpiresAt.IsZero() || reservation.ExpiresAt.After(now) {
				continue
			}
			end = reservation.ExpiresAt
		}
		if start.IsZero() || !end.After(start) {
			continue
		}

		cents := managedCostCents(reservation.HourlyCostMicros, end.Sub(start))
		if err := s.computeRepo.RecordOnDemandSpend(ctx, now, cents); err != nil {
			log.Warn().Err(err).Str("pool_name", state.Name).Msg("failed to record on-demand spend")
			continue
		}
		reservation.BillingCursorAt = end
		changed = true
	}
	return changed
}

// reclaimOnDemandMachines releases failover hardware the platform should stop
// paying for: reservations past their TTL, and machines with no work on them for
// the configured idle window while nothing waits for this GPU type.
func (s *Service) reclaimOnDemandMachines(ctx context.Context, workspaceID string, state *model.PoolState, failover types.FailoverConfig, hasDemand bool, now time.Time) bool {
	idleWindow := failover.OnDemand.ScaleDownAfterIdle
	if idleWindow <= 0 {
		idleWindow = defaultOnDemandScaleDownAfterIdle
	}

	vendors := s.computeVendors()
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.Status != model.ReservationActive {
			continue
		}

		// An expired reservation has already been torn down vendor-side, but
		// its join token and machine record are still ours to clean up.
		if !reservation.ActiveAt(now) {
			changed = s.terminateOnDemandReservation(ctx, workspaceID, state, reservation, vendors, reconcileReasonReservationExpired, "on-demand failover reservation expired", now) || changed
			continue
		}

		if hasDemand || s.onDemandMachineBusy(ctx, workspaceID, state.Name, reservation) {
			if !reservation.IdleSince.IsZero() {
				reservation.IdleSince = time.Time{}
				changed = true
			}
			continue
		}
		if reservation.IdleSince.IsZero() {
			reservation.IdleSince = now
			changed = true
			continue
		}
		if now.Sub(reservation.IdleSince) < idleWindow {
			continue
		}

		changed = s.terminateOnDemandReservation(ctx, workspaceID, state, reservation, vendors, onDemandIdleReason, "on-demand failover capacity idle", now) || changed
	}
	return changed
}

func (s *Service) terminateOnDemandReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, vendors map[string]model.Vendor, reason, message string, now time.Time) bool {
	if !s.terminateReservation(ctx, workspaceID, state, reservation, vendors, reason, message) {
		return false
	}

	s.releaseOnDemandMachine(ctx, workspaceID, state.Name, reservation.MachineID)
	s.emitOnDemandEvent(workspaceID, state, types.EventComputeActionOnDemandTerminated, now, map[string]string{
		types.EventComputeAttrGPU:             reservation.GPU,
		types.EventComputeAttrProvider:        reservation.Provider,
		types.EventComputeAttrReasons:         reason,
		types.EventComputeAttrHourlyCostCents: fmt.Sprintf("%d", types.MicrosToCents(reservation.HourlyCostMicros)),
	})
	return true
}

// releaseOnDemandMachine drops the machine record of a terminated reservation
// so the pool does not keep a dead worker around for the stale-machine
// retention window.
func (s *Service) releaseOnDemandMachine(ctx context.Context, workspaceID, poolName, machineID string) {
	if machineID == "" || s.computeRepo == nil {
		return
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	if err != nil || machine == nil {
		return
	}
	if err := s.removePrivateMachine(ctx, machine); err != nil {
		log.Warn().Err(err).Str("pool_name", poolName).Str("machine_id", machineID).Msg("failed to release on-demand machine")
	}
}

// onDemandMachineBusy reports whether a reservation's machine is running
// containers. A machine that has not joined yet counts as busy: it was
// reserved for waiting work and must be given time to arrive.
func (s *Service) onDemandMachineBusy(ctx context.Context, workspaceID, poolName string, reservation *model.Reservation) bool {
	if reservation.MachineID == "" || s.computeRepo == nil || s.containerRepo == nil {
		return true
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, poolName, reservation.MachineID)
	if err != nil || machine == nil {
		return true
	}
	containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(reservation.MachineID))
	if err != nil {
		return true
	}
	return len(containers) > 0
}

func activeOnDemandReservations(state *model.PoolState, now time.Time) []model.Reservation {
	active := make([]model.Reservation, 0, len(state.Reservations))
	for _, reservation := range state.Reservations {
		if reservation.Managed() && reservation.ActiveAt(now) {
			active = append(active, reservation)
		}
	}
	return active
}

// pruneClosedOnDemandReservations drops fully closed reservations so the pool
// state does not grow without bound.
func pruneClosedOnDemandReservations(reservations []model.Reservation) []model.Reservation {
	kept := make([]model.Reservation, 0, len(reservations))
	for _, reservation := range reservations {
		if reservation.Status == model.ReservationDeleted && reservation.LastError == "" {
			continue
		}
		kept = append(kept, reservation)
	}
	return kept
}

func onDemandStepGPUs(gpu string, step *types.FailoverOnDemandStep) []string {
	if len(step.GPUs) == 0 {
		return []string{gpu}
	}
	gpus := make([]string, 0, len(step.GPUs))
	for _, value := range step.GPUs {
		gpus = append(gpus, string(value))
	}
	return gpus
}

func onDemandStepProviders(step *types.FailoverOnDemandStep) []string {
	providers := make([]string, 0, len(step.Providers))
	for _, provider := range step.Providers {
		providers = append(providers, string(provider))
	}
	return providers
}

// emitOnDemandEvent reports an on-demand lifecycle change alongside the pool's
// resulting burn rate and node count, so a dashboard can read current spend off
// any single event rather than replaying the whole history.
func (s *Service) emitOnDemandEvent(workspaceID string, state *model.PoolState, action string, now time.Time, attrs map[string]string) {
	event := computePoolEvent(workspaceID, state, action, "")
	if event.Attrs == nil {
		event.Attrs = map[string]string{}
	}

	hourlyMicros := int64(0)
	active := activeOnDemandReservations(state, now)
	for _, reservation := range active {
		hourlyMicros += reservation.HourlyCostMicros
	}
	event.Attrs[types.EventComputeAttrOnDemandHourlyCents] = fmt.Sprintf("%d", types.MicrosToCents(hourlyMicros))
	event.Attrs[types.EventComputeAttrOnDemandNodeCount] = fmt.Sprintf("%d", len(active))

	for key, value := range attrs {
		event.Attrs[key] = value
	}
	s.emitComputeEvent(types.EventComputePool, event)
}
