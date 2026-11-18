package compute

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	reconcileStatusCheckInterval = time.Minute
	minManagedUsageRecordCents   = 1

	reconcileReasonAgentDisconnected    = "agent_disconnected"
	reconcileReasonBillingUnreachable   = "billing_unreachable"
	reconcileReasonCreditExhausted      = "credit_exhausted"
	reconcileReasonExternalAvailability = "external_availability"
	reconcileReasonInterrupted          = "interrupted"
	reconcileReasonLaunchFailed         = "launch_failed"
	reconcileReasonMachineDisconnected  = "machine_disconnected"
	reconcileReasonMachineReleased      = "machine_released"
	reconcileReasonProviderFailed       = "provider_failed"
	reconcileReasonReservationExpired   = "expired"

	instanceGoneMessage   = "the provider no longer has this machine"
	instanceFailedMessage = "the provider failed to start this machine"

	instanceInterruptedMessage = "the provider took this machine back"
)

func (s *Service) runReconciler(ctx context.Context) {
	interval := s.appConfig.ManagedCompute.Billing.ReconcileIntervalOrDefault()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := s.ReconcileManagedCompute(ctx); err != nil {
				log.Warn().Err(err).Msg("managed compute reconcile failed")
			}
			timer.Reset(interval)
		}
	}
}

func (s *Service) ReconcileManagedCompute(ctx context.Context) error {
	return s.reconcileManagedComputeWithClock(ctx, func() time.Time {
		return time.Now().UTC()
	})
}

func (s *Service) reconcileManagedComputeWithClock(ctx context.Context, nowFunc func() time.Time) error {
	if s == nil || s.computeRepo == nil {
		return nil
	}
	states, err := s.computeRepo.ListAllPoolStates(ctx, 0)
	if err != nil {
		return err
	}
	for _, state := range states {
		if state == nil || state.WorkspaceID == "" || state.Name == "" {
			continue
		}
		if err := s.lockAndReconcilePool(ctx, state.WorkspaceID, state.Name, nowFunc().UTC()); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", state.WorkspaceID).
				Str("pool_name", state.Name).
				Msg("failed to reconcile managed compute pool")
		}
	}
	if s.managedPoolRepo == nil {
		return nil
	}
	return s.reconcileManagedPoolMachines(ctx, nowFunc().UTC())
}

func (s *Service) reconcileManagedPoolMachines(ctx context.Context, now time.Time) error {
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return err
	}
	states, err := s.managedPoolRepo.ListManagedPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return err
	}
	for _, state := range states {
		if state == nil || state.WorkspaceID != workspaceID || state.Name == "" || state.ManagementSource == "" {
			continue
		}
		if err := s.withManagedPoolStateLock(ctx, workspaceID, state.Name, func(lockCtx context.Context) error {
			current, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, workspaceID, state.Name)
			if err != nil || current == nil || current.ManagementSource == "" {
				return err
			}
			s.reconcileStaleMachines(lockCtx, workspaceID, current, now)
			return nil
		}); err != nil {
			return fmt.Errorf("reconcile managed pool %q: %w", state.Name, err)
		}
	}
	return nil
}

// lockAndReconcilePool takes the pool lock and reads the pool's record again
// under it, so that a stale snapshot cannot clobber what a launch or a release
// wrote in the meantime.
func (s *Service) lockAndReconcilePool(ctx context.Context, workspaceID, poolName string, now time.Time) error {
	return s.withPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		state, err := s.getPrivatePoolState(lockCtx, workspaceID, poolName)
		if err != nil || state == nil {
			return err
		}
		return s.reconcilePool(lockCtx, workspaceID, state, now)
	})
}

func (s *Service) reconcilePool(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time) error {
	changed := false
	// Second-aligned billing windows keep retried usage reports deduplicable,
	// but liveness checks below need the exact timestamp. A same-second
	// heartbeat can otherwise look like it came from the future.
	billingNow := now.Truncate(time.Second)
	if s.poolNeedsManagedBilling(state, billingNow) {
		changed = s.reconcileBilling(ctx, workspaceID, state, billingNow) || changed
	}

	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() {
			continue
		}
		if reservation.Closed() {
			changed = s.reconcileClosedReservationMachine(ctx, workspaceID, state, reservation) || changed
			continue
		}
		if reservation.Status == model.ReservationTerminating {
			changed = s.reconcileTerminatingReservation(ctx, workspaceID, state, reservation, now) || changed
			continue
		}
		changed = advanceBillingRenewal(reservation, now) || changed
		changed = s.reconcileReservationStatus(ctx, workspaceID, state, reservation, now) || changed
		changed = s.reconcileReservationExpiry(ctx, workspaceID, state, reservation, now) || changed
	}

	changed = s.reconcileStaleMachines(ctx, workspaceID, state, now) || changed
	changed = syncReservedNodes(state, now) || changed
	if !changed {
		return nil
	}
	state.UpdatedAt = now
	return s.savePrivatePoolState(ctx, workspaceID, state)
}

// advanceBillingRenewal keeps BillingRenewalAt, which the API reports, in the
// future by moving it on an hour at a time. Nothing is paid or renewed at
// that hour: usage is billed as it accrues, and a reservation is renewed as
// its term runs out.
func advanceBillingRenewal(reservation *model.Reservation, now time.Time) bool {
	if reservation == nil || !reservation.Managed() || reservation.BillingRenewalAt.IsZero() {
		return false
	}
	changed := false
	for !reservation.BillingRenewalAt.After(now) {
		reservation.BillingRenewalAt = reservation.BillingRenewalAt.Add(time.Hour)
		changed = true
	}
	return changed
}

func (s *Service) poolNeedsManagedBilling(state *model.PoolState, now time.Time) bool {
	if state.BYOC != nil {
		return true
	}
	inGrace := !state.BillingDegradedSince.IsZero() || !state.CreditLowSince.IsZero()
	for _, reservation := range state.Reservations {
		if reservation.Managed() && reservation.ActiveAt(now) {
			return true
		}
		// Keep checking a pool whose reservations are held past their TTL by a
		// grace period, so recovery renews them and expiry releases them.
		if inGrace && reservation.Managed() && reservation.Open() {
			return true
		}
		if _, _, ok := reservationBillingWindow(&reservation, now); ok {
			return true
		}
	}
	return false
}

func (s *Service) reconcileBilling(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time) bool {
	if s.billing == nil {
		return false
	}
	changed := s.recordManagedUsage(ctx, workspaceID, state, now, false)

	decision, err := s.billing.CheckBalance(ctx, workspaceID, state.Name)
	if err != nil {
		// Fail closed: terminate once balance checks have failed past the grace period.
		if state.BillingDegradedSince.IsZero() {
			state.BillingDegradedSince = now
			changed = true
		}
		grace := s.appConfig.ManagedCompute.Billing.FailureGracePeriodOrDefault()
		event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolBillingDegraded, "degraded")
		// Customer-visible: class and status only. The raw error can carry
		// the internal billing response body, so it goes to private logs.
		errorClass, httpStatus := classifyBillingError(err)
		event.Attrs["error_class"] = errorClass
		if httpStatus > 0 {
			event.Attrs["http_status"] = fmt.Sprintf("%d", httpStatus)
		}
		log.Warn().Err(err).
			Str("workspace_id", workspaceID).
			Str("pool_name", state.Name).
			Str("error_class", errorClass).
			Int("http_status", httpStatus).
			Time("billing_degraded_since", state.BillingDegradedSince).
			Msg("managed compute balance check failed")
		event.Attrs["release_at"] = state.BillingDegradedSince.Add(grace).UTC().Format(time.RFC3339)
		s.emitComputeEvent(types.EventComputePool, event)

		if now.Sub(state.BillingDegradedSince) >= grace {
			reason := fmt.Sprintf("managed compute billing unreachable for %s", grace)
			return s.exhaustPoolCredit(ctx, workspaceID, state, billingDecision{}, reconcileReasonBillingUnreachable, reason, now) || changed
		}
		return changed
	}
	if !state.BillingDegradedSince.IsZero() {
		state.BillingDegradedSince = time.Time{}
		changed = true
	}

	// Usage is recorded in arrears; project accrued cost plus one interval of
	// burn so termination happens before the balance hits zero.
	accrued := s.accruedUnrecordedCents(state, now)
	buffer := s.exhaustionBufferCents(state, now)
	reason := ""
	if !decision.OK {
		reason = firstNonEmpty(decision.Message, "managed compute credits exhausted")
	} else if accrued+buffer > 0 && float64(decision.AvailableCents) < accrued+buffer {
		reason = fmt.Sprintf("managed compute credits nearly exhausted (available %d cents, accrued %.2f cents)", decision.AvailableCents, accrued)
	}
	if reason != "" {
		// Releasing reserved machines is not undoable, so give an auto
		// top-up or a manual purchase time to land first.
		grace := s.appConfig.ManagedCompute.Billing.CreditGracePeriodOrDefault()
		if state.CreditLowSince.IsZero() {
			state.CreditLowSince = now
			changed = true
		}
		if now.Sub(state.CreditLowSince) < grace {
			s.emitCreditLowEvent(workspaceID, state, decision, reason, state.CreditLowSince.Add(grace))
			return changed
		}
		return s.exhaustPoolCredit(ctx, workspaceID, state, decision, reconcileReasonCreditExhausted, reason, now) || changed
	}
	if !state.CreditLowSince.IsZero() {
		state.CreditLowSince = time.Time{}
		changed = true
	}

	changed = s.renewManagedReservations(ctx, workspaceID, state, decision, accrued+buffer, now) || changed

	return changed
}

// renewManagedReservations extends reservations nearing expiry by one hour at
// a time while the workspace can afford it, so an instance runs until it is
// released or credits run out - not until an arbitrary launch TTL. Renewal is
// skipped (and the reservation expires and terminates normally) when the
// balance cannot cover the accrued cost plus another hour.
//
// A renewal is Beam's to give and stands whatever the machine's provider
// answers. The provider is told of it afterwards, and a provider that cannot
// be told is told again on the next pass.
func (s *Service) renewManagedReservations(ctx context.Context, workspaceID string, state *model.PoolState, decision billingDecision, reservedCents float64, now time.Time) bool {
	renewLead := 2 * s.appConfig.ManagedCompute.Billing.ReconcileIntervalOrDefault()
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		// Not ActiveAt(now): a reservation held past its TTL by a billing
		// grace period must still be renewable once billing recovers.
		if !reservation.Managed() || !reservation.Open() || reservation.ExpiresAt.IsZero() {
			continue
		}

		// A reservation held past its expiry by a billing grace period renews
		// from where it lapsed, committing the hours it ran in the meantime.
		hours := int64(0)
		newExpiry := reservation.ExpiresAt
		for newExpiry.Sub(now) <= renewLead {
			newExpiry = newExpiry.Add(time.Hour)
			hours++
		}
		cents := float64(hours) * managedCostCents(s.billableMicros(reservation.HourlyCostMicros), time.Hour)
		if hours > 0 && float64(decision.AvailableCents) >= reservedCents+cents {
			reservation.ExpiresAt = newExpiry
			reservation.CommittedMicros += reservation.HourlyCostMicros * hours
			reservation.ExtendPending = true
			state.CommittedSpendMicros += s.billableMicros(reservation.HourlyCostMicros) * hours
			if state.ExpiresAt.Before(newExpiry) {
				state.ExpiresAt = newExpiry
			}
			reservedCents += cents
			s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, types.EventComputeActionReservationRenewed, string(reservation.Status)))
			changed = true
		}
		if reservation.ExtendPending {
			s.extendAtProvider(ctx, workspaceID, state, reservation)
			changed = true
		}
	}
	return changed
}

// extendAtProvider tells a reservation's provider of a renewal. The
// reservation stays marked until the provider has been told.
func (s *Service) extendAtProvider(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation) {
	if err := s.extendInstance(ctx, *reservation, reservation.ExpiresAt); err != nil {
		reservationFailed(workspaceID, state.Name, reservation, "extend at provider", fmt.Errorf("the provider was not told of the renewal: %w", err))
		return
	}
	// LastError is left as it is: usage that could not be recorded a moment
	// ago is not put right by the provider answering.
	reservation.ExtendPending = false
}

// reservationFailed logs that an operation on a reservation failed, and
// records the error on the reservation for whoever reads the pool's record.
func reservationFailed(workspaceID, poolName string, reservation *model.Reservation, operation string, err error) {
	log.Warn().Err(err).
		Str("workspace_id", workspaceID).
		Str("pool_name", poolName).
		Str("reservation_id", reservation.ID).
		Str("machine_id", reservation.MachineID).
		Str("operation", operation).
		Msg("an operation on a reservation failed")
	reservation.LastError = err.Error()
}

// exhaustPoolCredit closes billing windows, flushes the final usage records,
// and terminates all managed reservations in the pool.
func (s *Service) exhaustPoolCredit(ctx context.Context, workspaceID string, state *model.PoolState, decision billingDecision, terminateReason, message string, now time.Time) bool {
	s.emitCreditExhaustedEvent(workspaceID, state, decision, message)
	changed := closeManagedReservationBillingWindows(state, now)
	changed = s.recordManagedUsage(ctx, workspaceID, state, now, true) || changed
	s.disablePoolMachines(ctx, workspaceID, state.Name, terminateReason)
	return s.terminateManagedReservations(ctx, workspaceID, state, terminateReason, message, now) || changed
}

// accruedUnrecordedCents is the cost accrued since each billing cursor that
// has not been recorded against the workspace balance yet.
func (s *Service) accruedUnrecordedCents(state *model.PoolState, now time.Time) float64 {
	total := 0.0
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		start, end, ok := reservationBillingWindow(reservation, now)
		if !ok {
			continue
		}
		total += managedCostCents(s.billableMicros(reservation.HourlyCostMicros), end.Sub(start))
	}
	return total
}

// exhaustionBufferCents is one reconcile interval of burn across the pool's
// open reservations.
func (s *Service) exhaustionBufferCents(state *model.PoolState, now time.Time) float64 {
	interval := s.appConfig.ManagedCompute.Billing.ReconcileIntervalOrDefault()
	total := 0.0
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || !reservation.ActiveAt(now) {
			continue
		}
		total += managedCostCents(s.billableMicros(reservation.HourlyCostMicros), interval)
	}
	return total
}

func (s *Service) recordManagedUsage(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time, force bool) bool {
	if s.billing == nil {
		return false
	}
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		start, end, ok := reservationBillingWindow(reservation, now)
		if !ok {
			continue
		}

		duration := end.Sub(start)
		hourlyCostMicros := s.billableMicros(reservation.HourlyCostMicros)
		costCents := managedCostCents(hourlyCostMicros, duration)
		if !force && !managedUsageRecordable(reservation, now, end, costCents) {
			continue
		}
		usage := managedUsage{
			WorkspaceID:        workspaceID,
			PoolName:           state.Name,
			ReservationID:      reservation.ID,
			Provider:           reservation.Provider,
			Cloud:              reservation.Cloud,
			ProviderInstanceID: computeReservationInstanceID(*reservation),
			MachineID:          reservation.MachineID,
			GPU:                reservation.GPU,
			GPUCount:           reservation.GPUCount,
			NodeCount:          reservation.NodeCount,
			CPUMillicores:      reservation.CPUMillicores,
			MemoryMB:           reservation.MemoryMB,
			StorageMB:          reservation.StorageMB,
			HourlyCostMicros:   hourlyCostMicros,
			DurationSeconds:    duration.Seconds(),
			CostCents:          costCents,
			StartAt:            start,
			EndAt:              end,
		}
		if err := s.billing.RecordManagedUsage(ctx, usage); err != nil {
			reservationFailed(workspaceID, state.Name, reservation, "record usage", err)
			changed = true
			continue
		}
		reservation.BillingCursorAt = end
		reservation.LastError = ""
		s.emitComputeEvent(types.EventComputePool, managedUsageRecordedEvent(workspaceID, state, *reservation, usage))
		if err := s.recordManagedUsageMetrics(usage); err != nil {
			reservationFailed(workspaceID, state.Name, reservation, "record usage metrics", err)
		}
		changed = true
	}
	changed = s.recordBYOCManagedUsage(ctx, workspaceID, state, now, force) || changed
	return changed
}

func (s *Service) recordBYOCManagedUsage(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time, force bool) bool {
	if state == nil || state.BYOC == nil || s.computeRepo == nil {
		return false
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		log.Warn().
			Err(err).
			Str("workspace_id", workspaceID).
			Str("pool_name", state.Name).
			Msg("failed to list BYOC machines for billing")
		return false
	}
	changed := false
	for _, machine := range machines {
		start, end, ok := byocMachineBillingWindow(machine, now, force)
		if !ok {
			continue
		}
		duration := end.Sub(start)
		hourlyCostMicros := byocMachineHourlyCostMicros(machine, s.appConfig.ManagedCompute)
		costCents := managedCostCents(hourlyCostMicros, duration)
		end, duration, costCents = byocManagedUsageBillableWindow(start, end, duration, costCents, force, now)
		if !managedBYOCUsageRecordable(force, costCents) {
			continue
		}
		usage := byocManagedUsage(workspaceID, state, machine, hourlyCostMicros, duration, costCents, start, end)
		if err := s.billing.RecordManagedUsage(ctx, usage); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", workspaceID).
				Str("pool_name", state.Name).
				Str("machine_id", machine.MachineID).
				Msg("failed to record BYOC managed usage")
			continue
		}
		machine.BillingCursorAt = end
		if err := s.saveComputeAgentTokenState(ctx, machine); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", workspaceID).
				Str("pool_name", state.Name).
				Str("machine_id", machine.MachineID).
				Msg("failed to advance BYOC managed billing cursor")
			continue
		}
		if err := s.recordManagedUsageMetrics(usage); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", workspaceID).
				Str("pool_name", state.Name).
				Str("machine_id", machine.MachineID).
				Msg("failed to record BYOC managed usage metrics")
		}
		changed = true
	}
	return changed
}

func byocMachineBillingWindow(machine *model.AgentTokenState, now time.Time, force bool) (time.Time, time.Time, bool) {
	if machine == nil {
		return time.Time{}, time.Time{}, false
	}
	start := machine.BillingCursorAt
	if start.IsZero() || (!machine.CreatedAt.IsZero() && start.Before(machine.CreatedAt)) {
		start = machine.CreatedAt
	}
	if start.IsZero() || (!machine.LastJoinAt.IsZero() && start.Before(machine.LastJoinAt)) {
		start = machine.LastJoinAt
	}
	if start.IsZero() {
		return time.Time{}, time.Time{}, false
	}

	end := model.AgentMachineLastSeen(machine)
	if model.AgentMachineConnected(machine, now) || (force && machine.LastDisconnectAt.IsZero()) {
		end = now
	}
	if end.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	if end.After(now) {
		end = now
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func byocManagedUsageBillableWindow(start, end time.Time, duration time.Duration, costCents float64, force bool, now time.Time) (time.Time, time.Duration, float64) {
	if force || end.Before(now) || costCents < minManagedUsageRecordCents {
		return end, duration, costCents
	}
	wholeCents := math.Floor(costCents)
	if wholeCents < minManagedUsageRecordCents {
		return end, duration, 0
	}
	billableDuration := time.Duration(float64(duration) * (wholeCents / costCents)).Truncate(time.Second)
	if billableDuration <= 0 {
		return end, duration, 0
	}
	return start.Add(billableDuration), billableDuration, wholeCents
}

func managedBYOCUsageRecordable(force bool, costCents float64) bool {
	if force {
		return costCents > 0
	}
	return costCents >= minManagedUsageRecordCents
}

func byocMachineHourlyCostMicros(machine *model.AgentTokenState, config types.ManagedComputeConfig) int64 {
	cpuMillicores := firstNonZeroInt64(machine.CPUMillicores, int64(machine.CPUCount)*1000)
	memoryMB := int64(firstNonZeroUint64(machine.MemoryMB, machine.Metrics.MemoryTotalMB))
	return managedBYOCNodeHourlyCostMicros(cpuMillicores, memoryMB, config)
}

func byocManagedUsage(workspaceID string, state *model.PoolState, machine *model.AgentTokenState, hourlyCostMicros int64, duration time.Duration, costCents float64, start, end time.Time) managedUsage {
	return managedUsage{
		WorkspaceID:        workspaceID,
		PoolName:           state.Name,
		ReservationID:      "byoc:" + machine.MachineID,
		Provider:           "byoc",
		Cloud:              firstNonEmpty(state.BYOC.Provider, string(state.Source.Canonical())),
		ProviderInstanceID: firstNonEmpty(machine.MachineFingerprint, machine.Hostname, machine.MachineID),
		MachineID:          machine.MachineID,
		GPU:                strings.Join(machine.GPUs, ","),
		GPUCount:           machine.GPUCount,
		NodeCount:          1,
		CPUMillicores:      firstNonZeroInt64(machine.CPUMillicores, int64(machine.CPUCount)*1000),
		MemoryMB:           int64(firstNonZeroUint64(machine.MemoryMB, machine.Metrics.MemoryTotalMB)),
		StorageMB:          int64(machine.Metrics.DiskTotalMB),
		HourlyCostMicros:   hourlyCostMicros,
		DurationSeconds:    duration.Seconds(),
		CostCents:          costCents,
		StartAt:            start,
		EndAt:              end,
	}
}

func managedUsageRecordedEvent(workspaceID string, state *model.PoolState, reservation model.Reservation, usage managedUsage) types.EventComputeSchema {
	event := computeReservationEvent(workspaceID, state, reservation, types.EventComputeActionReservationUsageRecorded, string(reservation.Status))
	event.Attrs["cost_cents"] = fmt.Sprintf("%.4f", usage.CostCents)
	event.Attrs["duration_seconds"] = fmt.Sprintf("%.0f", usage.DurationSeconds)
	event.Attrs["window_start"] = usage.StartAt.Format(time.RFC3339)
	event.Attrs["window_end"] = usage.EndAt.Format(time.RFC3339)
	return event
}

func managedUsageRecordable(reservation *model.Reservation, now, end time.Time, costCents float64) bool {
	if costCents >= minManagedUsageRecordCents {
		return true
	}
	return reservation != nil && !reservation.ExpiresAt.IsZero() && !now.Before(reservation.ExpiresAt) && end.Equal(reservation.ExpiresAt)
}

func closeManagedReservationBillingWindows(state *model.PoolState, now time.Time) bool {
	changed := false
	for i := range state.Reservations {
		changed = closeReservationBillingWindow(&state.Reservations[i], now) || changed
	}
	return changed
}

func closeReservationBillingWindow(reservation *model.Reservation, now time.Time) bool {
	if reservation == nil || !reservation.Managed() || reservation.Status != model.ReservationActive {
		return false
	}
	if !reservation.ExpiresAt.IsZero() && !reservation.ExpiresAt.After(now) {
		return false
	}
	reservation.ExpiresAt = now
	return true
}

func reservationBillingWindow(reservation *model.Reservation, now time.Time) (time.Time, time.Time, bool) {
	if reservation == nil || !reservation.Managed() || !reservation.Open() {
		return time.Time{}, time.Time{}, false
	}

	start := reservation.BillingCursorAt
	if start.IsZero() || start.Before(reservation.CreatedAt) {
		start = reservation.CreatedAt
	}
	if start.IsZero() {
		return time.Time{}, time.Time{}, false
	}

	end := now
	if !reservation.ExpiresAt.IsZero() && reservation.ExpiresAt.Before(end) {
		end = reservation.ExpiresAt
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func managedCostCents(hourlyMicros int64, duration time.Duration) float64 {
	if hourlyMicros <= 0 || duration <= 0 {
		return 0
	}
	return float64(hourlyMicros) * duration.Hours() / 10000
}

func (s *Service) recordManagedUsageMetrics(usage managedUsage) error {
	if s.usageMetricsRepo == nil {
		return nil
	}
	metadata := map[string]interface{}{
		"workspace_id":         usage.WorkspaceID,
		"pool_name":            usage.PoolName,
		"reservation_id":       usage.ReservationID,
		"provider":             usage.Provider,
		"cloud":                usage.Cloud,
		"provider_instance_id": usage.ProviderInstanceID,
		"machine_id":           usage.MachineID,
		"gpu":                  usage.GPU,
		"gpu_count":            usage.GPUCount,
		"node_count":           usage.NodeCount,
		"cpu_millicores":       usage.CPUMillicores,
		"memory_mb":            usage.MemoryMB,
		"storage_mb":           usage.StorageMB,
		"hourly_cost_micros":   usage.HourlyCostMicros,
		"duration_seconds":     usage.DurationSeconds,
		"cost_cents":           usage.CostCents,
	}
	if err := s.usageMetricsRepo.IncrementCounter(types.UsageMetricsManagedComputeReservationSeconds, metadata, usage.DurationSeconds); err != nil {
		return fmt.Errorf("record managed compute reservation seconds: %w", err)
	}
	if err := s.usageMetricsRepo.IncrementCounter(types.UsageMetricsManagedComputeReservationCost, metadata, usage.CostCents); err != nil {
		return fmt.Errorf("record managed compute reservation cost: %w", err)
	}
	return nil
}

// reconcileReservationStatus asks the provider how a reservation's machine is
// doing, at most once a minute. It reports whether the record must be saved,
// which is after every check: the time of the check is in the record, so the
// minute holds for every gateway.
func (s *Service) reconcileReservationStatus(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, now time.Time) bool {
	if reservation.Provider == "" {
		return false
	}
	if !reservation.LastStatusCheckAt.IsZero() && now.Sub(reservation.LastStatusCheckAt) < reconcileStatusCheckInterval {
		return false
	}
	reservation.LastStatusCheckAt = now
	instance, err := s.instanceOf(ctx, *reservation, now)
	if err != nil {
		reservationFailed(workspaceID, state.Name, reservation, "check status", err)
		return true
	}

	before := *reservation
	switch instance.Status {
	case providers.StatusPending:
		// A provider may report a running machine as pending again while it
		// reboots or undergoes maintenance. Its initial boot already finished.
		if reservation.Status == model.ReservationActive {
			break
		}
		bootTimeout := s.appConfig.ManagedCompute.Reservations.BootTimeoutOrDefault()
		if !reservation.CreatedAt.IsZero() && !now.Before(reservation.CreatedAt.Add(bootTimeout)) {
			s.terminateReservation(ctx, workspaceID, state, reservation, reconcileReasonLaunchFailed,
				fmt.Sprintf("the provider did not start this machine within %s", bootTimeout), now)
			return true
		}
		reservation.Status = model.ReservationPending
	case providers.StatusActive:
		if reservation.Status == model.ReservationPending {
			s.outcomes.Record(reservationOutcome(*reservation).CameUp(now.Sub(reservation.CreatedAt)))
		}
		reservation.Status = model.ReservationActive
	case providers.StatusFailed:
		// The machine will never work, and the provider may go on charging
		// for it until it is deleted.
		s.terminateReservation(ctx, workspaceID, state, reservation, reconcileReasonProviderFailed, firstNonEmpty(instance.StatusDetail, instanceFailedMessage), now)
		return true
	case providers.StatusInterrupted:
		// A spot machine is sold on the terms that its provider may take it
		// back, so that ends the reservation and what it is billed, and is
		// no failure. Should a provider take back a machine that was not
		// bought as spot, the provider failed. Either way what is left of
		// the machine is deleted.
		reason := reconcileReasonProviderFailed
		if reservation.Spot {
			reason = reconcileReasonInterrupted
		}
		s.terminateReservation(ctx, workspaceID, state, reservation, reason, firstNonEmpty(instance.StatusDetail, instanceInterruptedMessage), now)
		return true
	case providers.StatusDeleted:
		// The machine went without Beam deleting it: the provider took it
		// back, its own deletion timer fired, or someone removed it.
		s.revokeReservationJoinToken(ctx, reservation)
		s.outcomes.Record(reservationOutcome(*reservation).Ended(watch.ResultGone, now.Sub(reservation.CreatedAt)))
		reservation.Status = model.ReservationDeleted
		reservation.LastStatusMessage = firstNonEmpty(instance.StatusDetail, instanceGoneMessage)
		reservation.LastError = ""
		action := types.EventComputeActionReservationStatusUpdated
		// A running machine that went before its term was lost, not given
		// back, and stays listed like one the provider reported as taken back
		// or failed, so that the tenant sees where it went.
		if before.Status == model.ReservationActive && before.ActiveAt(now) {
			reservation.Status = model.ReservationFailed
			reservation.TerminatingReason = reconcileReasonProviderFailed
			reservation.LastStatusMessage = instanceGoneMessage
			if reservation.Spot {
				reservation.Status = model.ReservationInterrupted
				reservation.TerminatingReason = reconcileReasonInterrupted
				reservation.LastStatusMessage = instanceInterruptedMessage
				action = types.EventComputeActionReservationInterrupted
			}
		}
		s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, action, string(reservation.Status)))
		return true
	}
	// Any other status is one the provider's client does not know, and says
	// nothing about the machine: the reservation keeps the status it had.

	// What the provider does not say of where the machine is stays as it
	// was.
	reservation.LastStatusMessage = instance.StatusDetail
	reservation.PublicIP = cmp.Or(instance.PublicIP, reservation.PublicIP)
	reservation.SSHHost = cmp.Or(instance.SSHHost, reservation.SSHHost)
	reservation.SSHPort = cmp.Or(instance.SSHPort, reservation.SSHPort)
	changed := *reservation != before
	if err := s.updateMachineSSHProviderEndpoint(ctx, workspaceID, state.Name, reservation.MachineID, instance.PublicIP, instance.SSHHost, instance.SSHPort); err != nil {
		reservationFailed(workspaceID, state.Name, reservation, "check status", fmt.Errorf("update SSH endpoint: %w", err))
		return true
	}
	// LastError is left as it is. What billing or a renewal recorded earlier
	// in this pass is not put right by the provider answering.
	if changed {
		s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, types.EventComputeActionReservationStatusUpdated, string(reservation.Status)))
	}
	return true
}

func (s *Service) reconcileReservationExpiry(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, now time.Time) bool {
	// The status check that ran just before may have closed the reservation.
	if !reservation.Managed() || !reservation.Open() {
		return false
	}
	if reservation.ExpiresAt.IsZero() || reservation.ExpiresAt.After(now) {
		return false
	}
	// Renewal cannot run while billing is degraded or credit is low, so a
	// reservation can reach its TTL mid-grace. Hold it until the grace
	// resolves: billing recovers (renewal catches up) or the grace runs out
	// (the pool is released with the billing reason, at the advertised time).
	if releaseAt, ok := s.billingGraceReleaseAt(state); ok && now.Before(releaseAt) {
		return false
	}
	s.terminateReservation(ctx, workspaceID, state, reservation, reconcileReasonReservationExpired, "reservation ttl expired", now)
	return true
}

func (s *Service) reconcileClosedReservationMachine(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation) bool {
	if reservation.MachineID == "" {
		return false
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, state.Name, reservation.MachineID)
	if err != nil {
		reservationFailed(workspaceID, state.Name, reservation, "look up machine", err)
		return true
	}
	if machine == nil {
		if err := s.computeRepo.DeleteMachineSSHState(ctx, workspaceID, state.Name, reservation.MachineID); err != nil {
			reservationFailed(workspaceID, state.Name, reservation, "delete SSH state", err)
			return true
		}
		return false
	}
	if err := s.removePrivateMachine(ctx, machine); err != nil {
		reservationFailed(workspaceID, state.Name, reservation, "remove machine", err)
		return true
	}
	reservation.LastError = ""
	return true
}

func (s *Service) reconcileTerminatingReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, now time.Time) bool {
	if !reservation.LastReconcileAt.IsZero() && now.Sub(reservation.LastReconcileAt) < reconcileStatusCheckInterval {
		return false
	}
	reservation.LastReconcileAt = now
	s.terminateReservation(ctx, workspaceID, state, reservation, firstNonEmpty(reservation.TerminatingReason, "terminating"), firstNonEmpty(reservation.LastStatusMessage, "reservation terminating"), now)
	return true
}

func (s *Service) terminateManagedReservations(ctx context.Context, workspaceID string, state *model.PoolState, reason, message string, now time.Time) bool {
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || !reservation.Open() {
			continue
		}
		s.terminateReservation(ctx, workspaceID, state, reservation, reason, message, now)
		changed = true
	}
	return changed
}

// terminateReservation deletes a reservation's machine at its provider and
// closes the reservation. While the delete fails the reservation stays
// terminating, and the reconciler tries again. Callers pass a reservation
// that is not closed already.
func (s *Service) terminateReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, reason, message string, now time.Time) {
	// Registration tokens are persistent; kill them whenever a reservation closes.
	s.revokeReservationJoinToken(ctx, reservation)
	reservation.TerminatingReason = reason
	reservation.LastStatusMessage = message
	if err := s.deleteInstance(ctx, reservation, now); err != nil {
		reservation.Status = model.ReservationTerminating
		reservationFailed(workspaceID, state.Name, reservation, "delete machine", err)
		return
	}
	reservation.Status = model.ReservationDeleted
	action := types.EventComputeActionReservationTerminating
	switch reason {
	case reconcileReasonProviderFailed:
		// A machine that failed at its provider, or that the provider took
		// back without having sold it as spot, stays listed as failed until
		// the tenant dismisses it.
		reservation.Status = model.ReservationFailed
	case reconcileReasonInterrupted:
		// A spot machine its provider took back stays listed too, so that
		// the tenant sees where it went.
		reservation.Status = model.ReservationInterrupted
		action = types.EventComputeActionReservationInterrupted
	}
	reservation.LastError = ""
	if reservation.MachineID != "" {
		if err := s.computeRepo.DeleteMachineSSHState(ctx, workspaceID, state.Name, reservation.MachineID); err != nil {
			reservationFailed(workspaceID, state.Name, reservation, "delete SSH state", fmt.Errorf("delete SSH state: %w", err))
		}
	}
	syncReservedNodes(state, now)
	s.recordEnd(*reservation, reservationEnd(reason), now)
	s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, action, reason))
}

// syncReservedNodes counts the nodes of a pool's open reservations again,
// which change as reservations close or run out. It reports whether the
// count changed.
func syncReservedNodes(state *model.PoolState, now time.Time) bool {
	nodes := activeReservationNodes(state.Reservations, now)
	if nodes == state.ReservedNodes {
		return false
	}
	state.ReservedNodes = nodes
	return true
}

// recordEnd writes to the market's history that a reservation's machine is
// gone at its provider, and how it ended. A reservation whose provider
// never made a machine has none that ended.
func (s *Service) recordEnd(reservation model.Reservation, result string, now time.Time) {
	if computeReservationInstanceID(reservation) == "" {
		return
	}
	s.outcomes.Record(reservationOutcome(reservation).Ended(result, now.Sub(reservation.CreatedAt)))
}

// reservationOutcome is what the market's history is told of a reserved
// machine: what it was bought as, where, and for how much.
func reservationOutcome(reservation model.Reservation) watch.Outcome {
	outcome := watch.OutcomeOf(providers.Offer{
		ID:               reservation.OfferID,
		Provider:         reservation.Provider,
		Cloud:            reservation.Cloud,
		InstanceType:     reservation.InstanceType,
		Region:           reservation.Region,
		GPU:              reservation.GPU,
		GPUCount:         reservation.GPUCount,
		HourlyCostMicros: reservation.HourlyCostMicros,
		Spot:             reservation.Spot,
	})
	outcome.Source = watch.SourceReservation
	return outcome
}

// reservationEnd is how the market's history names the end of a reservation
// that ended for reason. Only what its provider did to the machine says
// anything of the provider: the rest is the tenant, or Beam, giving it back.
func reservationEnd(reason string) string {
	switch reason {
	case reconcileReasonInterrupted:
		return watch.ResultInterrupted
	case reconcileReasonProviderFailed:
		return watch.ResultFailed
	case reconcileReasonReservationExpired:
		return watch.ResultExpired
	}
	return watch.ResultReleased
}

func (s *Service) reconcileStaleMachines(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time) bool {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return false
	}
	changed := false
	for _, machine := range machines {
		if model.AgentMachineConnected(machine, now) {
			continue
		}
		if s.pruneDeadMachine(ctx, state, machine, now) {
			continue
		}
		changed = s.disableMachineWorker(ctx, machine, reconcileReasonMachineDisconnected) || changed
	}
	// Drop index entries whose machine keys no longer exist.
	if err := s.computeRepo.PruneAgentMachineIndex(ctx, workspaceID, state.Name); err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceID).Str("pool_name", state.Name).Msg("failed to prune agent machine index")
	}
	return changed
}

// staleMachineRetention is how long a disconnected machine's record is kept
// before it is deleted outright (it can always re-join with a new token).
const staleMachineRetention = 7 * 24 * time.Hour

// pruneDeadMachine deletes machines disconnected past the retention window
// whose managed reservation, if they had one, is closed.
func (s *Service) pruneDeadMachine(ctx context.Context, state *model.PoolState, machine *model.AgentTokenState, now time.Time) bool {
	if machine == nil {
		return false
	}
	lastSeen := model.AgentMachineLastSeen(machine)
	if lastSeen.IsZero() || now.Sub(lastSeen) < staleMachineRetention {
		return false
	}
	if managedReservationForMachine(state, machine.MachineID) != nil {
		return false
	}
	if err := s.removePrivateMachine(ctx, machine); err != nil {
		log.Warn().
			Err(err).
			Str("workspace_id", machine.WorkspaceID).
			Str("pool_name", machine.PoolName).
			Str("machine_id", machine.MachineID).
			Msg("failed to prune dead machine")
		return false
	}
	return true
}

func (s *Service) disablePoolMachines(ctx context.Context, workspaceID, poolName, reason string) {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, poolName)
	if err != nil {
		return
	}
	for _, machine := range machines {
		if (reason == reconcileReasonCreditExhausted || reason == reconcileReasonBillingUnreachable) && machine != nil && machine.Schedulable {
			machine.Schedulable = false
			_ = s.saveComputeAgentTokenState(ctx, machine)
		}
		s.disableMachineWorker(ctx, machine, reason)
	}
}

func (s *Service) disableMachineWorker(ctx context.Context, machine *model.AgentTokenState, reason string) bool {
	if s.workerRepo == nil || machine == nil {
		return false
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	worker, err := s.workerRepo.GetWorkerById(workerID)
	if err != nil || worker == nil || worker.MachineId != machine.MachineID || worker.PoolName != machine.PoolName {
		return false
	}
	if worker.Status == types.WorkerStatusDisabled {
		if shouldStopContainersForDisabledWorker(reason) {
			s.stopWorkerContainers(ctx, workerID, reason)
		}
		return false
	}
	if err := s.workerRepo.UpdateWorkerStatus(workerID, types.WorkerStatusDisabled); err != nil {
		return false
	}
	if shouldStopContainersForDisabledWorker(reason) {
		s.stopWorkerContainers(ctx, workerID, reason)
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		MachineID:   machine.MachineID,
		WorkerID:    workerID,
		Action:      types.EventComputeActionWorkerDisabled,
		Status:      string(types.WorkerStatusDisabled),
		Message:     "machine worker disabled: " + reason,
	})
	return true
}

func shouldStopContainersForDisabledWorker(reason string) bool {
	switch reason {
	case reconcileReasonAgentDisconnected, reconcileReasonMachineDisconnected:
		return false
	default:
		return true
	}
}

func (s *Service) stopWorkerContainers(ctx context.Context, workerID, reason string) {
	if s.containerRepo == nil || workerID == "" {
		return
	}
	containers, err := s.containerRepo.GetActiveContainersByWorkerId(workerID)
	if err != nil {
		log.Warn().Err(err).Str("worker_id", workerID).Msg("failed to list containers for disabled worker")
		return
	}
	for _, container := range containers {
		if container.ContainerId == "" || container.Status == types.ContainerStatusStopping {
			continue
		}
		s.stopContainerForDisabledWorker(ctx, container.ContainerId, reason)
	}
}

func (s *Service) stopContainerForDisabledWorker(_ context.Context, containerID, reason string) {
	stopReason := types.StopContainerReasonScheduler
	if reason == reconcileReasonMachineReleased {
		stopReason = types.StopContainerReasonUser
	}
	if s.scheduler != nil {
		if err := s.scheduler.Stop(&types.StopContainerArgs{ContainerId: containerID, Force: true, Reason: stopReason}); err != nil {
			log.Warn().Err(err).Str("container_id", containerID).Msg("failed to stop container on disabled worker")
		}
		return
	}
	if err := s.containerRepo.UpdateContainerStatus(containerID, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping); err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("failed to mark container stopping on disabled worker")
	}
}

func (s *Service) emitCreditExhaustedEvent(workspaceID string, state *model.PoolState, decision billingDecision, message string) {
	event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreditExhausted, "terminating")
	event.Message = message
	event.Attrs["available_cents"] = fmt.Sprintf("%d", decision.AvailableCents)
	event.Attrs["required_cents"] = fmt.Sprintf("%d", decision.RequiredCents)
	s.emitComputeEvent(types.EventComputePool, event)
}

// billingGraceReleaseAt is when a pool in a billing grace period (balance
// checks failing, or balance low) will be released if nothing changes.
func (s *Service) billingGraceReleaseAt(state *model.PoolState) (time.Time, bool) {
	cfg := s.appConfig.ManagedCompute.Billing
	var releaseAt time.Time
	if !state.BillingDegradedSince.IsZero() {
		releaseAt = state.BillingDegradedSince.Add(cfg.FailureGracePeriodOrDefault())
	}
	if !state.CreditLowSince.IsZero() {
		if at := state.CreditLowSince.Add(cfg.CreditGracePeriodOrDefault()); at.After(releaseAt) {
			releaseAt = at
		}
	}
	return releaseAt, !releaseAt.IsZero()
}

// emitCreditLowEvent warns that the pool will be released at releaseAt unless
// credits are added. Emitted every reconcile while the pool is in grace.
func (s *Service) emitCreditLowEvent(workspaceID string, state *model.PoolState, decision billingDecision, message string, releaseAt time.Time) {
	event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreditLow, "credit_low")
	event.Message = message
	event.Attrs["available_cents"] = fmt.Sprintf("%d", decision.AvailableCents)
	event.Attrs["required_cents"] = fmt.Sprintf("%d", decision.RequiredCents)
	event.Attrs["release_at"] = releaseAt.UTC().Format(time.RFC3339)
	s.emitComputeEvent(types.EventComputePool, event)
}

func computeReservationEvent(workspaceID string, state *model.PoolState, reservation model.Reservation, action, status string) types.EventComputeSchema {
	event := computePoolEvent(workspaceID, state, action, status)
	event.MachineID = reservation.MachineID
	event.Source = string(reservation.Source)
	event.GPUCount = reservation.GPUCount
	event.Attrs["reservation_id"] = reservation.ID
	event.Attrs["provider"] = reservation.Provider
	event.Attrs["cloud"] = reservation.Cloud
	event.Attrs["instance_id"] = reservation.InstanceID
	event.Attrs["offer_id"] = reservation.OfferID
	event.Attrs["terminating_reason"] = reservation.TerminatingReason
	event.Attrs["last_status_message"] = reservation.LastStatusMessage
	return event
}
