package compute

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	reconcileStatusCheckInterval = time.Minute
	minManagedUsageRecordCents   = 1

	reconcileReasonAgentDisconnected   = "agent_disconnected"
	reconcileReasonBillingUnreachable  = "billing_unreachable"
	reconcileReasonCreditExhausted     = "credit_exhausted"
	reconcileReasonMachineDisconnected = "machine_disconnected"
	reconcileReasonMachineReleased     = "machine_released"
	reconcileReasonReservationExpired  = "expired"
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

func (s *Service) reconcileManagedComputeAt(ctx context.Context, now time.Time) error {
	return s.reconcileManagedComputeWithClock(ctx, func() time.Time {
		return now
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
		now := time.Now().UTC()
		if nowFunc != nil {
			now = nowFunc().UTC()
		}
		if err := s.reconcilePoolLocked(ctx, state.WorkspaceID, state.Name, now); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", state.WorkspaceID).
				Str("pool_name", state.Name).
				Msg("failed to reconcile managed compute pool")
		}
	}
	return nil
}

// reconcilePoolLocked re-reads the pool state under the pool lock so a stale
// snapshot cannot clobber concurrent launch/release updates.
func (s *Service) reconcilePoolLocked(ctx context.Context, workspaceID, poolName string, now time.Time) error {
	return s.withPoolStateLock(ctx, workspaceID, poolName, func() error {
		state, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
		if err != nil || state == nil {
			return err
		}
		return s.reconcilePool(ctx, workspaceID, state, now)
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

	vendors := s.computeVendors()
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() {
			continue
		}
		if reservationClosed(reservation.Status) {
			changed = s.reconcileClosedReservationMachine(ctx, workspaceID, state, reservation) || changed
			continue
		}
		if reservation.Status == model.ReservationTerminating {
			changed = s.reconcileTerminatingReservation(ctx, workspaceID, state, reservation, vendors, now) || changed
			continue
		}
		changed = advanceBillingRenewal(reservation, now) || changed
		changed = s.reconcileReservationStatus(ctx, workspaceID, state, reservation, vendors, now) || changed
		changed = s.reconcileReservationExpiry(ctx, workspaceID, state, reservation, vendors, now) || changed
	}

	changed = s.reconcileStaleMachines(ctx, workspaceID, state, now) || changed
	if !changed {
		return nil
	}
	state.UpdatedAt = now
	return s.savePrivatePoolState(ctx, workspaceID, state)
}

// advanceBillingRenewal keeps the solver's hourly renewal boundary in the
// future; without advancement it goes stale one hour after creation.
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
	if state != nil && state.BYOC != nil {
		return true
	}
	for _, reservation := range state.Reservations {
		if reservation.Managed() && reservation.ActiveAt(now) {
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

	decision, err := s.billing.CheckBalance(ctx, workspaceID)
	if err != nil {
		// Fail closed: terminate once balance checks have failed past the grace period.
		if state.BillingDegradedSince.IsZero() {
			state.BillingDegradedSince = now
			changed = true
		}
		s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolBillingDegraded, "degraded"))

		grace := s.appConfig.ManagedCompute.Billing.FailureGracePeriodOrDefault()
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

	for i := range state.Reservations {
		state.Reservations[i].LastBillingCheckAt = now
	}

	if !decision.OK {
		reason := firstNonEmpty(decision.Message, "managed compute credits exhausted")
		return s.exhaustPoolCredit(ctx, workspaceID, state, decision, reconcileReasonCreditExhausted, reason, now) || changed
	}

	// Usage is recorded in arrears; project accrued cost plus one interval of
	// burn so termination happens before the balance hits zero.
	accrued := s.accruedUnrecordedCents(state, now)
	buffer := s.exhaustionBufferCents(state, now)
	if accrued+buffer > 0 && float64(decision.AvailableCents) < accrued+buffer {
		reason := fmt.Sprintf("managed compute credits nearly exhausted (available %d cents, accrued %.2f cents)", decision.AvailableCents, accrued)
		return s.exhaustPoolCredit(ctx, workspaceID, state, decision, reconcileReasonCreditExhausted, reason, now) || changed
	}

	changed = s.renewManagedReservations(ctx, workspaceID, state, decision, accrued+buffer, now, s.computeVendors()) || changed

	return changed
}

// renewManagedReservations extends reservations nearing expiry by one hour at
// a time while the workspace can afford it, so an instance runs until it is
// released or credits run out - not until an arbitrary launch TTL. Renewal is
// skipped (and the reservation expires and terminates normally) when the
// balance cannot cover the accrued cost plus another hour.
func (s *Service) renewManagedReservations(ctx context.Context, workspaceID string, state *model.PoolState, decision billingDecision, reservedCents float64, now time.Time, vendors map[string]model.Vendor) bool {
	renewLead := 2 * s.appConfig.ManagedCompute.Billing.ReconcileIntervalOrDefault()
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || !reservation.ActiveAt(now) || reservation.ExpiresAt.IsZero() {
			continue
		}
		if reservation.ExpiresAt.Sub(now) > renewLead {
			continue
		}

		hourlyCents := managedCostCents(s.billableMicros(reservation.HourlyCostMicros), time.Hour)
		if float64(decision.AvailableCents) < reservedCents+hourlyCents {
			continue
		}

		vendor := vendors[reservation.Provider]
		if vendor == nil {
			reservation.LastError = fmt.Sprintf("vendor %q is not configured", reservation.Provider)
			changed = true
			continue
		}
		newExpiry := reservation.ExpiresAt.Add(time.Hour)
		if err := vendor.ExtendReservation(ctx, computeReservationInstanceID(*reservation), newExpiry); err != nil {
			reservation.LastError = fmt.Sprintf("renewal failed: %v", err)
			changed = true
			continue
		}
		reservation.ExpiresAt = newExpiry
		reservation.CommittedMicros += reservation.HourlyCostMicros
		reservation.LastError = ""
		state.CommittedSpendMicros += s.billableMicros(reservation.HourlyCostMicros)
		if state.ExpiresAt.Before(newExpiry) {
			state.ExpiresAt = newExpiry
		}
		reservedCents += hourlyCents
		s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, types.EventComputeActionReservationRenewed, string(reservation.Status)))
		changed = true
	}
	return changed
}

// exhaustPoolCredit closes billing windows, flushes the final usage records,
// and terminates all managed reservations in the pool.
func (s *Service) exhaustPoolCredit(ctx context.Context, workspaceID string, state *model.PoolState, decision billingDecision, terminateReason, message string, now time.Time) bool {
	s.emitCreditExhaustedEvent(workspaceID, state, decision, message)
	changed := closeManagedReservationBillingWindows(state, now)
	changed = s.recordManagedUsage(ctx, workspaceID, state, now, true) || changed
	s.disablePoolMachines(ctx, workspaceID, state.Name, reconcileReasonCreditExhausted)
	return s.terminateManagedReservations(ctx, workspaceID, state, terminateReason, message) || changed
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
			reservation.LastError = err.Error()
			changed = true
			continue
		}
		reservation.BillingCursorAt = end
		reservation.LastError = ""
		s.emitComputeEvent(types.EventComputePool, managedUsageRecordedEvent(workspaceID, state, *reservation, usage))
		if err := s.recordManagedUsageMetrics(usage); err != nil {
			reservation.LastError = err.Error()
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
		hourlyCostMicros := byocMachineHourlyCostMicros(machine)
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

func byocMachineHourlyCostMicros(machine *model.AgentTokenState) int64 {
	if machine == nil {
		return 0
	}
	cpuMillicores := firstNonZeroInt64(machine.CPUMillicores, int64(machine.CPUCount)*1000)
	memoryMB := int64(firstNonZeroUint64(machine.MemoryMB, machine.Metrics.MemoryTotalMB))
	return managedBYOCNodeHourlyCostMicros(cpuMillicores, memoryMB)
}

func byocManagedUsage(workspaceID string, state *model.PoolState, machine *model.AgentTokenState, hourlyCostMicros int64, duration time.Duration, costCents float64, start, end time.Time) managedUsage {
	provider := "byoc"
	cloud := ""
	poolName := ""
	if state != nil && state.BYOC != nil {
		cloud = state.BYOC.Provider
	}
	if cloud == "" && state != nil {
		cloud = string(state.Source.Canonical())
	}
	if state != nil {
		poolName = state.Name
	}
	if machine == nil {
		machine = &model.AgentTokenState{}
	}
	return managedUsage{
		WorkspaceID:        workspaceID,
		PoolName:           poolName,
		ReservationID:      "byoc:" + machine.MachineID,
		Provider:           provider,
		Cloud:              cloud,
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
	if state == nil {
		return false
	}
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
	if reservation == nil || !reservation.Managed() {
		return time.Time{}, time.Time{}, false
	}
	if reservation.Status == model.ReservationDeleted ||
		reservation.Status == model.ReservationFailed ||
		reservation.Status == model.ReservationTerminating {
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

func (s *Service) reconcileReservationStatus(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, vendors map[string]model.Vendor, now time.Time) bool {
	if reservation.Status == model.ReservationDeleted || reservation.Status == model.ReservationFailed || reservation.Provider == "" {
		return false
	}
	if !reservation.LastStatusCheckAt.IsZero() && now.Sub(reservation.LastStatusCheckAt) < reconcileStatusCheckInterval {
		return false
	}
	vendor := vendors[reservation.Provider]
	if vendor == nil {
		reservation.LastStatusCheckAt = now
		reservation.LastError = fmt.Sprintf("vendor %q is not configured", reservation.Provider)
		return true
	}
	current, err := vendor.GetReservation(ctx, computeReservationInstanceID(*reservation))
	reservation.LastStatusCheckAt = now
	if err != nil {
		reservation.LastError = err.Error()
		return true
	}
	if current == nil {
		return true
	}

	changed := reservation.Status != current.Status ||
		reservation.LastStatusMessage != current.LastStatusMessage ||
		reservation.MachineID != current.MachineID ||
		(current.Cloud != "" && reservation.Cloud != current.Cloud) ||
		(current.Region != "" && reservation.Region != current.Region) ||
		reservation.ExpiresAt != current.ExpiresAt
	reservation.Status = current.Status
	if current.Cloud != "" {
		reservation.Cloud = current.Cloud
	}
	if current.Region != "" {
		reservation.Region = current.Region
	}
	reservation.LastStatusMessage = current.LastStatusMessage
	if current.MachineID != "" {
		reservation.MachineID = current.MachineID
	}
	if !current.ExpiresAt.IsZero() {
		reservation.ExpiresAt = current.ExpiresAt
	}
	reservation.LastError = ""
	if changed {
		s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, types.EventComputeActionReservationStatusUpdated, string(reservation.Status)))
	}
	return changed
}

func (s *Service) reconcileReservationExpiry(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, vendors map[string]model.Vendor, now time.Time) bool {
	if !reservation.Managed() || reservation.ExpiresAt.IsZero() || reservation.ExpiresAt.After(now) {
		return false
	}
	return s.terminateReservation(ctx, workspaceID, state, reservation, vendors, reconcileReasonReservationExpired, "reservation ttl expired")
}

func (s *Service) reconcileClosedReservationMachine(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation) bool {
	if reservation.MachineID == "" {
		return false
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, state.Name, reservation.MachineID)
	if err != nil {
		reservation.LastError = err.Error()
		return true
	}
	if machine == nil {
		return false
	}
	if err := s.removePrivateMachine(ctx, machine); err != nil {
		reservation.LastError = err.Error()
		return true
	}
	reservation.LastError = ""
	return true
}

func (s *Service) reconcileTerminatingReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, vendors map[string]model.Vendor, now time.Time) bool {
	if reservation.Status != model.ReservationTerminating {
		return false
	}
	if !reservation.LastReconcileAt.IsZero() && now.Sub(reservation.LastReconcileAt) < reconcileStatusCheckInterval {
		return false
	}
	reservation.LastReconcileAt = now
	s.terminateReservation(ctx, workspaceID, state, reservation, vendors, firstNonEmpty(reservation.TerminatingReason, "terminating"), firstNonEmpty(reservation.LastStatusMessage, "reservation terminating"))
	return true
}

func (s *Service) terminateManagedReservations(ctx context.Context, workspaceID string, state *model.PoolState, reason, message string) bool {
	vendors := s.computeVendors()
	changed := false
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservationClosed(reservation.Status) || reservation.Status == model.ReservationTerminating {
			continue
		}
		changed = s.terminateReservation(ctx, workspaceID, state, reservation, vendors, reason, message) || changed
	}
	return changed
}

func (s *Service) terminateReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation, vendors map[string]model.Vendor, reason, message string) bool {
	if reservation.Status == model.ReservationDeleted || reservation.Status == model.ReservationFailed {
		return false
	}
	vendor := vendors[reservation.Provider]
	if vendor == nil {
		reservation.Status = model.ReservationTerminating
		reservation.TerminatingReason = reason
		reservation.LastError = fmt.Sprintf("vendor %q is not configured", reservation.Provider)
		return true
	}
	if err := vendor.DeleteReservation(ctx, computeReservationInstanceID(*reservation)); err != nil {
		reservation.Status = model.ReservationTerminating
		reservation.TerminatingReason = reason
		reservation.LastError = err.Error()
		return true
	}
	reservation.Status = model.ReservationDeleted
	reservation.TerminatingReason = reason
	reservation.LastStatusMessage = message
	reservation.LastError = ""
	s.emitComputeEvent(types.EventComputePool, computeReservationEvent(workspaceID, state, *reservation, types.EventComputeActionReservationTerminating, reason))
	return true
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
// that have no open managed reservation.
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
		if reason == reconcileReasonCreditExhausted && machine != nil && machine.Schedulable {
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
	if err := s.containerRepo.UpdateContainerStatus(containerID, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending); err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("failed to mark container stopping on disabled worker")
	}
}

func (s *Service) emitCreditExhaustedEvent(workspaceID string, state *model.PoolState, decision billingDecision, message string) {
	event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolCreditExhausted, "terminating")
	event.Message = message
	if event.Attrs == nil {
		event.Attrs = map[string]string{}
	}
	event.Attrs["available_cents"] = fmt.Sprintf("%d", decision.AvailableCents)
	event.Attrs["required_cents"] = fmt.Sprintf("%d", decision.RequiredCents)
	s.emitComputeEvent(types.EventComputePool, event)
}

func computeReservationEvent(workspaceID string, state *model.PoolState, reservation model.Reservation, action, status string) types.EventComputeSchema {
	event := computePoolEvent(workspaceID, state, action, status)
	if event.Attrs == nil {
		event.Attrs = map[string]string{}
	}
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
