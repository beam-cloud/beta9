package compute

import (
	"context"
	"fmt"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	reconcileStatusCheckInterval  = time.Minute
	reconcileBillingCheckInterval = time.Minute

	reconcileReasonAgentDisconnected   = "agent_disconnected"
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
	if s == nil || s.computeRepo == nil {
		return nil
	}
	states, err := s.computeRepo.ListAllPoolStates(ctx, 0)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, state := range states {
		if state == nil || state.WorkspaceID == "" || state.Name == "" {
			continue
		}
		if err := s.reconcilePool(ctx, state.WorkspaceID, state, now); err != nil {
			log.Warn().
				Err(err).
				Str("workspace_id", state.WorkspaceID).
				Str("pool_name", state.Name).
				Msg("failed to reconcile managed compute pool")
		}
	}
	return nil
}

func (s *Service) reconcilePool(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time) error {
	changed := false
	if s.poolNeedsManagedBilling(state, now) {
		changed = s.reconcileBilling(ctx, workspaceID, state, now) || changed
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

func (s *Service) poolNeedsManagedBilling(state *model.PoolState, now time.Time) bool {
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
	changed := s.recordManagedUsage(ctx, workspaceID, state, now)
	if !changed && billingCheckFresh(state, now) {
		return changed
	}

	decision, err := s.billing.CheckBalance(ctx, workspaceID)
	if err != nil {
		s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolBillingDegraded, "degraded"))
		return changed
	}

	for i := range state.Reservations {
		state.Reservations[i].LastBillingCheckAt = now
	}

	if !decision.OK {
		reason := firstNonEmpty(decision.Message, "managed compute credits exhausted")
		s.emitCreditExhaustedEvent(workspaceID, state, decision, reason)
		s.disablePoolMachines(ctx, workspaceID, state.Name, reconcileReasonCreditExhausted)
		return s.terminateManagedReservations(ctx, workspaceID, state, reconcileReasonCreditExhausted, reason) || changed
	}

	return changed
}

func billingCheckFresh(state *model.PoolState, now time.Time) bool {
	for _, reservation := range state.Reservations {
		if reservation.Managed() && reservation.ActiveAt(now) {
			return !reservation.LastBillingCheckAt.IsZero() && now.Sub(reservation.LastBillingCheckAt) < reconcileBillingCheckInterval
		}
	}
	return false
}

func (s *Service) recordManagedUsage(ctx context.Context, workspaceID string, state *model.PoolState, now time.Time) bool {
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
		usage := managedUsage{
			WorkspaceID:        workspaceID,
			PoolName:           state.Name,
			ReservationID:      reservation.ID,
			Provider:           reservation.Provider,
			ProviderInstanceID: computeReservationInstanceID(*reservation),
			MachineID:          reservation.MachineID,
			GPU:                reservation.GPU,
			GPUCount:           reservation.GPUCount,
			CPUMillicores:      reservation.CPUMillicores,
			MemoryMB:           reservation.MemoryMB,
			HourlyCostMicros:   reservation.HourlyCostMicros,
			DurationSeconds:    duration.Seconds(),
			CostCents:          managedCostCents(reservation.HourlyCostMicros, duration),
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
		if err := s.recordManagedUsageMetrics(usage); err != nil {
			reservation.LastError = err.Error()
		}
		changed = true
	}
	return changed
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
		"provider_instance_id": usage.ProviderInstanceID,
		"machine_id":           usage.MachineID,
		"gpu":                  usage.GPU,
		"gpu_count":            usage.GPUCount,
		"cpu_millicores":       usage.CPUMillicores,
		"memory_mb":            usage.MemoryMB,
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
		reservation.ExpiresAt != current.ExpiresAt
	reservation.Status = current.Status
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
		if !reservation.Managed() || !reservation.ActiveAt(time.Now()) {
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
		changed = s.disableMachineWorker(ctx, machine, reconcileReasonMachineDisconnected) || changed
	}
	return changed
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
		return false
	}
	if err := s.workerRepo.UpdateWorkerStatus(workerID, types.WorkerStatusDisabled); err != nil {
		return false
	}
	s.stopWorkerContainers(ctx, workerID, reason)
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
	event.Attrs["instance_id"] = reservation.InstanceID
	event.Attrs["offer_id"] = reservation.OfferID
	event.Attrs["terminating_reason"] = reservation.TerminatingReason
	event.Attrs["last_status_message"] = reservation.LastStatusMessage
	return event
}
