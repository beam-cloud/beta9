package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

const machineReleasedMessage = "machine released from private pool"

func (s *Service) releasePrivateMachine(ctx context.Context, machine *model.AgentTokenState) error {
	if machine == nil {
		return nil
	}
	if s == nil || s.computeRepo == nil {
		return errors.New("compute repository is unavailable")
	}
	if err := s.releaseBYOCProviderMachine(ctx, machine); err != nil {
		reconciled, reconcileErr := s.reconcileBYOCMachineReleaseFailure(ctx, machine, err)
		if reconcileErr != nil {
			return reconcileErr
		}
		if reconciled {
			return nil
		}
		return err
	}
	if err := s.releaseManagedMachineReservation(ctx, machine); err != nil {
		return err
	}
	return s.removePrivateMachine(ctx, machine)
}

func (s *Service) releaseBYOCProviderMachine(ctx context.Context, machine *model.AgentTokenState) error {
	if machine == nil {
		return nil
	}
	state, err := s.getPrivatePoolState(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil || state == nil || state.BYOC == nil {
		return err
	}
	provider, err := byocProviderForState(state)
	if err != nil {
		return err
	}
	return provider.ReleaseMachine(ctx, byocProviderReleaseMachineInput{
		WorkspaceID:  machine.WorkspaceID,
		Pool:         state,
		ProviderData: state.BYOC,
		Machine:      machine,
	})
}

func (s *Service) reconcileBYOCMachineReleaseFailure(ctx context.Context, machine *model.AgentTokenState, releaseErr error) (bool, error) {
	if machine == nil || !byocReleaseErrorAllowsLocalReconcile(releaseErr) {
		return false, nil
	}
	state, err := s.getPrivatePoolState(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil || state == nil || state.BYOC == nil {
		return false, err
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil {
		return false, err
	}
	deleted, err := s.cleanupDeletedBYOCPoolIfNeeded(ctx, machine.WorkspaceID, state, machines, time.Now().UTC())
	if err != nil || deleted {
		return deleted, err
	}
	if model.AgentMachineConnected(machine, time.Now().UTC()) {
		return false, nil
	}
	return true, s.removePrivateMachine(ctx, machine)
}

func byocReleaseErrorAllowsLocalReconcile(err error) bool {
	return errors.Is(err, errBYOCProviderResourceNotFound) ||
		errors.Is(err, errBYOCProviderControlUnavailable)
}

func (s *Service) removePrivateMachine(ctx context.Context, machine *model.AgentTokenState) error {
	if machine == nil {
		return nil
	}
	if s == nil || s.computeRepo == nil {
		return errors.New("compute repository is unavailable")
	}
	s.flushBYOCMachineManagedUsage(ctx, machine)
	if err := s.markPrivateMachineUnschedulable(ctx, machine); err != nil {
		return err
	}
	if err := s.removePrivateMachineWorker(machine); err != nil {
		return err
	}
	if err := s.computeRepo.DeleteAgentMachineState(ctx, machine.WorkspaceID, machine.PoolName, machine.MachineID); err != nil {
		return err
	}
	if s.containerRepo != nil {
		// Clean up the machine's backend routes so they do not leak after release.
		if err := s.containerRepo.DeleteBackendRoutesByMachine(ctx, machine.WorkspaceID, machine.PoolName, machine.MachineID); err != nil {
			return err
		}
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   time.Now().UTC(),
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		MachineID:   machine.MachineID,
		WorkerID:    workerID,
		Action:      types.EventComputeActionMachineReleased,
		Status:      string(types.MachineStatusDisabled),
		Message:     machineReleasedMessage,
	})
	return nil
}

func (s *Service) flushBYOCMachineManagedUsage(ctx context.Context, machine *model.AgentTokenState) {
	if s == nil || s.billing == nil || machine == nil {
		return
	}
	state, err := s.getPrivatePoolState(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil || state == nil || state.BYOC == nil {
		return
	}
	s.recordBYOCManagedUsage(ctx, machine.WorkspaceID, state, time.Now().UTC().Truncate(time.Second), true)
}

func (s *Service) markPrivateMachineUnschedulable(ctx context.Context, machine *model.AgentTokenState) error {
	if !machine.Schedulable {
		return nil
	}
	machine.Schedulable = false
	return s.saveComputeAgentTokenState(ctx, machine)
}

func (s *Service) releaseManagedMachineReservation(ctx context.Context, machine *model.AgentTokenState) error {
	return s.withPoolStateLock(ctx, machine.WorkspaceID, machine.PoolName, func(lockCtx context.Context) error {
		state, err := s.getPrivatePoolState(lockCtx, machine.WorkspaceID, machine.PoolName)
		if err != nil || state == nil {
			return err
		}
		reservation, err := managedReservationForMachineRelease(state, machine.MachineID)
		if err != nil {
			return err
		}
		if reservation == nil {
			return nil
		}
		return s.releaseManagedReservation(lockCtx, machine.WorkspaceID, state, reservation)
	})
}

// releaseManagedReservation closes out a reservation. Callers must hold the
// pool state lock and have loaded `state` under it.
func (s *Service) releaseManagedReservation(ctx context.Context, workspaceID string, state *model.PoolState, reservation *model.Reservation) error {
	if state == nil || reservation == nil || !reservation.Managed() {
		return nil
	}

	now := time.Now().UTC().Truncate(time.Second)
	changed := closeReservationBillingWindow(reservation, now)
	// Force-record the final window; a terminating reservation never gets
	// another chance to bill its sub-cent tail.
	changed = s.recordManagedUsage(ctx, workspaceID, state, now, true) || changed
	s.revokeReservationJoinToken(ctx, reservation)
	changed = markReservationTerminating(reservation, reconcileReasonMachineReleased, machineReleasedMessage) || changed
	if changed {
		// Persist terminating state before provider deletion so reconcile can retry cleanup.
		state.UpdatedAt = now
		if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
			return err
		}
	}

	if s.terminateReservation(ctx, workspaceID, state, reservation, s.computeVendors(), reconcileReasonMachineReleased, machineReleasedMessage) {
		state.UpdatedAt = time.Now().UTC()
		return s.savePrivatePoolState(ctx, workspaceID, state)
	}
	return nil
}

func (s *Service) revokeReservationJoinToken(ctx context.Context, reservation *model.Reservation) {
	if s == nil || s.computeRepo == nil || reservation == nil || reservation.RegistrationTokenHash == "" {
		return
	}
	_ = s.revokeComputeJoinTokenHash(ctx, reservation.RegistrationTokenHash)
}

func (s *Service) removePrivateMachineWorker(machine *model.AgentTokenState) error {
	if s.workerRepo == nil {
		return nil
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	worker, err := s.workerRepo.GetWorkerById(workerID)
	if err != nil {
		notFoundErr := &types.ErrWorkerNotFound{}
		if !notFoundErr.From(err) {
			return err
		}
	}
	if worker != nil && worker.MachineId == machine.MachineID && worker.PoolName == machine.PoolName {
		if err := s.workerRepo.RemoveWorker(workerID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) assignManagedReservationToMachine(ctx context.Context, state *model.PoolState, token *model.JoinTokenState, machine *model.AgentTokenState) error {
	if machine == nil {
		return nil
	}
	if token != nil && token.ManagedPoolInstanceID != "" {
		return nil
	}
	if s == nil || s.computeRepo == nil {
		return s.assignManagedReservationToMachineLocked(ctx, state, token, machine)
	}
	return s.withPoolStateLock(ctx, machine.WorkspaceID, machine.PoolName, func(lockCtx context.Context) error {
		fresh, err := s.getPrivatePoolState(lockCtx, machine.WorkspaceID, machine.PoolName)
		if err != nil {
			return err
		}
		if fresh == nil {
			return errors.New("pool no longer exists")
		}
		return s.assignManagedReservationToMachineLocked(lockCtx, fresh, token, machine)
	})
}

func (s *Service) assignManagedReservationToMachineLocked(ctx context.Context, state *model.PoolState, token *model.JoinTokenState, machine *model.AgentTokenState) error {
	reservation := managedReservationForToken(state, token)
	if reservation == nil {
		if token != nil && token.MachineID != "" {
			return fmt.Errorf("managed reservation for machine %q is not active", token.MachineID)
		}
		return nil
	}
	if reservation.MachineID != "" && reservation.MachineID != machine.MachineID {
		return fmt.Errorf("managed reservation %q is already linked to machine %q", reservation.ID, reservation.MachineID)
	}
	if !attachReservationToMachine(reservation, machine.MachineID) {
		return nil
	}
	state.UpdatedAt = time.Now().UTC()
	return s.savePrivatePoolState(ctx, machine.WorkspaceID, state)
}

func managedReservationForToken(state *model.PoolState, token *model.JoinTokenState) *model.Reservation {
	if state == nil || token == nil || token.TokenHash == "" {
		return nil
	}
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.RegistrationTokenHash != token.TokenHash {
			continue
		}
		if reservationClosed(reservation.Status) || reservation.Status == model.ReservationTerminating {
			continue
		}
		return reservation
	}
	return nil
}

func managedReservationForMachine(state *model.PoolState, machineID string) *model.Reservation {
	if state == nil || machineID == "" {
		return nil
	}
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.MachineID != machineID {
			continue
		}
		if reservationClosed(reservation.Status) {
			continue
		}
		return reservation
	}
	return nil
}

func managedReservationForMachineRelease(state *model.PoolState, machineID string) (*model.Reservation, error) {
	if reservation := managedReservationForMachine(state, machineID); reservation != nil {
		return reservation, nil
	}

	var unlinked *model.Reservation
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.MachineID != "" || reservationClosed(reservation.Status) {
			continue
		}
		if unlinked != nil {
			return nil, fmt.Errorf("managed reservation for machine %q is ambiguous", machineID)
		}
		unlinked = reservation
	}
	if unlinked != nil {
		attachReservationToMachine(unlinked, machineID)
		return unlinked, nil
	}

	if hasOpenManagedReservations(state) {
		return nil, fmt.Errorf("managed reservation for machine %q is not linked", machineID)
	}
	return nil, nil
}

func managedReservationIndexForDeleteTarget(state *model.PoolState, targetID string) int {
	if state == nil || targetID == "" {
		return -1
	}
	for i := range state.Reservations {
		reservation := &state.Reservations[i]
		if !reservation.Managed() || reservation.Status == model.ReservationDeleted {
			continue
		}
		if reservation.ID == targetID ||
			reservation.MachineID == targetID ||
			reservation.InstanceID == targetID ||
			reservation.Name == targetID {
			return i
		}
	}
	return -1
}

func hasOpenManagedReservations(state *model.PoolState) bool {
	if state == nil {
		return false
	}
	for i := range state.Reservations {
		reservation := state.Reservations[i]
		if !reservation.Managed() {
			continue
		}
		if reservationClosed(reservation.Status) {
			continue
		}
		return true
	}
	return false
}

func attachReservationToMachine(reservation *model.Reservation, machineID string) bool {
	if reservation == nil || machineID == "" || reservation.MachineID == machineID {
		return false
	}
	reservation.MachineID = machineID
	return true
}

func markReservationTerminating(reservation *model.Reservation, reason, message string) bool {
	if reservation == nil || reservationClosed(reservation.Status) {
		return false
	}
	changed := reservation.Status != model.ReservationTerminating ||
		reservation.TerminatingReason != reason ||
		reservation.LastStatusMessage != message
	reservation.Status = model.ReservationTerminating
	reservation.TerminatingReason = reason
	reservation.LastStatusMessage = message
	return changed
}

func reservationClosed(status model.ReservationStatus) bool {
	return status == model.ReservationDeleted || status == model.ReservationFailed
}
