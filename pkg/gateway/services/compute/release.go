package compute

import (
	"context"
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
	if err := s.releaseManagedMachineReservation(ctx, machine); err != nil {
		return err
	}
	if err := s.markPrivateMachineUnschedulable(ctx, machine); err != nil {
		return err
	}
	if err := s.removePrivateMachineWorker(machine); err != nil {
		return err
	}
	if err := s.computeRepo.DeleteAgentMachineState(ctx, machine.WorkspaceID, machine.PoolName, machine.MachineID); err != nil {
		return err
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

func (s *Service) markPrivateMachineUnschedulable(ctx context.Context, machine *model.AgentTokenState) error {
	if !machine.Schedulable {
		return nil
	}
	machine.Schedulable = false
	return s.saveComputeAgentTokenState(ctx, machine)
}

func (s *Service) releaseManagedMachineReservation(ctx context.Context, machine *model.AgentTokenState) error {
	state, err := s.getPrivatePoolState(ctx, machine.WorkspaceID, machine.PoolName)
	if err != nil || state == nil {
		return err
	}
	reservation := managedReservationForMachine(state, machine.MachineID)
	if reservation == nil {
		if hasOpenManagedReservations(state) {
			return fmt.Errorf("managed reservation for machine %q is not linked", machine.MachineID)
		}
		return nil
	}

	now := time.Now().UTC()
	changed := s.recordManagedUsage(ctx, machine.WorkspaceID, state, now)
	changed = markReservationTerminating(reservation, reconcileReasonMachineReleased, machineReleasedMessage) || changed
	if changed {
		// Persist terminating state before provider deletion so reconcile can retry cleanup.
		state.UpdatedAt = now
		if err := s.savePrivatePoolState(ctx, machine.WorkspaceID, state); err != nil {
			return err
		}
	}

	if s.terminateReservation(ctx, machine.WorkspaceID, state, reservation, s.computeVendors(), reconcileReasonMachineReleased, machineReleasedMessage) {
		state.UpdatedAt = time.Now().UTC()
		return s.savePrivatePoolState(ctx, machine.WorkspaceID, state)
	}
	return nil
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
	reservation := managedReservationForToken(state, token)
	if reservation == nil {
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
		if reservationClosed(reservation.Status) {
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
