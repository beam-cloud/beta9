package compute

import (
	"context"
	"fmt"

	model "github.com/beam-cloud/beta9/pkg/compute"
)

func (s *Service) invalidateJoinedAgents(ctx context.Context, join *model.JoinTokenState) error {
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, join.WorkspaceID, join.PoolName)
	if err != nil {
		return err
	}
	for _, machine := range machines {
		if machine == nil || machine.JoinTokenHash != "" && machine.JoinTokenHash != join.TokenHash {
			continue
		}
		slots, err := s.computeRepo.ListAgentWorkerSlotStates(ctx, machine.WorkspaceID, machine.PoolName, machine.MachineID)
		if err != nil {
			return err
		}
		for _, slot := range slots {
			if slot == nil || slot.WorkerTokenID == "" {
				continue
			}
			revoker, ok := s.backendRepo.(interface {
				RevokeTokenByExternalId(context.Context, string) error
			})
			if !ok {
				return fmt.Errorf("worker credential revocation is unavailable")
			}
			if err := revoker.RevokeTokenByExternalId(ctx, slot.WorkerTokenID); err != nil {
				return err
			}
		}
		if err := s.computeRepo.DeleteAgentMachineState(ctx, machine.WorkspaceID, machine.PoolName, machine.MachineID); err != nil {
			return err
		}
	}
	return nil
}
