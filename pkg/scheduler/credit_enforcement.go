package scheduler

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// StartCreditEnforcement periodically stops the running containers of
// workspaces that no longer have prepaid credit. The gate on Run only covers
// new containers; long-lived ones (deployments, sandboxes, pods) would
// otherwise keep burning through a zero balance until they exited on their
// own. Blocks until the scheduler context is cancelled.
func (s *Scheduler) StartCreditEnforcement() {
	if s.creditGate == nil {
		return
	}

	interval := s.config.GatewayService.CreditGate.EnforceIntervalOrDefault()
	if interval <= 0 {
		log.Info().Msg("credit gate: enforcement sweep disabled")
		return
	}

	log.Info().Dur("interval", interval).Msg("credit gate: starting enforcement sweep")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ran, err := s.creditGate.withEnforcementLease(s.ctx, s.enforceCredits)
			if err != nil {
				log.Error().Err(err).Msg("credit gate: enforcement sweep failed")
			}
			if !ran {
				log.Debug().Msg("credit gate: enforcement sweep running on another replica")
			}
		}
	}
}

// enforceCredits stops every managed container whose workspace is denied by
// the credit gate. Containers on the workspace's own private pools are left
// alone, mirroring the exemption in Run.
func (s *Scheduler) enforceCredits(ctx context.Context) error {
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return err
	}

	privatePools := s.privatePoolNames()
	containersByWorkspace := map[string][]types.ContainerState{}
	for _, worker := range workers {
		if worker == nil || privatePools[worker.PoolName] {
			continue
		}

		containers, err := s.containerRepo.GetActiveContainersByWorkerId(worker.Id)
		if err != nil {
			log.Warn().Err(err).Str("worker_id", worker.Id).Msg("credit gate: failed to list worker containers")
			continue
		}

		for _, container := range containers {
			if container.WorkspaceId == "" {
				continue
			}
			containersByWorkspace[container.WorkspaceId] = append(containersByWorkspace[container.WorkspaceId], container)
		}
	}

	stopped := 0
	for workspaceId, containers := range containersByWorkspace {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		decision, err := s.creditGate.Decision(ctx, workspaceId)
		if err != nil {
			// Fail-closed with billing unreachable. Rejecting *new* work is
			// the right call there; killing running work on a guess is not.
			log.Warn().Err(err).Str("workspace_id", workspaceId).Msg("credit gate: skipping enforcement, no decision available")
			continue
		}
		if decision.OK {
			continue
		}

		log.Info().
			Str("workspace_id", workspaceId).
			Str("error_code", decision.ErrorCode).
			Int64("available_cents", decision.AvailableCents).
			Int("containers", len(containers)).
			Msg("credit gate: stopping containers for workspace without credit")

		for _, container := range containers {
			err := s.Stop(&types.StopContainerArgs{
				ContainerId: container.ContainerId,
				Reason:      types.StopContainerReasonInsufficientCredits,
			})
			if err != nil {
				log.Warn().Err(err).Str("container_id", container.ContainerId).Msg("credit gate: failed to stop container")
				continue
			}
			stopped++
		}
	}

	if stopped > 0 {
		log.Info().Int("stopped", stopped).Msg("credit gate: enforcement sweep complete")
	}
	return nil
}

// privatePoolNames returns the names of pools whose containers are exempt
// from the credit gate (a workspace's own hardware).
func (s *Scheduler) privatePoolNames() map[string]bool {
	names := map[string]bool{}
	if s.workerPoolManager == nil {
		return names
	}

	s.workerPoolManager.poolMap.Range(func(key string, pool *WorkerPool) bool {
		if pool != nil && pool.Config.Mode == types.PoolModePrivate {
			names[key] = true
			names[pool.Name] = true
		}
		return true
	})
	return names
}
