package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const (
	durableDiskCleanupGrace            = 30 * time.Second
	durableDiskProgressRefreshInterval = 30 * time.Second
)

func (s *Worker) durableDiskCleanupContext() context.Context {
	return context.WithoutCancel(s.durableDiskContext(nil))
}

func (s *Worker) durableDiskFinalizationContext(request *types.ContainerRequest) (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.durableDiskCleanupContext(), durableDiskCleanupBudget(request))
}

func durableDiskCleanupBudget(request *types.ContainerRequest) time.Duration {
	// Each disk is finalized serially. Preserve the complete lock and transfer
	// allowance for every mount, plus setup allowance for each one.
	budget := time.Duration(0)
	if request != nil {
		for _, mount := range request.Mounts {
			if mount.DurableDisk == nil {
				continue
			}
			sizeBytes, _ := durableDiskSizeBytes(mount.DurableDisk.Size)
			budget = addDurableDiskCleanupBudget(budget, durableDiskLockWait)
			budget = addDurableDiskCleanupBudget(budget, durableDiskTransferTimeout(sizeBytes))
			budget = addDurableDiskCleanupBudget(budget, durableDiskCleanupGrace)
		}
	}
	return budget
}

func addDurableDiskCleanupBudget(current, allowance time.Duration) time.Duration {
	const maximum = time.Duration(1<<63 - 1)
	if allowance > maximum-current {
		return maximum
	}
	return current + allowance
}

func (s *Worker) finalizeDurableDiskMounts(containerID string, request *types.ContainerRequest, exitCode int, exitReported bool) (int, bool) {
	ctx, cancel := s.durableDiskFinalizationContext(request)
	defer cancel()
	return s.finalizeDurableDiskMountsWithContext(ctx, containerID, request, exitCode, exitReported)
}

func (s *Worker) finalizeDurableDiskMountsWithContext(ctx context.Context, containerID string, request *types.ContainerRequest, exitCode int, exitReported bool) (finalExitCode int, finalExitReported bool) {
	finalExitCode, finalExitReported = exitCode, exitReported
	progressCtx, stopProgress := s.durableDiskStoppingProgressContext(
		ctx,
		containerID,
		durableDiskProgressRefreshInterval,
	)
	defer stopProgress()

	_, syncErr := s.syncDurableDiskMounts(progressCtx, request, durableDiskSyncFinal)
	// Final sync can consume or cancel its transfer budget. Detach gets a fresh
	// cleanup context so the NBD is still released after the container exits.
	detachErr := s.detachFinalQcowDurableDisks(request)
	if finalErr := errors.Join(syncErr, detachErr); finalErr != nil {
		log.Error().Str("container_id", containerID).Err(finalErr).Msg("failed to finalize durable disks during container cleanup")
		finalExitCode = durableDiskSyncFailureExitCode(exitCode)
		if finalExitCode != exitCode {
			s.setLocalContainerExitCode(containerID, finalExitCode)
			finalExitReported = false
		}
	}
	return finalExitCode, finalExitReported
}

func (s *Worker) detachFinalQcowDurableDisks(request *types.ContainerRequest) error {
	if request == nil || s.diskManager == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(s.durableDiskCleanupContext(), durableDiskCleanupGrace)
	defer cancel()

	var errs error
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if isQcowDurableDiskMount(mount) {
			errs = errors.Join(errs, s.detachQcowDurableDiskMount(ctx, request, mount))
		}
	}
	return errs
}

func (s *Worker) cleanupIdleQcowVolumes() {
	if s.diskManager == nil || s.containerInstances == nil {
		return
	}
	s.containerLock.Lock()
	defer s.containerLock.Unlock()
	if s.containerInstances.Len() != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(s.durableDiskCleanupContext(), durableDiskCleanupGrace)
	defer cancel()
	if err := s.diskManager.DetachAll(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to detach idle qcow volumes")
	}
}

func durableDiskSyncFailureExitCode(exitCode int) int {
	switch types.ContainerExitCode(exitCode) {
	case types.ContainerExitCodeSuccess,
		types.ContainerExitCodeScheduler,
		types.ContainerExitCodeTtl,
		types.ContainerExitCodeUser,
		types.ContainerExitCodeAdmin:
		return int(types.ContainerExitCodeUnknownError)
	default:
		return exitCode
	}
}

func (s *Worker) durableDiskStoppingProgressContext(ctx context.Context, containerID string, refreshInterval time.Duration) (context.Context, func()) {
	if refreshInterval <= 0 {
		refreshInterval = durableDiskProgressRefreshInterval
	}

	events := make(chan struct{}, 1)
	progressCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var logicalBytes, files, chunks atomic.Int64
	report := func(progress durableDiskProgressEvent) {
		logicalBytes.Add(progress.logicalBytes)
		files.Add(progress.files)
		chunks.Add(progress.chunks)
		select {
		case events <- struct{}{}:
		default:
		}
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		dirty := false
		first := true
		refresh := func() {
			s.refreshDurableDiskStoppingLeaseOnce(containerID)
			log.Info().
				Str("container_id", containerID).
				Int64("logical_bytes", logicalBytes.Load()).
				Int64("files", files.Load()).
				Int64("chunks", chunks.Load()).
				Msg("durable disk finalization progress")
		}
		for {
			select {
			case <-events:
				dirty = true
				if first {
					refresh()
					first = false
					dirty = false
				}
			case <-ticker.C:
				if dirty {
					refresh()
					dirty = false
				}
			case <-progressCtx.Done():
				return
			}
		}
	}()

	return withDurableDiskProgressReporter(progressCtx, report), func() {
		cancel()
		<-done
	}
}

func (s *Worker) refreshDurableDiskStoppingLeaseOnce(containerID string) {
	if s.containerRepoClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerRepositoryAttemptTimeout)
	defer cancel()
	_, err := handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(ctx, &pb.UpdateContainerStatusRequest{
		ContainerId:   containerID,
		Status:        string(types.ContainerStatusStopping),
		ExpirySeconds: types.ContainerStateTtlSWhileStopping,
	}))
	if err != nil && !(&types.ErrContainerStateNotFound{}).From(err) {
		log.Debug().Str("container_id", containerID).Err(err).Msg("failed to refresh durable disk finalization lease")
	}
}
