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

// stoppingLease is how a durable-disk container's STOPPING state outlives its
// worker. The gateway starts no replacement while it lasts.
type stoppingLease struct {
	expirySeconds int64
	refresh       time.Duration
	// heartbeat renews on every refresh rather than only after progress.
	heartbeat bool
}

// A snapshot disk's long lease keeps its replacement from restoring an older
// snapshot than the one being published, so it lapses only once progress
// stops. A journal fences its own writers, so a journaled disk's lease only
// has to show that its worker is alive: a dead worker's replacement then waits
// about one journal lease rather than the snapshot disk's lease.
var (
	snapshotDiskStoppingLease  = stoppingLease{types.ContainerStateTtlSWhileStopping, durableDiskProgressRefreshInterval, false}
	journaledDiskStoppingLease = stoppingLease{60, 20 * time.Second, true}
)

// containerStoppingLease is the lease of an exited durable-disk container. An
// eviction victim keeps the long lease whatever its disks: it holds its
// resources until finalization ends, and the worker's capacity counts them
// only while its state exists.
func (s *Worker) containerStoppingLease(containerID string, request *types.ContainerRequest) stoppingLease {
	if s.containerInstances != nil {
		if instance, exists := s.containerInstances.Get(containerID); exists && instance != nil {
			if _, reason := instance.lifecycleState(); reason == types.StopContainerReasonEvicted {
				return snapshotDiskStoppingLease
			}
		}
	}
	return durableDiskStoppingLease(request)
}

func durableDiskStoppingLease(request *types.ContainerRequest) stoppingLease {
	if request == nil {
		return snapshotDiskStoppingLease
	}
	journaled := false
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk == nil || mount.ReadOnly {
			continue
		}
		if !journaledDiskMount(request, mount) {
			return snapshotDiskStoppingLease
		}
		journaled = true
	}
	if journaled {
		return journaledDiskStoppingLease
	}
	return snapshotDiskStoppingLease
}

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
	progressCtx, stopProgress := s.durableDiskStoppingProgressContext(ctx, containerID, s.containerStoppingLease(containerID, request))
	defer stopProgress()

	_, syncErr := s.syncDurableDiskMounts(progressCtx, request, durableDiskFinalSyncMode(exitCode))
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

// exitedCleanly separates a container that exited cleanly or was stopped by
// the platform from one that failed on its own.
func exitedCleanly(exitCode int) bool {
	switch types.ContainerExitCode(exitCode) {
	case types.ContainerExitCodeSuccess,
		types.ContainerExitCodeScheduler,
		types.ContainerExitCodeTtl,
		types.ContainerExitCodeUser,
		types.ContainerExitCodeAdmin,
		types.ContainerExitCodeEvicted:
		return true
	default:
		return false
	}
}

func durableDiskFinalSyncMode(exitCode int) durableDiskSyncMode {
	if exitedCleanly(exitCode) {
		return durableDiskSyncFinal
	}
	return durableDiskSyncFailed
}

// durableDiskSyncFailureExitCode reports a failed final sync as a failure
// unless the container already failed. An eviction keeps its code: replica
// controllers read it as the authoritative sign of an eviction.
func durableDiskSyncFailureExitCode(exitCode int) int {
	if exitedCleanly(exitCode) && types.ContainerExitCode(exitCode) != types.ContainerExitCodeEvicted {
		return int(types.ContainerExitCodeUnknownError)
	}
	return exitCode
}

func (s *Worker) durableDiskStoppingProgressContext(ctx context.Context, containerID string, lease stoppingLease) (context.Context, func()) {
	refreshInterval := lease.refresh
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
			s.refreshDurableDiskStoppingLeaseOnce(containerID, lease.expirySeconds)
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
				} else if lease.heartbeat {
					s.refreshDurableDiskStoppingLeaseOnce(containerID, lease.expirySeconds)
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

func (s *Worker) refreshDurableDiskStoppingLeaseOnce(containerID string, expirySeconds int64) {
	if s.containerRepoClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerRepositoryAttemptTimeout)
	defer cancel()
	_, err := handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(ctx, &pb.UpdateContainerStatusRequest{
		ContainerId:   containerID,
		Status:        string(types.ContainerStatusStopping),
		ExpirySeconds: expirySeconds,
	}))
	if err != nil && !(&types.ErrContainerStateNotFound{}).From(err) {
		log.Debug().Str("container_id", containerID).Err(err).Msg("failed to refresh durable disk finalization lease")
	}
}
