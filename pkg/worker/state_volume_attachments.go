package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const stateVolumeAttachmentRenewInterval = 30 * time.Second

const (
	stateVolumeAttachmentRetryFloor = 10 * time.Millisecond
	stateVolumeAttachmentRetryCeil  = 5 * time.Second
)

type stateVolumeAttachmentRejectedError struct{ reason string }

func (e *stateVolumeAttachmentRejectedError) Error() string { return e.reason }

func stateVolumeAttachmentRenewalRejected(err error) bool {
	var rejected *stateVolumeAttachmentRejectedError
	return errors.As(err, &rejected)
}

func stateVolumeAttachmentRetryDelay(interval time.Duration, attempt int) time.Duration {
	base := interval / 4
	if base < stateVolumeAttachmentRetryFloor {
		base = stateVolumeAttachmentRetryFloor
	}
	if base > time.Second {
		base = time.Second
	}
	if attempt > 6 {
		attempt = 6
	}
	delay := base * time.Duration(1<<attempt)
	if delay > stateVolumeAttachmentRetryCeil {
		delay = stateVolumeAttachmentRetryCeil
	}
	// A bounded 0-25% jitter prevents a repository outage from synchronizing
	// every writer on a storage node while preserving the authoritative local
	// lease deadline as the hard fence boundary.
	jitterWindow := delay / 4
	if jitterWindow > 0 {
		delay += time.Duration(rand.Int64N(int64(jitterWindow) + 1))
	}
	if delay > stateVolumeAttachmentRetryCeil {
		return stateVolumeAttachmentRetryCeil
	}
	return delay
}

type stateVolumeAttachmentState struct {
	mu              sync.RWMutex
	leases          []*pb.StateVolumeLease
	expiresAt       time.Time
	prepareCtx      context.Context
	writerCtx       context.Context
	cancelPrepare   context.CancelCauseFunc
	cancelWriter    context.CancelCauseFunc
	fenceErr        error
	preparationDone chan struct{}
	preparationOnce sync.Once
	cleanupDone     chan struct{}
	cleanupErr      error
	cleanupOnce     sync.Once
	cancel          context.CancelFunc
	done            chan struct{}
	once            sync.Once
	fenceOnce       sync.Once
}

func validateStateVolumeWriterLeaseTuple(attachmentToken string, fencingToken int64) error {
	if parsed, err := uuid.Parse(attachmentToken); err != nil || parsed.String() != strings.ToLower(attachmentToken) {
		return fmt.Errorf("attachment_token must be a canonical UUID")
	}
	if fencingToken <= 0 {
		return fmt.Errorf("fencing_token must be positive")
	}
	return nil
}

func validateStateVolumeWriterLeaseIdentity(attachmentToken string, fencingToken, expiresAtUnix int64) error {
	if err := validateStateVolumeWriterLeaseTuple(attachmentToken, fencingToken); err != nil {
		return err
	}
	if expiresAtUnix <= time.Now().Unix() {
		return fmt.Errorf("writer lease is already expired")
	}
	return nil
}

func validateStateVolumeWriterLease(config *types.DurableDiskMountConfig) error {
	if config == nil {
		return fmt.Errorf("writer lease configuration is missing")
	}
	return validateStateVolumeWriterLeaseIdentity(config.AttachmentToken, config.FencingToken, config.LeaseExpiresAtUnix)
}

func stateVolumeWriterLeaseTuples(request *types.ContainerRequest, requireUnexpired bool) ([]*pb.StateVolumeLease, error) {
	if request == nil {
		return nil, nil
	}
	seen := make(map[string]struct{})
	leases := make([]*pb.StateVolumeLease, 0)
	appendLease := func(volumeID, attachmentToken string, fencingToken, expiresAtUnix int64) error {
		if parsed, err := uuid.Parse(volumeID); err != nil || parsed.String() != strings.ToLower(volumeID) {
			return fmt.Errorf("volume_id must be a canonical UUID")
		}
		if err := validateStateVolumeWriterLeaseTuple(attachmentToken, fencingToken); err != nil {
			return err
		}
		if requireUnexpired && expiresAtUnix <= time.Now().Unix() {
			return fmt.Errorf("writer lease is already expired")
		}
		if _, duplicate := seen[volumeID]; duplicate {
			return fmt.Errorf("duplicate writable state volume lease %q", volumeID)
		}
		seen[volumeID] = struct{}{}
		leases = append(leases, &pb.StateVolumeLease{VolumeId: volumeID, AttachmentToken: attachmentToken, FencingToken: fencingToken})
		return nil
	}
	if request.PersistentRoot != nil {
		if request.RootState == nil {
			return nil, fmt.Errorf("persistent root writer lease configuration is missing")
		}
		if err := appendLease(request.RootState.VolumeId, request.RootState.AttachmentToken, request.RootState.FencingToken, request.RootState.LeaseExpiresAtUnix); err != nil {
			return nil, fmt.Errorf("persistent root: %w", err)
		}
	}
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk == nil || mount.ReadOnly {
			continue
		}
		config := mount.DurableDisk
		if err := appendLease(config.VolumeId, config.AttachmentToken, config.FencingToken, config.LeaseExpiresAtUnix); err != nil {
			return nil, fmt.Errorf("durable disk %q: %w", config.Name, err)
		}
	}
	return leases, nil
}

func stateVolumeWriterLeases(request *types.ContainerRequest) ([]*pb.StateVolumeLease, error) {
	return stateVolumeWriterLeaseTuples(request, true)
}

func cloneStateVolumeLeases(leases []*pb.StateVolumeLease) []*pb.StateVolumeLease {
	cloned := make([]*pb.StateVolumeLease, 0, len(leases))
	for _, lease := range leases {
		if lease != nil {
			cloned = append(cloned, &pb.StateVolumeLease{
				VolumeId: lease.VolumeId, AttachmentToken: lease.AttachmentToken, FencingToken: lease.FencingToken,
			})
		}
	}
	return cloned
}

func (state *stateVolumeAttachmentState) updateDeadline(deadline time.Time) {
	state.mu.Lock()
	state.expiresAt = deadline
	state.mu.Unlock()
}

func (state *stateVolumeAttachmentState) tripFence(cause error) error {
	if state == nil {
		return cause
	}
	if cause == nil {
		cause = errors.New("state volume writer lease fenced")
	}
	state.mu.Lock()
	if state.fenceErr == nil {
		state.fenceErr = cause
	}
	result := state.fenceErr
	cancelPrepare := state.cancelPrepare
	cancelWriter := state.cancelWriter
	state.mu.Unlock()
	if cancelPrepare != nil {
		cancelPrepare(result)
	}
	if cancelWriter != nil {
		cancelWriter(result)
	}
	return result
}

func (state *stateVolumeAttachmentState) failureOrExpired(now time.Time) error {
	if state == nil {
		return nil
	}
	state.mu.Lock()
	if state.fenceErr == nil && !state.expiresAt.After(now) {
		state.fenceErr = errors.New("state volume writer lease expired before runtime start")
	}
	result := state.fenceErr
	cancelPrepare := state.cancelPrepare
	cancelWriter := state.cancelWriter
	state.mu.Unlock()
	if result != nil {
		if cancelPrepare != nil {
			cancelPrepare(result)
		}
		if cancelWriter != nil {
			cancelWriter(result)
		}
	}
	return result
}

func (state *stateVolumeAttachmentState) finishPreparation() {
	if state == nil || state.preparationDone == nil {
		return
	}
	state.preparationOnce.Do(func() { close(state.preparationDone) })
}

func (state *stateVolumeAttachmentState) fenced() error {
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.fenceErr
}

func (state *stateVolumeAttachmentState) finishCleanup(err error) {
	if state == nil {
		return
	}
	state.cleanupOnce.Do(func() {
		state.mu.Lock()
		state.cleanupErr = err
		done := state.cleanupDone
		state.mu.Unlock()
		if done != nil {
			close(done)
		}
	})
}

func (state *stateVolumeAttachmentState) waitCleanup(ctx context.Context) error {
	if state == nil || state.cleanupDone == nil {
		return nil
	}
	select {
	case <-state.cleanupDone:
		state.mu.RLock()
		defer state.mu.RUnlock()
		return state.cleanupErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stateVolumePreparationFence(instance *ContainerInstance) error {
	if instance == nil {
		return nil
	}
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	if state == nil {
		return nil
	}
	return state.failureOrExpired(time.Now())
}

func (state *stateVolumeAttachmentState) snapshotLeases(allowExpiredEscrow bool) ([]*pb.StateVolumeLease, error) {
	if state == nil {
		return nil, nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if !allowExpiredEscrow && !state.expiresAt.After(time.Now()) {
		return nil, fmt.Errorf("state volume writer lease expired before snapshot")
	}
	return cloneStateVolumeLeases(state.leases), nil
}

func stateVolumeLeaseTuplesEqual(left, right []*pb.StateVolumeLease) bool {
	if len(left) != len(right) {
		return false
	}
	byVolume := make(map[string]*pb.StateVolumeLease, len(left))
	for _, lease := range left {
		if lease == nil || byVolume[lease.VolumeId] != nil {
			return false
		}
		byVolume[lease.VolumeId] = lease
	}
	seenRight := make(map[string]struct{}, len(right))
	for _, lease := range right {
		if lease == nil {
			return false
		}
		if _, duplicate := seenRight[lease.VolumeId]; duplicate {
			return false
		}
		seenRight[lease.VolumeId] = struct{}{}
		expected := byVolume[lease.GetVolumeId()]
		if expected == nil || expected.AttachmentToken != lease.GetAttachmentToken() || expected.FencingToken != lease.GetFencingToken() {
			return false
		}
	}
	return true
}

func stateVolumeWriterLeasesForSnapshot(request *types.ContainerRequest, instance *ContainerInstance, allowExpiredEscrow bool) ([]*pb.StateVolumeLease, error) {
	expected, err := stateVolumeWriterLeaseTuples(request, false)
	if err != nil || len(expected) == 0 {
		return expected, err
	}
	if instance == nil {
		return nil, fmt.Errorf("state volume attachment state is unavailable")
	}
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	if state == nil {
		return nil, fmt.Errorf("state volume attachment renewal is unavailable")
	}
	leases, err := state.snapshotLeases(allowExpiredEscrow)
	if err != nil {
		return nil, err
	}
	if !stateVolumeLeaseTuplesEqual(expected, leases) {
		return nil, fmt.Errorf("state volume attachment token/fence changed after launch")
	}
	return leases, nil
}

func updateRequestStateVolumeLeaseDeadline(request *types.ContainerRequest, deadline time.Time) {
	if request == nil || deadline.IsZero() {
		return
	}
	expiresAtUnix := deadline.Unix()
	if request.PersistentRoot != nil && request.RootState != nil {
		request.RootState.LeaseExpiresAtUnix = expiresAtUnix
	}
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk != nil && !mount.ReadOnly {
			mount.DurableDisk.LeaseExpiresAtUnix = expiresAtUnix
		}
	}
}

func (s *Worker) renewStateVolumeAttachments(ctx context.Context, request *types.ContainerRequest, leases []*pb.StateVolumeLease) (time.Time, error) {
	if s.backendRepoClient == nil {
		return time.Time{}, fmt.Errorf("state volume attachment repository is unavailable")
	}
	response, err := s.backendRepoClient.RenewStateVolumeAttachments(ctx, &pb.RenewStateVolumeAttachmentsRequest{
		WorkspaceId: request.Workspace.ExternalId, ContainerId: request.ContainerId, Leases: leases,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
	})
	if err != nil {
		return time.Time{}, err
	}
	if response == nil || !response.Ok || response.LeaseExpiresAt == nil {
		message := "state volume attachment renewal was rejected"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return time.Time{}, &stateVolumeAttachmentRejectedError{reason: message}
	}
	expiresAt := response.LeaseExpiresAt.AsTime()
	if !expiresAt.After(time.Now()) {
		return time.Time{}, &stateVolumeAttachmentRejectedError{reason: "state volume attachment renewal returned an expired deadline"}
	}
	return expiresAt, nil
}

func (s *Worker) startStateVolumeAttachmentRenewal(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) error {
	// The request deadline is only the scheduler's dispatch-time observation
	// and may be stale after redelivery. Validate the immutable tuple, then let
	// the authoritative repository renewal prove the lease is still current
	// before any state volume is mounted.
	leases, err := stateVolumeWriterLeaseTuples(request, false)
	if err != nil || len(leases) == 0 {
		return err
	}
	if strings.TrimSpace(request.Workspace.ExternalId) == "" {
		return fmt.Errorf("writable state volumes require a workspace identity")
	}
	renewCtx, cancelRenewal := context.WithCancel(context.Background())
	prepareCtx, cancelPrepare := context.WithCancelCause(ctx)
	writerCtx, cancelWriter := context.WithCancelCause(context.Background())
	deadline, err := s.renewStateVolumeAttachments(ctx, request, leases)
	if err != nil {
		cancelRenewal()
		cancelPrepare(err)
		cancelWriter(err)
		return err
	}
	updateRequestStateVolumeLeaseDeadline(request, deadline)
	state := &stateVolumeAttachmentState{
		leases: cloneStateVolumeLeases(leases), expiresAt: deadline,
		prepareCtx: prepareCtx, writerCtx: writerCtx, cancelPrepare: cancelPrepare, cancelWriter: cancelWriter,
		preparationDone: make(chan struct{}), cleanupDone: make(chan struct{}), done: make(chan struct{}),
	}
	state.cancel = func() {
		cancelRenewal()
		cancelPrepare(context.Canceled)
		cancelWriter(context.Canceled)
	}
	instance.stateMu.Lock()
	if instance.StateVolumeAttachments != nil {
		instance.stateMu.Unlock()
		state.cancel()
		return fmt.Errorf("state volume attachment renewal is already active")
	}
	instance.StateVolumeAttachments = state
	alreadyPrepared := instance.StateVolumes != nil
	instance.stateMu.Unlock()
	if alreadyPrepared {
		state.finishPreparation()
	}
	interval := s.stateVolumeLeaseRenewInterval
	if interval <= 0 || interval > stateVolumeAttachmentRenewInterval {
		interval = stateVolumeAttachmentRenewInterval
	}
	go func() {
		defer close(state.done)
		currentDeadline := deadline
		for {
			wait := interval
			if untilHalf := time.Until(currentDeadline) / 2; untilHalf > 0 && untilHalf < wait {
				wait = untilHalf
			}
			if wait <= 0 {
				s.fenceStateVolumeContainer(request.ContainerId, fmt.Errorf("state volume writer lease expired"))
				return
			}
			timer := time.NewTimer(wait)
			select {
			case <-renewCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			nextDeadline, err := s.renewStateVolumeAttachments(renewCtx, request, leases)
			for attempt := 0; err != nil; attempt++ {
				if renewCtx.Err() != nil {
					return
				}
				if stateVolumeAttachmentRenewalRejected(err) {
					s.fenceStateVolumeContainer(request.ContainerId, fmt.Errorf("state volume writer lease renewal rejected: %w", err))
					return
				}
				remaining := time.Until(currentDeadline)
				if remaining <= 0 {
					s.fenceStateVolumeContainer(request.ContainerId, fmt.Errorf("state volume writer lease expired after renewal transport failure: %w", err))
					return
				}
				retryDelay := stateVolumeAttachmentRetryDelay(interval, attempt)
				if retryDelay > remaining {
					retryDelay = remaining
				}
				retryTimer := time.NewTimer(retryDelay)
				select {
				case <-renewCtx.Done():
					if !retryTimer.Stop() {
						<-retryTimer.C
					}
					return
				case <-retryTimer.C:
				}
				nextDeadline, err = s.renewStateVolumeAttachments(renewCtx, request, leases)
			}
			currentDeadline = nextDeadline
			state.updateDeadline(nextDeadline)
		}
	}()
	return nil
}

func (s *Worker) releaseStateVolumeAttachments(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) error {
	if request == nil || instance == nil {
		return nil
	}
	// A lease fence and the ordinary lifecycle teardown can arrive at the same
	// time. Serialize the complete release obligation and recheck the exact
	// attachment state under the lock so a loser never tries to detach/finalize
	// a journal that the winner already completed.
	instance.stateVolumeCleanupMu.Lock()
	defer instance.stateVolumeCleanupMu.Unlock()
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	if state == nil {
		return nil
	}
	if s.stateVolumeManager != nil {
		completed, err := s.persistAndBeginStateVolumeRelease(ctx, request, instance)
		if err != nil {
			return err
		}
		if err := stopStateVolumeAttachmentRenewal(ctx, instance); err != nil {
			return err
		}
		if err := s.stateVolumeManager.DetachReleaseIntent(ctx, request.ContainerId); err != nil {
			return err
		}
		return s.completeStateVolumeRelease(ctx, request, instance, completed)
	}
	// Unit-only/minimal workers without a manager have no block graph to
	// detach. Production always uses the durable release-intent path above.
	state.once.Do(state.cancel)
	select {
	case <-state.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	response, err := s.backendRepoClient.ReleaseStateVolumeAttachments(ctx, &pb.ReleaseStateVolumeAttachmentsRequest{
		WorkspaceId: request.Workspace.ExternalId, ContainerId: request.ContainerId, Leases: state.leases,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
	})
	if err != nil {
		return err
	}
	if response == nil || !response.Ok {
		message := "state volume attachment release was rejected"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	instance.stateMu.Lock()
	if instance.StateVolumeAttachments == state {
		instance.StateVolumeAttachments = nil
	}
	instance.stateMu.Unlock()
	state.finishCleanup(nil)
	return nil
}

func stopStateVolumeAttachmentRenewal(ctx context.Context, instance *ContainerInstance) error {
	if instance == nil {
		return nil
	}
	instance.stateMu.Lock()
	state := instance.StateVolumeAttachments
	instance.stateMu.Unlock()
	if state == nil {
		return nil
	}
	state.once.Do(state.cancel)
	select {
	case <-state.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stateVolumeReleaseMembers(leases []*pb.StateVolumeLease) ([]StateVolumeReleaseMember, error) {
	members := make([]StateVolumeReleaseMember, 0, len(leases))
	for _, lease := range leases {
		if lease == nil {
			return nil, fmt.Errorf("state-volume release contains an empty lease")
		}
		members = append(members, StateVolumeReleaseMember{
			VolumeID: lease.VolumeId, FencingToken: lease.FencingToken,
		})
	}
	return canonicalStateVolumeReleaseMembers(members)
}

func (s *Worker) persistAndBeginStateVolumeRelease(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) (bool, error) {
	if s == nil || s.stateVolumeManager == nil || s.backendRepoClient == nil || request == nil || instance == nil {
		return false, fmt.Errorf("state-volume release services are unavailable")
	}
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	if state == nil {
		return false, nil
	}
	state.mu.RLock()
	leases := cloneStateVolumeLeases(state.leases)
	state.mu.RUnlock()
	members, err := stateVolumeReleaseMembers(leases)
	if err != nil {
		return false, err
	}
	release := StateVolumeReleaseEnvelope{
		WorkspaceID: request.Workspace.ExternalId, SourceWorkerID: s.workerId,
		SourceWorkerInstanceID: s.workerInstanceId, StorageNodeID: s.machineID,
		Members: members,
	}
	release.JournalDigest, err = stateVolumeReleaseJournalDigest(request.ContainerId, release)
	if err != nil {
		return false, err
	}
	// The local, non-secret obligation is fsynced before the source asks the
	// repository to escrow its attachment credentials. No detach occurs in the
	// one-sided gap; a replacement must fail closed until control resolves it.
	if err := s.stateVolumeManager.PersistReleaseDetachIntent(request.ContainerId, release); err != nil {
		return false, err
	}
	protoMembers := make([]*pb.StateVolumeReleaseMember, 0, len(members))
	for _, member := range members {
		protoMembers = append(protoMembers, &pb.StateVolumeReleaseMember{
			VolumeId: member.VolumeID, FencingToken: member.FencingToken,
		})
	}
	response, err := s.backendRepoClient.BeginStateVolumeReleaseIntent(ctx, &pb.BeginStateVolumeReleaseIntentRequest{
		WorkspaceId: request.Workspace.ExternalId, ContainerId: request.ContainerId,
		SourceWorkerId: s.workerId, SourceWorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		JournalDigest: release.JournalDigest, Members: protoMembers,
	})
	if err != nil {
		return false, err
	}
	if response == nil || !response.Ok || response.ReleaseClaimId == "" {
		message := "state-volume release intent escrow was rejected"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return false, fmt.Errorf("%s", message)
	}
	if err := s.stateVolumeManager.ArmReleaseIntent(request.ContainerId, response.ReleaseClaimId, response.ReleaseClaimGeneration); err != nil {
		return false, err
	}
	return response.Completed, nil
}

func (s *Worker) releaseEscrowedStateVolumeAttachments(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) error {
	if request == nil || instance == nil {
		return nil
	}
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	if state == nil {
		return nil
	}
	response, err := s.backendRepoClient.ReleaseStateVolumeAttachments(ctx, &pb.ReleaseStateVolumeAttachmentsRequest{
		WorkspaceId: request.Workspace.ExternalId, ContainerId: request.ContainerId, Leases: cloneStateVolumeLeases(state.leases),
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
	})
	if err != nil {
		return err
	}
	if response == nil || !response.Ok {
		message := "state volume attachment release was rejected"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	return nil
}

func clearStateVolumeAttachmentState(instance *ContainerInstance) {
	if instance == nil {
		return
	}
	instance.stateMu.Lock()
	state := instance.StateVolumeAttachments
	instance.StateVolumeAttachments = nil
	instance.stateMu.Unlock()
	state.finishCleanup(nil)
}

func (s *Worker) completeStateVolumeRelease(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance, alreadyCompleted bool) error {
	if !alreadyCompleted {
		if err := s.releaseEscrowedStateVolumeAttachments(ctx, request, instance); err != nil {
			return err
		}
	}
	if err := s.stateVolumeManager.MarkReleaseCompleted(request.ContainerId); err != nil {
		return err
	}
	if err := s.stateVolumeManager.FinalizeReleaseIntent(request.ContainerId); err != nil {
		return err
	}
	clearStateVolumeAttachmentState(instance)
	return nil
}

func stateVolumeReleaseProtoMembers(members []StateVolumeReleaseMember) []*pb.StateVolumeReleaseMember {
	result := make([]*pb.StateVolumeReleaseMember, 0, len(members))
	for _, member := range members {
		result = append(result, &pb.StateVolumeReleaseMember{
			VolumeId: member.VolumeID, FencingToken: member.FencingToken,
		})
	}
	return result
}

// reconcileStateVolumeReleaseJournals runs before capacity advertisement. It
// claims only a locally authenticated, kernel-clear obligation; the control
// plane then proves the old process dead and either returns a completed source
// release or hands off the exact server escrow to this process epoch.
func (s *Worker) reconcileStateVolumeReleaseJournals(ctx context.Context) error {
	if s == nil || s.stateVolumeManager == nil || s.backendRepoClient == nil {
		return fmt.Errorf("state-volume release reconciliation services are unavailable")
	}
	journals, err := s.stateVolumeManager.Journals.List()
	if err != nil {
		return err
	}
	for _, journal := range journals {
		release := journal.Release
		if release == nil || release.StorageNodeID != s.machineID {
			continue
		}
		if (journal.Phase != "release-intent" && journal.Phase != "release-completed") || !release.LocalCleanupVerified {
			return fmt.Errorf("state-volume release %q has not reached authenticated local cleanup", journal.ContainerID)
		}
		claim, err := s.backendRepoClient.ClaimStateVolumeRelease(ctx, &pb.ClaimStateVolumeReleaseRequest{
			WorkspaceId: release.WorkspaceID, ContainerId: journal.ContainerID,
			SourceWorkerId: release.SourceWorkerID, SourceWorkerInstanceId: release.SourceWorkerInstanceID,
			StorageNodeId: release.StorageNodeID, RecoveryWorkerId: s.workerId, RecoveryWorkerInstanceId: s.workerInstanceId,
			JournalDigest: release.JournalDigest, PreviousClaimGeneration: release.ReleaseClaimGeneration,
			Members: stateVolumeReleaseProtoMembers(release.Members),
		})
		if err != nil {
			return fmt.Errorf("claim state-volume release %q: %w", journal.ContainerID, err)
		}
		if claim == nil || !claim.Ok || claim.ReleaseClaimId == "" {
			message := "state-volume release claim was rejected"
			if claim != nil && claim.ErrorMsg != "" {
				message = claim.ErrorMsg
			}
			return fmt.Errorf("%s", message)
		}
		if !claim.Completed {
			if claim.ReleaseClaimGeneration <= 0 {
				return fmt.Errorf("replacement state-volume release claim has no positive generation")
			}
			if err := s.stateVolumeManager.RecordClaimedRelease(journal.ContainerID, claim.ReleaseClaimId, claim.ReleaseClaimGeneration); err != nil {
				return err
			}
			completed, err := s.backendRepoClient.CompleteClaimedStateVolumeRelease(ctx, &pb.CompleteClaimedStateVolumeReleaseRequest{
				WorkspaceId: release.WorkspaceID, ContainerId: journal.ContainerID,
				ReleaseClaimId: claim.ReleaseClaimId, ReleaseClaimGeneration: claim.ReleaseClaimGeneration,
				RecoveryWorkerId: s.workerId, RecoveryWorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
			})
			if err != nil {
				return fmt.Errorf("complete claimed state-volume release %q: %w", journal.ContainerID, err)
			}
			if completed == nil || !completed.Ok {
				message := "claimed state-volume release completion was rejected"
				if completed != nil && completed.ErrorMsg != "" {
					message = completed.ErrorMsg
				}
				return fmt.Errorf("%s", message)
			}
		}
		if err := s.stateVolumeManager.MarkReleaseCompleted(journal.ContainerID); err != nil {
			return err
		}
		if err := s.stateVolumeManager.FinalizeReleaseIntent(journal.ContainerID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Worker) stopAndReleaseStateVolumes(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) error {
	if request == nil || instance == nil {
		return nil
	}
	instance.stateVolumeCleanupMu.Lock()
	defer instance.stateVolumeCleanupMu.Unlock()
	instance.stateMu.RLock()
	hasStateVolumes := instance.StateVolumes != nil
	instance.stateMu.RUnlock()
	if !hasStateVolumes {
		return nil
	}
	if s.stateVolumeManager.TerminalCommitOwnsRelease(request.ContainerId) {
		// CommitStateSnapshot atomically terminalizes the exact state group and
		// deletes its attachment escrow. Do not create a second gen-0 release
		// obligation for rows that no longer exist.
		if err := stopStateVolumeAttachmentRenewal(ctx, instance); err != nil {
			return err
		}
		if instance.Overlay != nil {
			if err := instance.Overlay.Cleanup(); err != nil {
				return fmt.Errorf("unmount overlay after terminal state commit: %w", err)
			}
		}
		if err := s.stateVolumeManager.Stop(ctx, request.ContainerId); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			return err
		}
		clearStateVolumeAttachmentState(instance)
		instance.stateMu.Lock()
		instance.StateVolumes = nil
		instance.stateMu.Unlock()
		s.containerInstances.Set(request.ContainerId, instance)
		return nil
	}
	alreadyCompleted, err := s.persistAndBeginStateVolumeRelease(ctx, request, instance)
	if err != nil {
		return err
	}
	if err := stopStateVolumeAttachmentRenewal(ctx, instance); err != nil {
		return err
	}
	if instance.Overlay != nil {
		if err := instance.Overlay.Cleanup(); err != nil {
			return fmt.Errorf("unmount overlay before state volume detach: %w", err)
		}
	}
	if err := s.stateVolumeManager.DetachReleaseIntent(ctx, request.ContainerId); err != nil {
		return err
	}
	if err := s.completeStateVolumeRelease(ctx, request, instance, alreadyCompleted); err != nil {
		return err
	}
	instance.stateMu.Lock()
	instance.StateVolumes = nil
	instance.stateMu.Unlock()
	s.containerInstances.Set(request.ContainerId, instance)
	return nil
}

// detachTerminalPendingStateVolumes releases every live mount/NBD/QSD and
// writer lease after a terminal operation has stopped the runtime, while
// intentionally retaining the immutable pending graph and its handle for an
// idempotent upload/commit retry.
func (s *Worker) detachTerminalPendingStateVolumes(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance, operationID string) error {
	if request == nil || instance == nil {
		return nil
	}
	instance.stateMu.RLock()
	hasStateVolumes := instance.StateVolumes != nil
	instance.stateMu.RUnlock()
	if !hasStateVolumes {
		return nil
	}
	instance.stateVolumeCleanupMu.Lock()
	defer instance.stateVolumeCleanupMu.Unlock()
	if instance.Overlay != nil {
		if err := instance.Overlay.Cleanup(); err != nil {
			return fmt.Errorf("unmount overlay before terminal pending detach: %w", err)
		}
	}
	if err := s.stateVolumeManager.SealAndDetachTerminalPending(ctx, request.ContainerId, operationID); err != nil {
		return err
	}
	// Stop renewal once no writer exists, but retain the exact DB attachment
	// rows and identities through atomic snapshot Commit. Pending snapshot
	// escrow authorizes an offline replay after their ordinary TTL expires.
	if err := stopStateVolumeAttachmentRenewal(ctx, instance); err != nil {
		return err
	}
	s.containerInstances.Set(request.ContainerId, instance)
	return nil
}

func (s *Worker) fenceStateVolumeContainer(containerID string, cause error) {
	log.Error().Str("container_id", containerID).Err(cause).Msg("state volume safety fence tripped")
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		return
	}
	instance.stateMu.Lock()
	state := instance.StateVolumeAttachments
	instance.stateMu.Unlock()
	if state == nil {
		s.killStateVolumeWriter(containerID, instance, cause)
		return
	}
	state.fenceOnce.Do(func() {
		cause = state.tripFence(cause)
		instance.stateMu.Lock()
		instance.Err = cause
		instance.stateMu.Unlock()
		s.containerInstances.Set(containerID, instance)
		s.markContainerStopping(containerID, types.ContainerStateTtlSWhileStopping)
		// Kill the writer immediately, but never wait on state.done or unmount
		// from the renewal goroutine that owns closing state.done.
		s.killStateVolumeWriter(containerID, instance, cause)
		state.once.Do(state.cancel)
		go func() {
			<-state.done
			if state.preparationDone != nil {
				<-state.preparationDone
			}
			instance.stateMu.RLock()
			hasStateVolumes := instance.StateVolumes != nil
			instance.stateMu.RUnlock()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var err error
			if !hasStateVolumes {
				// Preparation was canceled before a graph became live. The manager
				// persists a path-free release-only journal, binds the server escrow,
				// and only then proves local cleanup and releases the exact tuples.
				err = s.releaseStateVolumeAttachments(ctx, instance.Request, instance)
			} else if pendingOperationID, pending := s.stateVolumeManager.PendingOperation(containerID); pending {
				recovery, recoveryErr := s.stateVolumeManager.SnapshotRecovery(containerID, pendingOperationID)
				if recoveryErr != nil {
					err = recoveryErr
				} else {
					response, snapshotErr := s.snapshotContainerStateWithRuntimeStopped(ctx, &pb.SnapshotContainerStateRequest{
						ContainerId: containerID, OperationId: pendingOperationID, Mode: recovery.Mode,
						IncludeMemory: recovery.IncludeMemory, Visible: recovery.Visible,
					}, true)
					err = snapshotErr
					if err == nil && (response == nil || !response.Ok) {
						message := "escrowed state volume fence operation failed"
						if response != nil && response.ErrorMsg != "" {
							message = response.ErrorMsg
						}
						err = fmt.Errorf("%s", message)
					}
				}
			} else {
				// Lease loss cannot authorize a new snapshot. Kill the writer and
				// quarantine its private child without publishing a generation;
				// only an operation escrowed before the loss may complete.
				err = s.quarantineFencedStateVolume(ctx, instance)
			}
			instance.stateMu.Lock()
			instance.StateFinalCommitError = err
			instance.stateMu.Unlock()
			s.containerInstances.Set(containerID, instance)
			state.finishCleanup(err)
			if err != nil {
				// The writer is dead, but no mount/lease is released until the
				// exact terminal generation reaches its durable journal/escrow
				// boundary. Startup recovery owns the next attempt.
				log.Error().Str("container_id", containerID).Err(err).Msg("state volume fence could not reach a safe escrow/quarantine boundary")
			}
		}()
	})
}

func (s *Worker) quarantineFencedStateVolume(ctx context.Context, instance *ContainerInstance) error {
	if instance == nil || instance.Request == nil {
		return nil
	}
	instance.stateVolumeCleanupMu.Lock()
	defer instance.stateVolumeCleanupMu.Unlock()
	instance.stateMu.RLock()
	hasStateVolumes := instance.StateVolumes != nil
	instance.stateMu.RUnlock()
	if !hasStateVolumes {
		return nil
	}
	alreadyCompleted, err := s.persistAndBeginStateVolumeRelease(ctx, instance.Request, instance)
	if err != nil {
		return err
	}
	if err := stopStateVolumeAttachmentRenewal(ctx, instance); err != nil {
		return err
	}
	if instance.Overlay != nil {
		if err := instance.Overlay.Cleanup(); err != nil {
			return fmt.Errorf("unmount overlay before fenced state quarantine: %w", err)
		}
	}
	if err := s.stateVolumeManager.DetachReleaseIntent(ctx, instance.Request.ContainerId); err != nil {
		return err
	}
	if err := s.completeStateVolumeRelease(ctx, instance.Request, instance, alreadyCompleted); err != nil {
		return err
	}
	instance.stateMu.Lock()
	instance.StateVolumes = nil
	instance.stateMu.Unlock()
	s.containerInstances.Set(instance.Request.ContainerId, instance)
	return nil
}

func (s *Worker) killStateVolumeWriter(containerID string, instance *ContainerInstance, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	killErr := s.stopContainerWithoutCheckpointDeferral(ctx, containerID, true)
	rt := instance.Runtime
	if rt == nil {
		rt = s.runtime
	}
	if killErr != nil && rt != nil {
		deleteErr := rt.Delete(ctx, containerID, &runtime.DeleteOpts{Force: true})
		if deleteErr == nil || runtimeContainerNotFound(deleteErr) {
			killErr = nil
		} else {
			killErr = errors.Join(killErr, deleteErr)
		}
	}
	if killErr != nil && !runtimeContainerNotFound(killErr) {
		log.Error().Str("container_id", containerID).Err(killErr).Err(cause).Msg("failed to terminate writer after state volume fence")
	}
}
