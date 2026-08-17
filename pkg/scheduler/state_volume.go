package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/api/resource"
)

const stateVolumeAttachmentPlanOrphanAge = 2 * time.Minute

func stateVolumeAttachmentPlanIdentity(request *types.ContainerRequest) (string, int, error) {
	if request == nil {
		return "", 0, fmt.Errorf("container request is required")
	}
	writable := 0
	if request.PersistentRoot != nil {
		writable++
	}
	for index := range request.Mounts {
		mount := request.Mounts[index]
		if mount.MountType != types.StorageModeDurableDisk && mount.DurableDisk == nil {
			continue
		}
		if mount.DurableDisk == nil {
			return "", 0, fmt.Errorf("durable mount %q has no disk configuration", mount.MountPath)
		}
		if !mount.ReadOnly {
			writable++
		}
	}
	if writable == 0 {
		return "", 0, nil
	}
	canonical := request.Clone()
	canonical.Timestamp = time.Time{}
	canonical.RetryCount = 0
	canonical.DeliveryToken = ""
	canonical.ProvisioningAttempts = 0
	canonical.RootState = nil
	canonical.StateVolumePlanId = ""
	canonical.StateVolumePlanHash = ""
	for index := range canonical.Mounts {
		if canonical.Mounts[index].DurableDisk == nil {
			continue
		}
		disk := canonical.Mounts[index].DurableDisk
		disk.VolumeId = ""
		disk.Initialize = false
		disk.CloneSource = false
		disk.AttachmentToken = ""
		disk.FencingToken = 0
		disk.LeaseExpiresAtUnix = 0
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", 0, fmt.Errorf("canonicalize state-volume attachment plan: %w", err)
	}
	digest := sha256.Sum256(append([]byte("state-volume-attachment-plan.v1\x00"), payload...))
	return hex.EncodeToString(digest[:]), writable, nil
}

func canonicalStateVolumeOutboxRequest(request *types.ContainerRequest, plan *types.StateVolumeAttachmentPlan) (*types.ContainerRequest, error) {
	if request == nil || plan == nil || plan.CreatedAt.IsZero() {
		return nil, fmt.Errorf("state-volume attachment plan has no durable creation time")
	}
	queued := request.Clone()
	queued.Timestamp = plan.CreatedAt.UTC()
	queued.RetryCount = 1
	queued.DeliveryToken = ""
	queued.ProvisioningAttempts = 0
	if queued.RootState != nil {
		queued.RootState.LeaseExpiresAtUnix = 0
	}
	for index := range queued.Mounts {
		if queued.Mounts[index].DurableDisk != nil && !queued.Mounts[index].ReadOnly {
			queued.Mounts[index].DurableDisk.LeaseExpiresAtUnix = 0
		}
	}
	return queued, nil
}

// resolveStateVolumeAttachments binds every named durable disk to its stable
// repository volume and immutable source generation before the request enters
// Redis. Writable bindings carry a renewable fencing lease; read-only bindings
// point directly at an immutable generation and need no lease.
func (s *Scheduler) resolveStateVolumeAttachments(ctx context.Context, request *types.ContainerRequest) ([]types.StateVolumeLease, error) {
	if request == nil {
		return nil, fmt.Errorf("container request is required")
	}
	hasState := request.PersistentRoot != nil
	for i := range request.Mounts {
		if request.Mounts[i].MountType == types.StorageModeDurableDisk || request.Mounts[i].DurableDisk != nil {
			hasState = true
			break
		}
	}
	if !hasState {
		return nil, nil
	}
	if s.backendRepo == nil {
		return nil, fmt.Errorf("state-volume repository is unavailable")
	}
	workspaceID := request.Workspace.Id
	if workspaceID == 0 {
		workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, request.WorkspaceId)
		if err != nil {
			return nil, err
		}
		workspaceID = workspace.Id
	}

	exactSources := map[string]types.StateGeneration{}
	var rootSource *types.StateGeneration
	var snapshot *types.StateSnapshot
	forkState := false
	if request.StateSnapshotId != "" {
		var err error
		snapshot, err = s.backendRepo.GetStateSnapshot(ctx, workspaceID, request.StateSnapshotId)
		if err != nil {
			return nil, err
		}
		if snapshot.Status != types.StateSnapshotStatusAvailable {
			return nil, fmt.Errorf("state snapshot %q is not available", request.StateSnapshotId)
		}
		destinationStubId := strings.TrimSpace(request.StubId)
		if destinationStubId == "" {
			destinationStubId = strings.TrimSpace(request.Stub.ExternalId)
		}
		forkState = destinationStubId != snapshot.SourceStubExternalId
		if !forkState && snapshot.Mode != "terminal" {
			return nil, fmt.Errorf("live state snapshot %q can only be restored as a fork", request.StateSnapshotId)
		}
		request.StateFork = forkState
		for index := range snapshot.Generations {
			member := snapshot.Generations[index]
			if member.Root {
				copy := member
				rootSource = &copy
			} else {
				exactSources[member.Name] = member
			}
		}
	}

	rollback := make([]types.StateVolumeLease, 0, len(request.Mounts)+1)
	destinationStubId := strings.TrimSpace(request.StubId)
	if destinationStubId == "" {
		destinationStubId = strings.TrimSpace(request.Stub.ExternalId)
	}
	if destinationStubId == "" {
		return nil, fmt.Errorf("state volumes require a destination stub identity")
	}
	if request.PersistentRoot != nil {
		rootVolumeId := branchStateVolumeID(request.WorkspaceId, destinationStubId, "root")
		sourceGenerationId := ""
		cloneSource := false
		if snapshot != nil {
			if rootSource == nil || rootSource.Name != "root" || rootSource.MountPath != "/" || rootSource.ReadOnly {
				return rollback, fmt.Errorf("state snapshot has no canonical writable root")
			}
			sourceGenerationId = rootSource.GenerationId
			cloneSource = forkState
			if !forkState {
				if rootSource.VolumeId != rootVolumeId {
					return rollback, fmt.Errorf("terminal root state does not match the destination stub lineage")
				}
				rootVolumeId = rootSource.VolumeId
			}
		}
		attachment, err := s.backendRepo.ResolveBranchStateAttachment(ctx, workspaceID, destinationStubId,
			request.ContainerId, request.StateVolumePlanId, request.StateVolumePlanHash,
			rootVolumeId, "root", request.PersistentRoot.Size, "/",
			sourceGenerationId, true, cloneSource)
		if err != nil {
			return rollback, err
		}
		if !attachment.Replayed {
			rollback = append(rollback, types.StateVolumeLease{VolumeId: attachment.VolumeId,
				AttachmentToken: attachment.AttachmentToken, FencingToken: attachment.FencingToken})
		}
		request.RootState = &types.RootStateMountConfig{
			VolumeId: attachment.VolumeId, Size: attachment.Size,
			SourceGenerationId: attachment.SourceGenerationId, CloneSource: attachment.CloneSource,
			Initialize: attachment.Initialize, AttachmentToken: attachment.AttachmentToken,
			FencingToken: attachment.FencingToken, LeaseExpiresAtUnix: attachment.ExpiresAt.Unix(),
		}
	} else if rootSource != nil {
		return rollback, fmt.Errorf("state snapshot root requires persistent_root on the destination stub")
	}

	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.MountType != types.StorageModeDurableDisk && mount.DurableDisk == nil {
			continue
		}
		if mount.DurableDisk == nil {
			return rollback, fmt.Errorf("durable mount %q has no disk configuration", mount.MountPath)
		}
		sourceGenerationID := strings.TrimSpace(mount.DurableDisk.SourceGenerationId)
		var exactMember *types.StateGeneration
		if request.StateSnapshotId != "" {
			member, ok := exactSources[mount.DurableDisk.Name]
			if !ok || member.MountPath != mount.MountPath || member.ReadOnly != mount.ReadOnly {
				return rollback, fmt.Errorf("state snapshot volume %q does not exactly match requested mount", mount.DurableDisk.Name)
			}
			sourceGenerationID = member.GenerationId
			exactMember = &member
			if mount.ReadOnly {
				generation, err := s.backendRepo.GetVolumeGeneration(ctx, workspaceID, member.GenerationId)
				if err != nil {
					return rollback, err
				}
				if generation.Status != types.StateSnapshotStatusAvailable || generation.VolumeId != member.VolumeId ||
					generation.Name != member.Name || generation.Generation != member.Generation ||
					generation.ParentGenerationId != member.ParentGenerationId ||
					generation.CloneParentGenerationId != member.CloneParentGenerationId {
					return rollback, fmt.Errorf("state snapshot read-only volume %q does not match its immutable generation", member.Name)
				}
				if err := s.backendRepo.ResolveReadOnlyStateAttachment(ctx, workspaceID, request.ContainerId,
					member.VolumeId, member.GenerationId, member.Name, member.MountPath, member.Root); err != nil {
					return rollback, err
				}
				mount.DurableDisk.VolumeId = member.VolumeId
				mount.DurableDisk.SourceGenerationId = member.GenerationId
				mount.DurableDisk.Initialize = false
				mount.DurableDisk.CloneSource = false
				mount.DurableDisk.AttachmentToken = ""
				mount.DurableDisk.FencingToken = 0
				mount.DurableDisk.LeaseExpiresAtUnix = 0
				continue
			}
		}
		if mount.ReadOnly {
			registered, err := s.backendRepo.GetDisk(ctx, workspaceID, mount.DurableDisk.Name)
			if err != nil {
				return rollback, err
			}
			requestedSize, requestedErr := resource.ParseQuantity(mount.DurableDisk.Size)
			registeredSize, registeredErr := resource.ParseQuantity(registered.Size)
			if requestedErr != nil || registeredErr != nil || requestedSize.Cmp(registeredSize) != 0 ||
				registered.MountPath != mount.MountPath {
				return rollback, fmt.Errorf("read-only disk %q geometry does not match its registered state volume", registered.Name)
			}
			var generation *types.VolumeGeneration
			if sourceGenerationID == "" {
				generation, err = s.backendRepo.GetLatestVolumeGeneration(ctx, workspaceID, registered.ExternalId)
			} else {
				generation, err = s.backendRepo.GetVolumeGeneration(ctx, workspaceID, sourceGenerationID)
			}
			if err != nil {
				return rollback, err
			}
			if generation.Status != types.StateSnapshotStatusAvailable || generation.VolumeId != registered.ExternalId ||
				generation.Name != registered.Name {
				return rollback, fmt.Errorf("read-only disk %q source is not an exact available generation", registered.Name)
			}
			if err := s.backendRepo.ResolveReadOnlyStateAttachment(ctx, workspaceID, request.ContainerId,
				registered.ExternalId, generation.ExternalId, registered.Name, registered.MountPath, false); err != nil {
				return rollback, err
			}
			mount.DurableDisk.VolumeId = registered.ExternalId
			mount.DurableDisk.SourceGenerationId = generation.ExternalId
			mount.DurableDisk.Initialize = false
			mount.DurableDisk.CloneSource = false
			mount.DurableDisk.AttachmentToken = ""
			mount.DurableDisk.FencingToken = 0
			mount.DurableDisk.LeaseExpiresAtUnix = 0
			continue
		}

		branchVolumeId := branchStateVolumeID(request.WorkspaceId, destinationStubId, mount.DurableDisk.Name)
		useBranch := exactMember != nil && (forkState || exactMember.VolumeId == branchVolumeId)
		var attachment *types.StateVolumeAttachment
		var err error
		if useBranch {
			attachment, err = s.backendRepo.ResolveBranchStateAttachment(ctx, workspaceID, destinationStubId,
				request.ContainerId, request.StateVolumePlanId, request.StateVolumePlanHash,
				branchVolumeId, mount.DurableDisk.Name, mount.DurableDisk.Size,
				mount.MountPath, sourceGenerationID, false, forkState)
		} else {
			attachment, err = s.backendRepo.ResolveStateVolumeAttachment(ctx, workspaceID, request.ContainerId,
				request.StateVolumePlanId, request.StateVolumePlanHash, &types.Disk{
					Name: mount.DurableDisk.Name, Size: mount.DurableDisk.Size, MountPath: mount.MountPath,
				}, sourceGenerationID)
		}
		if err != nil {
			return rollback, err
		}
		if !attachment.ReadOnly && !attachment.Replayed {
			rollback = append(rollback, types.StateVolumeLease{
				VolumeId: attachment.VolumeId, AttachmentToken: attachment.AttachmentToken,
				FencingToken: attachment.FencingToken,
			})
		}
		if member, ok := exactSources[mount.DurableDisk.Name]; ok && !forkState && attachment.VolumeId != member.VolumeId {
			return rollback, fmt.Errorf("state snapshot volume %q does not match registered durable disk", mount.DurableDisk.Name)
		}
		mount.DurableDisk.VolumeId = attachment.VolumeId
		mount.DurableDisk.SourceGenerationId = attachment.SourceGenerationId
		mount.DurableDisk.Initialize = attachment.Initialize
		mount.DurableDisk.CloneSource = attachment.CloneSource
		mount.DurableDisk.AttachmentToken = attachment.AttachmentToken
		mount.DurableDisk.FencingToken = attachment.FencingToken
		if !attachment.ExpiresAt.IsZero() {
			mount.DurableDisk.LeaseExpiresAtUnix = attachment.ExpiresAt.Unix()
		}
	}
	return rollback, nil
}

func branchStateVolumeID(workspaceId, stubId, memberName string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID,
		[]byte("beta9-state-branch\x00"+workspaceId+"\x00"+stubId+"\x00"+memberName)).String()
}

func (s *Scheduler) stateVolumeWorkspaceID(ctx context.Context, request *types.ContainerRequest) (uint, error) {
	if request == nil {
		return 0, fmt.Errorf("container request is required")
	}
	if request.Workspace.Id != 0 {
		return request.Workspace.Id, nil
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, request.WorkspaceId)
	if err != nil {
		return 0, err
	}
	return workspace.Id, nil
}

func (s *Scheduler) beginStateVolumeAttachmentPlan(ctx context.Context, request *types.ContainerRequest, planHash string, writableMembers int) (*types.StateVolumeAttachmentPlan, error) {
	if writableMembers == 0 {
		return nil, nil
	}
	workspaceID, err := s.stateVolumeWorkspaceID(ctx, request)
	if err != nil {
		return nil, err
	}
	plan, err := s.backendRepo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, request.ContainerId, planHash, writableMembers)
	if err != nil {
		return nil, err
	}
	request.StateVolumePlanId, request.StateVolumePlanHash = plan.PlanId, plan.RequestHash
	return plan, nil
}

func (s *Scheduler) completeStateVolumeAttachmentPlan(ctx context.Context, request *types.ContainerRequest, plan *types.StateVolumeAttachmentPlan) error {
	if plan == nil {
		return nil
	}
	workspaceID, err := s.stateVolumeWorkspaceID(ctx, request)
	if err != nil {
		return err
	}
	if err := s.backendRepo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, request.ContainerId, plan.PlanId, plan.RequestHash); err != nil {
		return err
	}
	plan.Admitted = true
	return nil
}

func (s *Scheduler) abortStateVolumeAttachmentPlan(ctx context.Context, request *types.ContainerRequest, plan *types.StateVolumeAttachmentPlan) error {
	if plan == nil || !plan.Owned || plan.Admitted {
		return nil
	}
	workspaceID, err := s.stateVolumeWorkspaceID(ctx, request)
	if err != nil {
		return err
	}
	fenced, err := s.containerRepo.FencePendingContainerStateVolumePlan(
		request.ContainerId, plan.PlanId, plan.RequestHash, types.ContainerStateTtlSWhileStopping)
	if err != nil {
		return err
	}
	if !fenced {
		return fmt.Errorf("state-volume attachment plan could not install its durable Redis abort fence")
	}
	return s.backendRepo.AbortStateVolumeAttachmentPlan(ctx, workspaceID, request.ContainerId, plan.PlanId, plan.RequestHash)
}

// reconcilePendingStateVolumeAttachmentPlans closes the only cross-store
// crash window in admission. A PostgreSQL plan older than the lease horizon is
// completed when Redis proves the container was admitted; otherwise its
// never-mounted writer attachments are atomically removed. TTL alone is never
// treated as proof that a QSD writer is gone.
func (s *Scheduler) reconcilePendingStateVolumeAttachmentPlans(ctx context.Context, olderThan time.Time) error {
	if s == nil || s.backendRepo == nil || s.containerRepo == nil {
		return nil
	}
	plans, err := s.backendRepo.ListIncompleteStateVolumeAttachmentPlans(ctx, olderThan)
	if err != nil {
		return err
	}
	var reconcileErr error
	for _, plan := range plans {
		state, stateErr := s.containerRepo.GetContainerState(plan.ContainerId)
		if plan.Aborted {
			fenced, fenceErr := s.containerRepo.FencePendingContainerStateVolumePlan(
				plan.ContainerId, plan.PlanId, plan.RequestHash, types.ContainerStateTtlSWhileStopping)
			if fenceErr != nil || !fenced {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("restore durable abort fence for state-volume plan %q: %w", plan.ContainerId, fenceErr))
				continue
			}
			if stateErr == nil && state.WorkerId == "" &&
				(state.Status == types.ContainerStatusPending || state.StateVolumeAborting == plan.PlanId) {
				if err := s.containerRepo.DeleteContainerState(plan.ContainerId); err != nil {
					reconcileErr = errors.Join(reconcileErr, fmt.Errorf("delete Redis state for aborted state-volume plan %q: %w", plan.ContainerId, err))
				}
			}
			continue
		}
		if stateErr == nil {
			if state.StateVolumePlanId != plan.PlanId || state.StateVolumePlanHash != plan.RequestHash {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("container %q belongs to a different state-volume attachment plan", plan.ContainerId))
				continue
			}
			resumingAbort := state.WorkerId == "" && state.Status == types.ContainerStatusStopping &&
				state.StateVolumeAborting == plan.PlanId
			if state.WorkerId != "" || (state.Status != types.ContainerStatusPending && !resumingAbort) {
				if plan.Admitted {
					if err := s.requestBacklog.AcknowledgeStateVolumePlanDispatch(plan.PlanId); err != nil {
						reconcileErr = errors.Join(reconcileErr, fmt.Errorf("acknowledge assigned state-volume plan %q: %w", plan.ContainerId, err))
					}
					continue
				}
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("unadmitted state-volume plan %q reached a non-pending or assigned container", plan.ContainerId))
				continue
			}
			if !plan.Admitted {
				hasOutbox, outboxErr := s.requestBacklog.StateVolumePlanOutboxExists(plan.PlanId, plan.ContainerId)
				if outboxErr != nil {
					reconcileErr = errors.Join(reconcileErr, fmt.Errorf("inspect state-volume attachment plan outbox %q: %w", plan.ContainerId, outboxErr))
					continue
				}
				if !hasOutbox {
					fenced, abortErr := s.containerRepo.FencePendingContainerStateVolumePlan(
						plan.ContainerId, plan.PlanId, plan.RequestHash, types.ContainerStateTtlSWhileStopping)
					if abortErr == nil && !fenced {
						abortErr = fmt.Errorf("exact pending state disappeared before abort fencing")
					}
					if abortErr == nil {
						abortErr = s.backendRepo.AbortStateVolumeAttachmentPlan(ctx, plan.WorkspaceId, plan.ContainerId, plan.PlanId, plan.RequestHash)
					}
					if abortErr == nil {
						abortErr = s.containerRepo.DeleteContainerState(plan.ContainerId)
					}
					if abortErr != nil {
						reconcileErr = errors.Join(reconcileErr, fmt.Errorf("roll back unadmitted state-volume plan %q: %w", plan.ContainerId, abortErr))
					}
					continue
				}
				if err := s.backendRepo.CompleteStateVolumeAttachmentPlan(ctx, plan.WorkspaceId, plan.ContainerId, plan.PlanId, plan.RequestHash); err != nil {
					reconcileErr = errors.Join(reconcileErr, fmt.Errorf("complete state-volume attachment plan %q: %w", plan.ContainerId, err))
					continue
				}
			}
			if _, err := s.requestBacklog.PromoteStateVolumePlan(plan.PlanId, plan.ContainerId); err != nil {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("promote state-volume attachment plan %q: %w", plan.ContainerId, err))
				continue
			}
			if err := s.backendRepo.MarkStateVolumeAttachmentPlanEnqueued(ctx, plan.WorkspaceId, plan.ContainerId, plan.PlanId, plan.RequestHash); err != nil {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("mark state-volume attachment plan %q enqueued: %w", plan.ContainerId, err))
				continue
			}
			if _, err := s.requestBacklog.RecoverStateVolumePlan(plan.PlanId, plan.ContainerId, plan.RequestHash, time.Now()); err != nil {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("recover state-volume attachment plan %q after processing lease: %w", plan.ContainerId, err))
			}
			continue
		}
		notFound := &types.ErrContainerStateNotFound{}
		if !notFound.From(stateErr) {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("read container %q for state-volume plan: %w", plan.ContainerId, stateErr))
			continue
		}
		if plan.Admitted {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("admitted state-volume attachment plan %q lost its container state", plan.ContainerId))
			continue
		}
		fenced, fenceErr := s.containerRepo.FencePendingContainerStateVolumePlan(
			plan.ContainerId, plan.PlanId, plan.RequestHash, types.ContainerStateTtlSWhileStopping)
		if fenceErr != nil || !fenced {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("fence orphan state-volume attachment plan %q: %w", plan.ContainerId, fenceErr))
			continue
		}
		if err := s.backendRepo.AbortStateVolumeAttachmentPlan(ctx, plan.WorkspaceId, plan.ContainerId, plan.PlanId, plan.RequestHash); err != nil {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("abort orphan state-volume attachment plan %q: %w", plan.ContainerId, err))
		}
	}
	return reconcileErr
}

// reconcileUnarmedStateSnapshots closes the Create -> durable journal Bind
// crash window. An unarmed operation has not crossed the worker's fsynced
// recovery boundary, so it can be failed only after control-plane state proves
// both that the source worker is gone/disabled and that the source container is
// no longer assigned to it. The repository CASes armed_at while holding the
// snapshot row lock, so a concurrent successful Arm always wins or makes this
// fail closed. Writer attachments are deliberately left fenced for the normal
// container teardown path; absence from Redis is not proof that its QSD and
// mount are gone.
func (s *Scheduler) reconcileUnarmedStateSnapshots(ctx context.Context, olderThan time.Time) error {
	if s == nil || s.backendRepo == nil || s.containerRepo == nil || s.workerRepo == nil {
		return nil
	}
	snapshots, err := s.backendRepo.ListUnarmedPendingStateSnapshots(ctx, olderThan)
	if err != nil {
		return err
	}
	var reconcileErr error
	for index := range snapshots {
		snapshot := &snapshots[index]
		worker, workerErr := s.workerRepo.GetWorkerById(snapshot.SourceWorkerId)
		workerGone := false
		if workerErr != nil {
			var notFound types.ErrWorkerNotFound
			if !notFound.From(workerErr) {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("inspect source worker %q for unarmed state snapshot %q: %w", snapshot.SourceWorkerId, snapshot.ExternalId, workerErr))
				continue
			}
			workerGone = true
		} else {
			workerGone = worker.Status == types.WorkerStatusDisabled
		}
		if !workerGone {
			continue
		}

		state, stateErr := s.containerRepo.GetContainerState(snapshot.SourceContainerId)
		if stateErr == nil && state.WorkerId == snapshot.SourceWorkerId {
			// A disabled worker may still be draining the source container. Its
			// exact assignment remains authoritative until teardown moves/removes it.
			continue
		}
		if stateErr != nil {
			var notFound types.ErrContainerStateNotFound
			if !notFound.From(stateErr) {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("inspect source container %q for unarmed state snapshot %q: %w", snapshot.SourceContainerId, snapshot.ExternalId, stateErr))
				continue
			}
		}

		reason := "snapshot recovery was never armed before source worker teardown"
		if _, err := s.backendRepo.FailUnarmedStateSnapshot(ctx, snapshot.ExternalId, reason); err != nil {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("fail unarmed state snapshot %q: %w", snapshot.ExternalId, err))
		}
	}
	return reconcileErr
}

func (s *Scheduler) runStateVolumeAttachmentPlanReconciler(ctx context.Context) {
	reconcile := func() {
		olderThan := time.Now().Add(-stateVolumeAttachmentPlanOrphanAge)
		if err := s.reconcilePendingStateVolumeAttachmentPlans(ctx, olderThan); err != nil {
			log.Error().Err(err).Msg("failed to reconcile pending state-volume attachment plans")
		}
		if err := s.reconcileUnarmedStateSnapshots(ctx, olderThan); err != nil {
			log.Error().Err(err).Msg("failed to reconcile unarmed state snapshots")
		}
	}
	reconcile()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}
