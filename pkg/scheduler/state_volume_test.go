package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type stateVolumeBackendForTest struct {
	repository.BackendRepository
	mu               sync.Mutex
	snapshot         *types.StateSnapshot
	attachments      map[string]*types.StateVolumeAttachment
	requestedSeeds   map[string]string
	branchSeeds      map[string]string
	released         []types.StateVolumeLease
	pendingReleased  bool
	planHash         string
	plannedMembers   int
	planCompleted    bool
	planCreatedAt    time.Time
	planAborted      int
	planMarked       int
	pendingPlans     []types.StateVolumeAttachmentPlan
	unarmedSnapshots []types.StateSnapshot
	failedUnarmed    []string
	resolvedBranches map[string]*types.StateVolumeAttachment
}

func (r *stateVolumeBackendForTest) GetWorkspaceByExternalId(_ context.Context, externalID string) (types.Workspace, error) {
	return types.Workspace{Id: 9, ExternalId: externalID}, nil
}

func (r *stateVolumeBackendForTest) GetStateSnapshot(context.Context, uint, string) (*types.StateSnapshot, error) {
	return r.snapshot, nil
}

func (r *stateVolumeBackendForTest) GetDisk(_ context.Context, _ uint, name string) (*types.Disk, error) {
	attachment := r.attachments[name]
	if attachment == nil {
		return nil, errors.New("disk not found")
	}
	return &types.Disk{ExternalId: attachment.VolumeId, Name: name, Size: attachment.Size, MountPath: attachment.MountPath}, nil
}

func (r *stateVolumeBackendForTest) GetVolumeGeneration(_ context.Context, _ uint, generationID string) (*types.VolumeGeneration, error) {
	if r.snapshot != nil {
		for _, member := range r.snapshot.Generations {
			if member.GenerationId == generationID {
				return &types.VolumeGeneration{ExternalId: member.GenerationId, VolumeId: member.VolumeId,
					Name: member.Name, ParentGenerationId: member.ParentGenerationId,
					CloneParentGenerationId: member.CloneParentGenerationId,
					Generation:              member.Generation, Status: types.StateSnapshotStatusAvailable}, nil
			}
		}
	}
	for name, attachment := range r.attachments {
		if attachment.SourceGenerationId == generationID {
			return &types.VolumeGeneration{ExternalId: generationID, VolumeId: attachment.VolumeId,
				Name: name, Generation: 1, Status: types.StateSnapshotStatusAvailable}, nil
		}
	}
	return nil, errors.New("generation not found")
}

func (r *stateVolumeBackendForTest) GetLatestVolumeGeneration(ctx context.Context, workspaceID uint, volumeID string) (*types.VolumeGeneration, error) {
	for _, attachment := range r.attachments {
		if attachment.VolumeId == volumeID {
			return r.GetVolumeGeneration(ctx, workspaceID, attachment.SourceGenerationId)
		}
	}
	return nil, errors.New("generation not found")
}

func (r *stateVolumeBackendForTest) BeginStateVolumeAttachmentPlan(_ context.Context, workspaceId uint, containerId, requestHash string, expectedWritableMembers int) (*types.StateVolumeAttachmentPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.planHash != "" && (r.planHash != requestHash || r.plannedMembers != expectedWritableMembers) {
		return nil, errors.New("immutable plan mismatch")
	}
	owned := r.planHash == ""
	r.planHash, r.plannedMembers = requestHash, expectedWritableMembers
	if r.planCreatedAt.IsZero() {
		r.planCreatedAt = time.Unix(1_700_000_000, 0).UTC()
	}
	return &types.StateVolumeAttachmentPlan{PlanId: "7aee3365-2963-4a6d-b9fb-2c934924880d", WorkspaceId: workspaceId,
		ContainerId: containerId, RequestHash: requestHash, ExpectedWritableMembers: expectedWritableMembers,
		CreatedAt: r.planCreatedAt, Owned: owned}, nil
}

func (r *stateVolumeBackendForTest) CompleteStateVolumeAttachmentPlan(_ context.Context, _ uint, _ string, _ string, requestHash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if requestHash != r.planHash {
		return errors.New("immutable plan mismatch")
	}
	r.planCompleted = true
	return nil
}

func (r *stateVolumeBackendForTest) MarkStateVolumeAttachmentPlanEnqueued(context.Context, uint, string, string, string) error {
	r.mu.Lock()
	r.planMarked++
	r.mu.Unlock()
	return nil
}

func (r *stateVolumeBackendForTest) ListIncompleteStateVolumeAttachmentPlans(context.Context, time.Time) ([]types.StateVolumeAttachmentPlan, error) {
	return r.pendingPlans, nil
}

func (r *stateVolumeBackendForTest) ListUnarmedPendingStateSnapshots(context.Context, time.Time) ([]types.StateSnapshot, error) {
	return r.unarmedSnapshots, nil
}

func (r *stateVolumeBackendForTest) FailUnarmedStateSnapshot(_ context.Context, snapshotID, _ string) (*types.StateSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := range r.unarmedSnapshots {
		if r.unarmedSnapshots[index].ExternalId != snapshotID {
			continue
		}
		r.failedUnarmed = append(r.failedUnarmed, snapshotID)
		failed := r.unarmedSnapshots[index]
		failed.Status = types.StateSnapshotStatusFailed
		return &failed, nil
	}
	return nil, errors.New("state snapshot not found")
}

func (r *stateVolumeBackendForTest) ResolveStateVolumeAttachment(_ context.Context, _ uint, containerID, _, _ string, disk *types.Disk, sourceGenerationID string) (*types.StateVolumeAttachment, error) {
	r.requestedSeeds[disk.Name] = sourceGenerationID
	attachment := *r.attachments[disk.Name]
	attachment.ContainerId = containerID
	attachment.Name, attachment.Size, attachment.MountPath = disk.Name, disk.Size, disk.MountPath
	return &attachment, nil
}

func (r *stateVolumeBackendForTest) ResolveReadOnlyStateAttachment(_ context.Context, _ uint, _ string, _, generationID, name, _ string, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.requestedSeeds == nil {
		r.requestedSeeds = map[string]string{}
	}
	r.requestedSeeds[name] = generationID
	return nil
}

func (r *stateVolumeBackendForTest) ResolveBranchStateAttachment(_ context.Context, _ uint, _ string, containerID, _, _, volumeID, name, size, mountPath, sourceGenerationID string, root, cloneSource bool) (*types.StateVolumeAttachment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.branchSeeds == nil {
		r.branchSeeds = map[string]string{}
	}
	r.branchSeeds[name] = sourceGenerationID
	if r.resolvedBranches == nil {
		r.resolvedBranches = map[string]*types.StateVolumeAttachment{}
	}
	if existing := r.resolvedBranches[volumeID]; existing != nil {
		replayed := *existing
		replayed.Replayed = true
		return &replayed, nil
	}
	attachment := &types.StateVolumeAttachment{
		VolumeId: volumeID, Name: name, Size: size, MountPath: mountPath, ContainerId: containerID,
		SourceGenerationId: sourceGenerationID, CloneSource: cloneSource,
		AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: int64(len(r.branchSeeds)),
		ExpiresAt: time.Now().Add(time.Minute),
	}
	r.resolvedBranches[volumeID] = attachment
	copy := *attachment
	return &copy, nil
}

func (r *stateVolumeBackendForTest) ReleaseStateVolumeAttachments(_ context.Context, _ uint, _, _, _, _ string, leases []types.StateVolumeLease) error {
	r.released = append(r.released, leases...)
	return nil
}

func (r *stateVolumeBackendForTest) AbortStateVolumeAttachmentPlan(_ context.Context, workspaceID uint, containerID, _, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.planCompleted {
		return errors.New("admitted state-volume attachment plan cannot be aborted")
	}
	r.planAborted++
	if workspaceID == 9 && containerID == "pending-state-volume" {
		r.pendingReleased = true
	}
	return nil
}

func (r *stateVolumeBackendForTest) ReleasePendingStateVolumeAttachments(_ context.Context, workspaceID uint, containerID, _, _ string) error {
	if workspaceID == 9 && containerID == "pending-state-volume" {
		r.pendingReleased = true
	}
	return nil
}

func TestResolveStateVolumeAttachmentsBindsStableIdentityAndWritableLease(t *testing.T) {
	repo := &stateVolumeBackendForTest{
		requestedSeeds: map[string]string{},
		attachments: map[string]*types.StateVolumeAttachment{
			"data": {VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", SourceGenerationId: "acee3e88-20d7-4bbc-92cc-4b839ad6bc55", Initialize: false,
				AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 7, ExpiresAt: time.Now().Add(time.Minute)},
			"models": {VolumeId: "f0d82a87-d142-4333-a9c6-dc621128ee64", SourceGenerationId: "55d6eb7e-7040-44ab-b2a5-e0b690b862c1",
				Size: "8Gi", MountPath: "/models", ReadOnly: true},
		},
	}
	s := &Scheduler{backendRepo: repo}
	request := &types.ContainerRequest{ContainerId: "container-1", StubId: "destination-stub", Workspace: types.Workspace{Id: 9}, Mounts: []types.Mount{
		{MountPath: "/data", MountType: types.StorageModeDurableDisk, DurableDisk: &types.DurableDiskMountConfig{Name: "data", Size: "4Gi"}},
		{MountPath: "/models", ReadOnly: true, MountType: types.StorageModeDurableDisk, DurableDisk: &types.DurableDiskMountConfig{Name: "models", Size: "8Gi", SourceGenerationId: "55d6eb7e-7040-44ab-b2a5-e0b690b862c1"}},
	}}

	rollback, err := s.resolveStateVolumeAttachments(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "21d4182a-4930-47b4-a987-e50c4a80156f", request.Mounts[0].DurableDisk.VolumeId)
	require.Equal(t, "35141b8e-4591-4c72-856a-3ab7e831818e", request.Mounts[0].DurableDisk.AttachmentToken)
	require.EqualValues(t, 7, request.Mounts[0].DurableDisk.FencingToken)
	require.Empty(t, request.Mounts[1].DurableDisk.AttachmentToken)
	require.Equal(t, "55d6eb7e-7040-44ab-b2a5-e0b690b862c1", repo.requestedSeeds["models"])
	require.Equal(t, []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 7,
	}}, rollback)
}

func TestResolveStateSnapshotUsesExactGenerationAndRejectsDifferentStableVolume(t *testing.T) {
	repo := &stateVolumeBackendForTest{
		requestedSeeds: map[string]string{},
		snapshot: &types.StateSnapshot{Mode: "terminal", SourceStubExternalId: "restore-stub", Status: types.StateSnapshotStatusAvailable, Generations: []types.StateGeneration{
			{VolumeId: "snapshot-volume", GenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d", Name: "data", MountPath: "/data", Generation: 3},
		}},
		attachments: map[string]*types.StateVolumeAttachment{
			"data": {VolumeId: "different-volume", SourceGenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d",
				AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 8},
		},
	}
	s := &Scheduler{backendRepo: repo}
	request := &types.ContainerRequest{ContainerId: "container-restore", StubId: "restore-stub", Workspace: types.Workspace{Id: 9}, StateSnapshotId: "snapshot-1", Mounts: []types.Mount{
		{MountPath: "/data", MountType: types.StorageModeDurableDisk, DurableDisk: &types.DurableDiskMountConfig{Name: "data", Size: "4Gi"}},
	}}

	rollback, err := s.resolveStateVolumeAttachments(context.Background(), request)
	require.ErrorContains(t, err, "does not match registered durable disk")
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", repo.requestedSeeds["data"])
	require.Len(t, rollback, 1)
}

func TestResolveForkBindsWritableRootAndExtraMemberToSeparateBranchLeases(t *testing.T) {
	repo := &stateVolumeBackendForTest{snapshot: &types.StateSnapshot{
		Mode: "live", SourceStubExternalId: "source-stub", Status: types.StateSnapshotStatusAvailable,
		Generations: []types.StateGeneration{
			{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", GenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d", Name: "root", MountPath: "/", Root: true, Generation: 4},
			{VolumeId: "f0d82a87-d142-4333-a9c6-dc621128ee64", GenerationId: "55d6eb7e-7040-44ab-b2a5-e0b690b862c1", Name: "data", MountPath: "/data", Generation: 7},
		},
	}}
	s := &Scheduler{backendRepo: repo}
	request := &types.ContainerRequest{
		ContainerId: "fork-container", WorkspaceId: "workspace", Workspace: types.Workspace{Id: 9},
		StubId: "destination-stub", StateSnapshotId: "snapshot", PersistentRoot: &types.PersistentRoot{Size: "4Gi"},
		Mounts: []types.Mount{{MountPath: "/data", MountType: types.StorageModeDurableDisk,
			DurableDisk: &types.DurableDiskMountConfig{Name: "data", Size: "4Gi"}}},
	}
	rollback, err := s.resolveStateVolumeAttachments(context.Background(), request)
	require.NoError(t, err)
	require.True(t, request.StateFork)
	require.NotNil(t, request.RootState)
	require.True(t, request.RootState.CloneSource)
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", request.RootState.SourceGenerationId)
	require.True(t, request.Mounts[0].DurableDisk.CloneSource)
	require.Equal(t, "55d6eb7e-7040-44ab-b2a5-e0b690b862c1", request.Mounts[0].DurableDisk.SourceGenerationId)
	require.NotEqual(t, request.RootState.VolumeId, request.Mounts[0].DurableDisk.VolumeId)
	require.Len(t, rollback, 2)
}

func TestStopBeforeAssignmentReleasesPendingWritableLease(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	backend := &stateVolumeBackendForTest{}
	s.backendRepo = backend
	require.NoError(t, s.containerRepo.SetContainerState("pending-state-volume", &types.ContainerState{
		ContainerId: "pending-state-volume", WorkspaceId: "workspace-1", Status: types.ContainerStatusPending,
		NbdDevices: 1, ScheduledAt: time.Now().Unix(),
	}))
	require.NoError(t, s.Stop(&types.StopContainerArgs{ContainerId: "pending-state-volume", Reason: types.StopContainerReasonUser}))
	require.True(t, backend.pendingReleased)
}

func TestStateVolumeAttachmentPlanIdentityIgnoresEphemeralSchedulerFields(t *testing.T) {
	base := &types.ContainerRequest{
		ContainerId: "container", WorkspaceId: "workspace", StubId: "stub",
		PersistentRoot: &types.PersistentRoot{Size: "4Gi"},
		Timestamp:      time.Unix(100, 0), RetryCount: 1, DeliveryToken: "delivery-a", ProvisioningAttempts: 2,
		RootState: &types.RootStateMountConfig{VolumeId: "worker-authored"}, StateFork: true,
	}
	retry := base.Clone()
	retry.Timestamp = time.Unix(900, 0)
	retry.RetryCount = 12
	retry.DeliveryToken = "delivery-b"
	retry.ProvisioningAttempts = 9
	retry.RootState = &types.RootStateMountConfig{VolumeId: "different-worker-authored"}
	retry.StateFork = true

	baseHash, baseMembers, err := stateVolumeAttachmentPlanIdentity(base)
	require.NoError(t, err)
	retryHash, retryMembers, err := stateVolumeAttachmentPlanIdentity(retry)
	require.NoError(t, err)
	require.Equal(t, baseMembers, retryMembers)
	require.Equal(t, baseHash, retryHash)

	retry.StateFork = false
	forkHash, _, err := stateVolumeAttachmentPlanIdentity(retry)
	require.NoError(t, err)
	require.NotEqual(t, baseHash, forkHash)
	retry.StateFork = true
	retry.PersistentRoot.Size = "8Gi"
	changedHash, _, err := stateVolumeAttachmentPlanIdentity(retry)
	require.NoError(t, err)
	require.NotEqual(t, baseHash, changedHash)
}

func TestConcurrentExactStateVolumeRunsShareOneAdmissionAndNeverAbortWinner(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	backend := &stateVolumeBackendForTest{}
	s.backendRepo = &BackendRepoConcurrencyLimitsForTest{
		BackendRepository: backend, CPUConcurrencyLimit: 100_000,
	}
	base := &types.ContainerRequest{
		ContainerId: "state-volume-concurrent", WorkspaceId: "workspace", StubId: "stub",
		Workspace: types.Workspace{Id: 9}, PersistentRoot: &types.PersistentRoot{Size: "4Gi"},
		Cpu: 100, Memory: 128,
	}

	requests := []*types.ContainerRequest{base.Clone(), base.Clone()}
	errs := make(chan error, len(requests))
	var wg sync.WaitGroup
	for _, request := range requests {
		wg.Add(1)
		go func(request *types.ContainerRequest) {
			defer wg.Done()
			errs <- s.Run(request)
		}(request)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, s.requestBacklog.Len())
	state, err := s.containerRepo.GetContainerState(base.ContainerId)
	require.NoError(t, err)
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", state.StateVolumePlanId)
	require.NotEmpty(t, state.StateVolumePlanHash)
	backend.mu.Lock()
	require.True(t, backend.planCompleted)
	require.Zero(t, backend.planAborted)
	require.GreaterOrEqual(t, backend.planMarked, 1)
	backend.mu.Unlock()
}

func TestAttachmentPlanReconcilerPromotesExactOutboxAndNeverReleasesAdmittedPlan(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	plan := types.StateVolumeAttachmentPlan{
		PlanId: "7aee3365-2963-4a6d-b9fb-2c934924880d", WorkspaceId: 9,
		ContainerId: "state-volume-reconcile", RequestHash: strings.Repeat("a", 64),
		ExpectedWritableMembers: 1, CreatedAt: createdAt,
	}
	backend := &stateVolumeBackendForTest{
		planHash: plan.RequestHash, plannedMembers: 1, planCreatedAt: createdAt,
		pendingPlans: []types.StateVolumeAttachmentPlan{plan},
	}
	s.backendRepo = backend
	request := &types.ContainerRequest{
		ContainerId: plan.ContainerId, WorkspaceId: "workspace", StubId: "stub",
		StateVolumePlanId: plan.PlanId, StateVolumePlanHash: plan.RequestHash,
		PersistentRoot: &types.PersistentRoot{Size: "4Gi"}, Timestamp: time.Now(),
		RootState: &types.RootStateMountConfig{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f",
			AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 7,
			LeaseExpiresAtUnix: time.Now().Add(time.Minute).Unix()},
	}
	queued, err := canonicalStateVolumeOutboxRequest(request, &plan)
	require.NoError(t, err)
	payload, err := json.Marshal(queued)
	require.NoError(t, err)
	require.NoError(t, s.containerRepo.CreateContainerStateWithConcurrencyLimitAndStateVolumeOutbox(nil, request, payload, createdAt))

	require.NoError(t, s.reconcilePendingStateVolumeAttachmentPlans(context.Background(), time.Now()))
	popped, err := s.requestBacklog.Pop()
	require.NoError(t, err)
	require.Equal(t, request.ContainerId, popped.ContainerId)
	require.Equal(t, createdAt, popped.Timestamp)
	require.Zero(t, popped.RootState.LeaseExpiresAtUnix)

	// The destructive pop installs a processing lease. Reconciliation does not
	// duplicate an actively processed request, but redrives the exact retained
	// outbox payload once that lease is gone after a scheduler crash.
	plan.Admitted = true
	backend.pendingPlans = []types.StateVolumeAttachmentPlan{plan}
	require.NoError(t, s.reconcilePendingStateVolumeAttachmentPlans(context.Background(), time.Now()))
	require.Zero(t, s.requestBacklog.Len())
	require.NoError(t, s.requestBacklog.rdb.ZRem(context.Background(), common.RedisKeys.SchedulerStateVolumeProcessing(), plan.PlanId).Err())
	require.NoError(t, s.reconcilePendingStateVolumeAttachmentPlans(context.Background(), time.Now()))
	require.EqualValues(t, 1, s.requestBacklog.Len())
	popped, err = s.requestBacklog.Pop()
	require.NoError(t, err)
	require.Equal(t, request.ContainerId, popped.ContainerId)

	// Durable worker assignment is the processing acknowledgement. Even after
	// the lease disappears, an assigned request is never requeued.
	stateKey := common.RedisKeys.SchedulerContainerState(plan.ContainerId)
	require.NoError(t, s.requestBacklog.rdb.HSet(context.Background(), stateKey, "worker_id", "winner-worker").Err())
	require.NoError(t, s.reconcilePendingStateVolumeAttachmentPlans(context.Background(), time.Now()))
	require.Zero(t, s.requestBacklog.Len())

	require.NoError(t, s.containerRepo.DeleteContainerState(plan.ContainerId))
	err = s.reconcilePendingStateVolumeAttachmentPlans(context.Background(), time.Now())
	require.ErrorContains(t, err, "lost its container state")
	backend.mu.Lock()
	require.Zero(t, backend.planAborted)
	backend.mu.Unlock()
}

func TestUnarmedStateSnapshotReaperRequiresDeadWorkerAndChangedAssignment(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	snapshot := types.StateSnapshot{
		ExternalId:        "8b19f2b6-3f5b-475b-b16a-eeb3a0da5072",
		SourceContainerId: "source-container",
		SourceWorkerId:    "source-worker",
		StorageNodeId:     "storage-node",
		Mode:              "terminal",
		Status:            types.StateSnapshotStatusPending,
	}
	backend := &stateVolumeBackendForTest{unarmedSnapshots: []types.StateSnapshot{snapshot}}
	s.backendRepo = backend
	require.NoError(t, s.workerRepo.AddWorker(&types.Worker{
		Id: "source-worker", MachineId: "storage-node", Status: types.WorkerStatusAvailable,
	}))
	require.NoError(t, s.containerRepo.SetContainerState(snapshot.SourceContainerId, &types.ContainerState{
		ContainerId: snapshot.SourceContainerId, WorkerId: snapshot.SourceWorkerId,
		Status: types.ContainerStatusRunning,
	}))

	// A live source worker can still finish Bind -> Arm, so age alone is never
	// authority to cancel its pending operation.
	require.NoError(t, s.reconcileUnarmedStateSnapshots(context.Background(), time.Now()))
	require.Empty(t, backend.failedUnarmed)

	// Disabling the worker is also insufficient while the exact container
	// assignment remains; it may still be completing its graceful barrier.
	require.NoError(t, s.workerRepo.UpdateWorkerStatus(snapshot.SourceWorkerId, types.WorkerStatusDisabled))
	require.NoError(t, s.reconcileUnarmedStateSnapshots(context.Background(), time.Now()))
	require.Empty(t, backend.failedUnarmed)

	// Once scheduling has authoritatively moved the container away from the
	// disabled source, the repository's armed_at CAS may fail the orphan.
	require.NoError(t, s.containerRepo.SetContainerState(snapshot.SourceContainerId, &types.ContainerState{
		ContainerId: snapshot.SourceContainerId, WorkerId: "replacement-worker",
		Status: types.ContainerStatusPending,
	}))
	require.NoError(t, s.reconcileUnarmedStateSnapshots(context.Background(), time.Now()))
	require.Equal(t, []string{snapshot.ExternalId}, backend.failedUnarmed)
}

func TestUnarmedStateSnapshotReaperFailsMissingWorkerAndContainer(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	snapshot := types.StateSnapshot{
		ExternalId:        "d0932f99-cec0-47af-8f13-10c80a253325",
		SourceContainerId: "lost-container",
		SourceWorkerId:    "lost-worker",
		StorageNodeId:     "storage-node",
		Mode:              "terminal",
		Status:            types.StateSnapshotStatusPending,
	}
	backend := &stateVolumeBackendForTest{unarmedSnapshots: []types.StateSnapshot{snapshot}}
	s.backendRepo = backend

	require.NoError(t, s.reconcileUnarmedStateSnapshots(context.Background(), time.Now()))
	require.Equal(t, []string{snapshot.ExternalId}, backend.failedUnarmed)
}
