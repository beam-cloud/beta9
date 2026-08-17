package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	betaruntime "github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordingStateVolumeAttachmentRepo struct {
	pb.BackendRepositoryServiceClient
	mu              sync.Mutex
	renewRequests   []*pb.RenewStateVolumeAttachmentsRequest
	beginRequests   []*pb.BeginStateVolumeReleaseIntentRequest
	releaseRequests []*pb.ReleaseStateVolumeAttachmentsRequest
	renewFailures   []error
	renewResponses  []*pb.RenewStateVolumeAttachmentsResponse
	beginFailure    error
	releaseFailure  error
	events          []string
	beginCheck      func(*pb.BeginStateVolumeReleaseIntentRequest) error
	releaseCheck    func(*pb.ReleaseStateVolumeAttachmentsRequest) error
}

type snapshotBindingRepo struct {
	pb.BackendRepositoryServiceClient
	snapshot      *pb.StateSnapshot
	createRequest *pb.CreateStateSnapshotRequest
	armRequest    *pb.ArmStateSnapshotRequest
	operation     *pb.GetStateSnapshotResponse
}

type assignmentBoundRestoreReceiptRepo struct {
	pb.ContainerRepositoryServiceClient
	deliveryToken, planID, planHash string
}

func (r *assignmentBoundRestoreReceiptRepo) SetStateRestoreReceipt(_ context.Context, in *pb.SetStateRestoreReceiptRequest, _ ...grpc.CallOption) (*pb.SetStateRestoreReceiptResponse, error) {
	if in.DeliveryToken != r.deliveryToken || in.StateVolumePlanId != r.planID || in.StateVolumePlanHash != r.planHash {
		return &pb.SetStateRestoreReceiptResponse{Ok: false, ErrorMsg: "stale delivered state-volume assignment"}, nil
	}
	return &pb.SetStateRestoreReceiptResponse{Ok: true}, nil
}

func (r *snapshotBindingRepo) GetStateSnapshotByOperation(_ context.Context, _ *pb.GetStateSnapshotByOperationRequest, _ ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	if r.operation != nil {
		return r.operation, nil
	}
	return &pb.GetStateSnapshotResponse{Ok: false}, nil
}

func (r *snapshotBindingRepo) GetStateSnapshot(_ context.Context, _ *pb.GetStateSnapshotRequest, _ ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	return &pb.GetStateSnapshotResponse{Ok: r.snapshot != nil, Snapshot: r.snapshot}, nil
}

func (r *snapshotBindingRepo) CreateStateSnapshot(_ context.Context, in *pb.CreateStateSnapshotRequest, _ ...grpc.CallOption) (*pb.CreateStateSnapshotResponse, error) {
	r.createRequest = in
	r.snapshot = in.Snapshot
	return &pb.CreateStateSnapshotResponse{Ok: true, Snapshot: in.Snapshot, RecoveryProofToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, nil
}

func (r *snapshotBindingRepo) ArmStateSnapshot(_ context.Context, in *pb.ArmStateSnapshotRequest, _ ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	r.armRequest = in
	if in.RecoveryProofToken != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "recovery proof rejected"}, nil
	}
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: r.snapshot}, nil
}

func (r *recordingStateVolumeAttachmentRepo) RenewStateVolumeAttachments(_ context.Context, in *pb.RenewStateVolumeAttachmentsRequest, _ ...grpc.CallOption) (*pb.RenewStateVolumeAttachmentsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	call := len(r.renewRequests)
	r.renewRequests = append(r.renewRequests, in)
	if call < len(r.renewFailures) && r.renewFailures[call] != nil {
		return nil, r.renewFailures[call]
	}
	if call < len(r.renewResponses) && r.renewResponses[call] != nil {
		return r.renewResponses[call], nil
	}
	return &pb.RenewStateVolumeAttachmentsResponse{Ok: true, LeaseExpiresAt: timestamppb.New(time.Now().Add(2 * time.Minute))}, nil
}

func (r *recordingStateVolumeAttachmentRepo) ReleaseStateVolumeAttachments(_ context.Context, in *pb.ReleaseStateVolumeAttachmentsRequest, _ ...grpc.CallOption) (*pb.ReleaseStateVolumeAttachmentsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "release")
	r.releaseRequests = append(r.releaseRequests, in)
	if r.releaseCheck != nil {
		if err := r.releaseCheck(in); err != nil {
			return nil, err
		}
	}
	if r.releaseFailure != nil {
		return nil, r.releaseFailure
	}
	return &pb.ReleaseStateVolumeAttachmentsResponse{Ok: true}, nil
}

func (r *recordingStateVolumeAttachmentRepo) BeginStateVolumeReleaseIntent(_ context.Context, in *pb.BeginStateVolumeReleaseIntentRequest, _ ...grpc.CallOption) (*pb.ClaimStateVolumeReleaseResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "begin")
	r.beginRequests = append(r.beginRequests, in)
	if r.beginCheck != nil {
		if err := r.beginCheck(in); err != nil {
			return nil, err
		}
	}
	if r.beginFailure != nil {
		return nil, r.beginFailure
	}
	return &pb.ClaimStateVolumeReleaseResponse{
		Ok: true, ReleaseClaimId: "00000000-0000-4000-8000-000000000001",
	}, nil
}

type claimedReleaseRepo struct {
	pb.BackendRepositoryServiceClient
	mu               sync.Mutex
	claimGenerations []int64
	completeFailures []error
	completeCalls    int
}

func (r *claimedReleaseRepo) ClaimStateVolumeRelease(_ context.Context, in *pb.ClaimStateVolumeReleaseRequest, _ ...grpc.CallOption) (*pb.ClaimStateVolumeReleaseResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := in.PreviousClaimGeneration + 1
	r.claimGenerations = append(r.claimGenerations, next)
	return &pb.ClaimStateVolumeReleaseResponse{
		Ok: true, ReleaseClaimId: "00000000-0000-4000-8000-000000000001", ReleaseClaimGeneration: next,
	}, nil
}

func (r *claimedReleaseRepo) CompleteClaimedStateVolumeRelease(_ context.Context, _ *pb.CompleteClaimedStateVolumeReleaseRequest, _ ...grpc.CallOption) (*pb.CompleteClaimedStateVolumeReleaseResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	call := r.completeCalls
	r.completeCalls++
	if call < len(r.completeFailures) && r.completeFailures[call] != nil {
		return nil, r.completeFailures[call]
	}
	return &pb.CompleteClaimedStateVolumeReleaseResponse{Ok: true}, nil
}

func (r *recordingStateVolumeAttachmentRepo) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.renewRequests), len(r.releaseRequests)
}

func stateVolumeLeaseTestRequest(readOnly bool) *types.ContainerRequest {
	volumeID := uuid.NewString()
	config := &types.DurableDiskMountConfig{
		Name: "data", Size: "1Gi", VolumeId: volumeID, SourceGenerationId: uuid.NewString(),
		AttachmentToken: uuid.NewString(), FencingToken: 7, LeaseExpiresAtUnix: time.Now().Add(2 * time.Minute).Unix(),
	}
	if readOnly {
		config.AttachmentToken = ""
		config.FencingToken = 0
		config.LeaseExpiresAtUnix = 0
	}
	return &types.ContainerRequest{
		ContainerId: "container", Workspace: types.Workspace{ExternalId: "workspace"},
		Mounts: []types.Mount{{MountPath: "/data", ReadOnly: readOnly, DurableDisk: config}},
	}
}

func stateVolumeReleaseTestFixture(t *testing.T, repository *recordingStateVolumeAttachmentRepo) (*Worker, *types.ContainerRequest, *ContainerInstance, *StateVolumeManager) {
	t.Helper()
	request := stateVolumeLeaseTestRequest(false)
	request.Workspace.ExternalId = uuid.NewString()
	request.Mounts[0].DurableDisk.SourceGenerationId = ""
	request.Mounts[0].DurableDisk.Initialize = true
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "source-instance", StorageNodeID: "node",
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	handle, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: request.ContainerId, Volumes: []StateVolumeSpec{{
		ID: request.Mounts[0].DurableDisk.VolumeId, Name: "data", ContainerMountPath: "/data",
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024, Format: true,
		AttachmentToken: request.Mounts[0].DurableDisk.AttachmentToken, FencingToken: request.Mounts[0].DurableDisk.FencingToken,
	}}})
	require.NoError(t, err)
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, StateVolumes: handle}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances, stateVolumeManager: manager,
		workerId: "worker", workerInstanceId: "source-instance", machineID: "node",
		stateVolumeLeaseRenewInterval: time.Hour,
	}
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	return worker, request, instance, manager
}

func TestNormalStateVolumeReleasePersistsEscrowBeforeDetach(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	worker, request, instance, manager := stateVolumeReleaseTestFixture(t, repository)
	attachmentToken := request.Mounts[0].DurableDisk.AttachmentToken
	repository.beginCheck = func(in *pb.BeginStateVolumeReleaseIntentRequest) error {
		journal, err := manager.Journals.Load(request.ContainerId)
		if err != nil {
			return err
		}
		if journal.Phase != "release-detach-intent" || journal.Release == nil || journal.Release.LocalCleanupVerified {
			return fmt.Errorf("begin observed invalid local phase %q", journal.Phase)
		}
		if in.JournalDigest != journal.Release.JournalDigest || !strings.HasPrefix(in.JournalDigest, "sha256:") {
			return fmt.Errorf("begin digest does not match fsynced journal")
		}
		path, _ := manager.Journals.journalPath(request.ContainerId)
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(encoded, []byte(attachmentToken)) || bytes.Contains(encoded, []byte("attachment_token")) {
			return fmt.Errorf("release journal leaked attachment authority")
		}
		return nil
	}
	repository.releaseCheck = func(_ *pb.ReleaseStateVolumeAttachmentsRequest) error {
		journal, err := manager.Journals.Load(request.ContainerId)
		if err != nil {
			return err
		}
		if journal.Phase != "release-intent" || journal.Release == nil || !journal.Release.LocalCleanupVerified {
			return fmt.Errorf("release preceded verified local detach: phase=%q", journal.Phase)
		}
		group, err := manager.group(request.ContainerId)
		if err != nil {
			return err
		}
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.process != nil || group.qmp != nil || group.volumes[0].mounted || group.volumes[0].connected || group.volumes[0].lease != nil {
			return fmt.Errorf("release observed a live QSD/NBD/mount")
		}
		return nil
	}

	require.NoError(t, worker.stopAndReleaseStateVolumes(context.Background(), request, instance))
	repository.mu.Lock()
	require.Equal(t, []string{"begin", "release"}, repository.events)
	require.Len(t, repository.beginRequests, 1)
	require.Len(t, repository.releaseRequests, 1)
	repository.mu.Unlock()
	_, err := manager.Journals.Load(request.ContainerId)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, _, exists := manager.ExistingGroup(request.ContainerId)
	require.False(t, exists)
	instance.stateMu.RLock()
	require.Nil(t, instance.StateVolumeAttachments)
	instance.stateMu.RUnlock()
}

func TestReleaseOutageRetainsVerifiedJournalAndShutdownBoundary(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{releaseFailure: errors.New("repository outage")}
	worker, request, instance, manager := stateVolumeReleaseTestFixture(t, repository)
	err := worker.stopAndReleaseStateVolumes(context.Background(), request, instance)
	require.ErrorContains(t, err, "repository outage")
	journal, err := manager.Journals.Load(request.ContainerId)
	require.NoError(t, err)
	require.Equal(t, "release-intent", journal.Phase)
	require.True(t, journal.Release.LocalCleanupVerified)
	safe, err := manager.ShutdownSafeContainers()
	require.NoError(t, err)
	_, ok := safe[request.ContainerId]
	require.True(t, ok)
	require.NoError(t, worker.stateVolumeShutdownBoundaryError(), "actual worker shutdown barrier must accept only the verified replay obligation")
	instance.stateMu.RLock()
	require.NotNil(t, instance.StateVolumeAttachments, "failed release must retain its exact live tuple until server completion")
	instance.stateMu.RUnlock()

	repository.mu.Lock()
	repository.releaseFailure = nil
	repository.mu.Unlock()
	require.NoError(t, worker.stopAndReleaseStateVolumes(context.Background(), request, instance))
}

func TestReleaseOnlyCompletedJournalIsShutdownSafeAndFinalizable(t *testing.T) {
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "instance", StorageNodeID: "node",
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
	}
	release := StateVolumeReleaseEnvelope{
		WorkspaceID: uuid.NewString(), SourceWorkerID: "worker", SourceWorkerInstanceID: "instance", StorageNodeID: "node",
		Members: []StateVolumeReleaseMember{{VolumeID: uuid.NewString(), FencingToken: 3}},
	}
	release.JournalDigest, _ = stateVolumeReleaseJournalDigest("container", release)
	require.NoError(t, manager.PersistReleaseDetachIntent("container", release))
	require.NoError(t, manager.ArmReleaseIntent("container", "00000000-0000-4000-8000-000000000001", 0))
	require.NoError(t, manager.DetachReleaseIntent(context.Background(), "container"))
	require.NoError(t, manager.MarkReleaseCompleted("container"))
	safe, err := manager.ShutdownSafeContainers()
	require.NoError(t, err)
	_, ok := safe["container"]
	require.True(t, ok)
	require.NoError(t, manager.FinalizeReleaseIntent("container"))
	_, err = manager.Journals.Load("container")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReplacementReleaseClaimSurvivesClaimantCrash(t *testing.T) {
	root := t.TempDir()
	store := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	volumeID := uuid.NewString()
	release := StateVolumeReleaseEnvelope{
		WorkspaceID: uuid.NewString(), SourceWorkerID: "source-worker", SourceWorkerInstanceID: "source-instance",
		StorageNodeID: "node", Members: []StateVolumeReleaseMember{{VolumeID: volumeID, FencingToken: 9}},
	}
	release.JournalDigest, _ = stateVolumeReleaseJournalDigest("container", release)
	source := &StateVolumeManager{
		WorkerID: "source-worker", WorkerInstanceID: "source-instance", StorageNodeID: "node", Journals: store,
	}
	require.NoError(t, source.PersistReleaseDetachIntent("container", release))

	repository := &claimedReleaseRepo{completeFailures: []error{errors.New("claimant one crashed")}}
	claimantOneManager := &StateVolumeManager{
		WorkerID: "claimant-one", WorkerInstanceID: "claimant-one-instance", StorageNodeID: "node", Journals: store,
	}
	require.NoError(t, claimantOneManager.Reconcile(context.Background()))
	claimantOne := &Worker{
		stateVolumeManager: claimantOneManager, backendRepoClient: repository,
		workerId: "claimant-one", workerInstanceId: "claimant-one-instance", machineID: "node",
	}
	require.ErrorContains(t, claimantOne.reconcileStateVolumeReleaseJournals(context.Background()), "claimant one crashed")
	journal, err := store.Load("container")
	require.NoError(t, err)
	require.EqualValues(t, 1, journal.Release.ReleaseClaimGeneration)
	require.Equal(t, "claimant-one", journal.WorkerID)
	require.Equal(t, "source-worker", journal.Release.SourceWorkerID)

	claimantTwoManager := &StateVolumeManager{
		WorkerID: "claimant-two", WorkerInstanceID: "claimant-two-instance", StorageNodeID: "node", Journals: store,
	}
	require.NoError(t, claimantTwoManager.Reconcile(context.Background()))
	claimantTwo := &Worker{
		stateVolumeManager: claimantTwoManager, backendRepoClient: repository,
		workerId: "claimant-two", workerInstanceId: "claimant-two-instance", machineID: "node",
	}
	require.NoError(t, claimantTwo.reconcileStateVolumeReleaseJournals(context.Background()))
	_, err = store.Load("container")
	require.ErrorIs(t, err, os.ErrNotExist)
	repository.mu.Lock()
	require.Equal(t, []int64{1, 2}, repository.claimGenerations)
	require.Equal(t, 2, repository.completeCalls)
	repository.mu.Unlock()
}

func TestStateVolumeFreshDiskUsesSchedulerVolumeID(t *testing.T) {
	request := stateVolumeLeaseTestRequest(false)
	request.Mounts[0].DurableDisk.SourceGenerationId = ""
	request.Mounts[0].DurableDisk.Initialize = true

	spec, err := uninitializedStateVolumeGroupSpec(request)
	require.NoError(t, err)
	require.Len(t, spec.Volumes, 1)
	require.Equal(t, request.Mounts[0].DurableDisk.VolumeId, spec.Volumes[0].ID)
	require.Contains(t, spec.Volumes[0].BackingDir, stateVolumeToken("volume-", spec.Volumes[0].ID))
	require.NotContains(t, spec.Volumes[0].BackingDir, stateVolumeToken("container-", request.ContainerId))

	request.Mounts[0].DurableDisk.VolumeId = ""
	_, err = uninitializedStateVolumeGroupSpec(request)
	require.ErrorContains(t, err, "volume_id")
}

func TestPersistentRootIdentitySurvivesContainerRetry(t *testing.T) {
	volumeID := uuid.NewString()
	request := &types.ContainerRequest{
		ContainerId: "attempt-one", StubId: "stable-stub", Workspace: types.Workspace{ExternalId: "workspace"},
		PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
		RootState: &types.RootStateMountConfig{
			VolumeId: volumeID, Size: "1Gi", Initialize: true,
			AttachmentToken: uuid.NewString(), FencingToken: 9, LeaseExpiresAtUnix: time.Now().Add(2 * time.Minute).Unix(),
		},
	}
	first, err := uninitializedStateVolumeGroupSpec(request)
	require.NoError(t, err)
	require.Len(t, first.Volumes, 1)
	request.ContainerId = "attempt-two"
	second, err := uninitializedStateVolumeGroupSpec(request)
	require.NoError(t, err)
	require.Len(t, second.Volumes, 1)
	require.Equal(t, first.Volumes[0].ID, second.Volumes[0].ID)
	require.Equal(t, volumeID, first.Volumes[0].ID)
	require.NotEqual(t, first.Volumes[0].MountPath, second.Volumes[0].MountPath)
}

func TestStateVolumeReadOnlyAttachmentNeverRenews(t *testing.T) {
	leases, err := stateVolumeWriterLeases(stateVolumeLeaseTestRequest(true))
	require.NoError(t, err)
	require.Empty(t, leases)
}

func TestStateVolumePreflightRequiresMachineIdentityAndJammyNBDVersionFlag(t *testing.T) {
	require.ErrorContains(t, validateStateVolumeMachineIdentity("  "), "machine ID")
	require.NoError(t, validateStateVolumeMachineIdentity("node-one"))
	require.Equal(t, []string{"-V"}, stateVolumeNBDClientVersionArgs())
	total, free, err := validateStateVolumeNBDBudget(stateVolumeLocalNBDLimit, 0, true)
	require.NoError(t, err, "all journal-owned slots must reach reconciliation instead of failing preflight")
	require.EqualValues(t, stateVolumeLocalNBDLimit, total)
	require.Zero(t, free)
}

func TestStateVolumePreflightRejectsUnavailableCacheCapability(t *testing.T) {
	require.Error(t, validateStateVolumeStartupCache(nil))
	require.Error(t, validateStateVolumeStartupCache(&WorkerCacheManager{reporter: newTestReporter(&fakeEventRepo{})}))
	require.Error(t, validateStateVolumeStartupCache(&WorkerCacheManager{client: &cache.Client{}}))
	require.ErrorContains(t, validateStateVolumeStartupCache(&WorkerCacheManager{
		client: &cache.Client{}, reporter: newTestReporter(&repo.EventClientRepo{}),
	}), "durable scoped", "an initialized cache with no synchronous S2 sink must not advertise state-volume capacity")
	require.NoError(t, validateStateVolumeStartupCache(&WorkerCacheManager{
		client: &cache.Client{}, reporter: newTestReporter(&fakeEventRepo{}),
	}))
}

func TestStateVolumeAttachmentRenewsAndReleasesExactLease(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	request := stateVolumeLeaseTestRequest(false)
	instance := &ContainerInstance{Id: request.ContainerId, Request: request}
	worker := &Worker{backendRepoClient: repository, stateVolumeLeaseRenewInterval: 5 * time.Millisecond}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	require.Eventually(t, func() bool {
		renew, _ := repository.counts()
		return renew >= 2
	}, time.Second, 5*time.Millisecond)
	_, releasesBefore := repository.counts()
	require.Zero(t, releasesBefore)
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
	_, releasesAfter := repository.counts()
	require.Equal(t, 1, releasesAfter)
	repository.mu.Lock()
	require.Equal(t, request.Mounts[0].DurableDisk.VolumeId, repository.releaseRequests[0].Leases[0].VolumeId)
	require.Equal(t, request.Mounts[0].DurableDisk.AttachmentToken, repository.releaseRequests[0].Leases[0].AttachmentToken)
	require.Equal(t, request.Mounts[0].DurableDisk.FencingToken, repository.releaseRequests[0].Leases[0].FencingToken)
	repository.mu.Unlock()
}

func TestStateVolumeAttachmentTransientRenewalFailureRetriesWithoutFencing(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{
		renewFailures: []error{nil, errors.New("temporary repository transport outage")},
	}
	request := stateVolumeLeaseTestRequest(false)
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances,
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	require.Eventually(t, func() bool {
		renewals, _ := repository.counts()
		return renewals >= 3
	}, time.Second, 5*time.Millisecond)
	select {
	case signal := <-runtime.killed:
		t.Fatalf("transient renewal failure incorrectly fenced runtime with %s", signal)
	default:
	}
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestSnapshotUsesAuthoritativeRenewedLeaseAfterSchedulerDeadlineExpires(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	request := stateVolumeLeaseTestRequest(false)
	instance := &ContainerInstance{Id: request.ContainerId, Request: request}
	worker := &Worker{backendRepoClient: repository, stateVolumeLeaseRenewInterval: 5 * time.Millisecond}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	require.Eventually(t, func() bool {
		renew, _ := repository.counts()
		return renew >= 3
	}, time.Second, 5*time.Millisecond)
	// The scheduler-authored timestamp is intentionally immutable/stale. The
	// successful repository renewals above are the live authorization source.
	request.Mounts[0].DurableDisk.LeaseExpiresAtUnix = time.Now().Add(-time.Minute).Unix()
	leases, err := stateVolumeWriterLeasesForSnapshot(request, instance, false)
	require.NoError(t, err)
	require.Len(t, leases, 1)
	require.Equal(t, request.Mounts[0].DurableDisk.AttachmentToken, leases[0].AttachmentToken)
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestRedeliveredExpiredRequestRenewsBeforeStateVolumeMount(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	request := stateVolumeLeaseTestRequest(false)
	request.Mounts[0].DurableDisk.LeaseExpiresAtUnix = time.Now().Add(-time.Minute).Unix()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request}
	worker := &Worker{backendRepoClient: repository, stateVolumeLeaseRenewInterval: time.Hour}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	require.Greater(t, request.Mounts[0].DurableDisk.LeaseExpiresAtUnix, time.Now().Unix())
	renew, _ := repository.counts()
	require.Equal(t, 1, renew, "repository must authorize the tuple before block-device mount")
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestZeroDispatchExpiryRequiresAuthoritativeRenewalBeforeMount(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	request := stateVolumeLeaseTestRequest(false)
	request.Mounts[0].DurableDisk.LeaseExpiresAtUnix = 0
	instance := &ContainerInstance{Id: request.ContainerId, Request: request}
	worker := &Worker{backendRepoClient: repository, stateVolumeLeaseRenewInterval: time.Hour}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	renew, _ := repository.counts()
	require.Equal(t, 1, renew)
	require.Greater(t, request.Mounts[0].DurableDisk.LeaseExpiresAtUnix, time.Now().Unix())
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestDetachedPendingSnapshotUsesExactExpiredEscrowAndRejectsChangedFence(t *testing.T) {
	request := stateVolumeLeaseTestRequest(false)
	leasing, err := stateVolumeWriterLeaseTuples(request, false)
	require.NoError(t, err)
	state := &stateVolumeAttachmentState{leases: leasing, expiresAt: time.Now().Add(-time.Minute)}
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, StateVolumeAttachments: state}

	_, err = stateVolumeWriterLeasesForSnapshot(request, instance, false)
	require.ErrorContains(t, err, "expired")
	escrowed, err := stateVolumeWriterLeasesForSnapshot(request, instance, true)
	require.NoError(t, err)
	require.Equal(t, leasing, escrowed)

	request.Mounts[0].DurableDisk.FencingToken++
	_, err = stateVolumeWriterLeasesForSnapshot(request, instance, true)
	require.ErrorContains(t, err, "token/fence changed")
}

type leaseFenceRuntime struct {
	mockRuntime
	killed chan syscall.Signal
}

func (r *leaseFenceRuntime) Kill(_ context.Context, _ string, signal syscall.Signal, _ *betaruntime.KillOpts) error {
	r.killed <- signal
	return nil
}

func TestStateVolumeAttachmentRenewalFailureHardFencesRuntime(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{
		renewResponses: []*pb.RenewStateVolumeAttachmentsResponse{nil, {Ok: false, ErrorMsg: "attachment superseded"}},
	}
	request := stateVolumeLeaseTestRequest(false)
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances,
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}

	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	select {
	case signal := <-runtime.killed:
		require.Equal(t, syscall.SIGKILL, signal)
	case <-time.After(time.Second):
		t.Fatal("runtime was not fenced after attachment renewal failed")
	}
	updated, ok := instances.Get(request.ContainerId)
	require.True(t, ok)
	require.ErrorContains(t, updated.Err, "attachment superseded")
	updated.stateMu.RLock()
	state := updated.StateVolumeAttachments
	updated.stateMu.RUnlock()
	require.NotNil(t, state)
	select {
	case <-state.done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("renewal fence deadlocked waiting for its own done channel")
	}
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestLeaseRejectionCancelsPreparationAndCompletesPathFreeReleaseObligation(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{
		renewResponses: []*pb.RenewStateVolumeAttachmentsResponse{nil, {Ok: false, ErrorMsg: "attachment superseded during restore"}},
	}
	request := stateVolumeLeaseTestRequest(false)
	request.Workspace.ExternalId = uuid.NewString()
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "instance", StorageNodeID: "node",
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
	}
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances, stateVolumeManager: manager,
		workerId: "worker", workerInstanceId: "instance", machineID: "node",
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	require.NotNil(t, state)
	select {
	case <-state.prepareCtx.Done():
		require.ErrorContains(t, context.Cause(state.prepareCtx), "attachment superseded")
	case <-time.After(time.Second):
		t.Fatal("lease rejection did not cancel blocked state preparation")
	}
	select {
	case <-state.cleanupDone:
		t.Fatal("fence cleanup completed before preparation reached a definitive handoff")
	default:
	}
	state.finishPreparation()
	require.NoError(t, state.waitCleanup(context.Background()))
	require.Eventually(t, func() bool {
		current, ok := instances.Get(request.ContainerId)
		if !ok {
			return false
		}
		current.stateMu.RLock()
		defer current.stateMu.RUnlock()
		return current.StateVolumeAttachments == nil && current.StateVolumes == nil
	}, time.Second, 5*time.Millisecond)
	repository.mu.Lock()
	require.Equal(t, []string{"begin", "release"}, repository.events)
	repository.mu.Unlock()
	journals, err := manager.Journals.List()
	require.NoError(t, err)
	require.Empty(t, journals, "completed path-free release obligation must retire its journal")
}

func TestLeaseFenceWaitsForSlowHandleHandoffThenQuarantinesExactlyOnce(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{
		renewResponses: []*pb.RenewStateVolumeAttachmentsResponse{nil, {Ok: false, ErrorMsg: "attachment superseded during mount"}},
	}
	request := stateVolumeLeaseTestRequest(false)
	request.Workspace.ExternalId = uuid.NewString()
	request.Mounts[0].DurableDisk.SourceGenerationId = ""
	request.Mounts[0].DurableDisk.Initialize = true
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "instance", StorageNodeID: "node",
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	handle, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: request.ContainerId, Volumes: []StateVolumeSpec{{
		ID: request.Mounts[0].DurableDisk.VolumeId, Name: "data", ContainerMountPath: "/data",
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024, Format: true,
		AttachmentToken: request.Mounts[0].DurableDisk.AttachmentToken, FencingToken: request.Mounts[0].DurableDisk.FencingToken,
	}}})
	require.NoError(t, err)
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances, stateVolumeManager: manager,
		workerId: "worker", workerInstanceId: "instance", machineID: "node",
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	select {
	case <-state.prepareCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("lease fence did not cancel the slow preparation")
	}
	select {
	case <-state.cleanupDone:
		t.Fatal("cleanup raced ahead of slow manager handle handoff")
	default:
	}
	// Model a mount implementation that ignored cancellation and returned only
	// after the lease fence. The lifecycle publishes this exact handle before it
	// closes preparationDone, so the fence owner cannot miss the live writer.
	instance.stateMu.Lock()
	instance.StateVolumes = handle
	instance.stateMu.Unlock()
	instances.Set(request.ContainerId, instance)
	state.finishPreparation()
	require.NoError(t, state.waitCleanup(context.Background()))
	current, ok := instances.Get(request.ContainerId)
	require.True(t, ok)
	current.stateMu.RLock()
	require.Nil(t, current.StateVolumes)
	require.Nil(t, current.StateVolumeAttachments)
	require.NoError(t, current.StateFinalCommitError)
	current.stateMu.RUnlock()
	_, _, exists := manager.ExistingGroup(request.ContainerId)
	require.False(t, exists)
	repository.mu.Lock()
	require.Equal(t, []string{"begin", "release"}, repository.events)
	repository.mu.Unlock()
}

func TestStateVolumeAttachmentCleanupCompletionIsSingleOwner(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		state := &stateVolumeAttachmentState{cleanupDone: make(chan struct{})}
		instance := &ContainerInstance{StateVolumeAttachments: state}
		start := make(chan struct{})
		finished := make(chan struct{}, 2)
		go func() {
			<-start
			clearStateVolumeAttachmentState(instance)
			finished <- struct{}{}
		}()
		go func() {
			<-start
			state.finishCleanup(errors.New("fence cleanup failure"))
			finished <- struct{}{}
		}()
		close(start)
		<-finished
		<-finished
		require.Eventually(t, func() bool {
			select {
			case <-state.cleanupDone:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond)
		instance.stateMu.RLock()
		require.Nil(t, instance.StateVolumeAttachments)
		instance.stateMu.RUnlock()
	}
}

func TestStateVolumeAttachmentFenceAndOrdinaryReleaseShareOneObligation(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{}
	request := stateVolumeLeaseTestRequest(false)
	request.Workspace.ExternalId = uuid.NewString()
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "instance", StorageNodeID: "node",
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
	}
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances, stateVolumeManager: manager,
		workerId: "worker", workerInstanceId: "instance", machineID: "node",
		stateVolumeLeaseRenewInterval: time.Hour,
	}
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	instance.stateMu.RLock()
	state := instance.StateVolumeAttachments
	instance.stateMu.RUnlock()
	require.NotNil(t, state)
	state.finishPreparation()

	start := make(chan struct{})
	releaseResult := make(chan error, 1)
	go func() {
		<-start
		releaseResult <- worker.releaseStateVolumeAttachments(context.Background(), request, instance)
	}()
	go func() {
		<-start
		worker.fenceStateVolumeContainer(request.ContainerId, errors.New("lease fence raced ordinary stop"))
	}()
	close(start)
	require.NoError(t, <-releaseResult)
	require.NoError(t, state.waitCleanup(context.Background()))

	current, ok := instances.Get(request.ContainerId)
	require.True(t, ok)
	current.stateMu.RLock()
	require.Nil(t, current.StateVolumeAttachments)
	require.NoError(t, current.StateFinalCommitError)
	current.stateMu.RUnlock()
	repository.mu.Lock()
	require.Equal(t, []string{"begin", "release"}, repository.events)
	repository.mu.Unlock()
}

func TestExpiredLeaseFenceQuarantinesWithoutPublishingNewGeneration(t *testing.T) {
	repository := &recordingStateVolumeAttachmentRepo{
		renewResponses: []*pb.RenewStateVolumeAttachmentsResponse{nil, {Ok: false, ErrorMsg: "lease expired"}},
	}
	request := stateVolumeLeaseTestRequest(false)
	request.Mounts[0].DurableDisk.SourceGenerationId = ""
	request.Mounts[0].DurableDisk.Initialize = true
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "worker-instance", StorageNodeID: "node",
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	handle, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: request.ContainerId, Volumes: []StateVolumeSpec{{
		ID: request.Mounts[0].DurableDisk.VolumeId, Name: "data", ContainerMountPath: "/data",
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024, Format: true,
		AttachmentToken: request.Mounts[0].DurableDisk.AttachmentToken, FencingToken: request.Mounts[0].DurableDisk.FencingToken,
	}}})
	require.NoError(t, err)
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime, StateVolumes: handle}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances, stateVolumeManager: manager,
		workerId: "worker", workerInstanceId: "worker-instance", machineID: "node",
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	select {
	case signal := <-runtime.killed:
		require.Equal(t, syscall.SIGKILL, signal)
	case <-time.After(time.Second):
		t.Fatal("expired writer was not fenced")
	}
	require.Eventually(t, func() bool {
		current, ok := instances.Get(request.ContainerId)
		if !ok {
			return false
		}
		current.stateMu.RLock()
		defer current.stateMu.RUnlock()
		return current.StateVolumes == nil && current.StateVolumeAttachments == nil
	}, time.Second, 5*time.Millisecond)
	_, releases := repository.counts()
	require.Equal(t, 1, releases)
	_, _, exists := manager.ExistingGroup(request.ContainerId)
	require.False(t, exists)
}

func TestStateVolumeAttachmentSustainedTransportOutageFencesAtAuthoritativeDeadline(t *testing.T) {
	failures := make([]error, 64)
	for i := 1; i < len(failures); i++ {
		failures[i] = errors.New("repository transport unavailable")
	}
	repository := &recordingStateVolumeAttachmentRepo{
		renewFailures: failures,
		renewResponses: []*pb.RenewStateVolumeAttachmentsResponse{{
			Ok: true, LeaseExpiresAt: timestamppb.New(time.Now().Add(90 * time.Millisecond)),
		}},
	}
	request := stateVolumeLeaseTestRequest(false)
	runtime := &leaseFenceRuntime{killed: make(chan syscall.Signal, 1)}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: runtime}
	instances.Set(request.ContainerId, instance)
	worker := &Worker{
		backendRepoClient: repository, containerInstances: instances,
		stateVolumeLeaseRenewInterval: 5 * time.Millisecond,
	}
	started := time.Now()
	require.NoError(t, worker.startStateVolumeAttachmentRenewal(context.Background(), request, instance))
	select {
	case signal := <-runtime.killed:
		require.Equal(t, syscall.SIGKILL, signal)
		require.GreaterOrEqual(t, time.Since(started), 70*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("sustained renewal outage did not fence at the authoritative deadline")
	}
	require.NoError(t, worker.releaseStateVolumeAttachments(context.Background(), request, instance))
}

func TestWorkerRejectsLiveMemorySnapshotAtRPCBoundary(t *testing.T) {
	worker := &Worker{}
	response, err := worker.snapshotContainerState(context.Background(), &pb.SnapshotContainerStateRequest{
		ContainerId: "container", OperationId: "operation", Mode: string(StateSnapshotModeLive), IncludeMemory: true,
	})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "cannot include memory")
}

func TestTerminalOperationReplayFailsClosedWithoutCacheFinalizationCapability(t *testing.T) {
	operationID := "operation"
	snapshot := &pb.StateSnapshot{
		ExternalId: uuid.NewString(), OperationId: operationID, SourceContainerId: "container",
		Mode: string(StateSnapshotModeTerminal), IncludeMemory: false, Visible: false,
		Status: string(types.StateSnapshotStatusAvailable), RestoreMode: stateRestoreModeCold,
	}
	worker := &Worker{backendRepoClient: &snapshotBindingRepo{operation: &pb.GetStateSnapshotResponse{Ok: true, Snapshot: snapshot}}}
	response, err := worker.snapshotContainerState(context.Background(), &pb.SnapshotContainerStateRequest{
		ContainerId: "container", OperationId: operationID, Mode: string(StateSnapshotModeTerminal),
	})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "cache reporter/reconciler is unavailable")
}

func TestStateSnapshotRetryRejectsChangedModeOrMemoryInput(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container", ImageId: "image",
		Workspace: types.Workspace{ExternalId: "workspace"}, Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
	}
	operationID := "operation"
	stateSnapshotID := deterministicStateSnapshotID("workspace", "container", operationID)
	worker := &Worker{backendRepoClient: &snapshotBindingRepo{snapshot: &pb.StateSnapshot{
		ExternalId: stateSnapshotID, OperationId: operationID, SourceContainerId: "container",
		Mode: string(StateSnapshotModeTerminal), IncludeMemory: true,
	}}}
	_, _, err := worker.getOrCreatePendingStateSnapshot(context.Background(), request, operationID,
		StateSnapshotModeLive, false, false, "digest", "runc", nil, nil, nil)
	require.ErrorContains(t, err, "changed immutable mode/include_memory")
}

func TestTerminalMemorySnapshotBeginsAsColdPendingIntent(t *testing.T) {
	repository := &snapshotBindingRepo{}
	request := &types.ContainerRequest{
		ContainerId: "container", ImageId: "image",
		Workspace: types.Workspace{ExternalId: "workspace"}, Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
	}
	snapshot, proof, err := (&Worker{backendRepoClient: repository}).getOrCreatePendingStateSnapshot(
		context.Background(), request, "operation", StateSnapshotModeTerminal, true, false,
		"digest", "runc", []*pb.StateGeneration{{VolumeId: uuid.NewString(), GenerationId: uuid.NewString(), Generation: 1}}, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", proof)
	require.NotNil(t, repository.createRequest)
	require.True(t, snapshot.IncludeMemory)
	require.Equal(t, string(StateSnapshotModeTerminal), snapshot.Mode)
	require.Equal(t, stateRestoreModeCold, snapshot.RestoreMode)
}

func TestStateSnapshotCreateProofIsFsyncedInRecoveryJournalBeforeArm(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	manager := &StateVolumeManager{
		WorkerID: "worker", WorkerInstanceID: "worker-instance", StorageNodeID: "node",
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: uuid.NewString(), Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true, AttachmentToken: uuid.NewString(), FencingToken: 1,
	}}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Stop(context.Background(), "container") })

	storageID := uint(1)
	storageExternalID, bucket, region := "storage", "bucket", "us-east-1"
	request := &types.ContainerRequest{
		ContainerId: "container", ImageId: "image",
		Workspace: types.Workspace{
			ExternalId: "workspace", Name: "workspace-name",
			Storage: &types.WorkspaceStorage{Id: &storageID, ExternalId: &storageExternalID, BucketName: &bucket, Region: &region},
		},
		Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Name: "stub-name", Type: types.StubType(types.StubTypePod)}},
	}
	repository := &snapshotBindingRepo{}
	worker := &Worker{workerId: "worker", workerInstanceId: "worker-instance", machineID: "node", backendRepoClient: repository}
	snapshot, proof, err := worker.getOrCreatePendingStateSnapshot(
		context.Background(), request, "operation", StateSnapshotModeTerminal, false, false,
		"sha256:image", "runc", nil, nil, nil,
	)
	require.NoError(t, err)
	recovery, err := stateVolumeRecoveryEnvelopeFromRequest(request, "operation", StateSnapshotModeTerminal, false, false, "sha256:image", "runc")
	require.NoError(t, err)
	recovery.StateSnapshotID = snapshot.ExternalId
	recovery.RecoveryProofToken = proof
	require.NoError(t, manager.BindSnapshotRecovery("container", recovery))
	journal, err := manager.Journals.Load("container")
	require.NoError(t, err)
	require.NotNil(t, journal.Recovery)
	require.Equal(t, proof, journal.Recovery.RecoveryProofToken)
	journalPath, err := manager.Journals.journalPath("container")
	require.NoError(t, err)
	info, err := os.Stat(journalPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())

	_, err = worker.armStateSnapshot(context.Background(), snapshot, "container", "operation", proof)
	require.NoError(t, err)
	require.NotNil(t, repository.armRequest)
	require.Equal(t, proof, repository.armRequest.RecoveryProofToken)
}

func TestStateRestoreDestinationLineageSameBranchAndFork(t *testing.T) {
	now := time.Now().Add(2 * time.Minute).Unix()
	sourceVolumeID := uuid.NewString()
	sourceGenerationID := uuid.NewString()
	member := &pb.StateGeneration{
		VolumeId: sourceVolumeID, GenerationId: sourceGenerationID, Generation: 7,
		Name: "root", MountPath: "/", Root: true,
	}
	manifest := testBlockManifest(sourceVolumeID, sourceGenerationID, uuid.NewString(), 7)
	manifest.Generation = member.Generation

	sameRequest := &types.ContainerRequest{
		PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
		RootState: &types.RootStateMountConfig{
			VolumeId: sourceVolumeID, Size: "1Gi", SourceGenerationId: sourceGenerationID,
			AttachmentToken: uuid.NewString(), FencingToken: 10, LeaseExpiresAtUnix: now,
		},
	}
	sameDestination, err := resolveStateVolumeRestoreDestination(sameRequest, member, nil)
	require.NoError(t, err)
	require.Equal(t, sourceVolumeID, sameDestination.volumeID)
	require.False(t, sameDestination.cloneSource)
	sameLineage, err := resolveStateVolumeRestoredLineage(member, manifest, sameDestination)
	require.NoError(t, err)
	require.EqualValues(t, 7, sameLineage.generation)
	require.Equal(t, sourceGenerationID, sameLineage.parentGenerationID)
	require.Empty(t, sameLineage.cloneParentGenerationID)

	forkVolumeID := uuid.NewString()
	forkRequest := &types.ContainerRequest{
		StateFork: true, PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
		RootState: &types.RootStateMountConfig{
			VolumeId: forkVolumeID, Size: "1Gi", SourceGenerationId: sourceGenerationID, CloneSource: true,
			AttachmentToken: uuid.NewString(), FencingToken: 11, LeaseExpiresAtUnix: now,
		},
	}
	forkDestination, err := resolveStateVolumeRestoreDestination(forkRequest, member, nil)
	require.NoError(t, err)
	require.Equal(t, forkVolumeID, forkDestination.volumeID)
	require.True(t, forkDestination.cloneSource)
	forkLineage, err := resolveStateVolumeRestoredLineage(member, manifest, forkDestination)
	require.NoError(t, err)
	require.Zero(t, forkLineage.generation)
	require.Empty(t, forkLineage.parentGenerationID)
	require.Equal(t, sourceGenerationID, forkLineage.cloneParentGenerationID)
	require.Equal(t, manifest.Depth+1, forkLineage.depth)

	manager := &StateVolumeManager{groups: map[string]*stateVolumeGroup{
		"fork": {containerID: "fork", volumes: []*stateVolumeRuntime{{spec: StateVolumeSpec{
			ID: forkVolumeID, Name: "root", Root: true, ContainerMountPath: "/",
			Generation: forkLineage.generation, ActiveLayerPath: "/graph/active.qcow2", ActiveBackingPath: "/cache/source.qcow2",
			CloneParentGenerationID: forkLineage.cloneParentGenerationID, Depth: forkLineage.depth, SizeBytes: 1 << 30,
		}}}},
	}}
	plan, err := manager.PlanSnapshot(context.Background(), "fork", "operation")
	require.NoError(t, err)
	require.Len(t, plan.Generations, 1)
	require.EqualValues(t, 1, plan.Generations[0].Generation)
	require.Equal(t, forkVolumeID, plan.Generations[0].VolumeID)
	require.Equal(t, sourceGenerationID, plan.Generations[0].CloneParentGenerationID)
	require.Empty(t, plan.Generations[0].ParentGenerationID)

	forkRequest.StateFork = false
	_, err = resolveStateVolumeRestoreDestination(forkRequest, member, nil)
	require.ErrorContains(t, err, "clone intent")
}

func TestTerminalMemorySnapshotForkForcesTruthfulColdRestore(t *testing.T) {
	snapshot := &pb.StateSnapshot{
		RestoreMode: stateRestoreModeMemory, Mode: string(StateSnapshotModeTerminal), SourceStubExternalId: "source-stub",
	}
	sameSource := &types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "source-stub"}}}
	require.Empty(t, forcedColdStateRestoreReason(sameSource, snapshot))

	fork := &types.ContainerRequest{
		StateFork: true, Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "fork-stub"}},
	}
	reason := forcedColdStateRestoreReason(fork, snapshot)
	require.Contains(t, reason, "fork/template")
}

func TestPublishStateRestoreReceiptUsesAuthenticatedMountedMembershipAndOwner(t *testing.T) {
	volumeID, generationID, parentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	snapshotID := uuid.NewString()
	expected := &pb.StateGeneration{
		VolumeId: volumeID, GenerationId: generationID, Generation: 4,
		ParentGenerationId: parentID, Name: "root", MountPath: "/", Root: true,
	}
	request := &types.ContainerRequest{
		ContainerId: "container", StateSnapshotId: snapshotID,
		Workspace: types.Workspace{ExternalId: "workspace"}, DeliveryToken: "assignment:7",
		StateVolumePlanId: "plan-7", StateVolumePlanHash: strings.Repeat("7", 64),
	}
	handle := &StateVolumeGroupHandle{
		ContainerID: "container", SourceStateSnapshotID: snapshotID,
		SourceGenerations: []StateVolumeSourceGeneration{{
			VolumeID: volumeID, GenerationID: generationID, Generation: 4,
			ParentGenerationID: parentID, Name: "root", MountPath: "/", Root: true, Depth: 4,
		}},
	}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container", &ContainerInstance{Id: "container", Request: request, StateVolumes: handle})
	containerRepository := &fakeContainerRepoClient{}
	worker := &Worker{
		workerId: "worker-a", workerInstanceId: "worker-instance-a", machineID: "node-a", containerInstances: instances,
		containerRepoClient: containerRepository,
		backendRepoClient: &snapshotBindingRepo{snapshot: &pb.StateSnapshot{
			ExternalId: snapshotID, Status: string(types.StateSnapshotStatusAvailable), Generations: []*pb.StateGeneration{expected},
		}},
	}
	require.NoError(t, worker.publishStateRestoreReceipt(context.Background(), request, stateRestoreModeCold, "cold fallback"))
	containerRepository.mu.Lock()
	captured := containerRepository.lastStateRestoreReceipt
	containerRepository.mu.Unlock()
	require.NotNil(t, captured)
	require.Equal(t, "worker-a", captured.WorkerId)
	require.Equal(t, "worker-instance-a", captured.WorkerInstanceId)
	require.Equal(t, "node-a", captured.StorageNodeId)
	require.Equal(t, request.DeliveryToken, captured.DeliveryToken)
	require.Equal(t, request.StateVolumePlanId, captured.StateVolumePlanId)
	require.Equal(t, request.StateVolumePlanHash, captured.StateVolumePlanHash)
	require.Len(t, captured.Receipt.Generations, 1)
	require.Equal(t, generationID, captured.Receipt.Generations[0].GenerationId)

	// A same worker process must not be able to publish after Redis has moved
	// the container to a different delivery epoch/plan. The worker forwards the
	// caller-delivered tuple; the repository is authoritative for rejection.
	worker.containerRepoClient = &assignmentBoundRestoreReceiptRepo{
		deliveryToken: request.DeliveryToken, planID: request.StateVolumePlanId, planHash: request.StateVolumePlanHash,
	}
	request.DeliveryToken = "assignment:stale"
	require.ErrorContains(t, worker.publishStateRestoreReceipt(context.Background(), request, stateRestoreModeCold, "cold fallback"), "stale delivered")
	request.DeliveryToken = "assignment:7"

	// A forged/wrong local graph identity must fail before repository receipt
	// persistence; echoing repository membership would have hidden this.
	handle.SourceGenerations[0].GenerationID = uuid.NewString()
	require.ErrorContains(t, worker.publishStateRestoreReceipt(context.Background(), request, stateRestoreModeCold, "cold fallback"), "differs from repository")
}

func TestRuntimeExitPublishesInvisibleTerminalStateOnlySnapshot(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container", Workspace: types.Workspace{ExternalId: "workspace"}}
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, StateVolumes: &StateVolumeGroupHandle{ContainerID: request.ContainerId}}
	var captured *pb.SnapshotContainerStateRequest
	worker := &Worker{finalStateSnapshot: func(_ context.Context, in *pb.SnapshotContainerStateRequest) (*pb.SnapshotContainerStateResponse, error) {
		captured = in
		return &pb.SnapshotContainerStateResponse{Ok: true, Status: string(types.StateSnapshotStatusAvailable)}, nil
	}}

	require.NoError(t, worker.publishFinalContainerState(context.Background(), request, instance))
	require.NotNil(t, captured)
	require.Equal(t, string(StateSnapshotModeTerminal), captured.Mode)
	require.False(t, captured.IncludeMemory)
	require.False(t, captured.Visible)
	require.False(t, captured.Publish)
	require.Equal(t, finalStateSnapshotOperationID("workspace", "container"), captured.OperationId)
	require.Equal(t, captured.OperationId, instance.StateFinalCommitOperationID)

	worker.finalStateSnapshot = func(_ context.Context, _ *pb.SnapshotContainerStateRequest) (*pb.SnapshotContainerStateResponse, error) {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "storage unavailable"}, nil
	}
	require.ErrorContains(t, worker.publishFinalContainerState(context.Background(), request, instance), "storage unavailable")
}

func TestRestoreS0SnapshotS1WorkerAdoptionPlansS2ForRootAndDisk(t *testing.T) {
	rootVolumeID, dataVolumeID := uuid.NewString(), uuid.NewString()
	rootS0, rootS1 := uuid.NewString(), uuid.NewString()
	dataS0, dataS1 := uuid.NewString(), uuid.NewString()
	rootToken, dataToken := uuid.NewString(), uuid.NewString()
	expiry := time.Now().Add(2 * time.Minute).Unix()
	request := &types.ContainerRequest{
		ContainerId: "container", PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
		RootState: &types.RootStateMountConfig{
			VolumeId: rootVolumeID, Size: "1Gi", SourceGenerationId: rootS0,
			AttachmentToken: rootToken, FencingToken: 1, LeaseExpiresAtUnix: expiry,
		},
		Mounts: []types.Mount{{
			MountPath: "/data", DurableDisk: &types.DurableDiskMountConfig{
				Name: "data", Size: "1Gi", VolumeId: dataVolumeID, SourceGenerationId: dataS0,
				AttachmentToken: dataToken, FencingToken: 2, LeaseExpiresAtUnix: expiry,
			},
		}},
	}
	specs := []StateVolumeSpec{
		{
			ID: rootVolumeID, Name: "root", Root: true, ContainerMountPath: "/", MountPath: "/host/root",
			Generation: 2, CurrentGenerationID: rootS1, ParentGenerationID: rootS1, LineageSourceGenerationID: rootS0,
			AttachmentToken: rootToken, FencingToken: 1, ActiveLayerPath: "/graph/root-active", ActiveBackingPath: "/graph/root-s1", Depth: 3, SizeBytes: 1 << 30,
		},
		{
			ID: dataVolumeID, Name: "data", ContainerMountPath: "/data", MountPath: "/host/data",
			Generation: 5, CurrentGenerationID: dataS1, ParentGenerationID: dataS1, LineageSourceGenerationID: dataS0,
			AttachmentToken: dataToken, FencingToken: 2, ActiveLayerPath: "/graph/data-active", ActiveBackingPath: "/graph/data-s1", Depth: 6, SizeBytes: 1 << 30,
		},
	}
	require.NoError(t, bindReconciledStateVolumeGroup(request, specs))
	require.Equal(t, "/host/data", request.Mounts[0].LocalPath)

	group := &stateVolumeGroup{containerID: request.ContainerId}
	for i := range specs {
		group.volumes = append(group.volumes, &stateVolumeRuntime{spec: specs[i]})
	}
	manager := &StateVolumeManager{groups: map[string]*stateVolumeGroup{request.ContainerId: group}}
	plan, err := manager.PlanSnapshot(context.Background(), request.ContainerId, "snapshot-s2")
	require.NoError(t, err)
	require.Len(t, plan.Generations, 2)
	byVolume := map[string]StateVolumePivotGeneration{}
	for _, generation := range plan.Generations {
		byVolume[generation.VolumeID] = generation
	}
	require.EqualValues(t, 3, byVolume[rootVolumeID].Generation)
	require.Equal(t, rootS1, byVolume[rootVolumeID].ParentGenerationID)
	require.EqualValues(t, 6, byVolume[dataVolumeID].Generation)
	require.Equal(t, dataS1, byVolume[dataVolumeID].ParentGenerationID)
}

func TestTerminalSnapshotHoldBlocksTeardownUntilPublicationOwnerFinishes(t *testing.T) {
	instance := &ContainerInstance{}
	hold, err := instance.beginTerminalStateSnapshot("operation", StateSnapshotModeTerminal, true)
	require.NoError(t, err)
	hold.markRuntimeStopped()
	done := make(chan struct{})
	go func() {
		instance.waitForTerminalStateSnapshot()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("spawn teardown passed terminal snapshot hold before publication finished")
	case <-time.After(30 * time.Millisecond):
	}
	instance.finishTerminalStateSnapshot(hold)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("spawn teardown did not resume after terminal publication finished")
	}
}

func TestRestoreContainerStateBindsSnapshotAndOperationImmutably(t *testing.T) {
	instances := common.NewSafeMap[*ContainerInstance]()
	request := &types.ContainerRequest{ContainerId: "container", StateSnapshotId: "snapshot-a"}
	instance := &ContainerInstance{
		Id: "container", Request: request,
		StateVolumes: &StateVolumeGroupHandle{ContainerID: "container", SourceStateSnapshotID: "snapshot-a"},
	}
	instances.Set("container", instance)
	worker := &Worker{containerInstances: instances}

	call := func(operationID, snapshotID string) *pb.RestoreContainerStateResponse {
		response, err := worker.restoreContainerState(context.Background(), &pb.RestoreContainerStateRequest{
			ContainerId: "container", OperationId: operationID, StateSnapshotId: snapshotID,
		})
		require.NoError(t, err)
		return response
	}
	require.True(t, call("operation-a", "snapshot-a").Ok)
	require.True(t, call("operation-a", "snapshot-a").Ok, "exact replay must be idempotent")
	require.Contains(t, call("operation-a", "snapshot-b").ErrorMsg, "different immutable")
	require.Contains(t, call("operation-b", "snapshot-a").ErrorMsg, "different immutable")
}
