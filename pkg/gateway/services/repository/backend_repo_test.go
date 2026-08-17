package repository_services

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

const testStateSnapshotRecoveryProofToken = "f4821650-87ad-49d7-8866-04e68f61e6f1"

type stateRecoveryBackendForTest struct {
	repository.BackendRepository
	snapshot         *types.StateSnapshot
	claimed          string
	claimedInstance  string
	workspace        *types.Workspace
	plan             []types.StateGeneration
	renewedBy        string
	releasedBy       string
	releaseClaim     *types.StateVolumeReleaseClaim
	releaseCompleted bool
}

func (r *stateRecoveryBackendForTest) GetStateSnapshotByOperation(context.Context, string, string) (*types.StateSnapshot, error) {
	copy := *r.snapshot
	return &copy, nil
}

func (r *stateRecoveryBackendForTest) GetWorkspace(context.Context, uint) (*types.Workspace, error) {
	if r.workspace != nil {
		copy := *r.workspace
		return &copy, nil
	}
	return &types.Workspace{Id: 9, ExternalId: "workspace-1"}, nil
}

func (r *stateRecoveryBackendForTest) GetWorkspaceByExternalId(_ context.Context, externalID string) (types.Workspace, error) {
	workspace, err := r.GetWorkspace(context.Background(), 0)
	if err != nil {
		return types.Workspace{}, err
	}
	if workspace.ExternalId != externalID {
		return types.Workspace{}, errors.New("workspace not found")
	}
	return *workspace, nil
}

func (r *stateRecoveryBackendForTest) GetStateSnapshotPlan(context.Context, uint, string) ([]types.StateGeneration, error) {
	return append([]types.StateGeneration(nil), r.plan...), nil
}

func (r *stateRecoveryBackendForTest) RenewStateVolumeAttachments(_ context.Context, _ uint, _, workerID, workerInstanceID, storageNodeID string, _ []types.StateVolumeLease) (time.Time, error) {
	r.renewedBy = workerID + ":" + workerInstanceID + ":" + storageNodeID
	return time.Now().Add(time.Minute), nil
}

func (r *stateRecoveryBackendForTest) ReleaseStateVolumeAttachments(_ context.Context, _ uint, _, workerID, workerInstanceID, storageNodeID string, _ []types.StateVolumeLease) error {
	r.releasedBy = workerID + ":" + workerInstanceID + ":" + storageNodeID
	return nil
}

func (r *stateRecoveryBackendForTest) BeginStateVolumeReleaseIntent(_ context.Context, workspaceID uint,
	containerID, sourceWorkerID, sourceWorkerInstanceID, storageNodeID, journalDigest string,
	members []types.StateVolumeReleaseMember,
) (*types.StateVolumeReleaseClaim, error) {
	if r.releaseClaim == nil {
		r.releaseClaim = &types.StateVolumeReleaseClaim{ExternalId: "9bdcbd90-294f-4943-ae6f-a5dc7d325b1b",
			WorkspaceId: workspaceID, ContainerId: containerID, SourceWorkerId: sourceWorkerID,
			SourceWorkerInstanceId: sourceWorkerInstanceID, StorageNodeId: storageNodeID,
			RecoveryWorkerId: sourceWorkerID, RecoveryWorkerInstanceId: sourceWorkerInstanceID,
			JournalDigest: journalDigest, Phase: "source", Members: append([]types.StateVolumeReleaseMember(nil), members...)}
	}
	copy := *r.releaseClaim
	return &copy, nil
}

func (r *stateRecoveryBackendForTest) GetStateVolumeReleaseClaim(_ context.Context, workspaceID uint,
	containerID string,
) (*types.StateVolumeReleaseClaim, error) {
	if r.releaseClaim == nil || r.releaseClaim.WorkspaceId != workspaceID || r.releaseClaim.ContainerId != containerID {
		return nil, sql.ErrNoRows
	}
	copy := *r.releaseClaim
	return &copy, nil
}

func (r *stateRecoveryBackendForTest) ClaimStateVolumeRelease(_ context.Context, _ uint, _, _, _, _,
	recoveryWorkerID, recoveryWorkerInstanceID, _ string, previousClaimGeneration int64,
	_ []types.StateVolumeReleaseMember,
) (*types.StateVolumeReleaseClaim, error) {
	if r.releaseClaim == nil {
		return nil, errors.New("release intent not found")
	}
	if r.releaseClaim.Completed {
		copy := *r.releaseClaim
		return &copy, nil
	}
	if previousClaimGeneration != r.releaseClaim.ClaimGeneration {
		return nil, errors.New("release claim superseded")
	}
	r.releaseClaim.RecoveryWorkerId = recoveryWorkerID
	r.releaseClaim.RecoveryWorkerInstanceId = recoveryWorkerInstanceID
	r.releaseClaim.ClaimGeneration++
	r.releaseClaim.Phase = "claimed"
	copy := *r.releaseClaim
	return &copy, nil
}

func (r *stateRecoveryBackendForTest) CompleteClaimedStateVolumeRelease(_ context.Context, _ uint,
	_, _, _, _, _ string, _ int64,
) error {
	r.releaseCompleted = true
	if r.releaseClaim != nil {
		r.releaseClaim.Completed = true
		r.releaseClaim.Phase = "completed"
	}
	return nil
}

func (r *stateRecoveryBackendForTest) ClaimStateSnapshotRecovery(_ context.Context, _, _, _, workerID, workerInstanceID, _, recoveryProofToken string, previousClaimGeneration int64) (*types.StateSnapshot, error) {
	if recoveryProofToken != r.snapshot.RecoveryProofToken {
		return nil, errors.New("state snapshot recovery proof is invalid")
	}
	if r.snapshot.RecoveryWorkerId == workerID && r.snapshot.RecoveryWorkerInstanceId == workerInstanceID &&
		(r.snapshot.RecoveryClaimGeneration == previousClaimGeneration || r.snapshot.RecoveryClaimGeneration == previousClaimGeneration+1) {
		copy := *r.snapshot
		return &copy, nil
	}
	if r.snapshot.RecoveryClaimGeneration != previousClaimGeneration {
		return nil, errors.New("recovery claim was superseded")
	}
	r.claimed = workerID
	r.claimedInstance = workerInstanceID
	r.snapshot.RecoveryWorkerId = workerID
	r.snapshot.RecoveryWorkerInstanceId = workerInstanceID
	r.snapshot.RecoveryClaimGeneration++
	copy := *r.snapshot
	return &copy, nil
}

type stateRecoveryWorkerRepoForTest struct {
	repository.WorkerRepository
	workers          map[string]*types.Worker
	errors           map[string]error
	capacityUpdateBy string
	workerUpdateBy   string
	statusUpdateBy   string
}

func (r *stateRecoveryWorkerRepoForTest) GetWorkerById(workerID string) (*types.Worker, error) {
	if err := r.errors[workerID]; err != nil {
		return nil, err
	}
	worker := r.workers[workerID]
	if worker == nil {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	return worker, nil
}

func (r *stateRecoveryWorkerRepoForTest) SetWorkerStateVolumeCapacity(workerID, machineID string, total, free uint32) error {
	r.capacityUpdateBy = fmt.Sprintf("%s:%s:%d:%d", workerID, machineID, total, free)
	return nil
}

func (r *stateRecoveryWorkerRepoForTest) SetWorkerStateVolumeCapacityForProcess(workerID, workerInstanceID,
	machineID string, total, free uint32,
) error {
	r.capacityUpdateBy = fmt.Sprintf("%s:%s:%s:%d:%d", workerID, workerInstanceID, machineID, total, free)
	return nil
}

func (r *stateRecoveryWorkerRepoForTest) UpdateWorkerCapacityForProcess(worker *types.Worker, workerInstanceID,
	storageNodeID string, _ *types.ContainerRequest, update types.CapacityUpdateType,
) error {
	r.workerUpdateBy = fmt.Sprintf("%s:%s:%s:%d", worker.Id, workerInstanceID, storageNodeID, update)
	return nil
}

func (r *stateRecoveryWorkerRepoForTest) UpdateWorkerStatusForProcess(workerID, workerInstanceID,
	storageNodeID string, status types.WorkerStatus,
) error {
	r.statusUpdateBy = fmt.Sprintf("%s:%s:%s:%s", workerID, workerInstanceID, storageNodeID, status)
	return nil
}

type stateRecoveryContainerRepoForTest struct {
	repository.ContainerRepository
	state    *types.ContainerState
	err      error
	nilState bool
}

func (r *stateRecoveryContainerRepoForTest) GetContainerState(string) (*types.ContainerState, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.nilState {
		return nil, nil
	}
	if r.state == nil {
		return nil, &types.ErrContainerStateNotFound{ContainerId: "container"}
	}
	return r.state, nil
}

func TestStateSnapshotProtoRoundTripKeepsExactGenerationMembership(t *testing.T) {
	snapshot := &types.StateSnapshot{
		ExternalId:        "state-1",
		OperationId:       "op-1",
		SourceContainerId: "container-1",
		Status:            types.StateSnapshotStatusAvailable,
		ImageDigest:       "sha256:image",
		RuntimeProfile:    "runc-v1",
		CheckpointId:      "memory-1",
		RestoreMode:       "memory",
		Public:            true,
		Generations: []types.StateGeneration{{
			VolumeId: "root", GenerationId: "generation-1", Name: "root",
			ParentGenerationId: "generation-0", MountPath: "/", Root: true, Generation: 7,
		}},
	}

	roundTrip := stateSnapshotFromProto(stateSnapshotToProto(snapshot))
	require.Equal(t, snapshot.ExternalId, roundTrip.ExternalId)
	require.Equal(t, snapshot.OperationId, roundTrip.OperationId)
	require.Equal(t, snapshot.RestoreMode, roundTrip.RestoreMode)
	require.Equal(t, snapshot.Generations, roundTrip.Generations)
}

func TestVolumeGenerationProtoRoundTripUsesBlockMetadataOnly(t *testing.T) {
	generation := &types.VolumeGeneration{
		ExternalId: "generation-1", VolumeId: "root", Name: "root", Generation: 2,
		ParentGenerationId: "generation-0", Status: types.StateSnapshotStatusAvailable,
		ManifestKey: "state/chunks/manifest", ManifestDigest: "sha256:manifest",
		ChunkCount: 3, LogicalSizeBytes: 64 << 20, StoredSizeBytes: 12 << 20,
	}

	require.Equal(t, generation, volumeGenerationFromProto(volumeGenerationToProto(generation)))
}

func TestClaimStateSnapshotRecoveryRequiresDeadSourceAndExactNode(t *testing.T) {
	backend := &stateRecoveryBackendForTest{snapshot: &types.StateSnapshot{
		ExternalId: "e4f41f9a-524c-4906-8ea3-b36b32f45c27", OperationId: "operation",
		WorkspaceId: 9, SourceContainerId: "container", SourceWorkerId: "worker-old",
		SourceWorkerInstanceId: "old-instance",
		StorageNodeId:          "node-1", RecoveryProofToken: testStateSnapshotRecoveryProofToken,
		Armed: true, Mode: "terminal", Status: types.StateSnapshotStatusPending,
	}}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker-new": {Id: "worker-new", MachineId: "node-1", InstanceId: "new-instance", WorkerTokenId: "worker-token", Status: types.WorkerStatusAvailable},
	}}
	containers := &stateRecoveryContainerRepoForTest{state: &types.ContainerState{
		ContainerId: "container", WorkspaceId: "workspace-1", WorkerId: "worker-new", MachineId: "node-1",
	}}
	service := NewBackendRepositoryService(context.Background(), backend, containers, workers, nil)
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: types.TokenTypeWorker, ExternalId: "worker-token"},
	})
	request := &pb.ClaimStateSnapshotRecoveryRequest{
		StateSnapshotId: backend.snapshot.ExternalId, SourceContainerId: "container", OperationId: "operation",
		WorkerId: "worker-new", WorkerInstanceId: "new-instance", StorageNodeId: "node-1",
		RecoveryProofToken: testStateSnapshotRecoveryProofToken,
	}
	missingProof := *request
	missingProof.RecoveryProofToken = ""
	response, err := service.ClaimStateSnapshotRecovery(ctx, &missingProof)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "recovery proof")

	response, err = service.ClaimStateSnapshotRecovery(ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, "worker-new", response.Snapshot.RecoveryWorkerId)
	require.EqualValues(t, 1, response.Snapshot.RecoveryClaimGeneration)

	wrongNode := *request
	wrongNode.StorageNodeId = "node-2"
	response, err = service.ClaimStateSnapshotRecovery(ctx, &wrongNode)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "another storage node")

	workers.workers["worker-old"] = &types.Worker{Id: "worker-old", MachineId: "node-1", InstanceId: "old-instance", Status: types.WorkerStatusAvailable}
	backend.claimed = ""
	backend.snapshot.RecoveryWorkerId, backend.snapshot.RecoveryWorkerInstanceId = "", ""
	backend.snapshot.RecoveryClaimGeneration = 0
	response, err = service.ClaimStateSnapshotRecovery(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "still authoritative")

	delete(workers.workers, "worker-old")
	workers.errors = map[string]error{"worker-old": errors.New("worker repository unavailable")}
	response, err = service.ClaimStateSnapshotRecovery(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "worker repository unavailable")

	delete(workers.errors, "worker-old")
	containers.err = errors.New("container repository unavailable")
	response, err = service.ClaimStateSnapshotRecovery(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "container repository unavailable")
}

func TestStateSnapshotRecoveryCredentialsRequireExactClaimTokenAndProcessEpoch(t *testing.T) {
	storageID := uint(17)
	storageExternalID, bucket, access, secret := "storage-17", "bucket", "access", "secret"
	backend := &stateRecoveryBackendForTest{
		snapshot: &types.StateSnapshot{
			ExternalId: "e4f41f9a-524c-4906-8ea3-b36b32f45c27", OperationId: "operation",
			WorkspaceId: 9, SourceContainerId: "container", SourceWorkerId: "worker-old",
			SourceWorkerInstanceId: "old-instance", RecoveryWorkerId: "worker-new",
			RecoveryWorkerInstanceId: "new-instance", StorageNodeId: "node-1", Armed: true,
			RecoveryProofToken:      testStateSnapshotRecoveryProofToken,
			RecoveryClaimGeneration: 1,
			Mode:                    "terminal", Status: types.StateSnapshotStatusPending,
			SourceStubExternalId: "stub", SourceStubName: "machine", SourceStubType: "pod",
			ImageId: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc-v1",
		},
		workspace: &types.Workspace{Id: 9, ExternalId: "workspace-1", Name: "tenant", Storage: &types.WorkspaceStorage{
			Id: &storageID, ExternalId: &storageExternalID, BucketName: &bucket, AccessKey: &access, SecretKey: &secret,
		}},
		plan: []types.StateGeneration{{VolumeId: "volume", GenerationId: "generation", Name: "root", MountPath: "/", Root: true}},
	}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker-new": {Id: "worker-new", MachineId: "node-1", InstanceId: "new-instance", WorkerTokenId: "worker-token"},
	}}
	containers := &stateRecoveryContainerRepoForTest{state: &types.ContainerState{
		ContainerId: "container", WorkspaceId: "workspace-1", WorkerId: "worker-new", MachineId: "node-1",
	}}
	service := NewBackendRepositoryService(context.Background(), backend, containers, workers, nil)
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "worker-token",
	}})
	request := &pb.GetStateSnapshotRecoveryCredentialsRequest{
		StateSnapshotId: backend.snapshot.ExternalId, SourceContainerId: "container", OperationId: "operation",
		WorkerId: "worker-new", WorkerInstanceId: "new-instance", StorageNodeId: "node-1",
		RecoveryClaimGeneration: 1, RecoveryProofToken: testStateSnapshotRecoveryProofToken,
	}
	wrongProof := *request
	wrongProof.RecoveryProofToken = "00000000-0000-4000-8000-000000000001"
	response, err := service.GetStateSnapshotRecoveryCredentials(ctx, &wrongProof)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "exact active recovery claim")

	response, err = service.GetStateSnapshotRecoveryCredentials(ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.Equal(t, "workspace-1", response.WorkspaceId)
	require.Equal(t, "tenant", response.WorkspaceName)
	require.Equal(t, "stub", response.StubId)
	require.Equal(t, "sha256:image", response.ImageDigest)
	require.Equal(t, "access", response.WorkspaceStorage.AccessKey)

	containers.err = errors.New("container repository unavailable")
	response, err = service.GetStateSnapshotRecoveryCredentials(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "container repository unavailable")
	containers.err, containers.nilState = nil, true
	response, err = service.GetStateSnapshotRecoveryCredentials(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "assignment is corrupt")
	containers.nilState = false

	wrongInstance := *request
	wrongInstance.WorkerInstanceId = "forged-instance"
	response, err = service.GetStateSnapshotRecoveryCredentials(ctx, &wrongInstance)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "exact active recovery claim")

	spoofedCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "sibling-worker-token",
	}})
	response, err = service.GetStateSnapshotRecoveryCredentials(spoofedCtx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "not bound")
}

func TestStateVolumeReleaseIntentRequiresSourceOwnershipAndDeadEpochHandoff(t *testing.T) {
	backend := &stateRecoveryBackendForTest{workspace: &types.Workspace{Id: 9, ExternalId: "workspace-1"}}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker-source": {Id: "worker-source", MachineId: "node-1", InstanceId: "epoch-source",
			WorkerTokenId: "token-source", Status: types.WorkerStatusAvailable},
		"worker-recovery-1": {Id: "worker-recovery-1", MachineId: "node-1", InstanceId: "epoch-recovery-1",
			WorkerTokenId: "token-recovery-1", Status: types.WorkerStatusAvailable},
		"worker-recovery-2": {Id: "worker-recovery-2", MachineId: "node-1", InstanceId: "epoch-recovery-2",
			WorkerTokenId: "token-recovery-2", Status: types.WorkerStatusAvailable},
	}}
	containers := &stateRecoveryContainerRepoForTest{state: &types.ContainerState{
		ContainerId: "container-release", WorkspaceId: "workspace-1", WorkerId: "worker-source", MachineId: "node-1",
	}}
	service := NewBackendRepositoryService(context.Background(), backend, containers, workers, nil)
	member := &pb.StateVolumeReleaseMember{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", FencingToken: 7}
	sourceCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-source",
	}})
	begin, err := service.BeginStateVolumeReleaseIntent(sourceCtx, &pb.BeginStateVolumeReleaseIntentRequest{
		WorkspaceId: "workspace-1", ContainerId: "container-release", SourceWorkerId: "worker-source",
		SourceWorkerInstanceId: "epoch-source", StorageNodeId: "node-1",
		JournalDigest: "sha256:" + strings.Repeat("a", 64), Members: []*pb.StateVolumeReleaseMember{member},
	})
	require.NoError(t, err)
	require.True(t, begin.Ok)
	require.Zero(t, begin.ReleaseClaimGeneration)

	containers.state = nil
	workers.workers["worker-source"].Status = types.WorkerStatusDisabled
	firstCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-recovery-1",
	}})
	claimRequest := &pb.ClaimStateVolumeReleaseRequest{
		WorkspaceId: "workspace-1", ContainerId: "container-release", SourceWorkerId: "worker-source",
		SourceWorkerInstanceId: "epoch-source", StorageNodeId: "node-1",
		RecoveryWorkerId: "worker-recovery-1", RecoveryWorkerInstanceId: "epoch-recovery-1",
		JournalDigest: "sha256:" + strings.Repeat("a", 64), Members: []*pb.StateVolumeReleaseMember{member},
	}
	claimed, err := service.ClaimStateVolumeRelease(firstCtx, claimRequest)
	require.NoError(t, err)
	require.True(t, claimed.Ok)
	require.EqualValues(t, 1, claimed.ReleaseClaimGeneration)

	secondCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-recovery-2",
	}})
	second := *claimRequest
	second.RecoveryWorkerId, second.RecoveryWorkerInstanceId = "worker-recovery-2", "epoch-recovery-2"
	second.PreviousClaimGeneration = 1
	blocked, err := service.ClaimStateVolumeRelease(secondCtx, &second)
	require.NoError(t, err)
	require.False(t, blocked.Ok)
	require.Contains(t, blocked.ErrorMsg, "previous recovery claimant is still authoritative")

	workers.workers["worker-recovery-1"].Status = types.WorkerStatusDisabled
	claimed, err = service.ClaimStateVolumeRelease(secondCtx, &second)
	require.NoError(t, err)
	require.True(t, claimed.Ok)
	require.EqualValues(t, 2, claimed.ReleaseClaimGeneration)
	completed, err := service.CompleteClaimedStateVolumeRelease(secondCtx, &pb.CompleteClaimedStateVolumeReleaseRequest{
		WorkspaceId: "workspace-1", ContainerId: "container-release", ReleaseClaimId: claimed.ReleaseClaimId,
		ReleaseClaimGeneration: 2, RecoveryWorkerId: "worker-recovery-2",
		RecoveryWorkerInstanceId: "epoch-recovery-2", StorageNodeId: "node-1",
	})
	require.NoError(t, err)
	require.True(t, completed.Ok)
	require.True(t, backend.releaseCompleted)
}

func TestStateVolumeReleaseClaimReturnsCompletedSourceIntentAfterCrash(t *testing.T) {
	backend := &stateRecoveryBackendForTest{
		workspace: &types.Workspace{Id: 9, ExternalId: "workspace-1"},
		releaseClaim: &types.StateVolumeReleaseClaim{
			ExternalId: "9bdcbd90-294f-4943-ae6f-a5dc7d325b1b", WorkspaceId: 9,
			ContainerId: "container-release", SourceWorkerId: "worker-source", SourceWorkerInstanceId: "epoch-source",
			StorageNodeId: "node-1", RecoveryWorkerId: "worker-source", RecoveryWorkerInstanceId: "epoch-source",
			JournalDigest: "sha256:" + strings.Repeat("b", 64), Phase: "completed", Completed: true,
			Members: []types.StateVolumeReleaseMember{{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", FencingToken: 7}},
		},
	}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker-source": {Id: "worker-source", MachineId: "node-1", InstanceId: "epoch-source",
			WorkerTokenId: "token-source", Status: types.WorkerStatusDisabled},
		"worker-recovery": {Id: "worker-recovery", MachineId: "node-1", InstanceId: "epoch-recovery",
			WorkerTokenId: "token-recovery", Status: types.WorkerStatusAvailable},
	}}
	service := NewBackendRepositoryService(context.Background(), backend,
		&stateRecoveryContainerRepoForTest{}, workers, nil)
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-recovery",
	}})
	response, err := service.ClaimStateVolumeRelease(ctx, &pb.ClaimStateVolumeReleaseRequest{
		WorkspaceId: "workspace-1", ContainerId: "container-release", SourceWorkerId: "worker-source",
		SourceWorkerInstanceId: "epoch-source", StorageNodeId: "node-1",
		RecoveryWorkerId: "worker-recovery", RecoveryWorkerInstanceId: "epoch-recovery",
		JournalDigest: "sha256:" + strings.Repeat("b", 64),
		Members:       []*pb.StateVolumeReleaseMember{{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", FencingToken: 7}},
	})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.True(t, response.Completed)
	require.Zero(t, response.ReleaseClaimGeneration)
}

func TestStateSnapshotRecoveryClaimHandsOffAcrossTwoDeadReplacementEpochs(t *testing.T) {
	backend := &stateRecoveryBackendForTest{snapshot: &types.StateSnapshot{
		ExternalId: "e4f41f9a-524c-4906-8ea3-b36b32f45c27", OperationId: "operation",
		WorkspaceId: 9, SourceContainerId: "container", SourceWorkerId: "worker-source",
		SourceWorkerInstanceId: "source-instance", RecoveryWorkerId: "worker-one",
		RecoveryWorkerInstanceId: "instance-one", RecoveryClaimGeneration: 1,
		StorageNodeId: "node-1", RecoveryProofToken: testStateSnapshotRecoveryProofToken,
		Armed: true, Mode: "terminal", Status: types.StateSnapshotStatusAvailable,
	}}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker-one":   {Id: "worker-one", MachineId: "node-1", InstanceId: "instance-one", Status: types.WorkerStatusDisabled},
		"worker-two":   {Id: "worker-two", MachineId: "node-1", InstanceId: "instance-two", WorkerTokenId: "token-two", Status: types.WorkerStatusAvailable},
		"worker-three": {Id: "worker-three", MachineId: "node-1", InstanceId: "instance-three", WorkerTokenId: "token-three", Status: types.WorkerStatusAvailable},
	}}
	service := NewBackendRepositoryService(context.Background(), backend, &stateRecoveryContainerRepoForTest{}, workers, nil)
	workerTwoCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-two",
	}})
	request := &pb.ClaimStateSnapshotRecoveryRequest{
		StateSnapshotId: backend.snapshot.ExternalId, SourceContainerId: "container", OperationId: "operation",
		WorkerId: "worker-two", WorkerInstanceId: "instance-two", StorageNodeId: "node-1", PreviousClaimGeneration: 1,
		RecoveryProofToken: testStateSnapshotRecoveryProofToken,
	}
	response, err := service.ClaimStateSnapshotRecovery(workerTwoCtx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.EqualValues(t, 2, response.Snapshot.RecoveryClaimGeneration)

	// A lost response is byte-identical/idempotent at the same claim epoch.
	response, err = service.ClaimStateSnapshotRecovery(workerTwoCtx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.EqualValues(t, 2, response.Snapshot.RecoveryClaimGeneration)

	workers.workers["worker-two"].Status = types.WorkerStatusDisabled
	workerThreeCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "token-three",
	}})
	request.WorkerId, request.WorkerInstanceId, request.PreviousClaimGeneration = "worker-three", "instance-three", 2
	response, err = service.ClaimStateSnapshotRecovery(workerThreeCtx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.EqualValues(t, 3, response.Snapshot.RecoveryClaimGeneration)

	stale := *request
	stale.PreviousClaimGeneration = 1
	response, err = service.ClaimStateSnapshotRecovery(workerThreeCtx, &stale)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "superseded")
}

func TestStateVolumeLeaseMutationRequiresExactAssignedWorkerProcess(t *testing.T) {
	backend := &stateRecoveryBackendForTest{workspace: &types.Workspace{Id: 9, ExternalId: "workspace-1"}}
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker": {Id: "worker", MachineId: "node-1", InstanceId: "instance-1", WorkerTokenId: "worker-token"},
	}}
	containers := &stateRecoveryContainerRepoForTest{state: &types.ContainerState{
		ContainerId: "container", WorkspaceId: "workspace-1", WorkerId: "worker", MachineId: "node-1",
	}}
	service := NewBackendRepositoryService(context.Background(), backend, containers, workers, nil)
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "worker-token",
	}})
	lease := &pb.StateVolumeLease{VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f",
		AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 8}
	renew := &pb.RenewStateVolumeAttachmentsRequest{WorkspaceId: "workspace-1", ContainerId: "container",
		WorkerId: "worker", WorkerInstanceId: "instance-1", StorageNodeId: "node-1", Leases: []*pb.StateVolumeLease{lease}}
	renewed, err := service.RenewStateVolumeAttachments(ctx, renew)
	require.NoError(t, err)
	require.True(t, renewed.Ok, renewed.ErrorMsg)
	require.Equal(t, "worker:instance-1:node-1", backend.renewedBy)

	stale := *renew
	stale.WorkerInstanceId = "stale-instance"
	renewed, err = service.RenewStateVolumeAttachments(ctx, &stale)
	require.NoError(t, err)
	require.False(t, renewed.Ok)
	require.Contains(t, renewed.ErrorMsg, "registered storage node")

	release := &pb.ReleaseStateVolumeAttachmentsRequest{WorkspaceId: renew.WorkspaceId, ContainerId: renew.ContainerId,
		WorkerId: renew.WorkerId, WorkerInstanceId: renew.WorkerInstanceId, StorageNodeId: renew.StorageNodeId,
		Leases: renew.Leases}
	released, err := service.ReleaseStateVolumeAttachments(ctx, release)
	require.NoError(t, err)
	require.True(t, released.Ok, released.ErrorMsg)
	require.Equal(t, "worker:instance-1:node-1", backend.releasedBy)

	spoofed := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "sibling-token",
	}})
	released, err = service.ReleaseStateVolumeAttachments(spoofed, release)
	require.NoError(t, err)
	require.False(t, released.Ok)
	require.Contains(t, released.ErrorMsg, "not bound")
}

func TestStateVolumeCapacityRequiresExactRegisteredWorkerProcess(t *testing.T) {
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker": {Id: "worker", MachineId: "node-1", InstanceId: "instance-1", WorkerTokenId: "worker-token"},
	}}
	service := NewWorkerRepositoryService(context.Background(), workers, nil, nil, nil, nil, nil, types.AppConfig{}, "")
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "worker-token",
	}})
	request := &pb.SetWorkerStateVolumeCapacityRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", MachineId: "node-1",
		TotalNbdDevices: 12, FreeNbdDevices: 11,
	}
	response, err := service.SetWorkerStateVolumeCapacity(ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.Equal(t, "worker:instance-1:node-1:12:11", workers.capacityUpdateBy)

	stale := *request
	stale.WorkerInstanceId = "stale-instance"
	response, err = service.SetWorkerStateVolumeCapacity(ctx, &stale)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "registered worker process")

	wrongNode := *request
	wrongNode.MachineId = "node-2"
	response, err = service.SetWorkerStateVolumeCapacity(ctx, &wrongNode)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "registered worker process")

	spoofed := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "sibling-token",
	}})
	response, err = service.SetWorkerStateVolumeCapacity(spoofed, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "not bound")
}

func TestWorkerDeliveryRequiresExactRegisteredTokenAndProcessEpoch(t *testing.T) {
	workers := &stateRecoveryWorkerRepoForTest{workers: map[string]*types.Worker{
		"worker": {Id: "worker", MachineId: "node-1", InstanceId: "instance-1", WorkerTokenId: "worker-token"},
	}}
	service := NewWorkerRepositoryService(context.Background(), workers, nil, nil, nil, nil, nil, types.AppConfig{}, "")
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "worker-token",
	}})
	require.NoError(t, service.authorizeWorkerDeliveryProcess(ctx, "worker", "instance-1", "node-1"))
	require.ErrorIs(t, service.authorizeWorkerDeliveryProcess(ctx, "worker", "stale-instance", "node-1"),
		errWorkerIdentityUnauthorized)
	require.ErrorIs(t, service.authorizeWorkerDeliveryProcess(ctx, "worker", "instance-1", "node-2"),
		errWorkerIdentityUnauthorized)

	sibling := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{
		TokenType: types.TokenTypeWorker, ExternalId: "sibling-token",
	}})
	require.ErrorIs(t, service.authorizeWorkerDeliveryProcess(sibling, "worker", "instance-1", "node-1"),
		errWorkerIdentityUnauthorized)
	staleAck, err := service.AddContainerToWorker(ctx, &pb.AddContainerToWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "stale-instance", StorageNodeId: "node-1", ContainerId: "container",
		DeliveryToken: "delivery", StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
	})
	require.NoError(t, err)
	require.False(t, staleAck.Ok)
	require.Contains(t, staleAck.ErrorMsg, "not bound")
	siblingAck, err := service.AddContainerToWorker(sibling, &pb.AddContainerToWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", StorageNodeId: "node-1", ContainerId: "container",
		DeliveryToken: "delivery", StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
	})
	require.NoError(t, err)
	require.False(t, siblingAck.Ok)
	require.Contains(t, siblingAck.ErrorMsg, "not bound")
	staleRemove, err := service.RemoveContainerFromWorker(ctx, &pb.RemoveContainerFromWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "stale-instance", StorageNodeId: "node-1", ContainerId: "container",
	})
	require.NoError(t, err)
	require.False(t, staleRemove.Ok)
	require.Contains(t, staleRemove.ErrorMsg, "not bound")

	staleDisable, err := service.DisableWorker(ctx, &pb.DisableWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "stale-instance", StorageNodeId: "node-1",
	})
	require.NoError(t, err)
	require.False(t, staleDisable.Ok)
	require.Contains(t, staleDisable.ErrorMsg, "not bound")
	siblingDisable, err := service.DisableWorker(sibling, &pb.DisableWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", StorageNodeId: "node-1",
	})
	require.NoError(t, err)
	require.False(t, siblingDisable.Ok)
	require.Contains(t, siblingDisable.ErrorMsg, "not bound")
	validDisable, err := service.DisableWorker(ctx, &pb.DisableWorkerRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", StorageNodeId: "node-1",
	})
	require.NoError(t, err)
	require.True(t, validDisable.Ok, validDisable.ErrorMsg)
	require.Equal(t, "worker:instance-1:node-1:disabled", workers.statusUpdateBy)

	capacityRequest := &pb.UpdateWorkerCapacityRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", StorageNodeId: "node-1",
		CapacityChange: int64(types.AddCapacity),
		ContainerRequest: (&types.ContainerRequest{
			ContainerId: "container",
		}).ToProto(),
	}
	staleCapacity := *capacityRequest
	staleCapacity.WorkerInstanceId = "stale-instance"
	capacityResponse, err := service.UpdateWorkerCapacity(ctx, &staleCapacity)
	require.NoError(t, err)
	require.False(t, capacityResponse.Ok)
	require.Contains(t, capacityResponse.ErrorMsg, "not bound")
	capacityResponse, err = service.UpdateWorkerCapacity(sibling, capacityRequest)
	require.NoError(t, err)
	require.False(t, capacityResponse.Ok)
	require.Contains(t, capacityResponse.ErrorMsg, "not bound")
	capacityResponse, err = service.UpdateWorkerCapacity(ctx, capacityRequest)
	require.NoError(t, err)
	require.True(t, capacityResponse.Ok, capacityResponse.ErrorMsg)
	require.Equal(t, fmt.Sprintf("worker:instance-1:node-1:%d", types.AddCapacity), workers.workerUpdateBy)

	type mutationResult struct {
		ok       bool
		errorMsg string
	}
	type mutationCase struct {
		name   string
		invoke func(context.Context, string) mutationResult
	}
	mutations := []mutationCase{
		{name: "set image pull lock", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.SetImagePullLock(callCtx, &pb.SetImagePullLockRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", ImageId: "image",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "remove image pull lock", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.RemoveImagePullLock(callCtx, &pb.RemoveImagePullLockRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", ImageId: "image", Token: "lock",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "set network lock", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.SetNetworkLock(callCtx, &pb.SetNetworkLockRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", NetworkPrefix: "network", Ttl: 10,
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "remove network lock", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.RemoveNetworkLock(callCtx, &pb.RemoveNetworkLockRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", NetworkPrefix: "network", Token: "lock",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "set container ip", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.SetContainerIp(callCtx, &pb.SetContainerIpRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", NetworkPrefix: "network", ContainerId: "container", IpAddress: "192.168.0.2",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "move container ip", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.MoveContainerIp(callCtx, &pb.MoveContainerIpRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", NetworkPrefix: "network", FromContainerId: "container", ToContainerId: "next", IpAddress: "192.168.0.2",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
		{name: "remove container ip", invoke: func(callCtx context.Context, instance string) mutationResult {
			response, _ := service.RemoveContainerIp(callCtx, &pb.RemoveContainerIpRequest{
				WorkerId: "worker", WorkerInstanceId: instance, StorageNodeId: "node-1", NetworkPrefix: "network", ContainerId: "container",
			})
			return mutationResult{response.Ok, response.ErrorMsg}
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name+" stale epoch", func(t *testing.T) {
			result := mutation.invoke(ctx, "stale-instance")
			require.False(t, result.ok)
			require.Contains(t, result.errorMsg, "not bound")
		})
		t.Run(mutation.name+" sibling token", func(t *testing.T) {
			result := mutation.invoke(sibling, "instance-1")
			require.False(t, result.ok)
			require.Contains(t, result.errorMsg, "not bound")
		})
	}

	keepAlive, err := service.SetWorkerKeepAlive(ctx, &pb.SetWorkerKeepAliveRequest{
		WorkerId: "worker", WorkerInstanceId: "instance-1", MachineId: "node-2",
	})
	require.NoError(t, err)
	require.False(t, keepAlive.Ok)
	require.Contains(t, keepAlive.ErrorMsg, "not bound")
}
