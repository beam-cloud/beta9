package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
)

const (
	stateRestoreModeMemory = "memory"
	stateRestoreModeCold   = "cold_state"
)

func deterministicStateSnapshotID(workspaceID, containerID, operationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("beta9-state-snapshot\x00"+workspaceID+"\x00"+containerID+"\x00"+operationID)).String()
}

func finalStateSnapshotOperationID(workspaceID, containerID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("beta9-final-container-state\x00"+workspaceID+"\x00"+containerID)).String()
}

func stateVolumeStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stateVolumeRecoveryEnvelopeFromRequest(request *types.ContainerRequest, operationID string, mode StateSnapshotMode, includeMemory, visible bool, imageDigest, runtimeProfile string) (StateVolumeRecoveryEnvelope, error) {
	if request == nil || request.Workspace.Storage == nil || request.Workspace.Storage.Id == nil {
		return StateVolumeRecoveryEnvelope{}, fmt.Errorf("state snapshot recovery requires workspace storage scope")
	}
	storage := request.Workspace.Storage
	envelope := StateVolumeRecoveryEnvelope{
		OperationID: operationID, WorkspaceID: request.Workspace.ExternalId, WorkspaceName: request.Workspace.Name,
		StubID: request.Stub.ExternalId, StubName: request.Stub.Name, StubType: string(request.Stub.Type),
		ImageID: request.ImageId, ImageDigest: imageDigest, RuntimeProfile: runtimeProfile,
		Mode: string(mode), IncludeMemory: includeMemory, Visible: visible,
		WorkspaceStorageID: *storage.Id, WorkspaceStorageExternalID: stateVolumeStringValue(storage.ExternalId),
		WorkspaceStorageBucket: stateVolumeStringValue(storage.BucketName), WorkspaceStorageEndpoint: stateVolumeStringValue(storage.EndpointUrl),
		WorkspaceStorageRegion: stateVolumeStringValue(storage.Region),
	}
	if envelope.OperationID == "" || envelope.WorkspaceID == "" || envelope.WorkspaceName == "" || envelope.StubID == "" ||
		envelope.ImageID == "" || envelope.ImageDigest == "" || envelope.RuntimeProfile == "" || envelope.WorkspaceStorageID == 0 ||
		envelope.WorkspaceStorageBucket == "" || envelope.WorkspaceStorageRegion == "" {
		return StateVolumeRecoveryEnvelope{}, fmt.Errorf("state snapshot recovery scope is incomplete")
	}
	return envelope, nil
}

func validateStateSnapshotOperationInputs(snapshot *pb.StateSnapshot, in *pb.SnapshotContainerStateRequest) error {
	if snapshot == nil || in == nil || snapshot.SourceContainerId != in.ContainerId || snapshot.OperationId != in.OperationId {
		return fmt.Errorf("state snapshot operation identity collision")
	}
	if snapshot.Mode != strings.ToLower(strings.TrimSpace(in.Mode)) || snapshot.IncludeMemory != in.IncludeMemory || snapshot.Visible != in.Visible {
		return fmt.Errorf("state snapshot retry changed immutable mode/include_memory/visible inputs")
	}
	return nil
}

func stateVolumePlannedMembersMatchReceipt(planned []*pb.StateGeneration, compactions []*pb.StateGenerationCompaction, receipt *StateVolumePivotReceipt) bool {
	if receipt == nil || len(planned) != len(receipt.Generations) {
		return false
	}
	byGeneration := make(map[string]StateVolumePivotGeneration, len(receipt.Generations))
	for _, generation := range receipt.Generations {
		if generation.GenerationID == "" {
			return false
		}
		byGeneration[generation.GenerationID] = generation
	}
	for _, member := range planned {
		if member == nil {
			return false
		}
		generation, ok := byGeneration[member.GenerationId]
		if !ok || generation.VolumeID != member.VolumeId || generation.Generation != member.Generation || generation.Name != member.Name ||
			generation.MountPath != member.MountPath || generation.ReadOnly != member.ReadOnly || generation.Root != member.Root ||
			generation.ParentGenerationID != member.ParentGenerationId || generation.CloneParentGenerationID != member.CloneParentGenerationId {
			return false
		}
	}
	plannedCompactions := make(map[string]*pb.StateGenerationCompaction, len(compactions))
	for _, compaction := range compactions {
		if compaction == nil || compaction.GenerationId == "" || compaction.VolumeId == "" || compaction.SourceGenerationId == "" {
			return false
		}
		if _, duplicate := plannedCompactions[compaction.GenerationId]; duplicate {
			return false
		}
		plannedCompactions[compaction.GenerationId] = compaction
	}
	for _, generation := range receipt.Generations {
		compaction := plannedCompactions[generation.GenerationID]
		if generation.Compaction {
			if compaction == nil || compaction.VolumeId != generation.VolumeID ||
				compaction.SourceGenerationId != generation.CompactionSourceGenerationID {
				return false
			}
			delete(plannedCompactions, generation.GenerationID)
		} else if compaction != nil {
			return false
		}
	}
	if len(plannedCompactions) != 0 {
		return false
	}
	return true
}

func stateVolumeStringPointer(value string) *string { return &value }
func stateVolumeUintPointer(value uint) *uint       { return &value }

func (s *Worker) recoveryRequestFromEnvelope(ctx context.Context, containerID string, envelope StateVolumeRecoveryEnvelope) (*types.ContainerRequest, error) {
	if s.backendRepoClient == nil || envelope.StateSnapshotID == "" || envelope.OperationID == "" {
		return nil, fmt.Errorf("claim-bound backend repository is unavailable for state recovery credentials")
	}
	request := &types.ContainerRequest{
		ContainerId: containerID, WorkspaceId: envelope.WorkspaceID, StubId: envelope.StubID,
		ImageId: envelope.ImageID, StateImageDigest: envelope.ImageDigest, StateRuntimeProfile: envelope.RuntimeProfile,
		Workspace: types.Workspace{
			ExternalId: envelope.WorkspaceID, Name: envelope.WorkspaceName,
			Storage: &types.WorkspaceStorage{
				Id: stateVolumeUintPointer(envelope.WorkspaceStorageID), ExternalId: stateVolumeStringPointer(envelope.WorkspaceStorageExternalID),
				BucketName: stateVolumeStringPointer(envelope.WorkspaceStorageBucket), EndpointUrl: stateVolumeStringPointer(envelope.WorkspaceStorageEndpoint),
				Region: stateVolumeStringPointer(envelope.WorkspaceStorageRegion),
			},
		},
		Stub: types.StubWithRelated{Stub: types.Stub{
			ExternalId: envelope.StubID, Name: envelope.StubName, Type: types.StubType(envelope.StubType),
		}},
	}
	credentials, err := s.backendRepoClient.GetStateSnapshotRecoveryCredentials(ctx, &pb.GetStateSnapshotRecoveryCredentialsRequest{
		StateSnapshotId: envelope.StateSnapshotID, SourceContainerId: containerID, OperationId: envelope.OperationID,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		RecoveryClaimGeneration: envelope.RecoveryClaimGeneration,
		RecoveryProofToken:      envelope.RecoveryProofToken,
	})
	if err != nil {
		return nil, err
	}
	if credentials == nil || !credentials.Ok || credentials.WorkspaceStorage == nil {
		message := "workspace storage credential rehydration failed"
		if credentials != nil && credentials.ErrorMsg != "" {
			message = credentials.ErrorMsg
		}
		return nil, fmt.Errorf("%s", message)
	}
	request.Workspace.ExternalId, request.Workspace.Name = credentials.WorkspaceId, credentials.WorkspaceName
	request.Stub.Stub.ExternalId, request.Stub.Stub.Name = credentials.StubId, credentials.StubName
	request.Stub.Stub.Type = types.StubType(credentials.StubType)
	request.ImageId, request.StateImageDigest, request.StateRuntimeProfile = credentials.ImageId, credentials.ImageDigest, credentials.RuntimeProfile
	storage := credentials.WorkspaceStorage
	request.Workspace.Storage = &types.WorkspaceStorage{
		Id: stateVolumeUintPointer(uint(credentials.WorkspaceStorageId)), ExternalId: stateVolumeStringPointer(credentials.WorkspaceStorageExternalId),
		BucketName: stateVolumeStringPointer(storage.BucketName), EndpointUrl: stateVolumeStringPointer(storage.EndpointUrl),
		Region: stateVolumeStringPointer(storage.Region), AccessKey: stateVolumeStringPointer(storage.AccessKey), SecretKey: stateVolumeStringPointer(storage.SecretKey),
	}
	if request.Workspace.Storage.AccessKey == nil || request.Workspace.Storage.SecretKey == nil {
		return nil, fmt.Errorf("workspace storage credential rehydration returned no access credentials")
	}
	return request, nil
}

func (s *Worker) newStateVolumeCAS(ctx context.Context, request *types.ContainerRequest) (BlockV1CAS, string, error) {
	if s.stateVolumeCASFactory != nil {
		return s.stateVolumeCASFactory(ctx, request)
	}
	if s == nil || s.cacheManager == nil || s.cacheManager.client == nil {
		return nil, "", fmt.Errorf("state volume content cache is unavailable")
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, "", err
	}
	return s.workspaceBlockV1CAS(storageClient), storageClient.BucketName(), nil
}

func (s *Worker) requireStateVolumeCacheCapability() error {
	if s == nil || s.cacheManager == nil || s.cacheManager.ContentReporter() == nil {
		return fmt.Errorf("state volume cache reporter/reconciler is unavailable")
	}
	reporter := s.cacheManager.ContentReporter()
	if reporter.eventRepo == nil || !reporter.eventRepo.HasDurableScopedStateSink() {
		return fmt.Errorf("state volume durable required-content writer is unavailable")
	}
	if s.stateVolumeCASFactory == nil && s.cacheManager.client == nil {
		return fmt.Errorf("state volume content cache is unavailable")
	}
	return nil
}

func (s *Worker) workspaceBlockV1CAS(storageClient *clients.WorkspaceStorageClient) *workspaceBlockV1CAS {
	cas := &workspaceBlockV1CAS{client: storageClient}
	if s != nil && s.cacheManager != nil {
		cas.cache = s.cacheManager.client
	}
	return cas
}

func (s *Worker) stateSnapshotReportingRequest(ctx context.Context, containerID, operationID string) (*types.ContainerRequest, error) {
	if err := s.requireStateVolumeCacheCapability(); err != nil {
		return nil, err
	}
	if s.containerInstances != nil {
		if instance, ok := s.containerInstances.Get(containerID); ok && instance != nil && instance.Request != nil {
			return instance.Request, nil
		}
	}
	if s.stateVolumeManager == nil {
		return nil, fmt.Errorf("state snapshot cache report recovery journal is unavailable")
	}
	envelope, err := s.stateVolumeManager.SnapshotRecovery(containerID, operationID)
	if err != nil {
		return nil, err
	}
	return s.recoveryRequestFromEnvelope(ctx, containerID, envelope)
}

func (s *Worker) reportAvailableStateSnapshotBeforeAck(ctx context.Context, containerID, operationID string, snapshot *pb.StateSnapshot) error {
	request, err := s.stateSnapshotReportingRequest(ctx, containerID, operationID)
	if err != nil || request == nil {
		return err
	}
	return s.reportCommittedStateSnapshotContent(ctx, request, snapshot)
}

func (s *Worker) publishFinalContainerState(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) error {
	if request == nil || instance == nil || instance.StateVolumes == nil {
		return nil
	}
	operationID := finalStateSnapshotOperationID(request.Workspace.ExternalId, request.ContainerId)
	instance.stateMu.Lock()
	instance.StateFinalCommitOperationID = operationID
	instance.stateMu.Unlock()
	in := &pb.SnapshotContainerStateRequest{
		ContainerId: request.ContainerId, OperationId: operationID,
		Mode: string(StateSnapshotModeTerminal), IncludeMemory: false, Publish: false, Visible: false,
	}
	var response *pb.SnapshotContainerStateResponse
	var err error
	if s.finalStateSnapshot != nil {
		response, err = s.finalStateSnapshot(ctx, in)
	} else {
		response, err = s.snapshotContainerStateWithRuntimeStopped(ctx, in, true)
	}
	if err != nil {
		return err
	}
	if response == nil || !response.Ok || response.Status != string(types.StateSnapshotStatusAvailable) {
		message := "final container state commit failed"
		if response != nil && strings.TrimSpace(response.ErrorMsg) != "" {
			message = response.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	return nil
}

// finalizeRecoveredTerminalState completes the lifecycle tail that the normal
// spawn path intentionally deferred while an immutable terminal publication
// was pending. Replacement workers normally have no ContainerInstance and
// only retire the adopted detached group. A same-worker replay must also stop
// renewal, release the exact attachment tuple idempotently, close any teardown
// hold, publish the final exit, and remove the retained remote/local state.
func (s *Worker) finalizeRecoveredTerminalState(ctx context.Context, containerID string) error {
	if s.containerInstances == nil {
		if err := s.stateVolumeManager.Stop(ctx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			return err
		}
		return nil
	}
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		if err := s.stateVolumeManager.Stop(ctx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			return err
		}
		return nil
	}
	request := instance.Request
	if request == nil {
		return fmt.Errorf("retained terminal container %q has no request", containerID)
	}
	if instance.StateVolumes != nil {
		if err := s.stopAndReleaseStateVolumes(ctx, request, instance); err != nil {
			return err
		}
	} else if err := s.stateVolumeManager.Stop(ctx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
		return err
	}
	instance.stateMu.Lock()
	instance.StateFinalCommitError = nil
	hold := instance.terminalStateSnapshot
	instance.stateMu.Unlock()
	instance.finishTerminalStateSnapshot(hold)
	exitCode, _ := instance.lifecycleState()
	if exitCode < 0 {
		exitCode = int(types.ContainerExitCodeUnknownError)
		instance.setExitCode(exitCode)
	}
	if !s.markContainerStopping(containerID, types.ContainerStateTtlS) {
		return fmt.Errorf("mark recovered terminal container %q stopping", containerID)
	}
	if s.containerRepoClient != nil {
		if !s.setContainerExitCode(containerID, exitCode) {
			return fmt.Errorf("publish recovered terminal exit for container %q", containerID)
		}
		if err := s.deleteContainerState(containerID); err != nil {
			return fmt.Errorf("delete recovered terminal container state %q: %w", containerID, err)
		}
	}
	instance.stopOOMWatcher()
	if instance.SandboxProcessManager != nil {
		if err := instance.SandboxProcessManager.Cleanup(); err != nil {
			return fmt.Errorf("cleanup recovered sandbox process manager: %w", err)
		}
	}
	s.containerInstances.Delete(containerID)
	if s.completedRequests != nil {
		select {
		case s.completedRequests <- request:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Worker) snapshotImageDigest(request *types.ContainerRequest) (string, error) {
	if request == nil || request.ImageId == "" {
		return "", fmt.Errorf("container image identity is unavailable")
	}
	if request.StateImageDigest != "" {
		return request.StateImageDigest, nil
	}
	if s.imageClient != nil {
		if metadata, ok := s.imageClient.GetCLIPImageMetadata(request.ImageId); ok && metadata != nil && strings.TrimSpace(metadata.Digest) != "" {
			return metadata.Digest, nil
		}
	}
	return "", fmt.Errorf("immutable digest for image %q is unavailable", request.ImageId)
}

func stateSnapshotResponse(snapshot *pb.StateSnapshot) *pb.SnapshotContainerStateResponse {
	if snapshot == nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "state snapshot is unavailable"}
	}
	return &pb.SnapshotContainerStateResponse{
		Ok: snapshot.Status != string(types.StateSnapshotStatusFailed), ErrorMsg: snapshot.Reason,
		StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status,
		ImageDigest: snapshot.ImageDigest, RuntimeProfile: snapshot.RuntimeProfile,
		CheckpointId: snapshot.CheckpointId, HasMemory: snapshot.CheckpointId != "",
		Generations: snapshot.Generations, RestoreMode: snapshot.RestoreMode,
		FallbackReason: snapshot.FallbackReason,
	}
}

func (s *Worker) getOrCreatePendingStateSnapshot(ctx context.Context, request *types.ContainerRequest, operationID string, snapshotMode StateSnapshotMode, includeMemory, visible bool, imageDigest, runtimeProfile string, members []*pb.StateGeneration, compactions []*pb.StateGenerationCompaction, leases []*pb.StateVolumeLease) (*pb.StateSnapshot, string, error) {
	stateSnapshotID := deterministicStateSnapshotID(request.Workspace.ExternalId, request.ContainerId, operationID)
	get, err := s.backendRepoClient.GetStateSnapshot(ctx, &pb.GetStateSnapshotRequest{
		WorkspaceId: request.Workspace.ExternalId, StateSnapshotId: stateSnapshotID,
	})
	if err == nil && get != nil && get.Ok && get.Snapshot != nil {
		if get.Snapshot.OperationId != operationID || get.Snapshot.SourceContainerId != request.ContainerId {
			return nil, "", fmt.Errorf("state snapshot operation identity collision")
		}
		if get.Snapshot.Mode != string(snapshotMode) || get.Snapshot.IncludeMemory != includeMemory || get.Snapshot.Visible != visible {
			return nil, "", fmt.Errorf("state snapshot retry changed immutable mode/include_memory/visible inputs")
		}
	}
	pending := &pb.StateSnapshot{
		ExternalId: stateSnapshotID, OperationId: operationID, SourceContainerId: request.ContainerId,
		Status: string(types.StateSnapshotStatusPending), ImageId: request.ImageId, ImageDigest: imageDigest,
		// Pending rows bind the requested memory intent through IncludeMemory,
		// but do not claim that checkpoint bytes exist. Commit replaces this
		// with the truthful memory/cold outcome after all durable members and
		// optional checkpoint metadata are authenticated.
		RuntimeProfile: runtimeProfile, RestoreMode: stateRestoreModeCold,
		Mode: string(snapshotMode), IncludeMemory: includeMemory, Visible: visible,
		SourceStubExternalId: request.Stub.ExternalId, SourceStubName: request.Stub.Name,
		SourceStubType: string(request.Stub.Type),
	}
	created, err := s.backendRepoClient.CreateStateSnapshot(ctx, &pb.CreateStateSnapshotRequest{
		WorkspaceId: request.Workspace.ExternalId, StubId: request.Stub.ExternalId, Snapshot: pending,
		Members: members, Leases: leases, WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		Compactions: compactions,
	})
	if err != nil {
		return nil, "", err
	}
	if created == nil || !created.Ok || created.Snapshot == nil {
		message := "create state snapshot failed"
		if created != nil && created.ErrorMsg != "" {
			message = created.ErrorMsg
		}
		return nil, "", fmt.Errorf("%s", message)
	}
	if parsed, err := uuid.Parse(created.RecoveryProofToken); err != nil || parsed.String() != strings.ToLower(created.RecoveryProofToken) {
		return nil, "", fmt.Errorf("create state snapshot returned no canonical recovery proof token")
	}
	return created.Snapshot, created.RecoveryProofToken, nil
}

func (s *Worker) armStateSnapshot(ctx context.Context, snapshot *pb.StateSnapshot, containerID, operationID, recoveryProofToken string) (*pb.StateSnapshot, error) {
	if snapshot == nil || snapshot.ExternalId == "" || containerID == "" || operationID == "" || s.workerId == "" || s.machineID == "" || recoveryProofToken == "" {
		return nil, fmt.Errorf("state snapshot arm identity is incomplete")
	}
	response, err := s.backendRepoClient.ArmStateSnapshot(ctx, &pb.ArmStateSnapshotRequest{
		StateSnapshotId: snapshot.ExternalId, SourceContainerId: containerID, OperationId: operationID,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		RecoveryProofToken: recoveryProofToken,
	})
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Ok || response.Snapshot == nil {
		message := "arm state snapshot failed"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return nil, fmt.Errorf("%s", message)
	}
	return response.Snapshot, nil
}

func (s *Worker) claimStateSnapshotRecovery(ctx context.Context, snapshot *pb.StateSnapshot, containerID, operationID, recoveryProofToken string) (*pb.StateSnapshot, error) {
	if snapshot == nil || snapshot.ExternalId == "" || containerID == "" || operationID == "" || s.workerId == "" || s.machineID == "" || recoveryProofToken == "" {
		return nil, fmt.Errorf("state snapshot recovery claim identity is incomplete")
	}
	response, err := s.backendRepoClient.ClaimStateSnapshotRecovery(ctx, &pb.ClaimStateSnapshotRecoveryRequest{
		StateSnapshotId: snapshot.ExternalId, SourceContainerId: containerID, OperationId: operationID,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		PreviousClaimGeneration: snapshot.RecoveryClaimGeneration,
		RecoveryProofToken:      recoveryProofToken,
	})
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Ok || response.Snapshot == nil {
		message := "claim state snapshot recovery failed"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return nil, fmt.Errorf("%s", message)
	}
	return response.Snapshot, nil
}

func (s *Worker) failStateSnapshot(ctx context.Context, snapshot *pb.StateSnapshot, containerID, operationID string, cause error) error {
	if snapshot == nil || snapshot.ExternalId == "" || cause == nil {
		return cause
	}
	if err := s.markStateSnapshotFailed(ctx, snapshot, containerID, operationID, cause.Error()); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (s *Worker) markStateSnapshotFailed(ctx context.Context, snapshot *pb.StateSnapshot, containerID, operationID, reason string) error {
	if snapshot == nil || snapshot.ExternalId == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("state snapshot failure identity is incomplete")
	}
	response, err := s.backendRepoClient.FailStateSnapshot(ctx, &pb.FailStateSnapshotRequest{
		StateSnapshotId: snapshot.ExternalId, SourceContainerId: containerID, OperationId: operationID,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID, Reason: reason,
		RecoveryClaimGeneration: snapshot.RecoveryClaimGeneration,
	})
	if err != nil {
		return err
	}
	if response == nil || !response.Ok {
		message := "fail state snapshot operation failed"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	return nil
}

func (s *Worker) failDeterminateStateSnapshot(ctx context.Context, snapshot *pb.StateSnapshot, containerID, operationID string, cause error) error {
	if cause == nil || s.stateVolumeManager == nil {
		return cause
	}
	if err := s.stateVolumeManager.CancelSnapshotRecovery(containerID, operationID); err != nil {
		// Pending/indeterminate/resume-required state remains owned by its durable
		// recovery journal and must not be terminalized as failed.
		return cause
	}
	return s.failStateSnapshot(ctx, snapshot, containerID, operationID, cause)
}

func (s *Worker) publishStateRestoreReceipt(ctx context.Context, request *types.ContainerRequest, restoreMode, fallbackReason string) error {
	if request == nil || request.StateSnapshotId == "" {
		return nil
	}
	if s.backendRepoClient == nil || s.containerRepoClient == nil || request.Workspace.ExternalId == "" {
		return fmt.Errorf("state restore receipt services are unavailable")
	}
	if strings.TrimSpace(request.DeliveryToken) == "" || strings.TrimSpace(request.StateVolumePlanId) == "" || strings.TrimSpace(request.StateVolumePlanHash) == "" {
		return fmt.Errorf("state restore receipt requires the exact delivered state-volume assignment tuple")
	}
	response, err := s.backendRepoClient.GetStateSnapshot(ctx, &pb.GetStateSnapshotRequest{
		WorkspaceId: request.Workspace.ExternalId, StateSnapshotId: request.StateSnapshotId,
	})
	if err != nil {
		return err
	}
	if response == nil || !response.Ok || response.Snapshot == nil ||
		response.Snapshot.ExternalId != request.StateSnapshotId ||
		response.Snapshot.Status != string(types.StateSnapshotStatusAvailable) {
		return fmt.Errorf("state snapshot %q is unavailable while recording restore outcome", request.StateSnapshotId)
	}
	if restoreMode != stateRestoreModeMemory && restoreMode != stateRestoreModeCold {
		return fmt.Errorf("invalid final state restore mode %q", restoreMode)
	}
	if restoreMode == stateRestoreModeMemory {
		fallbackReason = ""
	} else if fallbackReason == "" {
		fallbackReason = response.Snapshot.FallbackReason
	}
	instance, ok := s.containerInstances.Get(request.ContainerId)
	if !ok || instance == nil || instance.StateVolumes == nil {
		return fmt.Errorf("container %q has no mounted state-volume group for restore receipt", request.ContainerId)
	}
	handle := instance.StateVolumes
	if handle.SourceStateSnapshotID != request.StateSnapshotId || len(handle.SourceGenerations) == 0 {
		return fmt.Errorf("mounted state-volume group is not authenticated to snapshot %q", request.StateSnapshotId)
	}
	authenticated := make([]*pb.StateGeneration, 0, len(handle.SourceGenerations))
	for _, generation := range handle.SourceGenerations {
		if generation.VolumeID == "" || generation.GenerationID == "" || generation.Generation <= 0 ||
			generation.Name == "" || generation.MountPath == "" {
			return fmt.Errorf("mounted state-volume group has incomplete authenticated source membership")
		}
		authenticated = append(authenticated, &pb.StateGeneration{
			VolumeId: generation.VolumeID, GenerationId: generation.GenerationID,
			Generation: generation.Generation, Name: generation.Name, MountPath: generation.MountPath,
			ReadOnly: generation.ReadOnly, Root: generation.Root,
			ParentGenerationId:      generation.ParentGenerationID,
			CloneParentGenerationId: generation.CloneParentGenerationID,
		})
	}
	sort.Slice(authenticated, func(i, j int) bool { return authenticated[i].VolumeId < authenticated[j].VolumeId })
	if err := compareAuthenticatedRestoreMembers(authenticated, response.Snapshot.Generations); err != nil {
		return fmt.Errorf("mounted state-volume group does not match snapshot %q: %w", request.StateSnapshotId, err)
	}
	receiptResponse, err := s.containerRepoClient.SetStateRestoreReceipt(ctx, &pb.SetStateRestoreReceiptRequest{
		ContainerId:         request.ContainerId,
		WorkerId:            s.workerId,
		WorkerInstanceId:    s.workerInstanceId,
		StorageNodeId:       s.machineID,
		DeliveryToken:       request.DeliveryToken,
		StateVolumePlanId:   request.StateVolumePlanId,
		StateVolumePlanHash: request.StateVolumePlanHash,
		Receipt: &pb.StateRestoreReceipt{
			StateSnapshotId: request.StateSnapshotId, RestoreMode: restoreMode,
			FallbackReason: fallbackReason, Generations: authenticated,
		},
	})
	if err != nil {
		return err
	}
	if receiptResponse == nil || !receiptResponse.Ok {
		message := "persist final state restore receipt failed"
		if receiptResponse != nil && receiptResponse.ErrorMsg != "" {
			message = receiptResponse.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	return nil
}

func compareAuthenticatedRestoreMembers(authenticated, expected []*pb.StateGeneration) error {
	if len(authenticated) != len(expected) {
		return fmt.Errorf("member count %d does not match %d", len(authenticated), len(expected))
	}
	byVolume := make(map[string]*pb.StateGeneration, len(expected))
	for _, member := range expected {
		if member == nil || member.VolumeId == "" {
			return fmt.Errorf("repository membership is incomplete")
		}
		if _, duplicate := byVolume[member.VolumeId]; duplicate {
			return fmt.Errorf("repository membership duplicates volume %q", member.VolumeId)
		}
		byVolume[member.VolumeId] = member
	}
	for _, actual := range authenticated {
		want := byVolume[actual.VolumeId]
		if want == nil || actual.GenerationId != want.GenerationId || actual.Generation != want.Generation ||
			actual.ParentGenerationId != want.ParentGenerationId || actual.CloneParentGenerationId != want.CloneParentGenerationId ||
			actual.Name != want.Name || actual.MountPath != want.MountPath || actual.ReadOnly != want.ReadOnly || actual.Root != want.Root {
			return fmt.Errorf("authenticated member for volume %q differs from repository membership", actual.VolumeId)
		}
	}
	return nil
}

func (s *Worker) snapshotContainerState(ctx context.Context, in *pb.SnapshotContainerStateRequest) (*pb.SnapshotContainerStateResponse, error) {
	return s.snapshotContainerStateWithRuntimeStopped(ctx, in, false)
}

func (s *Worker) snapshotContainerStateWithRuntimeStopped(ctx context.Context, in *pb.SnapshotContainerStateRequest, runtimeAlreadyStopped bool) (response *pb.SnapshotContainerStateResponse, responseErr error) {
	if in == nil || strings.TrimSpace(in.ContainerId) == "" || strings.TrimSpace(in.OperationId) == "" {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "container_id and operation_id are required"}, nil
	}
	if in.Publish {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "public state publication is unavailable"}, nil
	}
	mode := StateSnapshotMode(strings.ToLower(strings.TrimSpace(in.Mode)))
	if mode != StateSnapshotModeLive && mode != StateSnapshotModeTerminal {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "mode must be live or terminal"}, nil
	}
	if mode == StateSnapshotModeLive && in.IncludeMemory {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "live state snapshots cannot include memory"}, nil
	}
	if err := s.requireStateVolumeCacheCapability(); err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	unlockOperation := s.lockStateSnapshotOperation(in.ContainerId)
	defer unlockOperation()
	if s.backendRepoClient == nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "state snapshot repository is unavailable"}, nil
	}
	operation, err := s.backendRepoClient.GetStateSnapshotByOperation(ctx, &pb.GetStateSnapshotByOperationRequest{
		SourceContainerId: in.ContainerId, OperationId: in.OperationId,
	})
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if operation != nil && operation.Ok && operation.Snapshot != nil {
		if err := validateStateSnapshotOperationInputs(operation.Snapshot, in); err != nil {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		switch operation.Snapshot.Status {
		case string(types.StateSnapshotStatusAvailable):
			if err := s.reportAvailableStateSnapshotBeforeAck(ctx, in.ContainerId, in.OperationId, operation.Snapshot); err != nil {
				return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: operation.Snapshot.ExternalId, Status: operation.Snapshot.Status}, nil
			}
			if s.stateVolumeManager != nil {
				if pendingOperationID, pending := s.stateVolumeManager.PendingOperation(in.ContainerId); pending && pendingOperationID == in.OperationId {
					if err := s.stateVolumeManager.AcknowledgePending(in.ContainerId, in.OperationId); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
						return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: operation.Snapshot.ExternalId, Status: operation.Snapshot.Status}, nil
					}
				} else if pending && pendingOperationID != in.OperationId {
					return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: ErrStateVolumePivotPending.Error(), StateSnapshotId: operation.Snapshot.ExternalId, Status: operation.Snapshot.Status}, nil
				}
				if operation.Snapshot.Mode == string(StateSnapshotModeTerminal) {
					if err := s.finalizeRecoveredTerminalState(ctx, in.ContainerId); err != nil {
						return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: operation.Snapshot.ExternalId, Status: operation.Snapshot.Status}, nil
					}
				}
			}
			return stateSnapshotResponse(operation.Snapshot), nil
		case string(types.StateSnapshotStatusFailed):
			return stateSnapshotResponse(operation.Snapshot), nil
		case string(types.StateSnapshotStatusPending):
			if _, exists := s.containerInstances.Get(in.ContainerId); !exists {
				return s.resumeDetachedStateSnapshot(ctx, in, operation)
			}
		default:
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "state snapshot operation has an invalid status", StateSnapshotId: operation.Snapshot.ExternalId, Status: operation.Snapshot.Status}, nil
		}
	}
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists || instance == nil || instance.Request == nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "container not found"}, nil
	}
	if instance.StateVolumes == nil || s.stateVolumeManager == nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "container has no block state volume group"}, nil
	}
	if instance.Runtime == nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "container runtime not found"}, nil
	}
	request := instance.Request
	pendingOperationID, hasPendingOperation := s.stateVolumeManager.PendingOperation(in.ContainerId)
	if hasPendingOperation && pendingOperationID != in.OperationId {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: ErrStateVolumePivotPending.Error()}, nil
	}
	commitLeases, err := stateVolumeWriterLeasesForSnapshot(request, instance, hasPendingOperation)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	terminalHold, err := instance.beginTerminalStateSnapshot(in.OperationId, mode, in.IncludeMemory)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	terminalFinished := false
	terminalPublished := false
	if runtimeAlreadyStopped && terminalHold != nil {
		terminalHold.markRuntimeStopped()
	}
	defer func() {
		if terminalHold == nil || terminalFinished {
			return
		}
		if !terminalHold.stopped() {
			instance.finishTerminalStateSnapshot(terminalHold)
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		var cleanupErr error
		if terminalPublished {
			cleanupErr = s.stopAndReleaseStateVolumes(cleanupCtx, request, instance)
		} else {
			pendingOperationID, pending := s.stateVolumeManager.PendingOperation(in.ContainerId)
			if pending && pendingOperationID == in.OperationId {
				cleanupErr = s.detachTerminalPendingStateVolumes(cleanupCtx, request, instance, in.OperationId)
			}
		}
		// Never leave spawn or worker shutdown blocked on an abandoned terminal
		// owner. Any failed safe detach is surfaced and the normal spawn teardown
		// gets one final bounded attempt after this hold is released.
		instance.finishTerminalStateSnapshot(terminalHold)
		if cleanupErr == nil {
			return
		}
		if response == nil {
			response = &pb.SnapshotContainerStateResponse{Ok: false}
		}
		response.Ok = false
		if response.ErrorMsg == "" {
			response.ErrorMsg = cleanupErr.Error()
		} else {
			response.ErrorMsg += "; terminal state detach failed: " + cleanupErr.Error()
		}
		responseErr = errors.Join(responseErr, cleanupErr)
	}()
	imageDigest, err := s.snapshotImageDigest(request)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	restoreMode := stateRestoreModeCold
	if in.IncludeMemory {
		restoreMode = stateRestoreModeMemory
	}
	plannedReceipt, err := s.stateVolumeManager.PlanSnapshot(ctx, in.ContainerId, in.OperationId)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	plannedMembers := make([]*pb.StateGeneration, 0, len(plannedReceipt.Generations))
	plannedCompactions := make([]*pb.StateGenerationCompaction, 0, len(plannedReceipt.Generations))
	for _, generation := range plannedReceipt.Generations {
		plannedMembers = append(plannedMembers, &pb.StateGeneration{
			VolumeId: generation.VolumeID, GenerationId: generation.GenerationID,
			ParentGenerationId:      generation.ParentGenerationID,
			CloneParentGenerationId: generation.CloneParentGenerationID,
			Generation:              generation.Generation, Name: generation.Name,
			MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
		})
		if generation.Compaction {
			plannedCompactions = append(plannedCompactions, &pb.StateGenerationCompaction{
				VolumeId: generation.VolumeID, GenerationId: generation.GenerationID,
				SourceGenerationId: generation.CompactionSourceGenerationID,
			})
		}
	}
	recoveryEnvelope, err := stateVolumeRecoveryEnvelopeFromRequest(request, in.OperationId, mode, in.IncludeMemory, in.Visible, imageDigest, instance.Runtime.Name())
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	// The control-plane pending row atomically escrows the exact member plan and
	// writer fences. It must exist before a local recovery journal can claim
	// authority to stop writers or seal any layer. A crash before the journal is
	// therefore a harmless DB-only pending retry; a journal can never select or
	// publish tenant state without matching server-side escrow.
	snapshot, recoveryProofToken, err := s.getOrCreatePendingStateSnapshot(ctx, request, in.OperationId, mode, in.IncludeMemory, in.Visible, imageDigest, instance.Runtime.Name(), plannedMembers, plannedCompactions, commitLeases)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	recoveryEnvelope.StateSnapshotID = snapshot.ExternalId
	recoveryEnvelope.RecoveryProofToken = recoveryProofToken
	if err := s.stateVolumeManager.BindSnapshotRecovery(in.ContainerId, recoveryEnvelope); err != nil {
		failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, err)
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
	}
	if snapshot.Status == string(types.StateSnapshotStatusPending) {
		armedSnapshot, armErr := s.armStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, recoveryProofToken)
		if armErr != nil {
			failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, armErr)
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		snapshot = armedSnapshot
	}
	if snapshot.Status == string(types.StateSnapshotStatusAvailable) {
		if err := s.reportAvailableStateSnapshotBeforeAck(ctx, in.ContainerId, in.OperationId, snapshot); err != nil {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		if err := s.stateVolumeManager.AcknowledgePending(in.ContainerId, in.OperationId); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		if mode == StateSnapshotModeTerminal {
			terminalPublished = true
			if err := instance.Runtime.Delete(ctx, in.ContainerId, &runtime.DeleteOpts{Force: true}); err != nil && !runtimeContainerNotFound(err) {
				return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
			}
			terminalHold.markRuntimeStopped()
			if err := s.stopAndReleaseStateVolumes(ctx, request, instance); err != nil {
				return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
			}
			terminalFinished = true
			instance.finishTerminalStateSnapshot(terminalHold)
		}
		return stateSnapshotResponse(snapshot), nil
	}
	if snapshot.Status != string(types.StateSnapshotStatusPending) {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "state snapshot is not retryable", StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
	}
	checkpointID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(snapshot.ExternalId+"\x00memory")).String()
	checkpointMetadata := (*checkpointCacheMetadata)(nil)
	fallbackReason := ""
	if _, err := s.stateVolumeManager.ReconcilePendingOperation(ctx, in.ContainerId, in.OperationId); err != nil {
		failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, err)
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
	}
	pendingReceipt, detachedPending, err := s.stateVolumeManager.PendingReceipt(in.ContainerId, in.OperationId)
	if err != nil {
		failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, err)
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
	}
	if pendingReceipt != nil {
		if mode == StateSnapshotModeTerminal {
			terminalHold.markRuntimeStopped()
			// Checkpoint bytes are committed atomically with their exact block
			// group. If a retry reaches a detached pending group without the
			// in-flight metadata, publish a truthful cold-state fallback instead
			// of claiming a memory restore that cannot be authenticated.
			if in.IncludeMemory {
				restoreMode = stateRestoreModeCold
				fallbackReason = "terminal publication retry resumed from immutable block state without checkpoint metadata"
			}
		}
	} else {
		if mode == StateSnapshotModeTerminal && in.IncludeMemory {
			opts := &CreateCheckpointOpts{
				Request: request, CheckpointId: checkpointID, ContainerIp: instance.ContainerIp,
				TerminateAfterCheckpoint: true, RequireListenerProof: true,
			}
			if checkpointErr := s.createCheckpoint(ctx, opts); checkpointErr != nil {
				restoreMode = stateRestoreModeCold
				fallbackReason = "terminal memory checkpoint failed: " + checkpointErr.Error()
			} else if opts.CheckpointMetadata == nil {
				restoreMode = stateRestoreModeCold
				fallbackReason = "terminal memory checkpoint persistence metadata is unavailable"
			} else {
				checkpointMetadata = opts.CheckpointMetadata
			}
			if checkpointRuntimeHasStopped(ctx, instance.Runtime, in.ContainerId) {
				terminalHold.markRuntimeStopped()
			}
		}
		hooks := StateVolumePivotHooks{}
		if !runtimeAlreadyStopped {
			hooks.Quiesce = func(hookCtx context.Context) error {
				err := instance.Runtime.Kill(hookCtx, in.ContainerId, syscall.SIGSTOP, &runtime.KillOpts{All: true})
				if mode == StateSnapshotModeTerminal && runtimeContainerNotFound(err) {
					return nil
				}
				return err
			}
		}
		if mode == StateSnapshotModeLive {
			hooks.Resume = func(hookCtx context.Context) error {
				return instance.Runtime.Kill(hookCtx, in.ContainerId, syscall.SIGCONT, &runtime.KillOpts{All: true})
			}
		} else if !runtimeAlreadyStopped {
			hooks.Complete = func(hookCtx context.Context, committed bool) error {
				if !committed {
					err := instance.Runtime.Kill(hookCtx, in.ContainerId, syscall.SIGCONT, &runtime.KillOpts{All: true})
					if runtimeContainerNotFound(err) {
						return nil
					}
					return err
				}
				err := instance.Runtime.Delete(hookCtx, in.ContainerId, &runtime.DeleteOpts{Force: true})
				if runtimeContainerNotFound(err) {
					return nil
				}
				return err
			}
		}
		pivotReceipt, err := s.stateVolumeManager.PivotWithHooks(ctx, in.ContainerId, in.OperationId, hooks)
		if err != nil {
			if errors.Is(err, ErrStateVolumePivotIndeterminate) {
				reconciled, reconcileErr := s.stateVolumeManager.ReconcilePendingOperation(ctx, in.ContainerId, in.OperationId)
				if reconcileErr != nil {
					resumeErr := s.stateVolumeManager.ResumeIndeterminateWriters(context.Background(), in.ContainerId, in.OperationId)
					failure := errors.Join(err, reconcileErr, resumeErr)
					return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
				}
				if reconciled != nil {
					pivotReceipt = reconciled
					err = nil
				} else {
					failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, err)
					return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
				}
			}
		}
		if err != nil {
			failure := s.failDeterminateStateSnapshot(ctx, snapshot, in.ContainerId, in.OperationId, err)
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: failure.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		if mode == StateSnapshotModeTerminal && pivotReceipt != nil {
			// ReconcilePendingOperation invokes Complete(committed=true) before
			// returning a committed receipt, so this is termination proof rather
			// than the ambiguous receipt that accompanied a lost QMP reply.
			terminalHold.markRuntimeStopped()
		}
	}
	if mode == StateSnapshotModeTerminal && !detachedPending {
		// Terminal snapshots have no remaining writers. Detach the live block
		// stack before any remote upload so storage outages and worker shutdown
		// cannot strand a mounted ext4/QSD/NBD group.
		if err := s.detachTerminalPendingStateVolumes(ctx, request, instance, in.OperationId); err != nil {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		detachedPending = true
	}
	snapshot, err = s.commitPendingStateSnapshot(ctx, request, snapshot, in.OperationId, restoreMode, fallbackReason, checkpointID, checkpointMetadata, commitLeases)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
	}
	if mode == StateSnapshotModeTerminal {
		terminalPublished = true
		if err := instance.Runtime.Delete(ctx, in.ContainerId, &runtime.DeleteOpts{Force: true}); err != nil && !runtimeContainerNotFound(err) {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		terminalHold.markRuntimeStopped()
		if err := s.stopAndReleaseStateVolumes(ctx, request, instance); err != nil {
			return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: snapshot.Status}, nil
		}
		terminalFinished = true
		instance.finishTerminalStateSnapshot(terminalHold)
	}
	return stateSnapshotResponse(snapshot), nil
}

func (s *Worker) commitPendingStateSnapshot(
	ctx context.Context,
	request *types.ContainerRequest,
	snapshot *pb.StateSnapshot,
	operationID, restoreMode, fallbackReason, checkpointID string,
	checkpointMetadata *checkpointCacheMetadata,
	commitLeases []*pb.StateVolumeLease,
) (*pb.StateSnapshot, error) {
	if request == nil || snapshot == nil {
		return nil, fmt.Errorf("pending state snapshot publication context is incomplete")
	}
	cas, bucketName, err := s.newStateVolumeCAS(ctx, request)
	if err != nil {
		return nil, err
	}
	generations, err := s.stateVolumeManager.UploadPending(ctx, request.ContainerId, operationID, cas)
	if err != nil {
		return nil, err
	}
	snapshot.Generations = nil
	terminalGenerations := make([]*pb.VolumeGeneration, 0, len(generations))
	publishedManifests := make(map[string]BlockV1Manifest, len(generations))
	existingResolver := &repositoryBlockV1Resolver{workspaceID: request.Workspace.ExternalId, repository: s.backendRepoClient, cas: cas}
	for _, generation := range generations {
		var terminal *pb.VolumeGeneration
		if generation.Reused {
			existing, err := s.backendRepoClient.GetVolumeGeneration(ctx, &pb.GetVolumeGenerationRequest{
				WorkspaceId: request.Workspace.ExternalId, GenerationId: generation.GenerationID,
			})
			if err != nil || existing == nil || !existing.Ok || existing.Generation == nil {
				if err == nil {
					err = fmt.Errorf("reused state generation %q is unavailable", generation.GenerationID)
				}
				return nil, err
			}
			terminal = existing.Generation
			manifest, err := existingResolver.ResolveBlockV1Manifest(ctx, generation.GenerationID)
			if err != nil {
				return nil, err
			}
			if terminal.Status != string(types.StateSnapshotStatusAvailable) || terminal.ExternalId != generation.GenerationID ||
				terminal.VolumeId != generation.VolumeID || terminal.Generation != generation.Generation ||
				manifest.ParentGenerationID != generation.ParentGenerationID ||
				manifest.CloneParentGenerationID != generation.CloneParentGenerationID ||
				manifest.Depth != generation.Depth || manifest.VirtualSizeBytes != generation.VirtualSizeBytes {
				return nil, fmt.Errorf("reused state generation %q does not match the exact planned ancestry", generation.GenerationID)
			}
			generation.Manifest = manifest
			publishedManifests[generation.GenerationID] = manifest
		} else {
			publishedManifests[generation.GenerationID] = generation.Manifest
			manifestData, manifestDigest, err := EncodeBlockV1ManifestCanonical(generation.Manifest)
			if err != nil {
				return nil, err
			}
			publishedDigest, err := PublishBlockV1Manifest(ctx, generation.Manifest, cas)
			if err != nil || publishedDigest != manifestDigest {
				if err == nil {
					err = fmt.Errorf("published manifest digest mismatch")
				}
				return nil, err
			}
			manifestKey, _ := stateBlockObjectKey(manifestDigest)
			storedBytes := int64(0)
			for _, chunk := range generation.Manifest.Chunks {
				storedBytes += chunk.SizeBytes
			}
			terminal = &pb.VolumeGeneration{
				ExternalId: generation.GenerationID, VolumeId: generation.VolumeID, Name: generation.Name,
				ParentGenerationId:      generation.Manifest.ParentGenerationID,
				CloneParentGenerationId: generation.Manifest.CloneParentGenerationID,
				Generation:              generation.Generation,
				Status:                  string(types.StateSnapshotStatusAvailable), ManifestKey: manifestKey,
				ManifestDigest: manifestDigest, ManifestSizeBytes: int64(len(manifestData)),
				ChunkCount: int64(len(generation.Manifest.Chunks)), LogicalSizeBytes: generation.Manifest.VirtualSizeBytes,
				StoredSizeBytes: storedBytes, BucketName: bucketName, ObjectPrefix: stateBlockObjectPrefix,
			}
		}
		terminalGenerations = append(terminalGenerations, terminal)
		snapshot.Generations = append(snapshot.Generations, &pb.StateGeneration{
			VolumeId: generation.VolumeID, GenerationId: generation.GenerationID, Generation: generation.Generation,
			ParentGenerationId:      generation.Manifest.ParentGenerationID,
			CloneParentGenerationId: generation.Manifest.CloneParentGenerationID,
			Name:                    generation.Name, MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
		})
	}
	snapshot.Status = string(types.StateSnapshotStatusAvailable)
	snapshot.RestoreMode = restoreMode
	snapshot.FallbackReason = fallbackReason
	snapshot.CheckpointId = ""
	snapshot.CheckpointDigest = ""
	snapshot.CheckpointCacheHash = ""
	snapshot.CheckpointSizeBytes = 0
	snapshot.CheckpointOriginKey = ""
	snapshot.CheckpointAccelerator = ""
	snapshot.CheckpointLocality = ""
	if checkpointMetadata != nil && restoreMode == stateRestoreModeMemory {
		snapshot.CheckpointId = checkpointID
		snapshot.CheckpointDigest = checkpointMetadata.hash
		snapshot.CheckpointCacheHash = checkpointMetadata.hash
		snapshot.CheckpointSizeBytes = checkpointMetadata.sizeBytes
		snapshot.CheckpointOriginKey = checkpointMetadata.originKey
		snapshot.CheckpointAccelerator = checkpointMetadata.accelerator
		snapshot.CheckpointLocality = checkpointMetadata.locality
	}
	requiredContentReports, err := s.stateBlockRequiredContentReports(ctx, request, terminalGenerations, publishedManifests, cas)
	if err != nil {
		return nil, err
	}
	committed, err := s.backendRepoClient.CommitStateSnapshot(ctx, &pb.CommitStateSnapshotRequest{
		WorkspaceId: request.Workspace.ExternalId, Snapshot: snapshot, Generations: terminalGenerations, Leases: commitLeases,
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, StorageNodeId: s.machineID,
		RecoveryClaimGeneration: snapshot.RecoveryClaimGeneration,
	})
	if err != nil || committed == nil || !committed.Ok || committed.Snapshot == nil {
		if err == nil {
			message := "atomic state snapshot commit failed"
			if committed != nil && committed.ErrorMsg != "" {
				message = committed.ErrorMsg
			}
			err = fmt.Errorf("%s", message)
		}
		return nil, err
	}
	reporter := s.cacheManager.ContentReporter()
	if reporter == nil {
		return nil, fmt.Errorf("state volume cache reporter/reconciler became unavailable after commit")
	}
	if err := reporter.reportBatchesAndFlush(request.Workspace.ExternalId, request.Stub.ExternalId, requiredContentReports); err != nil {
		return nil, err
	}
	if err := s.stateVolumeManager.AcknowledgePending(request.ContainerId, operationID); err != nil {
		return nil, err
	}
	return committed.Snapshot, nil
}

func (s *Worker) resumeDetachedStateSnapshot(
	ctx context.Context,
	in *pb.SnapshotContainerStateRequest,
	lookup *pb.GetStateSnapshotResponse,
) (*pb.SnapshotContainerStateResponse, error) {
	if lookup == nil || lookup.Snapshot == nil || lookup.Snapshot.Status != string(types.StateSnapshotStatusPending) {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "pending state snapshot recovery record is unavailable"}, nil
	}
	envelope, err := s.stateVolumeManager.SnapshotRecovery(in.ContainerId, in.OperationId)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: lookup.Snapshot.ExternalId, Status: lookup.Snapshot.Status}, nil
	}
	if envelope.WorkspaceID != lookup.WorkspaceId || envelope.StubID != lookup.StubId || envelope.Mode != lookup.Snapshot.Mode ||
		envelope.IncludeMemory != lookup.Snapshot.IncludeMemory || envelope.Visible != lookup.Snapshot.Visible ||
		envelope.ImageID != lookup.Snapshot.ImageId || envelope.ImageDigest != lookup.Snapshot.ImageDigest || envelope.RuntimeProfile != lookup.Snapshot.RuntimeProfile {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "pending state snapshot recovery envelope does not match repository escrow"}, nil
	}
	receipt, detached, err := s.stateVolumeManager.PendingReceipt(in.ContainerId, in.OperationId)
	if err != nil || receipt == nil || !detached {
		if err == nil {
			err = fmt.Errorf("pending state snapshot is not safely detached for offline recovery")
		}
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: lookup.Snapshot.ExternalId, Status: lookup.Snapshot.Status}, nil
	}
	if !stateVolumePlannedMembersMatchReceipt(lookup.PlannedMembers, lookup.PlannedCompactions, receipt) {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: "pending state snapshot journal does not match repository planned members"}, nil
	}
	// Claim only after authenticating the local fsynced envelope, detached
	// receipt, and exact server-side plan. This is the last non-secret boundary
	// before credential re-vend/upload, so a forged or journal-less replacement
	// can never steal another worker's recovery operation.
	claimed, err := s.claimStateSnapshotRecovery(ctx, lookup.Snapshot, in.ContainerId, in.OperationId, envelope.RecoveryProofToken)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: lookup.Snapshot.ExternalId, Status: lookup.Snapshot.Status}, nil
	}
	lookup.Snapshot = claimed
	envelope.RecoveryClaimGeneration = claimed.RecoveryClaimGeneration
	request, err := s.recoveryRequestFromEnvelope(ctx, in.ContainerId, envelope)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: lookup.Snapshot.ExternalId, Status: lookup.Snapshot.Status}, nil
	}
	snapshot := *lookup.Snapshot
	snapshot.Generations = nil
	fallbackReason := snapshot.FallbackReason
	if snapshot.IncludeMemory {
		fallbackReason = "worker replacement resumed exact block state without in-flight checkpoint metadata"
	}
	committed, err := s.commitPendingStateSnapshot(ctx, request, &snapshot, in.OperationId, stateRestoreModeCold, fallbackReason, "", nil, nil)
	if err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: snapshot.ExternalId, Status: string(types.StateSnapshotStatusPending)}, nil
	}
	if err := s.finalizeRecoveredTerminalState(ctx, in.ContainerId); err != nil {
		return &pb.SnapshotContainerStateResponse{Ok: false, ErrorMsg: err.Error(), StateSnapshotId: committed.ExternalId, Status: committed.Status}, nil
	}
	return stateSnapshotResponse(committed), nil
}

func (s *Worker) reconcileTerminalStateSnapshotJournals(ctx context.Context) error {
	if s.stateVolumeManager == nil || s.backendRepoClient == nil {
		return fmt.Errorf("state snapshot journal reconciliation services are unavailable")
	}
	journals, err := s.stateVolumeManager.Journals.List()
	if err != nil {
		return err
	}
	for _, journal := range journals {
		if journal.Recovery == nil || journal.Recovery.OperationID == "" {
			continue
		}
		operation, err := s.backendRepoClient.GetStateSnapshotByOperation(ctx, &pb.GetStateSnapshotByOperationRequest{
			SourceContainerId: journal.ContainerID, OperationId: journal.Recovery.OperationID,
		})
		if err != nil {
			return err
		}
		if operation == nil || !operation.Ok || operation.Snapshot == nil {
			return fmt.Errorf("state snapshot operation %q has a durable local recovery journal but no repository escrow", journal.Recovery.OperationID)
		}
		if operation.Snapshot.SourceContainerId != journal.ContainerID || operation.Snapshot.OperationId != journal.Recovery.OperationID {
			return fmt.Errorf("repository state snapshot operation does not match journal")
		}
		prePivotCleanup := journal.Phase == "recovery-bound" || journal.Phase == "prepivot-quarantine"
		if prePivotCleanup && (journal.Recovery.StateSnapshotID != operation.Snapshot.ExternalId ||
			journal.Recovery.WorkspaceID != operation.WorkspaceId || journal.Recovery.StubID != operation.StubId ||
			journal.Recovery.Mode != operation.Snapshot.Mode || journal.Recovery.IncludeMemory != operation.Snapshot.IncludeMemory ||
			journal.Recovery.Visible != operation.Snapshot.Visible || journal.Recovery.ImageID != operation.Snapshot.ImageId ||
			journal.Recovery.ImageDigest != operation.Snapshot.ImageDigest || journal.Recovery.RuntimeProfile != operation.Snapshot.RuntimeProfile) {
			return fmt.Errorf("pre-pivot state snapshot recovery envelope does not match repository escrow")
		}
		switch operation.Snapshot.Status {
		case string(types.StateSnapshotStatusPending):
			if prePivotCleanup {
				if !operation.Snapshot.Armed {
					return fmt.Errorf("pre-pivot state snapshot operation %q is awaiting authoritative unarmed escrow failure", journal.Recovery.OperationID)
				}
				claimed, err := s.claimStateSnapshotRecovery(ctx, operation.Snapshot, journal.ContainerID, journal.Recovery.OperationID, journal.Recovery.RecoveryProofToken)
				if err != nil {
					return err
				}
				reason := "source worker died before the terminal all-writers-stopped consistency boundary"
				if err := s.markStateSnapshotFailed(ctx, claimed, journal.ContainerID, journal.Recovery.OperationID, reason); err != nil {
					return err
				}
				if err := s.stateVolumeManager.QuarantinePrePivotRecovery(ctx, journal.ContainerID, journal.Recovery.OperationID); err != nil {
					return err
				}
				continue
			}
			response, err := s.resumeDetachedStateSnapshot(ctx, &pb.SnapshotContainerStateRequest{
				ContainerId: journal.ContainerID, OperationId: journal.Recovery.OperationID,
				Mode: journal.Recovery.Mode, IncludeMemory: journal.Recovery.IncludeMemory, Visible: journal.Recovery.Visible,
			}, operation)
			if err != nil {
				return err
			}
			if response == nil || !response.Ok {
				message := "offline state snapshot recovery failed"
				if response != nil && response.ErrorMsg != "" {
					message = response.ErrorMsg
				}
				return fmt.Errorf("%s", message)
			}
		case string(types.StateSnapshotStatusAvailable):
			if err := s.reportAvailableStateSnapshotBeforeAck(ctx, journal.ContainerID, journal.Recovery.OperationID, operation.Snapshot); err != nil {
				return err
			}
			pendingOperationID, pending := s.stateVolumeManager.PendingOperation(journal.ContainerID)
			if pending && pendingOperationID != journal.Recovery.OperationID {
				return ErrStateVolumePivotPending
			}
			if pending {
				if err := s.stateVolumeManager.AcknowledgePending(journal.ContainerID, journal.Recovery.OperationID); err != nil {
					return err
				}
			}
			if operation.Snapshot.Mode == string(StateSnapshotModeTerminal) {
				if err := s.finalizeRecoveredTerminalState(ctx, journal.ContainerID); err != nil {
					return err
				}
			}
		case string(types.StateSnapshotStatusFailed):
			if prePivotCleanup {
				if err := s.stateVolumeManager.QuarantinePrePivotRecovery(ctx, journal.ContainerID, journal.Recovery.OperationID); err != nil {
					return err
				}
				continue
			}
			// A failed terminal row is authoritative, but its local immutable
			// graph remains quarantined until an explicit safe retirement pass;
			// never advertise the worker while ambiguous block state exists.
			return fmt.Errorf("state snapshot operation %q failed while a local pending graph remains: %s", journal.Recovery.OperationID, operation.Snapshot.Reason)
		}
	}
	return nil
}

func (s *Worker) restoreContainerState(ctx context.Context, in *pb.RestoreContainerStateRequest) (*pb.RestoreContainerStateResponse, error) {
	if in == nil || in.ContainerId == "" || in.OperationId == "" || in.StateSnapshotId == "" {
		return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: "container_id, operation_id, and state_snapshot_id are required"}, nil
	}
	unlockOperation := s.lockStateSnapshotOperation(in.ContainerId)
	defer unlockOperation()
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists || instance == nil || instance.Request == nil {
		return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: "container not found"}, nil
	}
	if running, _ := instance.runtimeStartState(); running {
		return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: "state must be restored before container runtime start"}, nil
	}
	instance.stateMu.Lock()
	boundOperation, boundSnapshot := instance.StateRestoreOperationID, instance.StateRestoreSnapshotID
	instance.stateMu.Unlock()
	if boundOperation != "" || boundSnapshot != "" {
		if boundOperation != in.OperationId || boundSnapshot != in.StateSnapshotId {
			return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: "container restore is already bound to a different immutable operation/snapshot"}, nil
		}
		return &pb.RestoreContainerStateResponse{Ok: true}, nil
	}
	request := instance.Request
	if instance.StateVolumes == nil {
		handle, err := s.restoreStateVolumes(ctx, request, instance, in.StateSnapshotId)
		if err != nil {
			return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		instance.StateVolumes = handle
	} else if instance.StateVolumes.SourceStateSnapshotID != in.StateSnapshotId {
		return &pb.RestoreContainerStateResponse{Ok: false, ErrorMsg: "mounted state-volume group does not match requested snapshot"}, nil
	}
	request.StateSnapshotId = in.StateSnapshotId
	instance.stateMu.Lock()
	instance.StateRestoreOperationID = in.OperationId
	instance.StateRestoreSnapshotID = in.StateSnapshotId
	instance.stateMu.Unlock()
	instance.Request = request
	s.containerInstances.Set(in.ContainerId, instance)
	// The authoritative receipt is persisted by runContainer only after memory
	// restore succeeds, or after an exact re-clone and cold boot succeeds.
	return &pb.RestoreContainerStateResponse{Ok: true}, nil
}
