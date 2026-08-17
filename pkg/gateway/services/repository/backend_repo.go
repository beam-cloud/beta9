package repository_services

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackendRepositoryService struct {
	ctx           context.Context
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	workerRepo    repository.WorkerRepository
	computeRepo   repository.ComputeRepository
	pb.UnimplementedBackendRepositoryServiceServer
}

func NewBackendRepositoryService(ctx context.Context, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, workerRepo repository.WorkerRepository, computeRepo repository.ComputeRepository) *BackendRepositoryService {
	return &BackendRepositoryService{ctx: ctx, backendRepo: backendRepo, containerRepo: containerRepo, workerRepo: workerRepo, computeRepo: computeRepo}
}

func (s *BackendRepositoryService) authorizeStateSnapshotWorker(
	ctx context.Context,
	containerId, workerId, workerInstanceId, storageNodeId, workspaceId string,
	allowDetached bool,
) error {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return fmt.Errorf("state snapshot operation requires an authenticated worker")
	}
	if s.containerRepo == nil || s.workerRepo == nil || strings.TrimSpace(containerId) == "" || strings.TrimSpace(workerId) == "" || strings.TrimSpace(workerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" {
		return fmt.Errorf("state snapshot worker assignment is unavailable")
	}
	worker, err := s.workerRepo.GetWorkerById(workerId)
	if err != nil || worker == nil || worker.Id != workerId || worker.InstanceId != workerInstanceId || worker.MachineId != storageNodeId {
		return fmt.Errorf("state snapshot caller does not match its registered storage node")
	}
	if err := authorizeRegisteredWorkerToken(ctx, authInfo, worker, s.computeRepo); err != nil {
		return err
	}
	state, err := s.containerRepo.GetContainerState(containerId)
	if err != nil {
		var notFound *types.ErrContainerStateNotFound
		if !allowDetached || !errors.As(err, &notFound) {
			return err
		}
	} else if state == nil {
		return fmt.Errorf("state snapshot source container assignment is corrupt")
	} else if state.WorkerId != workerId || state.MachineId != storageNodeId ||
		(workspaceId != "" && state.WorkspaceId != workspaceId) {
		return fmt.Errorf("state snapshot worker does not own the source container assignment")
	}
	if authInfo.Token.TokenType == types.TokenTypeWorkerPrivate &&
		(authInfo.Workspace == nil || (workspaceId != "" && authInfo.Workspace.ExternalId != workspaceId)) {
		return fmt.Errorf("state snapshot worker workspace does not match the source container")
	}
	return nil
}

func (s *BackendRepositoryService) CreateStateSnapshot(ctx context.Context, req *pb.CreateStateSnapshotRequest) (*pb.CreateStateSnapshotResponse, error) {
	if req == nil || req.Snapshot == nil {
		return &pb.CreateStateSnapshotResponse{ErrorMsg: "state snapshot is required"}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.Snapshot.SourceContainerId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.WorkspaceId, false); err != nil {
		return &pb.CreateStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	workspace, stub, err := s.resolveWorkspaceAndStub(ctx, req.WorkspaceId, req.StubId)
	if err != nil {
		return &pb.CreateStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	snapshot := stateSnapshotFromProto(req.Snapshot)
	if snapshot == nil {
		return &pb.CreateStateSnapshotResponse{ErrorMsg: "state snapshot is required"}, nil
	}
	snapshot.WorkspaceId, snapshot.StubId = workspace.Id, stub.Id
	snapshot.SourceWorkerId, snapshot.SourceWorkerInstanceId, snapshot.StorageNodeId = req.WorkerId, req.WorkerInstanceId, req.StorageNodeId
	snapshot.SourceStubExternalId = stub.ExternalId
	snapshot.SourceStubName = stub.Name
	snapshot.SourceStubType = string(stub.Type)
	members := make([]types.StateGeneration, 0, len(req.Members))
	for _, member := range req.Members {
		if member == nil {
			return &pb.CreateStateSnapshotResponse{ErrorMsg: "state snapshot member is required"}, nil
		}
		members = append(members, stateGenerationFromProto(member))
	}
	compactions := make([]types.StateGenerationCompaction, 0, len(req.Compactions))
	for _, compaction := range req.Compactions {
		if compaction == nil {
			return &pb.CreateStateSnapshotResponse{ErrorMsg: "state snapshot compaction plan is required"}, nil
		}
		compactions = append(compactions, stateGenerationCompactionFromProto(compaction))
	}
	var leases []types.StateVolumeLease
	if len(req.Leases) != 0 {
		leases, err = stateVolumeLeasesFromProto(req.Leases)
		if err != nil {
			return &pb.CreateStateSnapshotResponse{ErrorMsg: err.Error()}, nil
		}
	}
	created, err := s.backendRepo.CreateStateSnapshot(ctx, snapshot, members, compactions, leases)
	if err != nil {
		return &pb.CreateStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.CreateStateSnapshotResponse{Ok: true, Snapshot: stateSnapshotToProto(created),
		RecoveryProofToken: created.RecoveryProofToken}, nil
}

func (s *BackendRepositoryService) ArmStateSnapshot(ctx context.Context, req *pb.ArmStateSnapshotRequest) (*pb.StateSnapshotMutationResponse, error) {
	if req == nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot arm request is required"}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.SourceContainerId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, "", false); err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	if strings.TrimSpace(req.RecoveryProofToken) == "" {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot recovery proof token is required"}, nil
	}
	snapshot, err := s.backendRepo.ArmStateSnapshot(ctx, req.StateSnapshotId, req.SourceContainerId,
		req.OperationId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.RecoveryProofToken)
	if err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: stateSnapshotToProto(snapshot)}, nil
}

func (s *BackendRepositoryService) requireDeadStateSnapshotRecoveryOwner(workerId, workerInstanceId string) error {
	owner, err := s.workerRepo.GetWorkerById(workerId)
	if err != nil {
		var notFound *types.ErrWorkerNotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("verify previous state snapshot recovery owner: %w", err)
	}
	if owner != nil && owner.InstanceId == workerInstanceId && owner.Status != types.WorkerStatusDisabled {
		return fmt.Errorf("previous recovery owner is still authoritative for this operation")
	}
	return nil
}

func (s *BackendRepositoryService) ClaimStateSnapshotRecovery(ctx context.Context, req *pb.ClaimStateSnapshotRecoveryRequest) (*pb.StateSnapshotMutationResponse, error) {
	if req == nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot recovery claim is required"}, nil
	}
	stored, err := s.backendRepo.GetStateSnapshotByOperation(ctx, req.SourceContainerId, req.OperationId)
	if err != nil || stored.ExternalId != req.StateSnapshotId ||
		(stored.Status != types.StateSnapshotStatusPending && stored.Status != types.StateSnapshotStatusAvailable) ||
		stored.Mode != "terminal" || !stored.Armed {
		if err == nil {
			err = fmt.Errorf("state snapshot is not an exact armed terminal recovery candidate")
		}
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	if stored.StorageNodeId != req.StorageNodeId {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot belongs to another storage node"}, nil
	}
	if strings.TrimSpace(req.RecoveryProofToken) == "" || stored.RecoveryProofToken != req.RecoveryProofToken {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot recovery proof is invalid"}, nil
	}
	if req.PreviousClaimGeneration < 0 ||
		(req.PreviousClaimGeneration != stored.RecoveryClaimGeneration &&
			!(stored.RecoveryWorkerId == req.WorkerId && stored.RecoveryWorkerInstanceId == req.WorkerInstanceId &&
				stored.RecoveryClaimGeneration == req.PreviousClaimGeneration+1)) {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot recovery claim generation was superseded"}, nil
	}
	if stored.RecoveryClaimGeneration == 0 && req.WorkerId == stored.SourceWorkerId && req.WorkerInstanceId == stored.SourceWorkerInstanceId {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "source worker process does not need a recovery claim"}, nil
	}
	workspace, err := s.backendRepo.GetWorkspace(ctx, stored.WorkspaceId)
	if err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, stored.SourceContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, true); err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	activeWorker, activeInstance := stored.SourceWorkerId, stored.SourceWorkerInstanceId
	if stored.RecoveryWorkerId != "" {
		activeWorker, activeInstance = stored.RecoveryWorkerId, stored.RecoveryWorkerInstanceId
	}
	if activeWorker == req.WorkerId && activeInstance == req.WorkerInstanceId {
		claimed, claimErr := s.backendRepo.ClaimStateSnapshotRecovery(ctx, req.StateSnapshotId, req.SourceContainerId,
			req.OperationId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
			req.RecoveryProofToken, req.PreviousClaimGeneration)
		if claimErr != nil {
			return &pb.StateSnapshotMutationResponse{ErrorMsg: claimErr.Error()}, nil
		}
		return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: stateSnapshotToProto(claimed)}, nil
	}
	if err := s.requireDeadStateSnapshotRecoveryOwner(activeWorker, activeInstance); err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	state, stateErr := s.containerRepo.GetContainerState(stored.SourceContainerId)
	if stateErr != nil {
		var notFound *types.ErrContainerStateNotFound
		if !errors.As(stateErr, &notFound) {
			return &pb.StateSnapshotMutationResponse{ErrorMsg: fmt.Sprintf("verify source container recovery assignment: %v", stateErr)}, nil
		}
	} else if state != nil && state.WorkerId == activeWorker {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "source container is still assigned to the previous recovery owner"}, nil
	}
	claimed, err := s.backendRepo.ClaimStateSnapshotRecovery(ctx, req.StateSnapshotId, req.SourceContainerId,
		req.OperationId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId,
		req.RecoveryProofToken, req.PreviousClaimGeneration)
	if err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: stateSnapshotToProto(claimed)}, nil
}

// GetStateSnapshotRecoveryCredentials vends only the object-store credentials
// for an exact, server-authorized recovery claim. Workspace and stub scope are
// derived from the immutable snapshot operation; caller/journal scope is never
// trusted and the generic container credential path has no detached bypass.
func (s *BackendRepositoryService) GetStateSnapshotRecoveryCredentials(ctx context.Context, req *pb.GetStateSnapshotRecoveryCredentialsRequest) (*pb.GetStateSnapshotRecoveryCredentialsResponse, error) {
	fail := func(err error) (*pb.GetStateSnapshotRecoveryCredentialsResponse, error) {
		return &pb.GetStateSnapshotRecoveryCredentialsResponse{ErrorMsg: err.Error()}, nil
	}
	if req == nil || strings.TrimSpace(req.StateSnapshotId) == "" || strings.TrimSpace(req.SourceContainerId) == "" ||
		strings.TrimSpace(req.OperationId) == "" || strings.TrimSpace(req.WorkerId) == "" ||
		strings.TrimSpace(req.WorkerInstanceId) == "" || strings.TrimSpace(req.StorageNodeId) == "" ||
		strings.TrimSpace(req.RecoveryProofToken) == "" {
		return fail(fmt.Errorf("exact state snapshot operation and worker process identities are required"))
	}
	stored, err := s.backendRepo.GetStateSnapshotByOperation(ctx, req.SourceContainerId, req.OperationId)
	if err != nil {
		return fail(err)
	}
	if stored.ExternalId != req.StateSnapshotId || !stored.Armed || stored.Mode != "terminal" ||
		(stored.Status != types.StateSnapshotStatusPending && stored.Status != types.StateSnapshotStatusAvailable) ||
		stored.RecoveryWorkerId != req.WorkerId || stored.RecoveryWorkerInstanceId != req.WorkerInstanceId ||
		stored.RecoveryClaimGeneration != req.RecoveryClaimGeneration || stored.StorageNodeId != req.StorageNodeId ||
		strings.TrimSpace(req.RecoveryProofToken) == "" || stored.RecoveryProofToken != req.RecoveryProofToken {
		return fail(fmt.Errorf("state snapshot recovery credentials require the exact active recovery claim"))
	}
	workspace, err := s.backendRepo.GetWorkspace(ctx, stored.WorkspaceId)
	if err != nil {
		return fail(err)
	}
	if err := s.authorizeStateSnapshotWorker(ctx, stored.SourceContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, true); err != nil {
		return fail(err)
	}
	planned, err := s.backendRepo.GetStateSnapshotPlan(ctx, stored.WorkspaceId, stored.ExternalId)
	if err != nil {
		return fail(err)
	}
	if len(planned) == 0 {
		return fail(fmt.Errorf("state snapshot recovery claim has no immutable member escrow"))
	}
	if !workspace.StorageAvailable() || workspace.Storage == nil {
		return fail(fmt.Errorf("workspace storage is unavailable for state snapshot recovery"))
	}
	storage := workspace.Storage
	return &pb.GetStateSnapshotRecoveryCredentialsResponse{
		Ok: true, WorkspaceId: workspace.ExternalId, WorkspaceName: workspace.Name,
		StubId: stored.SourceStubExternalId, StubName: stored.SourceStubName, StubType: stored.SourceStubType,
		ImageId: stored.ImageId, ImageDigest: stored.ImageDigest, RuntimeProfile: stored.RuntimeProfile,
		WorkspaceStorageId: uint32(derefUint(storage.Id)), WorkspaceStorageExternalId: derefString(storage.ExternalId),
		WorkspaceStorage: &pb.StateSnapshotWorkspaceStorageCredentials{
			EndpointUrl: derefString(storage.EndpointUrl), Region: derefString(storage.Region),
			BucketName: derefString(storage.BucketName), AccessKey: derefString(storage.AccessKey),
			SecretKey: derefString(storage.SecretKey), ForcePathStyle: true,
		},
	}, nil
}

func (s *BackendRepositoryService) FailStateSnapshot(ctx context.Context, req *pb.FailStateSnapshotRequest) (*pb.StateSnapshotMutationResponse, error) {
	if req == nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: "state snapshot failure request is required"}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.SourceContainerId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, "", true); err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	snapshot, err := s.backendRepo.FailStateSnapshot(ctx, req.StateSnapshotId, req.SourceContainerId,
		req.OperationId, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, req.Reason, req.RecoveryClaimGeneration)
	if err != nil {
		return &pb.StateSnapshotMutationResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: stateSnapshotToProto(snapshot)}, nil
}

func (s *BackendRepositoryService) CommitStateSnapshot(ctx context.Context, req *pb.CommitStateSnapshotRequest) (*pb.CommitStateSnapshotResponse, error) {
	if req == nil || req.Snapshot == nil {
		return &pb.CommitStateSnapshotResponse{ErrorMsg: "state snapshot is required"}, nil
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.CommitStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	snapshot := stateSnapshotFromProto(req.Snapshot)
	if snapshot == nil {
		return &pb.CommitStateSnapshotResponse{ErrorMsg: "state snapshot is required"}, nil
	}
	snapshot.WorkspaceId = workspace.Id
	stored, err := s.backendRepo.GetStateSnapshotByOperation(ctx, snapshot.SourceContainerId, snapshot.OperationId)
	if err != nil || stored.ExternalId != snapshot.ExternalId || stored.WorkspaceId != workspace.Id || stored.StorageNodeId != req.StorageNodeId {
		if err == nil {
			err = fmt.Errorf("state snapshot commit does not match its immutable operation owner")
		}
		return &pb.CommitStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, stored.SourceContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, true); err != nil {
		return &pb.CommitStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	generations := make([]types.VolumeGeneration, 0, len(req.Generations))
	for _, generation := range req.Generations {
		parsed := volumeGenerationFromProto(generation)
		if parsed == nil {
			return &pb.CommitStateSnapshotResponse{ErrorMsg: "volume generation is required"}, nil
		}
		parsed.WorkspaceId = workspace.Id
		generations = append(generations, *parsed)
	}
	var leases []types.StateVolumeLease
	if len(req.Leases) != 0 {
		leases, err = stateVolumeLeasesFromProto(req.Leases)
		if err != nil {
			return &pb.CommitStateSnapshotResponse{ErrorMsg: err.Error()}, nil
		}
	}
	updated, err := s.backendRepo.CommitStateSnapshot(ctx, snapshot, generations, leases, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, req.RecoveryClaimGeneration)
	if err != nil {
		return &pb.CommitStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.CommitStateSnapshotResponse{Ok: true, Snapshot: stateSnapshotToProto(updated)}, nil
}

func (s *BackendRepositoryService) GetStateSnapshot(ctx context.Context, req *pb.GetStateSnapshotRequest) (*pb.GetStateSnapshotResponse, error) {
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	snapshot, err := s.backendRepo.GetStateSnapshot(ctx, workspace.Id, req.StateSnapshotId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.GetStateSnapshotResponse{Ok: true, Snapshot: stateSnapshotToProto(snapshot),
		WorkspaceId: workspace.ExternalId, StubId: snapshot.SourceStubExternalId}, nil
}

func (s *BackendRepositoryService) GetStateSnapshotByOperation(ctx context.Context, req *pb.GetStateSnapshotByOperationRequest) (*pb.GetStateSnapshotResponse, error) {
	if req == nil || req.SourceContainerId == "" || req.OperationId == "" {
		return &pb.GetStateSnapshotResponse{ErrorMsg: "source_container_id and operation_id are required"}, nil
	}
	snapshot, err := s.backendRepo.GetStateSnapshotByOperation(ctx, req.SourceContainerId, req.OperationId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	workspace, err := s.backendRepo.GetWorkspace(ctx, snapshot.WorkspaceId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	members, err := s.backendRepo.GetStateSnapshotPlan(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	compactions, err := s.backendRepo.GetStateSnapshotCompactionPlan(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.GetStateSnapshotResponse{Ok: true, Snapshot: stateSnapshotToProto(snapshot),
		WorkspaceId: workspace.ExternalId, StubId: snapshot.SourceStubExternalId,
		PlannedMembers: stateGenerationsToProto(members), PlannedCompactions: stateGenerationCompactionsToProto(compactions)}, nil
}

func (s *BackendRepositoryService) GetPendingStateSnapshotByContainer(ctx context.Context, req *pb.GetPendingStateSnapshotByContainerRequest) (*pb.GetStateSnapshotResponse, error) {
	if req == nil || strings.TrimSpace(req.SourceContainerId) == "" {
		return &pb.GetStateSnapshotResponse{ErrorMsg: "source_container_id is required"}, nil
	}
	snapshot, err := s.backendRepo.GetPendingStateSnapshotByContainer(ctx, req.SourceContainerId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	workspace, err := s.backendRepo.GetWorkspace(ctx, snapshot.WorkspaceId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	if req.StorageNodeId != snapshot.StorageNodeId {
		return &pb.GetStateSnapshotResponse{ErrorMsg: "pending state snapshot belongs to another storage node"}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, snapshot.SourceContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, true); err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	members, err := s.backendRepo.GetStateSnapshotPlan(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	compactions, err := s.backendRepo.GetStateSnapshotCompactionPlan(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
	if err != nil {
		return &pb.GetStateSnapshotResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.GetStateSnapshotResponse{Ok: true, Snapshot: stateSnapshotToProto(snapshot),
		WorkspaceId: workspace.ExternalId, StubId: snapshot.SourceStubExternalId,
		PlannedMembers: stateGenerationsToProto(members), PlannedCompactions: stateGenerationCompactionsToProto(compactions)}, nil
}

func (s *BackendRepositoryService) GetVolumeGeneration(ctx context.Context, req *pb.GetVolumeGenerationRequest) (*pb.GetVolumeGenerationResponse, error) {
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.GetVolumeGenerationResponse{ErrorMsg: err.Error()}, nil
	}
	generation, err := s.backendRepo.GetVolumeGeneration(ctx, workspace.Id, req.GenerationId)
	if err != nil {
		return &pb.GetVolumeGenerationResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.GetVolumeGenerationResponse{Ok: true, Generation: volumeGenerationToProto(generation)}, nil
}

func (s *BackendRepositoryService) RenewStateVolumeAttachments(ctx context.Context, req *pb.RenewStateVolumeAttachmentsRequest) (*pb.RenewStateVolumeAttachmentsResponse, error) {
	if req == nil {
		return &pb.RenewStateVolumeAttachmentsResponse{ErrorMsg: "state-volume renewal request is required"}, nil
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.RenewStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	leases, err := stateVolumeLeasesFromProto(req.Leases)
	if err != nil {
		return &pb.RenewStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.ContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, false); err != nil {
		return &pb.RenewStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	expiresAt, err := s.backendRepo.RenewStateVolumeAttachments(ctx, workspace.Id, req.ContainerId,
		req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, leases)
	if err != nil {
		return &pb.RenewStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.RenewStateVolumeAttachmentsResponse{Ok: true, LeaseExpiresAt: timestamppb.New(expiresAt)}, nil
}

func (s *BackendRepositoryService) ReleaseStateVolumeAttachments(ctx context.Context, req *pb.ReleaseStateVolumeAttachmentsRequest) (*pb.ReleaseStateVolumeAttachmentsResponse, error) {
	if req == nil {
		return &pb.ReleaseStateVolumeAttachmentsResponse{ErrorMsg: "state-volume release request is required"}, nil
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ReleaseStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	leases, err := stateVolumeLeasesFromProto(req.Leases)
	if err != nil {
		return &pb.ReleaseStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.ContainerId, req.WorkerId,
		req.WorkerInstanceId, req.StorageNodeId, workspace.ExternalId, false); err != nil {
		return &pb.ReleaseStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.backendRepo.ReleaseStateVolumeAttachments(ctx, workspace.Id, req.ContainerId,
		req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, leases); err != nil {
		return &pb.ReleaseStateVolumeAttachmentsResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.ReleaseStateVolumeAttachmentsResponse{Ok: true}, nil
}

func (s *BackendRepositoryService) requireDeadStateVolumeReleaseOwner(workerId, workerInstanceId string) error {
	owner, err := s.workerRepo.GetWorkerById(workerId)
	if err != nil {
		var notFound *types.ErrWorkerNotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}
	if owner != nil && owner.InstanceId == workerInstanceId && owner.Status != types.WorkerStatusDisabled {
		return fmt.Errorf("state-volume release owner process is still authoritative")
	}
	return nil
}

func (s *BackendRepositoryService) authorizeDetachedStateVolumeReleaseWorker(
	ctx context.Context, workspaceExternalId, containerId, workerId, workerInstanceId, storageNodeId string,
) error {
	if err := s.authorizeStateSnapshotWorker(ctx, containerId, workerId, workerInstanceId,
		storageNodeId, workspaceExternalId, true); err != nil {
		return err
	}
	state, err := s.containerRepo.GetContainerState(containerId)
	if err == nil && state != nil {
		return fmt.Errorf("state-volume release requires the source container assignment to be absent")
	}
	if err != nil {
		var notFound *types.ErrContainerStateNotFound
		if !errors.As(err, &notFound) {
			return err
		}
	}
	return nil
}

func (s *BackendRepositoryService) ClaimStateVolumeRelease(ctx context.Context, req *pb.ClaimStateVolumeReleaseRequest) (*pb.ClaimStateVolumeReleaseResponse, error) {
	fail := func(err error) (*pb.ClaimStateVolumeReleaseResponse, error) {
		return &pb.ClaimStateVolumeReleaseResponse{ErrorMsg: err.Error()}, nil
	}
	if req == nil {
		return fail(fmt.Errorf("state-volume release claim is required"))
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return fail(err)
	}
	if req.RecoveryWorkerId == "" || req.RecoveryWorkerInstanceId == "" || req.StorageNodeId == "" {
		return fail(fmt.Errorf("exact recovery worker process and storage node identities are required"))
	}
	if err := s.authorizeDetachedStateVolumeReleaseWorker(ctx, workspace.ExternalId, req.ContainerId,
		req.RecoveryWorkerId, req.RecoveryWorkerInstanceId, req.StorageNodeId); err != nil {
		return fail(err)
	}
	if err := s.requireDeadStateVolumeReleaseOwner(req.SourceWorkerId, req.SourceWorkerInstanceId); err != nil {
		return fail(err)
	}
	if existing, claimErr := s.backendRepo.GetStateVolumeReleaseClaim(ctx, workspace.Id, req.ContainerId); claimErr == nil {
		if existing.StorageNodeId != req.StorageNodeId {
			return fail(fmt.Errorf("state-volume release obligation belongs to another storage node"))
		}
		if existing.RecoveryWorkerId != req.RecoveryWorkerId || existing.RecoveryWorkerInstanceId != req.RecoveryWorkerInstanceId {
			if err := s.requireDeadStateVolumeReleaseOwner(existing.RecoveryWorkerId, existing.RecoveryWorkerInstanceId); err != nil {
				return fail(fmt.Errorf("previous recovery claimant is still authoritative: %w", err))
			}
		}
	} else if !errors.Is(claimErr, sql.ErrNoRows) {
		return fail(claimErr)
	}
	members, err := stateVolumeReleaseMembersFromProto(req.Members)
	if err != nil {
		return fail(err)
	}
	claim, err := s.backendRepo.ClaimStateVolumeRelease(ctx, workspace.Id, req.ContainerId,
		req.SourceWorkerId, req.SourceWorkerInstanceId, req.StorageNodeId,
		req.RecoveryWorkerId, req.RecoveryWorkerInstanceId, req.JournalDigest,
		req.PreviousClaimGeneration, members)
	if err != nil {
		return fail(err)
	}
	return &pb.ClaimStateVolumeReleaseResponse{Ok: true, ReleaseClaimId: claim.ExternalId,
		ReleaseClaimGeneration: claim.ClaimGeneration, Completed: claim.Completed}, nil
}

func (s *BackendRepositoryService) BeginStateVolumeReleaseIntent(ctx context.Context, req *pb.BeginStateVolumeReleaseIntentRequest) (*pb.ClaimStateVolumeReleaseResponse, error) {
	fail := func(err error) (*pb.ClaimStateVolumeReleaseResponse, error) {
		return &pb.ClaimStateVolumeReleaseResponse{ErrorMsg: err.Error()}, nil
	}
	if req == nil {
		return fail(fmt.Errorf("state-volume release intent is required"))
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return fail(err)
	}
	if err := s.authorizeStateSnapshotWorker(ctx, req.ContainerId, req.SourceWorkerId,
		req.SourceWorkerInstanceId, req.StorageNodeId, workspace.ExternalId, false); err != nil {
		return fail(err)
	}
	members, err := stateVolumeReleaseMembersFromProto(req.Members)
	if err != nil {
		return fail(err)
	}
	claim, err := s.backendRepo.BeginStateVolumeReleaseIntent(ctx, workspace.Id, req.ContainerId,
		req.SourceWorkerId, req.SourceWorkerInstanceId, req.StorageNodeId, req.JournalDigest, members)
	if err != nil {
		return fail(err)
	}
	return &pb.ClaimStateVolumeReleaseResponse{Ok: true, ReleaseClaimId: claim.ExternalId,
		ReleaseClaimGeneration: claim.ClaimGeneration, Completed: claim.Completed}, nil
}

func (s *BackendRepositoryService) CompleteClaimedStateVolumeRelease(ctx context.Context, req *pb.CompleteClaimedStateVolumeReleaseRequest) (*pb.CompleteClaimedStateVolumeReleaseResponse, error) {
	fail := func(err error) (*pb.CompleteClaimedStateVolumeReleaseResponse, error) {
		return &pb.CompleteClaimedStateVolumeReleaseResponse{ErrorMsg: err.Error()}, nil
	}
	if req == nil {
		return fail(fmt.Errorf("claimed state-volume release completion is required"))
	}
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil {
		return fail(err)
	}
	if err := s.authorizeDetachedStateVolumeReleaseWorker(ctx, workspace.ExternalId, req.ContainerId,
		req.RecoveryWorkerId, req.RecoveryWorkerInstanceId, req.StorageNodeId); err != nil {
		return fail(err)
	}
	claim, err := s.backendRepo.GetStateVolumeReleaseClaim(ctx, workspace.Id, req.ContainerId)
	if err != nil {
		return fail(err)
	}
	if claim.ExternalId != req.ReleaseClaimId || claim.RecoveryWorkerId != req.RecoveryWorkerId ||
		claim.RecoveryWorkerInstanceId != req.RecoveryWorkerInstanceId || claim.StorageNodeId != req.StorageNodeId ||
		claim.ClaimGeneration != req.ReleaseClaimGeneration {
		return fail(fmt.Errorf("state-volume release completion does not match the active recovery claim"))
	}
	if err := s.backendRepo.CompleteClaimedStateVolumeRelease(ctx, workspace.Id, req.ContainerId,
		req.ReleaseClaimId, req.RecoveryWorkerId, req.RecoveryWorkerInstanceId,
		req.StorageNodeId, req.ReleaseClaimGeneration); err != nil {
		return fail(err)
	}
	return &pb.CompleteClaimedStateVolumeReleaseResponse{Ok: true}, nil
}

func stateVolumeReleaseMembersFromProto(in []*pb.StateVolumeReleaseMember) ([]types.StateVolumeReleaseMember, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("at least one state-volume release member is required")
	}
	members := make([]types.StateVolumeReleaseMember, 0, len(in))
	for _, member := range in {
		if member == nil {
			return nil, fmt.Errorf("state-volume release member is required")
		}
		members = append(members, types.StateVolumeReleaseMember{VolumeId: member.VolumeId, FencingToken: member.FencingToken})
	}
	return members, nil
}

func stateVolumeLeasesFromProto(in []*pb.StateVolumeLease) ([]types.StateVolumeLease, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("at least one state-volume lease is required")
	}
	leases := make([]types.StateVolumeLease, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, lease := range in {
		if lease == nil || lease.VolumeId == "" || lease.AttachmentToken == "" || lease.FencingToken <= 0 {
			return nil, fmt.Errorf("state-volume lease requires volume_id, attachment_token, and a positive fencing_token")
		}
		key := lease.VolumeId + ":" + lease.AttachmentToken
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate state-volume lease")
		}
		seen[key] = struct{}{}
		leases = append(leases, types.StateVolumeLease{
			VolumeId: lease.VolumeId, AttachmentToken: lease.AttachmentToken, FencingToken: lease.FencingToken,
		})
	}
	return leases, nil
}

func (s *BackendRepositoryService) resolveWorkspaceAndStub(ctx context.Context, workspaceId, stubId string) (*types.Workspace, *types.Stub, error) {
	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil {
		return nil, nil, err
	}
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubId)
	if err != nil {
		return nil, nil, err
	}
	if stub.WorkspaceId != workspace.Id {
		return nil, nil, fmt.Errorf("stub does not belong to workspace")
	}
	return &workspace, &stub.Stub, nil
}

func stateSnapshotToProto(snapshot *types.StateSnapshot) *pb.StateSnapshot {
	if snapshot == nil {
		return nil
	}
	generations := make([]*pb.StateGeneration, 0, len(snapshot.Generations))
	for _, generation := range snapshot.Generations {
		generations = append(generations, stateGenerationToProto(generation))
	}
	return &pb.StateSnapshot{
		ExternalId: snapshot.ExternalId, OperationId: snapshot.OperationId,
		SourceContainerId: snapshot.SourceContainerId, SourceWorkerId: snapshot.SourceWorkerId,
		SourceWorkerInstanceId: snapshot.SourceWorkerInstanceId,
		RecoveryWorkerId:       snapshot.RecoveryWorkerId, RecoveryWorkerInstanceId: snapshot.RecoveryWorkerInstanceId,
		RecoveryClaimGeneration: snapshot.RecoveryClaimGeneration,
		StorageNodeId:           snapshot.StorageNodeId, Armed: snapshot.Armed,
		Status: string(snapshot.Status), Reason: snapshot.Reason,
		Mode: snapshot.Mode, IncludeMemory: snapshot.IncludeMemory, Visible: snapshot.Visible,
		ImageId: snapshot.ImageId, ImageDigest: snapshot.ImageDigest,
		RuntimeProfile: snapshot.RuntimeProfile, CheckpointId: snapshot.CheckpointId,
		RestoreMode: snapshot.RestoreMode, FallbackReason: snapshot.FallbackReason,
		CheckpointDigest: snapshot.CheckpointDigest, CheckpointCacheHash: snapshot.CheckpointCacheHash,
		CheckpointSizeBytes: snapshot.CheckpointSizeBytes, CheckpointOriginKey: snapshot.CheckpointOriginKey,
		CheckpointAccelerator: snapshot.CheckpointAccelerator, CheckpointLocality: snapshot.CheckpointLocality,
		SourceStubExternalId: snapshot.SourceStubExternalId, SourceStubName: snapshot.SourceStubName,
		SourceStubType: snapshot.SourceStubType,
		Public:         snapshot.Public, Generations: generations,
		CreatedAt: timestamp(snapshot.CreatedAt), UpdatedAt: timestamp(snapshot.UpdatedAt), CompletedAt: nullTimestamp(snapshot.CompletedAt),
	}
}

func stateSnapshotFromProto(in *pb.StateSnapshot) *types.StateSnapshot {
	if in == nil {
		return nil
	}
	generations := make([]types.StateGeneration, 0, len(in.Generations))
	for _, generation := range in.Generations {
		if generation != nil {
			generations = append(generations, stateGenerationFromProto(generation))
		}
	}
	return &types.StateSnapshot{
		ExternalId: in.ExternalId, OperationId: in.OperationId, SourceContainerId: in.SourceContainerId,
		SourceWorkerId: in.SourceWorkerId, SourceWorkerInstanceId: in.SourceWorkerInstanceId,
		RecoveryWorkerId: in.RecoveryWorkerId, RecoveryWorkerInstanceId: in.RecoveryWorkerInstanceId,
		RecoveryClaimGeneration: in.RecoveryClaimGeneration,
		StorageNodeId:           in.StorageNodeId, Armed: in.Armed,
		Mode: in.Mode, IncludeMemory: in.IncludeMemory, Visible: in.Visible,
		Status: types.StateSnapshotStatus(in.Status), Reason: in.Reason, ImageId: in.ImageId, ImageDigest: in.ImageDigest,
		RuntimeProfile: in.RuntimeProfile, CheckpointId: in.CheckpointId, RestoreMode: in.RestoreMode,
		FallbackReason: in.FallbackReason, CheckpointDigest: in.CheckpointDigest,
		CheckpointCacheHash: in.CheckpointCacheHash, CheckpointSizeBytes: in.CheckpointSizeBytes,
		CheckpointOriginKey: in.CheckpointOriginKey, CheckpointAccelerator: in.CheckpointAccelerator,
		CheckpointLocality: in.CheckpointLocality, SourceStubExternalId: in.SourceStubExternalId,
		SourceStubName: in.SourceStubName, SourceStubType: in.SourceStubType,
		Public: in.Public, Generations: generations,
	}
}

func volumeGenerationToProto(generation *types.VolumeGeneration) *pb.VolumeGeneration {
	if generation == nil {
		return nil
	}
	return &pb.VolumeGeneration{
		ExternalId: generation.ExternalId, VolumeId: generation.VolumeId, Name: generation.Name,
		ParentGenerationId: generation.ParentGenerationId, Generation: generation.Generation,
		CloneParentGenerationId: generation.CloneParentGenerationId,
		Status:                  string(generation.Status), Reason: generation.Reason, ManifestKey: generation.ManifestKey,
		ManifestDigest: generation.ManifestDigest, ManifestSizeBytes: generation.ManifestSizeBytes,
		ChunkCount: generation.ChunkCount, LogicalSizeBytes: generation.LogicalSizeBytes,
		StoredSizeBytes: generation.StoredSizeBytes, BucketName: generation.BucketName,
		ObjectPrefix: generation.ObjectPrefix, Public: generation.Public,
		CreatedAt: timestamp(generation.CreatedAt), UpdatedAt: timestamp(generation.UpdatedAt),
		CompletedAt: nullTimestamp(generation.CompletedAt),
	}
}

func volumeGenerationFromProto(in *pb.VolumeGeneration) *types.VolumeGeneration {
	if in == nil {
		return nil
	}
	return &types.VolumeGeneration{
		ExternalId: in.ExternalId, VolumeId: in.VolumeId, Name: in.Name,
		ParentGenerationId: in.ParentGenerationId, Generation: in.Generation,
		CloneParentGenerationId: in.CloneParentGenerationId,
		Status:                  types.StateSnapshotStatus(in.Status), Reason: in.Reason, ManifestKey: in.ManifestKey,
		ManifestDigest: in.ManifestDigest, ManifestSizeBytes: in.ManifestSizeBytes,
		ChunkCount: in.ChunkCount, LogicalSizeBytes: in.LogicalSizeBytes,
		StoredSizeBytes: in.StoredSizeBytes, BucketName: in.BucketName,
		ObjectPrefix: in.ObjectPrefix, Public: in.Public,
	}
}

func stateGenerationToProto(generation types.StateGeneration) *pb.StateGeneration {
	return &pb.StateGeneration{
		VolumeId: generation.VolumeId, GenerationId: generation.GenerationId, Name: generation.Name,
		MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
		Generation: generation.Generation, ParentGenerationId: generation.ParentGenerationId,
		CloneParentGenerationId: generation.CloneParentGenerationId,
	}
}

func stateGenerationFromProto(generation *pb.StateGeneration) types.StateGeneration {
	return types.StateGeneration{
		VolumeId: generation.VolumeId, GenerationId: generation.GenerationId, Name: generation.Name,
		MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
		Generation: generation.Generation, ParentGenerationId: generation.ParentGenerationId,
		CloneParentGenerationId: generation.CloneParentGenerationId,
	}
}

func stateGenerationCompactionFromProto(in *pb.StateGenerationCompaction) types.StateGenerationCompaction {
	return types.StateGenerationCompaction{
		VolumeId: in.VolumeId, GenerationId: in.GenerationId, SourceGenerationId: in.SourceGenerationId,
	}
}

func stateGenerationCompactionsToProto(in []types.StateGenerationCompaction) []*pb.StateGenerationCompaction {
	plans := make([]*pb.StateGenerationCompaction, 0, len(in))
	for _, plan := range in {
		plans = append(plans, &pb.StateGenerationCompaction{
			VolumeId: plan.VolumeId, GenerationId: plan.GenerationId, SourceGenerationId: plan.SourceGenerationId,
		})
	}
	return plans
}

func stateGenerationsToProto(in []types.StateGeneration) []*pb.StateGeneration {
	members := make([]*pb.StateGeneration, 0, len(in))
	for _, member := range in {
		members = append(members, stateGenerationToProto(member))
	}
	return members
}

func timestamp(value types.Time) *timestamppb.Timestamp {
	if value.Time.IsZero() {
		return nil
	}
	return timestamppb.New(value.Time)
}

func nullTimestamp(value types.NullTime) *timestamppb.Timestamp {
	if !value.Valid {
		return nil
	}
	return timestamppb.New(value.Time)
}

func derefUint(value *uint) uint {
	if value == nil {
		return 0
	}
	return *value
}
