package repository_services

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type restoreReceiptBackendForTest struct {
	repository.BackendRepository
	snapshot *types.StateSnapshot
}

func (r *restoreReceiptBackendForTest) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	return types.Workspace{Id: 9, ExternalId: "workspace"}, nil
}

func (r *restoreReceiptBackendForTest) GetStateSnapshot(context.Context, uint, string) (*types.StateSnapshot, error) {
	copy := *r.snapshot
	copy.Generations = append([]types.StateGeneration(nil), r.snapshot.Generations...)
	return &copy, nil
}

type restoreReceiptContainerForTest struct {
	repository.ContainerRepository
	state                 *types.ContainerState
	receipt               *types.StateRestoreReceipt
	mutateBeforeSet       bool
	beforeSet             func()
	currentWorkerInstance func() string
}

func (r *restoreReceiptContainerForTest) GetContainerState(string) (*types.ContainerState, error) {
	copy := *r.state
	return &copy, nil
}

func (r *restoreReceiptContainerForTest) GetStateRestoreReceipt(string) (*types.StateRestoreReceipt, error) {
	if r.receipt == nil {
		return nil, errors.New("state restore receipt is unavailable")
	}
	copy := *r.receipt
	copy.Generations = append([]types.StateGeneration(nil), r.receipt.Generations...)
	return &copy, nil
}

func (r *restoreReceiptContainerForTest) SetStateRestoreReceipt(_ string, workerInstanceID string, receipt *types.StateRestoreReceipt, expected *types.ContainerState) error {
	if r.beforeSet != nil {
		r.beforeSet()
		r.beforeSet = nil
	}
	if r.mutateBeforeSet {
		r.state.AssignmentId = "replacement-assignment:1"
		r.mutateBeforeSet = false
	}
	currentWorkerInstance := "instance"
	if r.currentWorkerInstance != nil {
		currentWorkerInstance = r.currentWorkerInstance()
	}
	if workerInstanceID != currentWorkerInstance {
		return errors.New("state restore receipt worker process was superseded before its outcome could be persisted")
	}
	if expected.WorkerId != r.state.WorkerId || expected.MachineId != r.state.MachineId ||
		expected.StateSnapshotId != r.state.StateSnapshotId || expected.AssignmentId != r.state.AssignmentId ||
		expected.StateVolumePlanId != r.state.StateVolumePlanId || expected.StateVolumePlanHash != r.state.StateVolumePlanHash {
		return errors.New("state restore receipt assignment changed before its outcome could be persisted")
	}
	copy := *receipt
	copy.Generations = append([]types.StateGeneration(nil), receipt.Generations...)
	r.receipt = &copy
	return nil
}

type restoreReceiptWorkerForTest struct {
	repository.WorkerRepository
	workers map[string]*types.Worker
}

func (r *restoreReceiptWorkerForTest) GetWorkerById(workerID string) (*types.Worker, error) {
	worker := r.workers[workerID]
	if worker == nil {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	return worker, nil
}

func restoreReceiptWorkerContext() context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: types.TokenTypeWorker, ExternalId: "worker-token"},
	})
}

func restoreReceiptWorkspaceContext(workspaceID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary, ExternalId: "workspace-token"},
		Workspace: &types.Workspace{ExternalId: workspaceID},
	})
}

func TestRegisteredRouteContainerIDKeepsSharedWorkerRouteUnscoped(t *testing.T) {
	route := types.BackendRoute{Kind: types.BackendRouteKindWorker}

	got := registeredRouteContainerID("container-a", route)
	if got != "" {
		t.Fatalf("container id = %q, want empty", got)
	}
}

func TestRegisteredRouteContainerIDScopesContainerRoutes(t *testing.T) {
	route := types.BackendRoute{Kind: types.BackendRouteKindContainer}

	got := registeredRouteContainerID("container-a", route)
	if got != "container-a" {
		t.Fatalf("container id = %q, want container-a", got)
	}
}

func TestSetStateRestoreReceiptRequiresCurrentWorkerAndExactSnapshotMembership(t *testing.T) {
	generations := []types.StateGeneration{{
		VolumeId: "bd9a783d-9857-4d1d-ae42-62629f7ecf89", GenerationId: "4f96d83a-0eb8-4a9a-afd4-fcae79069302",
		Name: "root", MountPath: "/", Root: true, Generation: 3,
	}}
	backend := &restoreReceiptBackendForTest{snapshot: &types.StateSnapshot{
		ExternalId: "86dd770a-1adc-4e2e-9677-4acbc7601ef9", SourceStubExternalId: "source-stub",
		Mode: "terminal", Status: types.StateSnapshotStatusAvailable, Generations: generations,
	}}
	containers := &restoreReceiptContainerForTest{state: &types.ContainerState{
		ContainerId: "container", StubId: "source-stub", WorkspaceId: "workspace",
		WorkerId: "worker", MachineId: "node", StateSnapshotId: backend.snapshot.ExternalId, AssignmentId: "assignment:1",
		StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
	}}
	workers := &restoreReceiptWorkerForTest{workers: map[string]*types.Worker{
		"worker": {Id: "worker", MachineId: "node", InstanceId: "instance", WorkerTokenId: "worker-token", Status: types.WorkerStatusAvailable},
		"stale":  {Id: "stale", MachineId: "node", InstanceId: "stale-instance", WorkerTokenId: "worker-token", Status: types.WorkerStatusAvailable},
	}}
	service := NewContainerRepositoryService(context.Background(), containers, backend, workers, nil)
	request := &pb.SetStateRestoreReceiptRequest{
		ContainerId: "container", WorkerId: "stale", WorkerInstanceId: "stale-instance", StorageNodeId: "node",
		DeliveryToken: "assignment:1", StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
		Receipt: &pb.StateRestoreReceipt{StateSnapshotId: backend.snapshot.ExternalId, RestoreMode: "cold_state",
			Generations: []*pb.StateGeneration{{VolumeId: generations[0].VolumeId, GenerationId: generations[0].GenerationId,
				Name: "root", MountPath: "/", Root: true, Generation: 3}}},
	}
	response, err := service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "current container assignment")

	request.WorkerId = "worker"
	request.WorkerInstanceId = "instance"
	request.Receipt.Generations[0].GenerationId = "f02d0b8d-eb6a-4660-af86-47ca9b42526d"
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "authoritative snapshot membership")

	request.Receipt.Generations[0].GenerationId = generations[0].GenerationId
	containers.state.AssignmentId = "assignment:2"
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "current delivery epoch")
	containers.state.AssignmentId = "assignment:1"

	containers.mutateBeforeSet = true
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "assignment changed")
	require.Nil(t, containers.receipt)

	containers.state.AssignmentId = "assignment:1"
	containers.currentWorkerInstance = func() string { return workers.workers["worker"].InstanceId }
	containers.beforeSet = func() { workers.workers["worker"].InstanceId = "replacement-instance" }
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "worker process was superseded")
	require.Nil(t, containers.receipt)
	workers.workers["worker"].InstanceId = "instance"

	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, "cold_state", containers.receipt.RestoreMode)
}

func TestSetStateRestoreReceiptValidatesMemoryAndColdFallbackSemantics(t *testing.T) {
	generation := types.StateGeneration{VolumeId: "bd9a783d-9857-4d1d-ae42-62629f7ecf89", GenerationId: "4f96d83a-0eb8-4a9a-afd4-fcae79069302",
		Name: "root", MountPath: "/", Root: true, Generation: 3}
	backend := &restoreReceiptBackendForTest{snapshot: &types.StateSnapshot{
		ExternalId: "86dd770a-1adc-4e2e-9677-4acbc7601ef9", SourceStubExternalId: "source-stub",
		Mode: "terminal", Status: types.StateSnapshotStatusAvailable, CheckpointId: "checkpoint", Generations: []types.StateGeneration{generation},
	}}
	containers := &restoreReceiptContainerForTest{state: &types.ContainerState{
		ContainerId: "container", StubId: "source-stub", WorkspaceId: "workspace", WorkerId: "worker", MachineId: "node",
		StateSnapshotId: backend.snapshot.ExternalId, AssignmentId: "assignment:1", StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
	}}
	workers := &restoreReceiptWorkerForTest{workers: map[string]*types.Worker{"worker": {Id: "worker", MachineId: "node", InstanceId: "instance", WorkerTokenId: "worker-token"}}}
	service := NewContainerRepositoryService(context.Background(), containers, backend, workers, nil)
	request := &pb.SetStateRestoreReceiptRequest{ContainerId: "container", WorkerId: "worker", WorkerInstanceId: "instance", StorageNodeId: "node",
		DeliveryToken: "assignment:1", StateVolumePlanId: "plan", StateVolumePlanHash: "hash",
		Receipt: &pb.StateRestoreReceipt{StateSnapshotId: backend.snapshot.ExternalId, RestoreMode: "memory",
			Generations: []*pb.StateGeneration{{VolumeId: generation.VolumeId, GenerationId: generation.GenerationId,
				Name: generation.Name, MountPath: generation.MountPath, Root: true, Generation: generation.Generation}}}}

	response, err := service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.True(t, response.Ok)

	containers.state.StateFork = true
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "memory restore receipt")

	request.Receipt.RestoreMode = "cold_state"
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "fallback reason")
	request.Receipt.FallbackReason = "memory restore was deliberately forbidden for a fork"
	response, err = service.SetStateRestoreReceipt(restoreReceiptWorkerContext(), request)
	require.NoError(t, err)
	require.True(t, response.Ok)
}

func TestGetStateRestoreReceiptDirectRPCRequiresContainerWorkspace(t *testing.T) {
	containers := &restoreReceiptContainerForTest{
		state: &types.ContainerState{ContainerId: "container", WorkspaceId: "workspace-a"},
		receipt: &types.StateRestoreReceipt{StateSnapshotId: "snapshot", RestoreMode: "cold_state", Generations: []types.StateGeneration{{
			VolumeId: "volume", GenerationId: "generation", Name: "root", MountPath: "/", Root: true, Generation: 1,
		}}},
	}
	service := NewContainerRepositoryService(context.Background(), containers, nil, nil, nil)

	response, err := service.GetStateRestoreReceipt(restoreReceiptWorkspaceContext("workspace-b"), &pb.GetStateRestoreReceiptRequest{ContainerId: "container"})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "unauthorized")

	response, err = service.GetStateRestoreReceipt(restoreReceiptWorkerContext(), &pb.GetStateRestoreReceiptRequest{ContainerId: "container"})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "unauthorized")

	response, err = service.GetStateRestoreReceipt(restoreReceiptWorkspaceContext("workspace-a"), &pb.GetStateRestoreReceiptRequest{ContainerId: "container"})
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.Equal(t, "snapshot", response.Receipt.StateSnapshotId)
}
