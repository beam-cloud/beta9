package vm

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func (r *vmRuntime) SandboxSnapshotMemory(_ context.Context, req *pb.PodSandboxSnapshotMemoryRequest) (*pb.PodSandboxSnapshotMemoryResponse, error) {
	if r.memoryResponse == nil {
		return &pb.PodSandboxSnapshotMemoryResponse{ErrorMsg: "checkpoint unavailable"}, nil
	}
	if req.TerminateAfterCheckpoint {
		delete(r.containers.states, req.ContainerId)
	}
	return r.memoryResponse, nil
}

func TestPauseReleasesComputeAndRestoresPairedMemory(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.TokenID = info.Token.ExternalId
	runtime.memoryResponse = &pb.PodSandboxSnapshotMemoryResponse{Ok: true, Runtime: "microvm", CheckpointId: "memory", DiskSnapshots: []*pb.PodSandboxDiskSnapshot{{DiskName: rootDisk(v), SnapshotId: "root-after-shutdown"}}}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.NoError(t, s.pause(ctx, v))
	require.Equal(t, "paused", v.Status)
	require.Equal(t, "paused", v.DesiredState)
	require.Empty(t, v.ContainerID)
	require.Equal(t, 0, gateway.stops, "terminal checkpoint owns shutdown")
	require.Equal(t, 0, runtime.snapshots, "do not change the disk after pausing RAM")
	v, err := s.repo.GetVM(ctx, v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Equal(t, "memory", v.MemoryCheckpointID)
	require.NoError(t, s.activate(ctx, info, v))
	require.Equal(t, "memory", runtime.checkpoint)
	require.Equal(t, map[string]string{rootDisk(v): "root-after-shutdown"}, runtime.checkpointDisks)
	require.NotEmpty(t, v.ContainerID)
	require.NoError(t, s.start(ctx, info, v))
	require.Empty(t, v.MemoryCheckpointID, "consume the checkpoint after successful restore")
}

func TestPauseFailureRequiresExplicitColdRecovery(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.Error(t, s.pause(ctx, v))
	stored, err := s.repo.GetVM(ctx, v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Equal(t, "paused", stored.DesiredState)
	require.Empty(t, stored.MemoryCheckpointID)
	require.ErrorContains(t, s.activate(ctx, info, stored), "cold=true")
	require.Empty(t, runtime.requests)
	e := managementAPI(s, info)
	response := vmRequest(e, "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/start", `{"cold":true}`)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Len(t, runtime.requests, 1)
}

func TestWarmResumeRejectsChangedDiskAndCredential(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	delete(runtime.containers.states, v.ContainerID)
	v.ContainerID, v.DesiredState, v.TokenID = "", "paused", info.Token.ExternalId
	v.MemoryCheckpointID = "memory"
	v.MemoryDiskSnapshots = map[string]string{rootDisk(v): "older-disk"}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.ErrorContains(t, s.activate(ctx, info, v), "changed after memory pause")
	require.Equal(t, "paused", v.DesiredState)
	require.Empty(t, runtime.requests)
	v.MemoryDiskSnapshots[rootDisk(v)] = "root-after-shutdown"
	v.TokenID = "another-credential"
	require.ErrorContains(t, s.activate(ctx, info, v), "original owner credential")
	require.Empty(t, runtime.requests)
}

func TestReconcileConsumesCheckpointEvenAfterAmbiguousRunningStatus(t *testing.T) {
	s, v, _, _, _ := fixture()
	v.Status = "running"
	v.MemoryCheckpointID = "already-restored"
	v.MemoryDiskSnapshots = map[string]string{rootDisk(v): "old-generation"}
	require.NoError(t, s.reconcileVM(context.Background(), v))
	stored, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Empty(t, stored.MemoryCheckpointID)
	require.Empty(t, stored.MemoryDiskSnapshots)
}

func TestExtraDisksUseMicroVMDefaultsAndValidatedSizes(t *testing.T) {
	spec := types.VMSpec{ImageID: "base", Disks: []*pb.DurableDisk{{Name: "data", MountPath: "/data", Size: "5GiB"}}}
	require.NoError(t, validate(&spec))
	require.Equal(t, "qcow", spec.Disks[0].Driver)
	require.Equal(t, "ext4", spec.Disks[0].Filesystem)
	require.Equal(t, "5368709120", spec.Disks[0].Size)
	spec.Disks[0].Size = "500Mi"
	require.Error(t, validate(&spec))
}

func TestIncompletePauseReconciliationDrainsComputeWithoutImplicitColdResume(t *testing.T) {
	s, v, info, _, gateway := fixture()
	v.DesiredState, v.Status = "paused", "error"
	v.Error = "memory pause failed"
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.NoError(t, s.reconcileVM(ctx, v))
	require.Empty(t, v.ContainerID)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, "paused", v.DesiredState)
	require.ErrorContains(t, s.activate(ctx, info, v), "cold=true")
}
