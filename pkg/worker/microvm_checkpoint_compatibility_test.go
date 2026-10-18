package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func microVMCompatibilityRequest(persistent bool, key string) *types.ContainerRequest {
	request := &types.ContainerRequest{ContainerId: "vm-restore", UseVM: true, Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "vm-stub", Type: types.StubType(types.StubTypeSandbox)}}, Checkpoint: &types.Checkpoint{CheckpointId: "memory", Runtime: "microvm", Status: string(types.CheckpointStatusAvailable), CompatibilityKey: key}}
	if persistent {
		request.Env = []string{"BEAM_VM_SYSTEMD=1"}
	}
	return request
}

func TestMicroVMCheckpointStatePersistsCompatibility(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		request := microVMCompatibilityRequest(persistent, "")
		require.Equal(t, persistent, request.IsPersistentVM())
		backend := &fakeBackendRepoClient{}
		worker := &Worker{backendRepoClient: backend, checkpointCompatibilityKey: "v1:guest-v6-profile"}
		require.NoError(t, worker.createCheckpointState(context.Background(), "memory", request, types.CheckpointStatusAvailable, "", "microvm", nil))
		require.NotNil(t, backend.lastCreate)
		require.Equal(t, "microvm", backend.lastCreate.Runtime)
		require.Equal(t, worker.checkpointCompatibilityKey, backend.lastCreate.CompatibilityKey, "CPU VM compatibility must reach the durable checkpoint record")
		require.Equal(t, request.ContainerId, backend.lastCreate.SourceContainerId)
	}
}

func TestMicroVMRestoreRejectsProfilesBeforePreparation(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, key := range []string{"", "v1:main-one-share", "v1:custom-daemon-v5"} {
			worker := &Worker{runtime: NewMockRuntime("microvm", runtime.Capabilities{CheckpointRestore: true, BlockRoot: true}), checkpointCompatibilityKey: "v1:guest-v6-profile"}
			request := microVMCompatibilityRequest(persistent, key)
			checkpoint := request.Checkpoint
			err := worker.runContainerWithEvictionBarrier(context.Background(), request, func() error { t.Fatal("incompatible CPU VM restore reached eviction"); return nil })
			var incompatible *ErrCheckpointHostIncompatible
			require.ErrorAs(t, err, &incompatible)
			require.Nil(t, worker.containerInstances, "compatibility must be checked before image/disk preparation")
			require.Same(t, checkpoint, request.Checkpoint, "never silently drop a requested memory restore")
		}
	}
}

func TestMicroVMDirectRestoreRejectsProfilesBeforeMaterialization(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, key := range []string{"", "v1:main-one-share", "v1:custom-daemon-v5"} {
			rt := NewMockRuntime("microvm", runtime.Capabilities{CheckpointRestore: true, BlockRoot: true})
			worker := &Worker{checkpointCompatibilityKey: "v1:guest-v6-profile", criuManager: &staticCheckpointManager{}, containerInstances: common.NewSafeMap[*ContainerInstance]()}
			request := microVMCompatibilityRequest(persistent, key)
			worker.containerInstances.Set(request.ContainerId, &ContainerInstance{Runtime: rt})
			code, restored, started, err := worker.attemptRestoreCheckpoint(context.Background(), request, nil, nil, nil, nil)
			var incompatible *ErrCheckpointHostIncompatible
			require.ErrorAs(t, err, &incompatible)
			require.Equal(t, -1, code)
			require.False(t, restored)
			require.False(t, started)
		}
	}
}

func TestMicroVMRestoreRequiresMatchingMemoryProfile(t *testing.T) {
	rt := NewMockRuntime("microvm", runtime.Capabilities{CheckpointRestore: true, BlockRoot: true})
	worker := &Worker{runtime: rt, checkpointCompatibilityKey: "v1:guest-v6-profile", criuManager: &staticCheckpointManager{}}
	request := microVMCompatibilityRequest(true, worker.checkpointCompatibilityKey)
	require.NoError(t, worker.validateCheckpointRestorePlacement(request))
	require.True(t, worker.canRestoreCheckpoint(request, rt))
	request.Checkpoint.CompatibilityKey = ""
	require.False(t, worker.canRestoreCheckpoint(request, rt))
	request.Checkpoint.CompatibilityKey = worker.checkpointCompatibilityKey
	worker.checkpointCompatibilityKey = ""
	require.False(t, worker.canRestoreCheckpoint(request, rt), "unknown worker profile is not compatible")
	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	require.NoError(t, worker.validateCheckpointRestorePlacement(request))
	require.True(t, worker.canRestoreCheckpoint(request, rt), "filesystem-only restore has no guest-memory ABI")
}

func TestMicroVMCheckpointCannotPublishAvailableWithoutProfile(t *testing.T) {
	backend := &fakeBackendRepoClient{}
	worker := &Worker{backendRepoClient: backend}
	request := microVMCompatibilityRequest(true, "")
	require.ErrorContains(t, worker.createCheckpointState(context.Background(), "memory", request, types.CheckpointStatusAvailable, "", "microvm", nil), "without its compatibility profile")
	require.Zero(t, backend.createCalls)
	require.NoError(t, worker.createCheckpointState(context.Background(), "memory", request, types.CheckpointStatusCheckpointFailed, "", "microvm", nil), "failure state must remain visible")
	require.Equal(t, 1, backend.createCalls)
	require.Equal(t, string(types.CheckpointStatusCheckpointFailed), backend.lastCreate.Status)
}
