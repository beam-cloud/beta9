package scheduler

import (
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCPUMicroVMCheckpointRequiresReportedCompatibleWorker(t *testing.T) {
	request := &types.ContainerRequest{Cpu: 1000, Memory: 512, UseVM: true, Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}, Checkpoint: &types.Checkpoint{Status: string(types.CheckpointStatusAvailable), Runtime: "microvm", CompatibilityKey: types.CheckpointCompatibilityMicroVMPrefix + strings.Repeat("a", 64)}}
	worker := func(key string, status types.WorkerStatus) *types.Worker {
		return &types.Worker{Runtime: "microvm", Status: status, FreeCpu: 1000, FreeMemory: 2048, CheckpointCompatibilityKey: key}
	}
	matching := worker(request.Checkpoint.CompatibilityKey, types.WorkerStatusAvailable)
	oldMain := worker(types.CheckpointCompatibilityLegacyPrefix+strings.Repeat("b", 64), types.WorkerStatusAvailable)
	oldCustom := worker(types.CheckpointCompatibilityLegacyPrefix+strings.Repeat("c", 64), types.WorkerStatusAvailable)
	missing := worker("", types.WorkerStatusAvailable)
	pending := worker("", types.WorkerStatusPending)
	workers := []*types.Worker{oldMain, oldCustom, missing, pending, matching}
	require.Equal(t, []*types.Worker{matching}, filterWorkersByResources(workers, request, nil))
	require.False(t, hostMatchesCheckpoint(request.Checkpoint, pending), "unknown pending profile cannot accept CPU VM memory")
	request.Checkpoint.Runtime = "" // Historical runtime metadata must not bypass UseVM placement.
	require.Equal(t, []*types.Worker{matching}, filterWorkersByResources(workers, request, nil))
	request.Checkpoint.CompatibilityKey = ""
	require.Equal(t, []*types.Worker{oldMain, oldCustom}, filterWorkersByResources(workers, request, nil), "keyless historical checkpoints remain on reported legacy workers")
	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	require.Equal(t, workers, filterWorkersByResources(workers, request, nil), "filesystem-only restore has no saved guest-memory ABI")
	request.Checkpoint = nil
	require.Equal(t, workers, filterWorkersByResources(workers, request, nil), "fresh starts retain normal capacity selection")
}

func TestMicroVMMixedFleetCheckpointRouting(t *testing.T) {
	legacy := types.CheckpointCompatibilityLegacyPrefix + strings.Repeat("a", 64)
	current := types.CheckpointCompatibilityMicroVMPrefix + strings.Repeat("b", 64)
	other := types.CheckpointCompatibilityMicroVMPrefix + strings.Repeat("c", 64)
	for _, tc := range []struct {
		name, key               string
		status                  types.WorkerStatus
		wantLegacy, wantCurrent bool
	}{
		{"reported_main", legacy, types.WorkerStatusAvailable, true, false},
		{"new_guest", current, types.WorkerStatusAvailable, false, true},
		{"other_new_guest", other, types.WorkerStatusAvailable, false, false},
		{"unknown_pending", "", types.WorkerStatusPending, false, false},
		{"unreported_ready", "", types.WorkerStatusAvailable, false, false},
		{"malformed_legacy", "v1:", types.WorkerStatusAvailable, false, false},
		{"malformed_new", types.CheckpointCompatibilityMicroVMPrefix + strings.Repeat("z", 64), types.WorkerStatusAvailable, false, false},
		{"unknown_runtime_profile", "unknown:process", types.WorkerStatusAvailable, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &types.Worker{Runtime: "microvm", Status: tc.status, CheckpointCompatibilityKey: tc.key}
			checkpoint := &types.Checkpoint{Runtime: "microvm", Status: string(types.CheckpointStatusAvailable)}
			require.Equal(t, tc.wantLegacy, hostMatchesCheckpoint(checkpoint, worker), "legacy keyless checkpoint routing")
			checkpoint.CompatibilityKey = current
			require.Equal(t, tc.wantCurrent, hostMatchesCheckpoint(checkpoint, worker), "new checkpoint requires exact reported ABI")
		})
	}
	checkpoint := &types.Checkpoint{Runtime: "microvm", CompatibilityKey: legacy}
	require.True(t, hostMatchesCheckpoint(checkpoint, &types.Worker{Runtime: "microvm", CheckpointCompatibilityKey: legacy}))
	require.False(t, hostMatchesCheckpoint(checkpoint, &types.Worker{Runtime: "microvm", CheckpointCompatibilityKey: current}))
	checkpoint.CompatibilityKey = types.CheckpointCompatibilityMicroVMPrefix
	require.False(t, hostMatchesCheckpoint(checkpoint, &types.Worker{Runtime: "microvm", CheckpointCompatibilityKey: checkpoint.CompatibilityKey}), "equal malformed keys are not a compatibility proof")
}

func TestMetadataMicroVMCheckpointRoutingRemainsSeparate(t *testing.T) {
	metadata := types.CheckpointCompatibilityMicroVMMetadataPrefix + strings.Repeat("d", 64)
	block := types.CheckpointCompatibilityMicroVMPrefix + strings.Repeat("e", 64)
	legacy := types.CheckpointCompatibilityLegacyPrefix + strings.Repeat("f", 64)
	for _, key := range []string{metadata, block, legacy, "", "microvm-v8:", "microvm-v8:" + strings.Repeat("z", 64)} {
		worker := &types.Worker{Runtime: "microvm", Status: types.WorkerStatusAvailable, CheckpointCompatibilityKey: key}
		checkpoint := &types.Checkpoint{Runtime: "microvm", CompatibilityKey: metadata}
		require.Equal(t, key == metadata, hostMatchesCheckpoint(checkpoint, worker), key)
		checkpoint.CompatibilityKey = ""
		require.Equal(t, key == legacy, hostMatchesCheckpoint(checkpoint, worker), "keyless memory must not reach v7/v8 workers")
	}
}
