package apiv1

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/stretchr/testify/require"
)

func TestTaskSaysWhatItWaitsFor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	container := func(id string, phase types.ContainerPendingPhase, reason types.ContainerPendingReason) types.ContainerState {
		return types.ContainerState{
			ContainerId:   id,
			Status:        types.ContainerStatusPending,
			PendingPhase:  phase,
			PendingReason: reason,
			PendingSince:  now.Unix() - 30,
		}
	}
	task := func(status types.TaskStatus, containerID string) *types.TaskWithRelated {
		return &types.TaskWithRelated{Task: types.Task{Status: status, ContainerId: containerID}}
	}
	own := container("function-1", types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	other := container("function-2", types.ContainerPendingProvisioning, types.ContainerPendingWorkerStarting)
	running := types.ContainerState{ContainerId: "endpoint-1", Status: types.ContainerStatusRunning}

	// A function call waits for its own container, whatever the stub's
	// other calls wait for.
	pending := taskPending(task(types.TaskStatusPending, "function-1"), []types.ContainerState{other, own}, now)
	require.Equal(t, types.ContainerPendingWaitingForCapacity, pending.Phase)
	require.Equal(t, types.ContainerPendingNoCapacity, pending.Reason)
	require.EqualValues(t, 30, pending.WaitedSeconds)
	require.Equal(t, "No capacity is available right now. Waiting for some.", pending.Message)

	require.Nil(t, taskPending(task(types.TaskStatusPending, "function-3"), []types.ContainerState{other, own}, now),
		"a container that is not there yet, or no longer, says nothing")

	// A task that any container of its stub may take waits for whichever
	// is closest to running.
	pending = taskPending(task(types.TaskStatusPending, ""), []types.ContainerState{own, other}, now)
	require.Equal(t, types.ContainerPendingProvisioning, pending.Phase)
	require.Nil(t, taskPending(task(types.TaskStatusPending, ""), []types.ContainerState{own, running}, now),
		"it waits its turn on the container that runs")
	require.Nil(t, taskPending(task(types.TaskStatusPending, ""), nil, now))

	// A task that is tried again waits like one that is tried first.
	require.NotNil(t, taskPending(task(types.TaskStatusRetry, "function-1"), []types.ContainerState{own}, now))
	for _, status := range []types.TaskStatus{types.TaskStatusRunning, types.TaskStatusComplete, types.TaskStatusError, types.TaskStatusCancelled} {
		require.Nil(t, taskPending(task(status, "function-1"), []types.ContainerState{own}, now), status)
	}
}

func TestTaskResponseCarriesWhatItWaitsForOnlyWhileItWaits(t *testing.T) {
	waiting := &types.TaskWithRelated{Task: types.Task{Status: types.TaskStatusPending}}
	waiting.Pending = &types.Pending{
		Phase:         types.ContainerPendingProvisioning,
		Reason:        types.ContainerPendingBackupWorkerStarting,
		WaitedSeconds: 12,
		Message:       types.ContainerPendingProvisioning.Message(types.ContainerPendingBackupWorkerStarting),
	}
	serialized, err := serializer.Serialize(waiting)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"phase":          types.ContainerPendingProvisioning,
		"reason":         types.ContainerPendingBackupWorkerStarting,
		"waited_seconds": int64(12),
		"message":        "The preferred capacity is unavailable. A machine is starting on backup capacity.",
	}, serialized.(map[string]interface{})["pending"])

	// A phase with no reason has no reason key.
	waiting.Pending = &types.Pending{Phase: types.ContainerPendingQueued, Message: "Waiting to be placed on a machine."}
	serialized, err = serializer.Serialize(waiting)
	require.NoError(t, err)
	require.NotContains(t, serialized.(map[string]interface{})["pending"], "reason")

	started := &types.TaskWithRelated{Task: types.Task{Status: types.TaskStatusRunning}}
	serialized, err = serializer.Serialize(started)
	require.NoError(t, err)
	require.NotContains(t, serialized.(map[string]interface{}), "pending")
}
