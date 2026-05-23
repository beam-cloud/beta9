package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types/trace"
	"github.com/stretchr/testify/require"
)

func TestTraceRedisRepositoryRecordsContainerTrace(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewTraceRedisRepository(rdb)
	ctx := context.Background()
	containerID := "container-123"
	taskID := "task-123"
	success := true

	err = repo.RecordEvent(ctx, trace.Event{
		ID:          trace.EventRuntimeExited,
		ContainerID: containerID,
		StubID:      "stub-123",
		StubType:    "function",
		TaskID:      taskID,
		WorkspaceID: "workspace-123",
		Reason:      "",
		Source:      "test",
		Message:     "runtime exited",
		Attrs: map[string]string{
			"authorization": "Bearer secret",
		},
	})
	require.NoError(t, err)

	err = repo.RecordSpan(ctx, trace.Span{
		ID:          trace.SpanImageLoad,
		StartTime:   time.Now().Add(-150 * time.Millisecond),
		EndTime:     time.Now(),
		DurationMs:  150,
		ContainerID: containerID,
		TaskID:      taskID,
		WorkspaceID: "workspace-123",
		Success:     &success,
	})
	require.NoError(t, err)

	err = repo.RecordLog(ctx, trace.LogEntry{
		ContainerID: containerID,
		TaskID:      taskID,
		Stream:      "stdout",
		Line:        "hello",
	})
	require.NoError(t, err)

	result, err := repo.GetContainerTrace(ctx, containerID)
	require.NoError(t, err)
	require.Equal(t, containerID, result.ContainerID)
	require.Equal(t, taskID, result.TaskID)
	require.Equal(t, "workspace-123", result.WorkspaceID)
	require.Equal(t, "UNKNOWN", result.StopReason)
	require.Equal(t, string(trace.EventRuntimeExited), result.RootCauseEvent)
	require.Equal(t, int64(150), result.Summary["image_ms"])
	require.Len(t, result.Events, 1)
	require.Equal(t, "[redacted]", result.Events[0].Attrs["authorization"])
	require.Len(t, result.Spans, 1)
	require.Len(t, result.Logs.Tail, 1)
	require.Equal(t, "hello", result.Logs.Tail[0].Line)

	indexedContainerID, err := rdb.Get(ctx, common.RedisKeys.TraceTaskContainerIndex(taskID)).Result()
	require.NoError(t, err)
	require.Equal(t, containerID, indexedContainerID)
}

func TestTraceRedisRepositoryPreservesFirstRootCause(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewTraceRedisRepository(rdb)
	ctx := context.Background()
	containerID := "container-123"

	err = repo.RecordEvent(ctx, trace.Event{
		ID:          trace.EventTaskCancelRequested,
		ContainerID: containerID,
		Reason:      string(trace.ReasonUnknown),
	})
	require.NoError(t, err)

	err = repo.RecordEvent(ctx, trace.Event{
		ID:          trace.EventRuntimeExited,
		ContainerID: containerID,
		Reason:      string(trace.ReasonUnknown),
	})
	require.NoError(t, err)

	result, err := repo.GetContainerTrace(ctx, containerID)
	require.NoError(t, err)
	require.Equal(t, string(trace.EventTaskCancelRequested), result.RootCauseEvent)
	require.Len(t, result.Events, 2)
}
