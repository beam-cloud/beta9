package pod

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPodRunTaskTerminalStatus(t *testing.T) {
	tests := []struct {
		name           string
		exitCode       types.ContainerExitCode
		expectedStatus types.TaskStatus
		expectReason   bool
	}{
		{name: "success", exitCode: types.ContainerExitCodeSuccess, expectedStatus: types.TaskStatusComplete, expectReason: false},
		{name: "stopped by user", exitCode: types.ContainerExitCodeUser, expectedStatus: types.TaskStatusCancelled, expectReason: true},
		{name: "stopped by admin", exitCode: types.ContainerExitCodeAdmin, expectedStatus: types.TaskStatusCancelled, expectReason: true},
		{name: "stopped by scheduler", exitCode: types.ContainerExitCodeScheduler, expectedStatus: types.TaskStatusCancelled, expectReason: true},
		{name: "ttl exceeded", exitCode: types.ContainerExitCodeTtl, expectedStatus: types.TaskStatusTimeout, expectReason: true},
		{name: "oom killed", exitCode: types.ContainerExitCodeOomKill, expectedStatus: types.TaskStatusError, expectReason: true},
		{name: "unknown non-zero exit", exitCode: types.ContainerExitCode(1), expectedStatus: types.TaskStatusError, expectReason: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, reason := podRunTaskTerminalStatus(tt.exitCode)
			require.Equal(t, tt.expectedStatus, status)
			if tt.expectReason {
				require.NotEmpty(t, reason)
			} else {
				require.Empty(t, reason)
			}
		})
	}
}

func TestPodRunTaskMappingRoundTrip(t *testing.T) {
	ctx := context.Background()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	service := &GenericPodService{rdb: rdb}
	msg := &types.TaskMessage{
		TaskId:        "task-123",
		WorkspaceName: "workspace-1",
		StubId:        "stub-abc",
	}

	// Missing mapping resolves to an error, not a zero-value mapping
	_, err = service.getPodRunTaskMapping(ctx, "pod-stub-abc-1234")
	require.Error(t, err)

	require.NoError(t, service.setPodRunTaskMapping(ctx, "pod-stub-abc-1234", msg))

	mapping, err := service.getPodRunTaskMapping(ctx, "pod-stub-abc-1234")
	require.NoError(t, err)
	require.Equal(t, "task-123", mapping.TaskId)
	require.Equal(t, "workspace-1", mapping.WorkspaceName)
	require.Equal(t, "stub-abc", mapping.StubId)

	// The mapping expires on its own if never cleaned up
	require.Greater(t, server.TTL(Keys.podRunTask("pod-stub-abc-1234")), time.Duration(0))

	service.deletePodRunTaskMapping(ctx, "pod-stub-abc-1234")
	_, err = service.getPodRunTaskMapping(ctx, "pod-stub-abc-1234")
	require.Error(t, err)
}

func TestTrackRunAsTask(t *testing.T) {
	withDispatcher := &GenericPodService{taskDispatcher: nil}
	runStub := &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodRun)}}
	deploymentStub := &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodDeployment)}}

	// No dispatcher wired -> never track
	require.False(t, withDispatcher.trackRunAsTask(runStub))

	service := &GenericPodService{taskDispatcher: &task.Dispatcher{}}
	require.True(t, service.trackRunAsTask(runStub))
	require.False(t, service.trackRunAsTask(deploymentStub))
}
