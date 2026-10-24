package function

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	taskdispatcher "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type completedRetryBackend struct{ repository.BackendRepository }

func (completedRetryBackend) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return &types.TaskWithRelated{Task: types.Task{ExternalId: "task", Status: types.TaskStatusComplete}}, nil
}

func TestRetryCleansUpCompletedFunctionWithoutReexecuting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	tasks := repository.NewTaskRedisRepository(rdb)
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
	dispatcher, err := taskdispatcher.NewDispatcher(ctx, tasks)
	require.NoError(t, err)
	function := &FunctionTask{
		msg: &types.TaskMessage{TaskId: "task", WorkspaceName: "workspace", StubId: "stub"},
		fs:  &ContainerFunctionService{backendRepo: completedRetryBackend{}, taskDispatcher: dispatcher},
	}
	// No scheduler or stub lookup is configured: user code must never run again.
	require.NoError(t, function.Retry(ctx))
	inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub")
	require.NoError(t, err)
	require.Zero(t, inFlight)
}

type heartbeatBackend struct {
	repository.BackendRepository
	task *types.Task
}

func (r heartbeatBackend) GetTask(context.Context, string) (*types.Task, error) { return r.task, nil }

type heartbeatContainers struct {
	repository.ContainerRepository
	state *types.ContainerState
	err   error
}

func (r heartbeatContainers) GetContainerState(string) (*types.ContainerState, error) {
	return r.state, r.err
}

func TestMissingGatewayHeartbeatDoesNotRetryRunningContainer(t *testing.T) {
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	for _, test := range []struct {
		name  string
		state *types.ContainerState
		err   error
		alive bool
	}{
		{"running", &types.ContainerState{Status: types.ContainerStatusRunning}, nil, true},
		{"stopped", &types.ContainerState{Status: types.ContainerStatusStopping}, nil, false},
		{"removed", nil, &types.ErrContainerStateNotFound{ContainerId: "container"}, false},
		{"repository error", nil, errors.New("redis unavailable"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := &FunctionTask{msg: &types.TaskMessage{TaskId: "task", WorkspaceName: "workspace"}, fs: &ContainerFunctionService{
				rdb: rdb,
				backendRepo: heartbeatBackend{task: &types.Task{
					Status: types.TaskStatusRunning, ContainerId: "container",
					StartedAt: types.NullTime{Time: time.Now().Add(-2 * time.Minute), Valid: true},
				}},
				containerRepo: heartbeatContainers{state: test.state, err: test.err},
			}}
			alive, err := task.HeartBeat(context.Background())
			if test.err != nil && !(&types.ErrContainerStateNotFound{}).From(test.err) {
				require.ErrorIs(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.alive, alive)
		})
	}
}

func TestScheduledRetryDoesNotConsumeAnotherAttemptBeforeStartTask(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	for _, state := range []types.ContainerStatus{types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping} {
		t.Run(string(state), func(t *testing.T) {
			task := &FunctionTask{msg: &types.TaskMessage{TaskId: "task"}, fs: &ContainerFunctionService{
				rdb:           rdb,
				backendRepo:   heartbeatBackend{task: &types.Task{Status: types.TaskStatusRetry, ContainerId: "retry-container"}},
				containerRepo: heartbeatContainers{state: &types.ContainerState{Status: state}},
			}}
			alive, err := task.HeartBeat(context.Background())
			require.NoError(t, err)
			require.Equal(t, state != types.ContainerStatusStopping, alive)
		})
	}
}
