package function

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
