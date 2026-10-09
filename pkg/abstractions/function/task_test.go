package function

import (
	"context"
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
}

func (r heartbeatContainers) GetContainerState(string) (*types.ContainerState, error) {
	return r.state, nil
}

func TestMissingGatewayHeartbeatDoesNotRetryRunningContainer(t *testing.T) {
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	for _, running := range []bool{true, false} {
		t.Run(map[bool]string{true: "running", false: "stopped"}[running], func(t *testing.T) {
			state := &types.ContainerState{Status: types.ContainerStatusStopping}
			if running {
				state.Status = types.ContainerStatusRunning
			}
			task := &FunctionTask{msg: &types.TaskMessage{TaskId: "task", WorkspaceName: "workspace"}, fs: &ContainerFunctionService{
				rdb: rdb,
				backendRepo: heartbeatBackend{task: &types.Task{
					Status: types.TaskStatusRunning, ContainerId: "container",
					StartedAt: types.NullTime{Time: time.Now().Add(-2 * time.Minute), Valid: true},
				}},
				containerRepo: heartbeatContainers{state: state},
			}}
			alive, err := task.HeartBeat(context.Background())
			require.NoError(t, err)
			require.Equal(t, running, alive)
		})
	}
}
