package function

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	taskdispatcher "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type schedulingFailureBackend struct {
	repository.BackendRepository
	task *types.TaskWithRelated
}

func (r *schedulingFailureBackend) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return r.task, nil
}

func (r *schedulingFailureBackend) UpdateTask(_ context.Context, _ string, task types.Task) (*types.Task, error) {
	r.task.Task = task
	return &task, nil
}

func TestContainerSchedulingFailureEndsTaskAndClearsDispatcherState(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	taskRepo := repository.NewTaskRedisRepository(rdb)
	dispatcher, err := taskdispatcher.NewDispatcher(ctx, taskRepo)
	require.NoError(t, err)

	message := &types.TaskMessage{TaskId: "task-id", StubId: "stub-id", WorkspaceName: "workspace"}
	encoded, err := message.Encode()
	require.NoError(t, err)
	require.NoError(t, taskRepo.SetTaskState(ctx, "workspace", "stub-id", "task-id", encoded))

	backend := &schedulingFailureBackend{task: &types.TaskWithRelated{
		Task: types.Task{
			ExternalId:  "task-id",
			ContainerId: "container-id",
			Status:      types.TaskStatusPending,
		},
		Workspace: types.Workspace{Name: "workspace"},
		Stub:      types.Stub{ExternalId: "stub-id"},
	}}
	service := &ContainerFunctionService{backendRepo: backend, taskDispatcher: dispatcher}

	handled := service.handleContainerSchedulingFailure(common.NewContainerSchedulingFailedEvent(common.ContainerSchedulingFailure{
		TaskID:       "task-id",
		ContainerID:  "container-id",
		PoolSelector: "my-pool",
		Reason:       "worker_capacity_timeout",
	}))

	require.True(t, handled)
	require.Equal(t, types.TaskStatusError, backend.task.Status)
	require.True(t, backend.task.EndedAt.Valid)
	require.Equal(t, "No compatible worker in pool \"my-pool\" became available before scheduling timed out. Check that a machine is online and has enough CPU, memory, and GPU capacity.", backend.task.FailureReason)
	_, err = taskRepo.GetTaskState(ctx, "workspace", "stub-id", "task-id")
	require.Error(t, err)
}

func TestContainerSchedulingFailureIgnoresStaleContainer(t *testing.T) {
	backend := &schedulingFailureBackend{task: &types.TaskWithRelated{
		Task: types.Task{ExternalId: "task-id", ContainerId: "new-container", Status: types.TaskStatusPending},
	}}
	service := &ContainerFunctionService{backendRepo: backend}

	handled := service.handleContainerSchedulingFailure(common.NewContainerSchedulingFailedEvent(common.ContainerSchedulingFailure{
		TaskID:      "task-id",
		ContainerID: "old-container",
		Reason:      "worker_capacity_timeout",
	}))

	require.True(t, handled)
	require.Equal(t, types.TaskStatusPending, backend.task.Status)
}
