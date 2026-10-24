package taskqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type completionBackend struct {
	repository.BackendRepository
	update func() error
}

func (b completionBackend) GetTask(context.Context, string) (*types.Task, error) {
	return &types.Task{ExternalId: "task", ContainerId: "container", Status: types.TaskStatusRunning}, nil
}

func (b completionBackend) GetTaskWithRelated(ctx context.Context, id string) (*types.TaskWithRelated, error) {
	task, err := b.GetTask(ctx, id)
	return &types.TaskWithRelated{Task: *task, Workspace: types.Workspace{Name: "workspace"}, Stub: types.Stub{ExternalId: "stub"}}, err
}

func (b completionBackend) UpdateTask(_ context.Context, _ string, updated types.Task) (*types.Task, error) {
	return &updated, b.update()
}

type completionTasks struct {
	repository.TaskRepository
	err error
}

func (r completionTasks) DeleteTaskState(ctx context.Context, workspace, stub, taskID string) error {
	if r.err != nil {
		return r.err
	}
	return r.TaskRepository.DeleteTaskState(ctx, workspace, stub, taskID)
}

func TestTaskQueueCompletionKeepsRolloutLeaseUntilSuccessfulFinalization(t *testing.T) {
	for _, failure := range []string{"duration", "dispatcher", "database", ""} {
		t.Run("failure="+failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}})
			rdb, err := repository.NewRedisClientForTest()
			require.NoError(t, err)
			leaseKey := Keys.taskQueueTaskRunningLock("workspace", "stub", "container", "task")
			indexKey := Keys.taskQueueTaskRunningLockIndex("workspace", "stub", "container")
			require.NoError(t, rdb.Set(ctx, leaseKey, 1, time.Minute).Err())
			require.NoError(t, rdb.SAdd(ctx, indexKey, "task").Err())
			tasks := completionTasks{TaskRepository: repository.NewTaskRedisRepository(rdb)}
			require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
			require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			if failure == "dispatcher" {
				tasks.err = errors.New("dispatcher unavailable")
			}
			dispatcher, err := task.NewDispatcher(ctx, tasks)
			require.NoError(t, err)
			backend := completionBackend{update: func() error {
				require.EqualValues(t, 1, rdb.Exists(ctx, leaseKey).Val(), "final database update must still own the container")
				if failure == "database" {
					return errors.New("database unavailable")
				}
				return nil
			}}
			if failure == "duration" {
				require.NoError(t, rdb.Set(ctx, Keys.taskQueueTaskDuration("workspace", "stub"), "wrong-type", time.Minute).Err())
			}
			queue := &RedisTaskQueue{config: completionTestConfig(), rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			response, err := queue.TaskQueueComplete(ctx, &pb.TaskQueueCompleteRequest{TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete)})
			if failure != "" {
				require.Equal(t, codes.Unavailable, status.Code(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, failure == "", response.GetOk())
			}
			require.Equal(t, failure != "", rdb.Exists(ctx, leaseKey).Val() == 1)
			require.Equal(t, failure != "", rdb.SIsMember(ctx, indexKey, "task").Val())
		})
	}
}

func TestTaskQueueRolloutStopsNewClaimsWithoutLosingQueuedTasks(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	client := newRedisTaskQueueClient(rdb, nil)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker", Status: types.WorkerStatusAvailable}
	require.NoError(t, workerRepo.AddWorker(worker))
	containerKey := common.RedisKeys.SchedulerContainerState("container")
	require.NoError(t, rdb.HSet(ctx, containerKey, "worker_id", worker.Id, "status", string(types.ContainerStatusRunning)).Err())
	for _, id := range []string{"active", "queued"} {
		require.NoError(t, client.Push(ctx, &types.TaskMessage{TaskId: id, WorkspaceName: "workspace", StubId: "stub"}))
	}
	message, err := client.Pop(ctx, "workspace", "stub", "container")
	require.NoError(t, err)
	require.NotEmpty(t, message)
	_, err = workerRepo.PrepareWorkerRollout(worker.Id, "next")
	require.NoError(t, err)
	message, err = client.Pop(ctx, "workspace", "stub", "container")
	require.NoError(t, err)
	require.Empty(t, message)
	require.EqualValues(t, 1, rdb.LLen(ctx, Keys.taskQueueList("workspace", "stub")).Val())
	require.EqualValues(t, 1, rdb.Exists(ctx, Keys.taskQueueTaskRunningLock("workspace", "stub", "container", "active")).Val(), "the running task keeps its lease")
	require.NoError(t, workerRepo.CancelWorkerRollout(worker.Id, "next"))
	message, err = client.Pop(ctx, "workspace", "stub", "container")
	require.NoError(t, err)
	require.NotEmpty(t, message, "cancelled rollout reopens admission")
	require.EqualValues(t, 0, rdb.Exists(ctx, Keys.taskQueueProcessingLock("workspace", "stub", "container")).Val())

	require.NoError(t, client.Push(ctx, &types.TaskMessage{TaskId: "after-stop", WorkspaceName: "workspace", StubId: "stub"}))
	require.NoError(t, rdb.HSet(ctx, containerKey, "status", string(types.ContainerStatusStopping)).Err())
	message, err = client.Pop(ctx, "workspace", "stub", "container")
	require.NoError(t, err)
	require.Empty(t, message, "STOPPING remains fenced even if the worker rollout was cancelled")
	require.EqualValues(t, 1, rdb.LLen(ctx, Keys.taskQueueList("workspace", "stub")).Val())
}

func TestDeploymentScaleFuncWithDefaults(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     1,
		TasksPerContainer: 1,
	}
	autoscaledInstance.AppConfig = types.AppConfig{}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}

	instance := &taskQueueInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	// Check for default scaling up behavior - scale to 1
	sample := &taskQueueAutoscalerSample{
		QueueLength: 10,
	}

	result := taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers)

	// Check for default scaling down behavior - scale to 0
	sample = &taskQueueAutoscalerSample{
		QueueLength: 0,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)

	// Check for invalid queue depth
	sample = &taskQueueAutoscalerSample{
		QueueLength: -1,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, false, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)
}

func TestDeploymentScaleFuncWithMaxTasksPerContainer(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     3,
		TasksPerContainer: 1,
	}
	autoscaledInstance.AppConfig = types.AppConfig{}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}

	// Make sure we scale up to max containers
	instance := &taskQueueInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	sample := &taskQueueAutoscalerSample{
		QueueLength: 3,
	}

	result := taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Ensure we don't exceed max containers
	sample = &taskQueueAutoscalerSample{
		QueueLength: 4,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure modulo operator makes sense
	sample = &taskQueueAutoscalerSample{
		QueueLength: 11,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure no modulo case works
	sample = &taskQueueAutoscalerSample{
		QueueLength: 10,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers)
}
