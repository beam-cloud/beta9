package taskqueue

import (
	"context"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
)

type TaskQueueTask struct {
	msg *types.TaskMessage
	tq  *RedisTaskQueue
}

func (t *TaskQueueTask) Execute(ctx context.Context, options ...interface{}) error {
	authInfo := options[0].(*auth.AuthInfo)

	instance, err := t.tq.getOrCreateQueueInstance(t.msg.StubId)
	if err != nil {
		return err
	}

	var externalWorkspaceId *uint
	if instance.StubConfig.Pricing != nil && instance.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		abstractions.TrackTaskCount(instance.Stub, t.tq.usageMetricsRepo, t.msg.TaskId, authInfo.Workspace.ExternalId)
		externalWorkspaceId = &authInfo.Workspace.Id
	}

	_, err = t.tq.backendRepo.CreateTask(ctx, &types.TaskParams{
		TaskId:              t.msg.TaskId,
		StubId:              instance.Stub.Id,
		WorkspaceId:         instance.Stub.WorkspaceId,
		ExternalWorkspaceId: externalWorkspaceId,
	})
	if err != nil {
		return err
	}

	err = t.tq.queueClient.Push(ctx, t.msg)
	if err != nil {
		t.tq.backendRepo.DeleteTask(context.TODO(), t.msg.TaskId)
		return err
	}

	return nil
}

func (t *TaskQueueTask) Retry(ctx context.Context) error {
	_, err := t.tq.getOrCreateQueueInstance(t.msg.StubId)
	if err != nil {
		return err
	}

	task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	task.Status = types.TaskStatusRetry
	_, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return t.tq.queueClient.Push(ctx, t.msg)
}

func (t *TaskQueueTask) HeartBeat(ctx context.Context) (bool, error) {
	res, err := t.tq.rdb.Exists(ctx, Keys.taskQueueTaskHeartbeat(t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)).Result()
	if err != nil {
		return false, err
	}

	return res > 0, nil
}

func (t *TaskQueueTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	// Don't update tasks that are already in a terminal state
	if task.Status.IsCompleted() {
		return nil
	}

	switch reason {
	case types.TaskExpired:
		task.Status = types.TaskStatusExpired
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	default:
		task.Status = types.TaskStatusError
	}

	_, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *TaskQueueTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		TaskId:        t.msg.TaskId,
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
	}
}

func (t *TaskQueueTask) Message() *types.TaskMessage {
	return t.msg
}
