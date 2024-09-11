package bot

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/types"
)

type BotTask struct {
	msg *types.TaskMessage
	pbs *PetriBotService
}

func (t *BotTask) Execute(ctx context.Context, options ...interface{}) error {
	// instance, err := t.tq.getOrCreateQueueInstance(t.msg.StubId)
	// if err != nil {
	// 	return err
	// }

	// _, err := t.tq.backendRepo.CreateTask(ctx, &types.TaskParams{
	// 	TaskId:      t.msg.TaskId,
	// 	StubId:      instance.Stub.Id,
	// 	WorkspaceId: instance.Stub.WorkspaceId,
	// })
	// if err != nil {
	// 	return err
	// }

	// err = t.tq.queueClient.Push(ctx, t.msg)
	// if err != nil {
	// 	t.tq.backendRepo.DeleteTask(context.TODO(), t.msg.TaskId)
	// 	return err
	// }

	return nil
}

func (t *BotTask) Retry(ctx context.Context) error {
	// _, err := t.tq.getOrCreateQueueInstance(t.msg.StubId)
	// if err != nil {
	// 	return err
	// }

	// task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	// if err != nil {
	// 	return err
	// }

	// task.Status = types.TaskStatusRetry
	// _, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	// if err != nil {
	// 	return err
	// }

	// return t.tq.queueClient.Push(ctx, t.msg)
	return nil
}

func (t *BotTask) HeartBeat(ctx context.Context) (bool, error) {
	// res, err := t.tq.rdb.Exists(ctx, Keys.taskQueueTaskHeartbeat(t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)).Result()
	// if err != nil {
	// 	return false, err
	// }

	return true, nil // res > 0, nil
}

func (t *BotTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	// task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	// if err != nil {
	// 	return err
	// }

	// switch reason {
	// case types.TaskExpired:
	// 	task.Status = types.TaskStatusExpired
	// case types.TaskExceededRetryLimit:
	// 	task.Status = types.TaskStatusError
	// default:
	// 	task.Status = types.TaskStatusError
	// }

	// _, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (t *BotTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		TaskId:        t.msg.TaskId,
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
	}
}
