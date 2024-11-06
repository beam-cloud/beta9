package bot

import (
	"context"
	"errors"

	"github.com/beam-cloud/beta9/pkg/types"
)

type BotTask struct {
	msg *types.TaskMessage
	pbs *PetriBotService
}

func (t *BotTask) Execute(ctx context.Context, options ...interface{}) error {
	instance, err := t.pbs.getOrCreateBotInstance(t.msg.StubId)
	if err != nil {
		return err
	}

	_, err = t.pbs.backendRepo.CreateTask(ctx, &types.TaskParams{
		TaskId:      t.msg.TaskId,
		StubId:      instance.stub.Id,
		WorkspaceId: instance.stub.WorkspaceId,
	})
	if err != nil {
		return err
	}

	// TODO: need to think about the logic of how a task is executed.
	// Does each task execute its own container? If so, its fairly straightforward

	instance.run("transitionName", "sessionId")
	return nil
}

func (t *BotTask) Retry(ctx context.Context) error {
	return errors.New("retry not implemented")
}

func (t *BotTask) HeartBeat(ctx context.Context) (bool, error) {
	return true, nil
}

func (t *BotTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	task, err := t.pbs.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	switch reason {
	case types.TaskExpired:
		task.Status = types.TaskStatusExpired
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	default:
		task.Status = types.TaskStatusError
	}

	_, err = t.pbs.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *BotTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		TaskId:        t.msg.TaskId,
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
	}
}
