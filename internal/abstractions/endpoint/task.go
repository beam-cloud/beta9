package endpoint

import (
	"context"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type EndpointTask struct {
	msg *types.TaskMessage
	es  *HttpEndpointService
}

func (t *EndpointTask) Execute(ctx context.Context, options ...interface{}) error {
	echoCtx := options[0].(echo.Context)
	instance, exists := t.es.endpointInstances.Get(t.msg.StubId)
	if !exists {
		err := t.es.createEndpointInstance(t.msg.StubId)
		if err != nil {
			return err
		}

		instance, _ = t.es.endpointInstances.Get(t.msg.StubId)
	}

	_, err := t.es.backendRepo.CreateTask(echoCtx.Request().Context(), &types.TaskParams{
		TaskId:      t.msg.TaskId,
		StubId:      instance.stub.Id,
		WorkspaceId: instance.stub.WorkspaceId,
	})
	if err != nil {
		return err
	}

	return instance.buffer.ForwardRequest(echoCtx, &types.TaskPayload{
		Args:   t.msg.Args,
		Kwargs: t.msg.Kwargs,
	})
}

func (t *EndpointTask) Retry(ctx context.Context) error {
	task, err := t.es.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	task.Status = types.TaskStatusError
	_, err = t.es.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *EndpointTask) Cancel(ctx context.Context) error {
	task, err := t.es.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	task.Status = types.TaskStatusError
	_, err = t.es.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *EndpointTask) HeartBeat(ctx context.Context) (bool, error) {
	// Endpoints don't support retries, so heartbeats are not required
	return false, nil
}

func (t *EndpointTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
		TaskId:        t.msg.TaskId,
	}
}
