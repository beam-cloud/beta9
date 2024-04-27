package endpoint

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type EndpointTask struct {
	msg *types.TaskMessage
	es  *HttpEndpointService
}

func (t *EndpointTask) Execute(ctx context.Context, options ...interface{}) error {
	var err error = nil
	echoCtx := options[0].(echo.Context)
	instance, err := t.es.getOrCreateEndpointInstance(t.msg.StubId)
	if err != nil {
		return err
	}

	_, err = t.es.backendRepo.CreateTask(echoCtx.Request().Context(), &types.TaskParams{
		TaskId:      t.msg.TaskId,
		StubId:      instance.Stub.Id,
		WorkspaceId: instance.Stub.WorkspaceId,
	})
	if err != nil {
		return err
	}

	return instance.buffer.ForwardRequest(echoCtx, &types.TaskPayload{
		Args:   t.msg.Args,
		Kwargs: t.msg.Kwargs,
	}, t.msg.TaskId)
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

func (t *EndpointTask) Cancel(ctx context.Context, reason types.TaskCancellationReason) error {
	task, err := t.es.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	switch reason {
	case types.TaskExpired:
		task.Status = types.TaskStatusTimeout
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	default:
		task.Status = types.TaskStatusError
	}

	_, err = t.es.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *EndpointTask) HeartBeat(ctx context.Context) (bool, error) {
	heartbeatKey := Keys.endpointRequestHeartbeat(t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)
	exists, err := t.es.rdb.Exists(ctx, heartbeatKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to retrieve endpoint heartbeat key <%v>: %w", heartbeatKey, err)
	}

	return exists > 0, nil
}

func (t *EndpointTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
		TaskId:        t.msg.TaskId,
	}
}
