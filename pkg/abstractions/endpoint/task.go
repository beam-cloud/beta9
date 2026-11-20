package endpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type EndpointTask struct {
	msg *types.TaskMessage
	es  *HttpEndpointService
}

func (t *EndpointTask) Execute(ctx context.Context, options ...interface{}) error {
	var err error = nil

	echoCtx := options[0].(echo.Context)

	instance, err := t.es.getOrCreateEndpointInstance(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	_, err = t.es.backendRepo.CreateTask(context.Background(), &types.TaskParams{
		TaskId:      t.msg.TaskId,
		StubId:      instance.Stub.Id,
		WorkspaceId: instance.Stub.WorkspaceId,
	})
	if err != nil {
		// Send persisted pending state, but this request has not entered the buffer.
		// Release it even if the client canceled while task creation was running.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if cleanupErr := t.es.taskDispatcher.Complete(cleanupCtx, t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId); cleanupErr != nil {
			log.Error().Err(cleanupErr).Str("task_id", t.msg.TaskId).Msg("unable to clean up state after task creation failed")
		}
		return err
	}

	return instance.buffer.ForwardRequest(echoCtx, t)
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
	task, err := t.es.backendRepo.GetTask(context.Background(), t.msg.TaskId)
	if err != nil {
		return err
	}

	defer t.es.taskDispatcher.Complete(context.Background(), t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)

	// Don't update tasks that are already in a terminal state
	if task.Status.IsCompleted() {
		return nil
	}

	switch reason {
	case types.TaskExpired:
		// For endpoints, we set the task status to timeout on expiryfor clarity
		task.Status = types.TaskStatusTimeout
	case types.TaskExceededRetryLimit:
		task.Status = types.TaskStatusError
	case types.TaskRequestCancelled:
		task.Status = types.TaskStatusCancelled
	case types.TaskInvalidRequestPayload:
		task.Status = types.TaskStatusError
	default:
		task.Status = types.TaskStatusError
	}

	_, err = t.es.backendRepo.UpdateTask(context.Background(), t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *EndpointTask) HeartBeat(ctx context.Context) (bool, error) {
	task, err := t.es.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return false, err
	}

	exists, err := t.es.containerRepo.EndpointRequestHeartbeatExists(ctx, t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId, task.ContainerId)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve endpoint heartbeat: %w", err)
	}

	return exists, nil
}

func (t *EndpointTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
		TaskId:        t.msg.TaskId,
	}
}

func (t *EndpointTask) Message() *types.TaskMessage {
	return t.msg
}
