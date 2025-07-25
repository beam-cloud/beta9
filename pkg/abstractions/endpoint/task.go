package endpoint

import (
	"context"
	"fmt"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type EndpointTask struct {
	msg *types.TaskMessage
	es  *HttpEndpointService
}

func (t *EndpointTask) Execute(ctx context.Context, options ...interface{}) error {
	var err error = nil

	echoCtx := options[0].(echo.Context)
	authInfo := options[1].(*auth.AuthInfo)

	instance, err := t.es.getOrCreateEndpointInstance(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	var externalWorkspaceId *uint
	if instance.StubConfig.Pricing != nil {
		abstractions.TrackTaskCount(instance.Stub, t.es.usageMetricsRepo, t.msg.TaskId, authInfo.Workspace.ExternalId)

		if instance.Workspace.ExternalId != authInfo.Workspace.ExternalId {
			externalWorkspaceId = &authInfo.Workspace.Id
		}
	}

	_, err = t.es.backendRepo.CreateTask(context.Background(), &types.TaskParams{
		TaskId:              t.msg.TaskId,
		StubId:              instance.Stub.Id,
		WorkspaceId:         instance.Stub.WorkspaceId,
		ExternalWorkspaceId: externalWorkspaceId,
	})
	if err != nil {
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

	heartbeatKey := Keys.endpointRequestHeartbeat(t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId, task.ContainerId)
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

func (t *EndpointTask) Message() *types.TaskMessage {
	return t.msg
}
