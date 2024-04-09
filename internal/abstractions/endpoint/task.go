package endpoint

import (
	"context"
	"log"

	"github.com/beam-cloud/beta9/internal/types"
)

type EndpointTask struct {
	msg *types.TaskMessage
	es  *HttpEndpointService
}

func (t *EndpointTask) Execute(ctx context.Context) error {
	stub, err := t.es.backendRepo.GetStubByExternalId(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	log.Println("stub: ", stub)
	return nil
}

func (t *EndpointTask) Retry(ctx context.Context) error {
	stub, err := t.es.backendRepo.GetStubByExternalId(ctx, t.msg.StubId)
	if err != nil {
		return err
	}

	log.Println("stub: ", stub)
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
