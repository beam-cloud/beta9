package bot

import (
	"context"
	"errors"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mitchellh/mapstructure"
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

	transitionName := t.msg.Kwargs["transition_name"].(string)
	sessionId := t.msg.Kwargs["session_id"].(string)

	// Try to cast the markers to a slice of Marker
	var markers []Marker
	rawMarkers, ok := t.msg.Kwargs["markers"].([]Marker)
	if !ok {
		// If that fails, manually decode using mapstructure
		rawMarkersInterface, ok := t.msg.Kwargs["markers"].([]interface{})
		if !ok {
			return errors.New("invalid markers format")
		}

		markers = make([]Marker, len(rawMarkersInterface))
		for i, rawMarker := range rawMarkersInterface {
			err := mapstructure.Decode(rawMarker, &markers[i])
			if err != nil {
				return err
			}
		}
	} else {
		markers = rawMarkers
	}

	err = t.pbs.botStateManager.pushTask(instance.workspace.Name, instance.stub.ExternalId, sessionId, transitionName, t.msg.TaskId, markers)
	if err != nil {
		return err
	}

	return instance.run(transitionName, sessionId, t.msg.TaskId)
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

func (t *BotTask) Message() *types.TaskMessage {
	return t.msg
}
