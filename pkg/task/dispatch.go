package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/gofrs/uuid"
	"github.com/rs/zerolog/log"
)

func GetTaskResultPath(taskId string) string {
	return fmt.Sprintf("task/%s/result", taskId)
}

func NewDispatcher(ctx context.Context, taskRepo repository.TaskRepository) (*Dispatcher, error) {
	d := &Dispatcher{
		ctx:                ctx,
		taskRepo:           taskRepo,
		executors:          common.NewSafeMap[func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)](),
		storageClientCache: sync.Map{},
	}

	go d.monitor(ctx)
	return d, nil
}

type Dispatcher struct {
	ctx                context.Context
	taskRepo           repository.TaskRepository
	executors          *common.SafeMap[func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)]
	storageClientCache sync.Map
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &types.TaskMessage{
			TaskId:        uuid.Must(uuid.NewV4()).String(),
			Args:          nil,
			Kwargs:        nil,
			Executor:      "",
			StubId:        "",
			WorkspaceName: "",
		}
	},
}

func (d *Dispatcher) getTaskMessage() *types.TaskMessage {
	msg := taskMessagePool.Get().(*types.TaskMessage)
	msg.TaskId = uuid.Must(uuid.NewV4()).String()
	msg.StubId = ""
	msg.WorkspaceName = ""
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.Executor = ""
	msg.Timestamp = time.Now().Unix()
	return msg
}

func (d *Dispatcher) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (d *Dispatcher) Register(executor string, taskFactory func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)) {
	d.executors.Set(executor, taskFactory)
}

func (d *Dispatcher) SendAndExecute(ctx context.Context, executor string, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload, policy types.TaskPolicy, options ...interface{}) (types.TaskInterface, error) {
	task, err := d.Send(ctx, executor, authInfo, stubId, payload, policy)
	if err != nil {
		return nil, err
	}

	return task, task.Execute(ctx, options...)
}

func (d *Dispatcher) Send(ctx context.Context, executor string, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload, policy types.TaskPolicy) (types.TaskInterface, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.Executor = executor
	taskMessage.WorkspaceName = authInfo.Workspace.Name
	taskMessage.StubId = stubId
	taskMessage.Args = payload.Args
	taskMessage.Kwargs = payload.Kwargs
	taskMessage.Policy = policy
	taskMessage.Timestamp = time.Now().Unix()

	taskFactory, exists := d.executors.Get(executor)
	if !exists {
		return nil, fmt.Errorf("invalid task executor: %v", executor)
	}

	defer d.releaseTaskMessage(taskMessage)
	task, err := taskFactory(ctx, *taskMessage)
	if err != nil {
		return nil, err
	}

	msg, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}

	taskId := task.Metadata().TaskId

	err = d.taskRepo.SetTaskState(ctx, authInfo.Workspace.Name, stubId, taskId, msg)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (d *Dispatcher) StoreTaskResult(workspace *types.Workspace, taskId string, result []byte) error {
	var err error
	var storageClient *clients.WorkspaceStorageClient

	if workspace.StorageAvailable() {
		if cachedStorageClient, ok := d.storageClientCache.Load(workspace.Name); ok {
			storageClient = cachedStorageClient.(*clients.WorkspaceStorageClient)
		} else {
			storageClient, err = clients.NewWorkspaceStorageClient(d.ctx, workspace.Name, workspace.Storage)
			if err != nil {
				return err
			}

			d.storageClientCache.Store(workspace.Name, storageClient)
		}

		fullPath := GetTaskResultPath(taskId)
		err = storageClient.Upload(context.Background(), fullPath, result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Dispatcher) Retrieve(ctx context.Context, workspaceName, stubId, taskId string) (types.TaskInterface, error) {
	taskMessage, err := d.taskRepo.GetTaskState(ctx, workspaceName, stubId, taskId)
	if err != nil {
		return nil, err
	}

	taskFactory, exists := d.executors.Get(taskMessage.Executor)
	if !exists {
		return nil, fmt.Errorf("invalid task executor: %v", taskMessage.Executor)
	}

	task, err := taskFactory(ctx, *taskMessage)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (d *Dispatcher) Complete(ctx context.Context, workspaceName, stubId, taskId string) error {
	return d.taskRepo.DeleteTaskState(ctx, workspaceName, stubId, taskId)
}

func (d *Dispatcher) Claim(ctx context.Context, workspaceName, stubId, taskId, containerId string) error {
	return d.taskRepo.ClaimTask(ctx, workspaceName, stubId, taskId, containerId)
}

func (d *Dispatcher) monitor(ctx context.Context) {
	monitorRate := time.Duration(5) * time.Second
	ticker := time.NewTicker(monitorRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := d.taskRepo.GetTasksInFlight(ctx)
			if err != nil {
				continue
			}

			for _, taskMessage := range tasks {
				taskFactory, exists := d.executors.Get(taskMessage.Executor)
				if !exists {
					d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
					continue
				}

				task, err := taskFactory(ctx, *taskMessage)
				if err != nil {
					continue
				}

				claimed, err := d.taskRepo.IsClaimed(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
				if err != nil {
					continue
				}

				if !claimed {
					if time.Now().After(taskMessage.Policy.Expires) {
						err = task.Cancel(ctx, types.TaskExpired)
						if err != nil {
							log.Error().Str("task_id", task.Metadata().TaskId).Err(err).Msg("dispatcher unable to cancel task")
						}

						d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
					}

					continue
				}

				heartbeat, err := task.HeartBeat(ctx)
				if err != nil {
					continue
				}

				if !heartbeat {
					d.RetryTask(ctx, task)
					continue
				}
			}
		}
	}
}

func (d *Dispatcher) RetryTask(ctx context.Context, task types.TaskInterface) error {
	taskMessage := task.Message()

	err := d.taskRepo.SetTaskRetryLock(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
	if err != nil {
		return err
	}
	defer d.taskRepo.RemoveTaskRetryLock(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)

	// Hit retry limit, cancel task and resolve
	if taskMessage.Retries >= taskMessage.Policy.MaxRetries {
		if taskMessage.Policy.MaxRetries > 0 {
			log.Info().Str("task_id", taskMessage.TaskId).Str("stub_id", taskMessage.StubId).Msg("dispatcher hit retry limit, not reinserting task")
		}

		err = task.Cancel(ctx, types.TaskExceededRetryLimit)
		if err != nil {
			log.Error().Str("task_id", task.Metadata().TaskId).Err(err).Msg("dispatcher unable to cancel task")
			return err
		}

		return d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
	}

	// Remove task claim so other replicas of Dispatcher don't try to retry the same task
	err = d.taskRepo.RemoveTaskClaim(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
	if err != nil {
		log.Error().Str("task_id", task.Metadata().TaskId).Err(err).Msg("dispatcher failed to remove task claim")
		return err
	}

	// Retry task
	log.Info().Str("workspace_name", taskMessage.WorkspaceName).Str("task_id", taskMessage.TaskId).Str("stub_id", taskMessage.StubId).Msg("dispatcher missing heartbeat, reinserting task")

	taskMessage.Retries += 1
	taskMessage.Timestamp = time.Now().Unix()

	msg, err := taskMessage.Encode()
	if err != nil {
		return err
	}

	err = d.taskRepo.SetTaskState(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId, msg)
	if err != nil {
		return err
	}

	err = task.Retry(ctx)
	if err != nil {
		log.Error().Err(err).Msg("dispatcher retry failed")
		return err
	}

	return nil
}
