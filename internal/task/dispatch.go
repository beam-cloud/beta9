package task

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

func NewDispatcher(ctx context.Context, taskRepo repository.TaskRepository) (*Dispatcher, error) {
	d := &Dispatcher{
		taskRepo:  taskRepo,
		executors: common.NewSafeMap[func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)](),
	}

	go d.monitor(ctx)
	return d, nil
}

type Dispatcher struct {
	taskRepo  repository.TaskRepository
	executors *common.SafeMap[func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)]
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
	return msg
}

func (d *Dispatcher) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (d *Dispatcher) Register(executor string, taskFactory func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)) {
	d.executors.Set(executor, taskFactory)
}

func (d *Dispatcher) Send(ctx context.Context, executor string, workspaceName, stubId string, payload *types.TaskPayload, policy types.TaskPolicy) (types.TaskInterface, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.Executor = executor
	taskMessage.WorkspaceName = workspaceName
	taskMessage.StubId = stubId
	taskMessage.Args = payload.Args
	taskMessage.Kwargs = payload.Kwargs
	taskMessage.Policy = policy

	taskFactory, exists := d.executors.Get(executor)
	if !exists {
		return nil, fmt.Errorf("invalid task executor: %v", executor)
	}

	defer d.releaseTaskMessage(taskMessage)
	task, err := taskFactory(ctx, taskMessage)
	if err != nil {
		return nil, err
	}

	msg, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}

	taskId := task.Metadata().TaskId

	err = d.taskRepo.SetTaskState(ctx, workspaceName, stubId, taskId, msg)
	if err != nil {
		return nil, err
	}

	err = task.Execute(ctx)
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
				claimed, err := d.taskRepo.IsClaimed(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
				if err != nil {
					continue
				}

				if !claimed {
					continue
				}

				log.Println("claimed...")

				taskFactory, exists := d.executors.Get(taskMessage.Executor)
				if !exists {
					d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
					continue
				}

				task, err := taskFactory(ctx, taskMessage)
				if err != nil {
					continue
				}

				heartbeat, err := task.HeartBeat(ctx)
				if err != nil {
					continue
				}

				if !heartbeat && claimed {

					// Hit retry limit, cancel task and resolve
					if taskMessage.Retries >= taskMessage.Policy.MaxRetries {
						log.Printf("<dispatcher> hit retry limit, not reinserting task <%s> into queue: %s\n", taskMessage.TaskId, taskMessage.StubId)

						err := task.Cancel(ctx)
						if err != nil {
							continue
						}

						err = d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
						if err != nil {
							continue
						}
					}

					// Retry task
					log.Printf("<dispatcher> missing heartbeat, reinserting task<%s:%s> into queue: %s\n",
						taskMessage.WorkspaceName, taskMessage.TaskId, taskMessage.StubId)

					taskMessage.Retries += 1
					msg, err := taskMessage.Encode()
					if err != nil {
						continue
					}

					err = d.taskRepo.SetTaskState(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId, msg)
					if err != nil {
						continue
					}

					err = task.Retry(ctx)
					if err != nil {
						log.Printf("<dispatcher> retry failed: %+v\n", err)
						continue
					}
				}
			}
		}
	}
}
