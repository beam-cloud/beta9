package task

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

func NewDispatcher(ctx context.Context, taskRepo repository.TaskRepository) (*Dispatcher, error) {
	d := &Dispatcher{
		taskRepo:  taskRepo,
		executors: common.NewSafeMap[func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)](),
	}

	go d.monitor(ctx)
	return d, nil
}

type Dispatcher struct {
	taskRepo  repository.TaskRepository
	executors *common.SafeMap[func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)]
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
	msg.Signature = ""
	msg.RequestedAt = time.Now()
	return msg
}

func (d *Dispatcher) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (d *Dispatcher) Register(executor string, taskFactory func(ctx context.Context, message types.TaskMessage) (types.TaskInterface, error)) {
	d.executors.Set(executor, taskFactory)
}

func (d *Dispatcher) SendAndExecute(ctx context.Context, executor string, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload, policy types.TaskPolicy) (types.TaskInterface, error) {
	task, err := d.Send(ctx, executor, authInfo, stubId, payload, policy)
	if err != nil {
		return nil, err
	}

	return task, task.Execute(ctx)
}

func (d *Dispatcher) sign(tm *types.TaskMessage, secretKey string) error {
	data := fmt.Sprintf("%s:%s:%v", tm.TaskId, tm.StubId, tm.RequestedAt)
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := h.Sum(nil)
	tm.Signature = hex.EncodeToString(signature)
	return nil
}

func (d *Dispatcher) Send(ctx context.Context, executor string, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload, policy types.TaskPolicy) (types.TaskInterface, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.Executor = executor
	taskMessage.WorkspaceName = authInfo.Workspace.Name
	taskMessage.StubId = stubId
	taskMessage.Args = payload.Args
	taskMessage.Kwargs = payload.Kwargs
	taskMessage.Policy = policy
	taskMessage.RequestedAt = time.Now()

	// Sign task message
	if authInfo.Workspace.SigningKey != nil && *authInfo.Workspace.SigningKey != "" {
		err := d.sign(taskMessage, *authInfo.Workspace.SigningKey)
		if err != nil {
			return nil, err
		}
	}

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
							log.Printf("<dispatcher> unable to cancel task: %s, %v\n", task.Metadata().TaskId, err)
						}

						d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
					}

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

						err := task.Cancel(ctx, types.TaskExceededRetryLimit)
						if err != nil {
							log.Printf("<dispatcher> unable to cancel task: %s, %v\n", task.Metadata().TaskId, err)
							continue
						}

						d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.StubId, taskMessage.TaskId)
						continue
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
