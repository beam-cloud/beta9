package task

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

func NewDispatcher(ctx context.Context, rdb *common.RedisClient) (*Dispatcher, error) {
	d := &Dispatcher{
		rdb:       rdb,
		executors: common.NewSafeMap[func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)](),
	}

	go d.monitor(ctx)
	return d, nil
}

type Dispatcher struct {
	rdb       *common.RedisClient
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

	err = d.setTaskState(ctx, workspaceName, taskId, msg)
	if err != nil {
		return nil, err
	}

	err = task.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (d *Dispatcher) Complete(ctx context.Context, workspaceName, taskId string) error {
	return d.removeTaskState(ctx, workspaceName, taskId)
}

func (d *Dispatcher) Claim(ctx context.Context, workspaceName, taskId, containerId string) error {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, taskId)
	err := d.rdb.Set(ctx, claimKey, containerId, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to claim task <%v>: %w", claimKey, err)
	}

	return nil
}

func (d *Dispatcher) setTaskState(ctx context.Context, workspaceName, taskId string, msg []byte) error {
	indexKey := common.RedisKeys.TaskIndex()
	entryKey := common.RedisKeys.TaskEntry(workspaceName, taskId)

	err := d.rdb.SAdd(ctx, indexKey, entryKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add task key to index <%v>: %w", indexKey, err)
	}

	err = d.rdb.Set(ctx, entryKey, msg, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to add task entry <%v>: %w", entryKey, err)
	}

	return nil

}

func (d *Dispatcher) removeTaskState(ctx context.Context, workspaceName, taskId string) error {
	indexKey := common.RedisKeys.TaskIndex()
	err := d.rdb.SRem(ctx, indexKey, taskId).Err()
	if err != nil {
		return err
	}

	entryKey := common.RedisKeys.TaskEntry(workspaceName, taskId)
	err = d.rdb.Del(ctx, entryKey).Err()
	if err != nil {
		return err
	}

	claimKey := common.RedisKeys.TaskClaim(workspaceName, taskId)
	err = d.rdb.Del(ctx, claimKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove claim <%v>: %w", claimKey, err)
	}

	return nil
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
			tasks, err := d.rdb.SMembers(ctx, common.RedisKeys.TaskIndex()).Result()
			if err != nil {
				continue
			}

			for _, taskKey := range tasks {
				msg, err := d.rdb.Get(ctx, taskKey).Bytes()
				if err != nil {
					continue
				}

				taskMessage := types.TaskMessage{}
				taskMessage.Decode(msg)

				taskFactory, exists := d.executors.Get(taskMessage.Executor)
				if !exists {
					d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.TaskId)
					continue
				}

				task, err := taskFactory(ctx, &taskMessage)
				if err != nil {
					continue
				}

				heartbeat, err := task.HeartBeat(ctx)
				if err != nil {
					continue
				}

				log.Printf("task %s, heartbeat: %v\n", taskMessage.TaskId, heartbeat)

				if !heartbeat {

					// Hit retry limit, cancel task and resolve
					if taskMessage.Retries >= taskMessage.Policy.MaxRetries {
						log.Printf("<dispatcher> hit retry limit, not reinserting task <%s> into queue: %s\n", taskMessage.TaskId, taskMessage.StubId)

						err := task.Cancel(ctx)
						if err != nil {
							continue
						}

						err = d.Complete(ctx, taskMessage.WorkspaceName, taskMessage.TaskId)
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

					err = d.setTaskState(ctx, taskMessage.WorkspaceName, taskMessage.TaskId, msg)
					if err != nil {
						continue
					}

					err = task.Retry(ctx)
					if err != nil {
						log.Printf("<dispatcher> retry failed: %+v\n", err)
					}
				}
			}
		}
	}
}
