package task

import (
	"context"
	"fmt"
	"sync"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

func NewDispatcher(rdb *common.RedisClient) (*Dispatcher, error) {
	return &Dispatcher{
		rdb:       rdb,
		executors: common.NewSafeMap[func()](),
	}, nil
}

type Dispatcher struct {
	rdb       *common.RedisClient
	executors *common.SafeMap[func()]
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &types.TaskMessage{
			TaskId:   uuid.Must(uuid.NewV4()).String(),
			Args:     nil,
			Kwargs:   nil,
			Executor: "",
		}
	},
}

func (d *Dispatcher) getTaskMessage() *types.TaskMessage {
	msg := taskMessagePool.Get().(*types.TaskMessage)
	msg.TaskId = uuid.Must(uuid.NewV4()).String()
	msg.StubId = ""
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.Executor = ""
	return msg
}

func (d *Dispatcher) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (d *Dispatcher) Register(executor string, callback func()) {
	d.executors.Set(executor, callback)
}

func (d *Dispatcher) Send(ctx context.Context, stubId string, args []interface{}, kwargs map[string]interface{}, taskFactory func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)) (types.TaskInterface, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.StubId = stubId
	taskMessage.Args = args
	taskMessage.Kwargs = kwargs

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

	indexKey := common.RedisKeys.TaskIndex()
	err = d.rdb.SAdd(ctx, indexKey, taskId).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to add task key to index <%v>: %w", indexKey, err)
	}

	entryKey := common.RedisKeys.TaskEntry(taskId)
	err = d.rdb.Set(ctx, entryKey, msg, 0).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to add task entry <%v>: %w", indexKey, err)
	}

	err = task.Execute()
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (d *Dispatcher) Resolve(ctx context.Context, taskId string) error {
	indexKey := common.RedisKeys.TaskIndex()
	err := d.rdb.SRem(ctx, indexKey, taskId).Err()
	if err != nil {
		return err
	}

	entryKey := common.RedisKeys.TaskEntry(taskId)
	err = d.rdb.Del(ctx, entryKey).Err()
	if err != nil {
		return err
	}

	return nil
}

func (d *Dispatcher) retry(ctx context.Context) {

}
