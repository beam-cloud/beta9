package task

import (
	"context"
	"sync"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

func NewDispatcher(rdb *common.RedisClient) (*Dispatcher, error) {
	return &Dispatcher{
		rdb: rdb,
	}, nil
}

type Dispatcher struct {
	rdb *common.RedisClient
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &types.TaskMessage{
			ID:     uuid.Must(uuid.NewV4()).String(),
			Args:   nil,
			Kwargs: nil,
		}
	},
}

func (d *Dispatcher) getTaskMessage() *types.TaskMessage {
	msg := taskMessagePool.Get().(*types.TaskMessage)
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	return msg
}

func (d *Dispatcher) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (d *Dispatcher) Register() {

}

func (d *Dispatcher) Send(ctx context.Context, args []interface{}, kwargs map[string]interface{}, taskFactory func(ctx context.Context, message *types.TaskMessage) (types.TaskInterface, error)) (types.TaskInterface, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.Args = args
	taskMessage.Kwargs = kwargs

	defer d.releaseTaskMessage(taskMessage)
	task, err := taskFactory(ctx, taskMessage)
	if err != nil {
		return nil, err
	}

	return task, nil
}
