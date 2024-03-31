package gateway

import (
	"sync"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/gofrs/uuid"
)

type Task interface {
	Replay() error
}

func NewDispatcher() (*Dispatcher, error) {
	return &Dispatcher{}, nil
}

type Dispatcher struct {
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

func (d *Dispatcher) Send(args []interface{}, kwargs map[string]interface{}) (Task, error) {
	taskMessage := d.getTaskMessage()
	taskMessage.Args = args
	taskMessage.Kwargs = kwargs

	defer d.releaseTaskMessage(taskMessage)
	_, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}

	// d.tf.Replay()

	return nil, nil
}

func (d *Dispatcher) Receive() (Task, error) {
	return nil, nil
}
