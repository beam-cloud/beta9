package gateway

import "github.com/beam-cloud/beta9/internal/repository"

type IncomingRequest struct {
}

func NewDispatcher() (*Dispatcher, error) {
	return &Dispatcher{}, nil
}

type Dispatcher struct {
	backendRepo repository.BackendRepository
}

func (d *Dispatcher) SendTask() error {
	return nil
}

// type TaskDispatcher interface {
// 	SendTask()
// 	CancelTask(taskId string) error
// 	QueueDepth()
// }
