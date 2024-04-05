package types

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gofrs/uuid"
)

type TaskPayload struct {
	Args   []interface{}          `json:"args"`
	Kwargs map[string]interface{} `json:"kwargs"`
}

type TaskMetadata struct {
	TaskId        string
	StubId        string
	WorkspaceName string
	ContainerId   string
}

type TaskInterface interface {
	Execute(ctx context.Context) error
	Cancel(ctx context.Context) error
	Retry(ctx context.Context) error
	HeartBeat(ctx context.Context) (bool, error)
	Metadata() TaskMetadata
}

type TaskExecutor string

var (
	ExecutorTaskQueue TaskExecutor = "taskqueue"
	ExecutorEndpoint  TaskExecutor = "endpoint"
	ExecutorFunction  TaskExecutor = "function"
	ExecutorContainer TaskExecutor = "container"
)

// TaskMessage represents a JSON serializable message
// to be added to the queue
type TaskMessage struct {
	TaskId        string                 `json:"task_id"`
	WorkspaceName string                 `json:"workspace_name"`
	StubId        string                 `json:"stub_id"`
	Executor      string                 `json:"executor"`
	Args          []interface{}          `json:"args"`
	Kwargs        map[string]interface{} `json:"kwargs"`
	Expires       *time.Time             `json:"expires"`
	Policy        TaskPolicy             `json:"policy"`
	Retries       uint                   `json:"retries"`
}

func (tm *TaskMessage) Reset() {
	tm.WorkspaceName = ""
	tm.TaskId = uuid.Must(uuid.NewV4()).String()
	tm.StubId = ""
	tm.Args = nil
	tm.Kwargs = nil
	tm.Policy = DefaultTaskPolicy
	tm.Retries = 0
}

// Encode returns a binary representation of the TaskMessage
func (tm *TaskMessage) Encode() ([]byte, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}

	encodedData, err := json.Marshal(tm)
	if err != nil {
		return nil, err
	}

	return encodedData, err
}

// Decode initializes the TaskMessage fields from a byte array
func (tm *TaskMessage) Decode(encodedData []byte) error {
	err := json.Unmarshal(encodedData, tm)
	if err != nil {
		return err
	}

	return nil
}

var DefaultTaskPolicy = TaskPolicy{
	MaxRetries: 3,
	Timeout:    3600,
}

type TaskPolicy struct {
	MaxRetries uint `json:"max_retries"`
	Timeout    int  `json:"timeout"`
}
