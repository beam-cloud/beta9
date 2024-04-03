package types

import (
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
}

type TaskInterface interface {
	Execute() error
	Cancel() error
	Update() error
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
	TaskId   string                 `json:"task_id"`
	StubId   string                 `json:"stub_id"`
	Executor string                 `json:"executor"`
	Args     []interface{}          `json:"args"`
	Kwargs   map[string]interface{} `json:"kwargs"`
	Expires  *time.Time             `json:"expires"`
}

func (tm *TaskMessage) Reset() {
	tm.TaskId = uuid.Must(uuid.NewV4()).String()
	tm.StubId = ""
	tm.Args = nil
	tm.Kwargs = nil
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
