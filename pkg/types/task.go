package types

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

type TaskCancellationReason string

const (
	TaskExpired               TaskCancellationReason = "expired"
	TaskExceededRetryLimit    TaskCancellationReason = "exceeded_retry_limit"
	TaskRequestCancelled      TaskCancellationReason = "request_cancelled"
	TaskInvalidRequestPayload TaskCancellationReason = "invalid_request_payload"
)

type TaskInterface interface {
	Execute(ctx context.Context, options ...interface{}) error
	Cancel(ctx context.Context, reason TaskCancellationReason) error
	Retry(ctx context.Context) error
	HeartBeat(ctx context.Context) (bool, error)
	Metadata() TaskMetadata
	Message() *TaskMessage
}

type TaskExecutor string

var (
	ExecutorTaskQueue TaskExecutor = "taskqueue"
	ExecutorEndpoint  TaskExecutor = "endpoint"
	ExecutorFunction  TaskExecutor = "function"
	ExecutorContainer TaskExecutor = "container"
	ExecutorBot       TaskExecutor = "bot"
)

// TaskMessage represents a JSON serializable message
// to be added to the queue
type TaskMessage struct {
	TaskId        string                 `json:"task_id" redis:"task_id"`
	WorkspaceName string                 `json:"workspace_name" redis:"workspace_name"`
	StubId        string                 `json:"stub_id" redis:"stub_id"`
	Executor      string                 `json:"executor" redis:"executor"`
	Args          []interface{}          `json:"args" redis:"args"`
	Kwargs        map[string]interface{} `json:"kwargs" redis:"kwargs"`
	Policy        TaskPolicy             `json:"policy" redis:"policy"`
	Retries       uint                   `json:"retries" redis:"retries"`
	Timestamp     int64                  `json:"timestamp" redis:"timestamp"`
}

func (tm *TaskMessage) Reset() {
	tm.WorkspaceName = ""
	tm.TaskId = uuid.Must(uuid.NewV4()).String()
	tm.StubId = ""
	tm.Args = nil
	tm.Kwargs = nil
	tm.Timestamp = time.Now().Unix()
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

	for i, arg := range tm.Args {
		if str, ok := arg.(string); ok {
			if data, err := base64.StdEncoding.DecodeString(str); err == nil {
				tm.Args[i] = data
			}
		}
	}

	return nil
}

var DefaultTaskPolicy = TaskPolicy{
	MaxRetries: 3,
	Timeout:    3600,
}

var MaxTaskTTL = 24 * 60 * 60
var MaxTaskRetries = 5

type TaskPolicy struct {
	MaxRetries uint      `json:"max_retries" redis:"max_retries"`
	Timeout    int       `json:"timeout" redis:"timeout"`
	Expires    time.Time `json:"expires" redis:"expires"`
	TTL        uint32    `json:"ttl" redis:"ttl"`
}

type ErrExceededTaskLimit struct {
	MaxPendingTasks uint
}

func (e *ErrExceededTaskLimit) Error() string {
	return fmt.Sprintf("exceeded max pending tasks: %d", e.MaxPendingTasks)
}
