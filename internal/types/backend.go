package types

import (
	"database/sql"
	"encoding/json"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

type Workspace struct {
	Id         uint      `db:"id" json:"id"`
	ExternalId string    `db:"external_id" json:"external_id"`
	Name       string    `db:"name" json:"name"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

const (
	TokenTypeClusterAdmin string = "admin"
	TokenTypeWorkspace    string = "workspace"
	TokenTypeWorker       string = "worker"
)

type Token struct {
	Id          uint       `db:"id" json:"id"`
	ExternalId  string     `db:"external_id" json:"external_id"`
	Key         string     `db:"key" json:"key"`
	Active      bool       `db:"active" json:"active"`
	Reusable    bool       `db:"reusable" json:"reusable"`
	WorkspaceId *uint      `db:"workspace_id" json:"workspace_id,omitempty"` // Foreign key to Workspace
	Workspace   *Workspace `db:"workspace" json:"workspace,omitempty"`       // Pointer to associated Workspace
	TokenType   string     `db:"token_type" json:"token_type"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at" json:"updated_at"`
}

type Volume struct {
	Id          uint      `db:"id"`
	ExternalId  string    `db:"external_id"`
	Name        string    `db:"name"`
	WorkspaceId uint      `db:"workspace_id"` // Foreign key to Workspace
	CreatedAt   time.Time `db:"created_at"`
}

type Deployment struct {
	Id          uint      `db:"id" json:"id"`
	ExternalId  string    `db:"external_id" json:"external_id"`
	Name        string    `db:"name" json:"name"`
	Active      bool      `db:"active" json:"active"`
	WorkspaceId uint      `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	StubId      uint      `db:"stub_id" json:"stub_id"`           // Foreign key to Stub
	StubType    string    `db:"stub_type" json:"stub_type"`
	Version     uint      `db:"version" json:"version"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

type DeploymentWithRelated struct {
	Deployment
	Workspace Workspace `db:"workspace" json:"workspace"`
	Stub      Stub      `db:"stub" json:"stub"`
}

type Object struct {
	Id          uint      `db:"id" json:"id"`
	ExternalId  string    `db:"external_id" json:"external_id"`
	Hash        string    `db:"hash" json:"hash"`
	Size        int64     `db:"size" json:"size"`
	WorkspaceId uint      `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

type TaskStatus string

func (ts TaskStatus) IsCompleted() bool {
	switch ts {
	case TaskStatusComplete, TaskStatusCancelled, TaskStatusError, TaskStatusTimeout:
		return true
	default:
		return false
	}
}

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusComplete  TaskStatus = "COMPLETE"
	TaskStatusError     TaskStatus = "ERROR"
	TaskStatusCancelled TaskStatus = "CANCELLED"
	TaskStatusTimeout   TaskStatus = "TIMEOUT"
	TaskStatusRetry     TaskStatus = "RETRY"
)

type TaskParams struct {
	TaskId      string
	ContainerId string
	StubId      uint
	WorkspaceId uint
}

type Task struct {
	Id          uint         `db:"id" json:"id,omitempty"`
	ExternalId  string       `db:"external_id" json:"external_id,omitempty"`
	Status      TaskStatus   `db:"status" json:"status,omitempty"`
	ContainerId string       `db:"container_id" json:"container_id,omitempty"`
	StartedAt   sql.NullTime `db:"started_at" json:"started_at,omitempty"`
	EndedAt     sql.NullTime `db:"ended_at" json:"ended_at,omitempty"`
	WorkspaceId uint         `db:"workspace_id" json:"workspace_id,omitempty"` // Foreign key to Workspace
	StubId      uint         `db:"stub_id" json:"stub_id,omitempty"`           // Foreign key to Stub
	CreatedAt   time.Time    `db:"created_at" json:"created_at,omitempty"`
	UpdatedAt   time.Time    `db:"updated_at" json:"updated_at,omitempty"`
}

type TaskWithRelated struct {
	Task
	Workspace Workspace `db:"workspace" json:"workspace"`
	Stub      Stub      `db:"stub" json:"stub"`
}

type TaskCountPerDeployment struct {
	DeploymentName string `db:"deployment_name" json:"deployment_name"`
	TaskCount      uint   `db:"task_count" json:"task_count"`
}

type TaskCountByTime struct {
	Time         time.Time       `db:"time" json:"time"`
	Count        uint            `count:"count" json:"count"`
	StatusCounts json.RawMessage `db:"status_counts" json:"status_counts"`
}

type StubConfigV1 struct {
	Runtime         Runtime      `json:"runtime"`
	Handler         string       `json:"handler"`
	PythonVersion   string       `json:"python_version"`
	KeepWarmSeconds uint         `json:"keep_warm_seconds"`
	MaxPendingTasks uint         `json:"max_pending_tasks"`
	CallbackUrl     string       `json:"callback_url"`
	TaskPolicy      TaskPolicy   `json:"task_policy"`
	Concurrency     uint         `json:"concurrency"`
	Authorized      bool         `json:"authorized"`
	MaxContainers   uint         `json:"max_containers"`
	Volumes         []*pb.Volume `json:"volumes"`
}

const (
	StubTypeFunction            string = "function"
	StubTypeFunctionDeployment  string = "function/deployment"
	StubTypeTaskQueue           string = "taskqueue"
	StubTypeTaskQueueDeployment string = "taskqueue/deployment"
	StubTypeEndpoint            string = "endpoint"
	StubTypeEndpointDeployment  string = "endpoint/deployment"
)

type Stub struct {
	Id            uint      `db:"id" json:"id"`
	ExternalId    string    `db:"external_id" json:"external_id"`
	Name          string    `db:"name" json:"name"`
	Type          string    `db:"type" json:"type"`
	Config        string    `db:"config" json:"config"`
	ConfigVersion uint      `db:"config_version" json:"config_version"`
	ObjectId      uint      `db:"object_id" json:"object_id"`       // Foreign key to Object
	WorkspaceId   uint      `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time `db:"updated_at" json:"updated_at"`
}

type StubWithRelated struct {
	Stub
	Workspace Workspace `db:"workspace" json:"workspace"`
	Object    Object    `db:"object" json:"object"`
}

type Image struct {
	Commands             []string `json:"commands"`
	PythonVersion        string   `json:"python_version"`
	PythonPackages       []string `json:"python_packages"`
	BaseImage            *string  `json:"base_image"`
	BaseImageCredentials *string  `json:"base_image_creds"`
}

type Runtime struct {
	Cpu     int64   `json:"cpu"`
	Gpu     GpuType `json:"gpu"`
	Memory  int64   `json:"memory"`
	ImageId string  `json:"image_id"`
}

type GpuType string

func (g *GpuType) UnmarshalJSON(data []byte) error {
	var gpuStr string
	err := json.Unmarshal(data, &gpuStr)
	if err == nil {
		*g = GpuType(gpuStr)
		return nil
	}

	var gpuInt int
	err = json.Unmarshal(data, &gpuInt)
	if err != nil {
		return err
	}

	if gpuInt == 0 {
		*g = GpuType("")
	} else if gpuInt > 0 {
		*g = GpuType("T4")
	}

	return nil
}

// FilterFieldMapping represents a mapping between a client-provided field and
// its corresponding database field, along with the values for filtering on the database field.
type FilterFieldMapping struct {
	ClientField   string
	ClientValues  []string
	DatabaseField string
}
