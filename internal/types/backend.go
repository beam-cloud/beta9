package types

import (
	"database/sql"
	"encoding/json"
	"time"
)

type Workspace struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Name       string    `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type Token struct {
	Id          uint       `db:"id"`
	ExternalId  string     `db:"external_id"`
	Key         string     `db:"key"`
	Active      bool       `db:"active"`
	WorkspaceId uint       `db:"Workspace_id"` // Foreign key to Workspace
	Workspace   *Workspace `db:"Workspace"`    // Pointer to associated Workspace
	CreatedAt   time.Time  `db:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at"`
}

type Volume struct {
	Id          uint      `db:"id"`
	ExternalId  string    `db:"external_id"`
	Name        string    `db:"name"`
	WorkspaceId uint      `db:"Workspace_id"` // Foreign key to Workspace
	CreatedAt   time.Time `db:"created_at"`
}

const (
	DeploymentStatusPending string = "PENDING"
	DeploymentStatusReady   string = "READY"
	DeploymentStatusError   string = "ERROR"
	DeploymentStatusStopped string = "STOPPED"
)

type Deployment struct {
	Id          uint      `db:"id"`
	ExternalId  string    `db:"external_id"`
	Version     uint      `db:"version"`
	Status      string    `db:"status"`
	WorkspaceId uint      `db:"Workspace_id"` // Foreign key to Workspace
	StubId      uint      `db:"stub_id"`      // Foreign key to Stub
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type Object struct {
	Id          uint      `db:"id"`
	ExternalId  string    `db:"external_id"`
	Hash        string    `db:"hash"`
	Size        int64     `db:"size"`
	WorkspaceId uint      `db:"Workspace_id"` // Foreign key to Workspace
	CreatedAt   time.Time `db:"created_at"`
}

const (
	TaskStatusPending   string = "PENDING"
	TaskStatusRunning   string = "RUNNING"
	TaskStatusComplete  string = "COMPLETE"
	TaskStatusError     string = "ERROR"
	TaskStatusCancelled string = "CANCELLED"
	TaskStatusTimeout   string = "TIMEOUT"
	TaskStatusRetry     string = "RETRY"
)

var DefaultTaskPolicy = TaskPolicy{
	MaxRetries: 3,
	Timeout:    3600,
}

type Task struct {
	Id          uint         `db:"id"`
	ExternalId  string       `db:"external_id"`
	Status      string       `db:"status"`
	ContainerId string       `db:"container_id"`
	StartedAt   sql.NullTime `db:"started_at"`
	EndedAt     sql.NullTime `db:"ended_at"`
	WorkspaceId uint         `db:"Workspace_id"` // Foreign key to Workspace
	StubId      uint         `db:"stub_id"`      // Foreign key to Stub
	CreatedAt   time.Time    `db:"created_at"`
	UpdatedAt   time.Time    `db:"updated_at"`
}

type StubConfigV1 struct {
	Runtime Runtime `json:"runtime"`
}

const (
	StubTypeFunction  string = "FUNCTION"
	StubTypeTaskQueue string = "TASK_QUEUE"
)

type Stub struct {
	Id            uint      `db:"id"`
	ExternalId    string    `db:"external_id"`
	Name          string    `db:"name"`
	Type          string    `db:"type"`
	Config        string    `db:"config"`
	ConfigVersion uint      `db:"config_version"`
	ObjectId      uint      `db:"object_id"`    // Foreign key to Object
	WorkspaceId   uint      `db:"Workspace_id"` // Foreign key to Workspace
	CreatedAt     time.Time `db:"created_at"`
	UpdatedAt     time.Time `db:"updated_at"`
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
