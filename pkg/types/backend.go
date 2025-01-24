package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

type Workspace struct {
	Id                 uint              `db:"id" json:"id,omitempty"`
	ExternalId         string            `db:"external_id" json:"external_id"`
	Name               string            `db:"name" json:"name"`
	CreatedAt          time.Time         `db:"created_at" json:"created_at,omitempty"`
	UpdatedAt          time.Time         `db:"updated_at" json:"updated_at,omitempty"`
	SigningKey         *string           `db:"signing_key" json:"signing_key"`
	VolumeCacheEnabled bool              `db:"volume_cache_enabled" json:"volume_cache_enabled"`
	MultiGpuEnabled    bool              `db:"multi_gpu_enabled" json:"multi_gpu_enabled"`
	ConcurrencyLimitId *uint             `db:"concurrency_limit_id" json:"concurrency_limit_id,omitempty"`
	ConcurrencyLimit   *ConcurrencyLimit `db:"concurrency_limit" json:"concurrency_limit"`
}

const (
	TokenTypeClusterAdmin string = "admin"
	TokenTypeWorkspace    string = "workspace"
	TokenTypeWorker       string = "worker"
	TokenTypeMachine      string = "machine"
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

	DisabledByClusterAdmin bool `db:"disabled_by_cluster_admin" json:"disabled_by_cluster_admin"`
}

type Volume struct {
	Id          uint      `db:"id" json:"id"`
	ExternalId  string    `db:"external_id" json:"external_id"`
	Name        string    `db:"name" json:"name"`
	Size        uint64    `json:"size"`                           // Populated by volume abstraction
	WorkspaceId uint      `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

type VolumeWithRelated struct {
	Volume
	Workspace Workspace `db:"workspace" json:"workspace"`
}

type Deployment struct {
	Id          uint         `db:"id" json:"id"`
	ExternalId  string       `db:"external_id" json:"external_id"`
	Name        string       `db:"name" json:"name"`
	Active      bool         `db:"active" json:"active"`
	Subdomain   string       `db:"subdomain" json:"subdomain"`
	WorkspaceId uint         `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	StubId      uint         `db:"stub_id" json:"stub_id"`           // Foreign key to Stub
	StubType    string       `db:"stub_type" json:"stub_type"`
	Version     uint         `db:"version" json:"version"`
	CreatedAt   time.Time    `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time    `db:"updated_at" json:"updated_at"`
	DeletedAt   sql.NullTime `db:"deleted_at" json:"deleted_at"`
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
	case TaskStatusComplete, TaskStatusCancelled, TaskStatusError, TaskStatusTimeout, TaskStatusExpired:
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
	TaskStatusExpired   TaskStatus = "EXPIRED"
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
	Deployment struct {
		ExternalId *string `db:"external_id" json:"external_id"`
		Name       *string `db:"name" json:"name"`
		Version    *uint   `db:"version" json:"version"`
	} `db:"deployment" json:"deployment"`
	Outputs   []TaskOutput `json:"outputs"`
	Stats     TaskStats    `json:"stats"`
	Workspace Workspace    `db:"workspace" json:"workspace"`
	Stub      Stub         `db:"stub" json:"stub"`
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

type TaskOutput struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	ExpiresIn uint32 `json:"expires_in"`
}

type TaskStats struct {
	ActiveContainers uint32 `json:"active_containers"`
	QueueDepth       uint32 `json:"queue_depth"`
}

type StubConfigV1 struct {
	Runtime            Runtime         `json:"runtime"`
	Handler            string          `json:"handler"`
	OnStart            string          `json:"on_start"`
	OnDeploy           string          `json:"on_deploy"`
	OnDeployStubId     string          `json:"on_deploy_stub_id"`
	PythonVersion      string          `json:"python_version"`
	KeepWarmSeconds    uint            `json:"keep_warm_seconds"`
	MaxPendingTasks    uint            `json:"max_pending_tasks"`
	CallbackUrl        string          `json:"callback_url"`
	TaskPolicy         TaskPolicy      `json:"task_policy"`
	Workers            uint            `json:"workers"`
	ConcurrentRequests uint            `json:"concurrent_requests"`
	Authorized         bool            `json:"authorized"`
	Volumes            []*pb.Volume    `json:"volumes"`
	Secrets            []Secret        `json:"secrets,omitempty"`
	Autoscaler         *Autoscaler     `json:"autoscaler"`
	Extra              json.RawMessage `json:"extra"`
	CheckpointEnabled  bool            `json:"checkpoint_enabled"`
}

func (c *StubConfigV1) RequiresGPU() bool {
	return len(c.Runtime.Gpus) > 0 || c.Runtime.Gpu != ""
}

type AutoscalerType string

const (
	QueueDepthAutoscaler AutoscalerType = "queue_depth"
)

type Autoscaler struct {
	Type              AutoscalerType `json:"type"`
	MaxContainers     uint           `json:"max_containers"`
	TasksPerContainer uint           `json:"tasks_per_container"`
	MinContainers     uint           `json:"min_containers"`
}

const (
	StubTypeFunction               string = "function"
	StubTypeFunctionDeployment     string = "function/deployment"
	StubTypeFunctionServe          string = "function/serve"
	StubTypeContainer              string = "container"
	StubTypeShell                  string = "shell"
	StubTypeTaskQueue              string = "taskqueue"
	StubTypeTaskQueueDeployment    string = "taskqueue/deployment"
	StubTypeTaskQueueServe         string = "taskqueue/serve"
	StubTypeEndpoint               string = "endpoint"
	StubTypeEndpointDeployment     string = "endpoint/deployment"
	StubTypeEndpointServe          string = "endpoint/serve"
	StubTypeASGI                   string = "asgi"
	StubTypeASGIDeployment         string = "asgi/deployment"
	StubTypeASGIServe              string = "asgi/serve"
	StubTypeScheduledJob           string = "schedule"
	StubTypeScheduledJobDeployment string = "schedule/deployment"
	StubTypeBot                    string = "bot"
	StubTypeBotDeployment          string = "bot/deployment"
	StubTypeBotServe               string = "bot/serve"
)

type StubType string

func (t StubType) IsServe() bool {
	return strings.HasSuffix(string(t), "/serve")
}

func (t StubType) IsDeployment() bool {
	return strings.HasSuffix(string(t), "/deployment")
}

func (t StubType) Kind() string {
	return strings.Split(string(t), "/")[0]
}

type Stub struct {
	Id            uint      `db:"id" json:"_"`
	ExternalId    string    `db:"external_id" json:"external_id"`
	Name          string    `db:"name" json:"name"`
	Type          StubType  `db:"type" json:"type"`
	Config        string    `db:"config" json:"config"`
	ConfigVersion uint      `db:"config_version" json:"config_version"`
	ObjectId      uint      `db:"object_id" json:"object_id"`       // Foreign key to Object
	WorkspaceId   uint      `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time `db:"updated_at" json:"updated_at"`
}

func (s *Stub) UnmarshalConfig() (*StubConfigV1, error) {
	var config *StubConfigV1
	err := json.Unmarshal([]byte(s.Config), &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (s *Stub) SanitizeConfig() error {
	var config StubConfigV1
	err := json.Unmarshal([]byte(s.Config), &config)
	if err != nil {
		return err
	}

	// Remove secret values from config
	for i := range config.Secrets {
		config.Secrets[i].Value = ""
	}

	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	s.Config = string(data)
	return nil
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
	Cpu      int64     `json:"cpu"`
	Gpu      GpuType   `json:"gpu"`
	GpuCount uint32    `json:"gpu_count"`
	Memory   int64     `json:"memory"`
	ImageId  string    `json:"image_id"`
	Gpus     []GpuType `json:"gpus"`
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

func (g *GpuType) MarshalJSON() ([]byte, error) {
	if *g == "" {
		return []byte("0"), nil
	}

	return json.Marshal(string(*g))
}

func (g *GpuType) String() string {
	return string(*g)
}

func GPUTypesFromString(gpu string) []GpuType {
	gpus := []GpuType{}
	gpuString := strings.Trim(gpu, " ")
	if len(gpuString) > 0 {
		for _, g := range strings.Split(gpuString, ",") {
			gpus = append(gpus, GpuType(g))
		}
	}

	return gpus
}

func GpuTypesToStrings(gpus []GpuType) []string {
	var gpuStrings []string
	for _, gpu := range gpus {
		gpuStrings = append(gpuStrings, string(gpu))
	}
	return gpuStrings
}

// FilterFieldMapping represents a mapping between a client-provided field and
// its corresponding database field, along with the values for filtering on the database field.
type FilterFieldMapping struct {
	ClientField   string
	ClientValues  []string
	DatabaseField string
}

type ConcurrencyLimit struct {
	Id                uint      `db:"id" json:"-" redis:"-"`
	ExternalId        string    `db:"external_id" json:"external_id,omitempty" redis:"external_id"`
	GPULimit          uint32    `db:"gpu_limit" json:"gpu_limit" redis:"gpu_limit"`
	CPUMillicoreLimit uint32    `db:"cpu_millicore_limit" json:"cpu_millicore_limit" redis:"cpu_millicore_limit"`
	CreatedAt         time.Time `db:"created_at" json:"created_at,omitempty" redis:"-"`
	UpdatedAt         time.Time `db:"updated_at" json:"updated_at,omitempty" redis:"-"`
}

type Secret struct {
	Id            uint      `db:"id" json:"-"`
	ExternalId    string    `db:"external_id" json:"external_id,omitempty"`
	CreatedAt     time.Time `db:"created_at" json:"created_at,omitempty"`
	UpdatedAt     time.Time `db:"updated_at" json:"updated_at,omitempty"`
	Name          string    `db:"name" json:"name"`
	Value         string    `db:"value" json:"value,omitempty"`
	WorkspaceId   uint      `db:"workspace_id" json:"workspace_id,omitempty"`
	LastUpdatedBy *uint     `db:"last_updated_by" json:"last_updated_by,omitempty"`
}

type ScheduledJob struct {
	Id         uint64 `db:"id"`
	ExternalId string `db:"external_id"`

	JobId    uint64              `db:"job_id"`
	JobName  string              `db:"job_name"`
	Schedule string              `db:"job_schedule"`
	Payload  ScheduledJobPayload `db:"job_payload"`

	StubId       uint         `db:"stub_id"`
	DeploymentId uint         `db:"deployment_id"`
	CreatedAt    time.Time    `db:"created_at"`
	UpdatedAt    time.Time    `db:"updated_at"`
	DeletedAt    sql.NullTime `db:"deleted_at"`
}

type ScheduledJobPayload struct {
	StubId        string      `json:"stub_id"`
	WorkspaceName string      `json:"workspace_name"`
	TaskPayload   TaskPayload `json:"task_payload"`
}

func (p *ScheduledJobPayload) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}

	return json.Unmarshal(bytes, p)
}

func (p ScheduledJobPayload) Value() (driver.Value, error) {
	return json.Marshal(p)
}
