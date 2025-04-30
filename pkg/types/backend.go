package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Time struct {
	time.Time
}

func (t Time) Serialize() string {
	return t.Time.Format(time.RFC3339Nano)
}

func (t *Time) Scan(value interface{}) error {
	_t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("type assertion to time.Time failed")
	}

	t.Time = _t
	return nil
}

type NullTime struct {
	sql.NullTime
}

func (t NullTime) Serialize() interface{} {
	if !t.Valid {
		return nil
	}

	return time.Time(t.Time).Format(time.RFC3339Nano)
}

func (t NullTime) Now() NullTime {
	return NullTime{
		NullTime: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
}

// @go2proto
type Workspace struct {
	Id                 uint              `db:"id" json:"id,omitempty" serializer:"id,source:external_id"`
	ExternalId         string            `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Name               string            `db:"name" json:"name" serializer:"name"`
	CreatedAt          Time              `db:"created_at" json:"created_at,omitempty" serializer:"created_at"`
	UpdatedAt          Time              `db:"updated_at" json:"updated_at,omitempty" serializer:"updated_at"`
	SigningKey         *string           `db:"signing_key" json:"signing_key"`
	VolumeCacheEnabled bool              `db:"volume_cache_enabled" json:"volume_cache_enabled" serializer:"volume_cache_enabled"`
	MultiGpuEnabled    bool              `db:"multi_gpu_enabled" json:"multi_gpu_enabled" serializer:"multi_gpu_enabled"`
	ConcurrencyLimitId *uint             `db:"concurrency_limit_id" json:"concurrency_limit_id,omitempty"`
	ConcurrencyLimit   *ConcurrencyLimit `db:"concurrency_limit" json:"concurrency_limit" serializer:"concurrency_limit,omitempty"`
	StorageId          *uint             `db:"storage_id" json:"storage_id,omitempty" serializer:"storage_id,from:storage.external_id,omitempty"`
	Storage            *WorkspaceStorage `db:"storage" json:"storage" serializer:"storage,omitempty"`
}

func (w *Workspace) StorageAvailable() bool {
	return w.Storage != nil && w.Storage.Id != nil && *w.Storage.Id > 0
}

// @go2proto
type WorkspaceWithRelated struct {
	Workspace
	ConcurrencyLimit *ConcurrencyLimit `db:"concurrency_limit" json:"concurrency_limit" serializer:"concurrency_limit"`
	Storage          *WorkspaceStorage `db:"storage" json:"storage" serializer:"storage"`
}

func (w *WorkspaceWithRelated) ToProto() *pb.WorkspaceWithRelated {
	return &pb.WorkspaceWithRelated{
		Workspace:        w.Workspace.ToProto(),
		Storage:          w.Storage.ToProto(),
		ConcurrencyLimit: w.ConcurrencyLimit.ToProto(),
	}
}

func (w *Workspace) ToProto() *pb.Workspace {
	concurrencyLimit := &pb.ConcurrencyLimit{}
	if w.ConcurrencyLimit != nil {
		concurrencyLimit = w.ConcurrencyLimit.ToProto()
	}

	storage := &pb.WorkspaceStorage{}
	if w.StorageAvailable() {
		storage = w.Storage.ToProto()
	}

	return &pb.Workspace{
		Id:                 uint32(w.Id),
		ExternalId:         w.ExternalId,
		Name:               w.Name,
		SigningKey:         getStringOrDefault(w.SigningKey),
		VolumeCacheEnabled: w.VolumeCacheEnabled,
		MultiGpuEnabled:    w.MultiGpuEnabled,
		ConcurrencyLimit:   concurrencyLimit,
		Storage:            storage,
	}
}

func NewWorkspaceFromProto(in *pb.Workspace) *Workspace {
	concurrencyLimit := &ConcurrencyLimit{}
	if in.ConcurrencyLimit != nil {
		concurrencyLimit = NewConcurrencyLimitFromProto(in.ConcurrencyLimit)
	}

	storage := &WorkspaceStorage{}
	if in.Storage != nil {
		storage = NewWorkspaceStorageFromProto(in.Storage)
	}

	return &Workspace{
		Id:                 uint(in.Id),
		ExternalId:         in.ExternalId,
		Name:               in.Name,
		SigningKey:         getPointerOrNil(in.SigningKey),
		VolumeCacheEnabled: in.VolumeCacheEnabled,
		MultiGpuEnabled:    in.MultiGpuEnabled,
		ConcurrencyLimit:   concurrencyLimit,
		Storage:            storage,
	}
}

// @go2proto
type WorkspaceStorage struct {
	Id          *uint      `db:"id" json:"id"`
	ExternalId  *string    `db:"external_id" json:"external_id"`
	BucketName  *string    `db:"bucket_name" json:"bucket_name"`
	AccessKey   *string    `db:"access_key" json:"access_key" encrypt:"true"`
	SecretKey   *string    `db:"secret_key" json:"secret_key" encrypt:"true"`
	EndpointUrl *string    `db:"endpoint_url" json:"endpoint_url"`
	Region      *string    `db:"region" json:"region"`
	CreatedAt   *time.Time `db:"created_at" json:"created_at,omitempty"`
	UpdatedAt   *time.Time `db:"updated_at" json:"updated_at,omitempty"`
}

func NewWorkspaceStorageFromProto(in *pb.WorkspaceStorage) *WorkspaceStorage {
	id := uint(in.Id)
	createdAt := in.CreatedAt.AsTime()
	updatedAt := in.UpdatedAt.AsTime()

	return &WorkspaceStorage{
		Id:          &id,
		ExternalId:  &in.ExternalId,
		BucketName:  &in.BucketName,
		AccessKey:   &in.AccessKey,
		SecretKey:   &in.SecretKey,
		EndpointUrl: &in.EndpointUrl,
		Region:      &in.Region,
		CreatedAt:   &createdAt,
		UpdatedAt:   &updatedAt,
	}
}

func (w *WorkspaceStorage) ToProto() *pb.WorkspaceStorage {
	return &pb.WorkspaceStorage{
		Id:          uint32(*w.Id),
		ExternalId:  *w.ExternalId,
		BucketName:  *w.BucketName,
		AccessKey:   *w.AccessKey,
		SecretKey:   *w.SecretKey,
		Region:      *w.Region,
		EndpointUrl: *w.EndpointUrl,
		CreatedAt:   timestamppb.New(*w.CreatedAt),
		UpdatedAt:   timestamppb.New(*w.UpdatedAt),
	}
}

const (
	TokenTypeClusterAdmin     string = "admin"
	TokenTypeWorkspacePrimary string = "workspace_primary"
	TokenTypeWorkspace        string = "workspace"
	TokenTypeWorker           string = "worker"
	TokenTypeMachine          string = "machine"
)

type Token struct {
	Id                     uint       `db:"id" json:"id" serializer:"id,source:external_id"`
	ExternalId             string     `db:"external_id" json:"external_id" serializer:"external_id"`
	Key                    string     `db:"key" json:"key" serializer:"key"`
	Active                 bool       `db:"active" json:"active" serializer:"active"`
	Reusable               bool       `db:"reusable" json:"reusable" serializer:"reusable"`
	WorkspaceId            *uint      `db:"workspace_id" json:"workspace_id,omitempty"`                            // Foreign key to Workspace
	Workspace              *Workspace `db:"workspace" json:"workspace,omitempty" serializer:"workspace,omitempty"` // Pointer to associated Workspace
	TokenType              string     `db:"token_type" json:"token_type" serializer:"token_type"`
	CreatedAt              Time       `db:"created_at" json:"created_at" serializer:"created_at"`
	UpdatedAt              Time       `db:"updated_at" json:"updated_at" serializer:"updated_at"`
	DisabledByClusterAdmin bool       `db:"disabled_by_cluster_admin" json:"disabled_by_cluster_admin" serializer:"disabled_by_cluster_admin"`
}

type Volume struct {
	Id          uint   `db:"id" json:"id"`
	ExternalId  string `db:"external_id" json:"external_id"`
	Name        string `db:"name" json:"name"`
	Size        uint64 `json:"size"`                           // Populated by volume abstraction
	WorkspaceId uint   `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt   Time   `db:"created_at" json:"created_at"`
	UpdatedAt   Time   `db:"updated_at" json:"updated_at"`
}

type VolumeWithRelated struct {
	Volume
	Workspace Workspace `db:"workspace" json:"workspace"`
}

type Deployment struct {
	Id          uint     `db:"id" json:"id" serializer:"id,source:external_id"`
	ExternalId  string   `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Name        string   `db:"name" json:"name" serializer:"name"`
	Active      bool     `db:"active" json:"active" serializer:"active"`
	Subdomain   string   `db:"subdomain" json:"subdomain" serializer:"subdomain"`
	WorkspaceId uint     `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	StubId      uint     `db:"stub_id" json:"stub_id"`           // Foreign key to Stub
	StubType    string   `db:"stub_type" json:"stub_type" serializer:"stub_type"`
	Version     uint     `db:"version" json:"version" serializer:"version"`
	CreatedAt   Time     `db:"created_at" json:"created_at" serializer:"created_at"`
	UpdatedAt   Time     `db:"updated_at" json:"updated_at" serializer:"updated_at"`
	DeletedAt   NullTime `db:"deleted_at" json:"deleted_at" serializer:"deleted_at"`
	AppId       uint     `db:"app_id" json:"app_id,omitempty"` // Foreign key to App
}

type DeploymentWithRelated struct {
	Deployment
	Workspace   Workspace `db:"workspace" json:"workspace" serializer:"workspace"`
	Stub        Stub      `db:"stub" json:"stub" serializer:"stub"`
	App         App       `db:"app" json:"app" serializer:"app"`
	StubId      string    `serializer:"stub_id,source:stub.id"`
	AppId       string    `serializer:"app_id,source:app.id"`
	WorkspaceId string    `serializer:"workspace_id,source:workspace.id"`
}

// @go2proto
type Object struct {
	Id          uint   `db:"id" json:"id" serializer:"id,source:external_id"`
	ExternalId  string `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Hash        string `db:"hash" json:"hash" serializer:"hash"`
	Size        int64  `db:"size" json:"size" serializer:"size"`
	WorkspaceId uint   `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt   Time   `db:"created_at" json:"created_at"`
}

func (o *Object) ToProto() *pb.Object {
	return &pb.Object{
		Id:          uint32(o.Id),
		ExternalId:  o.ExternalId,
		Hash:        o.Hash,
		Size:        o.Size,
		WorkspaceId: uint32(o.WorkspaceId),
	}
}

func NewObjectFromProto(in *pb.Object) *Object {
	return &Object{
		Id:          uint(in.Id),
		ExternalId:  in.ExternalId,
		Hash:        in.Hash,
		Size:        in.Size,
		WorkspaceId: uint(in.WorkspaceId),
	}
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

func (ts TaskStatus) IsInflight() bool {
	switch ts {
	case TaskStatusPending, TaskStatusRunning, TaskStatusRetry:
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
	Id          uint       `db:"id" json:"id,omitempty" serializer:"id,source:external_id"`
	ExternalId  string     `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Status      TaskStatus `db:"status" json:"status,omitempty" serializer:"status"`
	ContainerId string     `db:"container_id" json:"container_id,omitempty" serializer:"container_id"`
	StartedAt   NullTime   `db:"started_at" json:"started_at,omitempty" serializer:"started_at"`
	EndedAt     NullTime   `db:"ended_at" json:"ended_at,omitempty" serializer:"ended_at"`
	WorkspaceId uint       `db:"workspace_id" json:"workspace_id,omitempty"` // Foreign key to Workspace
	StubId      uint       `db:"stub_id" json:"stub_id,omitempty"`           // Foreign key to Stub
	CreatedAt   Time       `db:"created_at" json:"created_at,omitempty" serializer:"created_at"`
	UpdatedAt   Time       `db:"updated_at" json:"updated_at,omitempty" serializer:"updated_at"`
}

type TaskWithRelated struct {
	Task
	Deployment struct {
		ExternalId *string `db:"external_id" json:"external_id"`
		Name       *string `db:"name" json:"name"`
		Version    *uint   `db:"version" json:"version"`
	} `db:"deployment" json:"deployment" serializer:"deployment"`
	Outputs   []TaskOutput `json:"outputs" serializer:"outputs"`
	Stats     TaskStats    `json:"stats" serializer:"stats"`
	Workspace Workspace    `db:"workspace" json:"workspace" serializer:"workspace"`
	Stub      Stub         `db:"stub" json:"stub" serializer:"stub"`
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
	Name      string `json:"name" serializer:"name"`
	URL       string `json:"url" serializer:"url"`
	ExpiresIn uint32 `json:"expires_in" serializer:"expires_in"`
}

type TaskStats struct {
	ActiveContainers uint32 `json:"active_containers" serializer:"active_containers"`
	QueueDepth       uint32 `json:"queue_depth" serializer:"queue_depth"`
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
	Env                []string        `json:"env,omitempty"`
	Autoscaler         *Autoscaler     `json:"autoscaler"`
	Extra              json.RawMessage `json:"extra"`
	CheckpointEnabled  bool            `json:"checkpoint_enabled"`
	WorkDir            string          `json:"work_dir"`
	EntryPoint         []string        `json:"entry_point"`
	Ports              []uint32        `json:"ports"`
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

// @go2proto
type App struct {
	Id          uint     `db:"id" json:"id" serializer:"id,source:external_id"`
	ExternalId  string   `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Name        string   `db:"name" json:"name" serializer:"name"`
	Description string   `db:"description" json:"description" serializer:"description"`
	WorkspaceId uint     `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt   Time     `db:"created_at" json:"created_at" serializer:"created_at"`
	UpdatedAt   Time     `db:"updated_at" json:"updated_at" serializer:"updated_at"`
	DeletedAt   NullTime `db:"deleted_at" json:"deleted_at" serializer:"deleted_at" go2proto:"ignore"`
}

func (a *App) ToProto() *pb.App {
	return &pb.App{
		Id:          uint32(a.Id),
		ExternalId:  a.ExternalId,
		Name:        a.Name,
		Description: a.Description,
		CreatedAt:   timestamppb.New(a.CreatedAt.Time),
		UpdatedAt:   timestamppb.New(a.UpdatedAt.Time),
	}
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
	StubTypePod                    string = "pod"
	StubTypePodDeployment          string = "pod/deployment"
	StubTypePodRun                 string = "pod/run"
)

// @go2proto
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

// @go2proto
type Stub struct {
	Id            uint     `db:"id" json:"id,omitempty" serializer:"id,source:external_id"`
	ExternalId    string   `db:"external_id" json:"external_id,omitempty" serializer:"external_id"`
	Name          string   `db:"name" json:"name" serializer:"name"`
	Type          StubType `db:"type" json:"type" serializer:"type"`
	Config        string   `db:"config" json:"config" serializer:"config"`
	ConfigVersion uint     `db:"config_version" json:"config_version" serializer:"config_version"`
	ObjectId      uint     `db:"object_id" json:"object_id"`       // Foreign key to Object
	WorkspaceId   uint     `db:"workspace_id" json:"workspace_id"` // Foreign key to Workspace
	CreatedAt     Time     `db:"created_at" json:"created_at" serializer:"created_at"`
	UpdatedAt     Time     `db:"updated_at" json:"updated_at" serializer:"updated_at"`
	Public        bool     `db:"public" json:"public" serializer:"public"`
	AppId         uint     `db:"app_id" json:"app_id,omitempty"` // Foreign key to App
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

func (s *Stub) ToProto() *pb.Stub {
	return &pb.Stub{
		Id:            uint32(s.Id),
		ExternalId:    s.ExternalId,
		Name:          s.Name,
		Type:          string(s.Type),
		Config:        s.Config,
		ConfigVersion: uint32(s.ConfigVersion),
		WorkspaceId:   uint32(s.WorkspaceId),
		CreatedAt:     timestamppb.New(s.CreatedAt.Time),
		UpdatedAt:     timestamppb.New(s.UpdatedAt.Time),
	}
}

func NewStubFromProto(in *pb.Stub) *Stub {
	return &Stub{
		Id:            uint(in.Id),
		ExternalId:    in.ExternalId,
		Name:          in.Name,
		Type:          StubType(in.Type),
		Config:        in.Config,
		ConfigVersion: uint(in.ConfigVersion),
		WorkspaceId:   uint(in.WorkspaceId),
		CreatedAt:     Time{Time: in.CreatedAt.AsTime()},
		UpdatedAt:     Time{Time: in.UpdatedAt.AsTime()},
	}
}

// @go2proto
type StubWithRelated struct {
	Stub
	Workspace Workspace `db:"workspace" json:"workspace" serializer:"workspace"`
	Object    Object    `db:"object" json:"object" serializer:"object"`
	App       *App      `db:"app" json:"app" serializer:"app"`
}

func (s *StubWithRelated) ToProto() *pb.StubWithRelated {
	return &pb.StubWithRelated{
		Stub:      s.Stub.ToProto(),
		Workspace: s.Workspace.ToProto(),
		Object:    s.Object.ToProto(),
	}
}

func NewStubWithRelatedFromProto(in *pb.StubWithRelated) *StubWithRelated {
	return &StubWithRelated{
		Stub:      *NewStubFromProto(in.Stub),
		Workspace: *NewWorkspaceFromProto(in.Workspace),
		Object:    *NewObjectFromProto(in.Object),
	}
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

// FilterFieldMapping represents a mapping between a client-provided field and
// its corresponding database field, along with the values for filtering on the database field.
type FilterFieldMapping struct {
	ClientField   string
	ClientValues  []string
	DatabaseField string
}

// @go2proto
type ConcurrencyLimit struct {
	Id                uint      `db:"id" json:"-" redis:"-"`
	ExternalId        string    `db:"external_id" json:"external_id,omitempty" redis:"external_id"`
	GPULimit          uint32    `db:"gpu_limit" json:"gpu_limit" redis:"gpu_limit"`
	CPUMillicoreLimit uint32    `db:"cpu_millicore_limit" json:"cpu_millicore_limit" redis:"cpu_millicore_limit"`
	CreatedAt         time.Time `db:"created_at" json:"created_at,omitempty" redis:"-"`
	UpdatedAt         time.Time `db:"updated_at" json:"updated_at,omitempty" redis:"-"`
}

func (c *ConcurrencyLimit) ToProto() *pb.ConcurrencyLimit {
	return &pb.ConcurrencyLimit{
		Id:                uint32(c.Id),
		ExternalId:        c.ExternalId,
		GpuLimit:          c.GPULimit,
		CpuMillicoreLimit: c.CPUMillicoreLimit,
	}
}

func NewConcurrencyLimitFromProto(in *pb.ConcurrencyLimit) *ConcurrencyLimit {
	return &ConcurrencyLimit{
		Id:                uint(in.Id),
		ExternalId:        in.ExternalId,
		GPULimit:          in.GpuLimit,
		CPUMillicoreLimit: in.CpuMillicoreLimit,
	}
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
