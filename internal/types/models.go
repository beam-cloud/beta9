package types

import (
	"encoding/json"
	"time"
)

type BeamApp struct {
	ID         uint `gorm:"primarykey"`
	ExternalId string
	Created    time.Time
	Updated    time.Time
	ShortId    string
	IdentityId uint `gorm:"foreignKey:IdentityId"`
}

func (BeamApp) TableName() string {
	return "beam_app"
}

type BeamAppDeploymentPackage struct {
	ID       uint `gorm:"primarykey"`
	AppId    uint
	Config   json.RawMessage `gorm:"type:json"`
	Verified bool
}

func (BeamAppDeploymentPackage) TableName() string {
	return "beam_app_deployment_package"
}

type BeamAppDeployment struct {
	ID          uint `gorm:"primarykey"`
	ExternalId  string
	Created     time.Time
	Updated     time.Time
	Status      string
	ErrorMsg    string
	AppId       uint `gorm:"foreignKey:BeamAppId"`
	TriggerType string
	Version     uint
	PackageId   uint `gorm:"index"`
	ImageTag    string
}

func (BeamAppDeployment) TableName() string {
	return "beam_app_deployment"
}

type BeamAppServe struct {
	ID         uint `gorm:"primarykey"`
	ExternalId string
	Created    time.Time
	Updated    time.Time
	AppId      uint            `gorm:"foreignKey:BeamAppId"`
	Config     json.RawMessage `gorm:"type:json"`
	IdentityId uint            `gorm:"foreignKey:IdentityId"`
	TokenId    uint            `gorm:"foreignKey:TokenId"`
	ImageTag   string
	EndedAt    *int64
}

func (BeamAppServe) TableName() string {
	return "beam_app_serve"
}

type BeamScheduledJob struct {
	ID           uint   `gorm:"primarykey"`
	EntryID      string `gorm:"unique"`
	When         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeploymentID uint `gorm:"foreignKey:DeploymentID"`
}

func (BeamScheduledJob) TableName() string {
	return "beam_scheduled_job"
}

const (
	BeamAppTaskStatusPending   string = "PENDING"
	BeamAppTaskStatusRunning   string = "RUNNING"
	BeamAppTaskStatusComplete  string = "COMPLETE"
	BeamAppTaskStatusFailed    string = "FAILED"
	BeamAppTaskStatusRetry     string = "RETRY"
	BeamAppTaskStatusCancelled string = "CANCELLED"
	BeamAppTaskStatusTimeout   string = "TIMEOUT"
)

const (
	DeploymentStatusReady   string = "READY"
	DeploymentStatusError   string = "ERROR"
	DeploymentStatusStopped string = "STOPPED"
)

type BeamAppTask struct {
	ID              uint      `gorm:"primarykey"`
	AppDeploymentId *uint     `gorm:"foreignKey:BeamAppDeploymentId"`
	AppRunId        *uint     `gorm:"foreignKey:BeamAppRunId"`
	AppServeId      *uint     `gorm:"foreignKey:BeamAppServeId"`
	IdentityId      uint      `gorm:"foreignKey:IdentityId"`
	Created         time.Time `gorm:"autoCreateTime"`
	Updated         time.Time `gorm:"autoCreateTime"`
	StartedAt       *time.Time
	EndedAt         *time.Time
	Status          string
	ExternalId      string
	TaskId          string
	TaskPolicy      json.RawMessage `gorm:"type:json"`
}

func (BeamAppTask) TableName() string {
	return "beam_app_task"
}

type IdentityApiKey struct {
	ID           uint `gorm:"primarykey"`
	ExternalId   string
	ClientId     string
	ClientSecret string
	IdentityId   uint `gorm:"foreignKey:IdentityId"`
	IsEnabled    bool
	Identity     Identity `gorm:"foreignKey:IdentityId"`
}

func (IdentityApiKey) TableName() string {
	return "identity_identity_apikey"
}

type S2SEntityTypes string

const (
	S2SEntityBeamDevelopmentSession S2SEntityTypes = "BEAM_DEVELOP"
	S2SEntityBeamDeployment         S2SEntityTypes = "BEAM_DEPLOY"
	S2SEntityBeamRun                S2SEntityTypes = "BEAM_RUN"
	S2SEntityBeamSpawner            S2SEntityTypes = "BEAM_SPAWNER"
	S2SEntityBeamApi                S2SEntityTypes = "BEAM_API"
	S2SEntityBeamServe              S2SEntityTypes = "BEAM_SERVE"
)

type ServiceToServiceToken struct {
	ID               uint `gorm:"primaryKey"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ExternalID       string `gorm:"type:varchar(30);unique"`
	Key              string `gorm:"type:varchar(255);unique"`
	IdentityID       uint   `gorm:"foreignKey:IdentityId"`
	Identity         Identity
	EntityType       S2SEntityTypes `gorm:"type:varchar(30)"`
	EntityExternalID string         `gorm:"type:varchar(30)"`
	Expires          time.Time
}

func (ServiceToServiceToken) TableName() string {
	return "infra_s2s_token"
}

type Stats struct {
	ID          uint `gorm:"primarykey"`
	CollectedAt time.Time
	Identity    string
	Category    string
	Topic       string
	Subcategory string
	Metric      string
	Type        string
	Value       string
}

func (Stats) TableName() string {
	return "metrics_statsd"
}

type Agent struct {
	ID            uint `gorm:"primarykey"`
	ExternalID    string
	Created       time.Time `gorm:"autoCreateTime"`
	Updated       time.Time `gorm:"autoUpdateTime"`
	Name          string
	Token         string
	Pools         json.RawMessage `gorm:"type:json"`
	CloudProvider string
	Version       string
	IsOnline      bool
	Identity      Identity `gorm:"foreignKey:IdentityId"`
	IdentityId    uint     `gorm:"foreignKey:IdentityId"`
}

func (*Agent) TableName() string {
	return "infra_agent"
}

func (a *Agent) SetPools(pools map[string]string) error {
	p, err := json.Marshal(pools)
	if err != nil {
		return err
	}

	a.Pools = p
	return nil
}

func (a *Agent) GetPools() (map[string]string, error) {
	var pools map[string]string

	err := json.Unmarshal([]byte(a.Pools), &pools)
	if err != nil {
		return nil, err
	}

	return pools, nil
}

type IdentityQuota struct {
	ID                  uint `gorm:"primarykey"`
	ExternalId          string
	Created             time.Time `gorm:"autoCreateTime"`
	Updated             time.Time `gorm:"autoUpdateTime"`
	IdentityId          uint      `gorm:"foreignKey:IdentityId"`
	Identity            Identity
	CpuConcurrencyLimit int `redis:"cpu_concurrency_limit"`
	GpuConcurrencyLimit int `redis:"gpu_concurrency_limit"`
	AppCountLimit       int `redis:"app_count_limit"`
}

func (*IdentityQuota) TableName() string {
	return "identity_identity_quota"
}
