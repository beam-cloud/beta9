package types

import (
	"encoding/json"
	"errors"
	"time"
)

// Managed database services.

var (
	ErrDatabaseKind             = errors.New("kind must be postgres, redis, mysql or mongo")
	ErrDatabaseExists           = errors.New("a service with this name already exists")
	ErrDatabaseNotFound         = errors.New("database service not found")
	ErrDatabaseImageUnsupported = errors.New("image service is not available on this gateway")
)

type CreateDatabaseParams struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Database    string `json:"database"`
	Size        string `json:"size"`
	Pool        string `json:"pool"`
	AlwaysOn    bool   `json:"always_on"`
	SnapshotID  string `json:"snapshot_id,omitempty"`
	RestoreFrom string `json:"restore_from,omitempty"`
	RestoreTime string `json:"restore_time,omitempty"`
	// Millicores and megabytes; zero means the product default.
	Cpu    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

type DatabaseServiceInfo struct {
	Name                         string `json:"name"`
	Kind                         string `json:"kind"`
	DeploymentID                 string `json:"deployment_id"`
	StubID                       string `json:"stub_id"`
	AppID                        string `json:"app_id,omitempty"`
	Version                      uint   `json:"version"`
	Active                       bool   `json:"active"`
	Host                         string `json:"host"`
	Username                     string `json:"username"`
	Database                     string `json:"database,omitempty"`
	ConnectionString             string `json:"connection_string,omitempty"`
	ConnectionEnvName            string `json:"connection_env_name"`
	ConnectionStringSecret       string `json:"connection_string_secret"`
	UsernameSecret               string `json:"username_secret"`
	PasswordSecret               string `json:"password_secret"`
	DatabaseSecret               string `json:"database_secret,omitempty"`
	PooledConnectionString       string `json:"pooled_connection_string,omitempty"`
	PooledConnectionStringSecret string `json:"pooled_connection_string_secret,omitempty"`
}

type DatabaseReadiness struct {
	DeploymentID string `json:"deployment_id"`
	Ready        bool   `json:"ready"`
	TLSVerified  bool   `json:"tls_verified"`
	Error        string `json:"error,omitempty"`
}

// DatabaseBackupStatus is read from the backup volume, including after deletion.
type DatabaseBackupStatus struct {
	Kind             string          `json:"kind"`
	Status           string          `json:"status"`
	Error            string          `json:"error,omitempty"`
	Username         string          `json:"username,omitempty"`
	Database         string          `json:"database,omitempty"`
	ObservedAt       int64           `json:"observed_at"`
	ArchiveThrough   int64           `json:"archive_through"`
	RetentionDays    int             `json:"retention_days"`
	Repository       json.RawMessage `json:"repository,omitempty"`
	VolumeID         string          `json:"volume_id"`
	Stale            bool            `json:"stale"`
	RecoverableFrom  *time.Time      `json:"recoverable_from,omitempty"`
	RecoverableUntil *time.Time      `json:"recoverable_until,omitempty"`
}
