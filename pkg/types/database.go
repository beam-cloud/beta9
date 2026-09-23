package types

import "errors"

// Managed database services.

var (
	ErrDatabaseKind             = errors.New("kind must be postgres, redis, mysql or mongo")
	ErrDatabaseExists           = errors.New("a service with this name already exists")
	ErrDatabaseNotFound         = errors.New("database service not found")
	ErrDatabaseImageUnsupported = errors.New("image service is not available on this gateway")
)

type CreateDatabaseParams struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Size     string `json:"size"`
	Pool     string `json:"pool"`
	AlwaysOn bool   `json:"always_on"`
	// Millicores and megabytes; zero means the product default.
	Cpu    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

type DatabaseServiceInfo struct {
	Name                   string `json:"name"`
	Kind                   string `json:"kind"`
	DeploymentID           string `json:"deployment_id"`
	StubID                 string `json:"stub_id"`
	AppID                  string `json:"app_id,omitempty"`
	Version                uint   `json:"version"`
	Active                 bool   `json:"active"`
	Host                   string `json:"host"`
	Username               string `json:"username"`
	Database               string `json:"database,omitempty"`
	ConnectionString       string `json:"connection_string,omitempty"`
	ConnectionEnvName      string `json:"connection_env_name"`
	ConnectionStringSecret string `json:"connection_string_secret"`
	UsernameSecret         string `json:"username_secret"`
	PasswordSecret         string `json:"password_secret"`
	DatabaseSecret         string `json:"database_secret,omitempty"`
}
