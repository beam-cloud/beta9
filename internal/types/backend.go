package types

import (
	"database/sql"
	"time"
)

type Context struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Name       string    `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type Token struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Key        string    `db:"key"`
	Active     bool      `db:"active"`
	ContextId  uint      `db:"context_id"` // Foreign key to Context
	Context    *Context  `db:"context"`    // Pointer to associated Context
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type Volume struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Name       string    `db:"name"`
	ContextId  uint      `db:"context_id"` // Foreign key to Context
	CreatedAt  time.Time `db:"created_at"`
}

const (
	DeploymentStatusPending string = "PENDING"
	DeploymentStatusReady   string = "READY"
	DeploymentStatusError   string = "ERROR"
	DeploymentStatusStopped string = "STOPPED"
)

type Deployment struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Version    uint      `db:"version"`
	Status     string    `db:"status"`
	ContextId  uint      `db:"context_id"` // Foreign key to Context
	StubId     uint      `db:"stub_id"`    // Foreign key to Stub
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type Object struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Hash       string    `db:"hash"`
	Size       int64     `db:"size"`
	ContextId  uint      `db:"context_id"` // Foreign key to Context
	CreatedAt  time.Time `db:"created_at"`
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

type Task struct {
	Id          uint         `db:"id"`
	ExternalId  string       `db:"external_id"`
	Status      string       `db:"status"`
	ContainerId string       `db:"container_id"`
	StartedAt   time.Time    `db:"started_at"`
	EndedAt     sql.NullTime `db:"ended_at"`
	ContextId   uint         `db:"context_id"` // Foreign key to Context
	StubId      uint         `db:"stub_id"`    // Foreign key to Stub
	CreatedAt   time.Time    `db:"created_at"`
	UpdatedAt   time.Time    `db:"updated_at"`
}

type Stub struct {
	Id         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	Name       string    `db:"name"`
	Type       string    `db:"type"`
	Config     string    `db:"config"`
	ObjectId   uint      `db:"object_id"`  // Foreign key to Object
	ContextId  uint      `db:"context_id"` // Foreign key to Context
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}
