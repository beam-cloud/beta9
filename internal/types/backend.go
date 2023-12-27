package types

import (
	"database/sql"
	"time"
)

type Identity struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
	Active     bool      `db:"active"`
}

type IdentityToken struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	Key        string    `db:"key"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
	Active     bool      `db:"active"`
	IdentityID uint      `db:"identity_id"` // Foreign key to Identity
}

type Task struct {
	ID         uint         `db:"id"`
	ExternalID string       `db:"external_id"`
	CreatedAt  time.Time    `db:"created_at"`
	StartedAt  time.Time    `db:"started_at"`
	EndedAt    sql.NullTime `db:"ended_at"`   // Can be NULL if the task hasn't ended
	ContextID  uint         `db:"context_id"` // Foreign key to Context
}

const (
	DeploymentStatusPending string = "PENDING"
	DeploymentStatusReady   string = "READY"
	DeploymentStatusError   string = "ERROR"
	DeploymentStatusStopped string = "STOPPED"
)

type Deployment struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	Version    uint      `db:"version"`
	Status     string    `db:"status"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
	ContextID  uint      `db:"context_id"` // Foreign key to Context
}

const (
	ContextTypeDeployment string = "DEPLOYMENT"
	ContextTypeFunction   string = "FUNCTION"
)

type Context struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	Type       string    `db:"type"`
	Name       string    `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
	ObjectID   uint      `db:"object_id"` // Foreign key to Object
}

type Object struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	Hash       string    `db:"hash"`
	Size       int64     `db:"size"`
	CreatedAt  time.Time `db:"created_at"`
}

type Volume struct {
	ID         uint      `db:"id"`
	ExternalID string    `db:"external_id"`
	Name       string    `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
}
