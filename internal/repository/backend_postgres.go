package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	_ "github.com/beam-cloud/beam/internal/repository/backend_postgres_migrations"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
)

type PostgresBackendRepository struct {
	client *sqlx.DB
}

func NewBackendPostgresRepository() (*PostgresBackendRepository, error) {
	host := common.Secrets().Get("DB_HOST")
	port := common.Secrets().GetInt("DB_PORT")
	user := common.Secrets().Get("DB_USER")
	password := common.Secrets().Get("DB_PASS")
	dbName := common.Secrets().Get("DB_NAME")

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=UTC", host, user, password, dbName, port)
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}

	repo := &PostgresBackendRepository{
		client: db,
	}

	if err := repo.migrate(); err != nil {
		log.Fatalf("failed to run backend migrations: %v", err)
	}

	return repo, nil
}

func (r *PostgresBackendRepository) migrate() error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	if err := goose.Up(r.client.DB, "./"); err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) generateExternalId() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Context

func (r *PostgresBackendRepository) ListWorkspaces(ctx context.Context) ([]types.Workspace, error) {
	var workspaces []types.Workspace

	query := `SELECT id, name, external_id, created_at, updated_at FROM workspace;`
	err := r.client.SelectContext(ctx, &workspaces, query)
	if err != nil {
		return nil, err
	}

	return workspaces, nil
}

func (r *PostgresBackendRepository) CreateWorkspace(ctx context.Context) (types.Workspace, error) {
	name := uuid.New().String()[:6] // Generate a short UUID for the workspace name

	externalId, err := r.generateExternalId()
	if err != nil {
		return types.Workspace{}, err
	}

	query := `
	INSERT INTO workspace (name, external_id)
	VALUES ($1, $2)
	RETURNING id, name, external_id, created_at, updated_at;
	`

	var context types.Workspace
	if err := r.client.GetContext(ctx, &context, query, name, externalId); err != nil {
		return types.Workspace{}, err
	}

	return context, nil
}

// Token

const tokenLength = 64

func (r *PostgresBackendRepository) CreateToken(ctx context.Context, workspaceId uint) (types.Token, error) {
	externalId, err := r.generateExternalId()
	if err != nil {
		return types.Token{}, err
	}

	// Generate a new key for the token
	randomBytes := make([]byte, tokenLength)
	if _, err := rand.Read(randomBytes); err != nil {
		return types.Token{}, err
	}
	key := base64.URLEncoding.EncodeToString(randomBytes)

	query := `
	INSERT INTO token (external_id, key, active, workspace_id)
	VALUES ($1, $2, $3, $4)
	RETURNING id, external_id, key, created_at, updated_at, active, workspace_id;
	`

	var token types.Token
	if err := r.client.GetContext(ctx, &token, query, externalId, key, true, workspaceId); err != nil {
		return types.Token{}, err
	}

	return token, nil
}

func (r *PostgresBackendRepository) AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error) {
	query := `
	SELECT t.id, t.external_id, t.key, t.created_at, t.updated_at, t.active, t.workspace_id,
	       w.id "workspace.id", w.name "workspace.name", w.external_id "workspace.external_id", w.created_at "workspace.created_at", w.updated_at "workspace.updated_at"
	FROM token t
	INNER JOIN workspace w ON t.workspace_id = w.id
	WHERE t.key = $1 AND t.active = TRUE;
	`

	var token types.Token
	var workspace types.Workspace
	token.Workspace = &workspace

	if err := r.client.GetContext(ctx, &token, query, tokenKey); err != nil {
		return nil, nil, err
	}

	return &token, &workspace, nil
}

// Object

func (r *PostgresBackendRepository) CreateObject(ctx context.Context, hash string, size int64, workspaceId uint) (types.Object, error) {
	query := `
    INSERT INTO object (hash, size, workspace_id)
    VALUES ($1, $2, $3)
    RETURNING id, external_id, hash, size, created_at, workspace_id;
    `

	var newObject types.Object
	if err := r.client.GetContext(ctx, &newObject, query, hash, size, workspaceId); err != nil {
		return types.Object{}, err
	}

	return newObject, nil
}

func (r *PostgresBackendRepository) GetObjectByHash(ctx context.Context, hash string, workspaceId uint) (types.Object, error) {
	var object types.Object

	query := `SELECT id, external_id, hash, size, created_at FROM object WHERE hash = $1 AND workspace_id = $2;`
	err := r.client.GetContext(ctx, &object, query, hash, workspaceId)
	if err != nil {
		return types.Object{}, err
	}

	return object, nil
}

func (r *PostgresBackendRepository) GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error) {
	var object types.Object

	query := `SELECT id, external_id, hash, size, created_at FROM object WHERE external_id = $1 AND workspace_id = $2;`
	err := r.client.GetContext(ctx, &object, query, externalId, workspaceId)
	if err != nil {
		return types.Object{}, err
	}

	return object, nil
}

// Task

func (r *PostgresBackendRepository) CreateTask(ctx context.Context, containerId string, contextId, stubId uint) (*types.Task, error) {
	query := `
    INSERT INTO task (container_id, workspace_id, stub_id)
    VALUES ($1, $2, $3)
    RETURNING id, external_id, status, container_id, workspace_id, stub_id, started_at, ended_at, created_at, updated_at;
    `

	var newTask types.Task
	if err := r.client.GetContext(ctx, &newTask, query, containerId, contextId, stubId); err != nil {
		return &types.Task{}, err
	}

	return &newTask, nil
}

func (r *PostgresBackendRepository) UpdateTask(ctx context.Context, externalId string, updatedTask types.Task) (*types.Task, error) {
	query := `
	UPDATE task
	SET status = $2, container_id = $3, started_at = $4, ended_at = $5, workspace_id = $6, stub_id = $7, updated_at = CURRENT_TIMESTAMP
	WHERE external_id = $1
	RETURNING id, external_id, status, container_id, workspace_id, stub_id, started_at, ended_at, created_at, updated_at;
	`

	var task types.Task
	if err := r.client.GetContext(ctx, &task, query,
		externalId, updatedTask.Status, updatedTask.ContainerId,
		updatedTask.StartedAt, updatedTask.EndedAt,
		updatedTask.WorkspaceId, updatedTask.StubId); err != nil {
		return &types.Task{}, err
	}

	return &task, nil
}

func (r *PostgresBackendRepository) DeleteTask(ctx context.Context, externalId string) error {
	query := `DELETE FROM task WHERE external_id = $1;`
	_, err := r.client.ExecContext(ctx, query, externalId)
	return err
}

func (r *PostgresBackendRepository) GetTask(ctx context.Context, externalId string) (*types.Task, error) {
	var task types.Task
	query := `SELECT id, external_id, status, container_id, started_at, ended_at, workspace_id, stub_id, created_at, updated_at FROM task WHERE external_id = $1;`
	err := r.client.GetContext(ctx, &task, query, externalId)
	if err != nil {
		return &types.Task{}, err
	}

	return &task, nil
}

func (r *PostgresBackendRepository) ListTasks(ctx context.Context) ([]types.Task, error) {
	var tasks []types.Task
	query := `SELECT id, external_id, status, container_id, started_at, ended_at, workspace_id, stub_id, created_at, updated_at FROM task;`
	err := r.client.SelectContext(ctx, &tasks, query)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (r *PostgresBackendRepository) ListTasksWithRelated(ctx context.Context) ([]types.TaskWithRelated, error) {
	var tasks []types.TaskWithRelated
	query := `
	SELECT w.external_id AS "workspace.external_id", w.name AS "workspace.name", s.external_id AS "stub.external_id", s.name AS "stub.name", t.*
	FROM task t
	JOIN workspace w ON t.workspace_id = w.id
	JOIN stub s ON t.stub_id = s.id;
	`
	err := r.client.SelectContext(ctx, &tasks, query)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// Stub

func (r *PostgresBackendRepository) GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint) (types.Stub, error) {
	var stub types.Stub

	// Serialize config to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return types.Stub{}, err
	}

	// Query to check if a stub with the same name, type, object_id, and config exists
	queryGet := `
    SELECT id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at
    FROM stub
    WHERE name = $1 AND type = $2 AND object_id = $3 AND config::jsonb = $4::jsonb;
    `
	err = r.client.GetContext(ctx, &stub, queryGet, name, stubType, objectId, string(configJSON))
	if err == nil {
		// Stub found, return it
		return stub, nil
	}

	// Stub not found, create a new one
	queryCreate := `
    INSERT INTO stub (name, type, config, object_id, workspace_id)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at;
    `
	if err := r.client.GetContext(ctx, &stub, queryCreate, name, stubType, string(configJSON), objectId, workspaceId); err != nil {
		return types.Stub{}, err
	}

	return stub, nil
}

func (r *PostgresBackendRepository) GetStubByExternalId(ctx context.Context, externalId string, workspaceId uint) (*types.Stub, error) {
	var stub types.Stub

	query := `SELECT id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at FROM stub WHERE external_id = $1 AND workspace_id = $2;`
	err := r.client.GetContext(ctx, &stub, query, externalId, workspaceId)
	if err != nil {
		return &types.Stub{}, err
	}

	return &stub, nil
}
