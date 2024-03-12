package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/Masterminds/squirrel"
	_ "github.com/beam-cloud/beta9/internal/repository/backend_postgres_migrations"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
)

type PostgresBackendRepository struct {
	client *sqlx.DB
}

func NewBackendPostgresRepository(config types.PostgresConfig) (*PostgresBackendRepository, error) {
	sslMode := "disable"
	if config.EnableTLS {
		sslMode = "require"
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		config.Host,
		config.Username,
		config.Password,
		config.Name,
		config.Port,
		sslMode,
		config.TimeZone,
	)
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

func (r *PostgresBackendRepository) GetWorkspaceByExternalId(ctx context.Context, externalId string) (types.Workspace, error) {
	var workspace types.Workspace

	query := `SELECT id, name, created_at FROM workspace WHERE external_id = $1;`
	err := r.client.GetContext(ctx, &workspace, query, externalId)
	if err != nil {
		return types.Workspace{}, err
	}

	return workspace, nil
}

// Token

const tokenLength = 64

func (r *PostgresBackendRepository) CreateToken(ctx context.Context, workspaceId uint, tokenType string, reusable bool) (types.Token, error) {
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
	INSERT INTO token (external_id, key, active, token_type, reusable, workspace_id)
	VALUES ($1, $2, $3, $4, $5, $6)
	RETURNING id, external_id, key, created_at, updated_at, active, token_type, reusable, workspace_id;
	`

	var token types.Token
	if err := r.client.GetContext(ctx, &token, query, externalId, key, true, tokenType, reusable, workspaceId); err != nil {
		return types.Token{}, err
	}

	return token, nil
}

func (r *PostgresBackendRepository) AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error) {
	query := `
	SELECT t.id, t.external_id, t.key, t.created_at, t.updated_at, t.active, t.token_type, t.reusable, t.workspace_id,
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

	// After successful authorization, check if the token is reusable.
	// If it's not, update the token to expire it by setting active to false.
	if !token.Reusable {
		updateQuery := `
        UPDATE token
        SET active = FALSE
        WHERE id = $1;
        `

		if _, err := r.client.ExecContext(ctx, updateQuery, token.Id); err != nil {
			return nil, nil, err
		}
	}

	return &token, &workspace, nil
}

func (r *PostgresBackendRepository) RetrieveActiveToken(ctx context.Context, workspaceId uint) (*types.Token, error) {
	query := `
	SELECT id, external_id, key, created_at, updated_at, active, token_type, reusable, workspace_id
	FROM token
	WHERE workspace_id = $1 AND active = TRUE
	ORDER BY updated_at DESC
	LIMIT 1;
	`

	var token types.Token
	err := r.client.GetContext(ctx, &token, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

func (r *PostgresBackendRepository) ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error) {
	query := `
    SELECT id, external_id, key, created_at, updated_at, active, token_type, reusable, workspace_id
    FROM token
    WHERE workspace_id = $1
    ORDER BY created_at DESC;
    `

	var tokens []types.Token
	err := r.client.SelectContext(ctx, &tokens, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return tokens, nil
}

func (r *PostgresBackendRepository) RevokeTokenByExternalId(ctx context.Context, externalId string) error {
	updateQuery := `
    UPDATE token
    SET active = FALSE
    WHERE external_id = $1;
    `

	_, err := r.client.ExecContext(ctx, updateQuery, externalId)
	if err != nil {
		return err
	}

	return nil
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

func (r *PostgresBackendRepository) GetTaskWithRelated(ctx context.Context, externalId string) (*types.TaskWithRelated, error) {
	var taskWithRelated types.TaskWithRelated
	query := `
    SELECT w.external_id AS "workspace.external_id", w.name AS "workspace.name",
           s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config", t.*
    FROM task t
    JOIN workspace w ON t.workspace_id = w.id
    JOIN stub s ON t.stub_id = s.id
    WHERE t.external_id = $1;
    `
	err := r.client.GetContext(ctx, &taskWithRelated, query, externalId)
	if err != nil {
		return nil, err
	}

	return &taskWithRelated, nil
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

func (c *PostgresBackendRepository) ListTasksWithRelated(ctx context.Context, filters types.TaskFilter) ([]types.TaskWithRelated, error) {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"t.*, w.external_id AS \"workspace.external_id\", w.name AS \"workspace.name\", s.external_id AS \"stub.external_id\", s.name AS \"stub.name\"",
	).From("task t").
		Join("workspace w ON t.workspace_id = w.id").
		Join("stub s ON t.stub_id = s.id").OrderBy("t.id DESC")

	// Apply filters
	if filters.WorkspaceID > 0 {
		qb = qb.Where(squirrel.Eq{"t.workspace_id": filters.WorkspaceID})
	}

	if filters.StubId != "" {
		qb = qb.Where(squirrel.Eq{"s.external_id": filters.StubId})
	}

	if filters.StubType != "" {
		stubTypes := strings.Split(filters.StubType, ",")
		if len(stubTypes) > 0 {
			qb = qb.Where(squirrel.Eq{"s.type": stubTypes})
		}
	}

	if filters.Offset > 0 {
		qb = qb.Offset(uint64(filters.Offset))
	}

	if filters.Status != "" {
		statuses := strings.Split(filters.Status, ",")
		if len(statuses) > 0 {
			qb = qb.Where(squirrel.Eq{"t.status": statuses})
		}
	}

	if filters.Limit > 0 {
		qb = qb.Limit(uint64(filters.Limit))
	}

	sql, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	var tasks []types.TaskWithRelated
	err = c.client.SelectContext(ctx, &tasks, sql, args...)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// Stub

func (r *PostgresBackendRepository) GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool) (types.Stub, error) {
	var stub types.Stub

	// Serialize config to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return types.Stub{}, err
	}

	if !forceCreate {
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

func (r *PostgresBackendRepository) GetStubByExternalId(ctx context.Context, externalId string) (*types.StubWithRelated, error) {
	var stub types.StubWithRelated

	query := `
	SELECT
	    s.id, s.external_id, s.name, s.type, s.config, s.config_version, s.object_id, s.workspace_id, s.created_at, s.updated_at,
	    w.id AS "workspace.id", w.external_id AS "workspace.external_id", w.name AS "workspace.name", w.created_at AS "workspace.created_at", w.updated_at AS "workspace.updated_at",
	    o.id AS "object.id", o.external_id AS "object.external_id", o.hash AS "object.hash", o.size AS "object.size", o.workspace_id AS "object.workspace_id", o.created_at AS "object.created_at"
	FROM stub s
	JOIN workspace w ON s.workspace_id = w.id
	LEFT JOIN object o ON s.object_id = o.id
	WHERE s.external_id = $1;
	`
	err := r.client.GetContext(ctx, &stub, query, externalId)
	if err != nil {
		return &types.StubWithRelated{}, err
	}

	return &stub, nil
}

func (c *PostgresBackendRepository) GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error) {
	var volume types.Volume

	queryGet := `SELECT id, external_id, name, workspace_id, created_at FROM volume WHERE name = $1 AND workspace_id = $2;`

	err := c.client.GetContext(ctx, &volume, queryGet, name, workspaceId)
	if err == nil {
		return &volume, err
	}

	queryCreate := `INSERT INTO volume (name, workspace_id) VALUES ($1, $2) RETURNING id, external_id, name, workspace_id, created_at;`

	err = c.client.GetContext(ctx, &volume, queryCreate, name, workspaceId)
	if err != nil {
		return &types.Volume{}, err
	}

	return &volume, nil
}

// Deployment

func (c *PostgresBackendRepository) GetLatestDeploymentByName(ctx context.Context, workspaceId uint, name string, stubType string) (*types.Deployment, error) {
	var deployment types.Deployment

	query := `
        SELECT id, external_id, name, active, workspace_id, stub_id, version, created_at, updated_at
        FROM deployment
        WHERE workspace_id = $1 AND name = $2 AND stub_type = $3
        ORDER BY version DESC
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deployment, query, workspaceId, name, stubType)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Return nil if no deployment found
		}
		return nil, err
	}

	return &deployment, nil
}

func (c *PostgresBackendRepository) GetDeploymentByNameAndVersion(ctx context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	query := `
        SELECT d.*, 
               w.external_id AS "workspace.external_id", w.name AS "workspace.name", 
               s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config"
        FROM deployment d
        JOIN workspace w ON d.workspace_id = w.id
        JOIN stub s ON d.stub_id = s.id
        WHERE d.workspace_id = $1 AND d.name = $2 AND d.version = $3 AND d.stub_type = $4
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, name, version, stubType)
	if err != nil {
		return nil, err
	}

	return &deploymentWithRelated, nil
}

func (c *PostgresBackendRepository) GetDeploymentByExternalId(ctx context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	query := `
        SELECT d.*, 
               w.external_id AS "workspace.external_id", w.name AS "workspace.name", 
               s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config"
        FROM deployment d
        JOIN workspace w ON d.workspace_id = w.id
        JOIN stub s ON d.stub_id = s.id
        WHERE d.workspace_id = $1 AND d.external_id = $2
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, deploymentExternalId)
	if err != nil {
		return nil, err
	}

	return &deploymentWithRelated, nil
}

func (c *PostgresBackendRepository) ListDeployments(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"d.id, d.external_id, d.name, d.active, d.workspace_id, d.stub_id, d.stub_type, d.version, d.created_at, d.updated_at",
		"w.external_id AS \"workspace.external_id\"", "w.name AS \"workspace.name\"",
		"s.external_id AS \"stub.external_id\"", "s.name AS \"stub.name\"", "s.config AS \"stub.config\"",
	).From("deployment d").
		Join("workspace w ON d.workspace_id = w.id").
		Join("stub s ON d.stub_id = s.id").OrderBy("d.created_at DESC")

	// Apply filters
	qb = qb.Where(squirrel.Eq{"d.workspace_id": filters.WorkspaceID})

	if filters.StubType != "" {
		qb = qb.Where(squirrel.Eq{"d.stub_type": filters.StubType})
	}

	if filters.Name != "" {
		qb = qb.Where(squirrel.Eq{"d.name": filters.Name})
	}

	if filters.Offset > 0 {
		qb = qb.Offset(uint64(filters.Offset))
	}

	if filters.Limit > 0 {
		qb = qb.Limit(uint64(filters.Limit))
	}

	sql, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	var deployments []types.DeploymentWithRelated
	err = c.client.SelectContext(ctx, &deployments, sql, args...)
	if err != nil {
		return nil, err
	}

	return deployments, nil
}

func (c *PostgresBackendRepository) CreateDeployment(ctx context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string) (*types.Deployment, error) {
	var deployment types.Deployment

	query := `
        INSERT INTO deployment (name, active, workspace_id, stub_id, version, stub_type)
        VALUES ($1, true, $2, $3, $4, $5)
        RETURNING id, external_id, name, active, workspace_id, stub_id, version, created_at, updated_at;
    `

	err := c.client.GetContext(ctx, &deployment, query, name, workspaceId, stubId, version, stubType)
	if err != nil {
		return nil, err
	}

	return &deployment, nil
}
