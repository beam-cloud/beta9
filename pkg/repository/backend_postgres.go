package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"

	pkgCommon "github.com/beam-cloud/beta9/pkg/common"
	_ "github.com/beam-cloud/beta9/pkg/repository/backend_postgres_migrations"
	"github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	ScheduledJobsChannel                = "jobs"
	listenToChannelPingInterval         = 10 * time.Second
	listenToChannelMinReconnectInterval = 1 * time.Second
	listenToChannelMaxReconnectInterval = 10 * time.Second
)

var PostgresDataError = pq.ErrorClass("22")

func GenerateDSN(config types.PostgresConfig) string {
	sslMode := "disable"
	if config.EnableTLS {
		sslMode = "require"
	}

	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		config.Host,
		config.Username,
		config.Password,
		config.Name,
		config.Port,
		sslMode,
		config.TimeZone,
	)
}

type PostgresBackendRepository struct {
	client         *sqlx.DB
	config         types.PostgresConfig
	eventRepo      EventRepository
	adminWorkspace *types.Workspace
}

func NewBackendPostgresRepository(config types.PostgresConfig, eventRepo EventRepository) (*PostgresBackendRepository, error) {
	dsn := GenerateDSN(config)

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &PostgresBackendRepository{
		client:    db,
		config:    config,
		eventRepo: eventRepo,
	}, nil
}

type GooseLogger struct {
	zerolog.Logger
}

func (l *GooseLogger) Fatalf(format string, v ...any) {
	l.Logger.Fatal().Msgf(format, v...)
}

func (r *PostgresBackendRepository) Migrate() error {
	goose.SetLogger(&GooseLogger{log.Logger.Level(zerolog.InfoLevel)})
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	if err := goose.Up(r.client.DB, "./"); err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) Ping() error {
	return r.client.Ping()
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

	// Generate a secure signing key for tasks
	signingKeyBytes := make([]byte, 32) // 256 bits
	if _, err := rand.Read(signingKeyBytes); err != nil {
		return types.Workspace{}, err
	}
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(signingKeyBytes)

	query := `
	INSERT INTO workspace (name, external_id, signing_key)
	VALUES ($1, $2, $3)
	RETURNING id, name, external_id, signing_key, created_at, updated_at;
	`

	var context types.Workspace
	if err := r.client.GetContext(ctx, &context, query, name, externalId, signingKey); err != nil {
		return types.Workspace{}, err
	}

	return context, nil
}

func (r *PostgresBackendRepository) GetWorkspaceByExternalId(ctx context.Context, externalId string) (types.Workspace, error) {
	var workspace types.Workspace

	query := `SELECT id, name, created_at, concurrency_limit_id, volume_cache_enabled, multi_gpu_enabled FROM workspace WHERE external_id = $1;`
	err := r.client.GetContext(ctx, &workspace, query, externalId)
	if err != nil {
		return types.Workspace{}, err
	}

	return workspace, nil
}

func (r *PostgresBackendRepository) GetWorkspaceByExternalIdWithSigningKey(ctx context.Context, externalId string) (types.Workspace, error) {
	var workspace types.Workspace

	query := `SELECT id, name, created_at, concurrency_limit_id, signing_key, volume_cache_enabled, multi_gpu_enabled FROM workspace WHERE external_id = $1;`
	err := r.client.GetContext(ctx, &workspace, query, externalId)
	if err != nil {
		return types.Workspace{}, err
	}

	return workspace, nil
}

func (r *PostgresBackendRepository) GetAdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	if r.adminWorkspace != nil {
		return r.adminWorkspace, nil
	}

	var adminWorkspace types.Workspace

	query := `SELECT w.id, w.name, w.created_at, w.concurrency_limit_id, w.volume_cache_enabled, w.multi_gpu_enabled
	FROM token t
	INNER JOIN workspace w ON t.workspace_id = w.id
	WHERE t.token_type = 'admin';`
	err := r.client.GetContext(ctx, &adminWorkspace, query)
	if err != nil {
		return nil, err
	}

	r.adminWorkspace = &adminWorkspace
	return r.adminWorkspace, nil
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
	SELECT t.id, t.external_id, t.key, t.created_at, t.updated_at, t.active, t.disabled_by_cluster_admin , t.token_type, t.reusable, t.workspace_id,
	       w.id "workspace.id", w.name "workspace.name", w.external_id "workspace.external_id", w.signing_key "workspace.signing_key", w.created_at "workspace.created_at",
		   w.updated_at "workspace.updated_at", w.volume_cache_enabled "workspace.volume_cache_enabled", w.multi_gpu_enabled "workspace.multi_gpu_enabled"
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
	AND token_type != 'worker'
    ORDER BY created_at DESC;
    `

	var tokens []types.Token
	err := r.client.SelectContext(ctx, &tokens, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return tokens, nil
}

func (r *PostgresBackendRepository) ToggleToken(ctx context.Context, workspaceId uint, extTokenId string) (types.Token, error) {
	query := `
	UPDATE token
	SET active = NOT active
	WHERE external_id = $1 AND workspace_id = $2
	RETURNING id, external_id, key, created_at, updated_at, active, token_type, reusable, workspace_id;
	`

	var token types.Token
	err := r.client.GetContext(ctx, &token, query, extTokenId, workspaceId)
	if err != nil {
		return types.Token{}, err
	}

	return token, nil
}

func (r *PostgresBackendRepository) DeleteToken(ctx context.Context, workspaceId uint, extTokenId string) error {
	query := `DELETE FROM token WHERE external_id = $1 AND workspace_id = $2;`
	_, err := r.client.ExecContext(ctx, query, extTokenId, workspaceId)
	return err
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

func (r *PostgresBackendRepository) UpdateTokenAsClusterAdmin(ctx context.Context, tokenId string, disabled bool) error {
	updateQuery := `
		UPDATE token
		SET disabled_by_cluster_admin = $1
		WHERE external_id = $2;
		`

	_, err := r.client.ExecContext(ctx, updateQuery, disabled, tokenId)
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

func (r *PostgresBackendRepository) GetObjectByExternalStubId(ctx context.Context, stubId string, workspaceId uint) (types.Object, error) {
	query := `
	SELECT o.id, o.external_id, o.hash, o.size, o.created_at
	FROM object o
	INNER JOIN stub s ON o.id = s.object_id
	WHERE s.external_id = $1 AND o.workspace_id = $2;
	`

	var object types.Object
	err := r.client.GetContext(ctx, &object, query, stubId, workspaceId)
	if err != nil {
		return types.Object{}, err
	}

	return object, nil
}

func (r *PostgresBackendRepository) UpdateObjectSizeByExternalId(ctx context.Context, externalId string, size int) error {
	query := `
	UPDATE object
	SET size = $2
	WHERE external_id = $1;
	`
	_, err := r.client.ExecContext(ctx, query, externalId, size)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) DeleteObjectByExternalId(ctx context.Context, externalId string) error {
	query := `DELETE FROM object WHERE external_id = $1;`
	_, err := r.client.ExecContext(ctx, query, externalId)
	if err != nil {
		return err
	}

	return nil
}

// Task

func (r *PostgresBackendRepository) handleTaskEvent(taskId string, callback func(*types.TaskWithRelated)) {
	task, err := r.GetTaskWithRelated(context.Background(), taskId)
	if err != nil {
		return
	}

	callback(task)
}

func (r *PostgresBackendRepository) CreateTask(ctx context.Context, params *types.TaskParams) (*types.Task, error) {
	if params.TaskId == "" {
		externalId, err := r.generateExternalId()
		if err != nil {
			return nil, err
		}

		params.TaskId = externalId
	}

	query := `
    INSERT INTO task (external_id, container_id, workspace_id, stub_id)
    VALUES ($1, $2, $3, $4)
    RETURNING id, external_id, status, container_id, workspace_id, stub_id, started_at, ended_at, created_at, updated_at;
    `

	var newTask types.Task

	if err := r.client.GetContext(ctx, &newTask, query, params.TaskId, params.ContainerId, params.WorkspaceId, params.StubId); err != nil {
		return &types.Task{}, err
	}

	go r.handleTaskEvent(params.TaskId, r.eventRepo.PushTaskCreatedEvent)
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

	go r.handleTaskEvent(externalId, r.eventRepo.PushTaskUpdatedEvent)
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
		if err, ok := err.(*pq.Error); ok && err.Code.Class() == PostgresDataError {
			return nil, nil
		}
		return nil, err
	}

	return &taskWithRelated, nil
}

func (r *PostgresBackendRepository) GetTaskByWorkspace(ctx context.Context, externalId string, workspace *types.Workspace) (*types.TaskWithRelated, error) {
	var taskWithRelated types.TaskWithRelated
	query := `
	SELECT
		w.external_id AS "workspace.external_id", w.name AS "workspace.name", t.*,
		s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config", s.type AS "stub.type", s.created_at AS "stub.created_at", s.updated_at AS "stub.updated_at"
	FROM task t
	JOIN workspace w ON t.workspace_id = w.id
	JOIN stub s ON t.stub_id = s.id
	WHERE
		t.external_id = $1
		AND w.id = $2;
    `
	err := r.client.GetContext(ctx, &taskWithRelated, query, externalId, workspace.Id)
	if err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code.Class() == PostgresDataError {
			return nil, nil
		}
		if err == sql.ErrNoRows {
			return nil, nil
		}
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

func (c *PostgresBackendRepository) listTaskWithRelatedQueryBuilder(filters types.TaskFilter) squirrel.SelectBuilder {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"t.*, w.external_id AS \"workspace.external_id\", w.name AS \"workspace.name\", s.external_id AS \"stub.external_id\", s.name AS \"stub.name\", s.config AS \"stub.config\", s.type AS \"stub.type\", d.external_id AS \"deployment.external_id\", d.name AS \"deployment.name\", d.version AS \"deployment.version\"",
	).From("task t").
		Join("workspace w ON t.workspace_id = w.id").
		Join("stub s ON t.stub_id = s.id").
		LeftJoin("deployment d ON s.id = d.stub_id").
		OrderBy("t.id DESC")

	// Apply filters
	if filters.WorkspaceID > 0 {
		qb = qb.Where(squirrel.Eq{"t.workspace_id": filters.WorkspaceID})
	}

	if len(filters.StubIds) > 0 {
		// Subquery to get the stub ids from the external ids
		stubIdsSubquery := squirrel.Select("id").From("stub").Where(squirrel.Eq{"external_id": filters.StubIds})
		qb = qb.Where(squirrel.Expr("s.id in (?)", stubIdsSubquery))
	}

	if len(filters.StubNames) > 0 {
		qb = qb.Where(squirrel.Eq{"s.name": filters.StubNames})
	}

	if len(filters.StubTypes) > 0 {
		qb = qb.Where(squirrel.Eq{"s.type": filters.StubTypes})
	}

	if len(filters.ContainerIds) > 0 {
		qb = qb.Where(squirrel.Eq{"t.container_id": filters.ContainerIds})
	}

	if len(filters.TaskIds) > 0 {
		validTaskIds := []string{}

		for _, taskId := range filters.TaskIds {
			// Filter out invalid UUIDs
			if _, err := uuid.Parse(taskId); err == nil {
				validTaskIds = append(validTaskIds, taskId)
			}
		}

		if len(validTaskIds) > 0 {
			qb = qb.Where(squirrel.Eq{"t.external_id": validTaskIds})
		}
	}

	if filters.CreatedAtStart != "" {
		qb = qb.Where(squirrel.GtOrEq{"t.created_at": filters.CreatedAtStart})
	}

	if filters.CreatedAtEnd != "" {
		qb = qb.Where(squirrel.LtOrEq{"t.created_at": filters.CreatedAtEnd})
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

	if filters.MinDuration > 0 {
		qb = qb.Where("EXTRACT(EPOCH FROM (t.ended_at - t.started_at)) * 1000 >= ?", filters.MinDuration)
	}

	if filters.MaxDuration > 0 {
		qb = qb.Where("EXTRACT(EPOCH FROM (t.ended_at - t.started_at)) * 1000 <= ?", filters.MaxDuration)
	}

	if filters.Limit > 0 {
		qb = qb.Limit(uint64(filters.Limit))
	}

	return qb
}

func (c *PostgresBackendRepository) GetTaskCountPerDeployment(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountPerDeployment, error) {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"d.name as deployment_name, COUNT(t.id) AS task_count",
	).
		From("deployment d").
		Join("task t ON d.stub_id = t.stub_id").
		GroupBy(
			"d.name",
		).OrderBy("task_count DESC")

	// Apply filters
	if filters.WorkspaceID > 0 {
		qb = qb.Where(squirrel.Eq{"t.workspace_id": filters.WorkspaceID})
	}

	if len(filters.StubIds) > 0 {
		qb = qb.Join("stub s ON t.stub_id = s.id")
		qb = qb.Where(squirrel.Eq{"s.external_id": filters.StubIds})
	}

	if filters.CreatedAtStart != "" {
		qb = qb.Where(squirrel.GtOrEq{"t.created_at": filters.CreatedAtStart})
	}

	if filters.CreatedAtEnd != "" {
		qb = qb.Where(squirrel.LtOrEq{"t.created_at": filters.CreatedAtEnd})
	}

	sql, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	var taskCounts []types.TaskCountPerDeployment
	err = c.client.SelectContext(ctx, &taskCounts, sql, args...)
	if err != nil {
		return nil, err
	}

	return taskCounts, nil
}

func (c *PostgresBackendRepository) AggregateTasksByTimeWindow(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountByTime, error) {
	interval := strings.ToLower(filters.Interval)
	if interval == "" {
		interval = "hour"
	}

	date_truc := fmt.Sprintf("DATE_TRUNC('%s', t.created_at)", interval)

	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		date_truc + ` as time, COUNT(t.id) as count,
			json_build_object(
				'COMPLETE', SUM(CASE WHEN t.status = 'COMPLETE' THEN 1 ELSE 0 END),
				'ERROR', SUM(CASE WHEN t.status = 'ERROR' THEN 1 ELSE 0 END),
				'CANCELLED', SUM(CASE WHEN t.status = 'CANCELLED' THEN 1 ELSE 0 END),
				'TIMEOUT', SUM(CASE WHEN t.status = 'TIMEOUT' THEN 1 ELSE 0 END),
				'RETRY', SUM(CASE WHEN t.status = 'RETRY' THEN 1 ELSE 0 END),
				'PENDING', SUM(CASE WHEN t.status = 'PENDING' THEN 1 ELSE 0 END),
				'RUNNING', SUM(CASE WHEN t.status = 'RUNNING' THEN 1 ELSE 0 END)
			)::json AS status_counts`,
	).
		From("task t").GroupBy("time").OrderBy("time DESC")

	// Apply filters
	if filters.WorkspaceID > 0 {
		qb = qb.Where(squirrel.Eq{"t.workspace_id": filters.WorkspaceID})
	}

	if len(filters.StubIds) > 0 {
		qb = qb.Join("stub s ON t.stub_id = s.id")
		qb = qb.Where(squirrel.Eq{"s.external_id": filters.StubIds})

	}

	if filters.CreatedAtStart != "" {
		qb = qb.Where(squirrel.GtOrEq{"t.created_at": filters.CreatedAtStart})
	}

	if filters.CreatedAtEnd != "" {
		qb = qb.Where(squirrel.LtOrEq{"t.created_at": filters.CreatedAtEnd})
	}

	sql, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	var taskCounts []types.TaskCountByTime
	err = c.client.SelectContext(ctx, &taskCounts, sql, args...)
	if err != nil {
		return nil, err
	}

	return taskCounts, nil
}

func (c *PostgresBackendRepository) ListTasksWithRelated(ctx context.Context, filters types.TaskFilter) ([]types.TaskWithRelated, error) {
	qb := c.listTaskWithRelatedQueryBuilder(filters)

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

func (c *PostgresBackendRepository) ListTasksWithRelatedPaginated(ctx context.Context, filters types.TaskFilter) (common.CursorPaginationInfo[types.TaskWithRelated], error) {
	qb := c.listTaskWithRelatedQueryBuilder(filters)

	page, err := common.Paginate(
		common.SquirrelCursorPaginator[types.TaskWithRelated]{
			Client:          c.client,
			SelectBuilder:   qb,
			SortOrder:       "DESC",
			SortColumn:      "created_at",
			SortQueryPrefix: "t",
			PageSize:        int(filters.Limit),
		},
		filters.Cursor,
	)
	if err != nil {
		return common.CursorPaginationInfo[types.TaskWithRelated]{}, err
	}

	return *page, nil
}

// Stub

func (r *PostgresBackendRepository) GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool, appId uint) (types.Stub, error) {
	var stub types.Stub

	// Serialize config to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return types.Stub{}, err
	}

	// @Luke slightly concerned that this is can get the wrong stub. If name + stubtype + objectId + config matches (Likely can happen from clone but haven't seen it yet)
	if !forceCreate {
		// Query to check if a stub with the same name, type, object_id, and config exists
		queryGet := `
    SELECT id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id
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
    INSERT INTO stub (name, type, config, object_id, workspace_id, app_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id;
    `
	if err := r.client.GetContext(ctx, &stub, queryCreate, name, stubType, string(configJSON), objectId, workspaceId, appId); err != nil {
		return types.Stub{}, err
	}

	return stub, nil
}

func (r *PostgresBackendRepository) UpdateStubConfig(ctx context.Context, stubId uint, config *types.StubConfigV1) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}

	query := `UPDATE stub SET config = $1 WHERE id = $2;`
	_, err = r.client.ExecContext(ctx, query, string(configJSON), stubId)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error) {
	var stub types.StubWithRelated
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		`s.id, s.external_id, s.name, s.type, s.config, s.config_version, s.object_id, s.workspace_id, s.created_at, s.updated_at, s.public, s.app_id,
	    w.id AS "workspace.id", w.external_id AS "workspace.external_id", w.name AS "workspace.name", w.created_at AS "workspace.created_at", w.updated_at AS "workspace.updated_at", w.signing_key AS "workspace.signing_key", w.volume_cache_enabled AS "workspace.volume_cache_enabled", w.multi_gpu_enabled AS "workspace.multi_gpu_enabled",
	    o.id AS "object.id", o.external_id AS "object.external_id", o.hash AS "object.hash", o.size AS "object.size", o.workspace_id AS "object.workspace_id", o.created_at AS "object.created_at"`,
	).
		From("stub s").
		Join("workspace w ON s.workspace_id = w.id").
		LeftJoin("object o ON s.object_id = o.id").
		Where(squirrel.Eq{"s.external_id": externalId})

	var stubFilters types.StubFilter
	types.ParseConditionFromQueryFilters(&stubFilters, queryFilters...)

	if stubFilters.WorkspaceID != "" {
		qb = qb.Where(squirrel.Eq{"w.external_id": stubFilters.WorkspaceID})
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	err = r.client.GetContext(ctx, &stub, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return &types.StubWithRelated{}, err
	}

	return &stub, nil
}

// Volume
func (c *PostgresBackendRepository) GetVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error) {
	var volume types.Volume

	queryGet := `SELECT id, external_id, name, workspace_id, created_at, updated_at FROM volume WHERE name = $1 AND workspace_id = $2;`

	if err := c.client.GetContext(ctx, &volume, queryGet, name, workspaceId); err != nil {
		return nil, err
	}

	return &volume, nil
}

func (c *PostgresBackendRepository) GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error) {
	v, err := c.GetVolume(ctx, workspaceId, name)
	if err == nil {
		return v, nil
	}

	queryCreate := `INSERT INTO volume (name, workspace_id) VALUES ($1, $2) RETURNING id, external_id, name, workspace_id, created_at, updated_at;`

	var volume types.Volume
	err = c.client.GetContext(ctx, &volume, queryCreate, name, workspaceId)
	if err != nil {
		return nil, err
	}

	return &volume, nil
}

func (c *PostgresBackendRepository) DeleteVolume(ctx context.Context, workspaceId uint, name string) error {
	query := `DELETE FROM volume WHERE name = $1 AND workspace_id = $2;`
	_, err := c.client.ExecContext(ctx, query, name, workspaceId)
	if err != nil {
		return err
	}
	return nil
}

func (c *PostgresBackendRepository) ListVolumesWithRelated(ctx context.Context, workspaceId uint) ([]types.VolumeWithRelated, error) {
	var volumes []types.VolumeWithRelated
	query := `
		SELECT v.id, v.external_id, v.name, v.workspace_id, v.created_at, v.updated_at, w.name as "workspace.name"
		FROM volume v
		JOIN workspace w ON v.workspace_id = w.id
		WHERE v.workspace_id = $1;
	`

	err := c.client.SelectContext(ctx, &volumes, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return volumes, nil
}

// Deployment

func (c *PostgresBackendRepository) GetLatestDeploymentByName(ctx context.Context, workspaceId uint, name string, stubType string, filterDeleted bool) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	filterDeletedQuery := ""
	if filterDeleted {
		filterDeletedQuery = "AND d.deleted_at IS NULL "
	}

	query := `
        SELECT
            d.*,
            s.external_id AS "stub.external_id",
            s.name AS "stub.name",
            s.config AS "stub.config"
        FROM deployment d
        JOIN stub s ON d.stub_id = s.id
        WHERE d.workspace_id = $1 AND d.name = $2 AND d.stub_type = $3 ` + filterDeletedQuery + `
        ORDER BY d.version DESC
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, name, stubType)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Return nil if no deployment found
		}
		return nil, err
	}

	return &deploymentWithRelated, nil
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
        WHERE d.workspace_id = $1 AND d.name = $2 AND d.version = $3 AND d.stub_type = $4 and d.deleted_at IS NULL
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, name, version, stubType)
	if err != nil {
		return nil, err
	}

	return &deploymentWithRelated, nil
}

// GetDeploymentBySubdomain retrieves the deployment by name, version, and stub group
// If version is 0, it will return the latest version
func (c *PostgresBackendRepository) GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	query := `
		WITH deployment_data AS (
			SELECT
				d.*,
				w.external_id AS "workspace.external_id", w.name AS "workspace.name",
				s.external_id AS "stub.external_id", s.name AS "stub.name", s.type AS "stub.type", s.config AS "stub.config"
			FROM deployment d
			JOIN workspace w ON d.workspace_id = w.id
			JOIN stub s ON d.stub_id = s.id
			WHERE
				d.subdomain = $1
				AND d.deleted_at IS NULL
		)
		SELECT
			d.id, d.name, d.external_id, d.version,
			d."workspace.external_id", d."workspace.name",
			d."stub.external_id", d."stub.name", d."stub.type", d."stub.config"
		FROM deployment_data d
		WHERE version = CASE
				WHEN $2 = 0 THEN (SELECT MAX(version) FROM deployment_data)
				ELSE $2
			END
		LIMIT 1;
	`
	err := c.client.GetContext(ctx, &deploymentWithRelated, query, subdomain, version)
	if err != nil {
		return nil, err
	}

	return &deploymentWithRelated, nil
}

func (c *PostgresBackendRepository) ListLatestDeploymentsWithRelatedPaginated(ctx context.Context, filters types.DeploymentFilter) (common.CursorPaginationInfo[types.DeploymentWithRelated], error) {
	query := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Select(
			"d.*",
			"s.external_id AS \"stub.external_id\"", "s.name AS \"stub.name\"", "s.config AS \"stub.config\"",
		).
		From("deployment d").
		Join(`(
			select name, max(version) as version, stub_type
			from deployment
			where workspace_id = ? and deleted_at is null
			group by name, stub_type
		)
		as latest
		on d.name = latest.name
		and d.stub_type = latest.stub_type
		and d.version = latest.version
		and d.workspace_id = ?`, filters.WorkspaceID, filters.WorkspaceID).
		Join(
			"stub s ON d.stub_id = s.id",
		)

	page, err := common.Paginate(
		common.SquirrelCursorPaginator[types.DeploymentWithRelated]{
			Client:          c.client,
			SelectBuilder:   query,
			SortOrder:       "DESC",
			SortColumn:      "created_at",
			SortQueryPrefix: "d",
			PageSize:        10,
		},
		filters.Cursor,
	)

	if err != nil {
		log.Error().Err(err).Msg("error paginating deployments")
		return common.CursorPaginationInfo[types.DeploymentWithRelated]{}, err
	}

	return *page, nil
}

func (c *PostgresBackendRepository) GetDeploymentByExternalId(ctx context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	query := `
        SELECT d.*,
               w.external_id AS "workspace.external_id", w.name AS "workspace.name",
               s.id AS "stub.id", s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config", s.type AS "stub.type"
        FROM deployment d
        JOIN workspace w ON d.workspace_id = w.id
        JOIN stub s ON d.stub_id = s.id
        WHERE d.workspace_id = $1 AND d.external_id = $2 and d.deleted_at IS NULL
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, deploymentExternalId)
	if err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code.Class() == PostgresDataError {
			return nil, nil
		}

		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return &deploymentWithRelated, nil
}

func (c *PostgresBackendRepository) GetDeploymentByStubExternalId(ctx context.Context, workspaceId uint, stubExternalId string) (*types.DeploymentWithRelated, error) {
	var deploymentWithRelated types.DeploymentWithRelated

	query := `
        SELECT d.*,
               w.external_id AS "workspace.external_id", w.name AS "workspace.name",
               s.id AS "stub.id", s.external_id AS "stub.external_id", s.name AS "stub.name", s.config AS "stub.config", s.type AS "stub.type"
        FROM deployment d
        JOIN workspace w ON d.workspace_id = w.id
        JOIN stub s ON d.stub_id = s.id
        WHERE d.workspace_id = $1 AND s.external_id = $2 and d.deleted_at IS NULL
        LIMIT 1;
    `

	err := c.client.GetContext(ctx, &deploymentWithRelated, query, workspaceId, stubExternalId)
	if err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code.Class() == PostgresDataError {
			return nil, nil
		}

		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return &deploymentWithRelated, nil
}

func (c *PostgresBackendRepository) listDeploymentsQueryBuilder(filters types.DeploymentFilter) squirrel.SelectBuilder {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"d.id, d.external_id, d.name, d.active, d.subdomain, d.workspace_id, d.stub_id, d.stub_type, d.version, d.created_at, d.updated_at",
		"w.external_id AS \"workspace.external_id\"", "w.name AS \"workspace.name\"",
		"s.external_id AS \"stub.external_id\"", "s.name AS \"stub.name\"", "s.config AS \"stub.config\"",
	).From("deployment d").
		Join("workspace w ON d.workspace_id = w.id").
		Join("stub s ON d.stub_id = s.id").
		OrderBy("d.created_at DESC")

	// Apply filters
	if !filters.ShowDeleted {
		qb = qb.Where("d.deleted_at IS NULL")
	}

	if filters.WorkspaceID != 0 {
		qb = qb.Where(squirrel.Eq{"d.workspace_id": filters.WorkspaceID})
	}

	if len(filters.StubIds) > 0 {
		qb = qb.Where(squirrel.Eq{"s.external_id": filters.StubIds})
	}

	if len(filters.StubType) > 0 {
		qb = qb.Where(squirrel.Eq{"d.stub_type": filters.StubType})
	}

	if filters.Subdomain != "" {
		qb = qb.Where(squirrel.Eq{"d.subdomain": filters.Subdomain})
	}

	if filters.SearchQuery != "" {
		if err := uuid.Validate(filters.SearchQuery); err == nil {
			qb = qb.Where(squirrel.Eq{"d.external_id": filters.SearchQuery})
		} else {
			qb = qb.Where(squirrel.Like{"d.name": "%" + filters.SearchQuery + "%"})
		}
	}

	if filters.Name != "" {
		qb = qb.Where(squirrel.Like{"d.name": fmt.Sprintf("%%%s%%", filters.Name)})
	}

	if filters.Active != nil {
		qb = qb.Where(squirrel.Eq{"d.active": filters.Active})
	}

	if filters.Version > 0 {
		qb = qb.Where(squirrel.Eq{"d.version": filters.Version})
	}

	if filters.Offset > 0 {
		qb = qb.Offset(uint64(filters.Offset))
	}

	if filters.Limit > 0 {
		qb = qb.Limit(uint64(filters.Limit))
	}

	if filters.CreatedAtStart != "" {
		qb = qb.Where(squirrel.GtOrEq{"d.created_at": filters.CreatedAtStart})
	}

	if filters.CreatedAtEnd != "" {
		qb = qb.Where(squirrel.LtOrEq{"d.created_at": filters.CreatedAtEnd})
	}

	if filters.MinContainersGTE > 0 {
		qb = qb.Where("COALESCE((s.config->'autoscaler'->>'min_containers')::int, 0) >= ?", filters.MinContainersGTE)
	}

	if filters.AppId != "" {
		qb = qb.Join("app a ON d.app_id = a.id")
		qb = qb.Where(squirrel.Eq{"a.external_id": filters.AppId})
	}

	return qb
}

func (c *PostgresBackendRepository) ListDeploymentsWithRelated(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	qb := c.listDeploymentsQueryBuilder(filters)

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

func (c *PostgresBackendRepository) ListDeploymentsPaginated(ctx context.Context, filters types.DeploymentFilter) (common.CursorPaginationInfo[types.DeploymentWithRelated], error) {
	qb := c.listDeploymentsQueryBuilder(filters)

	page, err := common.Paginate(
		common.SquirrelCursorPaginator[types.DeploymentWithRelated]{
			Client:          c.client,
			SelectBuilder:   qb,
			SortOrder:       "DESC",
			SortColumn:      "created_at",
			SortQueryPrefix: "d",
			PageSize:        int(filters.Limit),
		},
		filters.Cursor,
	)
	if err != nil {
		return common.CursorPaginationInfo[types.DeploymentWithRelated]{}, err
	}

	return *page, nil
}

func (c *PostgresBackendRepository) CreateDeployment(ctx context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string, appId uint) (*types.Deployment, error) {
	var deployment types.Deployment

	subdomain := generateSubdomain(name, stubType, workspaceId)
	queryCreate := `
		INSERT INTO deployment (name, active, subdomain, workspace_id, stub_id, version, stub_type, app_id)
		VALUES ($1, true, $2, $3, $4, $5, $6, $7)
		RETURNING id, external_id, name, active, subdomain, workspace_id, stub_id, stub_type, version, created_at, updated_at, app_id;
	`
	err := c.client.GetContext(ctx, &deployment, queryCreate, name, subdomain, workspaceId, stubId, version, stubType, appId)
	if err != nil {
		return nil, err
	}

	return &deployment, nil
}

func (c *PostgresBackendRepository) listStubsQueryBuilder(filters types.StubFilter) squirrel.SelectBuilder {
	qb := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).Select(
		"s.*",
		"w.external_id AS \"workspace.external_id\"", "w.name AS \"workspace.name\"",
	).From("stub s").
		Join("workspace w ON s.workspace_id = w.id")

	// Apply filters
	if filters.WorkspaceID != "" {
		qb = qb.Where(squirrel.Eq{"w.external_id": filters.WorkspaceID})
	}

	if len(filters.StubIds) > 0 {
		qb = qb.Where(squirrel.Eq{"s.external_id": filters.StubIds})
	}

	if len(filters.StubTypes) > 0 {
		qb = qb.Where(squirrel.Eq{"s.type": filters.StubTypes})
	}

	if filters.AppId != "" {
		qb = qb.Join("app a ON s.app_id = a.id")
		qb = qb.Where(squirrel.Eq{"a.external_id": filters.AppId})
	}

	return qb
}

func (c *PostgresBackendRepository) ListStubs(ctx context.Context, filters types.StubFilter) ([]types.StubWithRelated, error) {
	qb := c.listStubsQueryBuilder(filters)

	sql, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	var stubs []types.StubWithRelated
	err = c.client.SelectContext(ctx, &stubs, sql, args...)
	if err != nil {
		return nil, err
	}

	return stubs, nil
}

func (c *PostgresBackendRepository) ListStubsPaginated(ctx context.Context, filters types.StubFilter) (common.CursorPaginationInfo[types.StubWithRelated], error) {
	qb := c.listStubsQueryBuilder(filters)

	page, err := common.Paginate(
		common.SquirrelCursorPaginator[types.StubWithRelated]{
			Client:          c.client,
			SelectBuilder:   qb,
			SortOrder:       "DESC",
			SortColumn:      "created_at",
			SortQueryPrefix: "s",
			PageSize:        10,
		},
		filters.Cursor,
	)
	if err != nil {
		return common.CursorPaginationInfo[types.StubWithRelated]{}, err
	}
	return *page, nil
}

func (r *PostgresBackendRepository) UpdateDeployment(ctx context.Context, deployment types.Deployment) (*types.Deployment, error) {
	query := `
	UPDATE deployment
	SET name = $3, active = $4, version = $5, updated_at = CURRENT_TIMESTAMP
	WHERE id = $1 OR external_id = $2 and deleted_at IS NULL
	RETURNING id, external_id, name, active, version, workspace_id, stub_id, stub_type, created_at, updated_at;
	`

	var updated types.Deployment
	if err := r.client.GetContext(
		ctx, &updated, query,
		deployment.Id, deployment.ExternalId,
		deployment.Name, deployment.Active, deployment.Version,
	); err != nil {
		return &updated, err
	}

	return &updated, nil
}

func (r *PostgresBackendRepository) DeleteDeployment(ctx context.Context, deployment types.Deployment) error {
	query := `
	UPDATE deployment
	SET deleted_at = CURRENT_TIMESTAMP,
	ACTIVE = FALSE
	WHERE id = $1;
	`

	if _, err := r.client.ExecContext(ctx, query, deployment.Id); err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) GetConcurrencyLimit(ctx context.Context, concurrencyLimitId uint) (*types.ConcurrencyLimit, error) {
	var limit types.ConcurrencyLimit

	query := `SELECT gpu_limit, cpu_millicore_limit, created_at, updated_at FROM concurrency_limit WHERE id = $1;`
	err := r.client.GetContext(ctx, &limit, query, concurrencyLimitId)
	if err != nil {
		return nil, err
	}

	return &limit, nil
}

func (r *PostgresBackendRepository) CreateConcurrencyLimit(ctx context.Context, workspaceId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error) {
	query := `
	INSERT INTO concurrency_limit (gpu_limit, cpu_millicore_limit)
	VALUES ($1, $2)
	RETURNING id, gpu_limit, cpu_millicore_limit, created_at, updated_at;
	`

	var limit types.ConcurrencyLimit
	if err := r.client.GetContext(ctx, &limit, query, gpuLimit, cpuMillicoreLimit); err != nil {
		return nil, err
	}

	// Add concurrency limit to workspace
	queryUpdateWorkspace := `
	UPDATE workspace
	SET concurrency_limit_id = $1
	WHERE id = $2;
	`

	if _, err := r.client.ExecContext(ctx, queryUpdateWorkspace, limit.Id, workspaceId); err != nil {
		return nil, err
	}

	return &limit, nil
}

func (r *PostgresBackendRepository) UpdateConcurrencyLimit(ctx context.Context, concurrencyLimitId uint, gpuLimit uint32, cpuMillicoreLimit uint32) (*types.ConcurrencyLimit, error) {
	query := `
	UPDATE concurrency_limit
	SET gpu_limit = $2, cpu_millicore_limit = $3, updated_at = CURRENT_TIMESTAMP
	WHERE id = $1
	RETURNING id, gpu_limit, cpu_millicore_limit, created_at, updated_at;
	`

	var limit types.ConcurrencyLimit
	if err := r.client.GetContext(ctx, &limit, query, concurrencyLimitId, gpuLimit, cpuMillicoreLimit); err != nil {
		return nil, err
	}

	return &limit, nil
}

func (r *PostgresBackendRepository) GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error) {
	var limit types.ConcurrencyLimit

	query := `SELECT cl.id, cl.gpu_limit, cl.cpu_millicore_limit, cl.created_at, cl.updated_at FROM concurrency_limit cl join workspace w on cl.id = w.concurrency_limit_id WHERE w.external_id = $1;`
	err := r.client.GetContext(ctx, &limit, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return &limit, nil
}

func (r *PostgresBackendRepository) DeleteConcurrencyLimit(ctx context.Context, workspaceId types.Workspace) error {
	updateWorkspaceQuery := `UPDATE workspace SET concurrency_limit_id = NULL WHERE id = $1;`
	deleteConcurrencyLimitQuery := `DELETE FROM concurrency_limit WHERE id = $1;`

	tx, err := r.client.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.Exec(updateWorkspaceQuery, workspaceId.Id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(deleteConcurrencyLimitQuery, workspaceId.ConcurrencyLimitId)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func validateEnvironmentVariableName(name string) error {
	/* https://docs.aws.amazon.com/opsworks/latest/APIReference/API_EnvironmentVariable.html#:~:text=(Required)%20The%20environment%20variable's%20name,with%20a%20letter%20or%20underscore.

	The environment variable's name, which can consist of up to 64 characters and must be specified. The name can contain upper- and lowercase letters, numbers, and underscores (_), but it must start with a letter or underscore.
	*/

	regexp := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
	if !regexp.MatchString(name) {
		return fmt.Errorf("invalid environment variable name: %s", name)
	}

	return nil
}

func (r *PostgresBackendRepository) CreateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, name string, value string) (*types.Secret, error) {
	query := `
	INSERT INTO workspace_secret (name, value, workspace_id, last_updated_by)
	VALUES ($1, $2, $3, $4)
	RETURNING id, external_id, name, workspace_id, last_updated_by, created_at, updated_at;
	`

	err := validateEnvironmentVariableName(name)
	if err != nil {
		return nil, err
	}

	signingKey, err := pkgCommon.ParseSigningKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	encryptedValue, err := pkgCommon.Encrypt(signingKey, value)
	if err != nil {
		return nil, err
	}

	var secret types.Secret
	if err := r.client.GetContext(ctx, &secret, query, name, encryptedValue, workspace.Id, tokenId); err != nil {
		return nil, err
	}

	return &secret, nil
}

func (r *PostgresBackendRepository) GetSecretByName(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error) {
	var secret types.Secret

	query := `SELECT id, external_id, name, value, workspace_id, last_updated_by, created_at, updated_at FROM workspace_secret WHERE name = $1 AND workspace_id = $2;`
	err := r.client.GetContext(ctx, &secret, query, name, workspace.Id)
	if err != nil {
		return nil, err
	}

	return &secret, nil
}

func (r *PostgresBackendRepository) GetSecretsByName(ctx context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error) {
	query := `SELECT id, external_id, name, value, workspace_id, last_updated_by, created_at, updated_at FROM workspace_secret WHERE name = ANY($1) AND workspace_id = $2;`

	var secrets []types.Secret
	err := r.client.SelectContext(ctx, &secrets, query, pq.Array(names), workspace.Id)
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func (r *PostgresBackendRepository) GetSecretByNameDecrypted(ctx context.Context, workspace *types.Workspace, name string) (*types.Secret, error) {
	secret, err := r.GetSecretByName(ctx, workspace, name)
	if err != nil {
		return nil, err
	}

	signingKey, err := pkgCommon.ParseSigningKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	decryptedSecret, err := pkgCommon.Decrypt(signingKey, secret.Value)
	if err != nil {
		return nil, err
	}

	secret.Value = string(decryptedSecret)

	return secret, nil
}

func (r *PostgresBackendRepository) GetSecretsByNameDecrypted(ctx context.Context, workspace *types.Workspace, names []string) ([]types.Secret, error) {
	secrets, err := r.GetSecretsByName(ctx, workspace, names)
	if err != nil {
		return nil, err
	}

	signingKey, err := pkgCommon.ParseSigningKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	for i, secret := range secrets {
		decryptedSecret, err := pkgCommon.Decrypt(signingKey, secret.Value)
		if err != nil {
			return nil, err
		}
		secrets[i].Value = string(decryptedSecret)
	}

	return secrets, nil
}

func (r *PostgresBackendRepository) ListSecrets(ctx context.Context, workspace *types.Workspace) ([]types.Secret, error) {
	query := `SELECT id, external_id, name, workspace_id, last_updated_by, created_at, updated_at FROM workspace_secret WHERE workspace_id = $1;`

	var secrets []types.Secret
	err := r.client.SelectContext(ctx, &secrets, query, workspace.Id)
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func (r *PostgresBackendRepository) DeleteSecret(ctx context.Context, workspace *types.Workspace, name string) error {
	query := `DELETE FROM workspace_secret WHERE name = $1 AND workspace_id = $2;`
	_, err := r.client.ExecContext(ctx, query, name, workspace.Id)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) UpdateSecret(ctx context.Context, workspace *types.Workspace, tokenId uint, secretId string, value string) (*types.Secret, error) {
	query := `
	UPDATE workspace_secret
	SET value = $3, last_updated_by = $4, updated_at = CURRENT_TIMESTAMP
	WHERE name = $1 AND workspace_id = $2
	RETURNING id, external_id, name, workspace_id, last_updated_by, created_at, updated_at;
	`

	signingKey, err := pkgCommon.ParseSigningKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	encryptedValue, err := pkgCommon.Encrypt(signingKey, value)
	if err != nil {
		return nil, err
	}

	var secret types.Secret
	if err := r.client.GetContext(ctx, &secret, query, secretId, workspace.Id, encryptedValue, tokenId); err != nil {
		return nil, err
	}

	return &secret, nil
}

func (r *PostgresBackendRepository) CreateScheduledJob(ctx context.Context, scheduledJob *types.ScheduledJob) (*types.ScheduledJob, error) {
	payloadJSON, err := json.Marshal(scheduledJob.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal scheduled job payload: %v", err)
	}

	if matched, _ := regexp.MatchString(`@reboot|@restart`, scheduledJob.Schedule); matched {
		return nil, fmt.Errorf("invalid schedule: %s", scheduledJob.Schedule)
	}

	var jobId *uint64
	query := `SELECT cron.schedule($1, $2, 'NOTIFY ' || $3 || ', ''' || $4 || '''');`
	if err := r.client.GetContext(ctx, &jobId, query, scheduledJob.JobName, scheduledJob.Schedule, ScheduledJobsChannel, payloadJSON); err != nil {
		return nil, err
	}

	var s types.ScheduledJob
	query = `
	INSERT INTO scheduled_job (job_id, job_name, job_schedule, job_payload, deployment_id, stub_id)
	VALUES ($1, $2, $3, $4::jsonb, $5, $6)
	RETURNING id, external_id, job_id, job_schedule, job_payload, stub_id, created_at, updated_at, deleted_at;
	`
	if err := r.client.GetContext(ctx, &s, query, jobId, scheduledJob.JobName, scheduledJob.Schedule, payloadJSON, scheduledJob.DeploymentId, scheduledJob.StubId); err != nil {
		r.unscheduleCron(ctx, *jobId)
		return nil, err
	}

	return &s, nil
}

func (r *PostgresBackendRepository) GetScheduledJob(ctx context.Context, deploymentId uint) (*types.ScheduledJob, error) {
	var scheduledJob types.ScheduledJob
	query := `
	SELECT sj.*
	FROM scheduled_job sj
	JOIN cron.job j ON sj.job_id = j.jobid
	JOIN deployment d ON sj.deployment_id = d.id
	WHERE sj.deployment_id = $1 AND sj.deleted_at IS NULL;
	`
	err := r.client.GetContext(ctx, &scheduledJob, query, deploymentId)
	if err != nil {
		return nil, err
	}

	return &scheduledJob, nil
}

func (r *PostgresBackendRepository) DeleteScheduledJob(ctx context.Context, scheduledJob *types.ScheduledJob) error {
	updateQuery := `
	UPDATE scheduled_job
	SET deleted_at = CURRENT_TIMESTAMP
	WHERE id = $1;
	`
	if _, err := r.client.ExecContext(ctx, updateQuery, scheduledJob.Id); err != nil {
		return err
	}

	if err := r.unscheduleCron(ctx, scheduledJob.JobId); err != nil {
		return fmt.Errorf("failed to unschedule job: %v", err)
	}

	return nil
}

func (r *PostgresBackendRepository) unscheduleCron(ctx context.Context, jobId uint64) error {
	unscheduleQuery := `SELECT cron.unschedule($1::integer);`
	if _, err := r.client.ExecContext(ctx, unscheduleQuery, jobId); err != nil {
		return err
	}
	return nil
}

func (r *PostgresBackendRepository) DeletePreviousScheduledJob(ctx context.Context, deployment *types.Deployment) error {
	// Get previous deployment if exists
	if previousDeployment, err := r.GetDeploymentByNameAndVersion(ctx, deployment.WorkspaceId, deployment.Name, deployment.Version-1, deployment.StubType); err == nil {
		// Found previous deployment. Get scheduled job of previous deployment.
		if previousScheduledJob, err := r.GetScheduledJob(ctx, previousDeployment.Id); err == nil {
			// Found previous scheduled job. Delete it.
			return r.DeleteScheduledJob(ctx, previousScheduledJob)
		}
	}

	return nil
}

func (r *PostgresBackendRepository) ListenToChannel(ctx context.Context, channel string) (<-chan string, error) {
	dsn := GenerateDSN(r.config)

	listener := pq.NewListener(dsn, listenToChannelMinReconnectInterval, listenToChannelMaxReconnectInterval, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Error().Err(err).Msg("failed to create new listener")
		}
	})

	if err := listener.Listen(channel); err != nil {
		return nil, err
	}

	ch := make(chan string)

	go func() {
		defer func() {
			close(ch)
			listener.Unlisten(channel)
			listener.Close()
		}()

		for {
			select {
			case n := <-listener.Notify:
				if n != nil {
					ch <- n.Extra
				}
			case <-time.After(listenToChannelPingInterval):
				listener.Ping()
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (r *PostgresBackendRepository) GetTaskMetrics(ctx context.Context, periodStart, periodEnd time.Time) (types.TaskMetrics, error) {
	query := `
	SELECT status, count(id)
	FROM task
	WHERE created_at >= $1
	AND created_at <= $2
	GROUP BY status;
	`

	rows, err := r.client.QueryContext(ctx, query, periodStart, periodEnd)
	if err != nil {
		return types.TaskMetrics{}, err
	}

	var metrics types.TaskMetrics
	metrics.TaskByStatusCounts = make(map[string]int)

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return types.TaskMetrics{}, err
		}

		metrics.TaskByStatusCounts[status] = count
	}

	return metrics, nil
}

func (r *PostgresBackendRepository) GetOrCreateApp(ctx context.Context, workspaceId uint, appName string) (*types.App, error) {
	// Check if the app already exists
	var app types.App
	query := `SELECT id, external_id, name, workspace_id, created_at, updated_at FROM app WHERE name=$1 and workspace_id=$2;`
	err := r.client.GetContext(ctx, &app, query, appName, workspaceId)
	if err == nil {
		return &app, nil
	} else if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	query = `
	INSERT INTO app (name, workspace_id, description)
	VALUES ($1, $2, $3)
	RETURNING id, external_id, name, description, workspace_id, created_at, updated_at;
	`

	if err := r.client.GetContext(ctx, &app, query, appName, workspaceId, ""); err != nil {
		return nil, err
	}

	return &app, nil
}

func (r *PostgresBackendRepository) ListApps(ctx context.Context, workspaceId uint) ([]types.App, error) {
	var apps []types.App

	query := `SELECT * FROM app WHERE workspace_id = $1;`
	err := r.client.SelectContext(ctx, &apps, query, workspaceId)
	if err != nil {
		return nil, err
	}

	return apps, nil
}
