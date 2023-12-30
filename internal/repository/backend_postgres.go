package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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

func (r *PostgresBackendRepository) generateExternalID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Context

func (r *PostgresBackendRepository) ListContexts(ctx context.Context) ([]types.Context, error) {
	var contexts []types.Context

	query := `SELECT id, name, external_id, created_at, updated_at FROM context;`
	err := r.client.SelectContext(ctx, &contexts, query)
	if err != nil {
		return nil, err
	}

	return contexts, nil
}

func (r *PostgresBackendRepository) CreateContext(ctx context.Context) (types.Context, error) {
	name := uuid.New().String()[:6] // Generate a short UUID for the context name

	externalID, err := r.generateExternalID()
	if err != nil {
		return types.Context{}, err
	}

	query := `
	INSERT INTO context (name, external_id)
	VALUES ($1, $2)
	RETURNING id, name, external_id, created_at, updated_at;
	`

	var context types.Context
	if err := r.client.GetContext(ctx, &context, query, name, externalID); err != nil {
		return types.Context{}, err
	}

	return context, nil
}

// Token

const tokenLength = 64

func (r *PostgresBackendRepository) CreateToken(ctx context.Context, contextID uint) (types.Token, error) {
	externalID, err := r.generateExternalID()
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
	INSERT INTO token (external_id, key, active, context_id)
	VALUES ($1, $2, $3, $4)
	RETURNING id, external_id, key, created_at, updated_at, active, context_id;
	`

	var token types.Token
	if err := r.client.GetContext(ctx, &token, query, externalID, key, true, contextID); err != nil {
		return types.Token{}, err
	}

	return token, nil
}

func (r *PostgresBackendRepository) AuthorizeToken(ctx context.Context, tokenKey string) (*types.Token, *types.Context, error) {
	query := `
	SELECT t.id, t.external_id, t.key, t.created_at, t.updated_at, t.active, t.context_id,
	       c.id "context.id", c.name "context.name", c.external_id "context.external_id", c.created_at "context.created_at", c.updated_at "context.updated_at"
	FROM token t
	INNER JOIN context c ON t.context_id = c.id
	WHERE t.key = $1 AND t.active = TRUE;
	`

	var token types.Token
	var context types.Context
	token.Context = &context

	if err := r.client.GetContext(ctx, &token, query, tokenKey); err != nil {
		return nil, nil, err
	}

	return &token, &context, nil
}

// Object

func (r *PostgresBackendRepository) CreateObject(ctx context.Context, newObj types.Object) (types.Object, error) {
	query := `
	INSERT INTO object (external_id, hash, size, context_id)
	VALUES (:external_id, :hash, :size, :context_id)
	RETURNING id, external_id, hash, size, created_at, context_id;
	`

	stmt, err := r.client.PrepareNamedContext(ctx, query)
	if err != nil {
		return types.Object{}, err
	}
	defer stmt.Close()

	var object types.Object
	if err := stmt.GetContext(ctx, &object, newObj); err != nil {
		return types.Object{}, err
	}

	return object, nil
}

// Task

func (r *PostgresBackendRepository) CreateTask(ctx context.Context, containerID string, contextID, stubID uint) (types.Task, error) {
	query := `
    INSERT INTO task (container_id, context_id, stub_id)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id, external_id, status, container_id, context_id, stub_id, started_at, ended_at, created_at, updated_at;
    `

	var newTask types.Task
	if err := r.client.GetContext(ctx, &newTask, query, containerID, contextID, stubID); err != nil {
		return types.Task{}, err
	}

	return newTask, nil
}

func (r *PostgresBackendRepository) UpdateTask(ctx context.Context, taskID uint, updatedTask types.Task) (types.Task, error) {
	query := `
	UPDATE task
	SET status = $2, container_id = $3, started_at = $4, ended_at = $5, context_id = $6, stub_id = $7, updated_at = CURRENT_TIMESTAMP
	WHERE id = $1
	RETURNING id, external_id, status, container_id, context_id, stub_id, started_at, ended_at, created_at, updated_at;
	`

	var task types.Task
	if err := r.client.GetContext(ctx, &task, query, taskID, updatedTask.Status, updatedTask.ContainerID, updatedTask.StartedAt, updatedTask.EndedAt, updatedTask.ContextID, updatedTask.StubID); err != nil {
		return types.Task{}, err
	}

	return task, nil
}

func (r *PostgresBackendRepository) DeleteTask(ctx context.Context, taskID uint) error {
	query := `DELETE FROM task WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, taskID)
	return err
}

func (r *PostgresBackendRepository) ListTasks(ctx context.Context) ([]types.Task, error) {
	var tasks []types.Task
	query := `SELECT id, external_id, status, container_id, started_at, ended_at, context_id, stub_id, created_at, updated_at FROM task;`
	err := r.client.SelectContext(ctx, &tasks, query)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}
