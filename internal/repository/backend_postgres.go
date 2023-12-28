package repository

import (
	"context"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	_ "github.com/beam-cloud/beam/internal/repository/backend_postgres_migrations"
	"github.com/beam-cloud/beam/internal/types"
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

func (r *PostgresBackendRepository) ListContexts(ctx context.Context) ([]types.Context, error) {
	var contexts []types.Context

	query := `SELECT id, name, external_id, created_at, updated_at FROM context;`
	err := r.client.SelectContext(ctx, &contexts, query)
	if err != nil {
		return nil, err
	}

	return contexts, nil
}

func (r *PostgresBackendRepository) CreateContext(ctx context.Context, newContext types.Context) (types.Context, error) {
	query := `
	INSERT INTO context (name, external_id)
	VALUES (:name, :external_id)
	RETURNING id, name, external_id, created_at, updated_at;
	`

	stmt, err := r.client.PrepareNamedContext(ctx, query)
	if err != nil {
		return types.Context{}, err
	}
	defer stmt.Close()

	var context types.Context
	if err := stmt.GetContext(ctx, &context, newContext); err != nil {
		return types.Context{}, err
	}

	return context, nil
}

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
