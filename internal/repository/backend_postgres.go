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

	// Run migrations
	if err := repo.migrate(); err != nil {
		log.Fatalf("failed to run backend migrations: %v", err)
	}

	return repo, nil
}

func (r *PostgresBackendRepository) migrate() error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	// Run migrations
	if err := goose.Up(r.client.DB, "./"); err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackendRepository) GetAllIdentities(ctx context.Context) ([]types.Identity, error) {
	var identities []types.Identity
	err := r.client.SelectContext(ctx, &identities, "SELECT * FROM identity_identity")
	if err != nil {
		log.Println("err: ", err)
		return nil, err
	}

	return identities, nil
}
