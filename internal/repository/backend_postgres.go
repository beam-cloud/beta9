package repository

import (
	"context"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

	return &PostgresBackendRepository{
		client: db,
	}, nil
}

func (r *PostgresBackendRepository) GetAllUsers(ctx context.Context) ([]types.User, error) {
	var users []types.User
	err := r.client.SelectContext(ctx, &users, "SELECT * FROM users")
	if err != nil {
		log.Println("err: ", err)
		return nil, err
	}

	return users, nil
}
