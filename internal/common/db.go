package common

import (
	"fmt"
	"log"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose/v3"
)

type SQLClient struct {
	*sqlx.DB
}

func NewPostgresClient(config types.PostgresConfig) (*SQLClient, error) {
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

	client := &SQLClient{db}

	if err := client.migrate(); err != nil {
		log.Fatalf("failed to run sql migrations: %v", err)
	}

	return client, nil
}

func (c *SQLClient) migrate() error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	if err := goose.Up(c.DB.DB, "./"); err != nil {
		return err
	}

	return nil
}
