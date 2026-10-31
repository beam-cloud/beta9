package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestTokenAttributesMigrationIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_CREDENTIAL_TEST_DSN")
	if dsn == "" {
		t.Skip("requires an isolated PostgreSQL database")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE workspace(signing_key VARCHAR(128));CREATE TEMP TABLE token(key VARCHAR(128));INSERT INTO workspace VALUES('original-signing-key');INSERT INTO token VALUES('original-token')`)
	require.NoError(t, err)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, upTokenAttributes(ctx, tx))
	require.NoError(t, tx.Commit())
	var token, key string
	require.NoError(t, db.QueryRow(`SELECT key FROM token`).Scan(&token))
	require.NoError(t, db.QueryRow(`SELECT signing_key FROM workspace`).Scan(&key))
	require.Equal(t, "original-token", token)
	require.Equal(t, "original-signing-key", key)
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.Error(t, downTokenAttributes(ctx, tx))
	require.NoError(t, tx.Rollback())
}
