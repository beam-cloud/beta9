package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func credentialTestDatabase(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("BETA9_CREDENTIAL_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_CREDENTIAL_TEST_DSN for an isolated PostgreSQL database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TEMP TABLE workspace (id SERIAL PRIMARY KEY,external_id TEXT,name TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),updated_at TIMESTAMPTZ DEFAULT NOW(),signing_key TEXT,volume_cache_enabled BOOLEAN DEFAULT FALSE,multi_gpu_enabled BOOLEAN DEFAULT FALSE,storage_id INT,concurrency_limit_id INT);
 CREATE TEMP TABLE workspace_storage (id INT,external_id TEXT,bucket_name TEXT,access_key TEXT,secret_key TEXT,endpoint_url TEXT,region TEXT,created_at TIMESTAMPTZ,updated_at TIMESTAMPTZ);
 CREATE TEMP TABLE token (id SERIAL PRIMARY KEY,external_id TEXT,key TEXT,key_hash TEXT,worker_id TEXT NOT NULL DEFAULT '',expires_at TIMESTAMPTZ,workspace_id INT,created_at TIMESTAMPTZ DEFAULT NOW(),updated_at TIMESTAMPTZ DEFAULT NOW(),active BOOLEAN DEFAULT TRUE,disabled_by_cluster_admin BOOLEAN DEFAULT FALSE,reusable BOOLEAN DEFAULT TRUE,token_type TEXT);
 CREATE TEMP TABLE workspace_secret (workspace_id INT,value TEXT);`)
	require.NoError(t, err)
	return db
}

func TestCredentialBackfillIntegration(t *testing.T) {
	db := credentialTestDatabase(t)
	ctx := context.Background()
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	key, err := common.ParseSecretKey(signingKey)
	require.NoError(t, err)
	ciphertext, err := common.Encrypt(key, "existing-secret")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO workspace(id,external_id,name,signing_key)VALUES(1,'workspace-one','one',$1)`, signingKey)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO workspace_secret VALUES(1,$1)`, ciphertext)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO token(external_id,key,workspace_id,token_type)VALUES('customer','customer-original',1,'workspace'),('primary','primary-original',1,'workspace_primary')`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{EncryptionKey: signingKey}}
	before, workspace, err := repo.AuthorizeToken(ctx, "customer-original")
	require.NoError(t, err)
	require.Equal(t, "customer", before.ExternalId)
	require.Equal(t, signingKey, *workspace.SigningKey)
	require.NoError(t, repo.ProtectCredentials(ctx))
	require.NoError(t, repo.ProtectCredentials(ctx))
	after, workspace, err := repo.AuthorizeToken(ctx, "customer-original")
	require.NoError(t, err)
	require.Equal(t, before.ExternalId, after.ExternalId)
	require.Equal(t, signingKey, *workspace.SigningKey)
	var stored, secret string
	require.NoError(t, db.Get(&stored, `SELECT key FROM token WHERE external_id='customer'`))
	require.Equal(t, "hash:"+tokenDigest("customer-original"), stored)
	require.NoError(t, db.Get(&secret, `SELECT value FROM workspace_secret`))
	require.Equal(t, ciphertext, secret)
	plain, err := common.Decrypt(key, secret)
	require.NoError(t, err)
	require.Equal(t, "existing-secret", plain)
	recovered, err := repo.GetTokenByExternalId(ctx, 1, "primary")
	require.NoError(t, err)
	require.Equal(t, "primary-original", recovered.Key)
	metadata, err := repo.GetTokenByExternalId(ctx, 1, "customer")
	require.NoError(t, err)
	require.Empty(t, metadata.Key)
	active, err := repo.TokenIsActive(ctx, "customer", "customer-original")
	require.NoError(t, err)
	require.True(t, active)
	require.NoError(t, repo.RevokeTokenByExternalId(ctx, "customer"))
	active, err = repo.TokenIsActive(ctx, "customer", "customer-original")
	require.NoError(t, err)
	require.False(t, active)
}

func TestWorkerCredentialLeaseIntegration(t *testing.T) {
	db := credentialTestDatabase(t)
	ctx := context.Background()
	workspaceID := uint(1)
	key := "sk_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{EncryptionKey: key}}
	token, err := repo.CreateToken(ctx, workspaceID, types.TokenTypeWorker, true)
	require.NoError(t, err)
	require.NoError(t, repo.BindWorkerToken(ctx, token.ExternalId, "worker-one"))
	require.Error(t, repo.BindWorkerToken(ctx, token.ExternalId, "worker-two"))
	require.Error(t, repo.RenewWorkerToken(ctx, token.ExternalId, "worker-two"))
	require.NoError(t, repo.RenewWorkerToken(ctx, token.ExternalId, "worker-one"))
	tokenRow, err := repo.GetTokenByExternalId(ctx, workspaceID, token.ExternalId)
	require.NoError(t, err)
	require.Equal(t, "worker-one", tokenRow.WorkerID)
	require.WithinDuration(t, time.Now().Add(10*time.Minute), *tokenRow.ExpiresAt, time.Minute)
	require.NoError(t, repo.RevokeWorkerTokens(ctx, "worker-one"))
	require.Error(t, repo.RenewWorkerToken(ctx, token.ExternalId, "worker-one"))
	require.Error(t, repo.BindWorkerToken(ctx, token.ExternalId, "worker-one"))
}

func TestCredentialBackfillResumesAfterInterruption(t *testing.T) {
	db := credentialTestDatabase(t)
	_, err := db.Exec(`INSERT INTO token(external_id,key,token_type,workspace_id)VALUES('first','first-original','workspace',1),('second','second-original','workspace',1);
CREATE FUNCTION pg_temp.reject_second_token() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.external_id='second' THEN RAISE EXCEPTION 'interrupted'; END IF; RETURN NEW; END $$;
CREATE TRIGGER interrupt_backfill BEFORE UPDATE ON token FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_second_token();`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db}
	require.Error(t, repo.ProtectCredentials(context.Background()))
	var first, second string
	require.NoError(t, db.Get(&first, `SELECT key FROM token WHERE external_id='first'`))
	require.NoError(t, db.Get(&second, `SELECT key FROM token WHERE external_id='second'`))
	require.Equal(t, "hash:"+tokenDigest("first-original"), first)
	require.Equal(t, "second-original", second)
	_, err = db.Exec(`DROP TRIGGER interrupt_backfill ON token`)
	require.NoError(t, err)
	require.NoError(t, repo.ProtectCredentials(context.Background()))
	require.NoError(t, repo.ProtectCredentials(context.Background()))
	require.NoError(t, db.Get(&second, `SELECT key FROM token WHERE external_id='second'`))
	require.Equal(t, "hash:"+tokenDigest("second-original"), second)
}

func TestUserRestrictedTokenIsNotRecoverable(t *testing.T) {
	db := credentialTestDatabase(t)
	key := "sk_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{EncryptionKey: key}}
	ctx := context.Background()
	user, err := CreateExpiringToken(ctx, repo, 1, types.TokenTypeWorkspaceRestricted, 0)
	require.NoError(t, err)
	require.NotEmpty(t, user.Key)
	require.NotNil(t, user.ExpiresAt)
	stored, err := repo.GetTokenByExternalId(ctx, 1, user.ExternalId)
	require.NoError(t, err)
	require.Empty(t, stored.Key)
	server, err := repo.CreateToken(ctx, 1, types.TokenTypeWorkspaceRestricted, true)
	require.NoError(t, err)
	stored, err = repo.GetTokenByExternalId(ctx, 1, server.ExternalId)
	require.NoError(t, err)
	require.Equal(t, server.Key, stored.Key)
}
