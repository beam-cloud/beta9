package repository

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// sealedArray matches a one-element array holding a sealed form of want.
type sealedArray struct {
	repo *PostgresBackendRepository
	want string
}

func (m sealedArray) Match(v driver.Value) bool {
	var sealed pq.StringArray
	if err := sealed.Scan(v); err != nil || len(sealed) != 1 || !strings.HasPrefix(sealed[0], encryptedCredentialPrefix) {
		return false
	}
	value, err := m.repo.openCredential(sealed[0])
	return err == nil && value == m.want
}

func TestEncryptStoredCredentialsUsesOnlyTheLockConnection(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.config.EncryptCredentials = true
	// The lock holds the only connection, so a backfill query sent through the
	// pool would wait for it forever.
	r.client.SetMaxOpenConns(1)

	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(credentialEncryptionLockID).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	mock.ExpectQuery(`SELECT NOT indisvalid`).
		WillReturnRows(sqlmock.NewRows([]string{"invalid"}).AddRow(false))
	mock.ExpectExec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_key_hash`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT id, key AS value FROM token`).WithArgs(int64(0), credentialEncryptionBatch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "value"}).AddRow(1, "token-key").AddRow(2, encryptedCredentialPrefix+"sealed"))
	mock.ExpectExec(`UPDATE token t SET key = v.sealed, key_hash = v.hash`).
		WithArgs(pq.Array([]int64{1}), pq.Array([]string{"token-key"}), sealedArray{r, "token-key"}, pq.Array([]string{tokenKeyHash("token-key")})).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT id, key AS value FROM token`).WithArgs(int64(2), credentialEncryptionBatch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "value"}))
	mock.ExpectQuery(`SELECT id, signing_key AS value FROM workspace`).WithArgs(int64(0), credentialEncryptionBatch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "value"}).AddRow(3, "sk_signing-key"))
	mock.ExpectExec(`UPDATE workspace t SET signing_key = v.sealed\s+FROM`).
		WithArgs(pq.Array([]int64{3}), pq.Array([]string{"sk_signing-key"}), sealedArray{r, "sk_signing-key"}, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT id, signing_key AS value FROM workspace`).WithArgs(int64(3), credentialEncryptionBatch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "value"}))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(credentialEncryptionLockID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, r.EncryptStoredCredentials(ctx))
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}
