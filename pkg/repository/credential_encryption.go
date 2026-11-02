package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	pkgCommon "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const (
	encryptedCredentialPrefix = "enc:v1:"
	credentialEncryptionBatch = 500
	// Arbitrary constant for pg_try_advisory_lock so one gateway backfills.
	credentialEncryptionLockID = 7364019
)

func tokenKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (r *PostgresBackendRepository) credentialKey() ([]byte, error) {
	if !strings.HasPrefix(r.config.EncryptionKey, "sk_") {
		return nil, errors.New("database encryption key is not configured")
	}
	return pkgCommon.ParseSecretKey(r.config.EncryptionKey)
}

// sealCredential encrypts a value for storage when credential encryption is on.
func (r *PostgresBackendRepository) sealCredential(value string) (string, error) {
	if !r.config.EncryptCredentials || value == "" || strings.HasPrefix(value, encryptedCredentialPrefix) {
		return value, nil
	}
	key, err := r.credentialKey()
	if err != nil {
		return "", err
	}
	sealed, err := pkgCommon.Encrypt(key, value)
	if err != nil {
		return "", err
	}
	return encryptedCredentialPrefix + sealed, nil
}

// openCredential reads both encrypted and legacy plaintext values.
func (r *PostgresBackendRepository) openCredential(value string) (string, error) {
	sealed, ok := strings.CutPrefix(value, encryptedCredentialPrefix)
	if !ok {
		return value, nil
	}
	key, err := r.credentialKey()
	if err != nil {
		return "", err
	}
	return pkgCommon.Decrypt(key, sealed)
}

func (r *PostgresBackendRepository) openSigningKey(workspace *types.Workspace) error {
	if workspace == nil || workspace.SigningKey == nil {
		return nil
	}
	key, err := r.openCredential(*workspace.SigningKey)
	if err != nil {
		return err
	}
	workspace.SigningKey = &key
	return nil
}

// EncryptStoredCredentials encrypts token keys and workspace signing keys
// still stored in plaintext.
func (r *PostgresBackendRepository) EncryptStoredCredentials(ctx context.Context) error {
	if !r.config.EncryptCredentials {
		return nil
	}
	conn, err := r.client.Connx(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var locked bool
	if err := conn.GetContext(ctx, &locked, `SELECT pg_try_advisory_lock($1)`, credentialEncryptionLockID); err != nil || !locked {
		return err
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, credentialEncryptionLockID)

	// Sealed keys are looked up by hash. An interrupted build leaves an invalid
	// index that IF NOT EXISTS would keep, so drop it and build again.
	var invalid bool
	if err := conn.GetContext(ctx, &invalid, `SELECT COALESCE((SELECT NOT indisvalid FROM pg_index WHERE indexrelid = to_regclass('idx_token_key_hash')), false)`); err != nil {
		return err
	}
	if invalid {
		if _, err := conn.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS idx_token_key_hash`); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_key_hash ON token (key_hash) WHERE key_hash IS NOT NULL`); err != nil {
		return err
	}

	if err := r.encryptColumn(ctx, conn, "token", "key", true); err != nil {
		return err
	}
	return r.encryptColumn(ctx, conn, "workspace", "signing_key", false)
}

// encryptColumn runs on the lock connection so the backfill never waits for
// a second pool connection while holding one.
func (r *PostgresBackendRepository) encryptColumn(ctx context.Context, conn *sqlx.Conn, table, column string, hashed bool) error {
	set := column + ` = v.sealed`
	if hashed {
		set += `, key_hash = v.hash`
	}
	selectQuery := `SELECT id, ` + column + ` AS value FROM ` + table + ` WHERE id > $1 AND ` + column + ` IS NOT NULL ORDER BY id LIMIT $2`
	updateQuery := `
	UPDATE ` + table + ` t SET ` + set + `
	FROM (SELECT unnest($1::bigint[]) AS id, unnest($2::text[]) AS value, unnest($3::text[]) AS sealed, unnest($4::text[]) AS hash) v
	WHERE t.id = v.id AND t.` + column + ` = v.value`

	for lastID := int64(0); ; {
		var rows []struct {
			Id    int64  `db:"id"`
			Value string `db:"value"`
		}
		if err := conn.SelectContext(ctx, &rows, selectQuery, lastID, credentialEncryptionBatch); err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		var ids []int64
		var values, sealed, hashes []string
		for _, row := range rows {
			lastID = row.Id
			if row.Value == "" || strings.HasPrefix(row.Value, encryptedCredentialPrefix) {
				continue
			}
			value, err := r.sealCredential(row.Value)
			if err != nil {
				return err
			}
			ids = append(ids, row.Id)
			values = append(values, row.Value)
			sealed = append(sealed, value)
			hashes = append(hashes, tokenKeyHash(row.Value))
		}
		if len(ids) == 0 {
			continue
		}
		if _, err := conn.ExecContext(ctx, updateQuery, pq.Array(ids), pq.Array(values), pq.Array(sealed), pq.Array(hashes)); err != nil {
			return err
		}
	}
}
