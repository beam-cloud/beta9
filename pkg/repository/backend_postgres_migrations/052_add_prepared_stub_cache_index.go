package backend_postgres_migrations

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddPreparedStubCacheIndex, downAddPreparedStubCacheIndex)
}

func upAddPreparedStubCacheIndex(ctx context.Context, db *sql.DB) error {
	if err := stripStubConfigNulls(ctx, db); err != nil {
		return err
	}
	return createIndexConcurrently(
		ctx,
		db,
		"idx_stub_preparation_cache_key",
		"ON stub (workspace_id, type, (config->>'preparation_cache_key'), updated_at DESC, id DESC) WHERE config->>'preparation_cache_key' IS NOT NULL",
	)
}

// stub.config is json, so Postgres re-parses it on every ->> and rejects any
// \u0000 escape the application happily stored. The index build evaluates the
// expression over every row, so rewrite those configs without the NULs first.
func stripStubConfigNulls(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT id, config::text FROM stub WHERE position('\u0000' in config::text) > 0`)
	if err != nil {
		return fmt.Errorf("find stub configs with NUL escapes: %w", err)
	}
	defer rows.Close()

	cleaned := map[uint][]byte{}
	for rows.Next() {
		var id uint
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		config, err := withoutNulls(raw)
		if err != nil {
			return fmt.Errorf("rewrite stub %d config: %w", id, err)
		}
		cleaned[id] = config
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for id, config := range cleaned {
		if _, err := db.ExecContext(ctx, `UPDATE stub SET config = $1 WHERE id = $2`, config, id); err != nil {
			return fmt.Errorf("update stub %d config: %w", id, err)
		}
	}
	return nil
}

func withoutNulls(raw string) ([]byte, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(stripNulls(value)); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(out.Bytes()), nil
}

func stripNulls(value any) any {
	switch v := value.(type) {
	case string:
		return strings.ReplaceAll(v, "\x00", "")
	case []any:
		for i := range v {
			v[i] = stripNulls(v[i])
		}
	case map[string]any:
		for k, e := range v {
			v[k] = stripNulls(e)
		}
	}
	return value
}

func downAddPreparedStubCacheIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS idx_stub_preparation_cache_key")
	return err
}
