package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upCreateImageTable, downDropImageTable)
}

func upCreateImageTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS image (
			id TEXT PRIMARY KEY,
			clip_version INT NOT NULL
		);
	`)
	if err != nil {
		return err
	}

	// Backfill the image table with clip_version 1 for all images in the stub table.
	rows, err := tx.QueryContext(ctx, `SELECT config FROM stub;`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Using a map for unique IDs
	imageIDs := make(map[string]struct{})

	for rows.Next() {
		var configJSON []byte
		if err := rows.Scan(&configJSON); err != nil {
			fmt.Printf("Error scanning config: %v\n", err)
			continue
		}

		if len(configJSON) == 0 {
			continue
		}

		var configs []map[string]interface{}
		if err := json.Unmarshal(configJSON, &configs); err != nil {
			fmt.Printf("Error unmarshalling config JSON: %v, json: %s\n", err, string(configJSON))
			continue
		}

		for _, configEntry := range configs {
			for _, itemData := range configEntry {
				itemMap, ok := itemData.(map[string]interface{})
				if !ok {
					continue
				}
				runtimeData, ok := itemMap["runtime"].(map[string]interface{})
				if !ok {
					continue
				}
				imageID, ok := runtimeData["image_id"].(string)
				if ok && imageID != "" {
					imageIDs[imageID] = struct{}{}
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(imageIDs) == 0 {
		return nil
	}

	var valueStrings []string
	var valueArgs []interface{}
	i := 1
	for id := range imageIDs {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, 1)", i))
		valueArgs = append(valueArgs, id)
		i++
	}

	insertQuery := fmt.Sprintf(`
		INSERT INTO image (id, clip_version)
		VALUES %s
		ON CONFLICT (id) DO NOTHING;
	`, strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(ctx, insertQuery, valueArgs...)
	if err != nil {
		return err
	}

	return nil
}

func downDropImageTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		DROP TABLE IF EXISTS image;
	`)
	return err
}
