package repository

import (
	"context"
	"encoding/json"
)

type vmReadKey struct {
	WorkspaceID uint   `json:"workspace_id"`
	Name        string `json:"name"`
}

// Batch independent lookups without caching lifecycle state. Lateral lookups
// retain the indexed name/id search and prefer an active VM after name reuse.
func (r *PostgresBackendRepository) readVMs(ctx context.Context, keys []vmReadKey) (map[vmReadKey]vmRow, error) {
	data, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		vmRow
		LookupName string `db:"lookup_name"`
	}
	err = r.client.SelectContext(ctx, &rows, `
	SELECT vm.*, request.name AS lookup_name
	FROM jsonb_to_recordset($1::jsonb) AS request(workspace_id bigint,name text)
	CROSS JOIN LATERAL (`+selectVM+`
		WHERE workspace_id=request.workspace_id AND (name=request.name OR id=request.name)
		ORDER BY (data->>'desired_state'='deleted') LIMIT 1
	) vm`, data)
	if err != nil {
		return nil, err
	}
	values := make(map[vmReadKey]vmRow, len(rows))
	for _, row := range rows {
		values[vmReadKey{WorkspaceID: row.WorkspaceID, Name: row.LookupName}] = row.vmRow
	}
	return values, nil
}
