package repository

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Only retire VM stubs with at least one VM record and exclusively deleted
// references. New stubs, legacy records, other stub types and live shared
// references remain eligible for reconciliation.
const retiredVMCacheStubKeysQuery = `WITH candidates AS MATERIALIZED (
 SELECT recent.stub_key,w.id AS workspace_id,s.external_id AS stub_id
 FROM unnest($1::text[]) recent(stub_key)
 JOIN workspace w ON w.external_id=split_part(recent.stub_key,'|',1)::uuid
 JOIN stub s ON s.workspace_id=w.id AND s.external_id=split_part(recent.stub_key,'|',2)::uuid AND s.type='vm'
)
SELECT c.stub_key FROM candidates c
JOIN persistent_vm v ON v.workspace_id=c.workspace_id AND v.data->>'stub_id'=c.stub_id::text
GROUP BY c.stub_key
HAVING bool_and(COALESCE(v.data->>'desired_state','')='deleted')`

func (r *PostgresBackendRepository) ListRetiredVMCacheStubKeys(ctx context.Context, keys []string) ([]string, error) {
	valid := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		workspace, stub, ok := strings.Cut(key, "|")
		if !ok || len(workspace) != 36 || len(stub) != 36 {
			continue
		}
		if _, err := uuid.Parse(workspace); err != nil {
			continue
		}
		if _, err := uuid.Parse(stub); err != nil {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		valid = append(valid, key)
	}
	if len(valid) == 0 {
		return nil, nil
	}
	var retired []string
	err := r.client.SelectContext(ctx, &retired, retiredVMCacheStubKeysQuery, pq.Array(valid))
	return retired, err
}
