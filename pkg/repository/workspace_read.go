package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// A launch burst repeatedly reads the same workspace during authorization and
// credential vending. Share only overlapping reads; the next read after one
// finishes must see database changes. Each caller gets its own nested values.
func (r *PostgresBackendRepository) readWorkspace(ctx context.Context, key string, load func(context.Context) (types.Workspace, error)) (types.Workspace, error) {
	if err := ctx.Err(); err != nil {
		return types.Workspace{}, err
	}
	result := r.workspaceReads.DoChan(key, func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		workspace, err := load(shared)
		if err != nil {
			return nil, err
		}
		// Serialization gives callers independent storage credentials and other
		// pointer fields without a second, manually maintained workspace schema.
		return json.Marshal(workspace)
	})
	select {
	case <-ctx.Done():
		return types.Workspace{}, ctx.Err()
	case resolved := <-result:
		if resolved.Err != nil {
			return types.Workspace{}, resolved.Err
		}
		var workspace types.Workspace
		err := json.Unmarshal(resolved.Val.([]byte), &workspace)
		return workspace, err
	}
}
