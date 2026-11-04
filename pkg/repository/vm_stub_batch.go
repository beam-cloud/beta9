package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

type vmStubInput struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Config      json.RawMessage `json:"config"`
	ObjectID    uint            `json:"object_id"`
	WorkspaceID uint            `json:"workspace_id"`
	AppID       uint            `json:"app_id"`
}

type vmStubResult struct {
	stub types.Stub
	err  error
}

type vmStubRequest struct {
	ctx   context.Context
	input vmStubInput
	done  chan vmStubResult
}

type vmStubBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmStubRequest
	limit int
}

func (r *PostgresBackendRepository) getOrCreateVMStubBatched(ctx context.Context, input vmStubInput) (types.Stub, error) {
	r.vmStubBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, 128)
		r.vmStubBatch = &vmStubBatcher{repo: r, queue: make(chan *vmStubRequest, limit*2), limit: limit}
		go r.vmStubBatch.run()
	})
	request := &vmStubRequest{ctx: ctx, input: input, done: make(chan vmStubResult, 1)}

	select {
	case r.vmStubBatch.queue <- request:
	case <-ctx.Done():
		return types.Stub{}, ctx.Err()
	}
	select {
	case result := <-request.done:
		return result.stub, result.err
	case <-ctx.Done():
		return types.Stub{}, ctx.Err()
	}
}

func (b *vmStubBatcher) run() {
	for first := range b.queue {
		batch := []*vmStubRequest{first}
		timer := time.NewTimer(time.Millisecond)
	collect:
		for len(batch) < b.limit {
			select {
			case request := <-b.queue:
				batch = append(batch, request)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		b.flush(batch)
	}
}

func (b *vmStubBatcher) flush(batch []*vmStubRequest) {
	// Cancellation before dispatch never writes. Once dispatched, one caller
	// must not cancel the shared transaction for other callers. Results are
	// buffered so a canceled caller cannot block publication to its peers.
	active := batch[:0]
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			request.done <- vmStubResult{err: err}
		} else {
			active = append(active, request)
		}
	}
	if len(active) == 0 {
		return
	}
	if len(active) == 1 {
		b.single(active[0])
		return
	}
	// Identical inputs share a result, rather than inserting duplicate stubs
	// within the same statement. The typed config marshaler orders map keys.
	inputs := make([]vmStubInput, 0, len(active))
	indices := make([]int, len(active))
	seen := make(map[string]int, len(active))
	for i, request := range active {
		encoded, err := json.Marshal(request.input)
		if err != nil {
			for _, request := range active {
				request.done <- vmStubResult{err: err}
			}
			return
		}
		key := string(encoded)
		index, ok := seen[key]
		if !ok {
			index = len(inputs)
			seen[key] = index
			inputs = append(inputs, request.input)
		}
		indices[i] = index
	}
	encoded, err := json.Marshal(inputs)
	var rows []vmStubBatchRow
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err == nil {
		err = b.repo.client.SelectContext(ctx, &rows, vmStubBatchQuery, string(encoded))
	}
	// A server-reported data/constraint error or aborted transaction has no
	// committed effects. Isolate it so one invalid request cannot poison its
	// peers. Never replay an ambiguous connection/commit failure.
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && (pgErr.Code.Class() == "22" || pgErr.Code.Class() == "23" || pgErr.Code.Class() == "40") {
		for _, request := range active {
			b.single(request)
		}
		return
	}
	results := make([]types.Stub, len(inputs))
	if err == nil {
		found := make([]bool, len(inputs))
		for _, row := range rows {
			index := row.Index - 1
			if index < 0 || index >= len(found) || found[index] {
				err = fmt.Errorf("unexpected VM stub batch result index %d", row.Index)
				break
			}
			found[index], results[index] = true, row.Stub
		}
		if err == nil && len(rows) != len(inputs) {
			err = fmt.Errorf("VM stub batch returned %d of %d results", len(rows), len(inputs))
		}
	}
	// SelectContext drains/closes the implicit transaction's result, including
	// its commit acknowledgement, before any launch receives a successful stub.
	for i, request := range active {

		request.done <- vmStubResult{stub: results[indices[i]], err: err}
	}
}

func (b *vmStubBatcher) single(request *vmStubRequest) {
	input := request.input
	stub, err := b.repo.getOrCreateStub(request.ctx, input.Name, input.Type, string(input.Config), input.ObjectID, input.WorkspaceID, false, input.AppID)
	request.done <- vmStubResult{stub: stub, err: err}
}

type vmStubBatchRow struct {
	Index int `db:"batch_index"`
	types.Stub
}

// Populate the actual composite type so JSON, enum and identifier column types
// follow the migrated schema. App activity is updated once per app per batch.
const vmStubBatchQuery = `
WITH input AS MATERIALIZED (
	SELECT i.*, (SELECT s.id FROM stub s
		WHERE s.name=i.name AND s.type=i.type AND s.object_id=i.object_id
		AND s.config::jsonb=i.config::jsonb AND s.workspace_id=i.workspace_id AND s.app_id=i.app_id
		LIMIT 1) AS existing_id
	FROM jsonb_populate_recordset(NULL::stub, $1::jsonb) WITH ORDINALITY AS i
), created AS (
	INSERT INTO stub (name, type, config, object_id, workspace_id, app_id)
	SELECT name, type, config, object_id, workspace_id, app_id FROM input WHERE existing_id IS NULL
	RETURNING id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id
), touched AS (
	UPDATE stub SET updated_at=CURRENT_TIMESTAMP WHERE id IN (SELECT existing_id FROM input)
	RETURNING id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id
), activity AS (
	UPDATE app SET updated_at=NOW() WHERE id IN (SELECT app_id FROM input)
	AND ` + appActivityNeedsRefresh + `
), mutated AS (SELECT * FROM created UNION ALL SELECT * FROM touched)
SELECT i.ordinality AS batch_index, s.* FROM input i JOIN mutated s
	ON s.name=i.name AND s.type=i.type AND s.object_id=i.object_id
	AND s.config::jsonb=i.config::jsonb AND s.workspace_id=i.workspace_id AND s.app_id=i.app_id
	AND (i.existing_id IS NULL OR s.id=i.existing_id)
`
