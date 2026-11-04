package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Serialize before enqueue: a caller may change its VM only after the durable
// result, and the batch must own an immutable copy of the submitted state.
type vmMutationRow struct {
	ID                  string          `json:"id"`
	WorkspaceID         uint            `json:"workspace_id"`
	WorkspaceExternalID string          `json:"workspace_external_id"`
	TokenID             string          `json:"token_id"`
	Name                string          `json:"name"`
	Handle              string          `json:"handle"`
	Data                json.RawMessage `json:"data"`
	LastActiveAt        time.Time       `json:"last_active_at"`
	PublicIPv6          *string         `json:"public_ipv6"`
}

type vmMutationInput struct {
	Create bool          `json:"is_create"`
	Row    vmMutationRow `json:"row"`
}

type vmMutationRequest struct {
	ctx   context.Context
	input vmMutationInput
	done  chan error
}

type vmMutationBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmMutationRequest
	limit int
}

func nullableVMAddress(address string) *string {
	if address == "" {
		return nil
	}
	return &address
}

func (r *PostgresBackendRepository) mutateVMBatched(ctx context.Context, input vmMutationInput) error {
	r.vmMutationBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, 128)
		r.vmMutationBatch = &vmMutationBatcher{repo: r, queue: make(chan *vmMutationRequest, limit*2), limit: limit}
		go r.vmMutationBatch.run()
	})
	request := &vmMutationRequest{ctx: ctx, input: input, done: make(chan error, 1)}

	select {
	case r.vmMutationBatch.queue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *vmMutationBatcher) run() {
	for first := range b.queue {
		batch := []*vmMutationRequest{first}
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

func (b *vmMutationBatcher) flush(batch []*vmMutationRequest) {
	// UPDATE ... FROM must never choose between two states for one VM. Split
	// at repeated identities, preserving arrival order even without a VM lock.
	seen := make(map[string]bool, len(batch))
	for i, request := range batch {
		if seen[request.input.Row.ID] {
			b.flush(batch[:i])
			b.flush(batch[i:])
			return
		}
		seen[request.input.Row.ID] = true
	}
	active := batch[:0]
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			request.done <- err
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
	inputs := make([]vmMutationInput, len(active))
	for i, request := range active {
		inputs[i] = request.input
	}
	encoded, err := json.Marshal(inputs)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var ids []string
	if err == nil {
		err = b.repo.client.SelectContext(ctx, &ids, vmMutationBatchQuery, string(encoded))
	}
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && (pgErr.Code.Class() == "22" || pgErr.Code.Class() == "23" || pgErr.Code.Class() == "40") {
		for _, request := range active {
			b.single(request)
		}
		return
	}
	committed := make(map[string]bool, len(ids))
	for _, id := range ids {
		committed[id] = true
	}
	for _, request := range active {
		resultErr := err
		if resultErr == nil && !committed[request.input.Row.ID] {
			resultErr = fmt.Errorf("VM not found")
		}

		request.done <- resultErr
	}
}

func (b *vmMutationBatcher) single(request *vmMutationRequest) {
	row := request.input.Row
	if request.input.Create {
		_, err := b.repo.client.ExecContext(request.ctx, `INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::inet)`, row.ID, row.WorkspaceID, row.WorkspaceExternalID, row.TokenID, row.Name, row.Handle, []byte(row.Data), row.LastActiveAt, row.PublicIPv6)
		request.done <- err
		return
	}
	result, err := b.repo.client.ExecContext(request.ctx, `UPDATE persistent_vm SET data=$2,token_id=$3 WHERE id=$1`, row.ID, []byte(row.Data), row.TokenID)
	if err == nil {
		var count int64
		count, err = result.RowsAffected()
		if err == nil && count != 1 {
			err = fmt.Errorf("VM not found")
		}
	}
	request.done <- err
}

const vmMutationBatchQuery = `
WITH input AS MATERIALIZED (
	SELECT i.is_create, r.* FROM jsonb_to_recordset($1::jsonb) AS i(is_create boolean, row jsonb)
	CROSS JOIN LATERAL jsonb_populate_record(NULL::persistent_vm, i.row) AS r
), created AS (
	INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6)
	SELECT id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6 FROM input WHERE is_create
	RETURNING id
), updated AS (
	UPDATE persistent_vm v SET data=i.data,token_id=i.token_id FROM input i WHERE NOT i.is_create AND v.id=i.id
	RETURNING v.id
)
SELECT id FROM created UNION ALL SELECT id FROM updated
`
