package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

const (
	vmBurstBatchLimit       = 128
	vmBatchOperationTimeout = 30 * time.Second
)

// Data/constraint errors and explicit rollback codes have no committed effects.
// Class 40 also includes statement_completion_unknown, which must not be replayed.
func retryVMBatchIndividually(err error) bool {
	var pgErr *pq.Error
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code.Class() == "22" || pgErr.Code.Class() == "23" {
		return true
	}
	switch pgErr.Code {
	case "40000", "40001", "40002", "40P01": // Rollback, serialization, integrity, deadlock.
		return true
	}
	return false
}

// Normalize nested config key ordering before comparing stub inputs.
// UseNumber preserves integers larger than float64 can represent exactly.
func canonicalStubInput(input types.StubUpsertInput) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(input.Config))
	decoder.UseNumber()
	var config any
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid trailing stub config: %v", err)
	}
	var err error
	input.Config, err = json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return json.Marshal(input)
}

const stubMatchPredicate = `s.name=i.name AND s.type=i.type AND s.object_id=i.object_id
 AND s.config::jsonb=i.config::jsonb AND s.workspace_id=i.workspace_id AND s.app_id=i.app_id`
const stubResultColumns = `id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id`

// Both entrypoints share matching, create/touch and app-activity semantics.
// Only their input cardinality and result-to-request mapping differ.
const stubUpsertCTEs = `created AS (
 INSERT INTO stub (name, type, config, object_id, workspace_id, app_id)
 SELECT name, type, config, object_id, workspace_id, app_id FROM input WHERE existing_id IS NULL
 RETURNING ` + stubResultColumns + `
), touched AS (
 UPDATE stub SET updated_at=CURRENT_TIMESTAMP WHERE id IN (SELECT existing_id FROM input)
 RETURNING ` + stubResultColumns + `
), activity AS (
 UPDATE app SET updated_at=NOW() WHERE id IN (SELECT app_id FROM input)
 AND ` + appActivityNeedsRefresh + `
), mutated AS (SELECT * FROM created UNION ALL SELECT * FROM touched)
`

const singleStubUpsertQuery = `WITH input AS MATERIALIZED (
 SELECT i.*, (SELECT s.id FROM stub s WHERE ` + stubMatchPredicate + ` AND NOT $2 LIMIT 1) AS existing_id
 FROM jsonb_populate_record(NULL::stub, $1::jsonb) AS i
), ` + stubUpsertCTEs + `SELECT * FROM mutated`

const vmStubBatchQuery = `WITH input AS MATERIALIZED (
 SELECT i.*, (SELECT s.id FROM stub s WHERE ` + stubMatchPredicate + ` LIMIT 1) AS existing_id
 FROM jsonb_populate_recordset(NULL::stub, $1::jsonb) WITH ORDINALITY AS i
), ` + stubUpsertCTEs + `SELECT i.ordinality AS batch_index, s.* FROM input i JOIN mutated s
 ON ` + stubMatchPredicate + ` AND (i.existing_id IS NULL OR s.id=i.existing_id)
`

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
}

func nullableVMAddress(address string) *string {
	if address == "" {
		return nil
	}
	return &address
}

func (r *PostgresBackendRepository) mutateVMBatched(ctx context.Context, input vmMutationInput) error {
	r.vmMutationBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, vmBurstBatchLimit)
		r.vmMutationBatch = &vmMutationBatcher{repo: r, queue: make(chan *vmMutationRequest, limit*2)}
		go common.RunBatchQueue(r.vmMutationBatch.queue, limit, r.vmMutationBatch.flush)
	})
	request := &vmMutationRequest{ctx: ctx, input: input, done: make(chan error, 1)}

	select {
	case r.vmMutationBatch.queue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Once enqueued, report the database outcome even if the caller cancels.
	// The batcher still rejects requests canceled before dispatch.
	return <-request.done
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
	ctx, cancel := context.WithTimeout(context.Background(), vmBatchOperationTimeout)
	defer cancel()

	var ids []string
	if err == nil {
		err = b.repo.client.SelectContext(ctx, &ids, vmMutationBatchQuery, string(encoded))
	}
	if retryVMBatchIndividually(err) {
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

type vmStubResult struct {
	stub types.Stub
	err  error
}

type vmStubRequest struct {
	ctx   context.Context
	input types.StubUpsertInput
	done  chan vmStubResult
}

type vmStubBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmStubRequest
}

func (r *PostgresBackendRepository) getOrCreateVMStubBatched(ctx context.Context, input types.StubUpsertInput) (types.Stub, error) {
	r.vmStubBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, vmBurstBatchLimit)
		r.vmStubBatch = &vmStubBatcher{repo: r, queue: make(chan *vmStubRequest, limit*2)}
		go common.RunBatchQueue(r.vmStubBatch.queue, limit, r.vmStubBatch.flush)
	})
	request := &vmStubRequest{ctx: ctx, input: input, done: make(chan vmStubResult, 1)}

	select {
	case r.vmStubBatch.queue <- request:
	case <-ctx.Done():
		return types.Stub{}, ctx.Err()
	}
	result := <-request.done
	return result.stub, result.err
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
	// within the same statement. Normalize nested config keys before comparing.
	inputs := make([]types.StubUpsertInput, 0, len(active))
	indices := make([]int, len(active))
	seen := make(map[string]int, len(active))
	for i, request := range active {
		encoded, err := canonicalStubInput(request.input)
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
	ctx, cancel := context.WithTimeout(context.Background(), vmBatchOperationTimeout)
	defer cancel()

	if err == nil {
		err = b.repo.client.SelectContext(ctx, &rows, vmStubBatchQuery, string(encoded))
	}
	// A server-reported data/constraint error or aborted transaction has no
	// committed effects. Isolate it so one invalid request cannot poison its
	// peers. Never replay an ambiguous connection/commit failure.
	if retryVMBatchIndividually(err) {
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

type vmDiskBurstKey struct{}

// ContextWithVMDiskBurst limits disk batching to VM preparation. Ordinary disk
// operations and existing-disk validation/resize do not take this path.
func ContextWithVMDiskBurst(ctx context.Context) context.Context {
	return context.WithValue(ctx, vmDiskBurstKey{}, true)
}

func vmDiskBurstEnabled(ctx context.Context) bool {
	enabled, _ := ctx.Value(vmDiskBurstKey{}).(bool)
	return enabled
}

type vmDiskKey struct {
	workspace uint
	name      string
}

type vmDiskResult struct {
	disk *types.Disk
	err  error
}

type vmDiskRequest struct {
	ctx   context.Context
	input types.DiskCreateInput
	done  chan vmDiskResult
}

type vmDiskBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmDiskRequest
}

func (r *PostgresBackendRepository) createVMDiskBatched(ctx context.Context, input types.DiskCreateInput) (*types.Disk, error) {
	r.vmDiskBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, vmBurstBatchLimit)
		r.vmDiskBatch = &vmDiskBatcher{repo: r, queue: make(chan *vmDiskRequest, limit*2)}
		go common.RunBatchQueue(r.vmDiskBatch.queue, limit, r.vmDiskBatch.flush)
	})
	request := &vmDiskRequest{ctx: ctx, input: input, done: make(chan vmDiskResult, 1)}

	select {
	case r.vmDiskBatch.queue <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	result := <-request.done
	return result.disk, result.err
}

func (b *vmDiskBatcher) flush(batch []*vmDiskRequest) {
	active := batch[:0]
	inputs := make([]types.DiskCreateInput, 0, len(batch))
	seen := make(map[vmDiskKey]bool, len(batch))
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			request.done <- vmDiskResult{err: err}
			continue
		}
		active = append(active, request)
		key := vmDiskKey{request.input.WorkspaceID, request.input.Name}
		if !seen[key] {
			seen[key] = true
			inputs = append(inputs, request.input)
		}
	}
	if len(active) == 0 {
		return
	}
	if len(active) == 1 {
		b.single(active[0])
		return
	}
	encoded, err := json.Marshal(inputs)
	ctx, cancel := context.WithTimeout(context.Background(), vmBatchOperationTimeout)
	defer cancel()

	var rows []types.Disk
	if err == nil {
		err = b.repo.client.SelectContext(ctx, &rows, vmDiskBatchQuery, string(encoded))
	}
	if retryVMBatchIndividually(err) {
		for _, request := range active {
			b.single(request)
		}
		return
	}
	committed := make(map[vmDiskKey]*types.Disk, len(rows))
	for i := range rows {
		row := &rows[i]
		committed[vmDiskKey{row.WorkspaceId, row.Name}] = row
	}
	for _, request := range active {

		resultErr := err
		disk := committed[vmDiskKey{request.input.WorkspaceID, request.input.Name}]
		if resultErr == nil && disk == nil {
			// Match the single INSERT's ON CONFLICT path: return the winner.
			disk, resultErr = b.repo.GetDisk(request.ctx, request.input.WorkspaceID, request.input.Name)
		}
		if disk != nil {
			copy := *disk
			disk = &copy
		}
		request.done <- vmDiskResult{disk: disk, err: resultErr}
	}
}

func (b *vmDiskBatcher) single(request *vmDiskRequest) {
	disk, err := b.repo.insertDisk(request.ctx, request.input)
	request.done <- vmDiskResult{disk: disk, err: err}
}

const vmDiskBatchQuery = `INSERT INTO disk (workspace_id,name,size,filesystem,driver,mount_path)
SELECT workspace_id,name,size,filesystem,driver,mount_path
FROM jsonb_populate_recordset(NULL::disk, $1::jsonb)
ON CONFLICT (workspace_id,name) WHERE deleted_at IS NULL DO NOTHING
RETURNING ` + diskColumns
