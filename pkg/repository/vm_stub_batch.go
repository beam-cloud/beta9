package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type vmStubResult struct {
	stub types.Stub
	err  error
}

type vmStubRequest struct {
	ctx   context.Context
	input stubUpsertInput
	done  chan vmStubResult
}

type vmStubBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmStubRequest
}

func (r *PostgresBackendRepository) getOrCreateVMStubBatched(ctx context.Context, input stubUpsertInput) (types.Stub, error) {
	r.vmStubBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, 128)
		r.vmStubBatch = &vmStubBatcher{repo: r, queue: make(chan *vmStubRequest, limit*2)}
		go common.RunBatchQueue(r.vmStubBatch.queue, limit, r.vmStubBatch.flush)
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
	inputs := make([]stubUpsertInput, 0, len(active))
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
