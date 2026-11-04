package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

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

type vmDiskInput struct {
	WorkspaceID uint   `json:"workspace_id"`
	Name        string `json:"name"`
	Size        string `json:"size"`
	Filesystem  string `json:"filesystem"`
	Driver      string `json:"driver"`
	MountPath   string `json:"mount_path"`
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
	input vmDiskInput
	done  chan vmDiskResult
}

type vmDiskBatcher struct {
	repo  *PostgresBackendRepository
	queue chan *vmDiskRequest
	limit int
}

func (r *PostgresBackendRepository) createVMDiskBatched(ctx context.Context, input vmDiskInput) (*types.Disk, error) {
	r.vmDiskBatchOnce.Do(func() {
		limit := min(r.config.VMBurstBatchSize, 128)
		r.vmDiskBatch = &vmDiskBatcher{repo: r, queue: make(chan *vmDiskRequest, limit*2), limit: limit}
		go r.vmDiskBatch.run()
	})
	request := &vmDiskRequest{ctx: ctx, input: input, done: make(chan vmDiskResult, 1)}

	select {
	case r.vmDiskBatch.queue <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-request.done:
		return result.disk, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *vmDiskBatcher) run() {
	for first := range b.queue {
		batch := []*vmDiskRequest{first}
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

func (b *vmDiskBatcher) flush(batch []*vmDiskRequest) {
	active := batch[:0]
	inputs := make([]vmDiskInput, 0, len(batch))
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var rows []types.Disk
	if err == nil {
		err = b.repo.client.SelectContext(ctx, &rows, vmDiskBatchQuery, string(encoded))
	}
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && (pgErr.Code.Class() == "22" || pgErr.Code.Class() == "23" || pgErr.Code.Class() == "40") {
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
