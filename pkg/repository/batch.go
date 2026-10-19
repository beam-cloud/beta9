package repository

import (
	"context"
	"sync"
	"time"
)

const repositoryBatchSize = 128

// operationBatch combines concurrent metadata operations without retaining results.
// Queries collect keys for one millisecond and contain at most 128 keys.
type operationBatch[K comparable, V any] struct {
	mu      sync.Mutex
	pending []batchRequest[K, V]
}

type batchRequest[K comparable, V any] struct {
	ctx    context.Context
	key    K
	result chan batchResult[V]
}

type batchResult[V any] struct {
	value V
	err   error
}

func (b *operationBatch[K, V]) get(ctx context.Context, key K, load func(context.Context, []K) (map[K]V, error)) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	request := batchRequest[K, V]{ctx: ctx, key: key, result: make(chan batchResult[V], 1)}
	b.mu.Lock()
	b.pending = append(b.pending, request)
	if len(b.pending) == 1 {
		time.AfterFunc(time.Millisecond, func() { b.flush(load) })
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case resolved := <-request.result:
		return resolved.value, resolved.err
	}
}

func (b *operationBatch[K, V]) flush(load func(context.Context, []K) (map[K]V, error)) {
	b.mu.Lock()
	requests := b.pending
	b.pending = nil
	b.mu.Unlock()
	for len(requests) > 0 {
		n := min(len(requests), repositoryBatchSize)
		resolveBatch(requests[:n], load)
		requests = requests[n:]
	}
}

func resolveBatch[K comparable, V any](requests []batchRequest[K, V], load func(context.Context, []K) (map[K]V, error)) {
	keys := make([]K, 0, len(requests))
	active := make([]batchRequest[K, V], 0, len(requests))
	seen := make(map[K]bool, len(requests))
	for _, request := range requests {
		if err := request.ctx.Err(); err != nil {
			request.result <- batchResult[V]{err: err}
			continue
		}
		active = append(active, request)
		if !seen[request.key] {
			keys = append(keys, request.key)
			seen[request.key] = true
		}
	}
	if len(keys) == 0 {
		return
	}
	// One canceled caller cannot tear down a query used by other callers.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(active[0].ctx), 30*time.Second)
	values, err := load(ctx, keys)
	cancel()
	for _, request := range active {
		request.result <- batchResult[V]{value: values[request.key], err: err}
	}
}

// commitBatch coalesces writes arriving during an in-flight commit. Groups
// commit independently, so a busy workspace cannot hold up another workspace.
// Completed groups retain neither results nor an idle goroutine.
type commitBatch[G comparable, K comparable, V any] struct {
	mu      sync.Mutex
	pending map[G][]batchRequest[K, V]
}

func (b *commitBatch[G, K, V]) commit(ctx context.Context, group G, key K, load func(context.Context, []K) (map[K]V, error)) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	request := batchRequest[K, V]{ctx: ctx, key: key, result: make(chan batchResult[V], 1)}
	b.mu.Lock()
	if b.pending == nil {
		b.pending = make(map[G][]batchRequest[K, V])
	}
	if _, running := b.pending[group]; !running {
		time.AfterFunc(time.Millisecond, func() { b.flush(group, load) })
	}
	b.pending[group] = append(b.pending[group], request)
	b.mu.Unlock()
	select {
	case resolved := <-request.result:
		return resolved.value, resolved.err
	case <-ctx.Done():
		b.mu.Lock()
		for i, queued := range b.pending[group] {
			if queued.result == request.result {
				pending := b.pending[group]
				copy(pending[i:], pending[i+1:])
				pending[len(pending)-1] = batchRequest[K, V]{}
				b.pending[group] = pending[:len(pending)-1]
				b.mu.Unlock()
				return zero, ctx.Err()
			}
		}
		b.mu.Unlock()
		// An executing write must settle before its caller releases a lifecycle
		// lock. Returning early could let the next owner race this commit.
		resolved := <-request.result
		return resolved.value, resolved.err
	}
}

func (b *commitBatch[G, K, V]) flush(group G, load func(context.Context, []K) (map[K]V, error)) {
	for {
		b.mu.Lock()
		pending := b.pending[group]
		if len(pending) == 0 {
			delete(b.pending, group)
			b.mu.Unlock()
			return
		}
		n := min(len(pending), repositoryBatchSize)
		b.pending[group] = pending[n:]
		b.mu.Unlock()
		resolveBatch(pending[:n], load)
	}
}
