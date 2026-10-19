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
	result, err := b.enqueue(ctx, key, load)
	if err != nil {
		return zero, err
	}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case resolved := <-result:
		return resolved.value, resolved.err
	}
}

// A write must settle before its caller releases a lifecycle lock, even when
// the HTTP request is canceled. Cancellation before execution still skips it.
func (b *operationBatch[K, V]) commit(ctx context.Context, key K, load func(context.Context, []K) (map[K]V, error)) (V, error) {
	var zero V
	result, err := b.enqueue(ctx, key, load)
	if err != nil {
		return zero, err
	}
	resolved := <-result
	return resolved.value, resolved.err
}

func (b *operationBatch[K, V]) enqueue(ctx context.Context, key K, load func(context.Context, []K) (map[K]V, error)) (<-chan batchResult[V], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := batchRequest[K, V]{ctx: ctx, key: key, result: make(chan batchResult[V], 1)}
	b.mu.Lock()
	b.pending = append(b.pending, request)
	if len(b.pending) == 1 {
		time.AfterFunc(time.Millisecond, func() { b.flush(load) })
	}
	b.mu.Unlock()
	return request.result, nil
}

func (b *operationBatch[K, V]) flush(load func(context.Context, []K) (map[K]V, error)) {
	b.mu.Lock()
	requests := b.pending
	b.pending = nil
	b.mu.Unlock()
	for len(requests) > 0 {
		n := min(len(requests), repositoryBatchSize)
		batch := requests[:n]
		requests = requests[n:]
		keys := make([]K, 0, n)
		active := make([]batchRequest[K, V], 0, n)
		seen := make(map[K]bool, n)
		for _, request := range batch {
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
			continue
		}
		// One canceled caller cannot tear down a query used by other callers.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(active[0].ctx), 30*time.Second)
		values, err := load(ctx, keys)
		cancel()
		for _, request := range active {
			request.result <- batchResult[V]{value: values[request.key], err: err}
		}
	}
}
