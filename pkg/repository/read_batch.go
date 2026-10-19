package repository

import (
	"context"
	"sync"
	"time"
)

const repositoryReadBatchSize = 128

// readBatch combines concurrent metadata lookups without retaining their results.
// Queries collect keys for one millisecond and contain at most 128 keys.
type readBatch[K comparable, V any] struct {
	mu      sync.Mutex
	pending []readRequest[K, V]
}

type readRequest[K comparable, V any] struct {
	ctx    context.Context
	key    K
	result chan readResult[V]
}

type readResult[V any] struct {
	value V
	err   error
}

func (b *readBatch[K, V]) get(ctx context.Context, key K, load func(context.Context, []K) (map[K]V, error)) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	request := readRequest[K, V]{ctx: ctx, key: key, result: make(chan readResult[V], 1)}
	b.mu.Lock()
	b.pending = append(b.pending, request)
	if len(b.pending) == 1 {
		time.AfterFunc(time.Millisecond, func() { b.flush(load) })
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case result := <-request.result:
		return result.value, result.err
	}
}

func (b *readBatch[K, V]) flush(load func(context.Context, []K) (map[K]V, error)) {
	b.mu.Lock()
	requests := b.pending
	b.pending = nil
	b.mu.Unlock()
	for len(requests) > 0 {
		n := min(len(requests), repositoryReadBatchSize)
		batch := requests[:n]
		requests = requests[n:]
		keys := make([]K, 0, n)
		seen := make(map[K]bool, n)
		for _, request := range batch {
			if request.ctx.Err() == nil && !seen[request.key] {
				keys = append(keys, request.key)
				seen[request.key] = true
			}
		}
		if len(keys) == 0 {
			continue
		}
		// One canceled caller cannot tear down a query used by other callers.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(batch[0].ctx), 30*time.Second)
		values, err := load(ctx, keys)
		cancel()
		for _, request := range batch {
			request.result <- readResult[V]{value: values[request.key], err: err}
		}
	}
}
