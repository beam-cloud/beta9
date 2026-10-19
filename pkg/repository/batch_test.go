package repository

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOperationBatchBoundsQueriesAndRoutesResults(t *testing.T) {
	var batch operationBatch[int, int]
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := range 300 {
		ctx := context.Background()
		if i%3 == 0 {
			ctx = canceled
		}
		batch.pending = append(batch.pending, batchRequest[int, int]{ctx: ctx, key: i % 50, result: make(chan batchResult[int], 1)})
	}
	requests := batch.pending
	queries := 0
	batch.flush(func(_ context.Context, keys []int) (map[int]int, error) {
		queries++
		require.LessOrEqual(t, len(keys), repositoryBatchSize)
		values := make(map[int]int)
		for _, key := range keys {
			_, duplicate := values[key]
			require.False(t, duplicate)
			values[key] = key + 10
		}
		return values, nil
	})
	require.Equal(t, 3, queries)
	for _, request := range requests {
		if request.ctx.Err() != nil {
			continue
		}
		result := <-request.result
		require.NoError(t, result.err)
		require.Equal(t, request.key+10, result.value)
	}
}

func TestOperationBatchCancellationDoesNotCancelSharedQuery(t *testing.T) {
	var batch operationBatch[int, int]
	started, release := make(chan context.Context, 1), make(chan struct{})
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := batch.get(ctx, 1, func(shared context.Context, _ []int) (map[int]int, error) {
			started <- shared
			<-release
			return map[int]int{1: 7}, nil
		})
		done <- err
	}()
	shared := <-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled caller waited for the shared query")
	}
	require.NoError(t, shared.Err())
}

func TestOperationBatchCommitSettlesBeforeReturningOnCancellation(t *testing.T) {
	var batch operationBatch[int, int]
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		value, err := batch.commit(ctx, 1, func(shared context.Context, keys []int) (map[int]int, error) {
			close(started)
			<-release
			require.NoError(t, shared.Err())
			return map[int]int{1: 7}, nil
		})
		require.Equal(t, 7, value)
		done <- err
	}()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("a canceled writer returned before its commit settled")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
}

func TestOperationBatchSkipsCanceledWriteBeforeExecution(t *testing.T) {
	var batch operationBatch[int, int]
	ctx, cancel := context.WithCancel(context.Background())
	request := batchRequest[int, int]{ctx: ctx, key: 1, result: make(chan batchResult[int], 1)}
	batch.pending = append(batch.pending, request)
	cancel()
	batch.flush(func(context.Context, []int) (map[int]int, error) {
		t.Fatal("a canceled write must not execute")
		return nil, nil
	})
	require.ErrorIs(t, (<-request.result).err, context.Canceled)
}

func TestOperationBatchCombinesBurstAndDoesNotCacheFailures(t *testing.T) {
	var batch operationBatch[int, int]
	var queries atomic.Int32
	failure := errors.New("database unavailable")
	load := func(_ context.Context, keys []int) (map[int]int, error) {
		queries.Add(1)
		return nil, failure
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(100)
	done.Add(100)
	for i := range 100 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, err := batch.get(context.Background(), i, load)
			require.ErrorIs(t, err, failure)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	require.Less(t, queries.Load(), int32(100), "concurrent requests must share queries")
	value, err := batch.get(context.Background(), 1, func(context.Context, []int) (map[int]int, error) {
		return map[int]int{1: 9}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 9, value)
}
