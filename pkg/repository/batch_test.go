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

func TestCommitBatchSettlesBeforeReturningOnCancellation(t *testing.T) {
	var batch commitBatch[int, int, int]
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		value, err := batch.commit(ctx, 1, 1, func(shared context.Context, keys []int) (map[int]int, error) {
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

func TestOperationBatchSkipsCanceledRequestBeforeExecution(t *testing.T) {
	var batch operationBatch[int, int]
	ctx, cancel := context.WithCancel(context.Background())
	request := batchRequest[int, int]{ctx: ctx, key: 1, result: make(chan batchResult[int], 1)}
	batch.pending = append(batch.pending, request)
	cancel()
	batch.flush(func(context.Context, []int) (map[int]int, error) {
		t.Fatal("a canceled request must not execute")
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

func TestCommitBatchCoalescesInFlightWritesWithoutBlockingOtherGroups(t *testing.T) {
	var batch commitBatch[int, int, int]
	started, release := make(chan struct{}), make(chan struct{})
	var queries atomic.Int32
	load := func(_ context.Context, keys []int) (map[int]int, error) {
		if queries.Add(1) == 1 {
			close(started)
			<-release
		}
		values := make(map[int]int, len(keys))
		for _, key := range keys {
			values[key] = key + 10
		}
		return values, nil
	}
	var done sync.WaitGroup
	commit := func(key int) {
		defer done.Done()
		value, err := batch.commit(context.Background(), 1, key, load)
		require.NoError(t, err)
		require.Equal(t, key+10, value)
	}
	done.Add(1)
	go commit(-1)
	<-started
	for key := range 100 {
		done.Add(1)
		go commit(key)
	}
	require.Eventually(t, func() bool {
		batch.mu.Lock()
		defer batch.mu.Unlock()
		return len(batch.pending[1]) == 100
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), queries.Load(), "writes must collect behind the in-flight commit")
	value, err := batch.commit(context.Background(), 2, 9, func(_ context.Context, keys []int) (map[int]int, error) {
		return map[int]int{9: 19}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 19, value, "another workspace must commit independently")
	close(release)
	done.Wait()
	require.Equal(t, int32(2), queries.Load())
	require.Eventually(t, func() bool {
		batch.mu.Lock()
		defer batch.mu.Unlock()
		return len(batch.pending) == 0
	}, time.Second, time.Millisecond, "idle groups must not be retained")
}

func TestCommitBatchCancelsQueuedWritesWithoutWaitingForAnExecutingWrite(t *testing.T) {
	var batch commitBatch[int, int, int]
	started, release, first := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(first)
		_, err := batch.commit(context.Background(), 1, 1, func(_ context.Context, keys []int) (map[int]int, error) {
			require.Equal(t, []int{1}, keys)
			close(started)
			<-release
			return nil, nil
		})
		require.NoError(t, err)
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := batch.commit(ctx, 1, 2, func(context.Context, []int) (map[int]int, error) {
			t.Error("a canceled queued write must not execute")
			return nil, nil
		})
		done <- err
	}()
	require.Eventually(t, func() bool {
		batch.mu.Lock()
		defer batch.mu.Unlock()
		return len(batch.pending[1]) == 1
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Error("a canceled queued writer waited for another commit")
	}
	close(release)
	<-first
}

func TestCommitBatchBoundsWritesAndRoutesErrors(t *testing.T) {
	var batch commitBatch[int, int, int]
	requests := make([]batchRequest[int, int], 300)
	for i := range requests {
		requests[i] = batchRequest[int, int]{ctx: context.Background(), key: i, result: make(chan batchResult[int], 1)}
	}
	batch.pending = map[int][]batchRequest[int, int]{1: requests}
	failure := errors.New("commit failed")
	queries := 0
	batch.flush(1, func(_ context.Context, keys []int) (map[int]int, error) {
		require.LessOrEqual(t, len(keys), repositoryBatchSize)
		queries++
		return nil, failure
	})
	require.Equal(t, 3, queries)
	for _, request := range requests {
		require.ErrorIs(t, (<-request.result).err, failure)
	}
	require.Empty(t, batch.pending)
}
