package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestVMLockQueueBatchesAndBoundsIndependentRequests(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	s := &vmLockSession{client: r.client, gate: make(chan struct{}, 1), batchRunning: true}
	var done sync.WaitGroup
	for i := range 300 {
		done.Add(1)
		go func() {
			defer done.Done()
			result := s.enqueue(context.Background(), fmt.Sprint(i))
			require.NoError(t, result.err)
			require.NotNil(t, result.busy)
		}()
	}
	require.Eventually(t, func() bool {
		s.batchMu.Lock()
		defer s.batchMu.Unlock()
		return len(s.pending) == 300
	}, time.Second, time.Millisecond)
	for offset := 0; offset < 300; offset += 128 {
		ids := []string{}
		rows := sqlmock.NewRows([]string{"id", "acquired"})
		for _, request := range s.pending[offset:min(offset+128, 300)] {
			ids = append(ids, request.id)
			rows.AddRow(request.id, false)
		}
		mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array(ids)).WillReturnRows(rows)
	}
	s.flushAcquires()
	done.Wait()
	require.Empty(t, s.pending)
	require.False(t, s.batchRunning)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockQueueCancellationDoesNotWaitForAnotherBatch(t *testing.T) {
	s := &vmLockSession{batchRunning: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan vmLockResult, 1)
	go func() { done <- s.enqueue(ctx, "queued") }()
	require.Eventually(t, func() bool {
		s.batchMu.Lock()
		defer s.batchMu.Unlock()
		return len(s.pending) == 1
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("a canceled queued request waited for another acquisition")
	}
	s.flushAcquires()
	require.Empty(t, s.pending)
	require.False(t, s.batchRunning)
}

func TestVMLocksLeaveConnectionsForLifecycleQueries(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	ctx := context.Background()

	lock := func(id string, acquired bool) {
		mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{id})).
			WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow(id, acquired))
	}
	unlock := func(id string) {
		mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(id).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	lock("a", true)
	a, err := r.LockVM(ctx, "a")
	require.NoError(t, err)
	lock("b", true)
	b, err := r.LockVM(ctx, "b")
	require.NoError(t, err)

	// Independent lifecycles share one lock session, leaving a connection for
	// queries even while both operations are in progress.
	mock.ExpectExec(`UPDATE persistent_vm SET last_active_at`).WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.TouchVM(ctx, "a"))
	_, err = r.TryLockVM(ctx, "a")
	require.ErrorContains(t, err, "operation already in progress")
	require.Equal(t, 1, r.client.Stats().InUse)

	unlock("a")
	a()
	// A competing gateway's lock must also return its local capacity.
	lock("c", false)
	_, err = r.TryLockVM(ctx, "c")
	require.ErrorContains(t, err, "operation already in progress")
	lock("c", true)
	c, err := r.LockVM(ctx, "c")
	require.NoError(t, err)
	unlock("b")
	b()
	unlock("c")
	c()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCanceledVMLockKeepsOtherSessionLocks(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	a, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b"})).
		WillDelayFor(20 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("b", true))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").
		WillReturnResult(sqlmock.NewResult(0, 1))
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = r.LockVM(ctx, "b")
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.TryLockVM(context.Background(), "a")
	require.ErrorContains(t, err, "operation already in progress")
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	a()
	a() // Unlock callbacks are idempotent.
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitLeavesSharedSessionAvailable(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(ctx, "a")
	require.NoError(t, err)

	type result struct {
		unlock func()
		err    error
	}
	waiting := make(chan result, 1)
	go func() { unlock, err := r.LockVM(ctx, "a"); waiting <- result{unlock, err} }()
	select {
	case <-waiting:
		t.Fatal("waiter acquired a held lifecycle lock")
	case <-time.After(20 * time.Millisecond):
	}
	// Waiting on a cannot hold its shared session gate or a pool connection.
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("b", true))
	other, err := r.TryLockVM(ctx, "b")
	require.NoError(t, err)
	mock.ExpectExec(`UPDATE persistent_vm SET last_active_at`).WithArgs("b").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.TouchVM(ctx, "b"))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	other()
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner()
	var acquired result
	select {
	case acquired = <-waiting:
	case <-ctx.Done():
		t.Fatal("waiter did not wake after release")
	}
	require.NoError(t, acquired.err)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	acquired.unlock()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitCancellationKeepsOwner(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r.LockVM(ctx, "a")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = r.TryLockVM(context.Background(), "a")
	require.ErrorContains(t, err, "operation already in progress")
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	owner()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitsForAnotherGateway(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", false))
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	unlock, err := r.LockVM(ctx, "a")
	require.NoError(t, err)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	unlock()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockBatchPreservesIndependentOwners(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	defer owner()
	s := &r.vmLocks[0]
	requests := []*vmLockAcquire{{id: "a"}, {id: "b"}, {id: "b"}, {id: "foreign"}}
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b", "foreign"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("foreign", false).AddRow("b", true))
	results, err := s.acquire(context.Background(), requests)
	require.NoError(t, err)
	require.Equal(t, (<-chan struct{})(s.held["a"]), results[requests[0]].busy.released)
	require.NotNil(t, results[requests[1]].conn)
	require.Equal(t, (<-chan struct{})(s.held["b"]), results[requests[2]].busy.released)
	require.NotNil(t, results[requests[3]].busy)
	require.Nil(t, results[requests[3]].busy.released, "a remote owner must not become a local owner")
	require.Len(t, s.held, 2)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	s.gate <- struct{}{}
	s.release("b", results[requests[1]].conn)
	<-s.gate
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	owner()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockBatchDiscardsAmbiguousAcquisitions(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	defer owner()
	s := &r.vmLocks[0]
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b", "c"})).
		WillReturnError(errors.New("connection lost while acquiring locks"))
	_, err = s.acquire(context.Background(), []*vmLockAcquire{{id: "b"}, {id: "c"}})
	require.ErrorContains(t, err, "connection lost")
	require.Nil(t, s.conn, "possibly acquired locks must never return to the connection pool")
	require.Len(t, s.held, 1, "the existing caller keeps its local fence until it finishes")
	owner()
	require.Empty(t, s.held)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}
