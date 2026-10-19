package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

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
