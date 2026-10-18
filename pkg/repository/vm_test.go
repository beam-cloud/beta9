package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestVMLocksLeaveConnectionsForLifecycleQueries(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	ctx := context.Background()

	lock := func(id string, acquired bool) {
		mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(id).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(acquired))
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
	_, err = r.LockVM(ctx, "a")
	require.ErrorContains(t, err, "operation already in progress")
	require.Equal(t, 1, r.client.Stats().InUse)

	unlock("a")
	a()
	// A competing gateway's lock must also return its local capacity.
	lock("c", false)
	_, err = r.LockVM(ctx, "c")
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
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs("a").
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	a, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs("b").
		WillDelayFor(20 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").
		WillReturnResult(sqlmock.NewResult(0, 1))
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = r.LockVM(ctx, "b")
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.LockVM(context.Background(), "a")
	require.ErrorContains(t, err, "operation already in progress")
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	a()
	a() // Unlock callbacks are idempotent.
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}
