package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWarmPostgresConnectionsHoldsDistinctConnectionsAndReleases(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(8)
	require.NoError(t, warmPostgresConnections(context.Background(), db, 8))
	require.Equal(t, 8, db.Stats().OpenConnections)
	require.Equal(t, 8, db.Stats().Idle)
	require.Zero(t, db.Stats().InUse)
}

func TestWarmPostgresConnectionsCapsOpenAndUnboundedPools(t *testing.T) {
	for _, maximum := range []int{0, 3} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		db.SetMaxOpenConns(maximum)
		require.NoError(t, warmPostgresConnections(context.Background(), db, 10000))
		want := 128
		if maximum > 0 {
			want = maximum
		}
		require.Equal(t, want, db.Stats().Idle)
		require.Zero(t, db.Stats().InUse)
		for range want {
			mock.ExpectClose()
		}
		require.NoError(t, db.Close())
	}
}

func TestWarmPostgresConnectionsCancellationReleasesPartialPool(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(2)
	existing, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer existing.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, warmPostgresConnections(ctx, db, 2), context.DeadlineExceeded)
	require.Equal(t, 1, db.Stats().InUse, "only the caller's preexisting connection remains checked out")
	require.Equal(t, 1, db.Stats().Idle, "the partial warmup must return its connection")
}

func TestWarmPostgresConnectionsDisabledDoesNotAcquire(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, requested := range []int{0, -1} {
		require.NoError(t, warmPostgresConnections(ctx, db, requested))
	}
	require.Zero(t, db.Stats().InUse)
}
