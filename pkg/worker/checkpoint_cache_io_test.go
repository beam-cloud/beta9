package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckpointCacheQuietWaitsForAllCriticalIO(t *testing.T) {
	var activity checkpointIOActivity
	one, two := activity.begin(), activity.begin()
	done := make(chan error, 1)
	go func() { done <- activity.wait(context.Background(), 20*time.Millisecond) }()
	one()
	one() // Releases are idempotent.
	select {
	case <-done:
		t.Fatal("cache fill started with foreground disk I/O active")
	case <-time.After(30 * time.Millisecond):
	}
	start := time.Now()
	two()
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

func TestCheckpointCacheQuietCancels(t *testing.T) {
	var activity checkpointIOActivity
	activity.begin()()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- activity.wait(ctx, time.Hour) }()
	release := activity.begin()
	release()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	activity.mu.Lock()
	require.Zero(t, activity.active)
	activity.mu.Unlock()
}

func TestCheckpointCacheQuietExtendsAfterNewCriticalIO(t *testing.T) {
	var activity checkpointIOActivity
	activity.begin()()
	done := make(chan error, 1)
	go func() { done <- activity.wait(context.Background(), 40*time.Millisecond) }()
	end := activity.begin()
	select {
	case <-done:
		t.Fatal("new critical I/O did not hold the idle waiter")
	case <-time.After(60 * time.Millisecond):
	}
	start := time.Now()
	end()
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestCheckpointCacheQuietConfiguration(t *testing.T) {
	for _, value := range []string{"", "bad", "-1ms", "2001ms"} {
		t.Setenv("BETA9_CHECKPOINT_CACHE_QUIET", value)
		require.Zero(t, checkpointCacheQuietPeriod())
	}
	t.Setenv("BETA9_CHECKPOINT_CACHE_QUIET", "250ms")
	require.Equal(t, 250*time.Millisecond, checkpointCacheQuietPeriod())
}
