package clock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFakeRunsTimersInTimeOrder(t *testing.T) {
	c := NewFake(start)
	var fired []string
	c.AfterFunc(3*time.Second, func() { fired = append(fired, "c") })
	c.AfterFunc(time.Second, func() { fired = append(fired, "a") })
	c.AfterFunc(time.Second, func() { fired = append(fired, "b") })

	c.Advance(2 * time.Second)
	require.Equal(t, []string{"a", "b"}, fired)
	require.Equal(t, start.Add(2*time.Second), c.Now())

	c.Advance(time.Second)
	require.Equal(t, []string{"a", "b", "c"}, fired)
}

func TestFakeTimerSeesItsOwnDueTime(t *testing.T) {
	c := NewFake(start)
	var at time.Time
	c.AfterFunc(time.Second, func() { at = c.Now() })
	c.Advance(time.Minute)
	require.Equal(t, start.Add(time.Second), at)
}

func TestFakeEveryTicksUntilCancelled(t *testing.T) {
	c := NewFake(start)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := 0
	c.Every(ctx, time.Second, func() { ticks++ })

	c.Advance(3500 * time.Millisecond)
	require.Equal(t, 3, ticks)

	cancel()
	c.Advance(time.Minute)
	require.Equal(t, 3, ticks)
}

func TestFakeStop(t *testing.T) {
	c := NewFake(start)
	fired := false
	timer := c.AfterFunc(time.Second, func() { fired = true })
	require.True(t, timer.Stop())
	require.False(t, timer.Stop())
	c.Advance(time.Minute)
	require.False(t, fired)
}

func TestFakeRunsTimersAddedByCallbacks(t *testing.T) {
	c := NewFake(start)
	var fired []time.Duration
	c.AfterFunc(time.Second, func() {
		fired = append(fired, c.Now().Sub(start))
		c.AfterFunc(time.Second, func() { fired = append(fired, c.Now().Sub(start)) })
	})
	c.Advance(5 * time.Second)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, fired)
}

func TestFakeOnStepSeesEveryMove(t *testing.T) {
	c := NewFake(start)
	var total time.Duration
	c.OnStep(func(d time.Duration) { total += d })
	c.AfterFunc(time.Second, func() {})
	c.Advance(10 * time.Second)
	require.Equal(t, 10*time.Second, total)
}
