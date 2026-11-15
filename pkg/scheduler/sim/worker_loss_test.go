package schedsim_test

import (
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/stretchr/testify/require"
)

// A worker that stops without warning leaves requests behind: the ones the
// scheduler queued on it before it noticed. They must return to the backlog
// and run somewhere else.

func TestRequestsQueuedOnALostWorkerRunElsewhere(t *testing.T) {
	sim := schedsim.New(t, onePool)
	sim.Warm("a10g")

	worker := sim.Pool("a10g").Workers()[0]
	worker.Vanish()

	// For up to a minute the scheduler still believes the worker is there
	// and places work on it.
	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events("container.placed"), 2, "the request and the warm-up should both have been placed")
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead worker")

	// The worker's state expires, the cleaner removes its Job, and the
	// request is placed again on a new worker.
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.NotEqual(t, worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

func TestRequestsDeliveredToALostWorkerRunElsewhere(t *testing.T) {
	sim := schedsim.New(t, onePool)
	sim.Warm("a10g")

	// The worker takes the request off its queue and dies before starting
	// the container.
	worker := sim.Pool("a10g").Workers()[0]
	worker.VanishAfterTakingWork()
	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.False(t, worker.Running())
	require.Equal(t, []string{""}, sim.PoolsOf(ids))

	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.NotEqual(t, worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}
