package worker

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The request loop looks for borrowed containers after every request it
// receives, and the search parses each container's stub config. A worker that
// has just been sent a request, is not draining and holds no headroom cannot
// reclaim anything, so it must not search: with thousands of containers on one
// worker the search is what decides how fast it can take requests.
func TestBusyWorkerDoesNotSearchForBorrowedContainers(t *testing.T) {
	worker, rt := evictionWorkerForTest(true)
	for i := 0; i < 1000; i++ {
		addRunningInstance(worker, rt, fmt.Sprintf("container-%d", i)).Request.Stub.Config = `{"cpu_tier":"performance"}`
	}
	addSpotInstance(worker, rt, "spot")
	now := time.Now()

	allocations := testing.AllocsPerRun(20, func() {
		worker.reclaimBorrowedCapacity(now, now)
	})

	require.Zero(t, allocations, "the worker parsed its containers' stub configs")
	require.False(t, worker.reclaiming.Load())
	require.Empty(t, rt.observed("spot"))

	// The search itself is the cost being avoided.
	searching := testing.AllocsPerRun(1, func() { worker.borrowedContainers() })
	require.GreaterOrEqual(t, searching, 1000.0)
}

// Every case in which reclaimBorrowedCapacity acts once it knows the
// containers is one it still searches in.
func TestWorkerSearchesWheneverBorrowedCapacityCouldBeReclaimed(t *testing.T) {
	now := time.Now()
	recent, idle := now, now.Add(-time.Duration(defaultWorkerSpindownTimeS+1)*time.Second)
	flags := []bool{false, true}
	for _, reclaiming := range flags {
		for _, draining := range flags {
			for _, headroom := range flags {
				for _, persistent := range flags {
					for _, last := range []time.Time{recent, idle} {
						worker := &Worker{draining: draining, persistent: persistent}
						worker.reclaiming.Store(reclaiming)
						worker.poolHeadroom.Store(headroom)

						// What the function does with borrowed containers on the
						// worker and nothing else, the case that allows the most.
						acts := !reclaiming && (draining || headroom || (!persistent && last.Equal(idle)))

						name := fmt.Sprintf("reclaiming=%t draining=%t headroom=%t persistent=%t idle=%t", reclaiming, draining, headroom, persistent, last.Equal(idle))
						require.Equal(t, acts, worker.mayReclaimBorrowed(last, now), name)
					}
				}
			}
		}
	}
}

func TestDrainingWorkerStillReclaimsBorrowedContainers(t *testing.T) {
	worker, rt := evictionWorkerForTest(true)
	worker.draining = true
	addSpotInstance(worker, rt, "spot")
	now := time.Now()

	worker.reclaimBorrowedCapacity(now, now)

	require.Eventually(t, func() bool { return len(rt.observed("spot")) > 0 }, time.Second, time.Millisecond)
	require.Equal(t, syscall.SIGTERM, rt.observed("spot")[0])
	require.Eventually(t, func() bool { return !worker.reclaiming.Load() }, time.Second, time.Millisecond)
}
