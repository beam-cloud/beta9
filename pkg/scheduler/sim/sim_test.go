package schedsim_test

import (
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
)

const onePool = `
worker:
  pools:
    a10g:
      gpuType: A10G
      priority: 10
`

func TestPoolGrowsForARequestAndShrinksWhenIdle(t *testing.T) {
	sim := schedsim.New(t, onePool)
	sim.Pool("a10g").BootAfter(30 * time.Second).IdleAfter(5 * time.Minute)

	sim.Submit(schedsim.Endpoint("A10G").For(time.Minute), 1)

	sim.Advance(10 * time.Second)
	sim.ExpectWorkers("a10g", 1)
	sim.ExpectPlaced("a10g", 0)
	sim.ExpectWaiting(1)

	sim.Advance(30 * time.Second)
	sim.ExpectPlaced("a10g", 1)
	sim.ExpectWaiting(0)

	// The container finishes, the worker idles out, and the cleaner removes
	// its Job.
	sim.Advance(10 * time.Minute)
	sim.ExpectWorkers("a10g", 0)
	if jobs := sim.Jobs(); len(jobs) != 0 {
		t.Fatalf("%d worker jobs left after the pool went idle, want 0", len(jobs))
	}
	if started := sim.Pool("a10g").Started(); started != 1 {
		t.Fatalf("pool started %d workers for one request, want 1", started)
	}
}
