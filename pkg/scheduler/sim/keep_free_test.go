package schedsim_test

import (
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Rule 3: every pool keeps its poolSizing.minFree* free.

// floorPools has an A10G pool of one-GPU workers that keeps two GPUs free.
const floorPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        defaultWorkerCPU: 4000m
        defaultWorkerMemory: 8Gi
        defaultWorkerGpuCount: "1"
        minFreeGPU: "2"
`

// freeGPUs counts the GPUs on the pool's workers that are ready and unused.
func freeGPUs(sim *schedsim.Sim, pool string) int {
	free := 0
	for _, worker := range sim.Workers(pool) {
		if worker.Status == types.WorkerStatusAvailable {
			free += int(worker.FreeGpuCount)
		}
	}
	return free
}

func TestPoolReachesItsFloorFromZeroAndHoldsIt(t *testing.T) {
	sim := schedsim.New(t, floorPools)
	sim.Pool("a10g").IdleAfter(time.Minute)

	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())

	// The workers holding the floor have nothing to run for a quarter of an
	// hour, far past their idle timeout. They stay, and none is replaced.
	sim.Advance(15 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

func TestPoolKeepsItsFloorFreeUnderLoadAndReturnsToItAfter(t *testing.T) {
	sim := schedsim.New(t, floorPools)
	sim.Pool("a10g").IdleAfter(time.Minute)
	sim.Advance(2 * time.Minute)

	// Three containers take the two free GPUs and one more. The pool adds
	// workers until two GPUs are free again.
	ids := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 3)
	sim.Advance(3 * time.Minute)

	require.Equal(t, []string{"a10g", "a10g", "a10g"}, sim.PoolsOf(ids))
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 5, sim.Pool("a10g").Started())

	// The load ends. Workers above the floor idle out; the two that hold it
	// stay, so nothing has to be started to replace them.
	sim.Advance(20 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	sim.ExpectWorkers("a10g", 2)
	require.Equal(t, 5, sim.Pool("a10g").Started())
}

// The floor is best effort. A pool that cannot get workers holds what it has
// and keeps trying; when the cluster has room again it reaches the floor.
func TestPoolThatCannotGetWorkersReachesItsFloorOnceItCan(t *testing.T) {
	sim := schedsim.New(t, floorPools)
	sim.Pool("a10g").RefuseWorkers()

	sim.Advance(time.Minute)
	require.Equal(t, 0, freeGPUs(sim, "a10g"))
	require.Equal(t, 0, sim.Pool("a10g").Started())
	require.Positive(t, sim.Pool("a10g").Refused())

	sim.Pool("a10g").AcceptWorkers()
	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

// A pool that says how many GPUs to keep free, and nothing about its
// workers, adds workers with a GPU each. A worker without one would not
// count towards the floor, and the pool would add another every second.
func TestGPUFloorAloneAddsWorkersWithAGPU(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        minFreeGPU: "2"
`)

	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

// A pool with no gpuType has no GPUs to keep free, so it adds no worker for
// a GPU floor. Its other floors hold.
func TestGPUFloorOnAPoolWithoutGPUsAddsNothing(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    cpu:
      poolSizing:
        minFreeGPU: "1"
    cpu-floor:
      poolSizing:
        defaultWorkerCPU: 4000m
        minFreeCPU: 4000m
        minFreeGPU: "1"
`)

	sim.Advance(5 * time.Minute)
	require.Zero(t, sim.Pool("cpu").Started())
	require.Equal(t, 1, sim.Pool("cpu-floor").Started())
}
