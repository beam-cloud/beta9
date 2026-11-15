package schedsim

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// A worker claims a request before it starts the container, and from then on
// the request is nowhere but with the worker. A worker that is lost between
// the two, in the middle of an image pull say, leaves a container that says
// it is starting and never will. The scheduler cannot place it again, so it
// ends it: whoever asked for the container sees that it is gone, and asks
// for another.

const claimedLossPool = `
worker:
  pools:
    a10g:
      gpuType: A10G
`

// countedByItsOwner reports whether whoever asked for the container still
// counts on it: an endpoint's autoscaler counts the containers that are
// starting or running, and starts one for each that it is short of.
func countedByItsOwner(s *Sim, containerID string) bool {
	state, err := s.containerRepo.GetContainerState(containerID)
	if err != nil {
		return false
	}
	return state.Status == types.ContainerStatusPending || state.Status == types.ContainerStatusRunning
}

func TestAContainerClaimedByAWorkerThatDiedBeforeStartingItIsReplaced(t *testing.T) {
	sim := New(t, claimedLossPool)
	sim.Warm("a10g")

	// The worker claims the request and dies before the container starts.
	worker := sim.Pool("a10g").Workers()[0]
	worker.VanishAfterClaimingWork()
	lost := sim.Submit(Endpoint("A10G"), 1)[0]
	sim.Advance(5 * time.Second)
	require.False(t, worker.Running())
	require.True(t, countedByItsOwner(sim, lost), "the container looks like one that is starting")
	require.Empty(t, sim.backlog(), "and its request is with the worker alone")

	// The worker's record expires, and with it goes the container.
	sim.Advance(2 * time.Minute)
	require.False(t, countedByItsOwner(sim, lost))
	sim.ExpectRequeued(0)

	// Its owner starts another, which runs on a new worker.
	replacement := sim.Submit(Endpoint("A10G"), 1)[0]
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf([]string{replacement}))
	require.NotEqual(t, worker.ID, sim.Container(replacement).Runs[0].WorkerID)
	require.Empty(t, sim.Container(lost).Runs, "the container that was claimed never ran")
}
