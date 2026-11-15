package schedsim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A Kubernetes pool makes a token for each worker before it asks the cluster
// for the worker's Job. When the cluster refuses the Job, the pool revokes
// the token: a pool that goes on failing to add workers would otherwise
// leave a working credential behind for each one it tried.

func TestAPoolRevokesTheTokenOfEachWorkerItsClusterRefused(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    t4:
      gpuType: T4
`)

	// The request asks the pool for a worker as often as it may, and the
	// cluster refuses each one.
	sim.Pool("t4").RefuseWorkers()
	sim.Submit(Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)
	sim.ExpectFailed(1)
	refused := sim.Pool("t4").Refused()
	require.Positive(t, refused)
	made, revoked := sim.backend.workerTokens()
	require.Equal(t, refused, made, "each attempt made a token")
	require.Equal(t, refused, revoked, "and each token was revoked")

	// A worker the cluster accepts keeps its token.
	sim.Pool("t4").AcceptWorkers()
	ids := sim.Submit(Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"t4"}, sim.PoolsOf(ids))
	made, revoked = sim.backend.workerTokens()
	require.Equal(t, refused+1, made)
	require.Equal(t, refused, revoked)
}
