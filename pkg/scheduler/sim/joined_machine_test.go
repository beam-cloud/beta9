package schedsim

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// A managed pool can hold machines that nobody bought: an operator starts the
// agent on each one and it joins. The simulation has no such machines of its
// own, so these tests play the operator, the machine's agent and the
// gateway's reconcile of the pool.

// joinedPool is a managed pool of A10G machines that are joined by hand.
const joinedPool = `
worker:
  pools:
    joined-a10g:
      mode: external
      gpuType: A10G
`

// reconcilePool loads the pool into every scheduler, now and every
// poolSyncInterval from now on, as the gateway does with its managed pools.
func reconcilePool(s *Sim, name string) {
	s.t.Helper()
	reconcile := func() {
		for _, replica := range s.schedulers {
			if err := replica.EnsureAgentPool(adminWorkspace, s.poolState(name)); err != nil {
				s.t.Fatalf("schedsim: reconcile pool %s: %v", name, err)
			}
		}
	}
	reconcile()
	s.Clock.Every(s.ctx, poolSyncInterval, reconcile)
}

// joinedMachine is a machine that an operator joined to a pool.
type joinedMachine struct {
	*machine
	agentStopped bool
}

// joinByHand adds a machine to the pool the way an operator does: the machine
// is given a token for the pool, its agent joins with it, and then heartbeats
// for as long as it runs.
func joinByHand(s *Sim, pool, id string) *joinedMachine {
	s.t.Helper()
	s.mu.Lock()
	s.tokens[id] = pool
	s.mu.Unlock()

	joined := &joinedMachine{machine: &machine{sim: s, id: id, pool: pool, instanceID: id, offer: providers.Offer{
		GPU: s.Config.Worker.Pools[pool].GPUType, GPUCount: 1, CPUMillicores: defaultMachineCPU, MemoryMB: defaultMachineMemoryMB,
	}}}
	joined.join(s.Clock.Now())
	if joined.state != machineJoined {
		s.t.Fatalf("schedsim: machine %s did not join pool %s", id, pool)
	}
	s.Clock.Every(s.ctx, workerInterval, func() {
		if joined.state == machineJoined && !joined.agentStopped {
			joined.heartbeat(s.Clock.Now())
		}
	})
	return joined
}

// stopAgent ends the machine's agent: the gateway hears no more heartbeats
// from the machine. Its worker runs on.
func (m *joinedMachine) stopAgent() { m.agentStopped = true }

// A machine that dies leaves behind the requests the scheduler queued on its
// worker before it noticed. The gateway goes on reconciling the pool, and
// that must not keep the dead worker on record: its requests go back to the
// backlog only once its record has expired.

func TestRequestsQueuedOnAJoinedMachineThatDiedRunElsewhere(t *testing.T) {
	sim := New(t, joinedPool)
	reconcilePool(sim, "joined-a10g")
	lost := joinByHand(sim, "joined-a10g", "machine-a")
	sim.Advance(5 * time.Second)
	require.True(t, lost.worker.Running())

	// The machine shuts down: its agent stops first and its worker a little
	// later. The gateway counts the machine as gone a minute after the
	// agent's last heartbeat. That is before the worker's record expires, so
	// a reconcile in between disables the worker, and every reconcile after
	// it finds the machine gone again.
	lost.stopAgent()
	sim.Advance(poolSyncInterval + 10*time.Second)
	lost.stop()

	// Until then the scheduler believes the worker is there and places work
	// on it.
	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events(types.EventComputeActionContainerPlaced), 1)
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead machine")

	// The operator joins another machine, and the request runs there.
	replacement := joinByHand(sim, "joined-a10g", "machine-b")
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, replacement.worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

func TestRequestsQueuedOnAWorkerThatDiedOnAJoinedMachineRunElsewhere(t *testing.T) {
	sim := New(t, joinedPool)
	reconcilePool(sim, "joined-a10g")
	lost := joinByHand(sim, "joined-a10g", "machine-a")
	sim.Advance(5 * time.Second)
	require.True(t, lost.worker.Running())

	// The worker dies and its machine stays up: the agent keeps heartbeating,
	// so every reconcile finds a machine to look after and saves its worker.
	lost.worker.Vanish()

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events(types.EventComputeActionContainerPlaced), 1)
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead worker")

	replacement := joinByHand(sim, "joined-a10g", "machine-b")
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, replacement.worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}
