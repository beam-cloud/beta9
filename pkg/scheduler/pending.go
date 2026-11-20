package scheduler

import (
	"slices"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// What a waiting request waits for.
//
// A container is PENDING from the moment it is asked for until it runs. That
// is a second when a worker has room, minutes when a machine has to start,
// and the whole scheduling window when there is no capacity to be had. The
// scheduler is the one that knows which, so it stores it on the container's
// state (types.ContainerPendingPhase) each time it sends a request back to
// wait. Whoever waits with the container reads it from there: the client
// that called the function, a list of containers, the dashboard.
//
// A request passes several times a second while it waits. It carries what it
// last stored (ContainerRequest.PendingReported) and stores only a change.

// waitsFor stores that the request waits in this phase, for this reason.
func (a *schedulingAttempt) waitsFor(phase types.ContainerPendingPhase, reason types.ContainerPendingReason) {
	a.storePending(string(phase)+"/"+string(reason), phase, reason)
}

// waitsForWorker stores that the request waits for a worker that the pool is
// adding. ok is false when the pool is not known.
func (a *schedulingAttempt) waitsForWorker(pool *WorkerPool, ok bool) {
	if !ok {
		a.waitsFor(types.ContainerPendingProvisioning, types.ContainerPendingWorkerStarting)
		return
	}
	// The pool decides the reason, and the route is not worked out again
	// for a pool that was stored already.
	reported := string(types.ContainerPendingProvisioning) + "/" + pool.Name
	if a.request.PendingReported == reported {
		return
	}
	reason := types.ContainerPendingWorkerStarting
	if a.scheduler.routeFor(a.request).backup(pool.Name) {
		reason = types.ContainerPendingBackupWorkerStarting
	}
	a.storePending(reported, types.ContainerPendingProvisioning, reason)
}

// storePending stores what the request waits for, unless reported says it
// is what the request stored last.
func (a *schedulingAttempt) storePending(reported string, phase types.ContainerPendingPhase, reason types.ContainerPendingReason) {
	if a.request.PendingReported == reported {
		return
	}
	// A container that is no longer waiting keeps its state as it is, and
	// the request stops asking for it.
	if _, err := a.scheduler.containerRepo.SetContainerPending(a.request.ContainerId, phase, reason, a.scheduler.now()); err != nil {
		// What a request waits for is only shown. It is stored on the next
		// pass instead.
		requestLog(log.Debug(), a.request).Err(err).Msg("unable to store what the request waits for")
		return
	}
	a.request.PendingReported = reported
}

// backup reports whether the pool is one the route falls back on: a pool it
// tries only after another. Pools that the route tells apart by name alone
// are tried first together, and none of them is a backup for the others. A
// pool that is not in the route is not known to be one.
func (r route) backup(name string) bool {
	i := slices.IndexFunc(r.pools, func(pool routedPool) bool { return pool.Name == name })
	return i > 0 && r.pools[i].standing() != r.pools[0].standing()
}

// poolStanding is what orders the pools of a route, apart from their names.
type poolStanding struct {
	overflow bool
	fallback bool
	asked    int
	priority int32
}

func (p routedPool) standing() poolStanding {
	return poolStanding{overflow: p.overflow, fallback: p.fallback, asked: p.asked, priority: p.Config.Priority}
}
