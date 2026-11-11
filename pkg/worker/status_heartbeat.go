package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// containerLeasesPerCall is how many containers one RenewContainerLeases call
// carries. The gateway accepts twice as many.
const containerLeasesPerCall = 500

// containerStatusHeartbeats are the containers whose status the worker
// reports to the gateway. One loop serves them all: a container that is
// simply alive is renewed in a call shared with the worker's other
// containers, where a loop for each made two calls for each.
type containerStatusHeartbeats struct {
	mu         sync.Mutex
	containers map[string]*containerStatusHeartbeat
	running    bool
}

type containerStatusHeartbeat struct {
	request *types.ContainerRequest
	// alone is set while a heartbeat of this container alone is under way.
	// The loop leaves the container out until it has returned.
	alone bool
}

// startContainerStatusHeartbeat begins the container's lifetime status
// heartbeat. It renews the persisted lease, reconciles pending->running from
// the runtime start signal, and stops containers whose state disappeared or
// turned STOPPING. The claim already refreshed the lease, so the first update
// waits for the loop's next interval. The heartbeat ends when the instance
// exits, when stopContainerStatusHeartbeat is called, or with the worker.
func (s *Worker) startContainerStatusHeartbeat(request *types.ContainerRequest) {
	heartbeats := &s.statusHeartbeats
	heartbeats.mu.Lock()
	defer heartbeats.mu.Unlock()

	if heartbeats.containers == nil {
		heartbeats.containers = make(map[string]*containerStatusHeartbeat)
	}
	heartbeats.containers[request.ContainerId] = &containerStatusHeartbeat{request: request}
	if !heartbeats.running {
		heartbeats.running = true
		go s.runContainerStatusHeartbeats(containerStatusUpdateInterval)
	}
}

// stopContainerStatusHeartbeat ends the heartbeat this request started. A
// later request for the same container id keeps its own.
func (s *Worker) stopContainerStatusHeartbeat(request *types.ContainerRequest) {
	heartbeats := &s.statusHeartbeats
	heartbeats.mu.Lock()
	defer heartbeats.mu.Unlock()

	if heartbeat := heartbeats.containers[request.ContainerId]; heartbeat != nil && heartbeat.request == request {
		delete(heartbeats.containers, request.ContainerId)
	}
}

// runContainerStatusHeartbeats updates the status of every container with a
// heartbeat, once an interval. It returns when no container is left, or when
// the worker shuts down.
func (s *Worker) runContainerStatusHeartbeats(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	heartbeats := &s.statusHeartbeats
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.updateContainerStatuses(s.ctx)

		heartbeats.mu.Lock()
		if len(heartbeats.containers) == 0 {
			heartbeats.running = false
			heartbeats.mu.Unlock()
			return
		}
		heartbeats.mu.Unlock()
	}
}

// updateContainerStatuses is one interval's heartbeat. Containers with
// nothing to report but that they are alive have their leases renewed
// together. Every other container gets updateContainerStatusOnce, which ends
// the heartbeat of one that exited and publishes STOPPING for one that is
// stopping. So does a managed endpoint, whose longer lease the gateway grants
// on that call only.
func (s *Worker) updateContainerStatuses(ctx context.Context) {
	heartbeats := &s.statusHeartbeats
	heartbeats.mu.Lock()
	due := make([]*containerStatusHeartbeat, 0, len(heartbeats.containers))
	for _, heartbeat := range heartbeats.containers {
		if !heartbeat.alone {
			due = append(due, heartbeat)
		}
	}
	heartbeats.mu.Unlock()

	alive := due[:0]
	for _, heartbeat := range due {
		request := heartbeat.request
		instance, exists := s.containerInstances.Get(request.ContainerId)
		if exists && !strings.HasPrefix(request.ContainerId, "managed-") {
			if exitCode, localStopReason := instance.lifecycleState(); exitCode < 0 && localStopReason == "" {
				alive = append(alive, heartbeat)
				continue
			}
		}
		s.updateContainerStatusAlone(heartbeat, func() (bool, error) {
			return s.updateContainerStatusOnce(ctx, request)
		})
	}

	var calls sync.WaitGroup
	for containers := range slices.Chunk(alive, containerLeasesPerCall) {
		calls.Go(func() { s.renewContainerLeases(ctx, containers) })
	}
	calls.Wait()
}

// updateContainerStatusAlone runs one container's heartbeat away from the
// loop, so that a slow one holds up no other container. update reports
// whether the container's heartbeat is over.
func (s *Worker) updateContainerStatusAlone(heartbeat *containerStatusHeartbeat, update func() (bool, error)) {
	heartbeats := &s.statusHeartbeats
	heartbeats.mu.Lock()
	heartbeat.alone = true
	heartbeats.mu.Unlock()

	go func() {
		done, err := update()
		if err != nil {
			log.Error().Str("container_id", heartbeat.request.ContainerId).Err(err).Msg("unable to update container state")
		}

		heartbeats.mu.Lock()
		heartbeat.alone = false
		heartbeats.mu.Unlock()
		if done {
			s.stopContainerStatusHeartbeat(heartbeat.request)
		}
	}()
}

// renewContainerLeases renews the leases of containers that are alive in one
// call, then acts on what the gateway found for each of them.
func (s *Worker) renewContainerLeases(ctx context.Context, heartbeats []*containerStatusHeartbeat) {
	request := &pb.RenewContainerLeasesRequest{}
	sent := make([]*containerStatusHeartbeat, 0, len(heartbeats))
	fenced := make([]*ContainerInstance, 0, len(heartbeats))
	for _, heartbeat := range heartbeats {
		// Finalization holds this lock while it publishes STOPPING and
		// records the exit, and a renewal must not be under way while it
		// does. So the lock is kept until the call returns, and a container
		// being finalized is left for the next interval, which finds it
		// exited.
		instance, exists := s.containerInstances.Get(heartbeat.request.ContainerId)
		if !exists || !instance.statusHeartbeatMu.TryLock() {
			continue
		}
		if exitCode, _ := instance.lifecycleState(); exitCode >= 0 {
			instance.statusHeartbeatMu.Unlock()
			continue
		}
		fenced = append(fenced, instance)
		sent = append(sent, heartbeat)
		runtimeStarted, _ := instance.runtimeStartState()
		request.Containers = append(request.Containers, &pb.ContainerLeaseRenewal{
			ContainerId:    heartbeat.request.ContainerId,
			RuntimeStarted: runtimeStarted,
		})
	}
	if len(sent) == 0 {
		return
	}

	// Allow a drained gateway connection to complete its TCP/TLS handshake.
	// Bound the whole heartbeat to half its refresh interval.
	callCtx, cancel := context.WithTimeout(ctx, containerStatusUpdateInterval/2)
	response, err := handleGRPCResponse(s.containerRepoClient.RenewContainerLeases(callCtx, request))
	cancel()
	for _, instance := range fenced {
		instance.statusHeartbeatMu.Unlock()
	}

	if status.Code(err) == codes.Unimplemented {
		// This gateway is older than RenewContainerLeases. It may be
		// replaced, or be one of several, so only this interval falls back
		// to the calls it does have.
		for _, heartbeat := range sent {
			s.updateContainerStatusAlone(heartbeat, func() (bool, error) {
				return s.updateContainerStatusOnce(ctx, heartbeat.request)
			})
		}
		return
	}
	if err == nil && len(response.Leases) != len(sent) {
		err = fmt.Errorf("gateway answered for %d of %d containers", len(response.Leases), len(sent))
	}
	if err != nil {
		log.Error().Int("containers", len(sent)).Err(err).Msg("unable to update container state")
		return
	}

	for i, lease := range response.Leases {
		heartbeat := sent[i]
		if lease.Ok && !lease.Started && types.ContainerStatus(lease.Status) != types.ContainerStatusStopping {
			log.Debug().Str("container_id", heartbeat.request.ContainerId).Str("image_id", heartbeat.request.ImageId).Msg("container still running")
			continue
		}
		// Stopping a container can take a while.
		s.updateContainerStatusAlone(heartbeat, func() (bool, error) {
			ctx, cancel := context.WithTimeout(ctx, containerStatusUpdateInterval/2)
			defer cancel()
			return false, s.applyContainerLease(ctx, heartbeat.request, lease)
		})
	}
}

// applyContainerLease acts on a lease that is not a plain renewal, the way
// updateContainerStatusOnce acts on the state it reads: a container whose
// state is gone is stopped as an orphan, a STOPPING one is stopped, and a
// container the gateway just moved from PENDING to RUNNING is recorded.
func (s *Worker) applyContainerLease(ctx context.Context, request *types.ContainerRequest, lease *pb.ContainerLease) error {
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return nil
	}

	if !lease.Ok {
		err := errors.New(lease.ErrorMsg)
		notFoundErr := &types.ErrContainerStateNotFound{}
		if !notFoundErr.From(err) {
			return err
		}
		_, previousStopReason := instance.lifecycleState()
		s.handleObservedOrphanedContainer(request.ContainerId, types.EventSourceWorkerStatusHeartbeat)
		if previousStopReason == "" {
			go s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
				ID:          types.ContainerEventWorkerOrphanStateMissing,
				ContainerID: request.ContainerId,
				Reason:      string(types.StopContainerReasonUnknown),
				Source:      types.EventSourceWorkerStatusHeartbeat.String(),
				Message:     types.EventMessageWorkerOrphanStateMissing.String(),
			})
		}
		return nil
	}
	if exitCode, _ := instance.lifecycleState(); exitCode >= 0 {
		return nil
	}

	log.Debug().Str("container_id", request.ContainerId).Str("image_id", request.ImageId).Msg("container still running")

	if lease.Started {
		_, runtimePID := instance.runtimeStartState()
		log.Info().
			Str("container_id", request.ContainerId).
			Int("pid", runtimePID).
			Msg("reconciling pending container to running from runtime start signal")
		s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
			ID:          types.ContainerEventWorkerPendingReconciled,
			ContainerID: request.ContainerId,
			Source:      types.EventSourceWorkerStatusHeartbeat.String(),
			Message:     types.EventMessagePendingReconciledRunning.String(),
			Attrs: map[string]string{
				types.EventAttrRuntimePID: fmt.Sprintf("%d", runtimePID),
			},
		})
	}
	// Finalization owns the STOPPING lease; the gateway did not renew it.
	if types.ContainerStatus(lease.Status) == types.ContainerStatusStopping {
		if lease.Evicting {
			// The scheduler picked this container as an eviction victim. The
			// displacing request normally drives the stop, but if it was
			// requeued elsewhere this path still reclaims the capacity, on
			// the container's own drain window.
			_, _, err := s.releaseVictim(ctx, request.ContainerId, min(time.Duration(lease.DrainSeconds)*time.Second, maxPreemptionDrain), "")
			return err
		}
		s.handleObservedStoppingContainer(request.ContainerId, types.EventSourceWorkerStatusHeartbeat)
	}
	return nil
}
