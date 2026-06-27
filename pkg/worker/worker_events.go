package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

func (s *Worker) listenForWorkerEvents() {
	delay := workerEventStreamReconnectMin

	for {
		stream, err := s.workerRepoClient.StreamWorkerEvents(s.ctx, &pb.StreamWorkerEventsRequest{
			WorkerId:      s.workerId,
			StorageNodeId: s.storageNodeID(),
		})
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}

			log.Warn().Err(err).Msg("failed to connect worker event stream")
			if !s.sleepBeforeWorkerEventReconnect(delay) {
				return
			}
			delay = nextWorkerEventReconnectDelay(delay)
			continue
		}

		delay = workerEventStreamReconnectMin

		for {
			event, err := stream.Recv()
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}

				log.Warn().Err(err).Msg("worker event stream closed")
				break
			}

			s.handleWorkerEvent(event)
		}

		if !s.sleepBeforeWorkerEventReconnect(delay) {
			return
		}
		delay = nextWorkerEventReconnectDelay(delay)
	}
}

func (s *Worker) sleepBeforeWorkerEventReconnect(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextWorkerEventReconnectDelay(delay time.Duration) time.Duration {
	delay *= 2
	if delay > workerEventStreamReconnectMax {
		return workerEventStreamReconnectMax
	}
	return delay
}

func (s *Worker) handleWorkerEvent(event *pb.WorkerEvent) {
	if event == nil {
		return
	}
	if event.Event == nil {
		if event.EventId != types.WorkerEventHeartbeatID {
			log.Warn().Str("event_id", event.EventId).Msg("received empty worker event")
		}
		return
	}

	switch e := event.Event.(type) {
	case *pb.WorkerEvent_StopContainer:
		if e.StopContainer == nil {
			return
		}

		s.handleStopContainerArgs(types.StopContainerArgs{
			ContainerId: e.StopContainer.ContainerId,
			Force:       e.StopContainer.Force,
			Reason:      types.StopContainerReason(e.StopContainer.Reason),
		}, types.EventSourceWorkerEventStream)
	case *pb.WorkerEvent_StopBuild:
		if e.StopBuild == nil {
			return
		}

		s.cancelBuild(e.StopBuild.ContainerId)
	case *pb.WorkerEvent_DurableDisk:
		if e.DurableDisk == nil {
			return
		}
		if e.DurableDisk.WorkerId != "" && e.DurableDisk.WorkerId != s.workerId {
			return
		}
		if e.DurableDisk.StorageNodeId != "" && e.DurableDisk.StorageNodeId != s.storageNodeID() {
			return
		}
		if err := s.handleDurableDiskEvent(e.DurableDisk); err != nil {
			log.Error().Err(err).Str("event_id", event.EventId).Msg("failed to handle durable disk event")
		}
	default:
		log.Warn().Str("event_id", event.EventId).Msg("received unknown worker event")
	}
}

func (s *Worker) storageNodeID() string {
	if s == nil {
		return ""
	}
	return types.StableStorageNodeID(s.machineID, s.workerId)
}

func (s *Worker) handleDurableDiskEvent(event *pb.DurableDiskEvent) error {
	mount := types.NewMountFromProto(event.Mount)
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk event is missing mount metadata")
	}

	switch types.DurableDiskCommandAction(event.Action) {
	case types.DurableDiskCommandActionPrepare:
		return s.prepareDRBDPeerMount(mount)
	case types.DurableDiskCommandActionDemote:
		return s.teardownDRBDDurableDiskMount(nil, mount)
	default:
		return fmt.Errorf("unsupported durable disk action %q", event.Action)
	}
}

func (s *Worker) registerBuildCancel(containerID string, cancel context.CancelFunc) {
	if s.buildCancels == nil {
		s.buildCancels = common.NewSafeMap[context.CancelFunc]()
	}

	s.buildCancels.Set(containerID, cancel)
}

func (s *Worker) unregisterBuildCancel(containerID string) {
	if s.buildCancels == nil {
		return
	}

	s.buildCancels.Delete(containerID)
}

func (s *Worker) cancelBuild(containerID string) bool {
	if s.buildCancels == nil {
		return false
	}

	cancel, ok := s.buildCancels.Get(containerID)
	if !ok {
		return false
	}

	log.Info().Str("container_id", containerID).Msg("received stop build event")
	cancel()
	return true
}
