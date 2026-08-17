package worker

import (
	"context"
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
			WorkerId:         s.workerId,
			WorkerInstanceId: s.workerInstanceId,
			StorageNodeId:    s.machineID,
		})
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}

			log.Warn().Err(err).Msg("failed to connect worker event stream")
			if !waitForReconnect(s.ctx, delay) {
				return
			}
			delay = nextReconnectDelay(delay, workerEventStreamReconnectMax)
			continue
		}

		delay = workerEventStreamReconnectMin
		s.cancelStoppingContainers()

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

		if !waitForReconnect(s.ctx, delay) {
			return
		}
		delay = nextReconnectDelay(delay, workerEventStreamReconnectMax)
	}
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextReconnectDelay(delay, maximum time.Duration) time.Duration {
	delay *= 2
	if delay > maximum {
		return maximum
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

		s.cancelContainer(e.StopBuild.ContainerId)
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

func (s *Worker) registerContainerCancel(containerID string, cancel context.CancelFunc) {
	if s.containerCancels == nil {
		s.containerCancels = common.NewSafeMap[context.CancelFunc]()
	}

	s.containerCancels.Set(containerID, cancel)
}

func (s *Worker) unregisterContainerCancel(containerID string) {
	if s.containerCancels == nil {
		return
	}

	s.containerCancels.Delete(containerID)
}

func (s *Worker) cancelContainer(containerID string) bool {
	if s.containerCancels == nil {
		return false
	}

	cancel, ok := s.containerCancels.Get(containerID)
	if !ok {
		return false
	}

	log.Info().Str("container_id", containerID).Msg("cancelling container startup context")
	cancel()
	return true
}

func (s *Worker) cancelStoppingContainers() {
	if s.containerCancels == nil {
		return
	}
	s.containerCancels.Range(func(containerID string, cancel context.CancelFunc) bool {
		s.cancelContainerIfAlreadyStopping(cancel, containerID)
		return true
	})
}
