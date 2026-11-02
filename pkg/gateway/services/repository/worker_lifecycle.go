package repository_services

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

var errWorkerLifecycleUnauthorized = errors.New("unauthorized worker lifecycle event")

func (s *WorkerRepositoryService) PushContainerLifecycleEvents(ctx context.Context, req *pb.PushContainerLifecycleEventsRequest) (*pb.PushContainerLifecycleEventsResponse, error) {
	caller, err := auth.WorkerFromContext(ctx)
	if err != nil || req == nil || req.WorkerId != caller.ID {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: errWorkerLifecycleUnauthorized.Error()}, nil
	}
	if len(req.Events) == 0 {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: "events are required"}, nil
	}
	if s.eventRepo == nil || s.containerRepo == nil || s.workerRepo == nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: "worker lifecycle service is unavailable"}, nil
	}

	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	if worker == nil || worker.Id != req.WorkerId {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: errWorkerLifecycleUnauthorized.Error()}, nil
	}
	states := make(map[string]*types.ContainerState)
	events := make([]types.EventContainerLifecycleSchema, 0, len(req.Events))
	// Per-event problems drop that event only: a poison event must not wedge the worker's relay by failing every retry of the batch it is in.
	for _, data := range req.Events {
		var event types.EventContainerLifecycleSchema
		if err := json.Unmarshal(data, &event); err != nil || event.ContainerID == "" {
			continue
		}
		if event.ID == "" || len(event.ID) > 128 {
			continue
		}

		state, seen := states[event.ContainerID]
		if !seen {
			state, err = s.containerRepo.GetContainerState(event.ContainerID)
			if err != nil {
				if !(&types.ErrContainerStateNotFound{}).From(err) {
					return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
				}
				state = nil
			}
			if !caller.Owns(state) {
				state = nil
			}
			states[event.ContainerID] = state
		}
		if state == nil {
			continue
		}

		event.ContainerID = state.ContainerId
		event.StubID = state.StubId
		event.WorkspaceID = state.WorkspaceId
		event.WorkerID = worker.Id
		event.MachineID = worker.MachineId
		events = append(events, event)
	}
	for _, event := range events {
		s.eventRepo.PushContainerLifecycleEvent(event)
	}

	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}
