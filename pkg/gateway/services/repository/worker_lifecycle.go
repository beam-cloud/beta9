package repository_services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

var errWorkerLifecycleUnauthorized = errors.New("unauthorized worker lifecycle event")

func (s *WorkerRepositoryService) PushContainerLifecycleEvents(ctx context.Context, req *pb.PushContainerLifecycleEventsRequest) (*pb.PushContainerLifecycleEventsResponse, error) {
	authInfo, err := workerLifecycleAuth(ctx, req)
	if err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	if s.eventRepo == nil || s.containerRepo == nil || s.workerRepo == nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: "worker lifecycle service is unavailable"}, nil
	}

	worker, err := s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeWorkerLifecycleWorker(ctx, authInfo, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, worker); err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	states := make(map[string]*types.ContainerState)
	events := make([]types.EventContainerLifecycleSchema, 0, len(req.Events))
	for _, data := range req.Events {
		var event types.EventContainerLifecycleSchema
		if err := json.Unmarshal(data, &event); err != nil || event.ContainerID == "" {
			return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: "invalid lifecycle event"}, nil
		}

		state := states[event.ContainerID]
		if state == nil {
			state, err = s.containerRepo.GetContainerState(event.ContainerID)
			if err != nil {
				return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
			}
			if state.WorkerId != req.WorkerId || state.MachineId != req.StorageNodeId {
				return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: errWorkerLifecycleUnauthorized.Error()}, nil
			}
			if authInfo.Token.TokenType == types.TokenTypeWorkerPrivate && (authInfo.Workspace == nil || authInfo.Workspace.ExternalId != state.WorkspaceId) {
				return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: errWorkerLifecycleUnauthorized.Error()}, nil
			}
			states[event.ContainerID] = state
		}

		if event.ID == "" || len(event.ID) > 128 {
			return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: "invalid lifecycle event identity"}, nil
		}
		event.ContainerID = state.ContainerId
		event.StubID = state.StubId
		event.WorkspaceID = state.WorkspaceId
		event.WorkerID = worker.Id
		event.MachineID = worker.MachineId
		events = append(events, event)
	}
	// The stream RPC may be delayed after its initial token check. Re-read the
	// authoritative process immediately before the irreversible event write so
	// a superseded worker epoch cannot emit lifecycle/billing events for its
	// replacement.
	worker, err = s.workerRepo.GetWorkerById(req.WorkerId)
	if err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	if err := s.authorizeWorkerLifecycleWorker(ctx, authInfo, req.WorkerId, req.WorkerInstanceId, req.StorageNodeId, worker); err != nil {
		return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
	}
	for containerID := range states {
		state, err := s.containerRepo.GetContainerState(containerID)
		if err != nil {
			return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: err.Error()}, nil
		}
		if state.WorkerId != req.WorkerId || state.MachineId != req.StorageNodeId {
			return &pb.PushContainerLifecycleEventsResponse{ErrorMsg: errWorkerLifecycleUnauthorized.Error()}, nil
		}
	}
	for _, event := range events {
		s.eventRepo.PushContainerLifecycleEvent(event)
	}

	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) authorizeWorkerLifecycleWorker(ctx context.Context, authInfo *auth.AuthInfo, workerID, workerInstanceID, storageNodeID string, worker *types.Worker) error {
	if worker == nil || worker.Id != workerID || worker.InstanceId == "" || worker.InstanceId != workerInstanceID ||
		worker.MachineId == "" || worker.MachineId != storageNodeID {
		return errWorkerLifecycleUnauthorized
	}
	if err := authorizeRegisteredWorkerToken(ctx, authInfo, worker, s.computeRepo); err != nil {
		return errWorkerLifecycleUnauthorized
	}
	return nil
}

func workerLifecycleAuth(ctx context.Context, req *pb.PushContainerLifecycleEventsRequest) (*auth.AuthInfo, error) {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return nil, errWorkerLifecycleUnauthorized
	}
	if req == nil || req.WorkerId == "" || req.WorkerInstanceId == "" || req.StorageNodeId == "" || len(req.Events) == 0 {
		return nil, fmt.Errorf("worker process identity and events are required")
	}
	return authInfo, nil
}
