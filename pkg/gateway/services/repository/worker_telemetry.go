package repository_services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"strings"
)

func (s *WorkerRepositoryService) PublishWorkerTelemetry(ctx context.Context, req *pb.PublishWorkerTelemetryRequest) (*pb.PublishWorkerTelemetryResponse, error) {
	fail := func(err error) (*pb.PublishWorkerTelemetryResponse, error) {
		return &pb.PublishWorkerTelemetryResponse{ErrorMsg: err.Error()}, nil
	}
	if req == nil || req.WorkerId == "" {
		return fail(fmt.Errorf("worker is required"))
	}
	batch := append([]string(nil), req.EventsJson...)
	if req.EventJson != "" {
		batch = append(batch, req.EventJson)
	}
	if len(batch) == 0 || len(batch) > 128 {
		return fail(fmt.Errorf("invalid event batch"))
	}
	size := 0
	events := make([]cloudevents.Event, 0, len(batch))
	for _, data := range batch {
		size += len(data)
		if size > 1<<20 {
			return fail(fmt.Errorf("event batch is too large"))
		}
		event, err := s.authorizeWorkerEvent(ctx, req.WorkerId, data)
		if err != nil {
			return fail(err)
		}
		events = append(events, event)
	}
	publisher, ok := s.eventRepo.(interface{ PublishWorkerEvent(cloudevents.Event) error })
	if !ok {
		return fail(fmt.Errorf("event storage is unavailable"))
	}
	for _, event := range events {
		if err := publisher.PublishWorkerEvent(event); err != nil {
			return fail(err)
		}
	}
	return &pb.PublishWorkerTelemetryResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) authorizeWorkerEvent(ctx context.Context, workerID, payload string) (cloudevents.Event, error) {
	fail := func(err error) (cloudevents.Event, error) { return cloudevents.Event{}, err }
	var event cloudevents.Event
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return fail(err)
	}
	if event.ID() == "" || !workerEventTypes[event.Type()] {
		return fail(fmt.Errorf("unsupported event"))
	}
	var data map[string]interface{}
	if err := event.DataAs(&data); err != nil || data == nil {
		return fail(fmt.Errorf("event data is required"))
	}
	delete(data, "app_id")
	info, ok := auth.AuthInfoFromContext(ctx)
	if ok && info != nil && info.Token != nil {
		if !types.IsWorkerTokenType(info.Token.TokenType) || info.Token.WorkerID == "" || info.Token.WorkerID != workerID {
			return fail(fmt.Errorf("worker credential does not match"))
		}
		worker, err := s.workerRepo.GetWorkerById(workerID)
		if err != nil || worker == nil {
			return fail(fmt.Errorf("worker is unavailable"))
		}
		data["worker_id"], data["machine_id"], data["workspace_id"] = worker.Id, worker.MachineId, worker.WorkspaceId
		containerID, _ := data["container_id"].(string)
		if strings.HasPrefix(event.Type(), "container.") && containerID == "" {
			return fail(fmt.Errorf("container is required"))
		}
		if containerID != "" {
			state, err := reportingContainerForWorker(ctx, s.containerRepo, containerID, workerID)
			if err != nil {
				return fail(err)
			}
			data["workspace_id"], data["stub_id"] = state.WorkspaceId, state.StubId
			stub, err := s.backendRepo.GetStubByExternalId(ctx, state.StubId)
			if err != nil || stub == nil {
				return fail(fmt.Errorf("stub is unavailable"))
			}
			data["app_id"] = stubAppID(stub)
		} else if stubID, _ := data["stub_id"].(string); stubID != "" {
			states, err := s.containerRepo.GetActiveContainersByWorkerId(workerID)
			if err != nil {
				return fail(err)
			}
			assigned := false
			for _, state := range states {
				if state.StubId == stubID && state.WorkerId == workerID {
					data["workspace_id"] = state.WorkspaceId
					assigned = true
					break
				}
			}
			if !assigned {
				return fail(fmt.Errorf("stub is not assigned to worker"))
			}
			stub, err := s.backendRepo.GetStubByExternalId(ctx, stubID)
			if err != nil || stub == nil {
				return fail(fmt.Errorf("stub is unavailable"))
			}
			data["app_id"] = stubAppID(stub)
		}
	} else {
		if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
			return fail(err)
		}
		if !strings.HasPrefix(workerID, "cache-server-") || event.Type() != types.EventPlatformCache {
			return fail(fmt.Errorf("cache event is required"))
		}
		for _, key := range []string{"workspace_id", "stub_id", "container_id", "app_id", "machine_id"} {
			delete(data, key)
		}
		data["worker_id"] = workerID
	}
	canonical := cloudevents.NewEvent()
	canonical.SetID(event.ID())
	canonical.SetType(event.Type())
	canonical.SetSource("beta9-worker")
	canonical.SetTime(event.Time())
	for key, name := range map[string]string{"workspace_id": "workspaceid", "stub_id": "stubid", "container_id": "containerid", "worker_id": "workerid", "app_id": "appid", "machine_id": "machineid"} {
		if value, ok := data[key].(string); ok && value != "" {
			canonical.SetExtension(name, value)
		}
	}
	if err := canonical.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return fail(err)
	}
	return canonical, nil
}

var workerEventTypes = map[string]bool{
	types.EventContainerLifecycle:       true,
	types.EventContainerMetrics:         true,
	types.EventContainerEvent:           true,
	types.EventContainerLog:             true,
	types.EventWorkerLifecycle:          true,
	types.EventStubCacheRequiredContent: true,
	types.EventPlatformCache:            true,
	types.EventWorkerLifecycleStarted:   true,
	types.EventWorkerLifecycleStopped:   true,
	types.EventWorkerLifecycleDeleted:   true,
}
