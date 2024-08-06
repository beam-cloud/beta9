package repository

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type TCPEventClientRepo struct {
	config            types.FluentBitEventConfig
	endpointAvailable bool
	eventTagMap       map[string]string
}

func NewTCPEventClientRepo(config types.FluentBitEventConfig) EventRepository {
	endpointAvailable := eventEndpointAvailable(config.Endpoint, time.Duration(config.DialTimeout))
	if !endpointAvailable {
		log.Println("[WARNING] fluentbit host does not appear to be up, events will be dropped")
	}

	// Parse event mapping
	eventTagMap := make(map[string]string)
	for _, mapping := range config.Mapping {
		eventTagMap[mapping.Name] = mapping.Tag
	}

	return &TCPEventClientRepo{
		config:            config,
		endpointAvailable: endpointAvailable,
		eventTagMap:       eventTagMap,
	}
}

func eventEndpointAvailable(addr string, timeout time.Duration) bool {
	addr = strings.NewReplacer("http://", "", "https://", "").Replace(addr)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func (t *TCPEventClientRepo) createEventObject(eventName string, schemaVersion string, data interface{}) (cloudevents.Event, error) {
	objectId, err := common.GenerateObjectId()
	if err != nil {
		return cloudevents.Event{}, err

	}

	event := cloudevents.NewEvent()
	event.SetID(objectId)
	event.SetSource("beta9-cluster")
	event.SetType(eventName)
	event.SetSpecVersion(schemaVersion)
	event.SetTime(time.Now())
	event.SetData(cloudevents.ApplicationJSON, data)

	return event, nil
}

func (t *TCPEventClientRepo) pushEvent(eventName string, schemaVersion string, data interface{}) {
	if !t.endpointAvailable {
		return
	}

	event, err := t.createEventObject(eventName, schemaVersion, data)
	if err != nil {
		log.Println("failed to create event object:", err)
		return
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Println("failed to marshal event object:", err)
		return
	}

	var tag string
	tag, ok := t.eventTagMap[eventName]
	if !ok {
		tag = ""
	}

	resp, err := http.Post(t.config.Endpoint+"/"+tag, "application/json", bytes.NewBuffer(eventBytes))
	if err != nil {
		log.Println("failed to send payload to event server:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		return
	}

	log.Println("unexpected status code from event server:", resp.StatusCode)
}

func (t *TCPEventClientRepo) PushContainerRequestedEvent(request *types.ContainerRequest) {
	t.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerStatusRequestedSchemaVersion,
		types.EventContainerStatusRequestedSchema{
			ContainerID: request.ContainerId,
			Request:     *request,
			Status:      types.EventContainerLifecycleRequested,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerScheduledEvent(containerID string, workerID string, request *types.ContainerRequest) {
	t.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			Request:     *request,
			Status:      types.EventContainerLifecycleScheduled,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerStartedEvent(containerID string, workerID string, request *types.ContainerRequest) {
	t.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			Request:     *request,
			Status:      types.EventContainerLifecycleStarted,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerStoppedEvent(containerID string, workerID string, request *types.ContainerRequest) {
	t.pushEvent(
		types.EventContainerLifecycle,
		types.EventContainerLifecycleSchemaVersion,
		types.EventContainerLifecycleSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
			Request:     *request,
			Status:      types.EventContainerLifecycleStopped,
		},
	)
}

func (t *TCPEventClientRepo) PushWorkerStartedEvent(workerID string) {
	t.pushEvent(
		types.EventWorkerLifecycle,
		types.EventWorkerLifecycleSchemaVersion,
		types.EventWorkerLifecycleSchema{
			WorkerID: workerID,
			Status:   types.EventWorkerLifecycleStarted,
		},
	)
}

func (t *TCPEventClientRepo) PushWorkerStoppedEvent(workerID string) {
	t.pushEvent(
		types.EventWorkerLifecycle,
		types.EventWorkerLifecycleSchemaVersion,
		types.EventWorkerLifecycleSchema{
			WorkerID: workerID,
			Status:   types.EventWorkerLifecycleStopped,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerResourceMetricsEvent(workerID string, request *types.ContainerRequest, metrics types.EventContainerMetricsData) {
	t.pushEvent(
		types.EventContainerMetrics,
		types.EventContainerMetricsSchemaVersion,
		types.EventContainerMetricsSchema{
			WorkerID:         workerID,
			ContainerID:      request.ContainerId,
			WorkspaceID:      request.WorkspaceId,
			StubID:           request.StubId,
			ContainerMetrics: metrics,
		},
	)
}

func (t *TCPEventClientRepo) PushDeployStubEvent(workspaceId string, stub *types.Stub) {
	t.pushEvent(
		types.EventStubDeploy,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}

func (t *TCPEventClientRepo) PushServeStubEvent(workspaceId string, stub *types.Stub) {
	t.pushEvent(
		types.EventStubServe,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}

func (t *TCPEventClientRepo) PushRunStubEvent(workspaceId string, stub *types.Stub) {
	t.pushEvent(
		types.EventStubRun,
		types.EventStubSchemaVersion,
		types.EventStubSchema{
			ID:          stub.ExternalId,
			StubType:    stub.Type,
			StubConfig:  stub.Config,
			WorkspaceID: workspaceId,
		},
	)
}
