package repository

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type TCPEventClientRepo struct {
	config            types.FluentBitEventConfig
	endpointAvailable bool
}

func NewTCPEventClientRepo(config types.FluentBitEventConfig) EventRepository {
	endpointAvailable := eventEndpointAvailable(config.Endpoint, time.Duration(config.DialTimeout))
	if !endpointAvailable {
		log.Println("[WARNING] fluentbit host does not appear to be up, events will be dropped")
	}

	return &TCPEventClientRepo{
		config:            config,
		endpointAvailable: endpointAvailable,
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
	event.SetSource("beam-cloud")
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

	resp, err := http.Post(t.config.Endpoint, "application/json", bytes.NewBuffer(eventBytes))
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
		types.EventContainerRequested,
		types.EventContainerStatusRequestedSchemaVersion,
		types.EventContainerStatusRequestedSchema{
			ContainerID: request.ContainerId,
			Request:     *request,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerScheduledEvent(containerID string, workerID string) {
	t.pushEvent(
		types.EventContainerScheduled,
		types.EventContainerStatusSchemaVersion,
		types.EventContainerStatusSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerStartedEvent(containerID string, workerID string) {
	t.pushEvent(
		types.EventContainerStarted,
		types.EventContainerStatusSchemaVersion,
		types.EventContainerStatusSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
		},
	)
}

func (t *TCPEventClientRepo) PushContainerStoppedEvent(containerID string, workerID string) {
	t.pushEvent(
		types.EventContainerStopped,
		types.EventContainerStatusSchemaVersion,
		types.EventContainerStatusSchema{
			ContainerID: containerID,
			WorkerID:    workerID,
		},
	)
}

func (t *TCPEventClientRepo) PushWorkerStartedEvent(workerID string) {
	t.pushEvent(
		types.EventWorkerStarted,
		types.EventWorkerStatusSchemaVersion,
		types.EventWorkerStatusSchema{
			WorkerID: workerID,
		},
	)
}

func (t *TCPEventClientRepo) PushWorkerStoppedEvent(workerID string) {
	t.pushEvent(
		types.EventWorkerStopped,
		types.EventWorkerStatusSchemaVersion,
		types.EventWorkerStatusSchema{
			WorkerID: workerID,
		},
	)
}
