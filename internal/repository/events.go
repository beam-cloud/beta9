package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type TCPEventClientRepo struct {
	config            types.FluentBitEventConfig
	http              *http.Client
	endpointAvailable bool
}

func NewTCPEventClientRepo(config types.FluentBitEventConfig) EventRepository {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: config.MaxConns,
			MaxIdleConns:    config.MaxIdleConns,
			IdleConnTimeout: config.IdleConnTimeout,
			DialContext: (&net.Dialer{
				Timeout:   config.DialTimeout,
				KeepAlive: config.KeepAlive,
			}).DialContext,
		},
	}

	endpointAvailable := eventEndpointAvailable(context.TODO(), config.Endpoint, time.Duration(config.DialTimeout))
	if !endpointAvailable {
		log.Println("[WARNING] fluentbit host does not appear to be up, events will be dropped")
	}

	return &TCPEventClientRepo{
		config:            config,
		http:              httpClient,
		endpointAvailable: endpointAvailable,
	}
}

func eventEndpointAvailable(ctx context.Context, addr string, timeout time.Duration) bool {
	addr = strings.NewReplacer("http://", "", "https://", "").Replace(addr)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func (t *TCPEventClientRepo) createEventObject(eventName string, schemaVersion string, data []byte) (types.Event, error) {
	objectId, err := common.GenerateObjectId()
	if err != nil {
		return types.Event{}, err
	}

	return types.Event{
		Id:            objectId,
		Name:          eventName,
		Created:       time.Now().Unix(),
		SchemaVersion: schemaVersion,
		Data:          data,
	}, nil
}

func (t *TCPEventClientRepo) pushEvent(eventName string, schemaVersion string, data interface{}) {
	if !t.endpointAvailable {
		return
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Println("failed to marshal event:", err)
		return
	}

	event, err := t.createEventObject(eventName, schemaVersion, dataBytes)
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
