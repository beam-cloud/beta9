package repository

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type TCPEventClientRepo struct {
	conn net.Conn
}

func NewTCPEventClientRepo(config types.FluentBitConfig) (EventRepository, error) {
	address := config.Events.Host + ":" + strconv.Itoa(config.Events.Port)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, errors.New("failed to connect to fluent-bit server %s: %s" + address + err.Error())
	}

	return &TCPEventClientRepo{
		conn: conn,
	}, nil
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

func (t *TCPEventClientRepo) pushEvent(eventName string, schemaVersion string, data interface{}) error {
	if t.conn == nil {
		return nil
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	event, err := t.createEventObject(eventName, schemaVersion, dataBytes)
	if err != nil {
		return err
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = t.conn.Write(eventBytes)
	if err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	_, err = t.conn.Read(buffer)
	if err != nil {
		return err
	}

	return nil
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
