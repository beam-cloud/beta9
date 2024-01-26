package repository

import (
	"encoding/json"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type TCPEventClientRepo struct {
	address string
}

func NewTCPEventClientRepo(config *types.AppConfig) EventRepository {
	return &TCPEventClientRepo{
		address: config.FluentBit.Events.Host + ":" + strconv.Itoa(config.FluentBit.Events.Port),
	}
}

func (t *TCPEventClientRepo) createEventObject(eventName string, schemaVersion string, data []byte) types.Event {
	return types.Event{
		Id:            common.GenerateObjectId(),
		Name:          eventName,
		Created:       time.Now().Unix(),
		SchemaVersion: schemaVersion,
		Data:          data,
	}
}

func (t *TCPEventClientRepo) pushEvent(eventName string, schemaVersion string, data interface{}) error {
	conn, err := net.Dial("tcp", t.address)
	if err != nil {
		log.Println(err)
		return err
	}
	defer conn.Close()

	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Println(err)
		return err
	}

	eventBytes, err := json.Marshal(
		t.createEventObject(eventName, schemaVersion, dataBytes),
	)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = conn.Write(eventBytes)
	if err != nil {
		log.Println(err)
		return err
	}

	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		log.Println(err)
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
