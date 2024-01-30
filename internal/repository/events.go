package repository

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type TCPEventClientRepo struct {
	conn    net.Conn
	address string
	mu      sync.Mutex
}

func NewTCPEventClientRepo(config types.FluentBitConfig) EventRepository {
	address := config.Events.Host + ":" + strconv.Itoa(config.Events.Port)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("failed to connect to fluent-bit server %s: %v\n", address, err.Error())
	} else {
		conn.SetDeadline(time.Time{}) // Prevent timing out
	}

	client := &TCPEventClientRepo{
		conn:    conn,
		address: address,
	}

	go client.KeepConnectionAlive()

	return client
}

func (t *TCPEventClientRepo) KeepConnectionAlive() {
	ticker := time.NewTicker(30 * time.Second) // Set to retry connection every 30 seconds if connection is lost
	defer ticker.Stop()

	for range ticker.C {
		if t.conn != nil {
			continue
		}

		conn, err := net.Dial("tcp", t.address)
		if err != nil {
			log.Printf("failed to connect to event server %s: %v\n", t.address, err.Error())
		} else {
			conn.SetDeadline(time.Time{}) // Prevent timing out
			t.mu.Lock()
			t.conn = conn
			t.mu.Unlock()
		}
	}
}

func (t *TCPEventClientRepo) Close() {
	if t.conn != nil {
		t.conn.Close()
	}
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
	if err == io.EOF {
		// Connection closed
		t.mu.Lock()
		t.conn.Close()
		t.conn = nil
		t.mu.Unlock()
		return nil
	}

	if err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	_, err = t.conn.Read(buffer)
	if err == io.EOF {
		// Connection closed
		t.mu.Lock()
		t.conn.Close()
		t.conn = nil
		t.mu.Unlock()
		return nil
	}

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
