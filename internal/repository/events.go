package repository

import (
	"encoding/json"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type packet struct {
	data    []byte
	retries int
}

type TCPEventClientRepo struct {
	conn        net.Conn
	address     string
	mu          sync.Mutex
	config      types.FluentBitConfig
	eventBuffer chan packet
}

func NewTCPEventClientRepo(config types.FluentBitConfig) EventRepository {
	address := config.Events.Host + ":" + strconv.Itoa(config.Events.Port)

	client := &TCPEventClientRepo{
		conn:        nil,
		address:     address,
		config:      config,
		eventBuffer: make(chan packet, config.Events.BufferSize),
	}

	go client.keepConnectionAlive()

	return client
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

func (t *TCPEventClientRepo) connect() error {
	conn, err := net.Dial("tcp", t.address)
	if err != nil {
		return err
	}

	conn.SetDeadline(time.Time{}) // Prevent timing out
	t.mu.Lock()
	t.conn = conn
	t.mu.Unlock()
	return nil
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

	err = t.send(eventBytes)
	if err == nil {
		return nil
	}

	t.eventBuffer <- packet{data: eventBytes, retries: 0}

	return nil
}

func (t *TCPEventClientRepo) send(packet []byte) error {
	_, err := t.conn.Write(packet)
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

func (t *TCPEventClientRepo) keepConnectionAlive() {
	ticker := time.NewTicker(30 * time.Second) // Set to retry connection every 30 seconds if connection is lost
	defer ticker.Stop()

	// Initial connection request since the ticker will not trigger immediately
	t.connect()

	for range ticker.C {
		if t.conn != nil {
			continue
		}

		t.connect()
	}
}

func (t *TCPEventClientRepo) pushBufferedEvents() {
	for {
		if t.conn == nil {
			time.Sleep(1 * time.Second)
		}

		packet := <-t.eventBuffer
		err := t.send(packet.data)
		if err == nil {
			continue
		}

		packet.retries++
		if packet.retries < t.config.Events.MaxEventRetries {
			t.eventBuffer <- packet
		}
	}
}
