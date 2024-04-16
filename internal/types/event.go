package types

import cloudevents "github.com/cloudevents/sdk-go/v2/event"

type EventSink = func(event []cloudevents.Event)

type EventClient interface {
	PushEvent(event cloudevents.Event) error
}

var (
	EventContainerScheduled = "container.scheduled"
	EventContainerRequested = "container.requested"
	EventContainerStarted   = "container.started"
	EventContainerStopped   = "container.stopped"

	EventWorkerStarted = "worker.started"
	EventWorkerStopped = "worker.stopped"
)

// Schema versions should be in ISO 8601 format

var EventContainerStatusSchemaVersion = "2024-01-24"

type EventContainerStatusSchema struct {
	ContainerID string `json:"container_id"`
	WorkerID    string `json:"worker_id"`
}

var EventContainerStatusRequestedSchemaVersion = "1.0"

type EventContainerStatusRequestedSchema struct {
	ContainerID string           `json:"container_id"`
	Request     ContainerRequest `json:"request"`
}

var EventWorkerStatusSchemaVersion = "1.0"

type EventWorkerStatusSchema struct {
	WorkerID string `json:"worker_id"`
}
