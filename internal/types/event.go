package types

type Event struct {
	Id            string `json:"id"`
	Name          string `json:"name"`
	SchemaVersion string `json:"schema_version"`
	Created       int64  `json:"created"`
	Data          []byte `json:"data"`
}

type EventSink = func(event []Event)

type EventClient interface {
	PushEvent(event Event) error
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

var EventContainerStatusRequestedSchemaVersion = "2024-01-24"

type EventContainerStatusRequestedSchema struct {
	ContainerID string           `json:"container_id"`
	Request     ContainerRequest `json:"request"`
}

var EventWorkerStatusSchemaVersion = "2024-01-24"

type EventWorkerStatusSchema struct {
	WorkerID string `json:"worker_id"`
}
