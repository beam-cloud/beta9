package types

import cloudevents "github.com/cloudevents/sdk-go/v2/event"

type EventSink = func(event []cloudevents.Event)

type EventClient interface {
	PushEvent(event cloudevents.Event) error
}

var (
	EventContainerLifecycle = "container.lifecycle"
	EventWorkerLifecycle    = "worker.lifecycle"
)

var (
	EventContainerLifecycleRequested = "requested"
	EventContainerLifecycleScheduled = "scheduled"
	EventContainerLifecycleStarted   = "started"
	EventContainerLifecycleStopped   = "stopped"
	EventContainerLifecycleFailed    = "failed"
)

var (
	EventWorkerLifecycleStarted = "started"
	EventWorkerLifecycleStopped = "stopped"
)

// Schema versions should be in ISO 8601 format

var EventContainerLifecycleSchemaVersion = "1.0"

type EventContainerLifecycleSchema struct {
	ContainerID string `json:"container_id"`
	WorkerID    string `json:"worker_id"`
	Status      string `json:"status"`
}

var EventContainerStatusRequestedSchemaVersion = "1.0"

type EventContainerStatusRequestedSchema struct {
	ContainerID string           `json:"container_id"`
	Request     ContainerRequest `json:"request"`
	Status      string           `json:"status"`
}

var EventWorkerLifecycleSchemaVersion = "1.0"

type EventWorkerLifecycleSchema struct {
	WorkerID string `json:"worker_id"`
	Status   string `json:"status"`
}
