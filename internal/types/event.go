package types

type Event struct {
	Id         string `json:"id"`
	Name       string `json:"name"`
	ApiVersion string `json:"api_version"`
	Created    int64  `json:"created"`
	Data       []byte `json:"data"`
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
)
