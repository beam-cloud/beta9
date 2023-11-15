package types

import "time"

const (
	EventNameContainerResourceStats = "CONTAINER_RESOURCE_STATS"
)

type Event struct {
	Name                        string
	Data                        []byte
	ApproximateArrivalTimestamp *time.Time
}

type EventSink = func(event []Event)

type EventClient interface {
	ListenToStream()
	AddSink(sinkId string, sink *EventSink) error
	RemoveSink(sinkId string) error
	PushEvent(event Event) error
}
