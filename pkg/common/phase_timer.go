package common

import (
	"time"

	"github.com/rs/zerolog"
)

// PhaseTimer records how long each phase of a multi-step operation took, so a
// slow attach or shutdown can be blamed on one phase from its log line alone.
//
// A nil timer is inert: callers create one only when debug instrumentation is
// on and use it unconditionally, so production paths carry no bookkeeping.
type PhaseTimer struct {
	last   time.Time
	phases []phase
}

type phase struct {
	name string
	took time.Duration
}

func NewPhaseTimer() *PhaseTimer {
	return &PhaseTimer{last: time.Now()}
}

// Mark closes the phase that began at the previous Mark (or at creation) and
// records it under name.
func (t *PhaseTimer) Mark(name string) {
	if t == nil {
		return
	}
	now := time.Now()
	t.phases = append(t.phases, phase{name: name, took: now.Sub(t.last)})
	t.last = now
}

// Fields adds one duration field per recorded phase to a log event.
func (t *PhaseTimer) Fields(event *zerolog.Event) *zerolog.Event {
	if t == nil {
		return event
	}
	for _, p := range t.phases {
		event = event.Dur(p.name, p.took)
	}
	return event
}
