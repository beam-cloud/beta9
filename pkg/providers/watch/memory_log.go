package watch

import (
	"context"
	"slices"
	"sync"
	"time"
)

// MemoryLog is a Log that exists only in memory. Tests write to it, and read
// what a sampler wrote.
type MemoryLog struct {
	mu      sync.Mutex
	streams map[string][]Record
	kept    map[string]time.Duration
	// Fail, when set, is the error of every call.
	Fail error
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{streams: map[string][]Record{}, kept: map[string]time.Duration{}}
}

func (l *MemoryLog) Append(ctx context.Context, stream Stream, records ...Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Fail != nil {
		return l.Fail
	}
	l.streams[stream.Name] = append(l.streams[stream.Name], records...)
	l.kept[stream.Name] = stream.Keep
	return nil
}

func (l *MemoryLog) Read(ctx context.Context, stream Stream, from, to time.Time) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Fail != nil {
		return nil, l.Fail
	}
	var records []Record
	for _, record := range l.streams[stream.Name] {
		if !record.At.Before(from) && record.At.Before(to) {
			records = append(records, record)
		}
	}
	return records, nil
}

// Names lists the streams that have been written to, in order.
func (l *MemoryLog) Names() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.streams))
	for name := range l.streams {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// All returns every record of a stream, in the order it was written.
func (l *MemoryLog) All(name string) []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.streams[name])
}

// Kept is how long the stream was last said to keep its records.
func (l *MemoryLog) Kept(name string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.kept[name]
}
