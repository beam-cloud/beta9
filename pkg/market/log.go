package market

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Log is the streams the market's history is kept in. Each is added to at
// its end and read by time.
type Log interface {
	// Append adds records to the end of a stream. A stream that does not
	// exist yet is made, keeping its records for as long as the stream says.
	Append(ctx context.Context, stream Stream, records ...Record) error

	// Read returns the records of a stream that are of a time from from
	// until to, the oldest first. A stream that does not exist has none.
	Read(ctx context.Context, stream Stream, from, to time.Time) ([]Record, error)
}

// Record is one entry of a stream: what was written, and the time it is of.
type Record struct {
	At   time.Time
	Body []byte
}

// Stream names a stream, under the prefix its log keeps the market in, and
// says how long its records are kept.
type Stream struct {
	Name string
	// Keep is how long a record is kept. Zero means for good.
	Keep time.Duration
}

// Tier is how far apart the entries of a GPU's series are.
type Tier string

const (
	// TierMinute is the samples as they were taken.
	TierMinute Tier = "1m"
	// TierHour is what the samples of each hour come to.
	TierHour Tier = "1h"
)

// Streams names the market's streams, and says how long each is kept.
type Streams struct {
	// SnapshotsFor is how long every offer of every sample is kept, and
	// SamplesFor how long the samples of a GPU's series are. What an hour of
	// samples comes to, and what became of the machines bought, are kept for
	// good.
	SnapshotsFor time.Duration
	SamplesFor   time.Duration
}

// Snapshots is the stream of everything one provider said.
func (s Streams) Snapshots(provider string) Stream {
	return Stream{Name: "snapshots/" + slug(provider), Keep: s.SnapshotsFor}
}

// Series is the stream a GPU type's chart is drawn from.
func (s Streams) Series(gpu string, tier Tier) Stream {
	stream := Stream{Name: "series/" + slug(gpu) + "/" + string(tier)}
	if tier == TierMinute {
		stream.Keep = s.SamplesFor
	}
	return stream
}

// Outcomes is the stream of what became of the machines that were bought.
func (s Streams) Outcomes() Stream {
	return Stream{Name: "outcomes"}
}

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
