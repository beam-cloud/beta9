package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/market"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	// s2MarketStreams is where the market's streams are, under the prefix
	// the events are under.
	s2MarketStreams = "market"

	// s2MaxRecordBytes is the most S2 takes in one record.
	s2MaxRecordBytes = 1 << 20
	// s2MaxAppendRecords is the most records S2 takes in one append.
	s2MaxAppendRecords = 1000
)

// S2MarketLog keeps the market's history in S2: what the providers sold,
// sampled at an interval, in the streams that pkg/market names.
//
// A stream is made before it is first written to, keeping its records for
// as long as the stream says. Left to be made by its first append, it would
// keep them for the basin's default, which is a week.
type S2MarketLog struct {
	basin  *s2.BasinClient
	prefix string

	// made are the streams this process has made, or found made, as they
	// should be.
	mu   sync.Mutex
	made map[s2.StreamName]time.Duration
}

// NewS2MarketLog returns the market's history in the basin the events are
// kept in, or nil when S2 is not configured.
func NewS2MarketLog(config types.S2Config) (*S2MarketLog, error) {
	if config.ApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when s2 api key is configured")
	}
	return newS2MarketLog(newS2EventClient(config.ApiKey).Basin(config.Basin), config.StreamPrefix), nil
}

func newS2MarketLog(basin *s2.BasinClient, prefix string) *S2MarketLog {
	return &S2MarketLog{
		basin:  basin,
		prefix: newS2StreamLayout(prefix).streamPrefix + "/" + s2MarketStreams + "/",
		made:   map[s2.StreamName]time.Duration{},
	}
}

func (l *S2MarketLog) name(stream market.Stream) s2.StreamName {
	return s2.StreamName(l.prefix + stream.Name)
}

func (l *S2MarketLog) Append(ctx context.Context, stream market.Stream, records ...market.Record) error {
	if len(records) == 0 {
		return nil
	}
	name := l.name(stream)
	batch := make([]s2.AppendRecord, 0, len(records))
	for _, record := range records {
		if len(record.Body) > s2MaxRecordBytes {
			return fmt.Errorf("a record of %d bytes is more than s2 stream %q takes", len(record.Body), name)
		}
		// A record is of the time it says, not of when it arrives: the
		// stream is read by time.
		at := uint64(record.At.UnixMilli())
		batch = append(batch, s2.AppendRecord{Timestamp: &at, Body: record.Body})
	}

	ctx, cancel := context.WithTimeout(ctx, 2*s2EventWriteTimeout)
	defer cancel()
	if err := l.ensure(ctx, name, stream.Keep); err != nil {
		return err
	}
	err := l.append(ctx, name, batch)
	if isS2StreamNotFound(err) {
		// It was deleted since this process made it.
		l.forget(name)
		if err := l.ensure(ctx, name, stream.Keep); err != nil {
			return err
		}
		err = l.append(ctx, name, batch)
	}
	if err != nil {
		return fmt.Errorf("append %d records to s2 stream %q: %w", len(batch), name, err)
	}
	return nil
}

// append writes the records in order, as many at a time as S2 takes in
// bytes and in number.
func (l *S2MarketLog) append(ctx context.Context, name s2.StreamName, records []s2.AppendRecord) error {
	for len(records) > 0 {
		n, size := 0, 0
		for n < len(records) && n < s2MaxAppendRecords {
			// What S2 meters of a record is a little more than its body.
			metered := len(records[n].Body) + 64
			if n > 0 && size+metered > s2MaxRecordBytes {
				break
			}
			size += metered
			n++
		}
		if _, err := l.basin.Stream(name).Append(ctx, &s2.AppendInput{Records: records[:n]}); err != nil {
			return err
		}
		records = records[n:]
	}
	return nil
}

// ensure makes the stream, keeping its records for keep, or for good when
// keep is zero. A stream that exists and keeps them for another time is
// changed to keep them for this one.
func (l *S2MarketLog) ensure(ctx context.Context, name s2.StreamName, keep time.Duration) error {
	l.mu.Lock()
	kept, made := l.made[name]
	l.mu.Unlock()
	if made && kept == keep {
		return nil
	}

	retention := &s2.RetentionPolicy{Infinite: &s2.InfiniteRetention{}}
	if keep > 0 {
		retention = &s2.RetentionPolicy{Age: s2.Int64(int64(keep.Seconds()))}
	}
	_, err := l.basin.Streams.Ensure(ctx, s2.EnsureStreamArgs{Stream: name, Config: &s2.StreamConfig{
		RetentionPolicy: retention,
		// The time of a record is the one it is written with, whatever the
		// basin's streams do by default.
		Timestamping: &s2.TimestampingConfig{Mode: s2.Ptr(s2.TimestampingModeClientPrefer)},
	}})
	if err != nil {
		return fmt.Errorf("make s2 stream %q: %w", name, err)
	}
	l.mu.Lock()
	l.made[name] = keep
	l.mu.Unlock()
	return nil
}

func (l *S2MarketLog) forget(name s2.StreamName) {
	l.mu.Lock()
	delete(l.made, name)
	l.mu.Unlock()
}

func (l *S2MarketLog) Read(ctx context.Context, stream market.Stream, from, to time.Time) ([]market.Record, error) {
	name := l.name(stream)
	start, end := uint64(max(from.UnixMilli(), 0)), uint64(max(to.UnixMilli(), 0))
	if end <= start {
		return nil, nil
	}

	var records []market.Record
	// The first read finds its place by time. The rest go on from where the
	// one before them stopped: a read returns no more than S2 gives at once.
	opts := &s2.ReadOptions{Timestamp: &start, Until: &end}
	for {
		batch, err := l.basin.Stream(name).Read(ctx, opts)
		if isS2ReadEmpty(err) {
			// The stream has nothing from here on, or is yet to be made.
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read s2 stream %q: %w", name, err)
		}
		if len(batch.Records) == 0 {
			return records, nil
		}
		for _, record := range batch.Records {
			if record.Timestamp >= end {
				return records, nil
			}
			records = append(records, market.Record{At: time.UnixMilli(int64(record.Timestamp)).UTC(), Body: record.Body})
		}
		next := batch.Records[len(batch.Records)-1].SeqNum + 1
		opts = &s2.ReadOptions{SeqNum: &next, Until: &end}
	}
}
