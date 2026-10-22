package repository

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"golang.org/x/sync/errgroup"
)

const (
	defaultS2EventStreamPrefix = "events"
	defaultS2EventReadLimit    = 10000
	s2EventHistoryReadLimit    = uint64(1000)
	s2StreamResumeDelay        = time.Second
	s2EventAggregateScanLimit  = 50000

	s2EventQueueSize         = 16384
	s2EventBatchSize         = 256
	s2EventAppendConcurrency = 16
	s2EventWriteTimeout      = 5 * time.Second
	s2EventEnqueueTimeout    = 250 * time.Millisecond
	s2EventFlushInterval     = 100 * time.Millisecond
	s2WriteWarnEvery         = time.Minute
)

// S2EventRepository reads and writes events and logs in one S2 basin.
type S2EventRepository struct {
	s2StreamLayout
	basin        *s2.BasinClient
	writer       *s2EventWriter
	stubCache    stubCacheContents
	historySkips historySkips
}

// ScopedS2EventRepository writes events with tokens scoped to stream
// prefixes, each stream through the first target whose prefix holds it.
type ScopedS2EventRepository struct {
	targets []scopedS2EventTarget
	writer  *s2EventWriter
}

type scopedS2EventTarget struct {
	name   string
	prefix string
	basin  *s2.BasinClient
}

// Share the HTTP/2-capable transport across event clients. The SDK default
// disables automatic HTTP/2 and retains only two idle connections, so fleet
// fanout continually opens new TLS connections instead of reusing them.
func newS2EventClient(token string) *s2.Client {
	return s2.New(token, &s2.ClientOptions{
		HTTPClient:     &http.Client{Transport: http.DefaultTransport, Timeout: s2EventWriteTimeout},
		RequestTimeout: s2EventWriteTimeout,
		RetryConfig: &s2.RetryConfig{
			MaxAttempts:       3,
			AppendRetryPolicy: s2.AppendRetryPolicyAll,
		},
	})
}

func NewS2EventRepository(config types.S2Config) (*S2EventRepository, error) {
	if config.ApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when s2 api key is configured")
	}

	repo := &S2EventRepository{
		s2StreamLayout: newS2StreamLayout(config.StreamPrefix),
		basin:          newS2EventClient(config.ApiKey).Basin(config.Basin),
	}
	repo.writer = newS2EventWriter("s2", repo.s2StreamLayout, func(s2.StreamName) *s2.BasinClient {
		return repo.basin
	})
	return repo, nil
}

func (r *S2EventRepository) PushEvent(event cloudevents.Event) error {
	return r.writer.push(event)
}

func (r *S2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.writer.write(event)
}

func NewScopedS2EventRepository(config types.S2Config) (*ScopedS2EventRepository, error) {
	if config.LogApiKey == "" && config.EventApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when scoped s2 api keys are configured")
	}

	repo := &ScopedS2EventRepository{}
	addTarget := func(name, token, prefix string) {
		token = strings.TrimSpace(token)
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if token == "" || prefix == "" {
			return
		}
		repo.targets = append(repo.targets, scopedS2EventTarget{
			name:   name,
			prefix: prefix,
			basin:  newS2EventClient(token).Basin(config.Basin),
		})
	}
	addTarget("logs", config.LogApiKey, config.LogStreamPrefix)
	addTarget("events", config.EventApiKey, config.EventStreamPrefix)
	if len(repo.targets) == 0 {
		return nil, nil
	}

	repo.writer = newS2EventWriter("scoped s2", newS2StreamLayout(config.StreamPrefix), repo.basinForStream)
	return repo, nil
}

func (r *ScopedS2EventRepository) PushEvent(event cloudevents.Event) error {
	return r.writer.push(event)
}

func (r *ScopedS2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.writer.write(event)
}

func (r *ScopedS2EventRepository) basinForStream(streamName s2.StreamName) *s2.BasinClient {
	if target := r.targetForStream(streamName); target != nil {
		return target.basin
	}
	return nil
}

func (r *ScopedS2EventRepository) targetForStream(streamName s2.StreamName) *scopedS2EventTarget {
	stream := strings.Trim(string(streamName), "/")
	for i := range r.targets {
		target := &r.targets[i]
		if stream == target.prefix || strings.HasPrefix(stream, target.prefix+"/") {
			return target
		}
	}
	return nil
}

// s2EventWriter appends events to the streams they route to, each through the
// basin basinFor returns for it. Streams without a basin are not written.
type s2EventWriter struct {
	name        string
	layout      s2StreamLayout
	basinFor    func(s2.StreamName) *s2.BasinClient
	queue       chan cloudevents.Event
	lastWarning atomic.Int64
}

// newS2EventWriter starts a writer draining its queue in the background.
func newS2EventWriter(name string, layout s2StreamLayout, basinFor func(s2.StreamName) *s2.BasinClient) *s2EventWriter {
	w := &s2EventWriter{
		name:     name,
		layout:   layout,
		basinFor: basinFor,
		queue:    make(chan cloudevents.Event, s2EventQueueSize),
	}
	go writeS2Events(w.queue, w.recordForEvent, w.append, w.warn)
	return w
}

// push queues an event, waiting briefly for room.
func (w *s2EventWriter) push(event cloudevents.Event) error {
	select {
	case w.queue <- event:
		return nil
	default:
	}

	timer := time.NewTimer(s2EventEnqueueTimeout)
	defer timer.Stop()

	select {
	case w.queue <- event:
		return nil
	case <-timer.C:
		return fmt.Errorf("%s event queue is full", w.name)
	}
}

// write appends events now instead of queueing them.
func (w *s2EventWriter) write(events ...cloudevents.Event) error {
	recordsByStream := map[s2.StreamName][]s2.AppendRecord{}
	for _, event := range events {
		record, streams, err := w.recordForEvent(event)
		if err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to build s2 event record")
			continue
		}
		for _, stream := range streams {
			recordsByStream[stream] = append(recordsByStream[stream], record)
		}
	}
	return appendS2EventStreams(recordsByStream, w.append)
}

func (w *s2EventWriter) recordForEvent(event cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error) {
	record, streams, err := w.layout.appendRecordForEvent(event)
	return record, slices.DeleteFunc(streams, func(stream s2.StreamName) bool {
		return w.basinFor(stream) == nil
	}), err
}

func (w *s2EventWriter) append(stream s2.StreamName, records []s2.AppendRecord) error {
	return appendS2RecordsForWrite(w.basinFor(stream), stream, records)
}

// warn logs a failed append at most once every s2WriteWarnEvery.
func (w *s2EventWriter) warn(err error, eventCount int) {
	now := time.Now()
	last := w.lastWarning.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < s2WriteWarnEvery {
		return
	}
	if !w.lastWarning.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	log.Warn().Err(err).Int("event_count", eventCount).Msgf("failed to append event batch to %s", w.name)
}

// Batches remain sequential, so each stream keeps its event order. Independent
// streams can append concurrently without a large fleet delaying telemetry.
func appendS2EventStreams(recordsByStream map[s2.StreamName][]s2.AppendRecord, appendRecords func(s2.StreamName, []s2.AppendRecord) error) error {
	var group errgroup.Group
	group.SetLimit(s2EventAppendConcurrency)
	for streamName, records := range recordsByStream {
		if len(records) == 0 {
			continue
		}
		group.Go(func() error { return appendS2EventStream(streamName, records, appendRecords) })
	}
	return group.Wait()
}

func appendS2EventStream(stream s2.StreamName, records []s2.AppendRecord, appendRecords func(s2.StreamName, []s2.AppendRecord) error) error {
	if err := appendRecords(stream, records); err != nil {
		if isS2EventStreamDeletionPending(err) {
			log.Debug().Err(err).Str("stream", string(stream)).Msg("dropping event batch for stream pending deletion")
			return nil
		}
		return fmt.Errorf("append %d events to s2 stream %q: %w", len(records), stream, err)
	}
	return nil
}

func appendS2RecordsForWrite(basin *s2.BasinClient, streamName s2.StreamName, records []s2.AppendRecord) error {
	appendRecords := func() error {
		ctx, cancel := s2EventWriteContext()
		defer cancel()
		_, err := basin.Stream(streamName).Append(ctx, &s2.AppendInput{Records: records})
		return err
	}

	err := appendRecords()
	if !isS2StreamNotFound(err) {
		return err
	}

	ensureCtx, ensureCancel := s2EventWriteContext()
	defer ensureCancel()
	if _, ensureErr := basin.Streams.Ensure(ensureCtx, s2.EnsureStreamArgs{Stream: streamName}); ensureErr != nil {
		return fmt.Errorf("ensure missing s2 stream %q: %w; append: %v", streamName, ensureErr, err)
	}

	return appendRecords()
}

func s2EventWriteContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s2EventWriteTimeout)
}

func isS2EventStreamDeletionPending(err error) bool {
	var s2Err *s2.S2Error
	return errors.As(err, &s2Err) && s2Err.Code == "stream_deletion_pending"
}

type s2EventWriteResult struct {
	stream  s2.StreamName
	records int
	err     error
}

// A slow stream must not hold the next batch of unrelated VM metrics or logs.
// Keep one append in flight per stream, with bounded pending records and total
// concurrency. Records arriving during an append join that stream's next batch.
func writeS2Events(
	queue <-chan cloudevents.Event,
	recordForEvent func(cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error),
	appendRecords func(s2.StreamName, []s2.AppendRecord) error,
	warn func(error, int),
) {
	ticker := time.NewTicker(s2EventFlushInterval)
	defer ticker.Stop()

	pending := map[s2.StreamName][]s2.AppendRecord{}
	active := map[s2.StreamName]bool{}
	ready := []s2.StreamName{}
	completed := make(chan s2EventWriteResult, s2EventAppendConcurrency)
	pendingCount := 0
	flushing := false

	for queue != nil || len(pending) > 0 || len(active) > 0 {
		if queue == nil {
			flushing = true
		}
		for flushing && len(ready) > 0 && len(active) < s2EventAppendConcurrency {
			stream := ready[0]
			ready = ready[1:]
			records := pending[stream]
			n := min(len(records), s2EventBatchSize)
			batch := records[:n:n]
			if n == len(records) {
				delete(pending, stream)
			} else {
				pending[stream] = records[n:]
			}
			pendingCount -= n
			active[stream] = true
			go func() {
				completed <- s2EventWriteResult{stream: stream, records: n, err: appendS2EventStream(stream, batch, appendRecords)}
			}()
		}
		if len(ready) == 0 {
			flushing = false
		}

		input := queue
		if pendingCount >= s2EventQueueSize {
			input = nil
		}
		select {
		case event, ok := <-input:
			if !ok {
				queue = nil
				continue
			}
			record, streams, err := recordForEvent(event)
			if err != nil {
				log.Debug().Err(err).Str("event_type", event.Type()).Msg("failed to build s2 event record")
				continue
			}
			for _, stream := range streams {
				if len(pending[stream]) == 0 && !active[stream] {
					ready = append(ready, stream)
				}
				pending[stream] = append(pending[stream], record)
				pendingCount++
			}
			if pendingCount >= s2EventBatchSize {
				flushing = true
			}
		case result := <-completed:
			delete(active, result.stream)
			if result.err != nil {
				warn(result.err, result.records)
			}
			if len(pending[result.stream]) > 0 {
				ready = append(ready, result.stream)
			}
		case <-ticker.C:
			flushing = true
		}
	}
}
