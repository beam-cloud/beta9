package repository

import (
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

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
