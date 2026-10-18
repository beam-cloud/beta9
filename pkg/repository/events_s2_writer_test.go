package repository

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"github.com/stretchr/testify/require"
)

func writerTestRecord(event cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error) {
	return s2.AppendRecord{Body: []byte(event.ID())}, []s2.StreamName{s2.StreamName(event.Subject())}, nil
}

func writerTestEvent(stream string, number int) cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetSubject(stream)
	event.SetID(strconv.Itoa(number))
	return event
}

func TestS2WriterSlowStreamDoesNotBlockUnrelatedMetrics(t *testing.T) {
	queue := make(chan cloudevents.Event, 32)
	blocked, release, fast := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeS2Events(queue, writerTestRecord, func(stream s2.StreamName, _ []s2.AppendRecord) error {
			if stream == "slow" {
				close(blocked)
				<-release
			} else {
				close(fast)
			}
			return nil
		}, func(err error, _ int) { t.Errorf("append failed: %v", err) })
	}()
	queue <- writerTestEvent("slow", 0)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("slow append never started")
	}
	queue <- writerTestEvent("metrics", 1)
	close(queue)
	select {
	case <-fast:
	case <-time.After(2 * time.Second):
		t.Fatal("a previous batch's slow stream blocked new VM metrics")
	}
	select {
	case <-done:
		t.Fatal("writer returned with an append still in flight")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("closed writer did not drain")
	}
}

func TestS2WriterBoundsPendingWorkAndPreservesStreamOrder(t *testing.T) {
	const streams, count = s2EventAppendConcurrency, 50000
	queue := make(chan cloudevents.Event)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var consumed, active, peak atomic.Int32
	started := make(chan struct{}, streams)
	var mu sync.Mutex
	next := make(map[s2.StreamName]int)
	inFlight := make(map[s2.StreamName]bool)
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeS2Events(queue, func(event cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error) {
			consumed.Add(1)
			return writerTestRecord(event)
		}, func(stream s2.StreamName, records []s2.AppendRecord) error {
			mu.Lock()
			if inFlight[stream] {
				t.Errorf("overlapping appends for %s", stream)
			}
			inFlight[stream] = true
			mu.Unlock()
			current := active.Add(1)
			for previous := peak.Load(); current > previous; previous = peak.Load() {
				if peak.CompareAndSwap(previous, current) {
					break
				}
			}
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			mu.Lock()
			for _, record := range records {
				number, err := strconv.Atoi(string(record.Body))
				if err != nil || number != next[stream] {
					t.Errorf("%s: got %s, expected %d", stream, record.Body, next[stream])
				}
				next[stream]++
			}
			inFlight[stream] = false
			mu.Unlock()
			active.Add(-1)
			return nil
		}, func(err error, _ int) { t.Errorf("append failed: %v", err) })
	}()
	go func() {
		defer close(queue)
		for i := 0; i < count; i++ {
			queue <- writerTestEvent(fmt.Sprintf("vm-%d", i%streams), i/streams)
		}
	}()
	for i := 0; i < streams; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("writer did not schedule independent streams")
		}
	}
	time.Sleep(50 * time.Millisecond)
	require.LessOrEqual(t, consumed.Load(), int32(s2EventQueueSize+s2EventAppendConcurrency*s2EventBatchSize))
	unblock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writer failed to drain the launch burst")
	}
	require.LessOrEqual(t, peak.Load(), int32(s2EventAppendConcurrency))
	total := 0
	for _, number := range next {
		total += number
	}
	require.Equal(t, count, total)
}
