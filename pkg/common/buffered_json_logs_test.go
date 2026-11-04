package common

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type blockedLogSink struct {
	bytes.Buffer
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *blockedLogSink) Write(data []byte) (int, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.Buffer.Write(data)
}

func TestBufferedJSONLogsApplyBoundedBackpressureWithoutDropping(t *testing.T) {
	sink := &blockedLogSink{started: make(chan struct{}), release: make(chan struct{})}
	w := NewBufferedJSONLogWriter(sink, 32, 2)
	_, err := w.Write([]byte("A"))
	require.NoError(t, err)
	<-sink.started
	_, err = w.Write([]byte(strings.Repeat("B", 32)))
	require.NoError(t, err, "an in-flight stdout write must not hold the producers' lock")
	blocked := make(chan error, 1)
	go func() { _, err := w.Write([]byte("C")); blocked <- err }()
	select {
	case <-blocked:
		t.Fatal("full queue did not apply backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	close(sink.release)
	require.NoError(t, <-blocked)
	require.NoError(t, w.Close())
	require.Equal(t, "A"+strings.Repeat("B", 32)+"C", sink.String())
}

func TestBufferedJSONLogsKeepConcurrentAndOversizedRecordsExact(t *testing.T) {
	var sink bytes.Buffer
	w := NewBufferedJSONLogWriter(&sink, 1024, 16)
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for producer := 0; producer < 16; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for seq := 0; seq < 40; seq++ {
				line := fmt.Sprintf("%02d:%02d:%s\n", producer, seq, strings.Repeat("x", 2048))
				if n, err := w.Write([]byte(line)); err != nil || n != len(line) {
					errors <- fmt.Errorf("record write: %d %v", n, err)
					return
				}
			}
		}(producer)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())
	stats := w.Stats()
	require.LessOrEqual(t, stats["peak_queued_bytes"], 1024)
	require.LessOrEqual(t, stats["peak_queued_chunks"], 16)
	require.Equal(t, stats["accepted_chunks"], stats["flushed_chunks"])
	require.Equal(t, false, stats["sink_error"])
	lines := strings.Split(strings.TrimSuffix(sink.String(), "\n"), "\n")
	require.Len(t, lines, 640)
	next := make([]int, 16)
	for _, line := range lines {
		var producer, seq int
		_, err := fmt.Sscanf(line[:5], "%02d:%02d", &producer, &seq)
		require.NoError(t, err)
		require.Equal(t, next[producer], seq, "per-producer ordering must survive batching")
		next[producer]++
		require.Equal(t, fmt.Sprintf("%02d:%02d:%s", producer, seq, strings.Repeat("x", 2048)), line)
	}
}

type failedLogSink struct{ short bool }

func (s failedLogSink) Write(data []byte) (int, error) {
	if s.short {
		return len(data) - 1, nil
	}
	return 0, io.ErrClosedPipe
}

func TestBufferedJSONLogsSurfaceSinkErrorsAndFlushFatal(t *testing.T) {
	for _, short := range []bool{false, true} {
		w := NewBufferedJSONLogWriter(failedLogSink{short: short}, 1024, 16)
		_, err := w.Write([]byte("record\n"))
		require.NoError(t, err)
		want := io.ErrClosedPipe
		if short {
			want = io.ErrShortWrite
		}
		require.ErrorIs(t, w.Flush(), want)
		require.ErrorIs(t, w.Close(), want)
		_, err = w.Write([]byte("later\n"))
		require.ErrorIs(t, err, want)
	}
	sink := &blockedLogSink{started: make(chan struct{}), release: make(chan struct{})}
	w := NewBufferedJSONLogWriter(sink, 1024, 16)
	_, err := w.WriteLevel(zerolog.InfoLevel, []byte("info\n"))
	require.NoError(t, err)
	<-sink.started
	fatal := make(chan error, 1)
	go func() { _, err := w.WriteLevel(zerolog.FatalLevel, []byte("fatal\n")); fatal <- err }()
	select {
	case <-fatal:
		t.Fatal("fatal log returned before its record reached the sink")
	case <-time.After(20 * time.Millisecond):
	}
	close(sink.release)
	require.NoError(t, <-fatal)
	require.NoError(t, w.Close())
	require.Equal(t, "info\nfatal\n", sink.String())
}
