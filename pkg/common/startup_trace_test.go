package common

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestStartupTraceCopiesSpanAttributes(t *testing.T) {
	trace := NewStartupTrace(true)
	ctx := ContextWithStartupTrace(context.Background(), trace)
	attributes := map[string]string{"tcp_local_port": "31337"}
	now := time.Now()
	RecordStartupSpanWithAttributes(ctx, "http", now, now, attributes)
	attributes["tcp_local_port"] = "44444"
	var out bytes.Buffer
	logger := zerolog.New(&out)
	trace.Log(logger.Info())
	require.Contains(t, out.String(), `"tcp_local_port":"31337"`)
	require.NotContains(t, out.String(), "44444")
}

func TestStartupTracePreservesConcurrentSpansAndIdentity(t *testing.T) {
	trace := NewStartupTrace(true)
	ctx := ContextWithStartupTrace(context.Background(), trace)
	trace.Identity("vm", "container")
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			end := StartupSpan(ctx, "parallel")
			end()
			end() // Error cleanup may close an already completed phase.
		}()
	}
	group.Wait()
	var buffer bytes.Buffer
	logger := zerolog.New(&buffer)
	trace.Log(logger.Info())
	var record struct {
		ContainerID string             `json:"container_id"`
		Spans       []startupTraceSpan `json:"startup_spans"`
	}
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &record))
	require.Equal(t, "container", record.ContainerID)
	require.Len(t, record.Spans, 100)
	for _, span := range record.Spans {
		require.GreaterOrEqual(t, span.EndNS, span.StartNS)
		require.GreaterOrEqual(t, span.DurationUS, int64(0))
	}
	require.Nil(t, NewStartupTrace(false))
	StartupSpan(context.Background(), "disabled")()
}
