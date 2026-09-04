package apiv1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// sseKeepaliveInterval bounds how long an idle stream goes without bytes on
// the wire. Proxies and browsers otherwise cannot tell a quiet stream from a
// dead socket, and the client only reconnects on a socket it knows is dead.
// The keepalive is a named event rather than an SSE comment because browser
// parsers drop comments, and the client watchdog needs to observe it.
const sseKeepaliveInterval = 15 * time.Second

const sseKeepaliveEvent = "ping"

// sseEncoder turns a stream record into an SSE event. Returning ok=false skips
// the record.
type sseEncoder func(record types.ContainerEventRecord) (name string, payload []byte, ok bool)

// streamSSE relays an event stream to the client as server-sent events. Each
// event carries the record's sequence number as its id, so a client resumes
// from where it dropped with Last-Event-ID. Records are read on a separate
// goroutine so keepalives and client disconnects are handled while the
// stream is waiting on the store; the deferred stream.Close in the caller
// unblocks that reader.
func streamSSE(ctx echo.Context, stream repository.EventStream, encode sseEncoder) error {
	response := ctx.Response()
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)

	flusher, _ := response.Writer.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}
	if _, err := fmt.Fprint(response.Writer, ": connected\n\n"); err != nil {
		return nil
	}
	flush()

	done := ctx.Request().Context().Done()
	records := make(chan types.ContainerEventRecord)
	go func() {
		defer close(records)
		for stream.Next() {
			select {
			case records <- stream.Record():
			case <-done:
				return
			}
		}
	}()

	keepalive := time.NewTicker(sseKeepaliveInterval)
	defer keepalive.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-keepalive.C:
			if err := writeSSEEvent(response.Writer, sseKeepaliveEvent, "", nil); err != nil {
				return nil
			}
			flush()
		case record, more := <-records:
			if !more {
				if err := stream.Err(); err != nil && ctx.Request().Context().Err() == nil {
					payload, _ := json.Marshal(map[string]string{"error": err.Error()})
					_ = writeSSEEvent(response.Writer, "error", "", payload)
					flush()
				}
				return nil
			}
			name, payload, ok := encode(record)
			if !ok {
				continue
			}
			if err := writeSSEEvent(response.Writer, name, strconv.FormatUint(record.SeqNum, 10), payload); err != nil {
				return nil
			}
			flush()
		}
	}
}

// encodeEventRecord serializes the record as-is under its own type name.
func encodeEventRecord(fallbackName string) sseEncoder {
	return func(record types.ContainerEventRecord) (string, []byte, bool) {
		payload, err := json.Marshal(record)
		if err != nil {
			log.Debug().Err(err).Uint64("seq_num", record.SeqNum).Msg("skipping unencodable event record")
			return "", nil, false
		}
		name := record.Type
		if name == "" {
			name = fallbackName
		}
		return name, payload, true
	}
}

func writeSSEEvent(w http.ResponseWriter, eventName string, id string, data []byte) error {
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if eventName != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", eventName); err != nil {
			return err
		}
	}
	for _, line := range strings.Split(string(data), "\n") {
		if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(w, "\n")
	return err
}
