package managedendpoint

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

const streamKeepaliveInterval = 10 * time.Second

// relayStream keeps one writer for both engine events and heartbeat comments.
// Comments keep a silent reasoning step connected without counting as output or
// completing billing. Accounting must succeed before the terminal marker leaves.
func relayStream(w *echo.Response, body io.Reader, requestID string, sentAt time.Time, finalize func(Usage, time.Duration) error) (Usage, time.Duration, error) {
	return relayStreamWithKeepalive(w, body, requestID, sentAt, finalize, streamKeepaliveInterval)
}

func relayStreamWithKeepalive(w *echo.Response, body io.Reader, requestID string, sentAt time.Time, finalize func(Usage, time.Duration) error, interval time.Duration) (Usage, time.Duration, error) {
	type eventRead struct {
		event []byte
		err   error
	}
	reads := make(chan eventRead, 1)
	stop := make(chan struct{})
	defer func() {
		close(stop)
		// In production this is an HTTP response body: Close releases a reader
		// blocked on the next event after a write failure or terminal marker.
		if closer, ok := body.(io.Closer); ok {
			_ = closer.Close()
		}
	}()
	go func() {
		reader := bufio.NewReaderSize(body, 64<<10)
		for {
			event, err := readSSEEvent(reader, maxBody)
			select {
			case reads <- eventRead{event, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	write := func(event []byte) error {
		if _, err := w.Write(event); err != nil {
			return err
		}
		if err := http.NewResponseController(w.Writer).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		return nil
	}
	const keepalive = ": keepalive\n\n"
	var usage Usage
	var ttft time.Duration
	toolChoices := make(map[int]bool)
	// Flush the HTTP response immediately, even if the engine has sent only
	// headers. No generated-token metric is inferred from this comment.
	if err := write([]byte(keepalive)); err != nil {
		return usage, ttft, err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := write([]byte(keepalive)); err != nil {
				return usage, ttft, err
			}
		case result := <-reads:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) {
					return usage, ttft, io.ErrUnexpectedEOF
				}
				return usage, ttft, result.err
			}
			payload, data := sseEventData(result.event)
			if data && len(bytes.TrimSpace(payload)) > 0 {
				if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
					if finalize != nil {
						if err := finalize(usage, ttft); err != nil {
							return usage, ttft, err
						}
					}
					return usage, ttft, write(result.event)
				}
				if err := streamPayloadError(payload); err != nil {
					// The proxy emits one canonical terminal error chunk. Do not
					// forward an engine error and then emit a second gateway error.
					return usage, ttft, err
				}
				line := append([]byte("data: "), payload...)
				if ttft == 0 && generatesOutput(line) {
					ttft = time.Since(sentAt)
				}
				if next := tokenUsage(payload); next.Found {
					usage = next
				}
				result.event = replaceSSEData(result.event, bytes.TrimSuffix(stampSSE(line, requestID, toolChoices), []byte("\n")))
			}
			if err := write(result.event); err != nil {
				return usage, ttft, err
			}
		}
	}
}

// Bound one event, including a line with no newline, without buffering the
// entire generation. Only complete SSE events may reach the client or meter.
func readSSEEvent(reader *bufio.Reader, limit int) ([]byte, error) {
	var event []byte
	lineStart := 0
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(event)+len(fragment) > limit {
			return nil, errors.New("upstream SSE event exceeds response limit")
		}
		event = append(event, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(bytes.TrimRight(event[lineStart:], "\r\n")) == 0 {
			return event, nil
		}
		lineStart = len(event)
	}
}

// SSE joins consecutive data fields with a newline. Other fields and comments
// remain intact when replacing those data fields with one equivalent JSON line.
func sseEventData(event []byte) ([]byte, bool) {
	var payload []byte
	found := false
	for line := range bytes.SplitSeq(event, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		value, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok && !bytes.Equal(line, []byte("data")) {
			continue
		}
		if found {
			payload = append(payload, '\n')
		}
		payload = append(payload, bytes.TrimPrefix(value, []byte(" "))...)
		found = true
	}
	return payload, found
}

func replaceSSEData(event, replacement []byte) []byte {
	var out []byte
	inserted := false
	for _, line := range bytes.SplitAfter(event, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data:")) || bytes.Equal(bytes.TrimRight(line, "\r\n"), []byte("data")) {
			if !inserted {
				out = append(out, replacement...)
				out = append(out, '\n')
				inserted = true
			}
			continue
		}
		out = append(out, line...)
	}
	return out
}

// generatesOutput excludes role/opening/usage frames and includes generated
// reasoning and nonempty tool deltas when measuring the first output token.
func generatesOutput(line []byte) bool {
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	var chunk struct {
		Choices []struct {
			Text  string `json:"text"`
			Delta struct {
				Content          string            `json:"content"`
				Reasoning        string            `json:"reasoning"`
				ReasoningContent string            `json:"reasoning_content"`
				ReasoningDetails []json.RawMessage `json:"reasoning_details"`
				ToolCalls        []json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(payload, &chunk) != nil {
		return false
	}
	for _, c := range chunk.Choices {
		d := c.Delta
		text := c.Text != "" || d.Content != "" || d.Reasoning != "" || d.ReasoningContent != ""
		if text || len(d.ReasoningDetails) > 0 || len(d.ToolCalls) > 0 {
			return true
		}
	}
	return false
}

// RawMessage preserves nested tool/reasoning fields and integers exactly while
// stamping the generation id and correcting completed tool-call finish reasons.
func stampSSE(line []byte, requestID string, toolChoices map[int]bool) []byte {
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	var chunk map[string]json.RawMessage
	if json.Unmarshal(payload, &chunk) != nil || chunk == nil {
		return line
	}
	chunk["id"], _ = json.Marshal(requestID)
	normalizeToolFinishReasons(chunk, toolChoices)
	out, err := json.Marshal(chunk)
	if err != nil {
		return line
	}
	return append(append([]byte("data: "), out...), '\n')
}

func decorateJSON(body []byte, requestID string, usage Usage, costMicro int64) []byte {
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return body
	}
	payload["id"], _ = json.Marshal(requestID)
	payload["provider"], _ = json.Marshal(providerName)
	normalizeToolFinishReasons(payload, nil)
	if usage.Found {
		var reported map[string]json.RawMessage
		if json.Unmarshal(payload["usage"], &reported) == nil && reported != nil {
			reported["cost"], _ = json.Marshal(costUSD(costMicro))
			payload["usage"], _ = json.Marshal(reported)
		}
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}

// Some engines finish valid tool calls with "stop". OpenAI clients need
// "tool_calls" to continue the tool loop. Never hide truncation or errors.
// Streamed calls can arrive before the final reason, independently per choice.
func normalizeToolFinishReasons(payload map[string]json.RawMessage, toolChoices map[int]bool) {
	var choices []json.RawMessage
	if json.Unmarshal(payload["choices"], &choices) != nil {
		return
	}
	changed := false
	for i, raw := range choices {
		var choice struct {
			Index        int    `json:"index"`
			FinishReason string `json:"finish_reason"`
			Message      struct {
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"message"`
			Delta struct {
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		}
		if json.Unmarshal(raw, &choice) != nil {
			continue
		}
		hasTools := len(choice.Message.ToolCalls) > 0
		if toolChoices != nil {
			if len(choice.Delta.ToolCalls) > 0 {
				toolChoices[choice.Index] = true
			}
			hasTools = toolChoices[choice.Index]
		}
		if !hasTools || choice.FinishReason != "stop" {
			continue
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			continue
		}
		fields["finish_reason"] = json.RawMessage(`"tool_calls"`)
		choices[i], _ = json.Marshal(fields)
		changed = true
	}
	if changed {
		payload["choices"], _ = json.Marshal(choices)
	}
}

type streamFailure struct {
	status  int
	message string
	code    string
}

func (e *streamFailure) Error() string { return e.message }
func (e *streamFailure) Unwrap() error { return io.ErrUnexpectedEOF }

func streamFailureFor(err error) *streamFailure {
	var failure *streamFailure
	if errors.As(err, &failure) {
		return failure
	}
	return &streamFailure{http.StatusBadGateway, "Upstream inference stream ended before completion", "upstream_stream_interrupted"}
}

func streamPayloadError(payload []byte) error {
	var frame struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(payload, &frame) != nil {
		return fmt.Errorf("invalid upstream SSE JSON: %w", io.ErrUnexpectedEOF)
	}
	if len(frame.Error) > 0 && !bytes.Equal(frame.Error, []byte("null")) {
		failure := streamFailureFor(nil)
		var reported struct {
			Code    json.RawMessage `json:"code"`
			Message string          `json:"message"`
		}
		if json.Unmarshal(frame.Error, &reported) == nil {
			var status int
			if json.Unmarshal(reported.Code, &status) == nil && status >= 400 && status <= 599 {
				failure.status = status
				if status < 500 && reported.Message != "" {
					failure.message = reported.Message
				}
			}
		}
		switch failure.status {
		case http.StatusTooManyRequests:
			failure.code = "rate_limit_exceeded"
		case http.StatusBadRequest:
			failure.code = "invalid_request_error"
		}
		return failure
	}
	for _, choice := range frame.Choices {
		if choice.FinishReason == "error" {
			return streamFailureFor(nil)
		}
	}
	return nil
}

func writeStreamError(w *echo.Response, requestID, model string, failure *streamFailure) {
	frame := map[string]any{
		"id": requestID, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model, "provider": providerName,
		"error":   map[string]any{"code": failure.status, "message": failure.message, "metadata": map[string]string{"error_type": failure.code}},
		"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": ""}, "finish_reason": "error"}},
	}
	body, _ := json.Marshal(frame)
	_, _ = w.Write(append(append([]byte("data: "), body...), '\n', '\n'))
	w.Flush()
}
