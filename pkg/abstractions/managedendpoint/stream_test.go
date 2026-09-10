package managedendpoint

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRelayStreamRequiresCompletionAndRetainsLatestUsage(t *testing.T) {
	const chunks = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":1}}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n"
	for _, tc := range []struct {
		name, ending string
		failed       bool
	}{
		{"completed", "data: [DONE]\n\n", false},
		{"clean EOF while generating", "", true},
		{"upstream error with terminal marker", "data: {\"error\":{\"message\":\"engine failed\"}}\n\ndata: [DONE]\n\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			response := echo.NewResponse(recorder, echo.New())
			usage, ttft, err := relayStream(response, strings.NewReader(chunks+tc.ending), "gen-test", time.Now().Add(-time.Second), nil)
			if tc.failed {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.NoError(t, err)
			}
			require.True(t, usage.Found)
			require.EqualValues(t, 12, usage.PromptTokens)
			require.EqualValues(t, 7, usage.CompletionTokens)
			require.Positive(t, ttft)
			require.Contains(t, recorder.Body.String(), "hello")
		})
	}
}

func TestRelayStreamCommitsAccountingBeforeTerminalMarker(t *testing.T) {
	const stream = "data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":64}}}\n\ndata: [DONE]\n\ndata: [DONE]\n\n"
	for _, fail := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		response := echo.NewResponse(recorder, echo.New())
		calls := 0
		_, _, err := relayStream(response, strings.NewReader(stream), "gen-test", time.Now(), func(u Usage, _ time.Duration) error {
			calls++
			require.NotContains(t, recorder.Body.String(), "[DONE]")
			require.EqualValues(t, 64, u.CachedTokens)
			if fail {
				return errors.New("accounting unavailable")
			}
			return nil
		})
		require.Equal(t, 1, calls)
		if fail {
			require.Error(t, err)
			require.NotContains(t, recorder.Body.String(), "[DONE]")
		} else {
			require.NoError(t, err)
			require.Equal(t, 1, strings.Count(recorder.Body.String(), "[DONE]"))
		}
	}
}

func TestRelayStreamKeepsSilentReasoningAliveWithoutMeteringComments(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	flushed := make(chan struct{}, 8)
	recorder := &flushObserver{ResponseRecorder: httptest.NewRecorder(), flushed: flushed}
	response := echo.NewResponse(recorder, echo.New())
	done := make(chan error, 1)
	go func() {
		_, ttft, err := relayStreamWithKeepalive(response, reader, "gen-test", time.Now().Add(-time.Second), nil, 10*time.Millisecond)
		if ttft == 0 && err == nil {
			err = errors.New("reasoning output did not set TTFT")
		}
		done <- err
	}()
	// The engine has not written anything. Headers and two comments must still
	// flush promptly, without waiting for a generated token.
	for range 2 {
		select {
		case <-flushed:
		case <-time.After(time.Second):
			t.Fatal("silent stream did not receive heartbeat")
		}
	}
	_, err := io.WriteString(writer, "data: {\"choices\":[{\"delta\":{\"reasoning\":\"thinking\"}}]}\n\ndata: [DONE]\n\n")
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, strings.Count(recorder.Body.String(), ": keepalive"), 2)
	require.Contains(t, recorder.Body.String(), "thinking")
}

type flushObserver struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
}

func (w *flushObserver) Flush() {
	w.ResponseRecorder.Flush()
	select {
	case w.flushed <- struct{}{}:
	default:
	}
}

func TestRelayStreamPreservesMultilineToolsReasoningAndExactNumbers(t *testing.T) {
	const stream = "event: message\r\ndata: {\"id\":\"upstream\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-original\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"value\\\":9007199254740993}\"}}],\"reasoning_details\":[{\"type\":\"reasoning.encrypted\",\"data\":\"opaque\"}]}}],\r\ndata: \"opaque_integer\":9007199254740993,\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":12}}}\r\n\r\ndata: [DONE]\n\n"
	recorder := httptest.NewRecorder()
	u, ttft, err := relayStream(echo.NewResponse(recorder, echo.New()), strings.NewReader(stream), "gen-test", time.Now(), nil)
	require.NoError(t, err)
	require.True(t, u.Found)
	require.EqualValues(t, 12, u.CachedTokens)
	require.Positive(t, ttft)
	body := recorder.Body.String()
	require.Contains(t, body, "event: message\r\n")
	require.Contains(t, body, `"id":"gen-test"`)
	require.Contains(t, body, `"id":"call-original"`)
	require.Contains(t, body, `"opaque_integer":9007199254740993`)
	require.Contains(t, body, `"reasoning_details":[{"type":"reasoning.encrypted","data":"opaque"}]`)
	require.Contains(t, body, `"arguments":"{\"value\":9007199254740993}"`)
}

func TestReadSSEEventBoundsPartialLineAndRequiresDelimiter(t *testing.T) {
	for _, input := range []string{strings.Repeat("a", 100), "data: " + strings.Repeat("a", 100) + "\n\n"} {
		_, err := readSSEEvent(bufio.NewReaderSize(strings.NewReader(input), 16), 32)
		require.ErrorContains(t, err, "exceeds response limit")
	}
	_, err := readSSEEvent(bufio.NewReader(strings.NewReader("data: [DONE]\n")), 32)
	require.ErrorIs(t, err, io.EOF, "an unterminated event cannot finalize billing")
}

func TestRelayStreamRejectsErrorFinishReasonAndMalformedJSON(t *testing.T) {
	for _, frame := range []string{
		`{"choices":[{"delta":{},"finish_reason":"error"}]}`,
		`{"error":{"code":429,"message":"overloaded"}}`,
		`{"invalid JSON"`,
	} {
		recorder := httptest.NewRecorder()
		calls := 0
		_, ttft, err := relayStream(echo.NewResponse(recorder, echo.New()), strings.NewReader("data: "+frame+"\n\ndata: [DONE]\n\n"), "gen-test", time.Now(), func(Usage, time.Duration) error { calls++; return nil })
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Zero(t, calls)
		require.Zero(t, ttft)
		require.NotContains(t, recorder.Body.String(), "[DONE]")
	}
}

func TestStreamOutputRecognizesGeneratedReasoningButNotOpeningFrames(t *testing.T) {
	for _, delta := range []string{`{"reasoning":"thinking"}`, `{"reasoning_content":"thinking"}`, `{"reasoning_details":[{"type":"reasoning.text","text":"thinking"}]}`, `{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}`} {
		require.True(t, generatesOutput([]byte("data: {\"choices\":[{\"delta\":"+delta+"}]}")), delta)
	}
	for _, frame := range []string{`{"choices":[{"delta":{"role":"assistant","content":null,"tool_calls":[]}}]}`, `{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":0}}`, `{"error":{"message":"failed"}}`} {
		require.False(t, generatesOutput([]byte("data: "+frame)), frame)
	}
}

func TestDecorateJSONPreservesOpaqueResponseValues(t *testing.T) {
	body := []byte(`{"id":"engine","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"original-call","function":{"arguments":"{\"id\":9007199254740993}"}}],"reasoning_details":[{"signature":"opaque","index":9007199254740993}]}}],"usage":{"prompt_tokens":12,"completion_tokens":3,"completion_tokens_details":{"reasoning_tokens":2}}}`)
	decorated := string(decorateJSON(body, "gen-test", tokenUsage(body), 5))
	require.Contains(t, decorated, `"id":"gen-test"`)
	require.Contains(t, decorated, `"id":"original-call"`)
	require.Contains(t, decorated, `"index":9007199254740993`)
	require.Contains(t, decorated, `"completion_tokens_details":{"reasoning_tokens":2}`)
	require.Contains(t, decorated, `"cost":0.000005`)
}

func TestDecorateJSONNormalizesOnlyCompletedToolCalls(t *testing.T) {
	const toolMessage = `{"content":null,"tool_calls":[{"id":"call-original","type":"function","function":{"name":"lookup","arguments":"{\"id\":9007199254740993}"}}],"opaque_integer":9007199254740993}`
	for _, tc := range []struct {
		name, message, reason, want string
	}{
		{"completed tools", toolMessage, `"stop"`, `"tool_calls"`},
		{"already correct", toolMessage, `"tool_calls"`, `"tool_calls"`},
		{"truncated tools", toolMessage, `"length"`, `"length"`},
		{"failed tools", toolMessage, `"error"`, `"error"`},
		{"filtered tools", toolMessage, `"content_filter"`, `"content_filter"`},
		{"unfinished tools", toolMessage, `null`, `null`},
		{"ordinary stop", `{"content":"hello"}`, `"stop"`, `"stop"`},
		{"empty tools", `{"content":"hello","tool_calls":[]}`, `"stop"`, `"stop"`},
		{"null tools", `{"content":"hello","tool_calls":null}`, `"stop"`, `"stop"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"choices":[{"index":0,"message":` + tc.message + `,"finish_reason":` + tc.reason + `}]}`)
			var result struct {
				Choices []struct {
					Message      json.RawMessage `json:"message"`
					FinishReason json.RawMessage `json:"finish_reason"`
				} `json:"choices"`
			}
			require.NoError(t, json.Unmarshal(decorateJSON(body, "gen-test", Usage{}, 0), &result))
			require.Equal(t, tc.want, string(result.Choices[0].FinishReason))
			require.Equal(t, tc.message, string(result.Choices[0].Message), "nested arguments and exact numbers remain opaque")
		})
	}

	body := []byte(`{"choices":[{"index":2,"message":` + toolMessage + `,"finish_reason":"stop"},{"index":0,"message":{"content":"hello"},"finish_reason":"stop"},{"index":1,"message":` + toolMessage + `,"finish_reason":"length"}]}`)
	var result struct {
		Choices []struct {
			Index        int    `json:"index"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	require.NoError(t, json.Unmarshal(decorateJSON(body, "gen-test", Usage{}, 0), &result))
	require.Equal(t, []int{2, 0, 1}, []int{result.Choices[0].Index, result.Choices[1].Index, result.Choices[2].Index})
	require.Equal(t, []string{"tool_calls", "stop", "length"}, []string{result.Choices[0].FinishReason, result.Choices[1].FinishReason, result.Choices[2].FinishReason})
}

func TestRelayStreamTracksToolCompletionByChoiceAcrossChunks(t *testing.T) {
	const first = `{"choices":[{"index":2,"delta":{"tool_calls":[{"index":0,"id":"call-original","type":"function","function":{"name":"lookup","arguments":"{\"id\":9007199254740993}"}}]},"finish_reason":null},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]},"finish_reason":null}]}`
	const last = `{"choices":[{"index":0,"delta":{"content":"hello","tool_calls":[]},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"length"},{"index":2,"delta":{},"finish_reason":"stop"}],"opaque_integer":9007199254740993}`
	recorder := httptest.NewRecorder()
	_, _, err := relayStream(echo.NewResponse(recorder, echo.New()), strings.NewReader("data: "+first+"\n\ndata: "+last+"\n\ndata: [DONE]\n\n"), "gen-test", time.Now(), nil)
	require.NoError(t, err)
	var frames []map[string]json.RawMessage
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var frame map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame))
		frames = append(frames, frame)
	}
	require.Len(t, frames, 2)
	var choices []struct {
		Index        int    `json:"index"`
		FinishReason string `json:"finish_reason"`
	}
	require.NoError(t, json.Unmarshal(frames[1]["choices"], &choices))
	require.Equal(t, []int{0, 1, 2}, []int{choices[0].Index, choices[1].Index, choices[2].Index})
	require.Equal(t, []string{"stop", "length", "tool_calls"}, []string{choices[0].FinishReason, choices[1].FinishReason, choices[2].FinishReason})
	require.Contains(t, string(frames[0]["choices"]), `"arguments":"{\"id\":9007199254740993}"`)
	require.Equal(t, "9007199254740993", string(frames[1]["opaque_integer"]))
	require.Contains(t, recorder.Body.String(), "[DONE]")
}

func TestStampSSEPreservesOtherToolFinishReasons(t *testing.T) {
	for _, reason := range []string{`"length"`, `"error"`, `"content_filter"`, `null`} {
		tools := map[int]bool{2: true}
		line := []byte(`data: {"choices":[{"index":2,"delta":{},"finish_reason":` + reason + `}]}`)
		require.Contains(t, string(stampSSE(line, "gen-test", tools)), `"finish_reason":`+reason)
	}
	line := []byte(`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-original"}]},"finish_reason":"stop"}]}`)
	require.Contains(t, string(stampSSE(line, "gen-test", make(map[int]bool))), `"finish_reason":"tool_calls"`, "a tool and its terminal reason can share one chunk")
}

func TestStreamErrorHasSingleTerminalChoiceAndNumericStatus(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeStreamError(echo.NewResponse(recorder, echo.New()), "gen-test", "acme/model", streamFailureFor(nil))
	payload, ok := sseEventData(recorder.Body.Bytes())
	require.True(t, ok)
	var frame struct {
		ID     string `json:"id"`
		Object string `json:"object"`
		Error  struct {
			Code int `json:"code"`
		} `json:"error"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	require.NoError(t, json.Unmarshal(payload, &frame))
	require.Equal(t, "gen-test", frame.ID)
	require.Equal(t, "chat.completion.chunk", frame.Object)
	require.Equal(t, 502, frame.Error.Code)
	require.Len(t, frame.Choices, 1)
	require.Equal(t, "error", frame.Choices[0].FinishReason)
	require.NotContains(t, recorder.Body.String(), "[DONE]")
}

func TestUpstreamStreamFailureRetains429AndSanitizesServerErrors(t *testing.T) {
	for _, tc := range []struct {
		body    string
		status  int
		message string
	}{
		{`{"error":{"code":429,"message":"too many requests"}}`, 429, "too many requests"},
		{`{"error":{"code":500,"message":"private engine traceback"}}`, 500, "Upstream inference stream ended before completion"},
		{`{"error":{"code":"engine_crash","message":"private engine traceback"}}`, 502, "Upstream inference stream ended before completion"},
	} {
		err := streamPayloadError([]byte(tc.body))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		failure := streamFailureFor(err)
		require.Equal(t, tc.status, failure.status)
		require.Equal(t, tc.message, failure.message)
	}
}

func TestRelayStreamDoesNotCountCommentOrEmptyDataAsOutput(t *testing.T) {
	recorder := httptest.NewRecorder()
	usage, ttft, err := relayStream(echo.NewResponse(recorder, echo.New()), strings.NewReader(": engine keepalive\n\ndata:\n\ndata: [DONE]\n\n"), "gen-test", time.Now(), nil)
	require.NoError(t, err)
	require.False(t, usage.Found)
	require.Zero(t, ttft)
	require.Contains(t, recorder.Body.String(), ": engine keepalive")
}
