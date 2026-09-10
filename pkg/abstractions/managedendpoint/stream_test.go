package managedendpoint

import (
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
			usage, ttft, err := relayStream(response, strings.NewReader(chunks+tc.ending), "gen-test", time.Now().Add(-time.Second))
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
