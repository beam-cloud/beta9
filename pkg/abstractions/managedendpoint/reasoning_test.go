package managedendpoint

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReasoningCompatibility(t *testing.T) {
	for _, tc := range []struct {
		body, effort string
		fail         bool
	}{
		{`{"reasoning":{"enabled":false}}`, "none", false},
		{`{"reasoning":{"enabled":true}}`, "medium", false},
		{`{"reasoning":{"effort":"low"}}`, "low", false},
		{`{"reasoning":{"effort":"none","exclude":false}}`, "none", false},
		{`{"reasoning":{"enabled":false},"reasoning_effort":"none"}`, "none", false},
		{`{"reasoning":{"enabled":false},"reasoning_effort":"high"}`, "", true},
		{`{"reasoning":{"enabled":false,"effort":"high"}}`, "", true},
		{`{"reasoning":{"enabled":"false"}}`, "", true},
		{`{"reasoning":{"max_tokens":1024}}`, "", true},
		{`{"reasoning":{"exclude":true}}`, "", true},
		{`{"reasoning":{"effort":"typo"}}`, "", true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var body map[string]any
			require.NoError(t, json.Unmarshal([]byte(tc.body), &body))
			changed, err := normalizeReasoning(body)
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, tc.effort, body["reasoning_effort"])
			require.NotContains(t, body, "reasoning")
		})
	}
}

func TestVLLMStreamingRequestsRetainUsageAcrossPreemption(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		request := &routeRequest{body: []byte(`{"model":"qwen/qwen3-8b","stream":true,"stream_options":{"include_usage":true}}`), stream: true}
		request.setModel("qwen/qwen3-8b", continuous)
		var body map[string]any
		require.NoError(t, json.Unmarshal(request.body, &body))
		options := body["stream_options"].(map[string]any)
		if continuous {
			require.Equal(t, true, options["continuous_usage_stats"])
		} else {
			require.NotContains(t, options, "continuous_usage_stats")
		}
	}
}
