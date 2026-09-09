package managedendpoint

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// An adapter describes how one OpenAI-style route is inspected, proxied and
// metered. Adding a modality (audio, ...) is a new adapter, nothing else.
type adapter struct {
	Route types.EndpointRoute
	// UpstreamPath is the path on the engine; empty means "same as the route".
	UpstreamPath string
	// LLM routes use prompt/session affinity and token-aware selection.
	LLM bool
	// Streamable routes accept "stream": true and must carry usage in the
	// final SSE chunk.
	Streamable bool
	// Usage extracts billable usage from a complete (non-stream) JSON body.
	Usage func(body []byte) Usage
}

var adapters = map[types.EndpointRoute]adapter{
	types.EndpointRouteChatCompletions: {
		Route: types.EndpointRouteChatCompletions, UpstreamPath: "/v1/chat/completions", LLM: true, Streamable: true, Usage: tokenUsage,
	},
	types.EndpointRouteCompletions: {
		Route: types.EndpointRouteCompletions, UpstreamPath: "/v1/completions", LLM: true, Streamable: true, Usage: tokenUsage,
	},
	types.EndpointRouteEmbeddings: {
		Route: types.EndpointRouteEmbeddings, UpstreamPath: "/v1/embeddings", Usage: tokenUsage,
	},
	types.EndpointRouteImageGenerations: {
		Route: types.EndpointRouteImageGenerations, UpstreamPath: "/v1/images/generations", Usage: imageUsage,
	},
	types.EndpointRouteImageEdits: {
		Route: types.EndpointRouteImageEdits, UpstreamPath: "/v1/images/edits", Usage: imageUsage,
	},
	types.EndpointRouteInvoke: {
		Route: types.EndpointRouteInvoke, UpstreamPath: "", Usage: requestUsage,
	},
}

// usageEnvelope is the OpenAI usage object as engines emit it.
type usageEnvelope struct {
	Usage *struct {
		PromptTokens        int64 `json:"prompt_tokens"`
		CompletionTokens    int64 `json:"completion_tokens"`
		TotalTokens         int64 `json:"total_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// tokenUsage reads prompt/completion/cached tokens from a response body.
func tokenUsage(body []byte) Usage {
	var env usageEnvelope
	if err := json.Unmarshal(body, &env); err != nil || env.Usage == nil {
		return Usage{}
	}
	u := Usage{
		PromptTokens:     env.Usage.PromptTokens,
		CompletionTokens: env.Usage.CompletionTokens,
		Requests:         1,
		Found:            true,
	}
	if env.Usage.PromptTokensDetails != nil {
		u.CachedTokens = env.Usage.PromptTokensDetails.CachedTokens
	}
	return u
}

// imageUsage counts generated images; token usage is added when present
// (gpt-image style engines report it).
func imageUsage(body []byte) Usage {
	var payload struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Usage{}
	}
	u := tokenUsage(body)
	u.Images = int64(len(payload.Data))
	u.Requests = 1
	u.Found = u.Images > 0 || u.Found
	return u
}

// requestUsage bills one request regardless of payload.
func requestUsage(body []byte) Usage {
	return Usage{Requests: 1, Found: true}
}

// sseUsage scans one SSE data line for a usage object.
func sseUsage(line []byte) (Usage, bool) {
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || !bytes.Contains(payload, []byte(`"usage"`)) {
		return Usage{}, false
	}
	u := tokenUsage(payload)
	return u, u.Found
}

// forceIncludeUsage rewrites a streaming request so the engine emits a final
// usage chunk (stream_options.include_usage). Returns the body unchanged when
// it is not a streaming request.
func forceIncludeUsage(payload map[string]any) bool {
	stream, _ := payload["stream"].(bool)
	if !stream {
		return false
	}
	opts, _ := payload["stream_options"].(map[string]any)
	if opts == nil {
		opts = map[string]any{}
	}
	opts["include_usage"] = true
	payload["stream_options"] = opts
	return true
}

// routeFromPath maps "/v1/chat/completions" -> chat/completions and
// "/v1/models/<id>/invoke" -> invoke with the model id.
func routeFromPath(prefix, path string) (types.EndpointRoute, string, bool) {
	rest := strings.TrimPrefix(strings.TrimSuffix(path, "/"), prefix)
	rest = strings.TrimPrefix(rest, "/")
	if strings.HasPrefix(rest, "models/") && strings.HasSuffix(rest, "/invoke") {
		id := strings.TrimSuffix(strings.TrimPrefix(rest, "models/"), "/invoke")
		return types.EndpointRouteInvoke, id, id != ""
	}
	switch types.EndpointRoute(rest) {
	case types.EndpointRouteChatCompletions, types.EndpointRouteCompletions, types.EndpointRouteEmbeddings,
		types.EndpointRouteImageGenerations, types.EndpointRouteImageEdits:
		return types.EndpointRoute(rest), "", true
	}
	return "", "", false
}
