// Package llmroute contains the LLM-aware request inspection, replica scoring
// and affinity primitives used to route OpenAI-compatible inference traffic.
//
// The package is deliberately free of gateway-specific dependencies: callers
// provide replicas, Redis key namespaces and HTTP clients, and llmroute
// provides deterministic decisions that are easy to unit test.
package llmroute

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	MaxInspectBytes     int64 = 128 * 1024
	DefaultContextLen   int64 = 4096
	DefaultOutputTokens int64 = 256

	affinityHashChars = 24
	prefixChars       = 4096
	prefixBlockChars  = 512
	maxPrefixBlocks   = 8
)

var (
	openAIPaths = []string{
		"/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/models",
	}
	sessionHeaders   = []string{"X-Beam-LLM-Session", "X-Beam-Session", "X-Session-ID"}
	requestIDHeaders = []string{"X-Request-ID", "X-Request-Id", "X-Amzn-Trace-Id", "Traceparent"}
)

// RequestInfo is everything the router learns about an inference request
// before choosing a replica.
type RequestInfo struct {
	Method        string
	Path          string
	RequestID     string
	Model         string
	SessionKey    string
	SessionHash   string
	PrefixHash    string
	PrefixBlocks  []string
	AffinityKey   string
	PromptTokens  int64
	OutputTokens  int64
	TokenPressure int64
	Stream        bool

	// Populated by Select.
	RouteReason        string
	RouteScore         int64
	CandidateCount     int
	ReadyReplicaCount  int
	PrefixCacheMatches int
	QueueWait          time.Duration
}

// InspectOptions tunes Inspect for a particular deployment.
type InspectOptions struct {
	// DefaultModel is used when the payload does not carry a model.
	DefaultModel string
	// MaxBytes bounds how much of the body is inspected. Zero means MaxInspectBytes.
	MaxBytes int64
}

// Inspect reads an OpenAI-compatible request body (restoring it for the
// upstream proxy) and derives the model, token estimate and affinity keys.
// path must already be normalized with NormalizePath.
func Inspect(req *http.Request, path string, opts InspectOptions) (*RequestInfo, error) {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = MaxInspectBytes
	}

	info := &RequestInfo{
		Method:       req.Method,
		Path:         path,
		RequestID:    RequestID(req),
		Model:        opts.DefaultModel,
		OutputTokens: DefaultOutputTokens,
		RouteReason:  "least_pressure",
	}

	if req.Body == nil || req.Method == http.MethodGet || req.Method == http.MethodHead {
		info.PromptTokens = 1
		info.TokenPressure = info.PromptTokens + info.OutputTokens
		info.setAffinity(path)
		return info, nil
	}

	body, overflow, err := ReadAndRestoreBody(req, maxBytes)
	if err != nil {
		return nil, err
	}
	if overflow {
		info.finalize(string(body), "")
		return info, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		info.finalize(string(body), "")
		return info, nil
	}

	if model, ok := payload["model"].(string); ok && strings.TrimSpace(model) != "" {
		info.Model = strings.TrimSpace(model)
	}
	info.Stream = boolValue(payload["stream"])
	info.OutputTokens = RequestedOutputTokens(payload)
	promptText := PromptText(payload)

	info.SessionKey = SessionKey(req, payload)
	if info.SessionKey != "" {
		info.SessionHash = PrefixHash(info.Model, "session:"+info.SessionKey)
	}
	info.finalize(promptText, string(body))
	return info, nil
}

func (info *RequestInfo) finalize(promptText, fallbackText string) {
	info.PromptTokens = EstimateTokens(promptText)
	if info.PromptTokens == 0 {
		info.PromptTokens = EstimateTokens(fallbackText)
	}
	info.TokenPressure = info.PromptTokens + info.OutputTokens
	if info.TokenPressure <= 0 {
		info.TokenPressure = 1
	}
	info.setAffinity(promptText)
}

func (info *RequestInfo) setAffinity(promptText string) {
	info.PrefixHash = PrefixHash(info.Model, promptText)
	info.PrefixBlocks = PrefixBlockHashes(info.Model, promptText)
	if info.SessionHash != "" {
		info.AffinityKey = info.SessionHash
		return
	}
	info.AffinityKey = info.PrefixHash
}

// NormalizePath maps any path ending in a known OpenAI route to that route.
// The boolean reports whether the path is an OpenAI route at all.
func NormalizePath(path string) (string, bool) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	for _, openAIPath := range openAIPaths {
		if path == openAIPath || strings.HasSuffix(path, openAIPath) {
			return openAIPath, true
		}
	}
	return path, false
}

// IsOpenAIPath reports whether path is (or ends in) a known OpenAI route.
func IsOpenAIPath(path string) bool {
	_, ok := NormalizePath(path)
	return ok
}

// ReadAndRestoreBody reads up to maxBytes of the request body and replaces
// req.Body with a reader that replays the consumed bytes before the remainder.
// The boolean reports whether the body was larger than maxBytes.
func ReadAndRestoreBody(req *http.Request, maxBytes int64) ([]byte, bool, error) {
	limited := io.LimitReader(req.Body, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}

	overflow := int64(len(body)) > maxBytes
	req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
	if overflow {
		return body[:int(maxBytes)], true, nil
	}
	return body, false, nil
}

// RequestedOutputTokens returns max_tokens / max_completion_tokens or the default.
func RequestedOutputTokens(payload map[string]any) int64 {
	for _, key := range []string{"max_tokens", "max_completion_tokens"} {
		value := int64Value(payload[key])
		if value > 0 {
			return value
		}
	}
	return DefaultOutputTokens
}

// PromptText concatenates the textual content of an OpenAI chat, completion
// or embedding payload.
func PromptText(payload map[string]any) string {
	var parts []string
	appendText := func(value string) {
		if strings.TrimSpace(value) != "" {
			parts = append(parts, value)
		}
	}

	if messages, ok := payload["messages"].([]any); ok {
		for _, message := range messages {
			m, ok := message.(map[string]any)
			if !ok {
				continue
			}
			appendText(contentText(m["content"]))
		}
	}

	switch prompt := payload["prompt"].(type) {
	case string:
		appendText(prompt)
	case []any:
		for _, item := range prompt {
			if text, ok := item.(string); ok {
				appendText(text)
			}
		}
	}

	switch input := payload["input"].(type) {
	case string:
		appendText(input)
	case []any:
		for _, item := range input {
			if text, ok := item.(string); ok {
				appendText(text)
			}
		}
	}

	return strings.Join(parts, "\n")
}

func contentText(value any) string {
	switch content := value.(type) {
	case string:
		return content
	case []any:
		var parts []string
		for _, item := range content {
			part, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := part["text"].(string); ok {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

// SessionKey returns the caller-provided session identifier, if any.
func SessionKey(req *http.Request, payload map[string]any) string {
	for _, header := range sessionHeaders {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	if user, ok := payload["user"].(string); ok {
		return strings.TrimSpace(user)
	}
	return ""
}

// RequestID returns the first tracing/request id header present.
func RequestID(req *http.Request) string {
	for _, header := range requestIDHeaders {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

// EstimateTokens approximates the token count of text (4 chars per token).
func EstimateTokens(text string) int64 {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	tokens := int64(math.Ceil(float64(len([]rune(text))) / 4.0))
	if tokens < 1 {
		return 1
	}
	return tokens
}

// PrefixHash hashes the normalized prompt prefix for a model.
func PrefixHash(model, text string) string {
	normalized := truncateText(normalizeText(text), prefixChars)
	sum := sha256.Sum256([]byte(model + "\n" + normalized))
	return hex.EncodeToString(sum[:])[:affinityHashChars]
}

// PrefixBlockHashes returns hashes of progressively longer prompt prefixes so
// that partially shared prompts still map to the same replica.
func PrefixBlockHashes(model, text string) []string {
	normalized := truncateText(normalizeText(text), prefixChars)
	if normalized == "" {
		return nil
	}

	runes := []rune(normalized)
	hashes := make([]string, 0, min(len(runes)/prefixBlockChars+1, maxPrefixBlocks))
	for end := prefixBlockChars; end < len(runes) && len(hashes) < maxPrefixBlocks; end += prefixBlockChars {
		hashes = append(hashes, PrefixHash(model, "block:"+string(runes[:end])))
	}
	if len(hashes) < maxPrefixBlocks {
		hashes = append(hashes, PrefixHash(model, "block:"+string(runes)))
	}
	return hashes
}

func normalizeText(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func truncateText(text string, maxChars int) string {
	if maxChars <= 0 || text == "" {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxChars {
		return text
	}
	return string(runes[:maxChars])
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, _ := strconv.ParseBool(v)
		return parsed
	default:
		return false
	}
}

func int64Value(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case json.Number:
		out, _ := v.Int64()
		return out
	case string:
		out, _ := strconv.ParseInt(v, 10, 64)
		return out
	default:
		return 0
	}
}
