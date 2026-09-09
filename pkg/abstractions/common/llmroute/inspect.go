// Package llmroute contains the LLM-aware request inspection, replica scoring
// and affinity primitives used to route OpenAI-compatible inference traffic.
package llmroute

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	maxInspectBytes     int64 = 128 * 1024
	defaultContextLen   int64 = 4096
	defaultOutputTokens int64 = 256

	affinityHashChars = 24
	prefixChars       = 4096
	prefixBlockChars  = 512
	maxPrefixBlocks   = 8
)

var (
	sessionHeaders   = []string{"X-Beam-LLM-Session", "X-Beam-Session", "X-Session-ID"}
	requestIDHeaders = []string{"X-Request-ID", "X-Request-Id", "X-Amzn-Trace-Id", "Traceparent"}
)

// RequestInfo is everything the router learns about an inference request
// before choosing a replica.
type RequestInfo struct {
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
	PrefixCacheMatches int
}

// InspectOptions tunes Inspect; DefaultModel is used when the payload carries no model.
type InspectOptions struct{ DefaultModel string }

// Inspect reads an OpenAI-compatible request body (restoring it for the
// upstream proxy) and derives the model, token estimate and affinity keys.
func Inspect(req *http.Request, path string, opts InspectOptions) (*RequestInfo, error) {
	info := &RequestInfo{
		Path:         path,
		RequestID:    headerValue(req, requestIDHeaders...),
		Model:        opts.DefaultModel,
		OutputTokens: defaultOutputTokens,
		RouteReason:  "least_pressure",
	}
	if req.Body == nil || req.Method == http.MethodGet || req.Method == http.MethodHead {
		info.PromptTokens = 1
		info.TokenPressure = 1 + info.OutputTokens
		info.setAffinity(path)
		return info, nil
	}

	body, overflow, err := readBody(req, maxInspectBytes)
	if err != nil {
		return nil, err
	}
	// Unparseable bodies are still hashed so identical retries share a replica.
	prompt, fallback := string(body), ""
	var payload map[string]any
	if !overflow && json.Unmarshal(body, &payload) == nil {
		if model, ok := payload["model"].(string); ok && strings.TrimSpace(model) != "" {
			info.Model = strings.TrimSpace(model)
		}
		info.Stream = boolValue(payload["stream"])
		for _, key := range []string{"max_tokens", "max_completion_tokens"} {
			if v := int64Value(payload[key]); v > 0 {
				info.OutputTokens = v
				break
			}
		}
		if info.SessionKey = headerValue(req, sessionHeaders...); info.SessionKey == "" {
			user, _ := payload["user"].(string)
			info.SessionKey = strings.TrimSpace(user)
		}
		if info.SessionKey != "" {
			info.SessionHash = prefixHash(info.Model, "session:"+info.SessionKey)
		}
		prompt, fallback = promptText(payload), string(body)
	}

	info.PromptTokens = estimateTokens(prompt)
	if info.PromptTokens == 0 {
		info.PromptTokens = estimateTokens(fallback)
	}
	info.TokenPressure = max(info.PromptTokens+info.OutputTokens, 1)
	info.setAffinity(prompt)
	return info, nil
}

func (info *RequestInfo) setAffinity(promptText string) {
	info.PrefixHash = prefixHash(info.Model, promptText)
	info.PrefixBlocks = prefixBlockHashes(info.Model, promptText)
	info.AffinityKey = info.PrefixHash
	if info.SessionHash != "" {
		info.AffinityKey = info.SessionHash
	}
}

// readBody reads up to maxBytes of the body and replaces req.Body with a reader
// that replays the consumed bytes. The boolean reports whether it was truncated.
func readBody(req *http.Request, maxBytes int64) ([]byte, bool, error) {
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBytes+1))
	if err != nil {
		return nil, false, err
	}
	req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
	if int64(len(body)) > maxBytes {
		return body[:maxBytes], true, nil
	}
	return body, false, nil
}

// promptText concatenates the textual content of an OpenAI chat, completion
// or embedding payload.
func promptText(payload map[string]any) string {
	var parts []string
	if messages, ok := payload["messages"].([]any); ok {
		for _, message := range messages {
			if m, ok := message.(map[string]any); ok {
				parts = appendText(parts, strings.Join(texts(m["content"]), "\n"))
			}
		}
	}
	for _, key := range []string{"prompt", "input"} {
		for _, text := range texts(payload[key]) {
			parts = appendText(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func appendText(parts []string, text string) []string {
	if strings.TrimSpace(text) == "" {
		return parts
	}
	return append(parts, text)
}

// texts extracts strings from a string, a string list or {"text": ...} content parts.
func texts(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, item := range v {
			switch item := item.(type) {
			case string:
				out = append(out, item)
			case map[string]any:
				if text, ok := item["text"].(string); ok {
					out = append(out, text)
				}
			}
		}
		return out
	}
	return nil
}

func headerValue(req *http.Request, names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(req.Header.Get(name)); v != "" {
			return v
		}
	}
	return ""
}

// estimateTokens approximates the token count of text (4 chars per token).
func estimateTokens(text string) int64 {
	return int64((len([]rune(strings.TrimSpace(text))) + 3) / 4)
}

// prefixHash hashes the normalized prompt prefix for a model.
func prefixHash(model, text string) string {
	sum := sha256.Sum256([]byte(model + "\n" + string(normalizePrefix(text))))
	return hex.EncodeToString(sum[:])[:affinityHashChars]
}

// prefixBlockHashes hashes progressively longer prompt prefixes so partially
// shared prompts still map to the same replica.
func prefixBlockHashes(model, text string) []string {
	runes := normalizePrefix(text)
	if len(runes) == 0 {
		return nil
	}
	var hashes []string
	for end := prefixBlockChars; end < len(runes) && len(hashes) < maxPrefixBlocks; end += prefixBlockChars {
		hashes = append(hashes, prefixHash(model, "block:"+string(runes[:end])))
	}
	if len(hashes) < maxPrefixBlocks {
		hashes = append(hashes, prefixHash(model, "block:"+string(runes)))
	}
	return hashes
}

// normalizePrefix collapses whitespace and truncates to prefixChars runes.
func normalizePrefix(text string) []rune {
	runes := []rune(strings.Join(strings.Fields(text), " "))
	return runes[:min(len(runes), prefixChars)]
}

// boolValue and int64Value accept the bool/number and string forms that
// json.Unmarshal produces for loosely typed clients.
func boolValue(value any) bool {
	if s, ok := value.(string); ok {
		value, _ = strconv.ParseBool(s)
	}
	b, _ := value.(bool)
	return b
}

func int64Value(value any) int64 {
	if s, ok := value.(string); ok {
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	f, _ := value.(float64)
	return int64(f)
}
