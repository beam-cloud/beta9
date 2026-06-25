package pod

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

	"github.com/labstack/echo/v4"
)

func (pb *PodProxyBuffer) inspectLLMRequest(ctx echo.Context) (*llmRequestInfo, error) {
	if !llmEnabled(pb.stubConfig) {
		return nil, nil
	}

	path := llmRequestPath(ctx)
	if !isOpenAIPath(path) {
		return nil, nil
	}

	req := ctx.Request()
	info := &llmRequestInfo{
		Enabled:      true,
		Method:       req.Method,
		Path:         path,
		RequestID:    llmRequestID(req),
		Model:        llmConfiguredModel(pb.stubConfig),
		OutputTokens: llmDefaultOutputTokens,
		RouteReason:  "least_pressure",
	}

	if req.Body == nil || req.Method == http.MethodGet || req.Method == http.MethodHead {
		info.PromptTokens = 1
		info.TokenPressure = info.PromptTokens + info.OutputTokens
		setLLMAffinity(info, path)
		return info, nil
	}

	body, overflow, err := readAndRestoreRequestBody(req, llmMaxInspectBytes)
	if err != nil {
		return nil, err
	}

	if overflow {
		finalizeLLMRequestInfo(info, string(body), "")
		return info, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		finalizeLLMRequestInfo(info, string(body), "")
		return info, nil
	}

	if model, ok := payload["model"].(string); ok && strings.TrimSpace(model) != "" {
		info.Model = strings.TrimSpace(model)
	}
	info.Stream = boolValue(payload["stream"])
	info.OutputTokens = requestedOutputTokens(payload)
	promptText := promptTextFromOpenAIPayload(payload)

	info.SessionKey = llmSessionKey(req, payload)
	if info.SessionKey != "" {
		info.SessionHash = hashLLMPrefix(info.Model, "session:"+info.SessionKey)
	}
	finalizeLLMRequestInfo(info, promptText, string(body))
	return info, nil
}

func finalizeLLMRequestInfo(info *llmRequestInfo, promptText, fallbackText string) {
	if info == nil {
		return
	}
	info.PromptTokens = estimateTokensFromText(promptText)
	if info.PromptTokens == 0 {
		info.PromptTokens = estimateTokensFromText(fallbackText)
	}
	info.TokenPressure = info.PromptTokens + info.OutputTokens
	if info.TokenPressure <= 0 {
		info.TokenPressure = 1
	}
	setLLMAffinity(info, promptText)
}

func llmRequestPath(ctx echo.Context) string {
	subPath := ctx.Param("subPath")
	if subPath == "" {
		subPath = ctx.Request().URL.Path
	}
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	if openAIPath, ok := normalizedOpenAIPath(subPath); ok {
		return openAIPath
	}
	return subPath
}

func isOpenAIPath(path string) bool {
	_, ok := normalizedOpenAIPath(path)
	return ok
}

func normalizedOpenAIPath(path string) (string, bool) {
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

func readAndRestoreRequestBody(req *http.Request, maxBytes int64) ([]byte, bool, error) {
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

func requestedOutputTokens(payload map[string]any) int64 {
	for _, key := range []string{"max_tokens", "max_completion_tokens"} {
		value := int64Value(payload[key])
		if value > 0 {
			return value
		}
	}
	return llmDefaultOutputTokens
}

func promptTextFromOpenAIPayload(payload map[string]any) string {
	var parts []string
	appendText := func(value string) {
		if strings.TrimSpace(value) != "" {
			parts = append(parts, value)
		}
	}

	switch messages := payload["messages"].(type) {
	case []any:
		for _, message := range messages {
			m, ok := message.(map[string]any)
			if !ok {
				continue
			}
			appendText(openAIContentText(m["content"]))
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

func openAIContentText(value any) string {
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

func llmSessionKey(req *http.Request, payload map[string]any) string {
	for _, header := range llmSessionHeaders {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	if user, ok := payload["user"].(string); ok {
		return strings.TrimSpace(user)
	}
	return ""
}

func llmRequestID(req *http.Request) string {
	for _, header := range llmRequestIDHeaders {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

func estimateTokensFromText(text string) int64 {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	runes := len([]rune(text))
	tokens := int64(math.Ceil(float64(runes) / 4.0))
	if tokens < 1 {
		return 1
	}
	return tokens
}

func setLLMAffinity(info *llmRequestInfo, promptText string) {
	if info == nil {
		return
	}
	info.PrefixHash = hashLLMPrefix(info.Model, promptText)
	info.PrefixBlocks = llmPrefixBlockHashes(info.Model, promptText)
	if info.SessionHash != "" {
		info.AffinityKey = info.SessionHash
		return
	}
	info.AffinityKey = info.PrefixHash
}

func hashLLMPrefix(model, text string) string {
	normalized := truncateLLMText(normalizeLLMText(text), llmPrefixChars)
	sum := sha256.Sum256([]byte(model + "\n" + normalized))
	return hex.EncodeToString(sum[:])[:llmAffinityHashChars]
}

func llmPrefixBlockHashes(model, text string) []string {
	normalized := truncateLLMText(normalizeLLMText(text), llmPrefixChars)
	if normalized == "" {
		return nil
	}

	runes := []rune(normalized)
	hashes := make([]string, 0, min(len(runes)/llmPrefixBlockChars+1, llmMaxPrefixBlocks))
	for end := llmPrefixBlockChars; end < len(runes) && len(hashes) < llmMaxPrefixBlocks; end += llmPrefixBlockChars {
		hashes = append(hashes, hashLLMPrefix(model, "block:"+string(runes[:end])))
	}
	if len(hashes) < llmMaxPrefixBlocks {
		hashes = append(hashes, hashLLMPrefix(model, "block:"+string(runes)))
	}
	return hashes
}

func normalizeLLMText(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func truncateLLMText(text string, maxChars int) string {
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
