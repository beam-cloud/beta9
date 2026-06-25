package pod

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func llmEnabled(config *types.StubConfigV1) bool {
	if config == nil {
		return false
	}
	return strings.EqualFold(config.EffectiveServingProtocol(), llmServingProtocolOpenAI)
}

func llmConfiguredModel(config *types.StubConfigV1) string {
	llm := config.EffectiveLLMConfig()
	if llm == nil {
		return ""
	}
	if llm.ServedModelName != "" {
		return llm.ServedModelName
	}
	return llm.ModelID
}

func llmContextLength(config *types.StubConfigV1) int64 {
	llm := config.EffectiveLLMConfig()
	if llm == nil || llm.ContextLength <= 0 {
		return llmDefaultContextLen
	}
	return int64(llm.ContextLength)
}

func llmReadinessProbePaths(config *types.StubConfigV1) []string {
	paths := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	if llm := config.EffectiveLLMConfig(); llm != nil && strings.TrimSpace(llm.MetricsPath) != "" {
		paths = append(paths, llm.MetricsPath)
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		path = "/" + strings.TrimPrefix(strings.TrimSpace(path), "/")
		if path == "/" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

func (pb *PodProxyBuffer) checkContainerReady(address string, timeout time.Duration) bool {
	if llmEnabled(pb.stubConfig) {
		return pb.checkLLMContainerReady(address, timeout)
	}
	return pb.checkContainerAvailableWithTimeout(address, timeout)
}

func (pb *PodProxyBuffer) checkLLMContainerReady(address string, timeout time.Duration) bool {
	if address == "" {
		return false
	}
	if timeout < llmReadinessTimeout {
		timeout = llmReadinessTimeout
	}

	client := &http.Client{
		Transport: pb.backendTransport(address, timeout),
		Timeout:   timeout,
	}
	for _, path := range llmReadinessProbePaths(pb.stubConfig) {
		ctx, cancel := context.WithTimeout(pb.baseContext(), timeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, podBackendURL("http", address, path, ""), nil)
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, llmReadinessBodyLimit))
		_ = resp.Body.Close()
		cancel()

		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			return true
		}
	}
	return false
}
