package llmroute

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	ReadinessTimeout   time.Duration = 5 * time.Second
	readinessBodyLimit int64         = 4 * 1024
	MetricsTimeout     time.Duration = 750 * time.Millisecond
	DefaultMetricsPath               = "/metrics"
)

// ReadinessPaths returns the ordered set of HTTP paths that indicate an
// OpenAI-compatible engine is serving, including the metrics path if given.
func ReadinessPaths(metricsPath string) []string {
	paths := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	if strings.TrimSpace(metricsPath) != "" {
		paths = append(paths, metricsPath)
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

// NormalizeMetricsPath returns metricsPath with a leading slash or the default.
func NormalizeMetricsPath(metricsPath string) string {
	metricsPath = strings.TrimSpace(metricsPath)
	if metricsPath == "" {
		return DefaultMetricsPath
	}
	return "/" + strings.TrimPrefix(metricsPath, "/")
}

// CheckReady probes each path on baseURL until one returns 2xx.
func CheckReady(ctx context.Context, client *http.Client, baseURL string, paths []string, timeout time.Duration) bool {
	if baseURL == "" {
		return false
	}
	if timeout < ReadinessTimeout {
		timeout = ReadinessTimeout
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	baseURL = strings.TrimRight(baseURL, "/")

	for _, path := range paths {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, baseURL+path, nil)
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
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, readinessBodyLimit))
		_ = resp.Body.Close()
		cancel()

		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			return true
		}
	}
	return false
}

// FetchEngineMetrics scrapes the engine's Prometheus endpoint and folds it
// into a snapshot relative to previous. It returns false when no data was
// retrieved.
func FetchEngineMetrics(ctx context.Context, client *http.Client, metricsURL string, previous EngineMetrics) (EngineMetrics, bool, error) {
	if client == nil {
		client = &http.Client{Timeout: MetricsTimeout}
	}
	ctx, cancel := context.WithTimeout(ctx, MetricsTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return EngineMetrics{}, false, err
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := client.Do(req)
	if err != nil {
		return EngineMetrics{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, readinessBodyLimit))
		return EngineMetrics{}, false, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, MetricsBodyLimit))
	if err != nil {
		return EngineMetrics{}, false, err
	}
	snapshot := EngineMetricsFromPrometheus(body, previous, time.Now())
	return snapshot, snapshot.HasData(), nil
}
