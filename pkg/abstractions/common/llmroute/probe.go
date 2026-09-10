package llmroute

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	readinessTimeout   = 5 * time.Second
	readinessBodyLimit = 4 * 1024
	metricsTimeout     = 750 * time.Millisecond
	defaultMetricsPath = "/metrics"
)

// ReadinessPaths returns the ordered HTTP paths that indicate an
// OpenAI-compatible engine is serving, plus the metrics path if given.
func ReadinessPaths(metricsPath string) []string {
	paths := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	if p := NormalizeMetricsPath(metricsPath); strings.TrimSpace(metricsPath) != "" && p != "/" && !slices.Contains(paths, p) {
		return append(paths, p)
	}
	return paths
}

// NormalizeMetricsPath returns metricsPath with a leading slash or the default.
func NormalizeMetricsPath(metricsPath string) string {
	metricsPath = strings.TrimSpace(metricsPath)
	if metricsPath == "" {
		return defaultMetricsPath
	}
	return "/" + strings.TrimPrefix(metricsPath, "/")
}

// CheckReady probes each path on baseURL until one returns 2xx.
func CheckReady(ctx context.Context, client *http.Client, baseURL string, paths []string, timeout time.Duration) bool {
	if baseURL == "" {
		return false
	}
	baseURL = strings.TrimRight(baseURL, "/")
	timeout = max(timeout, readinessTimeout)
	for _, path := range paths {
		if status, _, _ := get(ctx, client, baseURL+path, "application/json", timeout, readinessBodyLimit); status/100 == 2 {
			return true
		}
	}
	return false
}

// FetchEngineMetrics scrapes the engine's Prometheus endpoint into a snapshot
// relative to previous. The boolean is false when the scrape carried no
// recognized engine metric (an idle engine reporting zeros is still data).
func FetchEngineMetrics(ctx context.Context, client *http.Client, metricsURL string, previous EngineMetrics) (EngineMetrics, bool, error) {
	status, body, err := get(ctx, client, metricsURL, "text/plain", metricsTimeout, metricsBodyLimit)
	if status != 0 && status/100 != 2 {
		return EngineMetrics{}, false, nil
	}
	if err != nil {
		return EngineMetrics{}, false, err
	}
	snapshot, found := engineMetricsFromPrometheus(body, previous, time.Now())
	return snapshot, found, nil
}

// get performs a bounded GET, returning the status (0 if no response) and up to limit body bytes.
func get(ctx context.Context, client *http.Client, url, accept string, timeout time.Duration, limit int64) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", accept)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return resp.StatusCode, body, err
}
