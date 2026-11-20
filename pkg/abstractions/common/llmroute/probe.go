package llmroute

import (
	"cmp"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	readyTimeout   = 5 * time.Second
	readyBodyLimit = 4 * 1024
	metricsTimeout = 750 * time.Millisecond
)

// readyPaths are where an OpenAI-compatible engine answers once it serves.
var readyPaths = []string{"/v1/models", "/health", "/server_info", "/get_model_info"}

// Ready reports whether the engine at baseURL serves: its health path
// answers 2xx or, when it names none, one of the paths engines answer on.
func Ready(ctx context.Context, client *http.Client, baseURL, health string) bool {
	paths := readyPaths
	if health = strings.TrimSpace(health); health != "" {
		paths = []string{health}
	}
	for _, path := range paths {
		if status, _, _ := get(ctx, client, baseURL+path, "application/json", readyTimeout, readyBodyLimit); status/100 == 2 {
			return true
		}
	}
	return false
}

// NormalizeMetricsPath returns metricsPath with a leading slash, or /metrics
// when there is none.
func NormalizeMetricsPath(metricsPath string) string {
	return "/" + strings.TrimPrefix(cmp.Or(strings.TrimSpace(metricsPath), "metrics"), "/")
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
