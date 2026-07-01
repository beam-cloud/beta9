package vast

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func RunSentinel(ctx context.Context, opts SentinelOptions) error {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	opts.HostURL = strings.TrimRight(strings.TrimSpace(opts.HostURL), "/")
	if opts.HostURL == "" {
		return fmt.Errorf("host-url is required")
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = DefaultHeartbeat
	}
	if opts.PreemptTimeout <= 0 {
		opts.PreemptTimeout = DefaultPreemptTimeout
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if opts.DetectGPUUUID == nil {
		opts.DetectGPUUUID = DetectVisibleGPUUUID
	}
	token, err := readSecret(opts.Token, opts.TokenFile)
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("token is required")
	}
	gpuUUID := strings.TrimSpace(opts.GPUUUID)
	if gpuUUID == "" {
		gpuUUID, err = opts.DetectGPUUUID(ctx)
		if err != nil {
			return err
		}
	}

	event := sentinelEvent{
		GPUUUID:  gpuUUID,
		Hostname: hostname(),
		PID:      os.Getpid(),
	}
	if err := postSentinelEvent(ctx, opts.HTTPClient, opts.HostURL, "/heartbeat", token, event); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "vast sentinel heartbeating gpu %s\n", gpuUUID)

	ticker := time.NewTicker(opts.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			preemptCtx, cancel := context.WithTimeout(context.Background(), opts.PreemptTimeout)
			defer cancel()
			if err := postSentinelEvent(preemptCtx, opts.HTTPClient, opts.HostURL, "/preempt", token, event); err != nil {
				fmt.Fprintf(opts.Stderr, "vast sentinel preempt failed for gpu %s: %v\n", gpuUUID, err)
			}
			return nil
		case <-ticker.C:
			if err := postSentinelEvent(ctx, opts.HTTPClient, opts.HostURL, "/heartbeat", token, event); err != nil {
				fmt.Fprintf(opts.Stderr, "vast sentinel heartbeat failed for gpu %s: %v\n", gpuUUID, err)
			}
		}
	}
}

func postSentinelEvent(ctx context.Context, client *http.Client, hostURL, path, token string, event sentinelEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hostURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

func hostname() string {
	name, _ := os.Hostname()
	return name
}
