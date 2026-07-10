package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/sync/singleflight"
)

const (
	containerCostRequestTimeout = 5 * time.Second
	containerCostQuoteRefresh   = 5 * time.Minute
	containerCostRetryDelay     = 5 * time.Second
	containerCostResponseLimit  = 1 << 20
	containerCostErrorLimit     = 4 << 10
)

type ContainerCostResponse struct {
	CostPerMs      string `json:"cost_per_ms"`
	PricingVersion string `json:"pricing_version,omitempty"`
	EffectiveAt    string `json:"effective_at,omitempty"`
	ValidUntil     string `json:"valid_until,omitempty"`
}

// ContainerCostQuote is a validated quote returned by the billing service.
// Valid distinguishes a real zero-cost quote from the absence of a quote.
type ContainerCostQuote struct {
	CostPerMs      float64
	PricingVersion string
	EffectiveAt    time.Time
	ValidUntil     time.Time
	Valid          bool
}

type ContainerCostRequest struct {
	Cpu      int64  `json:"cpu"`
	Memory   int64  `json:"memory"`
	Gpu      string `json:"gpu"`
	GpuCount uint32 `json:"gpu_count"`
}

type containerCostCacheEntry struct {
	quote     ContainerCostQuote
	refreshAt time.Time
	retryAt   time.Time
}

type containerCostResult struct {
	quote ContainerCostQuote
	err   error
}

type ContainerCostClient struct {
	client   *http.Client
	endpoint string
	token    string

	mu      sync.RWMutex
	quotes  map[ContainerCostRequest]containerCostCacheEntry
	refresh singleflight.Group
	now     func() time.Time
}

func NewContainerCostClient(config types.ContainerCostHookConfig) *ContainerCostClient {
	if config.Endpoint == "" || config.Token == "" {
		return nil
	}
	return &ContainerCostClient{
		client:   &http.Client{Timeout: containerCostRequestTimeout},
		endpoint: config.Endpoint,
		token:    config.Token,
		quotes:   make(map[ContainerCostRequest]containerCostCacheEntry),
		now:      time.Now,
	}
}

// GetContainerCostQuote caches by resources for five minutes (or valid_until).
// A failed refresh returns the last good quote and retries on a later lookup.
func (c *ContainerCostClient) GetContainerCostQuote(ctx context.Context, request *types.ContainerRequest) (ContainerCostQuote, error) {
	if c == nil {
		return ContainerCostQuote{}, fmt.Errorf("container cost client is not configured")
	}
	if request == nil {
		return ContainerCostQuote{}, fmt.Errorf("container cost request is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	key := ContainerCostRequest{
		Cpu: request.Cpu, Memory: request.Memory,
		Gpu: strings.TrimSpace(request.Gpu), GpuCount: request.GpuCount,
	}
	if quote, ok := c.cached(key); ok {
		return quote, nil
	}
	if err := ctx.Err(); err != nil {
		return c.lastGood(key), err
	}

	result := c.refresh.DoChan(key.cacheKey(), func() (any, error) {
		if quote, ok := c.cached(key); ok {
			return containerCostResult{quote: quote}, nil
		}

		refreshCtx, cancel := context.WithTimeout(context.Background(), containerCostRequestTimeout)
		defer cancel()
		quote, refreshAt, err := c.fetch(refreshCtx, key)
		if err != nil {
			stale := c.rememberFailure(key)
			if stale.Valid {
				err = fmt.Errorf("refreshing container cost quote: %w", err)
			}
			return containerCostResult{quote: stale, err: err}, nil
		}

		c.mu.Lock()
		c.quotes[key] = containerCostCacheEntry{quote: quote, refreshAt: refreshAt}
		c.mu.Unlock()
		return containerCostResult{quote: quote}, nil
	})
	select {
	case <-ctx.Done():
		return c.lastGood(key), ctx.Err()
	case call := <-result:
		if call.Err != nil {
			return ContainerCostQuote{}, call.Err
		}
		value, ok := call.Val.(containerCostResult)
		if !ok {
			return ContainerCostQuote{}, fmt.Errorf("invalid container cost lookup result")
		}
		return value.quote, value.err
	}
}

// GetContainerCostPerMs keeps the scalar interface used by older callers.
func (c *ContainerCostClient) GetContainerCostPerMs(request *types.ContainerRequest) (float64, error) {
	quote, err := c.GetContainerCostQuote(context.Background(), request)
	if !quote.Valid || !quote.ValidUntil.IsZero() && !quote.ValidUntil.After(c.now()) {
		if err == nil {
			err = fmt.Errorf("container cost quote is unavailable or expired")
		}
		return 0, err
	}
	return quote.CostPerMs, err
}

func (c *ContainerCostClient) fetch(ctx context.Context, request ContainerCostRequest) (ContainerCostQuote, time.Time, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("encoding container cost request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("creating container cost request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, containerCostErrorLimit))
		if text := strings.TrimSpace(string(detail)); text != "" {
			return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: unexpected HTTP status %s: %q", resp.Status, text)
		}
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: unexpected HTTP status %s", resp.Status)
	}

	body, err = io.ReadAll(io.LimitReader(resp.Body, containerCostResponseLimit+1))
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("reading container cost quote: %w", err)
	}
	if len(body) > containerCostResponseLimit {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("container cost quote exceeds %d bytes", containerCostResponseLimit)
	}
	var response ContainerCostResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("decoding container cost quote: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("decoding container cost quote: %w", err)
	}

	cost, err := strconv.ParseFloat(response.CostPerMs, 64)
	if err != nil || math.IsNaN(cost) || math.IsInf(cost, 0) || cost < 0 {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("invalid cost_per_ms %q: must be finite and nonnegative", response.CostPerMs)
	}
	effectiveAt, err := optionalTime(response.EffectiveAt, "effective_at")
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, err
	}
	validUntil, err := optionalTime(response.ValidUntil, "valid_until")
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, err
	}

	now := c.now()
	if !validUntil.IsZero() && !validUntil.After(now) {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("invalid valid_until %q: must be in the future", response.ValidUntil)
	}
	if !effectiveAt.IsZero() && !validUntil.IsZero() && !validUntil.After(effectiveAt) {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("invalid quote interval: valid_until must follow effective_at")
	}
	refreshAt := now.Add(containerCostQuoteRefresh)
	if !validUntil.IsZero() && validUntil.Before(refreshAt) {
		refreshAt = validUntil
	}
	return ContainerCostQuote{
		CostPerMs: cost, PricingVersion: response.PricingVersion,
		EffectiveAt: effectiveAt, ValidUntil: validUntil, Valid: true,
	}, refreshAt, nil
}

func (c *ContainerCostClient) cached(key ContainerCostRequest) (ContainerCostQuote, bool) {
	c.mu.RLock()
	entry, ok := c.quotes[key]
	c.mu.RUnlock()
	if !ok {
		return ContainerCostQuote{}, false
	}
	now := c.now()
	if now.Before(entry.retryAt) || entry.quote.Valid && now.Before(entry.refreshAt) {
		return entry.quote, true
	}
	return ContainerCostQuote{}, false
}

func (c *ContainerCostClient) rememberFailure(key ContainerCostRequest) ContainerCostQuote {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.quotes[key]
	entry.retryAt = c.now().Add(containerCostRetryDelay)
	c.quotes[key] = entry
	return entry.quote
}

func (c *ContainerCostClient) lastGood(key ContainerCostRequest) ContainerCostQuote {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.quotes[key].quote
}

func (r ContainerCostRequest) cacheKey() string {
	return strconv.FormatInt(r.Cpu, 10) + "\x00" + strconv.FormatInt(r.Memory, 10) +
		"\x00" + r.Gpu + "\x00" + strconv.FormatUint(uint64(r.GpuCount), 10)
}

func optionalTime(value, name string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %s: %w", name, err)
	}
	return parsed, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values in response")
		}
		return err
	}
	return nil
}
