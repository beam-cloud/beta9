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
	containerCostRetryInitial   = 5 * time.Second
	containerCostRetryMax       = time.Minute
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

type containerCostCacheEntry struct {
	quote        ContainerCostQuote
	refreshAt    time.Time
	retryAt      time.Time
	failures     uint8
	refreshError error
}

type containerCostRefreshOwner struct {
	marker byte
}

type containerCostLookupResult struct {
	quote        ContainerCostQuote
	refreshError error
	owner        *containerCostRefreshOwner
}

type ContainerCostClient struct {
	client   *http.Client
	endpoint string
	token    string

	mu           sync.RWMutex
	quotes       map[ContainerCostRequest]containerCostCacheEntry
	refreshGroup singleflight.Group
	now          func() time.Time
}

type ContainerCostRequest struct {
	Cpu      int64  `json:"cpu"`
	Memory   int64  `json:"memory"`
	Gpu      string `json:"gpu"`
	GpuCount uint32 `json:"gpu_count"`
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

// GetContainerCostQuote returns a resource-keyed cached quote. Quotes are
// refreshed every five minutes, or sooner when valid_until says they expire.
// If a refresh fails, the last validated quote is returned together with the
// refresh error so callers can continue metering and surface the stale state.
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
		Cpu:      request.Cpu,
		Memory:   request.Memory,
		Gpu:      strings.TrimSpace(request.Gpu),
		GpuCount: request.GpuCount,
	}
	if quote, ok := c.cachedResult(key, c.now()); ok {
		return quote, nil
	}

	owner := &containerCostRefreshOwner{marker: 1}
	result, err, _ := c.refreshGroup.Do(key.singleflightKey(), func() (interface{}, error) {
		now := c.now()
		if quote, ok := c.cachedResult(key, now); ok {
			return containerCostLookupResult{quote: quote}, nil
		}

		quote, refreshAt, fetchErr := c.fetchQuote(ctx, key)
		if fetchErr != nil {
			quote, refreshErr := c.cacheFailure(key, c.now(), fetchErr)
			return containerCostLookupResult{quote: quote, refreshError: refreshErr, owner: owner}, nil
		}

		c.mu.Lock()
		c.quotes[key] = containerCostCacheEntry{quote: quote, refreshAt: refreshAt}
		c.mu.Unlock()
		return containerCostLookupResult{quote: quote}, nil
	})
	if err != nil {
		return ContainerCostQuote{}, err
	}

	lookup, ok := result.(containerCostLookupResult)
	if !ok {
		return ContainerCostQuote{}, fmt.Errorf("invalid container cost lookup result")
	}
	if lookup.owner == owner {
		return lookup.quote, lookup.refreshError
	}
	return lookup.quote, nil
}

// GetContainerCostPerMs is kept for callers that only need the legacy scalar
// response. New metering code should retain the quote metadata as well.
func (c *ContainerCostClient) GetContainerCostPerMs(request *types.ContainerRequest) (float64, error) {
	quote, err := c.GetContainerCostQuote(context.Background(), request)
	if !quote.Valid {
		if err == nil {
			err = fmt.Errorf("container cost quote is unavailable")
		}
		return 0, err
	}
	return quote.CostPerMs, err
}

func (c *ContainerCostClient) fetchQuote(ctx context.Context, request ContainerCostRequest) (ContainerCostQuote, time.Time, error) {
	var requestBody bytes.Buffer
	if err := json.NewEncoder(&requestBody).Encode(request); err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("encoding container cost request: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, containerCostRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.endpoint, &requestBody)
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("creating container cost request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.client.Do(req)
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, containerCostErrorLimit))
		detailText := strings.TrimSpace(string(detail))
		if detailText != "" {
			return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: unexpected HTTP status %s: %q", resp.Status, detailText)
		}
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("requesting container cost quote: unexpected HTTP status %s", resp.Status)
	}

	var response ContainerCostResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, containerCostResponseLimit))
	if err := decoder.Decode(&response); err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("decoding container cost quote: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("decoding container cost quote: %w", err)
	}

	costPerMs, err := strconv.ParseFloat(response.CostPerMs, 64)
	if err != nil {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("parsing cost_per_ms: %w", err)
	}
	if math.IsNaN(costPerMs) || math.IsInf(costPerMs, 0) || costPerMs < 0 {
		return ContainerCostQuote{}, time.Time{}, fmt.Errorf("invalid cost_per_ms %q: must be finite and nonnegative", response.CostPerMs)
	}

	quote := ContainerCostQuote{
		CostPerMs:      costPerMs,
		PricingVersion: response.PricingVersion,
		Valid:          true,
	}
	validatedAt := c.now()
	refreshAt := validatedAt.Add(containerCostQuoteRefresh)
	if response.EffectiveAt != "" {
		effectiveAt, err := time.Parse(time.RFC3339Nano, response.EffectiveAt)
		if err != nil {
			return ContainerCostQuote{}, time.Time{}, fmt.Errorf("parsing effective_at: %w", err)
		}
		quote.EffectiveAt = effectiveAt
	}
	if response.ValidUntil != "" {
		validUntil, err := time.Parse(time.RFC3339Nano, response.ValidUntil)
		if err != nil {
			return ContainerCostQuote{}, time.Time{}, fmt.Errorf("parsing valid_until: %w", err)
		}
		if !validUntil.After(validatedAt) {
			return ContainerCostQuote{}, time.Time{}, fmt.Errorf("invalid valid_until %q: must be in the future", response.ValidUntil)
		}
		quote.ValidUntil = validUntil
		if validUntil.Before(refreshAt) {
			refreshAt = validUntil
		}
	}

	return quote, refreshAt, nil
}

func (c *ContainerCostClient) cachedResult(key ContainerCostRequest, now time.Time) (ContainerCostQuote, bool) {
	c.mu.RLock()
	entry, ok := c.quotes[key]
	c.mu.RUnlock()
	if !ok {
		return ContainerCostQuote{}, false
	}
	if entry.refreshError != nil && now.Before(entry.retryAt) {
		return entry.quote, true
	}
	if entry.quote.Valid && now.Before(entry.refreshAt) {
		return entry.quote, true
	}
	return ContainerCostQuote{}, false
}

func (c *ContainerCostClient) cacheFailure(key ContainerCostRequest, failedAt time.Time, refreshErr error) (ContainerCostQuote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.quotes[key]
	if entry.failures < ^uint8(0) {
		entry.failures++
	}
	if entry.quote.Valid {
		refreshErr = fmt.Errorf("refreshing container cost quote: %w", refreshErr)
	}
	entry.refreshError = refreshErr
	entry.retryAt = failedAt.Add(containerCostRetryDelay(entry.failures))
	c.quotes[key] = entry
	return entry.quote, refreshErr
}

func containerCostRetryDelay(failures uint8) time.Duration {
	delay := containerCostRetryInitial
	for attempt := uint8(1); attempt < failures && delay < containerCostRetryMax; attempt++ {
		delay *= 2
		if delay > containerCostRetryMax {
			return containerCostRetryMax
		}
	}
	return delay
}

func (r ContainerCostRequest) singleflightKey() string {
	return strconv.FormatInt(r.Cpu, 10) + "\x00" +
		strconv.FormatInt(r.Memory, 10) + "\x00" + r.Gpu + "\x00" +
		strconv.FormatUint(uint64(r.GpuCount), 10)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values in response")
		}
		return err
	}
	return nil
}
