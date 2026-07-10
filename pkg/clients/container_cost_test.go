package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestContainerCostClientCachesByResourcesAndFallsBackToLastGoodQuote(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	var calls int
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("authorization = %q, want bearer token", r.Header.Get("Authorization"))
		}
		if fail {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(ContainerCostResponse{
			CostPerMs:      "0.0000017",
			PricingVersion: "cpu-only-202607",
			ValidUntil:     now.Add(time.Minute).Format(time.RFC3339Nano),
		})
	}))
	t.Cleanup(server.Close)

	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
	client.now = func() time.Time { return now }
	request := &types.ContainerRequest{ContainerId: "container-1", Cpu: 1_000, Memory: 1_024}

	quote, err := client.GetContainerCostQuote(context.Background(), request)
	if err != nil {
		t.Fatalf("initial quote: %v", err)
	}
	if !quote.Valid || quote.CostPerMs != 0.0000017 || quote.PricingVersion != "cpu-only-202607" {
		t.Fatalf("initial quote = %+v", quote)
	}

	// Container identity is deliberately absent from the cache key.
	request.ContainerId = "container-2"
	if _, err := client.GetContainerCostQuote(context.Background(), request); err != nil {
		t.Fatalf("cached quote: %v", err)
	}
	if calls != 1 {
		t.Fatalf("quote calls = %d, want one shared resource-keyed call", calls)
	}

	now = now.Add(2 * time.Minute)
	fail = true
	surfacedErrors := 0
	stale, err := client.GetContainerCostQuote(context.Background(), request)
	if err == nil {
		t.Fatal("stale refresh error = nil")
	}
	surfacedErrors++
	if !stale.Valid || stale.CostPerMs != quote.CostPerMs || stale.PricingVersion != quote.PricingVersion {
		t.Fatalf("stale quote = %+v, want last good %+v", stale, quote)
	}
	if calls != 2 {
		t.Fatalf("quote calls = %d, want refresh after valid_until", calls)
	}
	if cached, err := client.GetContainerCostQuote(context.Background(), request); err != nil || !cached.Valid {
		t.Fatalf("cached stale quote = %+v, err = %v; want quiet last-good hit", cached, err)
	}
	if calls != 2 {
		t.Fatalf("quote calls = %d, want failed-refresh backoff", calls)
	}

	now = now.Add(containerCostRetryInitial - time.Millisecond)
	if cached, err := client.GetContainerCostQuote(context.Background(), request); err != nil || !cached.Valid {
		t.Fatalf("backed-off stale quote = %+v, err = %v; want quiet hit", cached, err)
	}
	if calls != 2 {
		t.Fatalf("quote calls = %d before retry deadline, want 2", calls)
	}

	now = now.Add(time.Millisecond)
	if _, err := client.GetContainerCostQuote(context.Background(), request); err == nil {
		t.Fatal("second stale refresh error = nil")
	}
	surfacedErrors++
	if calls != 3 {
		t.Fatalf("quote calls = %d at retry deadline, want 3", calls)
	}

	fail = false
	now = now.Add(2 * containerCostRetryInitial)
	recovered, err := client.GetContainerCostQuote(context.Background(), request)
	if err != nil || !recovered.Valid {
		t.Fatalf("recovered quote = %+v, err = %v", recovered, err)
	}
	if calls != 4 {
		t.Fatalf("quote calls = %d after bounded exponential backoff, want 4", calls)
	}
	if surfacedErrors != 2 {
		t.Fatalf("surfaced refresh errors = %d, want one per failed HTTP refresh", surfacedErrors)
	}
}

func TestContainerCostClientRetriesWhenNoQuoteWasCached(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"cost_per_ms":"0.25","pricing_version":"legacy"}`))
	}))
	t.Cleanup(server.Close)

	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
	client.now = func() time.Time { return now }
	request := &types.ContainerRequest{Cpu: 1_000, Memory: 1_024}
	if quote, err := client.GetContainerCostQuote(context.Background(), request); err == nil || quote.Valid {
		t.Fatalf("first quote = %+v, err = %v; want missing quote", quote, err)
	} else if !strings.Contains(err.Error(), "try again") {
		t.Fatalf("first quote error = %q, want bounded server detail", err)
	}
	quote, err := client.GetContainerCostQuote(context.Background(), request)
	if err != nil || quote.Valid {
		t.Fatalf("negative cached quote = %+v, err = %v; want quiet unavailable hit", quote, err)
	}
	if calls != 1 {
		t.Fatalf("quote calls = %d during negative-cache window, want 1", calls)
	}
	if _, err := client.GetContainerCostPerMs(request); err == nil {
		t.Fatal("legacy scalar lookup error = nil for unavailable cached quote")
	}
	if calls != 1 {
		t.Fatalf("quote calls = %d after legacy cached lookup, want 1", calls)
	}

	now = now.Add(containerCostRetryInitial)
	quote, err = client.GetContainerCostQuote(context.Background(), request)
	if err != nil || !quote.Valid || quote.CostPerMs != 0.25 {
		t.Fatalf("retried quote = %+v, err = %v", quote, err)
	}
	if calls != 2 {
		t.Fatalf("quote calls = %d, want retry on next lookup", calls)
	}
}

func TestContainerCostClientCoalescesConcurrentRefreshes(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"cost_per_ms":"0.125"}`))
	}))
	t.Cleanup(server.Close)

	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
	request := &types.ContainerRequest{Cpu: 2_000, Memory: 4_096, Gpu: " T4 ", GpuCount: 1}

	const goroutines = 12
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			quote, err := client.GetContainerCostQuote(context.Background(), request)
			if err == nil && (!quote.Valid || quote.CostPerMs != 0.125) {
				err = fmt.Errorf("unexpected quote %+v", quote)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("quote calls = %d, want one coalesced refresh", got)
	}
	request.Gpu = "T4"
	if _, err := client.GetContainerCostQuote(context.Background(), request); err != nil {
		t.Fatalf("trimmed GPU cache lookup: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("quote calls after normalized GPU lookup = %d, want one", got)
	}
}

func TestContainerCostClientSurfacesRefreshFailureOnlyToOwner(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
	client.now = func() time.Time { return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC) }
	request := &types.ContainerRequest{Cpu: 1_000, Memory: 1_024}

	const goroutines = 32
	start := make(chan struct{})
	errorsSeen := make(chan bool, goroutines)
	var ready, done sync.WaitGroup
	ready.Add(goroutines)
	done.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			quote, err := client.GetContainerCostQuote(context.Background(), request)
			if quote.Valid {
				t.Errorf("failed lookup returned valid quote %+v", quote)
			}
			errorsSeen <- err != nil
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errorsSeen)

	var surfaced int
	for seen := range errorsSeen {
		if seen {
			surfaced++
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("HTTP refreshes = %d, want one coalesced failed attempt", got)
	}
	if surfaced != 1 {
		t.Fatalf("surfaced refresh errors = %d, want only the refresh owner", surfaced)
	}
}

func TestContainerCostClientRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "redirect", status: http.StatusFound, body: `{"cost_per_ms":"1"}`},
		{name: "not a number", status: http.StatusOK, body: `{"cost_per_ms":"NaN"}`},
		{name: "infinity", status: http.StatusOK, body: `{"cost_per_ms":"+Inf"}`},
		{name: "negative", status: http.StatusOK, body: `{"cost_per_ms":"-0.1"}`},
		{name: "invalid expiry", status: http.StatusOK, body: `{"cost_per_ms":"1","valid_until":"tomorrow"}`},
		{name: "trailing JSON", status: http.StatusOK, body: `{"cost_per_ms":"1"} {"extra":true}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)

			client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
			quote, err := client.GetContainerCostQuote(context.Background(), &types.ContainerRequest{Cpu: 1_000})
			if err == nil || quote.Valid {
				t.Fatalf("quote = %+v, err = %v; want rejected response", quote, err)
			}
		})
	}
}

func TestContainerCostClientUsesBoundedHTTPTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	t.Cleanup(server.Close)

	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test-token"})
	client.client.Timeout = 20 * time.Millisecond
	started := time.Now()
	quote, err := client.GetContainerCostQuote(context.Background(), &types.ContainerRequest{Cpu: 1_000})
	if err == nil || quote.Valid {
		t.Fatalf("quote = %+v, err = %v; want timeout", quote, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timeout took %v, want bounded request", elapsed)
	}
}

func TestContainerCostRetryDelayIsBounded(t *testing.T) {
	if got := containerCostRetryDelay(1); got != containerCostRetryInitial {
		t.Fatalf("first retry delay = %v, want %v", got, containerCostRetryInitial)
	}
	if got := containerCostRetryDelay(^uint8(0)); got != containerCostRetryMax {
		t.Fatalf("maximum retry delay = %v, want %v", got, containerCostRetryMax)
	}
}
