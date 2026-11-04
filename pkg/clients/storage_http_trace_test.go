package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/rs/zerolog"
)

func TestStorageHTTPTraceMeasuresContinueWaitWithoutPayloads(t *testing.T) {
	const payload = "private-body-not-for-traces"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Model an origin which does not acknowledge Expect until it reads the
		// body. The client's continue timeout must remain visible in the trace.
		time.Sleep(100 * time.Millisecond)
		data, err := io.ReadAll(r.Body)
		if err != nil || string(data) != payload {
			t.Errorf("body changed: %q, %v", data, err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	transport := &http.Transport{ExpectContinueTimeout: 40 * time.Millisecond}
	defer transport.CloseIdleConnections()
	client := storageTracingHTTPClient{base: &http.Client{Transport: transport}}
	trace := common.NewStartupTrace(true)
	ctx := common.ContextWithStartupTrace(context.Background(), trace)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/private-object-key", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Expect", "100-continue")
	request.Header.Set("Authorization", "private-auth-not-for-traces")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	var output bytes.Buffer
	logger := zerolog.New(&output)
	trace.Log(logger.Info())
	var event struct {
		Spans []struct {
			Name       string `json:"name"`
			DurationUS int64  `json:"duration_us"`
		} `json:"startup_spans"`
	}
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, span := range event.Spans {
		if span.Name == "storage.http.continue_to_first_body_read" {
			found = true
			if span.DurationUS < 30000 {
				t.Fatalf("continue wait was lost: %d us", span.DurationUS)
			}
		}
	}
	if !found {
		t.Fatal("missing continue/body boundary")
	}
	for _, secret := range []string{payload, "private-object-key", "private-auth-not-for-traces"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("trace exposed request data: %q", secret)
		}
	}
}

func TestStorageHTTPTracePreservesThrottlingAndSuccessfulRetryResults(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "private-object-throttle-message", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	trace := common.NewStartupTrace(true)
	ctx := common.ContextWithStartupTrace(context.Background(), trace)
	client := storageTracingHTTPClient{base: server.Client()}
	for range 2 {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/private-object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	var output bytes.Buffer
	logger := zerolog.New(&output)
	trace.Log(logger.Info())
	for _, result := range []string{"storage.http.status_503", "storage.http.status_200"} {
		if strings.Count(output.String(), `"name":"`+result+`"`) != 1 {
			t.Fatalf("missing independent attempt result %s: %s", result, &output)
		}
	}
	if strings.Count(output.String(), `"tcp_local_port":`) != 2 {
		t.Fatal("missing socket correlation for independent HTTP attempts")
	}
	for _, secret := range []string{server.URL, "private-object-key", "private-object-throttle-message"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("trace exposed request/response data: %q", secret)
		}
	}
}
