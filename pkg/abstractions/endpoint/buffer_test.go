package endpoint

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestBackendHTTPURLUsesPlaceholderHostForRouteAddresses(t *testing.T) {
	routeAddress := types.BackendRouteAddress("machine:worker:container:container:8001")

	got := backendHTTPURL("http", routeAddress, "health", "")
	if got != "http://backend.route/health" {
		t.Fatalf("backend route url = %q, want placeholder host url", got)
	}
}

func TestBackendHTTPURLPreservesDirectAddressAndQuery(t *testing.T) {
	got := backendHTTPURL("http", "127.0.0.1:8001", "/invoke", "a=b")
	if got != "http://127.0.0.1:8001/invoke?a=b" {
		t.Fatalf("direct backend url = %q", got)
	}
}

func TestBackendDialTimeoutCapsLongRequestTimeouts(t *testing.T) {
	if got := backendDialTimeout(175 * time.Second); got != backendConnectTimeout {
		t.Fatalf("backend dial timeout = %s, want %s", got, backendConnectTimeout)
	}
	if got := backendDialTimeout(3 * time.Second); got != 3*time.Second {
		t.Fatalf("backend dial timeout = %s, want caller timeout", got)
	}
}

func TestRouteHTTPClientReusesTransportForSameAddressAndTimeout(t *testing.T) {
	routeAddress := types.BackendRouteAddress("machine:worker:container:container:8001")
	rb := &RequestBuffer{maxTokens: 4}

	first := rb.getHttpClient(routeAddress, handleHttpRequestClientTimeout)
	second := rb.getHttpClient(routeAddress, handleHttpRequestClientTimeout)
	if first.Transport == nil || first.Transport != second.Transport {
		t.Fatal("expected route http clients with matching timeout to share transport")
	}

	readiness := rb.getHttpClient(routeAddress, checkAddressIsReadyTimeout)
	if readiness.Transport == first.Transport {
		t.Fatal("expected readiness and request clients to use separate transports")
	}
}

func TestPruneBackendTransportsRemovesStaleAddresses(t *testing.T) {
	activeAddress := types.BackendRouteAddress("machine:worker:active:container:8001")
	staleAddress := types.BackendRouteAddress("machine:worker:stale:container:8001")
	rb := &RequestBuffer{maxTokens: 1}

	rb.getHttpClient(activeAddress, handleHttpRequestClientTimeout)
	rb.getHttpClient(staleAddress, handleHttpRequestClientTimeout)

	rb.pruneBackendTransports([]container{{address: activeAddress}})

	if _, ok := rb.backendTransports.Load(backendTransportKey{address: activeAddress, timeout: handleHttpRequestClientTimeout}); !ok {
		t.Fatal("expected active address transport to remain")
	}
	if _, ok := rb.backendTransports.Load(backendTransportKey{address: staleAddress, timeout: handleHttpRequestClientTimeout}); ok {
		t.Fatal("expected stale address transport to be pruned")
	}
}

func TestForwardRequestTimesOutWhenNoBackendContainersAreReady(t *testing.T) {
	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(httpReq, rec)

	rb := &RequestBuffer{
		ctx:                 context.Background(),
		buffer:              abstractions.NewRingBuffer[*request](1),
		availableContainers: []container{},
		stubConfig: &types.StubConfigV1{
			TaskPolicy: types.TaskPolicy{Timeout: 1},
		},
	}

	if err := rb.ForwardRequest(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}

	var body map[string]interface{}
	if err := stdjson.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "Timed out waiting for a backend container" {
		t.Fatalf("error = %q, want timeout message", body["error"])
	}
}
