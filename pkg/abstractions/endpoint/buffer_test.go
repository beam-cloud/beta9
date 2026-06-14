package endpoint

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
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

func TestProcessRequestsWakesWhenBackendBecomesAvailable(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}

	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	reqCtx := e.NewContext(httpReq, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := &RequestBuffer{
		ctx:                 ctx,
		rdb:                 rdb,
		workspace:           &types.Workspace{Name: "workspace"},
		stubId:              "stub",
		stubConfig:          &types.StubConfigV1{TaskPolicy: types.TaskPolicy{Timeout: 30}},
		buffer:              abstractions.NewRingBuffer[*request](1),
		availableContainers: []container{},
		httpClient:          &http.Client{},
		workReady:           make(chan struct{}, 1),
	}
	req := &request{
		ctx:     reqCtx,
		task:    &EndpointTask{msg: &types.TaskMessage{TaskId: "task"}},
		done:    make(chan struct{}),
		started: make(chan struct{}),
	}
	if rb.buffer.Push(req, false) {
		t.Fatal("unexpected overwrite")
	}

	go rb.processRequests()
	rb.signalWork()

	time.Sleep(10 * time.Millisecond)
	if rb.buffer.Len() != 1 {
		t.Fatalf("buffer length = %d, want queued request while no backends are available", rb.buffer.Len())
	}

	rb.availableContainersLock.Lock()
	rb.availableContainers = []container{{id: "container-1", address: "127.0.0.1:8000"}}
	rb.availableContainersLock.Unlock()
	if err := rdb.Set(ctx, Keys.endpointRequestTokens("workspace", "stub", "container-1"), 1, 0).Err(); err != nil {
		t.Fatal(err)
	}
	rb.signalWork()

	waitForCondition(t, 50*time.Millisecond, func() bool {
		return rb.buffer.Len() == 0
	})
}

func TestReserveContainerSkipsSaturatedReplica(t *testing.T) {
	rb := &RequestBuffer{
		ctx:       context.Background(),
		workspace: &types.Workspace{Name: "workspace"},
		stubId:    "stub",
		stubConfig: &types.StubConfigV1{
			TaskPolicy: types.TaskPolicy{Timeout: 30},
		},
		maxTokens: 1,
		containerRequests: map[string]int{
			"full": 1,
		},
		availableContainers: []container{
			{id: "full", address: "127.0.0.1:8001"},
			{id: "open", address: "127.0.0.1:8002"},
		},
	}

	c, ok := rb.reserveContainer()
	if !ok {
		t.Fatal("expected available container")
	}
	if c.id != "open" {
		t.Fatalf("container id = %q, want open", c.id)
	}

	if got := rb.containerRequestCount("full"); got != 1 {
		t.Fatalf("full replica local reservations = %d, want 1", got)
	}
	if got := rb.containerRequestCount("open"); got != 1 {
		t.Fatalf("open replica local reservations = %d, want 1", got)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}

	if !fn() {
		t.Fatalf("condition not met within %s", timeout)
	}
}
