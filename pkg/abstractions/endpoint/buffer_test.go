package endpoint

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
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

func TestIsASGIHealthRequest(t *testing.T) {
	e := echo.New()
	ctx := e.NewContext(httptest.NewRequest(http.MethodGet, "/health", nil), httptest.NewRecorder())
	ctx.SetParamNames("subPath")
	ctx.SetParamValues("health")
	if !isASGIHealthRequest(ctx) {
		t.Fatal("expected /health to be treated as an ASGI health request")
	}

	ctx.SetParamValues("healthz")
	if isASGIHealthRequest(ctx) {
		t.Fatal("expected non-health subpath to use normal ASGI task forwarding")
	}
}

func TestForwardHealthRequestReturnsOKWhenNoBackendIsReady(t *testing.T) {
	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(httpReq, rec)
	ctx.SetParamNames("subPath")
	ctx.SetParamValues("health")

	rb := &RequestBuffer{}
	if err := rb.ForwardHealthRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestForwardHealthRequestProxiesWithoutTaskHeader(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-TASK-ID"); got != "" {
			t.Errorf("forwarded X-TASK-ID = %q, want empty", got)
			http.Error(w, "unexpected task header", http.StatusInternalServerError)
			return
		}
		if got := r.URL.String(); got != "/health?probe=1" {
			t.Errorf("backend URL = %q, want /health?probe=1", got)
			http.Error(w, "unexpected url", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Backend-Health", "ok")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}

	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodGet, "/health?probe=1", nil)
	httpReq.Header.Set("X-TASK-ID", "client-supplied-task")
	rec := httptest.NewRecorder()
	ctx := e.NewContext(httpReq, rec)
	ctx.SetParamNames("subPath")
	ctx.SetParamValues("health")

	rb := &RequestBuffer{
		ctx:        context.Background(),
		httpClient: backend.Client(),
		availableContainers: []container{{
			id:      "container-1",
			address: backendURL.Host,
		}},
	}
	if err := rb.ForwardHealthRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("X-Backend-Health"); got != "ok" {
		t.Fatalf("response header = %q, want ok", got)
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
		containerRepo:       repository.NewContainerRedisRepositoryForTest(rdb),
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

func TestEnqueueRequestFailsOverwrittenRequest(t *testing.T) {
	e := echo.New()
	firstReq := httptest.NewRequest(http.MethodPost, "/", nil)
	firstRec := httptest.NewRecorder()
	firstCtx := e.NewContext(firstReq, firstRec)
	firstDone := make(chan struct{})

	secondReq := httptest.NewRequest(http.MethodPost, "/", nil)
	secondRec := httptest.NewRecorder()
	secondCtx := e.NewContext(secondReq, secondRec)
	secondQueuedRequest := &request{ctx: secondCtx, done: make(chan struct{})}

	rb := &RequestBuffer{
		buffer: abstractions.NewRingBuffer[*request](1),
	}
	rb.enqueueRequest(&request{ctx: firstCtx, done: firstDone}, false)
	rb.enqueueRequest(secondQueuedRequest, false)

	select {
	case <-firstDone:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("overwritten request was not released")
	}
	if firstRec.Code != http.StatusTooManyRequests {
		t.Fatalf("overwritten response status = %d, want %d", firstRec.Code, http.StatusTooManyRequests)
	}

	got, ok := rb.buffer.Pop()
	if !ok || got != secondQueuedRequest {
		t.Fatal("expected newest request to remain queued")
	}
}

func TestReserveContainerSkipsSaturatedReplica(t *testing.T) {
	rdb := newEndpointBufferTestRedis(t)
	rb := &RequestBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		containerRepo: repository.NewContainerRedisRepositoryForTest(rdb),
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		stubConfig: &types.StubConfigV1{
			TaskPolicy: types.TaskPolicy{Timeout: 30},
		},
		maxTokens: 1,
		availableContainers: []container{
			{id: "full", address: "127.0.0.1:8001"},
			{id: "open", address: "127.0.0.1:8002"},
		},
	}
	if err := rdb.Set(context.Background(), Keys.endpointRequestTokens("workspace", "stub", "full"), 0, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), Keys.endpointRequestTokens("workspace", "stub", "open"), 1, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	c, ok := rb.reserveContainer()
	if !ok {
		t.Fatal("expected available container")
	}
	if c.id != "open" {
		t.Fatalf("container id = %q, want open", c.id)
	}

	if got := redisInt64(t, rdb, Keys.endpointRequestTokens("workspace", "stub", "open")); got != 0 {
		t.Fatalf("open replica tokens = %d, want 0", got)
	}
}

func TestReserveContainerUsesSharedRequestTokensAcrossBuffers(t *testing.T) {
	rdb := newEndpointBufferTestRedis(t)
	rb1 := newEndpointRequestTokenTestBuffer(rdb, 1)
	rb2 := newEndpointRequestTokenTestBuffer(rdb, 1)

	if _, ok := rb1.reserveContainer(); !ok {
		t.Fatal("expected first buffer to reserve the shared request token")
	}
	if _, ok := rb2.reserveContainer(); ok {
		t.Fatal("expected second buffer to be denied after shared request token was consumed")
	}

	key := Keys.endpointRequestTokens("workspace", "stub", "container-1")
	if got := redisInt64(t, rdb, key); got != 0 {
		t.Fatalf("request tokens = %d, want 0 after first reservation", got)
	}

	if err := rb1.releaseRequestToken("container-1", "task-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("request tokens after release = %d, want 1", got)
	}

	if _, ok := rb2.reserveContainer(); !ok {
		t.Fatal("expected second buffer to reserve after token release")
	}
}

func TestReleaseRequestTokenIsIdempotentAcrossBuffers(t *testing.T) {
	rdb := newEndpointBufferTestRedis(t)
	rb1 := newEndpointRequestTokenTestBuffer(rdb, 2)
	rb2 := newEndpointRequestTokenTestBuffer(rdb, 2)

	if _, ok := rb1.reserveContainer(); !ok {
		t.Fatal("expected first buffer to reserve")
	}
	if _, ok := rb2.reserveContainer(); !ok {
		t.Fatal("expected second buffer to reserve")
	}

	key := Keys.endpointRequestTokens("workspace", "stub", "container-1")
	if got := redisInt64(t, rdb, key); got != 0 {
		t.Fatalf("request tokens = %d, want 0 after both reservations", got)
	}

	if err := rb1.releaseRequestToken("container-1", "task-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("request tokens after first release = %d, want 1", got)
	}

	if err := rb1.releaseRequestToken("container-1", "task-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("request tokens after duplicate release = %d, want other buffer token preserved", got)
	}

	if err := rb2.releaseRequestToken("container-1", "task-2"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 2 {
		t.Fatalf("request tokens after second release = %d, want full bucket", got)
	}
}

func TestRefreshRequestTokenTTLExtendsTokenKey(t *testing.T) {
	rdb := newEndpointBufferTestRedis(t)
	rb := newEndpointRequestTokenTestBuffer(rdb, 1)
	key := Keys.endpointRequestTokens("workspace", "stub", "container-1")

	if err := rdb.Set(context.Background(), key, 0, time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	rb.refreshRequestTokenTTL("container-1")

	ttl, err := rdb.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < rb.requestTokenTTL()-time.Second {
		t.Fatalf("ttl = %s, want refreshed close to %s", ttl, rb.requestTokenTTL())
	}
}

func newEndpointBufferTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}
	return rdb
}

func newEndpointRequestTokenTestBuffer(rdb *common.RedisClient, maxTokens int) *RequestBuffer {
	return &RequestBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		containerRepo: repository.NewContainerRedisRepositoryForTest(rdb),
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		stubConfig:    &types.StubConfigV1{TaskPolicy: types.TaskPolicy{Timeout: 30}},
		maxTokens:     maxTokens,
		availableContainers: []container{
			{id: "container-1", address: "127.0.0.1:8001"},
		},
	}
}

func redisInt64(t *testing.T, rdb *common.RedisClient, key string) int64 {
	t.Helper()

	got, err := rdb.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatal(err)
	}
	return got
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
