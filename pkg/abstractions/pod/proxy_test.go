package pod

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

func TestPodBackendURLUsesPlaceholderHostForRouteAddresses(t *testing.T) {
	routeAddress := types.BackendRouteAddress("machine:worker:container:container:8001")

	got := podBackendURL("ws", routeAddress, "/socket", "token=1")
	if got != "ws://backend.route/socket?token=1" {
		t.Fatalf("backend route url = %q, want placeholder host url", got)
	}
}

func TestPodBackendURLPreservesDirectAddress(t *testing.T) {
	got := podBackendURL("http", "127.0.0.1:8001", "metrics", "")
	if got != "http://127.0.0.1:8001/metrics" {
		t.Fatalf("direct backend url = %q", got)
	}
}

func TestProcessBufferWakesWhenBackendBecomesAvailable(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

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
	httpReq := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	reqCtx := e.NewContext(httpReq, rec)
	reqCtx.SetParamNames("port")
	reqCtx.SetParamValues("8000")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pb := &PodProxyBuffer{
		ctx:                 ctx,
		rdb:                 rdb,
		workspace:           &types.Workspace{Name: "workspace"},
		stubId:              "stub",
		stubConfig:          &types.StubConfigV1{},
		buffer:              abstractions.NewRingBuffer[*connection](1),
		availableContainers: []container{},
		httpClient:          &http.Client{},
		workReady:           make(chan struct{}, 1),
	}
	if pb.buffer.Push(&connection{ctx: reqCtx, done: make(chan struct{})}, false) {
		t.Fatal("unexpected overwrite")
	}

	go pb.processBuffer()
	pb.signalWork()

	time.Sleep(10 * time.Millisecond)
	if pb.buffer.Len() != 1 {
		t.Fatalf("buffer length = %d, want queued connection while no backends are available", pb.buffer.Len())
	}

	pb.availableContainersLock.Lock()
	pb.availableContainers = []container{{
		id:              "container-1",
		addressMap:      map[int32]string{8000: backend.Listener.Addr().String()},
		readyAddressMap: map[int32]string{8000: backend.Listener.Addr().String()},
	}}
	pb.availableContainersLock.Unlock()
	pb.signalWork()

	waitForCondition(t, 50*time.Millisecond, func() bool {
		return pb.buffer.Len() == 0
	})
}

func TestForwardRequestWakesContainerDiscovery(t *testing.T) {
	e := echo.New()
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestCtx), httptest.NewRecorder())
	ctx.SetParamNames("port")
	ctx.SetParamValues("8000")

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		stubConfig:    &types.StubConfigV1{},
		buffer:        abstractions.NewRingBuffer[*connection](1),
		workReady:     make(chan struct{}, 1),
		discoverReady: make(chan struct{}, 1),
	}

	done := make(chan error, 1)
	go func() { done <- pb.ForwardRequest(ctx) }()

	select {
	case <-pb.discoverReady:
	case <-time.After(time.Second):
		t.Fatal("queued request did not wake container discovery")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDeletePodInstancePreservesReplacement(t *testing.T) {
	service := &GenericPodService{podInstances: common.NewSafeMap[*podInstance]()}
	oldInstance := &podInstance{}
	replacement := &podInstance{}
	service.podInstances.Set("stub", replacement)

	service.deletePodInstance("stub", oldInstance)
	if current, exists := service.podInstances.Get("stub"); !exists || current != replacement {
		t.Fatal("stale instance cleanup deleted its replacement")
	}

	service.deletePodInstance("stub", replacement)
	if _, exists := service.podInstances.Get("stub"); exists {
		t.Fatal("current instance was not deleted")
	}
}

func TestDrainDoesNotRejectClaimedConnection(t *testing.T) {
	e := echo.New()
	recorder := httptest.NewRecorder()
	conn := &connection{
		ctx:  e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), recorder),
		done: make(chan struct{}),
	}
	if !conn.claim() {
		t.Fatal("connection was not claimed")
	}

	pb := &PodProxyBuffer{}
	if pb.failQueuedConnection(conn, http.StatusServiceUnavailable, "Service is draining") {
		t.Fatal("claimed connection was rejected as queued")
	}
	if recorder.Body.Len() != 0 {
		t.Fatal("drain wrote to an active response")
	}
}

func TestEnqueueConnectionFailsOverwrittenHTTPConnection(t *testing.T) {
	e := echo.New()
	firstReq := httptest.NewRequest(http.MethodGet, "/", nil)
	firstRec := httptest.NewRecorder()
	firstCtx := e.NewContext(firstReq, firstRec)
	firstDone := make(chan struct{})

	secondReq := httptest.NewRequest(http.MethodGet, "/", nil)
	secondRec := httptest.NewRecorder()
	secondCtx := e.NewContext(secondReq, secondRec)
	secondConn := &connection{ctx: secondCtx, done: make(chan struct{})}

	pb := &PodProxyBuffer{
		ctx:    context.Background(),
		buffer: abstractions.NewRingBuffer[*connection](1),
	}
	pb.enqueueConnection(&connection{ctx: firstCtx, done: firstDone}, false)
	pb.enqueueConnection(secondConn, false)

	select {
	case <-firstDone:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("overwritten connection was not released")
	}
	if firstRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("overwritten response status = %d, want %d", firstRec.Code, http.StatusServiceUnavailable)
	}

	got, ok := pb.buffer.Pop()
	if !ok || got != secondConn {
		t.Fatal("expected newest connection to remain queued")
	}
}

func TestForwardRequestProxiesReadyBackendWithoutQueueing(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/health?probe=1" {
			t.Fatalf("backend URL = %q, want /health?probe=1", r.URL.String())
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	backendURL := backend.URL[len("http://"):]
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/health?probe=1", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port", "subPath")
	ctx.SetParamValues("8000", "health")

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pb := &PodProxyBuffer{
		ctx:        parentCtx,
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		stubConfig: &types.StubConfigV1{},
		buffer:     abstractions.NewRingBuffer[*connection](1),
		availableContainers: []container{{
			id:              "container-1",
			addressMap:      map[int32]string{8000: backendURL},
			readyAddressMap: map[int32]string{8000: backendURL},
		}},
	}

	if err := pb.ForwardRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if pb.buffer.Len() != 0 {
		t.Fatalf("buffer length = %d, want direct proxy without queueing", pb.buffer.Len())
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want released", got)
	}
	if got := pb.containerConnectionCount("container-1"); got != 0 {
		t.Fatalf("container connections = %d, want released", got)
	}
}

func TestForwardRequestRequeuesClosingRouteAndPreservesBody(t *testing.T) {
	staleRouteID := "stale-route"
	receivedBody := make(chan string, 1)
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read replacement request body: %v", err)
		}
		receivedBody <- string(body)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(replacement.Close)

	e := echo.New()
	body := &closeSensitiveBody{reader: bytes.NewReader([]byte(`{"request":"preserved"}`))}
	req := httptest.NewRequest(http.MethodPost, "/", body)
	rec := httptest.NewRecorder()
	requestCtx := e.NewContext(req, rec)
	requestCtx.SetParamNames("port")
	requestCtx.SetParamValues("8000")

	pb := newBackendRetryTestBuffer(t, staleRouteID)
	replacementStarts := 0
	pb.onBackendUnavailable = func() error {
		replacementStarts++
		address := replacement.Listener.Addr().String()
		pb.availableContainersLock.Lock()
		pb.availableContainers = []container{{
			id:              "replacement-container",
			addressMap:      map[int32]string{8000: address},
			readyAddressMap: map[int32]string{8000: address},
		}}
		pb.availableContainersLock.Unlock()
		return nil
	}
	if err := pb.ForwardRequest(requestCtx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if replacementStarts != 1 {
		t.Fatalf("replacement starts = %d, want 1", replacementStarts)
	}
	select {
	case got := <-receivedBody:
		if got != `{"request":"preserved"}` {
			t.Fatalf("replacement body = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not receive request")
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want 0", got)
	}
}

func TestForwardRequestRetriesBackendDialOnlyOnce(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	requestCtx := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)
	requestCtx.SetParamNames("port")
	requestCtx.SetParamValues("8000")

	pb := newBackendRetryTestBuffer(t, "stale-route", "broken-replacement-route")
	replacementStarts := 0
	pb.onBackendUnavailable = func() error {
		replacementStarts++
		address := types.BackendRouteAddress("broken-replacement-route")
		pb.availableContainersLock.Lock()
		pb.availableContainers = []container{{
			id:              "broken-replacement",
			addressMap:      map[int32]string{8000: address},
			readyAddressMap: map[int32]string{8000: address},
		}}
		pb.availableContainersLock.Unlock()
		return nil
	}
	if err := pb.ForwardRequest(requestCtx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
	if replacementStarts != 1 {
		t.Fatalf("replacement starts = %d, want 1", replacementStarts)
	}
}

func newBackendRetryTestBuffer(t *testing.T, routeIDs ...string) *PodProxyBuffer {
	t.Helper()
	if len(routeIDs) == 0 {
		t.Fatal("at least one backend route is required")
	}

	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	for _, routeID := range routeIDs {
		if err := repo.SetBackendRoute(context.Background(), types.BackendRoute{
			RouteID: routeID,
			State:   types.BackendRouteStateClosing,
		}); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	staleAddress := types.BackendRouteAddress(routeIDs[0])
	pb := &PodProxyBuffer{
		ctx:           ctx,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		stubConfig:    &types.StubConfigV1{},
		containerRepo: repo,
		buffer:        abstractions.NewRingBuffer[*connection](2),
		workReady:     make(chan struct{}, 1),
		discoverReady: make(chan struct{}, 1),
		availableContainers: []container{{
			id:              "stopping-container",
			addressMap:      map[int32]string{8000: staleAddress},
			readyAddressMap: map[int32]string{8000: staleAddress},
		}},
	}
	go pb.processBuffer()
	return pb
}

type closeSensitiveBody struct {
	reader *bytes.Reader
	closed atomic.Bool
}

func (b *closeSensitiveBody) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, errors.New("request body was closed before retry")
	}
	return b.reader.Read(p)
}

func (b *closeSensitiveBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestForwardRequestRejectsImmediatelyWhenDraining(t *testing.T) {
	drainCtx, cancel := context.WithCancel(context.Background())
	cancel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port")
	ctx.SetParamValues("8000")

	pb := &PodProxyBuffer{
		ctx:      context.Background(),
		drainCtx: drainCtx,
		stubId:   "stub",
	}

	if err := pb.ForwardRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get(echo.HeaderConnection); got != "close" {
		t.Fatalf("Connection header = %q, want close", got)
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want drain rejection before accounting", got)
	}
}

func TestForwardRequestFailsQueuedRequestWhenDraining(t *testing.T) {
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()

	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port")
	ctx.SetParamValues("8000")

	pb := &PodProxyBuffer{
		ctx:        parentCtx,
		drainCtx:   drainCtx,
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		stubConfig: &types.StubConfigV1{},
		buffer:     abstractions.NewRingBuffer[*connection](1),
		workReady:  make(chan struct{}, 1),
	}

	go pb.processBuffer()

	done := make(chan error, 1)
	go func() {
		done <- pb.ForwardRequest(ctx)
	}()

	waitForCondition(t, 50*time.Millisecond, func() bool {
		return pb.buffer.Len() == 1
	})
	cancelDrain()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("queued request was not released during drain")
	}

	waitForCondition(t, 50*time.Millisecond, func() bool {
		return pb.buffer.Len() == 0
	})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get(echo.HeaderConnection); got != "close" {
		t.Fatalf("Connection header = %q, want close", got)
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want released after drain", got)
	}
}

func TestForwardContainerRequestProxiesPinnedContainerWithoutQueueing(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/health?probe=1" {
			t.Fatalf("backend URL = %q, want /health?probe=1", r.URL.String())
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(backend.Close)

	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerId := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345"
	if err := repo.SetContainerAddressMap(containerId, map[int32]string{
		8765: backend.URL[len("http://"):],
	}); err != nil {
		t.Fatal(err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/health?probe=1", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port", "subPath")
	ctx.SetParamValues("8765", "health")

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "e9c29586-c465-4a67-9c9b-25293d1ce77b",
		stubConfig:    &types.StubConfigV1{},
		containerRepo: repo,
	}

	if err := pb.ForwardContainerRequest(ctx, containerId); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want released", got)
	}
	if got := pb.containerConnectionCount(containerId); got != 0 {
		t.Fatalf("container connections = %d, want released", got)
	}
}

func TestForwardContainerRequestRejectsImmediatelyWhenDraining(t *testing.T) {
	drainCtx, cancel := context.WithCancel(context.Background())
	cancel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port")
	ctx.SetParamValues("8765")

	pb := &PodProxyBuffer{
		ctx:      context.Background(),
		drainCtx: drainCtx,
		stubId:   "stub",
	}

	if err := pb.ForwardContainerRequest(ctx, "container-1"); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get(echo.HeaderConnection); got != "close" {
		t.Fatalf("Connection header = %q, want close", got)
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want drain rejection before accounting", got)
	}
}

func TestForwardContainerRequestPinsMultipleContainersAndPorts(t *testing.T) {
	stubID := "e9c29586-c465-4a67-9c9b-25293d1ce77b"
	containerA := "sandbox-" + stubID + "-aaaa1111"
	containerB := "sandbox-" + stubID + "-bbbb2222"

	serverA8765 := sandboxBackend(t, "a-8765")
	serverA9000 := sandboxBackend(t, "a-9000")
	serverB8765 := sandboxBackend(t, "b-8765")

	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	if err := repo.SetContainerAddressMap(containerA, map[int32]string{
		8765: serverA8765.URL[len("http://"):],
		9000: serverA9000.URL[len("http://"):],
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetContainerAddressMap(containerB, map[int32]string{
		8765: serverB8765.URL[len("http://"):],
	}); err != nil {
		t.Fatal(err)
	}

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        stubID,
		stubConfig:    &types.StubConfigV1{},
		containerRepo: repo,
	}

	for _, tt := range []struct {
		name        string
		containerID string
		port        string
		wantBody    string
	}{
		{name: "container-a-first-port", containerID: containerA, port: "8765", wantBody: "a-8765"},
		{name: "container-a-second-port", containerID: containerA, port: "9000", wantBody: "a-9000"},
		{name: "container-b-same-port", containerID: containerB, port: "8765", wantBody: "b-8765"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/id", nil)
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetParamNames("port", "subPath")
			ctx.SetParamValues(tt.port, "id")

			if err := pb.ForwardContainerRequest(ctx, tt.containerID); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			if got := rec.Body.String(); got != tt.wantBody {
				t.Fatalf("body = %q, want %q", got, tt.wantBody)
			}
		})
	}

	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want released", got)
	}
	if got := pb.containerConnectionCount(containerA); got != 0 {
		t.Fatalf("container A connections = %d, want released", got)
	}
	if got := pb.containerConnectionCount(containerB); got != 0 {
		t.Fatalf("container B connections = %d, want released", got)
	}
}

func TestReserveContainerForPortSkipsContainersWithoutPort(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb := newPodProxyConnectionTestBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "wrong-port",
			addressMap:      map[int32]string{9000: "127.0.0.1:9000"},
			readyAddressMap: map[int32]string{9000: "127.0.0.1:9000"},
		},
		{
			id:              "right-port",
			addressMap:      map[int32]string{8000: "127.0.0.1:8000"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:8000"},
		},
	}

	c, ok, hasContainers, hasPort := pb.reserveContainerForPort(8000)
	if !ok {
		t.Fatalf("expected available container, hasContainers=%v hasPort=%v", hasContainers, hasPort)
	}
	if c.id != "right-port" {
		t.Fatalf("container id = %q, want right-port", c.id)
	}

	if connections := pb.containerConnectionCount("right-port"); connections != 1 {
		t.Fatalf("reserved local connections = %d, want 1", connections)
	}

	pb.flushConnectionState()
	connections, err := sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "right-port")
	if err != nil {
		t.Fatal(err)
	}
	if connections != 1 {
		t.Fatalf("published connections = %d, want 1", connections)
	}
}

func TestReserveContainerForPortSkipsWarmingPort(t *testing.T) {
	pb := &PodProxyBuffer{
		ctx:        context.Background(),
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		stubConfig: &types.StubConfigV1{},
		availableContainers: []container{
			{
				id:              "warming-port",
				addressMap:      map[int32]string{8000: "127.0.0.1:8000", 9000: "127.0.0.1:9000"},
				readyAddressMap: map[int32]string{9000: "127.0.0.1:9000"},
			},
			{
				id:              "ready-port",
				addressMap:      map[int32]string{8000: "127.0.0.1:8001"},
				readyAddressMap: map[int32]string{8000: "127.0.0.1:8001"},
			},
		},
	}

	c, ok, hasContainers, hasPort := pb.reserveContainerForPort(8000)
	if !ok {
		t.Fatalf("expected ready container, hasContainers=%v hasPort=%v", hasContainers, hasPort)
	}
	if c.id != "ready-port" {
		t.Fatalf("container id = %q, want ready-port", c.id)
	}

}

func TestPodBackendTransportReusesTransportForSameTarget(t *testing.T) {
	pb := &PodProxyBuffer{}
	target := types.BackendRouteAddress("machine:worker:container:container:8001")

	first := pb.backendTransport(target, containerDialTimeoutDurationS)
	second := pb.backendTransport(target, containerDialTimeoutDurationS)
	if first != second {
		t.Fatal("expected pod backend transport to be reused for same target")
	}

	pinned := pb.backendTransport(target, containerPinnedDialTimeout)
	if pinned == first {
		t.Fatal("expected pinned sandbox transport to be isolated from default pod transport")
	}

	routeA := types.BackendRouteAddress("machine:worker:container-a:container:8765")
	routeB := types.BackendRouteAddress("machine:worker:container-b:container:8765")
	if pb.backendTransport(routeA, containerPinnedDialTimeout) == pb.backendTransport(routeB, containerPinnedDialTimeout) {
		t.Fatal("expected different sandbox container routes to use different backend transports")
	}
}

func TestReadyAddressMapFiltersUnreadyPorts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	pb := &PodProxyBuffer{ctx: context.Background()}
	readyAddress := listener.Addr().String()
	ready := pb.readyAddressMap(map[int32]string{
		8000: readyAddress,
		9000: "127.0.0.1:1",
	})

	if got := ready[8000]; got != readyAddress {
		t.Fatalf("ready address = %q, want %q", got, readyAddress)
	}
	if _, ok := ready[9000]; ok {
		t.Fatal("unready port was included in ready address map")
	}
}

func TestReadyAddressMapOnlyWaitsForConfiguredPorts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	routeID := "machine:worker:container:container:2222"
	if err := repo.SetBackendRoute(context.Background(), types.BackendRoute{
		RouteID:   routeID,
		Port:      2222,
		Transport: types.BackendRouteTransportDirect,
		State:     types.BackendRouteStateOpening,
	}); err != nil {
		t.Fatal(err)
	}

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		stubConfig:    &types.StubConfigV1{Ports: []uint32{8000}},
		containerRepo: repo,
	}
	start := time.Now()
	ready := pb.readyAddressMap(map[int32]string{
		8000: listener.Addr().String(),
		2222: types.BackendRouteAddress(routeID),
	})

	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("configured port discovery took %s, want below 500ms", elapsed)
	}
	if got := ready[8000]; got != listener.Addr().String() {
		t.Fatalf("ready address = %q, want %q", got, listener.Addr().String())
	}
	if _, ok := ready[2222]; ok {
		t.Fatal("unconfigured management port was included in ready address map")
	}
}

func TestPrimeContainerPortMakesPortImmediatelyReservable(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerID := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345"
	if err := repo.SetContainerAddressMap(containerID, map[int32]string{
		8765: backend.URL[len("http://"):],
	}); err != nil {
		t.Fatal(err)
	}

	pb := &PodProxyBuffer{
		ctx:                 context.Background(),
		rdb:                 rdb,
		workspace:           &types.Workspace{Name: "workspace"},
		stubId:              "e9c29586-c465-4a67-9c9b-25293d1ce77b",
		proxyId:             uuid.NewString(),
		stubConfig:          &types.StubConfigV1{},
		containerRepo:       repo,
		availableContainers: []container{},
		workReady:           make(chan struct{}, 1),
	}

	if ok := pb.primeContainerPort(containerID, 8765, 100*time.Millisecond); !ok {
		t.Fatal("expected sandbox port prime to succeed")
	}

	c, ok, hasContainers, hasPort := pb.reserveContainerForPort(8765)
	if !ok {
		t.Fatalf("expected primed port to be reservable, hasContainers=%v hasPort=%v", hasContainers, hasPort)
	}
	if c.id != containerID {
		t.Fatalf("container id = %q, want %q", c.id, containerID)
	}
	if got := c.readyAddressMap[8765]; got == "" {
		t.Fatal("primed port was not marked ready")
	}
}

func TestPrimeContainerPortMissingPortDoesNotPublishContainer(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerID := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345"
	if err := repo.SetContainerAddressMap(containerID, map[int32]string{
		9000: "127.0.0.1:9000",
	}); err != nil {
		t.Fatal(err)
	}

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "e9c29586-c465-4a67-9c9b-25293d1ce77b",
		containerRepo: repo,
	}

	if ok := pb.primeContainerPort(containerID, 8765, 50*time.Millisecond); ok {
		t.Fatal("expected sandbox port prime to fail for a missing port")
	}
	if _, ok, hasContainers, hasPort := pb.reserveContainerForPort(8765); ok || hasContainers || hasPort {
		t.Fatalf("missing port was published, ok=%v hasContainers=%v hasPort=%v", ok, hasContainers, hasPort)
	}
}

func TestForwardContainerRequestTimesOutOpeningRouteQuickly(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerID := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345"
	routeID := "machine:worker:" + containerID + ":container:8765"
	if err := repo.SetContainerAddressMap(containerID, map[int32]string{
		8765: types.BackendRouteAddress(routeID),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetBackendRoute(context.Background(), types.BackendRoute{
		RouteID:     routeID,
		ContainerID: containerID,
		Kind:        types.BackendRouteKindContainer,
		Port:        8765,
		Transport:   types.BackendRouteTransportDirect,
		State:       types.BackendRouteStateOpening,
	}); err != nil {
		t.Fatal(err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port", "subPath")
	ctx.SetParamValues("8765", "")

	pb := &PodProxyBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "e9c29586-c465-4a67-9c9b-25293d1ce77b",
		stubConfig:    &types.StubConfigV1{},
		containerRepo: repo,
	}

	start := time.Now()
	if err := pb.ForwardContainerRequest(ctx, containerID); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 1200*time.Millisecond {
		t.Fatalf("forwarding opening route took %s, want bounded below 1.2s", elapsed)
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
	if got := pb.totalConnectionCount(); got != 0 {
		t.Fatalf("total connections = %d, want released", got)
	}
	if got := pb.containerConnectionCount(containerID); got != 0 {
		t.Fatalf("container connections = %d, want released", got)
	}
}

func TestSharedTotalConnectionsAcrossProxyBuffers(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb1 := newPodProxyConnectionTestBuffer(rdb)
	pb2 := newPodProxyConnectionTestBuffer(rdb)

	if _, err := pb1.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if _, err := pb2.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb1.flushConnectionState()
	pb2.flushConnectionState()

	if got, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub"); err != nil {
		t.Fatal(err)
	} else if got != 2 {
		t.Fatalf("total connections = %d, want shared count from both proxy buffers", got)
	}

	if err := pb1.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb1.flushConnectionState()
	if got, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub"); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Fatalf("total connections after first decrement = %d, want 1", got)
	}

	if err := pb1.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb1.flushConnectionState()
	if got, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub"); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Fatalf("total connections after duplicate first-buffer decrement = %d, want other buffer count preserved", got)
	}

	if err := pb2.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb2.flushConnectionState()
	if got, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub"); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("total connections after second decrement = %d, want 0", got)
	}

	if err := pb2.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb2.flushConnectionState()
	if got, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub"); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("total connections after extra decrement = %d, want capped zero", got)
	}
}

func TestSharedContainerConnectionsAcrossProxyBuffers(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb1 := newPodProxyConnectionTestBuffer(rdb)
	pb2 := newPodProxyConnectionTestBuffer(rdb)

	if err := pb1.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	if err := pb2.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	pb1.flushConnectionState()
	pb2.flushConnectionState()

	if got, err := sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "container-1"); err != nil {
		t.Fatal(err)
	} else if got != 2 {
		t.Fatalf("container connections = %d, want aggregate count from both proxy buffers", got)
	}

	if err := pb1.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	pb1.flushConnectionState()
	if got, err := sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "container-1"); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Fatalf("container connections after first decrement = %d, want 1", got)
	}
	if got, err := pb1.sharedContainerConnectionCount("container-1"); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Fatalf("first buffer shared container connections = %d, want remote aggregate count", got)
	}

	if err := pb2.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	pb2.flushConnectionState()
	if got, err := sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "container-1"); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("container connections after second decrement = %d, want 0", got)
	}
}

func TestConnectionCountersPublishBusySnapshotsBeforeFlush(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb := newPodProxyConnectionTestBuffer(rdb)

	if _, err := pb.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if err := pb.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}

	total, err := sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub")
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("total connections before periodic flush = %d, want busy snapshot", total)
	}

	containerTotal, err := sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if containerTotal != 1 {
		t.Fatalf("container connections before periodic flush = %d, want busy snapshot", containerTotal)
	}

	if err := pb.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if err := pb.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	pb.flushConnectionState()
	total, err = sharedPodTotalConnections(context.Background(), rdb, "workspace", "stub")
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatalf("total connections after flush = %d, want 0", total)
	}
	containerTotal, err = sharedPodContainerConnections(context.Background(), rdb, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if containerTotal != 0 {
		t.Fatalf("container connections after flush = %d, want 0", containerTotal)
	}
}

func TestSharedPodConnectionsIgnoreStaleAggregatesWithoutSnapshots(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	ctx := context.Background()

	if err := rdb.Set(ctx, Keys.podTotalConnections("workspace", "stub"), 1, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, Keys.podContainerConnections("workspace", "stub", "container-1"), 1, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	total, err := sharedPodTotalConnections(ctx, rdb, "workspace", "stub")
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatalf("total connections = %d, want 0 without live proxy snapshots", total)
	}

	containerTotal, err := sharedPodContainerConnections(ctx, rdb, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if containerTotal != 0 {
		t.Fatalf("container connections = %d, want 0 without live proxy snapshots", containerTotal)
	}
}

func TestFlushConnectionStatePublishesSnapshots(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb := newPodProxyConnectionTestBuffer(rdb)
	pb.availableContainers = []container{{id: "container-1"}}

	if _, err := pb.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if err := pb.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}

	pb.flushConnectionState()

	totalSnapshotKey := Keys.podProxyConnections("workspace", "stub", pb.proxyId, "total")
	if got := redisInt64(t, rdb, totalSnapshotKey); got != 1 {
		t.Fatalf("snapshot total connections = %d, want 1", got)
	}
	ttl, err := rdb.TTL(context.Background(), totalSnapshotKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > connectionSnapshotTTL {
		t.Fatalf("snapshot ttl = %s, want within %s", ttl, connectionSnapshotTTL)
	}
}

func TestFlushConnectionStateSkipsIdleSnapshots(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb := newPodProxyConnectionTestBuffer(rdb)

	pb.flushConnectionState()

	totalSnapshotKey := Keys.podProxyConnections("workspace", "stub", pb.proxyId, "total")
	if exists, err := rdb.Exists(context.Background(), totalSnapshotKey).Result(); err != nil {
		t.Fatal(err)
	} else if exists != 0 {
		t.Fatalf("idle snapshot exists = %d, want absent", exists)
	}

	if _, err := pb.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if err := pb.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	pb.flushConnectionState()
	if got := redisInt64(t, rdb, totalSnapshotKey); got != 0 {
		t.Fatalf("idle transition snapshot = %d, want 0", got)
	}
}

func TestDecrementContainerConnectionsDoesNotBlockOnKeepWarmPersistence(t *testing.T) {
	repo := &blockingKeepWarmRepo{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	pb := &PodProxyBuffer{
		containerRepo: repo,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		stubConfig:    &types.StubConfigV1{KeepWarmSeconds: 30},
		workReady:     make(chan struct{}, 1),
	}

	if err := pb.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		if err := pb.decrementContainerConnections("container-1"); err != nil {
			t.Errorf("decrement container connections: %v", err)
		}
		close(done)
	}()

	select {
	case <-repo.entered:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("keep-warm lock was not attempted")
	}

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("decrement blocked on keep-warm lock persistence")
	}
	if !pb.pendingKeepWarmLockExists("container-1") {
		t.Fatal("expected pending keep-warm marker while persistence is blocked")
	}

	close(repo.release)
	waitForCondition(t, time.Second, func() bool {
		return !pb.pendingKeepWarmLockExists("container-1")
	})
}

func TestStoppableContainersDoesNotUseStartedAtForFiniteKeepWarm(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerID := "pod-stub-container-1"

	if err := repo.SetContainerState(containerID, &types.ContainerState{
		ContainerId: containerID,
		StubId:      "stub",
		WorkspaceId: "workspace",
		Status:      types.ContainerStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateContainerStatus(containerID, types.ContainerStatusRunning, int64(types.ContainerStateTtlS)); err != nil {
		t.Fatal(err)
	}
	state, err := repo.GetContainerState(containerID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StartedAt == 0 {
		t.Fatal("expected running container to have StartedAt set")
	}

	instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Rdb:           rdb,
		IsActive:      true,
		ContainerRepo: repo,
		Workspace:     &types.Workspace{Name: "workspace"},
		Stub:          &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
		StubConfig:    &types.StubConfigV1{KeepWarmSeconds: 600},
	}}

	stoppable, err := instance.stoppableContainers()
	if err != nil {
		t.Fatal(err)
	}
	if len(stoppable) != 1 || stoppable[0] != containerID {
		t.Fatalf("stoppable containers = %v, want [%s]", stoppable, containerID)
	}
}

func TestStoppableContainersSkipsPendingKeepWarmLock(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	containerID := "pod-stub-container-1"

	if err := repo.SetContainerState(containerID, &types.ContainerState{
		ContainerId: containerID,
		StubId:      "stub",
		WorkspaceId: "workspace",
		Status:      types.ContainerStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	buffer := &PodProxyBuffer{}
	buffer.pendingKeepWarmLocks.Store(containerID, struct{}{})

	instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Rdb:           rdb,
		IsActive:      true,
		ContainerRepo: repo,
		Workspace:     &types.Workspace{Name: "workspace"},
		Stub:          &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
		StubConfig:    &types.StubConfigV1{KeepWarmSeconds: 600},
	}, buffer: buffer}

	stoppable, err := instance.stoppableContainers()
	if err != nil {
		t.Fatal(err)
	}
	if len(stoppable) != 0 {
		t.Fatalf("stoppable containers = %v, want none", stoppable)
	}
}

func TestProxyConnectionIndexRefreshIsThrottled(t *testing.T) {
	pb := &PodProxyBuffer{}
	now := time.Unix(0, 100)

	if !pb.shouldRefreshProxyConnectionIndex(now) {
		t.Fatal("expected first index refresh to publish")
	}
	if pb.shouldRefreshProxyConnectionIndex(now.Add(connectionIndexRefreshEvery / 2)) {
		t.Fatal("expected index refresh to be throttled before refresh interval")
	}
	if !pb.shouldRefreshProxyConnectionIndex(now.Add(connectionIndexRefreshEvery + time.Nanosecond)) {
		t.Fatal("expected index refresh after interval")
	}
}

func newPodProxyTestRedis(t *testing.T) *common.RedisClient {
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

func sandboxBackend(t *testing.T, body string) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/id" {
			t.Fatalf("backend path = %q, want /id", r.URL.Path)
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func newPodProxyConnectionTestBuffer(rdb *common.RedisClient) *PodProxyBuffer {
	return &PodProxyBuffer{
		ctx:        context.Background(),
		rdb:        rdb,
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		proxyId:    uuid.NewString(),
		stubConfig: &types.StubConfigV1{},
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

type blockingKeepWarmRepo struct {
	repository.ContainerRepository
	entered chan struct{}
	release chan struct{}
}

func (r *blockingKeepWarmRepo) SetPodKeepWarmLock(ctx context.Context, workspaceName, stubId, containerId string, keepWarmSeconds int) error {
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
