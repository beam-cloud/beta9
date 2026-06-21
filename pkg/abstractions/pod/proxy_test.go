package pod

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
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
		addressMap:      map[int32]string{8000: "127.0.0.1:8000"},
		readyAddressMap: map[int32]string{8000: "127.0.0.1:8000"},
	}}
	pb.availableContainersLock.Unlock()
	pb.signalWork()

	waitForCondition(t, 50*time.Millisecond, func() bool {
		return pb.buffer.Len() == 0
	})
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

	first := pb.backendTransport(target)
	second := pb.backendTransport(target)
	if first != second {
		t.Fatal("expected pod backend transport to be reused for same target")
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
