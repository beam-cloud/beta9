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
	"github.com/beam-cloud/beta9/pkg/types"
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

func TestReserveContainerForPortSkipsContainersWithoutPort(t *testing.T) {
	pb := &PodProxyBuffer{
		ctx:        context.Background(),
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		stubConfig: &types.StubConfigV1{},
		availableContainers: []container{
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

	if warmingConnections := pb.containerConnectionCount("warming-port"); warmingConnections != 0 {
		t.Fatalf("warming port local connections = %d, want 0", warmingConnections)
	}
}

func TestPodBackendProxyReusesTransportForSameTarget(t *testing.T) {
	pb := &PodProxyBuffer{}
	target := types.BackendRouteAddress("machine:worker:container:container:8001")

	first := pb.backendProxy(target)
	second := pb.backendProxy(target)
	if first != second {
		t.Fatal("expected pod backend proxy to be reused for same target")
	}
	if first.Transport == nil || first.Transport != second.Transport {
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

func TestTotalConnectionsAggregateAcrossProxyBuffers(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb1 := newPodProxyConnectionTestBuffer(rdb)
	pb2 := newPodProxyConnectionTestBuffer(rdb)

	if _, err := pb1.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if _, err := pb2.incrementTotalConnections(); err != nil {
		t.Fatal(err)
	}

	key := Keys.podTotalConnections("workspace", "stub")
	if got := redisInt64(t, rdb, key); got != 2 {
		t.Fatalf("total connections = %d, want aggregate count from both proxy buffers", got)
	}

	if err := pb1.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("total connections after first decrement = %d, want 1", got)
	}

	if err := pb1.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("total connections after duplicate first-buffer decrement = %d, want other buffer count preserved", got)
	}

	if err := pb2.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 0 {
		t.Fatalf("total connections after second decrement = %d, want 0", got)
	}

	if err := pb2.decrementTotalConnections(); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 0 {
		t.Fatalf("total connections after extra decrement = %d, want capped zero", got)
	}
}

func TestContainerConnectionsAggregateAcrossProxyBuffers(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb1 := newPodProxyConnectionTestBuffer(rdb)
	pb2 := newPodProxyConnectionTestBuffer(rdb)

	if err := pb1.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	if err := pb2.incrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}

	key := Keys.podContainerConnections("workspace", "stub", "container-1")
	if got := redisInt64(t, rdb, key); got != 2 {
		t.Fatalf("container connections = %d, want aggregate count from both proxy buffers", got)
	}

	if err := pb1.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("container connections after first decrement = %d, want 1", got)
	}
	if got := pb1.containerConnectionCount("container-1"); got != 0 {
		t.Fatalf("first buffer local container connections = %d, want 0", got)
	}
	if got := pb2.containerConnectionCount("container-1"); got != 1 {
		t.Fatalf("second buffer local container connections = %d, want 1", got)
	}

	if err := pb1.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 1 {
		t.Fatalf("container connections after duplicate first-buffer decrement = %d, want other buffer count preserved", got)
	}

	if err := pb2.decrementContainerConnections("container-1"); err != nil {
		t.Fatal(err)
	}
	if got := redisInt64(t, rdb, key); got != 0 {
		t.Fatalf("container connections after second decrement = %d, want 0", got)
	}
}

func TestRefreshRedisConnectionKeysExtendsCounterTTL(t *testing.T) {
	rdb := newPodProxyTestRedis(t)
	pb := newPodProxyConnectionTestBuffer(rdb)
	key := Keys.podTotalConnections("workspace", "stub")

	if err := rdb.Set(context.Background(), key, 1, time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	pb.refreshRedisConnectionKeys(key)

	ttl, err := rdb.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < podContainerConnectionTimeout-time.Second {
		t.Fatalf("ttl = %s, want refreshed close to %s", ttl, podContainerConnectionTimeout)
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
