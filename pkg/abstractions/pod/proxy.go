package pod

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

const (
	containerDiscoveryInterval    time.Duration = time.Millisecond * 250
	containerDialTimeoutDurationS time.Duration = time.Second * 30
	containerPinnedDialTimeout    time.Duration = time.Millisecond * 900
	containerAvailableTimeout     time.Duration = time.Second * 2
	containerPrimeTimeout         time.Duration = time.Second * 3
	connectionKeepAliveInterval   time.Duration = time.Second * 1
	connectionReadTimeout         time.Duration = time.Minute * 5
)

type container struct {
	id              string
	addressMap      map[int32]string
	readyAddressMap map[int32]string
	connections     int
}

type connection struct {
	ctx         echo.Context
	tc          *tcpConnection
	done        chan struct{}
	enqueuedAt  time.Time
	dialTimeout time.Duration
}

type backendTransportKey struct {
	targetHost  string
	dialTimeout time.Duration
}

func (c *connection) backendDialTimeout() time.Duration {
	if c != nil && c.dialTimeout > 0 {
		return c.dialTimeout
	}
	return containerDialTimeoutDurationS
}

type PodProxyBuffer struct {
	ctx                     context.Context
	rdb                     *common.RedisClient
	workspace               *types.Workspace
	stubId                  string
	proxyId                 string
	size                    int
	containerRepo           repository.ContainerRepository
	keyEventManager         *common.KeyEventManager
	stubConfig              *types.StubConfigV1
	httpClient              *http.Client
	backendTransports       sync.Map
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	availableContainers     []container
	availableContainersLock sync.RWMutex
	totalConnections        atomic.Int64
	containerConnections    sync.Map
	buffer                  *abstractions.RingBuffer[*connection]
	workReady               chan struct{}
}

func NewPodProxyBuffer(ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	keyEventManager *common.KeyEventManager,
	stubConfig *types.StubConfigV1,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
) *PodProxyBuffer {
	pb := &PodProxyBuffer{
		ctx:                     ctx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		proxyId:                 uuid.NewString(),
		size:                    size,
		containerRepo:           containerRepo,
		keyEventManager:         keyEventManager,
		httpClient:              &http.Client{},
		stubConfig:              stubConfig,
		tailscale:               tailscale,
		tsConfig:                tsConfig,
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		buffer:                  abstractions.NewRingBuffer[*connection](size),
		workReady:               make(chan struct{}, 1),
	}

	go pb.discoverContainers()
	go pb.processBuffer()
	go pb.syncConnectionState()

	return pb
}

func (pb *PodProxyBuffer) ForwardRequest(ctx echo.Context) error {
	ctx.Set("stubId", pb.stubId)

	if _, err := pb.incrementTotalConnections(); err != nil {
		return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
	}
	defer pb.decrementTotalConnections()

	port, err := strconv.Atoi(ctx.Param("port"))
	if err != nil {
		return ctx.String(http.StatusBadRequest, "Invalid port")
	}

	done := make(chan struct{})
	conn := &connection{
		ctx:  ctx,
		done: done,
	}

	if ctx.Request().Context().Err() != nil {
		return nil
	}

	container, ok, hasContainers, hasPort := pb.reserveContainerForPort(int32(port))
	if ok {
		pb.handleConnection(conn, container, int32(port))
		return nil
	}
	if hasContainers && !hasPort {
		return ctx.String(http.StatusServiceUnavailable, "Port not available")
	}

	conn.enqueuedAt = time.Now()
	pb.enqueueConnection(conn, false)
	pb.signalWork()
	return pb.waitForConnection(ctx, conn)
}

func (pb *PodProxyBuffer) ForwardContainerRequest(ctx echo.Context, containerId string) error {
	ctx.Set("stubId", pb.stubId)

	if _, err := pb.incrementTotalConnections(); err != nil {
		return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
	}
	defer pb.decrementTotalConnections()

	port, err := strconv.Atoi(ctx.Param("port"))
	if err != nil {
		return ctx.String(http.StatusBadRequest, "Invalid port")
	}

	addressMap, err := pb.containerRepo.GetContainerAddressMap(containerId)
	if err != nil {
		return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
	}
	if _, ok := addressMap[int32(port)]; !ok {
		return ctx.String(http.StatusServiceUnavailable, "Port not available")
	}

	if err := pb.incrementContainerConnections(containerId); err != nil {
		return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
	}

	done := make(chan struct{})
	pb.handleConnection(&connection{ctx: ctx, done: done, dialTimeout: containerPinnedDialTimeout}, container{
		id:         containerId,
		addressMap: addressMap,
	}, int32(port))

	return nil
}

func (pb *PodProxyBuffer) waitForConnection(ctx echo.Context, conn *connection) error {
	for {
		select {
		case <-pb.ctx.Done():
			return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
		case <-conn.done:
			return nil
		case <-ctx.Request().Context().Done():
			return nil
		}
	}
}

func (pb *PodProxyBuffer) ForwardTCPRequest(tc *tcpConnection) error {
	if _, err := pb.incrementTotalConnections(); err != nil {
		tc.Conn.Close()
		return err
	}
	defer pb.decrementTotalConnections()

	done := make(chan struct{})
	conn := &connection{
		ctx:        nil,
		done:       done,
		tc:         tc,
		enqueuedAt: time.Now(),
	}

	pb.enqueueConnection(conn, false)
	pb.signalWork()

	for {
		select {
		case <-conn.done:
			return nil
		}
	}
}

func (pb *PodProxyBuffer) processBuffer() {
	for {
		select {
		case <-pb.ctx.Done():
			return
		case <-pb.workReady:
			for {
				conn, ok := pb.buffer.Pop()
				if !ok {
					break
				}
				pb.recordBufferOccupancy()

				if conn.tc != nil {
					port := int32(conn.tc.Fields.Port)
					container, ok, hasContainers, hasPort := pb.reserveContainerForPort(port)
					if !ok {
						if !hasContainers || hasPort {
							pb.requeueConnection(conn)
							break
						} else {
							close(conn.done)
							conn.tc.Conn.Close()
						}
						continue
					}

					go pb.handleTCPConnection(conn, container)
					continue
				}

				if conn.ctx.Request().Context().Err() != nil {
					close(conn.done)
					continue
				}

				port, err := strconv.Atoi(conn.ctx.Param("port"))
				if err != nil {
					conn.ctx.String(http.StatusBadRequest, "Invalid port")
					close(conn.done)
					continue
				}

				container, ok, hasContainers, hasPort := pb.reserveContainerForPort(int32(port))
				if !ok {
					if !hasContainers || hasPort {
						pb.requeueConnection(conn)
						break
					} else {
						conn.ctx.String(http.StatusServiceUnavailable, "Port not available")
						close(conn.done)
					}
					continue
				}

				go pb.handleConnection(conn, container, int32(port))
			}
		}
	}
}

func (pb *PodProxyBuffer) signalWork() {
	if pb.workReady == nil {
		return
	}
	select {
	case pb.workReady <- struct{}{}:
	default:
	}
}

func (pb *PodProxyBuffer) enqueueConnection(conn *connection, priority bool) {
	if overwritten, ok := pb.buffer.PushWithOverwrite(conn, priority); ok {
		metrics.RecordRingBufferOverwrite("pod", pb.workspaceName(), pb.stubId)
		pb.failQueuedConnection(overwritten, http.StatusServiceUnavailable, "Request queue full")
	}
	pb.recordBufferOccupancy()
}

func (pb *PodProxyBuffer) failQueuedConnection(conn *connection, status int, message string) {
	if conn == nil {
		return
	}
	if conn.tc != nil {
		conn.tc.Conn.Close()
	} else if conn.ctx != nil && !conn.ctx.Response().Committed {
		_ = conn.ctx.String(status, message)
	}
	if conn.done != nil {
		close(conn.done)
	}
}

func (pb *PodProxyBuffer) availableContainerSnapshot() []container {
	pb.availableContainersLock.RLock()
	defer pb.availableContainersLock.RUnlock()

	containers := make([]container, len(pb.availableContainers))
	copy(containers, pb.availableContainers)
	return containers
}

func (pb *PodProxyBuffer) hasAvailableContainers() bool {
	pb.availableContainersLock.RLock()
	defer pb.availableContainersLock.RUnlock()

	return len(pb.availableContainers) > 0
}

func (pb *PodProxyBuffer) reserveContainerForPort(port int32) (container, bool, bool, bool) {
	pb.availableContainersLock.RLock()
	defer pb.availableContainersLock.RUnlock()

	if len(pb.availableContainers) == 0 {
		return container{}, false, false, false
	}

	hasPort := false
	for _, c := range pb.availableContainers {
		if _, ok := c.addressMap[port]; !ok {
			continue
		}

		hasPort = true
		if _, ready := c.readyAddressMap[port]; !ready {
			continue
		}
		if err := pb.incrementContainerConnections(c.id); err == nil {
			return c, true, true, true
		}
	}

	return container{}, false, true, hasPort
}

func (pb *PodProxyBuffer) primeContainerPort(containerID string, port int32, timeout time.Duration) bool {
	if pb == nil || pb.containerRepo == nil || containerID == "" || port <= 0 {
		return false
	}

	addressMap, err := pb.containerRepo.GetContainerAddressMap(containerID)
	if err != nil || len(addressMap) == 0 {
		return false
	}

	address, ok := addressMap[port]
	if !ok || strings.TrimSpace(address) == "" {
		return false
	}
	if !pb.checkContainerAvailableWithTimeout(address, timeout) {
		return false
	}

	connections := int(pb.containerConnectionCount(containerID))
	if sharedConnections, err := pb.sharedContainerConnectionCount(containerID); err == nil && sharedConnections > connections {
		connections = sharedConnections
	}

	pb.availableContainersLock.Lock()
	next := make([]container, 0, len(pb.availableContainers)+1)
	updated := false
	for _, c := range pb.availableContainers {
		if c.id != containerID {
			next = append(next, c)
			continue
		}

		readyAddressMap := make(map[int32]string, len(c.readyAddressMap)+1)
		for readyPort, readyAddress := range c.readyAddressMap {
			readyAddressMap[readyPort] = readyAddress
		}
		readyAddressMap[port] = address

		c.addressMap = addressMap
		c.readyAddressMap = readyAddressMap
		c.connections = connections
		next = append(next, c)
		updated = true
	}
	if !updated {
		next = append(next, container{
			id:              containerID,
			addressMap:      addressMap,
			readyAddressMap: map[int32]string{port: address},
			connections:     connections,
		})
	}
	sort.Slice(next, func(i, j int) bool {
		return next[i].connections < next[j].connections
	})
	pb.availableContainers = next
	pb.availableContainersLock.Unlock()

	pb.signalWork()
	return true
}

func (pb *PodProxyBuffer) requeueConnection(conn *connection) {
	pb.enqueueConnection(conn, true)
}

func (pb *PodProxyBuffer) handleTCPConnection(conn *connection, container container) {
	defer close(conn.done)
	defer pb.decrementContainerConnections(container.id)

	tc := conn.tc
	pb.recordQueuedRequestWait(conn, "tcp")

	port := tc.Fields.Port
	targetHost, ok := container.addressMap[int32(port)]
	if !ok {
		log.Error().Uint32("port", port).Str("containerId", container.id).Str("stubId", pb.stubId).Msg("port not available in pod container")
		tc.Conn.Close()
		return
	}

	dialStart := time.Now()
	podConn, err := network.ConnectToBackend(pb.baseContext(), targetHost, conn.backendDialTimeout(), pb.tailscale, pb.tsConfig, pb.containerRepo)
	metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "tcp", err == nil, time.Since(dialStart))
	if err == nil {
		abstractions.SetConnOptions(podConn, true, connectionKeepAliveInterval, connectionReadTimeout)
	} else if err != nil {
		tc.Conn.Close()
		return
	}

	isExpectedError := func(err error) bool {
		if err == nil || err == io.EOF {
			return true
		}

		errStr := err.Error()

		return strings.Contains(errStr, "use of closed network connection") ||
			strings.Contains(errStr, "connection reset by peer") ||
			strings.Contains(errStr, "broken pipe")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := abstractions.CopyWithProxyBuffer(podConn, tc.Conn) // Client -> Pod
		if err != nil && !isExpectedError(err) {
			log.Warn().Err(err).Msg("error copying from client to pod")
		}

		if tcpConn, ok := podConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	go func() {
		defer wg.Done()

		_, err := abstractions.CopyWithProxyBuffer(tc.Conn, podConn) // Pod -> Client
		if err != nil && !isExpectedError(err) {
			log.Warn().Err(err).Msg("error copying from pod to client")
		}

		if tlsConn, ok := tc.Conn.(*tls.Conn); ok {
			tlsConn.CloseWrite()
		}
	}()

	wg.Wait()

	podConn.Close()
	tc.Conn.Close()
}

func (pb *PodProxyBuffer) handleConnection(conn *connection, container container, port int32) {
	defer close(conn.done)
	defer pb.decrementContainerConnections(container.id)
	pb.recordQueuedRequestWait(conn, "http")

	request := conn.ctx.Request()

	targetHost, ok := container.addressMap[port]
	if !ok {
		conn.ctx.String(http.StatusServiceUnavailable, "Port not available")
		return
	}

	subPath := conn.ctx.Param("subPath")
	if subPath != "" && subPath[0] != '/' {
		subPath = "/" + subPath
	} else if subPath == "" {
		subPath = "/"
	}

	request.URL.Scheme = "http"
	request.URL.Host = podBackendHost(targetHost)
	request.URL.Path = subPath

	// If it's a websocket request, upgrade the connection
	if websocket.IsWebSocketUpgrade(request) {
		pb.proxyWebSocket(conn, container, targetHost, subPath)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error().Interface("recover", r).Str("stubId", pb.stubId).Str("workspace", pb.workspace.Name).Msg("handled abort in pod proxy")
		}
	}()

	proxy, err := pb.backendProxy(targetHost, conn.backendDialTimeout())
	if err != nil {
		conn.ctx.String(http.StatusInternalServerError, "Invalid target URL")
		return
	}
	proxy.ServeHTTP(conn.ctx.Response(), request)
}

func (pb *PodProxyBuffer) recordQueuedRequestWait(conn *connection, protocol string) {
	if conn.enqueuedAt.IsZero() {
		return
	}
	metrics.RecordProxyQueuedRequestWait("pod", pb.workspaceName(), pb.stubId, protocol, time.Since(conn.enqueuedAt))
}

func podBackendHost(address string) string {
	if _, isRoute := types.ParseBackendRouteAddress(address); isRoute {
		return "backend.route"
	}
	return address
}

func (pb *PodProxyBuffer) backendProxy(targetHost string, dialTimeout time.Duration) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse(podBackendURL("http", targetHost, "", ""))
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = pb.backendTransport(targetHost, dialTimeout)
	proxy.BufferPool = abstractions.ProxyBufferPool{}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		http.Error(rw, "Backend route unavailable", http.StatusBadGateway)
	}
	return proxy, nil
}

func (pb *PodProxyBuffer) backendTransport(targetHost string, dialTimeout time.Duration) *http.Transport {
	if dialTimeout <= 0 {
		dialTimeout = containerDialTimeoutDurationS
	}
	key := backendTransportKey{targetHost: targetHost, dialTimeout: dialTimeout}
	if transport, ok := pb.backendTransports.Load(key); ok {
		return transport.(*http.Transport)
	}

	transport := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialAddress := addr
			if _, isRoute := types.ParseBackendRouteAddress(targetHost); isRoute {
				dialAddress = targetHost
			}
			start := time.Now()
			conn, err := network.ConnectToBackend(ctx, dialAddress, dialTimeout, pb.tailscale, pb.tsConfig, pb.containerRepo)
			metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "http", err == nil, time.Since(start))
			if err == nil {
				abstractions.SetConnOptions(conn, true, connectionKeepAliveInterval, connectionReadTimeout)
			}
			return conn, err
		},
	}

	actual, loaded := pb.backendTransports.LoadOrStore(key, transport)
	if loaded {
		transport.CloseIdleConnections()
		return actual.(*http.Transport)
	}
	return transport
}

func (pb *PodProxyBuffer) pruneBackendTransports(containers []container) {
	active := map[string]struct{}{}
	for _, c := range containers {
		for _, address := range c.readyAddressMap {
			active[address] = struct{}{}
		}
	}

	pb.backendTransports.Range(func(key, value any) bool {
		cacheKey, ok := key.(backendTransportKey)
		if !ok {
			pb.backendTransports.Delete(key)
			return true
		}
		if _, ok := active[cacheKey.targetHost]; ok {
			return true
		}
		value.(*http.Transport).CloseIdleConnections()
		pb.backendTransports.Delete(key)
		return true
	})
}

func (pb *PodProxyBuffer) proxyWebSocket(conn *connection, container container, addr string, path string) error {
	subprotocols := websocket.Subprotocols(conn.ctx.Request())

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins
		},
		Subprotocols: subprotocols,
	}

	clientConn, err := upgrader.Upgrade(conn.ctx.Response().Writer, conn.ctx.Request(), nil)
	if err != nil {
		return err
	}
	defer clientConn.Close()

	wsURL, err := url.Parse(podBackendURL("ws", addr, path, conn.ctx.Request().URL.RawQuery))
	if err != nil {
		return err
	}
	dstDialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, _, dialAddr string) (net.Conn, error) {
			dialAddress := dialAddr
			if _, isRoute := types.ParseBackendRouteAddress(addr); isRoute {
				dialAddress = addr
			}
			return network.ConnectToBackend(ctx, dialAddress, conn.backendDialTimeout(), pb.tailscale, pb.tsConfig, pb.containerRepo)
		},
		Subprotocols: subprotocols,
	}

	serverConn, _, err := dstDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return err
	}
	defer serverConn.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)

	proxyMessages := func(src, dst *websocket.Conn) {
		defer wg.Done()

		for {
			messageType, message, err := src.ReadMessage()
			if err != nil {
				break
			}
			if err := dst.WriteMessage(messageType, message); err != nil {
				break
			}
		}
	}

	go proxyMessages(clientConn, serverConn)
	go proxyMessages(serverConn, clientConn)

	wg.Wait()
	return nil
}

func podBackendURL(scheme, address, path, rawQuery string) string {
	host := address
	if _, isRoute := types.ParseBackendRouteAddress(address); isRoute {
		host = "backend.route"
	}

	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     "/" + strings.TrimPrefix(path, "/"),
		RawQuery: rawQuery,
	}
	return u.String()
}

func (pb *PodProxyBuffer) discoverContainers() {
	for {
		select {
		case <-pb.ctx.Done():
			return
		default:
			containerStates, err := pb.containerRepo.GetActiveContainersByStubId(pb.stubId)
			if err != nil {
				continue
			}

			var wg sync.WaitGroup
			availableContainersChan := make(chan container, len(containerStates))

			for _, containerState := range containerStates {
				wg.Add(1)

				go func(cs types.ContainerState) {
					defer wg.Done()
					if cs.Status != types.ContainerStatusRunning {
						return
					}

					addressMap, err := pb.containerRepo.GetContainerAddressMap(cs.ContainerId)
					if err != nil {
						return
					}

					readyAddressMap := pb.readyAddressMap(addressMap)
					if len(readyAddressMap) == 0 {
						return
					}

					connections, err := pb.sharedContainerConnectionCount(cs.ContainerId)
					if err != nil {
						return
					}

					availableContainersChan <- container{
						id:              cs.ContainerId,
						addressMap:      addressMap,
						readyAddressMap: readyAddressMap,
						connections:     connections,
					}
				}(containerState)
			}

			wg.Wait()
			close(availableContainersChan)

			// Collect available containers
			availableContainers := make([]container, 0)
			for c := range availableContainersChan {
				availableContainers = append(availableContainers, c)
			}

			// Sort availableContainers by # of connections (ascending)
			sort.Slice(availableContainers, func(i, j int) bool {
				return availableContainers[i].connections < availableContainers[j].connections
			})

			pb.availableContainersLock.Lock()
			pb.availableContainers = availableContainers
			pb.availableContainersLock.Unlock()
			pb.pruneBackendTransports(availableContainers)
			pb.signalWork()

			time.Sleep(containerDiscoveryInterval)
		}
	}
}

func (pb *PodProxyBuffer) readyAddressMap(addressMap map[int32]string) map[int32]string {
	var wg sync.WaitGroup
	var mu sync.Mutex
	readyAddressMap := map[int32]string{}

	for port, address := range addressMap {
		wg.Add(1)
		go func(port int32, address string) {
			defer wg.Done()
			if !pb.checkContainerAvailable(address) {
				return
			}

			mu.Lock()
			readyAddressMap[port] = address
			mu.Unlock()
		}(port, address)
	}

	wg.Wait()
	return readyAddressMap
}

// checkContainerAvailable checks if a container is available (meaning you can connect to it via a TCP dial)
func (pb *PodProxyBuffer) checkContainerAvailable(containerAddress string) bool {
	return pb.checkContainerAvailableWithTimeout(containerAddress, containerAvailableTimeout)
}

func (pb *PodProxyBuffer) checkContainerAvailableWithTimeout(containerAddress string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = containerAvailableTimeout
	}
	start := time.Now()
	conn, err := network.ConnectToBackend(pb.baseContext(), containerAddress, timeout, pb.tailscale, pb.tsConfig, pb.containerRepo)
	if err != nil {
		metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "discovery", false, time.Since(start))
		return false
	}
	defer conn.Close()
	metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "discovery", true, time.Since(start))
	return conn != nil
}

func (pb *PodProxyBuffer) baseContext() context.Context {
	if pb != nil && pb.ctx != nil {
		return pb.ctx
	}
	return context.Background()
}

func (pb *PodProxyBuffer) workspaceName() string {
	if pb.workspace == nil {
		return ""
	}
	return pb.workspace.Name
}

func (pb *PodProxyBuffer) recordBufferOccupancy() {
	if pb.buffer == nil {
		return
	}
	metrics.RecordRingBufferOccupancy("pod", pb.workspaceName(), pb.stubId, pb.buffer.Len(), pb.buffer.Capacity())
}
