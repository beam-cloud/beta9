package pod

import (
	"context"
	"crypto/tls"
	"errors"
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
	containerDiscoveryInterval       time.Duration = time.Millisecond * 250
	queuedContainerDiscoveryInterval time.Duration = time.Millisecond * 50
	containerDialTimeoutDurationS    time.Duration = time.Second * 30
	containerPinnedDialTimeout       time.Duration = time.Millisecond * 900
	containerAvailableTimeout        time.Duration = time.Second * 2
	containerPrimeTimeout            time.Duration = time.Second * 3
	connectionKeepAliveInterval      time.Duration = time.Second * 1
	connectionReadTimeout            time.Duration = time.Minute * 5
	backendDialRetryLimit                          = 1
)

type container struct {
	id              string
	addressMap      map[int32]string
	readyAddressMap map[int32]string
	connections     int
}

type connection struct {
	ctx              echo.Context
	tc               *tcpConnection
	done             chan struct{}
	finishOnce       sync.Once
	enqueuedAt       time.Time
	dialTimeout      time.Duration
	llm              *llmRequestInfo
	retryBackendDial bool
	retryCount       int
	state            atomic.Int32
}

const (
	connectionQueued int32 = iota
	connectionActive
	connectionFinished
)

type backendTransportKey struct {
	targetHost  string
	dialTimeout time.Duration
}

// backendDialError identifies failures that happened before a connection was
// established, so the request body has not been delivered to a backend.
type backendDialError struct {
	err error
}

func (e *backendDialError) Error() string { return e.err.Error() }
func (e *backendDialError) Unwrap() error { return e.err }

type preservedRequestBody struct {
	io.ReadCloser
}

func (preservedRequestBody) Close() error { return nil }

func (c *connection) backendDialTimeout() time.Duration {
	if c != nil && c.dialTimeout > 0 {
		return c.dialTimeout
	}
	return containerDialTimeoutDurationS
}

type PodProxyBuffer struct {
	ctx                     context.Context
	drainCtx                context.Context
	rdb                     *common.RedisClient
	workspace               *types.Workspace
	stubId                  string
	proxyId                 string
	size                    int
	containerRepo           repository.ContainerRepository
	keyEventManager         *common.KeyEventManager
	stubConfig              *types.StubConfigV1
	stubType                string
	appID                   string
	eventRepo               llmRouteEventPusher
	httpClient              *http.Client
	backendTransports       sync.Map
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	availableContainers     []container
	availableContainersLock sync.RWMutex
	totalConnections        atomic.Int64
	containerConnections    sync.Map
	pendingKeepWarmLocks    sync.Map
	llmMetricsRefreshAfter  sync.Map
	llmRouteCounter         atomic.Uint64
	idleSnapshotUntil       atomic.Int64
	proxyIndexRefreshAfter  atomic.Int64
	buffer                  *abstractions.RingBuffer[*connection]
	workReady               chan struct{}
	discoverReady           chan struct{}
	onBackendUnavailable    func() error
}

func NewPodProxyBuffer(ctx, drainCtx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	keyEventManager *common.KeyEventManager,
	stubConfig *types.StubConfigV1,
	stubType string,
	appID string,
	eventRepo llmRouteEventPusher,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
) *PodProxyBuffer {
	if drainCtx == nil {
		drainCtx = context.Background()
	}

	pb := &PodProxyBuffer{
		ctx:                     ctx,
		drainCtx:                drainCtx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		proxyId:                 uuid.NewString(),
		size:                    size,
		containerRepo:           containerRepo,
		keyEventManager:         keyEventManager,
		httpClient:              &http.Client{},
		stubConfig:              stubConfig,
		stubType:                stubType,
		appID:                   appID,
		eventRepo:               eventRepo,
		tailscale:               tailscale,
		tsConfig:                tsConfig,
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		buffer:                  abstractions.NewRingBuffer[*connection](size),
		workReady:               make(chan struct{}, 1),
		discoverReady:           make(chan struct{}, 1),
	}

	go pb.discoverContainers()
	go pb.processBuffer()
	go pb.syncConnectionState()

	return pb
}

func (pb *PodProxyBuffer) ForwardRequest(ctx echo.Context) error {
	ctx.Set("stubId", pb.stubId)

	if pb.isDraining() {
		return pb.failDrainingRequest(ctx)
	}

	if _, err := pb.incrementTotalConnections(); err != nil {
		return ctx.String(http.StatusServiceUnavailable, "Failed to connect to service")
	}
	defer pb.decrementTotalConnections()

	port, err := strconv.Atoi(ctx.Param("port"))
	if err != nil {
		return ctx.String(http.StatusBadRequest, "Invalid port")
	}

	llmInfo, err := pb.inspectLLMRequest(ctx)
	if err != nil {
		return ctx.String(http.StatusBadRequest, "Failed to inspect LLM request")
	}
	if denied, reason := pb.llmAdmissionDenied(llmInfo); denied {
		ctx.Response().Header().Set(echo.HeaderRetryAfter, "1")
		pb.pushRejectedLLMRouteEvent(llmInfo, http.StatusTooManyRequests, reason)
		return ctx.String(http.StatusTooManyRequests, reason)
	}

	done := make(chan struct{})
	conn := &connection{
		ctx:              ctx,
		done:             done,
		llm:              llmInfo,
		retryBackendDial: true,
	}

	if ctx.Request().Context().Err() != nil {
		return nil
	}

	container, ok, hasContainers, hasPort := pb.reserveContainerForRequest(int32(port), llmInfo)
	if ok {
		conn.claim()
		if pb.handleConnection(conn, container, int32(port)) {
			return pb.waitForConnection(conn, ctx.Request().Context().Done())
		}
		return nil
	}
	if hasContainers && !hasPort {
		return ctx.String(http.StatusServiceUnavailable, "Port not available")
	}

	conn.enqueuedAt = time.Now()
	pb.enqueueConnection(conn, false)
	pb.signalDiscovery()
	pb.signalWork()
	return pb.waitForConnection(conn, ctx.Request().Context().Done())
}

func (pb *PodProxyBuffer) ForwardContainerRequest(ctx echo.Context, containerId string) error {
	ctx.Set("stubId", pb.stubId)

	if pb.isDraining() {
		return pb.failDrainingRequest(ctx)
	}

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
	conn := &connection{ctx: ctx, done: done, dialTimeout: containerPinnedDialTimeout}
	conn.claim()
	pb.handleConnection(conn, container{
		id:         containerId,
		addressMap: addressMap,
	}, int32(port))

	return nil
}

func (pb *PodProxyBuffer) waitForConnection(conn *connection, requestDone <-chan struct{}) error {
	instanceDone := pb.ctx.Done()
	drainDone := pb.drainDone()
	for {
		select {
		case <-instanceDone:
			if pb.failQueuedConnection(conn, http.StatusServiceUnavailable, "Failed to connect to service") {
				return nil
			}
			instanceDone = nil
		case <-drainDone:
			if pb.failQueuedConnection(conn, http.StatusServiceUnavailable, "Service is draining") {
				return nil
			}
			drainDone = nil
		case <-conn.done:
			return nil
		case <-requestDone:
			return nil
		}
	}
}

func (pb *PodProxyBuffer) ForwardTCPRequest(tc *tcpConnection) error {
	if pb.isDraining() {
		tc.Conn.Close()
		return nil
	}

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
	pb.signalDiscovery()
	pb.signalWork()

	return pb.waitForConnection(conn, nil)
}

func (pb *PodProxyBuffer) processBuffer() {
	for {
		select {
		case <-pb.ctx.Done():
			pb.failQueuedConnections(http.StatusServiceUnavailable, "Failed to connect to service")
			return
		case <-pb.drainDone():
			pb.failQueuedConnections(http.StatusServiceUnavailable, "Service is draining")
			return
		case <-pb.workReady:
			for {
				conn, ok := pb.buffer.Pop()
				if !ok {
					break
				}
				pb.recordBufferOccupancy()
				if !conn.claim() {
					continue
				}

				if conn.tc != nil {
					if pb.isDraining() {
						pb.failConnection(conn, http.StatusServiceUnavailable, "Service is draining")
						continue
					}
					port := int32(conn.tc.Fields.Port)
					container, ok, hasContainers, hasPort := pb.reserveContainerForPort(port)
					if !ok {
						if !hasContainers || hasPort {
							pb.requeueConnection(conn)
							break
						} else {
							conn.finish()
							conn.tc.Conn.Close()
						}
						continue
					}

					go pb.handleTCPConnection(conn, container)
					continue
				}

				if conn.ctx.Request().Context().Err() != nil {
					conn.finish()
					continue
				}
				if pb.isDraining() {
					pb.failConnection(conn, http.StatusServiceUnavailable, "Service is draining")
					continue
				}

				port, err := strconv.Atoi(conn.ctx.Param("port"))
				if err != nil {
					conn.ctx.String(http.StatusBadRequest, "Invalid port")
					conn.finish()
					continue
				}

				container, ok, hasContainers, hasPort := pb.reserveContainerForRequest(int32(port), conn.llm)
				if !ok {
					if !hasContainers || hasPort {
						pb.requeueConnection(conn)
						break
					} else {
						conn.ctx.String(http.StatusServiceUnavailable, "Port not available")
						conn.finish()
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

func (pb *PodProxyBuffer) signalDiscovery() {
	if pb.discoverReady == nil {
		return
	}
	select {
	case pb.discoverReady <- struct{}{}:
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

func (pb *PodProxyBuffer) failQueuedConnection(conn *connection, status int, message string) bool {
	if conn == nil || !conn.claim() {
		return false
	}
	pb.failConnection(conn, status, message)
	return true
}

func (pb *PodProxyBuffer) failConnection(conn *connection, status int, message string) {
	if conn.tc != nil {
		conn.tc.Conn.Close()
	} else if conn.ctx != nil && !conn.ctx.Response().Committed {
		conn.ctx.Response().Header().Set(echo.HeaderConnection, "close")
		_ = conn.ctx.String(status, message)
	}
	pb.recordFailedQueuedLLMRoute(conn, status, message)
	conn.finish()
}

func (pb *PodProxyBuffer) failQueuedConnections(status int, message string) {
	for {
		conn, ok := pb.buffer.Pop()
		if !ok {
			break
		}
		pb.failQueuedConnection(conn, status, message)
	}
	pb.recordBufferOccupancy()
}

func (pb *PodProxyBuffer) failDrainingRequest(ctx echo.Context) error {
	ctx.Response().Header().Set(echo.HeaderConnection, "close")
	return ctx.String(http.StatusServiceUnavailable, "Service is draining")
}

func (pb *PodProxyBuffer) isDraining() bool {
	select {
	case <-pb.drainDone():
		return true
	default:
		return false
	}
}

func (pb *PodProxyBuffer) drainDone() <-chan struct{} {
	if pb == nil || pb.drainCtx == nil {
		return nil
	}
	return pb.drainCtx.Done()
}

func (c *connection) finish() {
	if c == nil || c.done == nil {
		return
	}
	c.state.Store(connectionFinished)
	c.finishOnce.Do(func() {
		close(c.done)
	})
}

func (c *connection) claim() bool {
	return c != nil && c.state.CompareAndSwap(connectionQueued, connectionActive)
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

func (pb *PodProxyBuffer) reserveContainerForRequest(port int32, llmInfo *llmRequestInfo) (container, bool, bool, bool) {
	if llmInfo != nil && llmInfo.Enabled {
		return pb.reserveLLMContainerForPort(port, llmInfo)
	}
	return pb.reserveContainerForPort(port)
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

func (pb *PodProxyBuffer) requeueConnection(conn *connection) bool {
	if conn == nil || !conn.state.CompareAndSwap(connectionActive, connectionQueued) {
		return false
	}
	pb.enqueueConnection(conn, true)
	return true
}

func (pb *PodProxyBuffer) handleTCPConnection(conn *connection, container container) {
	defer conn.finish()
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
		abstractions.SetConnOptions(podConn, true, connectionKeepAliveInterval, -1)
		abstractions.SetConnOptions(tc.Conn, true, connectionKeepAliveInterval, -1)
	} else if err != nil {
		tc.Conn.Close()
		return
	}
	idleDeadline := abstractions.NewConnIdleDeadline(connectionReadTimeout, tc.Conn, podConn)
	defer idleDeadline.Clear()

	isExpectedError := func(err error) bool {
		if err == nil || err == io.EOF {
			return true
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
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
		_, err := abstractions.CopyWithProxyBufferActivity(podConn, tc.Conn, idleDeadline.Refresh) // Client -> Pod
		if err != nil && !isExpectedError(err) {
			log.Warn().Err(err).Msg("error copying from client to pod")
		}

		if tcpConn, ok := podConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	go func() {
		defer wg.Done()

		_, err := abstractions.CopyWithProxyBufferActivity(tc.Conn, podConn, idleDeadline.Refresh) // Pod -> Client
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

func (pb *PodProxyBuffer) handleConnection(conn *connection, container container, port int32) (requeued bool) {
	defer func() {
		if !requeued {
			conn.finish()
		}
	}()
	var llmTracker *llmRequestTracker
	attemptReleased := false
	releaseAttempt := func() {
		if attemptReleased {
			return
		}
		attemptReleased = true
		if llmTracker != nil {
			llmTracker.finish()
		}
		_ = pb.decrementContainerConnections(container.id)
	}
	defer releaseAttempt()
	pb.recordQueuedRequestWait(conn, "http")

	request := conn.ctx.Request()

	targetHost, ok := container.addressMap[port]
	if !ok {
		conn.ctx.String(http.StatusServiceUnavailable, "Port not available")
		return false
	}
	llmTracker = pb.startLLMRequest(conn.llm, container.id, targetHost)

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
		return false
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error().Interface("recover", r).Str("stubId", pb.stubId).Str("workspace", pb.workspace.Name).Msg("handled abort in pod proxy")
		}
	}()

	proxy, err := pb.backendProxy(targetHost, conn.backendDialTimeout())
	if err != nil {
		conn.ctx.String(http.StatusInternalServerError, "Invalid target URL")
		return false
	}

	var retryErr error
	if llmTracker != nil {
		proxy.ModifyResponse = func(resp *http.Response) error {
			llmTracker.markFirstResponse(resp.StatusCode)
			return nil
		}
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		var dialErr *backendDialError
		if conn.retryBackendDial &&
			conn.retryCount < backendDialRetryLimit &&
			errors.As(err, &dialErr) &&
			req.Context().Err() == nil &&
			!conn.ctx.Response().Committed {
			retryErr = err
			return
		}
		if llmTracker != nil {
			llmTracker.markError(err.Error())
		}
		http.Error(rw, "Backend route unavailable", http.StatusBadGateway)
	}

	// The transport closes its outbound request body even when dialing fails.
	// Keep the inbound body open until the one safe pre-connect retry is decided.
	requestBody := request.Body
	func() {
		if conn.retryBackendDial && conn.retryCount < backendDialRetryLimit && requestBody != nil {
			request.Body = preservedRequestBody{ReadCloser: requestBody}
			defer func() { request.Body = requestBody }()
		}
		proxy.ServeHTTP(conn.ctx.Response(), request)
	}()

	if retryErr == nil {
		return false
	}
	if llmTracker != nil {
		llmTracker.markError(retryErr.Error())
	}
	releaseAttempt()
	if pb.retryBackendConnection(conn, container, retryErr) {
		return true
	}
	if !conn.ctx.Response().Committed {
		http.Error(conn.ctx.Response(), "Backend route unavailable", http.StatusBadGateway)
	}
	return false
}

func (pb *PodProxyBuffer) retryBackendConnection(conn *connection, failed container, err error) bool {
	if conn == nil || conn.ctx == nil || !conn.retryBackendDial || conn.retryCount >= backendDialRetryLimit || conn.ctx.Request().Context().Err() != nil {
		return false
	}

	pb.removeAvailableContainer(failed.id)
	conn.retryCount++
	if conn.enqueuedAt.IsZero() {
		conn.enqueuedAt = time.Now()
	}
	if !pb.requeueConnection(conn) {
		return false
	}

	log.Warn().
		Err(err).
		Str("container_id", failed.id).
		Str("stub_id", pb.stubId).
		Msg("backend unavailable before request delivery; requeueing request")
	pb.signalDiscovery()
	if pb.onBackendUnavailable != nil {
		if scaleErr := pb.onBackendUnavailable(); scaleErr != nil {
			log.Warn().Err(scaleErr).Str("stub_id", pb.stubId).Msg("failed to start replacement after backend became unavailable")
		}
	}
	pb.signalWork()
	return true
}

func (pb *PodProxyBuffer) removeAvailableContainer(containerID string) {
	if containerID == "" {
		return
	}

	pb.availableContainersLock.Lock()
	available := make([]container, 0, len(pb.availableContainers))
	removed := false
	for _, candidate := range pb.availableContainers {
		if candidate.id == containerID {
			removed = true
			continue
		}
		available = append(available, candidate)
	}
	if removed {
		pb.availableContainers = available
	}
	pb.availableContainersLock.Unlock()

	if removed {
		pb.pruneBackendTransports(available)
	}
}

func (pb *PodProxyBuffer) recordQueuedRequestWait(conn *connection, protocol string) {
	if conn.enqueuedAt.IsZero() {
		return
	}
	wait := time.Since(conn.enqueuedAt)
	if conn.llm != nil {
		conn.llm.QueueWait = wait
	}
	metrics.RecordProxyQueuedRequestWait("pod", pb.workspaceName(), pb.stubId, protocol, wait)
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
			} else {
				return nil, &backendDialError{err: err}
			}
			return conn, nil
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
		case <-pb.drainDone():
			return
		default:
		}

		containerStates, err := pb.containerRepo.GetActiveContainersByStubId(pb.stubId)
		if err == nil {

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
		}

		interval := containerDiscoveryInterval
		if pb.buffer != nil && pb.buffer.Len() > 0 {
			interval = queuedContainerDiscoveryInterval
		}

		timer := time.NewTimer(interval)
		select {
		case <-pb.ctx.Done():
			timer.Stop()
			return
		case <-pb.drainDone():
			timer.Stop()
			return
		case <-pb.discoverReady:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (pb *PodProxyBuffer) readyAddressMap(addressMap map[int32]string) map[int32]string {
	addressMap = pb.configuredAddressMap(addressMap)

	var wg sync.WaitGroup
	var mu sync.Mutex
	readyAddressMap := map[int32]string{}

	for port, address := range addressMap {
		wg.Add(1)
		go func(port int32, address string) {
			defer wg.Done()
			if !pb.checkContainerReady(address, containerAvailableTimeout) {
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

func (pb *PodProxyBuffer) configuredAddressMap(addressMap map[int32]string) map[int32]string {
	if pb.stubConfig == nil || len(pb.stubConfig.Ports) == 0 {
		return addressMap
	}

	configured := make(map[int32]string, len(pb.stubConfig.Ports))
	for _, port := range pb.stubConfig.Ports {
		if address, ok := addressMap[int32(port)]; ok {
			configured[int32(port)] = address
		}
	}
	return configured
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
