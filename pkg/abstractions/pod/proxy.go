package pod

import (
	"context"
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
	"syscall"
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
	// Covers a dropped SYN (kernel retransmits at 1s) and relayed tunnel dials
	// that routinely exceed 900ms under concurrency.
	containerPinnedDialTimeout  time.Duration = time.Second * 3
	containerPinnedWait         time.Duration = 2 * containerPinnedDialTimeout
	containerPinnedPollInterval time.Duration = time.Millisecond * 100
	containerAvailableTimeout   time.Duration = time.Second * 2
	containerPrimeTimeout       time.Duration = time.Second * 3
	connectionKeepAliveInterval time.Duration = time.Second * 1
	connectionReadTimeout       time.Duration = time.Minute * 5
	backendRetryLimit                         = 1

	// How long readiness waits for a relay or port proxy to hang up.
	readinessHoldMin time.Duration = time.Millisecond * 20
	readinessHoldMax time.Duration = time.Millisecond * 250
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
	finishOnce  sync.Once
	enqueuedAt  time.Time
	dialTimeout time.Duration
	pinned      bool
	retryCount  int
	state       atomic.Int32
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
	httpClient              *http.Client
	backendTransports       sync.Map
	readiness               readinessCache
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	availableContainers     []container
	availableContainersLock sync.RWMutex
	retiringContainers      map[string]struct{}
	totalConnections        atomic.Int64
	containerConnections    sync.Map
	pendingKeepWarmLocks    sync.Map
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
	conn := &connection{
		ctx:         ctx,
		done:        done,
		dialTimeout: containerPinnedDialTimeout,
		pinned:      true,
	}
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
			if conn.cancelQueued() {
				return nil
			}
			// An active proxy goroutine owns the response writer. Wait for it
			// to observe request cancellation before net/http finalizes headers.
			requestDone = nil
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

				container, ok, hasContainers, hasPort := pb.reserveContainerForPort(int32(port))
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

func (c *connection) cancelQueued() bool {
	if c == nil || !c.state.CompareAndSwap(connectionQueued, connectionFinished) {
		return false
	}
	c.finishOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
	})
	return true
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
	} else {
		pb.readiness.invalidateTarget(readinessTarget{container.id, int32(port), targetHost})
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

		if tlsConn, ok := tc.Conn.(interface{ CloseWrite() error }); ok {
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
	attemptReleased := false
	releaseAttempt := func() {
		if attemptReleased {
			return
		}
		attemptReleased = true
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
	subPath := conn.ctx.Param("subPath")
	if subPath != "" && subPath[0] != '/' {
		subPath = "/" + subPath
	} else if subPath == "" {
		subPath = "/"
	}
	routePrefix := podPathPrefix(request.RequestURI, subPath)

	request.URL.Scheme = "http"
	request.URL.Host = podBackendHost(targetHost)
	request.URL.Path = subPath

	defer func() {
		if r := recover(); r != nil {
			log.Error().Interface("recover", r).Str("stubId", pb.stubId).Str("workspace", pb.workspace.Name).Msg("handled abort in pod proxy")
		}
	}()

	pinnedUntil := time.Now().Add(containerPinnedWait)
	for {
		var unanswered error
		if websocket.IsWebSocketUpgrade(request) {
			unanswered = pb.proxyWebSocket(conn, targetHost, subPath)
		} else {
			unanswered = pb.proxyHTTP(conn, targetHost, routePrefix)
		}
		if unanswered == nil {
			return false
		}

		pb.readiness.invalidateTarget(readinessTarget{container.id, port, targetHost})
		if conn.retryCount >= backendRetryLimit || request.Context().Err() != nil || conn.ctx.Response().Committed {
			break
		}
		conn.retryCount++
		log.Warn().
			Err(unanswered).
			Str("container_id", container.id).
			Str("stub_id", pb.stubId).
			Bool("pinned", conn.pinned).
			Msg("backend did not answer; retrying once it accepts connections")

		// A pinned request names one container, so there is nowhere to requeue it.
		if conn.pinned {
			if !pb.awaitBackend(request.Context(), port, targetHost, pinnedUntil) {
				break
			}
			continue
		}
		releaseAttempt()
		if pb.requeueUnanswered(conn, container) {
			return true
		}
		break
	}

	if !conn.ctx.Response().Committed && request.Context().Err() == nil {
		http.Error(conn.ctx.Response(), "Backend route unavailable", http.StatusBadGateway)
	}
	return false
}

// proxyHTTP makes one attempt. A resendable failure is returned unanswered.
func (pb *PodProxyBuffer) proxyHTTP(conn *connection, targetHost, routePrefix string) error {
	request := conn.ctx.Request()
	proxy, err := pb.backendProxy(targetHost, conn.backendDialTimeout())
	if err != nil {
		conn.ctx.String(http.StatusInternalServerError, "Invalid target URL")
		return nil
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		if location := resp.Header.Get("Location"); location != "" {
			resp.Header.Set("Location", rewriteRedirect(location, routePrefix, targetHost))
		}
		return nil
	}

	replayable := replayableRequest(request)
	var unanswered error
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if backendNeverAnswered(req.Context(), err, replayable) {
			unanswered = err
			return
		}
		http.Error(rw, "Backend route unavailable", http.StatusBadGateway)
	}

	// The transport closes the body even when dialing fails; keep it for a resend.
	if body := request.Body; body != nil {
		request.Body = preservedRequestBody{ReadCloser: body}
		defer func() { request.Body = body }()
	}
	proxy.ServeHTTP(conn.ctx.Response(), request)
	return unanswered
}

// backendNeverAnswered reports whether a failed attempt is safe to resend: the
// dial failed, or a relay or port proxy hung up on a bodiless idempotent request.
func backendNeverAnswered(ctx context.Context, err error, replayable bool) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var dialErr *backendDialError
	if errors.As(err, &dialErr) {
		return true
	}
	return replayable && connectionClosed(err)
}

func replayableRequest(request *http.Request) bool {
	switch request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return request.ContentLength == 0 && (request.Body == nil || request.Body == http.NoBody)
	}
	return false
}

func connectionClosed(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	// net/http and the tsnet stack report some closes only as text.
	message := err.Error()
	return strings.Contains(message, "server closed idle connection") ||
		strings.Contains(message, "connection reset by peer") ||
		strings.Contains(message, "broken pipe")
}

// awaitBackend waits until a pinned container's port passes readiness.
func (pb *PodProxyBuffer) awaitBackend(ctx context.Context, port int32, address string, deadline time.Time) bool {
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if pb.checkContainerReady(port, address, min(containerAvailableTimeout, remaining)) {
			return true
		}

		timer := time.NewTimer(min(containerPinnedPollInterval, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-pb.baseContext().Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

// requeueUnanswered drops the container from rotation and requeues the request first.
func (pb *PodProxyBuffer) requeueUnanswered(conn *connection, failed container) bool {
	pb.removeAvailableContainer(failed.id)
	if conn.enqueuedAt.IsZero() {
		conn.enqueuedAt = time.Now()
	}
	if !pb.requeueConnection(conn) {
		return false
	}

	pb.signalDiscovery()
	if pb.onBackendUnavailable != nil {
		if err := pb.onBackendUnavailable(); err != nil {
			log.Warn().Err(err).Str("stub_id", pb.stubId).Msg("failed to start replacement after backend became unavailable")
		}
	}
	pb.signalWork()
	return true
}

func (pb *PodProxyBuffer) removeAvailableContainer(containerID string) {
	if containerID == "" {
		return
	}
	pb.readiness.invalidate(containerID)

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

func (pb *PodProxyBuffer) retireAvailableContainer(containerID string) {
	if containerID == "" {
		return
	}
	pb.readiness.invalidate(containerID)

	pb.availableContainersLock.Lock()
	if pb.retiringContainers == nil {
		pb.retiringContainers = make(map[string]struct{})
	}
	pb.retiringContainers[containerID] = struct{}{}
	available := make([]container, 0, len(pb.availableContainers))
	for _, candidate := range pb.availableContainers {
		if candidate.id != containerID {
			available = append(available, candidate)
		}
	}
	pb.availableContainers = available
	pb.availableContainersLock.Unlock()

	pb.pruneBackendTransports(available)
}

func (pb *PodProxyBuffer) cancelContainerRetirement(containerID string) {
	if containerID == "" {
		return
	}

	pb.availableContainersLock.Lock()
	delete(pb.retiringContainers, containerID)
	pb.availableContainersLock.Unlock()
	pb.signalDiscovery()
}

func (pb *PodProxyBuffer) recordQueuedRequestWait(conn *connection, protocol string) {
	if conn.enqueuedAt.IsZero() {
		return
	}
	wait := time.Since(conn.enqueuedAt)
	metrics.RecordProxyQueuedRequestWait("pod", pb.workspaceName(), pb.stubId, protocol, wait)
}

func podBackendHost(address string) string {
	if _, isRoute := types.ParseBackendRouteAddress(address); isRoute {
		return "backend.route"
	}
	return address
}

// podPathPrefix is the part of the browser-facing path in front of subPath
// (`/pod/<name>/latest/<port>`); empty for subdomain-routed requests, whose
// raw request line already starts at subPath.
func podPathPrefix(requestURI, subPath string) string {
	raw, _, _ := strings.Cut(requestURI, "?")
	if subPath == "/" {
		return strings.TrimSuffix(raw, "/")
	}
	if strings.HasSuffix(raw, subPath) {
		return raw[:len(raw)-len(subPath)]
	}
	return ""
}

// rewriteRedirect keeps an app's redirects inside its path prefix: root-relative
// Locations and absolute ones addressed to the backend itself get the prefix.
func rewriteRedirect(location, prefix, backendHost string) string {
	if prefix == "" {
		return location
	}
	if u, err := url.Parse(location); err == nil && u.Host != "" {
		if u.Host != backendHost && u.Host != podBackendHost(backendHost) {
			return location
		}
		location = u.RequestURI()
	}
	if !strings.HasPrefix(location, "/") || strings.HasPrefix(location, "//") || strings.HasPrefix(location, prefix+"/") || location == prefix {
		return location
	}
	return prefix + location
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

// proxyWebSocket makes one attempt, like proxyHTTP.
func (pb *PodProxyBuffer) proxyWebSocket(conn *connection, addr string, path string) error {
	request := conn.ctx.Request()
	subprotocols := websocket.Subprotocols(request)

	wsURL, err := url.Parse(podBackendURL("ws", addr, path, request.URL.RawQuery))
	if err != nil {
		conn.ctx.String(http.StatusInternalServerError, "Invalid target URL")
		return nil
	}
	dstDialer := websocket.Dialer{
		ReadBufferSize:   64 << 10,
		WriteBufferSize:  64 << 10,
		HandshakeTimeout: conn.backendDialTimeout(),
		NetDialContext: func(ctx context.Context, _, dialAddr string) (net.Conn, error) {
			dialAddress := dialAddr
			if _, isRoute := types.ParseBackendRouteAddress(addr); isRoute {
				dialAddress = addr
			}
			backend, err := network.ConnectToBackend(ctx, dialAddress, conn.backendDialTimeout(), pb.tailscale, pb.tsConfig, pb.containerRepo)
			if err != nil {
				return nil, &backendDialError{err: err}
			}
			return backend, nil
		},
		Subprotocols: subprotocols,
	}

	// The backend is dialed before the client is upgraded: once the upgrade
	// has happened this handler can only answer a dead backend by slamming the
	// fresh socket shut, which the browser reports as a connection that never
	// worked. Before the upgrade a failure is still an ordinary HTTP error,
	// and a starting backend gets the same retry the HTTP path has.
	serverConn, _, err := dstDialer.DialContext(request.Context(), wsURL.String(), forwardedWebSocketHeaders(request))
	if err != nil {
		if backendNeverAnswered(request.Context(), err, true) {
			return err
		}
		http.Error(conn.ctx.Response(), "Backend route unavailable", http.StatusBadGateway)
		return nil
	}
	defer serverConn.Close()

	upgrader := websocket.Upgrader{
		ReadBufferSize:  64 << 10,
		WriteBufferSize: 64 << 10,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins
		},
		Subprotocols: subprotocols,
	}

	// Upgrade answers the client itself when it fails.
	clientConn, err := upgrader.Upgrade(conn.ctx.Response().Writer, request, nil)
	if err != nil {
		return nil
	}
	defer clientConn.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)

	// Close both sockets: ReadMessage consumes keepalive pings and can otherwise block forever.
	proxyMessages := func(src, dst *websocket.Conn) {
		defer wg.Done()
		defer src.Close()
		defer dst.Close()

		// Desktop frames can be megabytes. Stream them through a reusable
		// buffer instead of allocating and waiting for a complete frame.
		buffer := make([]byte, 64<<10)
		for {
			messageType, reader, err := src.NextReader()
			if err != nil {
				break
			}
			writer, err := dst.NextWriter(messageType)
			if err != nil {
				break
			}
			_, err = io.CopyBuffer(writer, reader, buffer)
			if err != nil {
				break
			}
			if err := writer.Close(); err != nil {
				break
			}
		}
	}

	go proxyMessages(clientConn, serverConn)
	go proxyMessages(serverConn, clientConn)

	wg.Wait()
	return nil
}

var webSocketHandshakeHeaders = []string{
	"Upgrade",
	"Connection",
	"Sec-Websocket-Key",
	"Sec-Websocket-Version",
	"Sec-Websocket-Extensions",
	"Sec-Websocket-Protocol",
}

func forwardedWebSocketHeaders(request *http.Request) http.Header {
	forwarded := request.Header.Clone()
	// Preserve the public host for backend origin checks.
	forwarded.Set("Host", request.Host)

	for _, header := range webSocketHandshakeHeaders {
		forwarded.Del(header)
	}
	return forwarded
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

			activeContainerIDs := make(map[string]struct{}, len(containerStates))
			var wg sync.WaitGroup
			availableContainersChan := make(chan container, len(containerStates))

			for _, containerState := range containerStates {
				activeContainerIDs[containerState.ContainerId] = struct{}{}
				if containerState.Status != types.ContainerStatusRunning {
					pb.readiness.invalidate(containerState.ContainerId)
					continue
				}
				wg.Add(1)

				go func(cs types.ContainerState) {
					defer wg.Done()
					addressMap, err := pb.containerRepo.GetContainerAddressMap(cs.ContainerId)
					if err != nil {
						return
					}

					readyAddressMap := pb.readyAddressMap(cs.ContainerId, addressMap)
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

			pb.updateDiscoveredContainers(availableContainers, activeContainerIDs)
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

func (pb *PodProxyBuffer) updateDiscoveredContainers(discovered []container, activeContainerIDs map[string]struct{}) {
	pb.readiness.prune(activeContainerIDs)
	pb.availableContainersLock.Lock()
	for containerID := range pb.retiringContainers {
		if _, active := activeContainerIDs[containerID]; !active {
			delete(pb.retiringContainers, containerID)
		}
	}

	available := discovered[:0]
	for _, candidate := range discovered {
		if _, retiring := pb.retiringContainers[candidate.id]; !retiring {
			available = append(available, candidate)
		}
	}
	pb.availableContainers = available
	pb.availableContainersLock.Unlock()

	pb.pruneBackendTransports(available)
	pb.signalWork()
}

func (pb *PodProxyBuffer) readyAddressMap(containerID string, addressMap map[int32]string) map[int32]string {
	addressMap = pb.configuredAddressMap(addressMap)
	pb.readiness.prunePorts(containerID, addressMap)
	foreground := pb.buffer != nil && pb.buffer.Len() > 0

	var wg sync.WaitGroup
	var mu sync.Mutex
	readyAddressMap := map[int32]string{}

	for port, address := range addressMap {
		wg.Add(1)
		go func(port int32, address string) {
			defer wg.Done()
			if !pb.readiness.check(readinessTarget{containerID, port, address}, foreground, func() bool {
				return pb.checkContainerReady(port, address, containerAvailableTimeout)
			}) {
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
	dialLatency := time.Since(start)
	if err != nil {
		metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "discovery", false, dialLatency)
		return false
	}
	defer conn.Close()
	metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, "discovery", true, dialLatency)
	return backendHoldsConnection(conn, dialLatency)
}

// backendHoldsConnection reports whether the far end kept the connection open or
// spoke first; relays and port proxies accept before reaching the backend.
func backendHoldsConnection(conn net.Conn, dialLatency time.Duration) bool {
	hold := min(max(2*dialLatency, readinessHoldMin), readinessHoldMax)
	if err := conn.SetReadDeadline(time.Now().Add(hold)); err != nil {
		return true
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	if n > 0 {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
