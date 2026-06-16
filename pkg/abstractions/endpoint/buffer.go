package endpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	readyCheckInterval             time.Duration = 500 * time.Millisecond
	connectToHostTimeout           time.Duration = 2 * time.Second
	httpConnectionTimeout          time.Duration = 2 * time.Second
	checkAddressIsReadyTimeout     time.Duration = 2 * time.Second
	handleHttpRequestClientTimeout time.Duration = 175 * time.Second
	backendConnectTimeout          time.Duration = 10 * time.Second
)

type request struct {
	ctx        echo.Context
	task       *EndpointTask
	done       chan struct{}
	started    chan struct{}
	abandoned  atomic.Bool
	enqueuedAt time.Time
}

type container struct {
	id               string
	address          string
	inFlightRequests int
}

type backendTransportKey struct {
	address string
	timeout time.Duration
}

type RequestBuffer struct {
	ctx                     context.Context
	httpClient              *http.Client
	backendTransports       sync.Map
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	stubId                  string
	stubConfig              *types.StubConfigV1
	workspace               *types.Workspace
	rdb                     *common.RedisClient
	containerRepo           repository.ContainerRepository
	buffer                  *abstractions.RingBuffer[*request]
	availableContainers     []container
	availableContainersLock sync.RWMutex
	containerRequests       map[string]int
	containerRequestsLock   sync.Mutex
	maxTokens               int
	isASGI                  bool
	keyEventManager         *common.KeyEventManager
	keyEventChan            chan common.KeyEvent
	workReady               chan struct{}
}

func NewRequestBuffer(
	ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	keyEventManager *common.KeyEventManager,
	stubConfig *types.StubConfigV1,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
	isASGI bool,
) *RequestBuffer {
	rb := &RequestBuffer{
		ctx:                     ctx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		stubConfig:              stubConfig,
		buffer:                  abstractions.NewRingBuffer[*request](size),
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		containerRequests:       map[string]int{},
		containerRepo:           containerRepo,
		keyEventManager:         keyEventManager,
		keyEventChan:            make(chan common.KeyEvent),
		httpClient:              &http.Client{},
		tailscale:               tailscale,
		tsConfig:                tsConfig,
		maxTokens:               int(stubConfig.Workers),
		isASGI:                  isASGI,
		workReady:               make(chan struct{}, 1),
	}

	if stubConfig.ConcurrentRequests > 1 && isASGI {
		// Floor is set to the number of workers
		rb.maxTokens = max(int(stubConfig.ConcurrentRequests), rb.maxTokens)
	}

	go rb.discoverContainers()
	go rb.processRequests()

	// Listen for heartbeat key events
	go rb.keyEventManager.ListenForPattern(rb.ctx, Keys.endpointRequestHeartbeat(rb.workspace.Name, rb.stubId, "*", "*"), rb.keyEventChan)
	go rb.handleHeartbeatEvents()

	return rb
}

func (rb *RequestBuffer) handleHeartbeatEvents() {
	for {
		select {
		case event := <-rb.keyEventChan:
			operation := event.Operation

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet, common.KeyOperationDel, common.KeyOperationExpire:
				// Do nothing
			case common.KeyOperationExpired:
				if parts := strings.Split(event.Key, ":"); len(parts) >= 2 {
					taskId, containerId := parts[len(parts)-2], parts[len(parts)-1]
					if err := rb.releaseRequestToken(containerId, taskId); err == nil {
						rb.signalWork()
					}
				}
			}
		case <-rb.ctx.Done():
			return
		}
	}
}

func (rb *RequestBuffer) ForwardRequest(ctx echo.Context, task *EndpointTask) error {
	ctx.Set("stubId", rb.stubId)

	done := make(chan struct{})
	started := make(chan struct{})
	req := &request{
		ctx:        ctx,
		done:       done,
		started:    started,
		task:       task,
		enqueuedAt: time.Now(),
	}
	rb.enqueueRequest(req, false)
	rb.signalWork()

	waitTimer := time.NewTimer(rb.requestQueueTimeout())
	defer waitTimer.Stop()

	for {
		select {
		case <-rb.ctx.Done():
			return nil
		case <-ctx.Request().Context().Done():
			select {
			case <-started:
			default:
				req.abandoned.Store(true)
				rb.cancelInFlightTask(req.task, types.TaskRequestCancelled)
			}
			return nil
		case <-started:
			started = nil
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
		case <-waitTimer.C:
			req.abandoned.Store(true)
			rb.cancelInFlightTask(req.task, types.TaskExpired)
			ctx.JSON(http.StatusGatewayTimeout, map[string]interface{}{
				"error": "Timed out waiting for a backend container",
			})
			return nil
		case <-done:
			return nil
		}
	}
}

func (rb *RequestBuffer) requestQueueTimeout() time.Duration {
	if rb.stubConfig != nil && rb.stubConfig.TaskPolicy.Timeout > 0 {
		return time.Duration(rb.stubConfig.TaskPolicy.Timeout) * time.Second
	}
	return handleHttpRequestClientTimeout
}

func (rb *RequestBuffer) processRequests() {
	for {
		select {
		case <-rb.ctx.Done():
			return
		case <-rb.workReady:
			for {
				req, ok := rb.buffer.Pop()
				if !ok {
					break
				}
				rb.recordBufferOccupancy()

				if req.abandoned.Load() {
					rb.closeRequest(req)
					continue
				}

				if req.ctx.Request().Context().Err() != nil {
					rb.cancelInFlightTask(req.task, types.TaskRequestCancelled)
					rb.closeRequest(req)
					continue
				}

				c, ok := rb.reserveContainer()
				if !ok {
					rb.requeueRequest(req)
					break
				}

				go rb.handleRequest(req, c)
			}
		}
	}
}

func (rb *RequestBuffer) signalWork() {
	if rb.workReady == nil {
		return
	}
	select {
	case rb.workReady <- struct{}{}:
	default:
	}
}

func (rb *RequestBuffer) enqueueRequest(req *request, priority bool) {
	if overwritten, ok := rb.buffer.PushWithOverwrite(req, priority); ok {
		metrics.RecordRingBufferOverwrite("endpoint", rb.workspaceName(), rb.stubId)
		rb.failQueuedRequest(overwritten, http.StatusTooManyRequests, "Request queue full", types.TaskExpired)
	}
	rb.recordBufferOccupancy()
}

func (rb *RequestBuffer) failQueuedRequest(req *request, status int, message string, reason types.TaskCancellationReason) {
	if req == nil {
		return
	}

	req.abandoned.Store(true)
	rb.cancelInFlightTask(req.task, reason)
	if req.ctx != nil && !req.ctx.Response().Committed {
		_ = req.ctx.JSON(status, map[string]interface{}{
			"error": message,
		})
	}
	rb.closeRequest(req)
}

func (rb *RequestBuffer) closeRequest(req *request) {
	if req == nil || req.done == nil {
		return
	}
	close(req.done)
}

func (rb *RequestBuffer) availableContainerSnapshot() []container {
	rb.availableContainersLock.RLock()
	defer rb.availableContainersLock.RUnlock()

	containers := make([]container, len(rb.availableContainers))
	copy(containers, rb.availableContainers)
	return containers
}

func (rb *RequestBuffer) reserveContainer() (container, bool) {
	containers := rb.availableContainerSnapshot()
	if len(containers) == 0 {
		return container{}, false
	}

	// Containers are sorted by in-flight requests, so this is least-connections
	// load balancing. If a replica's shared token bucket is already full, try
	// the next least-loaded replica.
	for _, c := range containers {
		if rb.containerRequestCount(c.id) >= rb.effectiveMaxTokens() {
			continue
		}

		if err := rb.acquireRequestToken(c.id); err != nil {
			continue
		}

		selectedLoad := rb.incrementLocalContainerRequests(c.id)
		c.inFlightRequests = selectedLoad
		return c, true
	}

	metrics.RecordProxyTokenDenial("endpoint", rb.workspaceName(), rb.stubId)
	return container{}, false
}

func (rb *RequestBuffer) requeueRequest(req *request) {
	rb.enqueueRequest(req, true)
}

func (rb *RequestBuffer) containerRequestCount(containerId string) int {
	rb.containerRequestsLock.Lock()
	defer rb.containerRequestsLock.Unlock()

	if rb.containerRequests == nil {
		return 0
	}
	return rb.containerRequests[containerId]
}

func (rb *RequestBuffer) incrementLocalContainerRequests(containerId string) int {
	rb.containerRequestsLock.Lock()
	defer rb.containerRequestsLock.Unlock()

	if rb.containerRequests == nil {
		rb.containerRequests = map[string]int{}
	}
	rb.containerRequests[containerId]++
	return rb.containerRequests[containerId]
}

func (rb *RequestBuffer) decrementLocalContainerRequests(containerId string) bool {
	rb.containerRequestsLock.Lock()
	defer rb.containerRequestsLock.Unlock()

	if rb.containerRequests == nil || rb.containerRequests[containerId] <= 0 {
		return false
	}

	if rb.containerRequests[containerId] == 1 {
		delete(rb.containerRequests, containerId)
	} else {
		rb.containerRequests[containerId]--
	}
	return true
}

func (rb *RequestBuffer) pruneContainerRequestCounts(containers []container) {
	active := make(map[string]struct{}, len(containers))
	for _, c := range containers {
		active[c.id] = struct{}{}
	}

	rb.containerRequestsLock.Lock()
	defer rb.containerRequestsLock.Unlock()
	for containerId, count := range rb.containerRequests {
		if _, ok := active[containerId]; !ok && count <= 0 {
			delete(rb.containerRequests, containerId)
		}
	}
}

func (rb *RequestBuffer) checkAddressIsReady(address string) bool {
	httpClient := rb.getHttpClient(address, checkAddressIsReadyTimeout)

	start := time.Now()
	ctx, cancel := context.WithTimeout(rb.ctx, httpConnectionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", backendHTTPURL("http", address, "health", ""), nil)
	if err != nil {
		return false
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		metrics.RecordProxyBackendDialLatency("endpoint", rb.workspaceName(), rb.stubId, "http", false, time.Since(start))
		return false
	}
	defer resp.Body.Close()

	ready := resp.StatusCode == http.StatusOK
	metrics.RecordProxyBackendDialLatency("endpoint", rb.workspaceName(), rb.stubId, "http", ready, time.Since(start))
	return ready
}

func (rb *RequestBuffer) discoverContainers() {
	for {
		select {
		case <-rb.ctx.Done():
			return
		default:
			containerStates, err := rb.containerRepo.GetActiveContainersByStubId(rb.stubId)
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

					containerAddress, err := rb.containerRepo.GetContainerAddress(cs.ContainerId)
					if err != nil {
						return
					}

					availableTokens, err := rb.requestTokens(cs.ContainerId)
					if err != nil {
						return
					}

					// If a replica has five tokens and three are still available,
					// then two requests are currently in flight for that replica.
					inFlightRequests := rb.effectiveMaxTokens() - availableTokens

					if rb.checkAddressIsReady(containerAddress) {
						availableContainersChan <- container{
							id:               cs.ContainerId,
							address:          containerAddress,
							inFlightRequests: inFlightRequests,
						}

						return
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

			// Sort availableContainers by # of in-flight requests (ascending)
			sort.Slice(availableContainers, func(i, j int) bool {
				return availableContainers[i].inFlightRequests < availableContainers[j].inFlightRequests
			})

			rb.availableContainersLock.Lock()
			rb.availableContainers = availableContainers
			rb.availableContainersLock.Unlock()
			rb.pruneContainerRequestCounts(availableContainers)
			rb.pruneBackendTransports(availableContainers)
			rb.signalWork()

			time.Sleep(readyCheckInterval)
		}
	}
}

func (rb *RequestBuffer) requestTokens(containerId string) (int, error) {
	maxTokens := rb.effectiveMaxTokens()
	if rb.containerRepo == nil || rb.workspace == nil || rb.stubId == "" || containerId == "" {
		return maxTokens - rb.containerRequestCount(containerId), nil
	}

	return rb.containerRepo.GetEndpointRequestTokens(
		rb.ctx,
		rb.workspace.Name,
		rb.stubId,
		containerId,
		maxTokens,
		rb.requestTokenTTL(),
	)
}

func (rb *RequestBuffer) acquireRequestToken(containerId string) error {
	if rb.containerRepo == nil || rb.workspace == nil || rb.stubId == "" || containerId == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	acquired, err := rb.containerRepo.AcquireEndpointRequestToken(
		ctx,
		rb.workspace.Name,
		rb.stubId,
		containerId,
		rb.effectiveMaxTokens(),
		rb.requestTokenTTL(),
	)
	if err != nil {
		return err
	}

	// If no token was acquired, this replica has reached its request
	// concurrency threshold and the request should wait for another backend.
	if !acquired {
		metrics.RecordProxyTokenDenial("endpoint", rb.workspaceName(), rb.stubId)
		return errors.New("too many in-flight requests")
	}

	return nil
}

func (rb *RequestBuffer) releaseRequestToken(containerId, taskId string) error {
	rb.decrementLocalContainerRequests(containerId)

	if rb.containerRepo == nil || rb.workspace == nil || rb.stubId == "" || containerId == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return rb.containerRepo.ReleaseEndpointRequestToken(
		ctx,
		rb.workspace.Name,
		rb.stubId,
		containerId,
		taskId,
		rb.effectiveMaxTokens(),
		rb.requestTokenTTL(),
	)
}

func (rb *RequestBuffer) effectiveMaxTokens() int {
	if rb.maxTokens > 0 {
		return rb.maxTokens
	}
	return 1
}

func (rb *RequestBuffer) requestTokenTTL() time.Duration {
	if rb.stubConfig != nil && rb.stubConfig.TaskPolicy.Timeout > 0 {
		return time.Duration(rb.stubConfig.TaskPolicy.Timeout) * time.Second
	}
	return handleHttpRequestClientTimeout
}

func (rb *RequestBuffer) getHttpClient(address string, timeout time.Duration) *http.Client {
	// If it isn't an tailnet address, just return the standard http client
	if _, isRoute := types.ParseBackendRouteAddress(address); !isRoute && (!rb.tsConfig.Enabled || !strings.Contains(address, rb.tsConfig.HostName)) {
		return rb.httpClient
	}

	return &http.Client{
		Transport: rb.backendTransport(address, timeout),
		Timeout:   timeout,
	}
}

func (rb *RequestBuffer) backendTransport(address string, timeout time.Duration) *http.Transport {
	key := backendTransportKey{address: address, timeout: timeout}
	if transport, ok := rb.backendTransports.Load(key); ok {
		return transport.(*http.Transport)
	}

	transport := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: max(32, rb.maxTokens*2),
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialAddress := addr
			if _, isRoute := types.ParseBackendRouteAddress(address); isRoute {
				dialAddress = address
			}

			start := time.Now()
			conn, err := network.ConnectToBackend(ctx, dialAddress, backendDialTimeout(timeout), rb.tailscale, rb.tsConfig, rb.containerRepo)
			metrics.RecordDialTime(time.Since(start), dialAddress)
			metrics.RecordProxyBackendDialLatency("endpoint", rb.workspaceName(), rb.stubId, "http", err == nil, time.Since(start))
			return conn, err
		},
	}

	actual, loaded := rb.backendTransports.LoadOrStore(key, transport)
	if loaded {
		transport.CloseIdleConnections()
		return actual.(*http.Transport)
	}
	return transport
}

func (rb *RequestBuffer) pruneBackendTransports(containers []container) {
	if len(containers) == 0 {
		rb.backendTransports.Range(func(key, value any) bool {
			value.(*http.Transport).CloseIdleConnections()
			rb.backendTransports.Delete(key)
			return true
		})
		return
	}

	active := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		active[container.address] = struct{}{}
	}
	rb.backendTransports.Range(func(key, value any) bool {
		transportKey, ok := key.(backendTransportKey)
		if !ok {
			rb.backendTransports.Delete(key)
			return true
		}
		if _, ok := active[transportKey.address]; ok {
			return true
		}
		value.(*http.Transport).CloseIdleConnections()
		rb.backendTransports.Delete(key)
		return true
	})
}

func (rb *RequestBuffer) handleRequest(req *request, c container) {
	defer rb.afterRequest(req, c.id)

	if req.abandoned.Load() {
		return
	}

	if req.ctx.Request().Context().Err() != nil {
		rb.cancelInFlightTask(req.task, types.TaskRequestCancelled)
		return
	}

	close(req.started)
	go rb.heartBeat(req, c.id)
	protocol := "http"
	if req.ctx.IsWebSocket() {
		protocol = "ws"
	}
	metrics.RecordProxyQueuedRequestWait("endpoint", rb.workspaceName(), rb.stubId, protocol, time.Since(req.enqueuedAt))

	if req.ctx.IsWebSocket() {
		rb.handleWSRequest(req, c)
	} else {
		rb.handleHttpRequest(req, c)
	}
}

func (rb *RequestBuffer) workspaceName() string {
	if rb.workspace == nil {
		return ""
	}
	return rb.workspace.Name
}

func backendDialTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		return backendConnectTimeout
	}
	if requestTimeout < backendConnectTimeout {
		return requestTimeout
	}
	return backendConnectTimeout
}

func (rb *RequestBuffer) recordBufferOccupancy() {
	metrics.RecordRingBufferOccupancy("endpoint", rb.workspaceName(), rb.stubId, rb.buffer.Len(), rb.buffer.Capacity())
}

func (rb *RequestBuffer) handleWSRequest(req *request, c container) {
	dstDialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialAddress := addr
			if _, isRoute := types.ParseBackendRouteAddress(c.address); isRoute {
				dialAddress = c.address
			}
			return network.ConnectToBackend(ctx, dialAddress, backendDialTimeout(handleHttpRequestClientTimeout), rb.tailscale, rb.tsConfig, rb.containerRepo)
		},
	}

	err := rb.proxyWebsocketConnection(
		req,
		c,
		dstDialer,
		backendHTTPURL("ws", c.address, req.ctx.Param("subPath"), req.ctx.QueryString()),
	)
	if err != nil {
		return
	}
}

func (rb *RequestBuffer) handleHttpRequest(req *request, c container) {
	request := req.ctx.Request()

	var requestBody io.ReadCloser = request.Body
	if !rb.isASGI {
		payload, err := task.SerializeHttpPayload(req.ctx)
		if err != nil {
			if req.ctx.Request().Context().Err() == context.Canceled {
				rb.cancelInFlightTask(req.task, types.TaskRequestCancelled)
				return
			}

			req.ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": err.Error(),
			})

			rb.cancelInFlightTask(req.task, types.TaskInvalidRequestPayload)
			return
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			req.ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		requestBody = io.NopCloser(bytes.NewReader(payloadBytes))
	}

	httpClient := rb.getHttpClient(c.address, handleHttpRequestClientTimeout)
	containerUrl := backendHTTPURL("http", c.address, req.ctx.Param("subPath"), "")

	// Forward query params to the container if ASGI
	if rb.isASGI {
		containerUrl = backendHTTPURL("http", c.address, req.ctx.Param("subPath"), req.ctx.QueryString())
	}

	httpReq, err := http.NewRequestWithContext(request.Context(), request.Method, containerUrl, requestBody)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		return
	}

	// Copy headers to new request
	for key, values := range request.Header {
		for _, val := range values {
			httpReq.Header.Add(key, val)
		}
	}

	httpReq.Header.Add("X-TASK-ID", req.task.msg.TaskId) // Add task ID to header

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		if req.ctx.Request().Context().Err() == context.Canceled {
			rb.cancelInFlightTask(req.task, types.TaskRequestCancelled)
			return
		}
		req.ctx.JSON(http.StatusBadGateway, map[string]interface{}{
			"error": "Backend route unavailable",
		})
		return
	}
	defer resp.Body.Close()

	// Set response headers and status code before writing the body
	for key, values := range resp.Header {
		for _, value := range values {
			req.ctx.Response().Header().Add(key, value)
		}
	}
	req.ctx.Response().WriteHeader(resp.StatusCode)

	// Check if we can stream the response
	streamingSupported := true
	flusher, ok := req.ctx.Response().Writer.(http.Flusher)
	if !ok {
		streamingSupported = false
	}

	if streamingSupported && shouldFlushProxyResponse(resp) {
		_, err = abstractions.CopyWithProxyBufferFlush(req.ctx.Response().Writer, resp.Body, flusher.Flush)
	} else {
		_, err = abstractions.CopyWithProxyBuffer(req.ctx.Response().Writer, resp.Body)
	}
	if err != nil && err != io.EOF && err != context.Canceled {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
	}
}

func shouldFlushProxyResponse(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	if resp.ContentLength < 0 {
		return true
	}
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	return strings.Contains(contentType, "text/event-stream") || strings.Contains(contentType, "stream")
}

func backendHTTPURL(scheme, address, subPath, rawQuery string) string {
	host := address
	if _, isRoute := types.ParseBackendRouteAddress(address); isRoute {
		host = "backend.route"
	}

	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     "/" + strings.TrimPrefix(subPath, "/"),
		RawQuery: rawQuery,
	}
	return u.String()
}

func (rb *RequestBuffer) cancelInFlightTask(task *EndpointTask, reason types.TaskCancellationReason) {
	if task == nil {
		return
	}
	task.Cancel(context.Background(), reason)
}

func (rb *RequestBuffer) heartBeat(req *request, containerId string) {
	if rb.containerRepo == nil || rb.workspace == nil || req == nil || req.task == nil || req.task.msg == nil {
		return
	}

	ctx := req.ctx.Request().Context()
	ticker := time.NewTicker(endpointRequestHeartbeatInterval)
	defer ticker.Stop()

	select {
	case <-req.done:
		return
	default:
	}
	_ = rb.containerRepo.SetEndpointRequestHeartbeat(rb.ctx, rb.workspace.Name, rb.stubId, req.task.msg.TaskId, containerId, endpointRequestHeartbeatKeepAlive)
	rb.refreshRequestTokenTTL(containerId)
	for {
		select {
		case <-ctx.Done():
			return
		case <-rb.ctx.Done():
			return
		case <-req.done:
			return
		case <-ticker.C:
			_ = rb.containerRepo.SetEndpointRequestHeartbeat(rb.ctx, rb.workspace.Name, rb.stubId, req.task.msg.TaskId, containerId, endpointRequestHeartbeatKeepAlive)
			rb.refreshRequestTokenTTL(containerId)
		}
	}
}

func (rb *RequestBuffer) refreshRequestTokenTTL(containerId string) {
	if rb.containerRepo == nil || rb.workspace == nil || rb.stubId == "" || containerId == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = rb.containerRepo.RefreshEndpointRequestTokenTTL(ctx, rb.workspace.Name, rb.stubId, containerId, rb.requestTokenTTL())
}

func (rb *RequestBuffer) afterRequest(req *request, containerId string) {
	defer rb.signalWork()
	defer rb.releaseRequestToken(containerId, req.task.msg.TaskId)
	defer close(req.done)

	if rb.rdb == nil || rb.workspace == nil || rb.stubConfig.KeepWarmSeconds == 0 {
		return
	}

	go rb.rdb.SetEx(
		context.Background(),
		Keys.endpointKeepWarmLock(rb.workspace.Name, rb.stubId, containerId),
		1,
		time.Duration(rb.stubConfig.KeepWarmSeconds)*time.Second,
	)
}

func (rb *RequestBuffer) proxyWebsocketConnection(r *request, c container, dialer websocket.Dialer, dstAddress string) error {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			// Allow all origins
			return true
		},
	}

	w := r.ctx.Response().Writer
	req := r.ctx.Request()

	wsSrc, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return err
	}

	headers := http.Header{}
	headers.Add("X-TASK-ID", r.task.msg.TaskId) // Add task ID to header

	wsDst, resp, err := dialer.Dial(dstAddress, headers)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	go rb.heartBeat(r, c.id) // Send heartbeat via redis for duration of request
	go forwardWSConn(wsSrc.NetConn(), wsDst.NetConn())

	forwardWSConn(wsDst.NetConn(), wsSrc.NetConn())

	return nil
}

func forwardWSConn(src net.Conn, dst net.Conn) {
	defer func() {
		src.Close()
		dst.Close()
	}()

	_, err := abstractions.CopyWithProxyBuffer(src, dst)
	if err != nil {
		return
	}
}
