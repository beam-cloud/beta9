package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type request struct {
	ctx         echo.Context
	payload     *types.TaskPayload
	taskMessage *types.TaskMessage
	done        chan bool
}

type container struct {
	id               string
	address          string
	inFlightRequests int
}

type RequestBuffer struct {
	ctx        context.Context
	httpClient *http.Client
	tailscale  *network.Tailscale
	tsConfig   types.TailscaleConfig
	// tsClients               *common.SafeMap[*http.Client]
	stubId                  string
	stubConfig              *types.StubConfigV1
	workspace               *types.Workspace
	rdb                     *common.RedisClient
	containerRepo           repository.ContainerRepository
	buffer                  *abstractions.RingBuffer[request]
	availableContainers     []container
	availableContainersLock sync.RWMutex

	length atomic.Int32

	isASGI bool
}

func NewRequestBuffer(
	ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	stubConfig *types.StubConfigV1,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
	isASGI bool,
) *RequestBuffer {
	b := &RequestBuffer{
		ctx:                 ctx,
		rdb:                 rdb,
		workspace:           workspace,
		stubId:              stubId,
		stubConfig:          stubConfig,
		buffer:              abstractions.NewRingBuffer[request](size),
		availableContainers: []container{},

		availableContainersLock: sync.RWMutex{},
		containerRepo:           containerRepo,
		httpClient:              &http.Client{},
		length:                  atomic.Int32{},

		tailscale: tailscale,
		tsConfig:  tsConfig,

		isASGI: isASGI,
	}

	go b.discoverContainers()
	go b.processRequests()

	return b
}

func (rb *RequestBuffer) ForwardRequest(ctx echo.Context, payload *types.TaskPayload, taskMessage *types.TaskMessage) error {
	done := make(chan bool)
	rb.buffer.Push(request{
		ctx:         ctx,
		done:        done,
		payload:     payload,
		taskMessage: taskMessage,
	})

	rb.length.Add(1)
	defer func() {
		rb.length.Add(-1)
	}()

	for {
		select {
		case <-rb.ctx.Done():
			return nil
		case <-done:
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (rb *RequestBuffer) processRequests() {
	for {
		select {
		case <-rb.ctx.Done():
			return
		default:
			req, ok := rb.buffer.Pop()
			if !ok {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			if req.ctx.Request().Context().Err() != nil {
				// Context has been cancelled
				continue
			}

			go rb.handleHttpRequest(req)
		}
	}
}

func (rb *RequestBuffer) Length() int {
	return int(rb.length.Load())
}

func (rb *RequestBuffer) checkAddressIsReady(address string) bool {
	httpClient, err := rb.getHttpClient(address)
	if err != nil {
		return false
	}

	ctx, cancel := context.WithTimeout(rb.ctx, 1*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/health", address), nil)
	if err != nil {
		return false
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
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

					inFlightRequests, err := rb.requestsInFlight(cs.ContainerId)
					if err != nil {
						return
					}

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

			time.Sleep(1 * time.Second)
		}
	}
}

func (rb *RequestBuffer) requestsInFlight(containerId string) (int, error) {
	val, err := rb.rdb.Get(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Int()
	if err != nil && err != redis.Nil {
		return 0, err
	} else if err == redis.Nil {
		return 0, nil
	}

	return val, nil
}

func (rb *RequestBuffer) incrementRequestsInFlight(containerId string) error {
	err := rb.rdb.Incr(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Err()
	if err != nil {
		return err
	}

	err = rb.rdb.Expire(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId), time.Duration(EndpointRequestTimeoutS)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RequestBuffer) decrementRequestsInFlight(containerId string) error {
	err := rb.rdb.Decr(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Err()
	if err != nil {
		return err
	}

	err = rb.rdb.Expire(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId), time.Duration(EndpointRequestTimeoutS)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RequestBuffer) getHttpClient(address string) (*http.Client, error) {
	// If it isn't an tailnet address, just return the standard http client
	if !rb.tsConfig.Enabled || !strings.Contains(address, rb.tsConfig.HostName) {
		return rb.httpClient, nil
	}

	conn, err := network.ConnectToHost(rb.ctx, address, types.RequestTimeoutDurationS, rb.tailscale, rb.tsConfig)
	if err != nil {
		return nil, err
	}

	// Create a custom transport that uses the established connection
	// Either using tailscale or not
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return conn, nil
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}

func (rb *RequestBuffer) handleHttpRequest(req request) {
	rb.availableContainersLock.RLock()
	if len(rb.availableContainers) == 0 {
		rb.availableContainersLock.RUnlock()
		rb.buffer.Push(req)
		return
	}

	// Select an available container to forward the request to (whichever one has the lowest # of inflight requests)
	// Basically least-connections load balancing
	c := rb.availableContainers[0]
	rb.availableContainersLock.RUnlock()

	err := rb.incrementRequestsInFlight(c.id)
	if err != nil {
		return
	}

	request := req.ctx.Request()
	requestBody := request.Body
	if !rb.isASGI {
		b, err := json.Marshal(req.payload)
		if err != nil {
			return
		}
		requestBody = io.NopCloser(bytes.NewReader(b))
	}

	httpClient, err := rb.getHttpClient(c.address)
	if err != nil {
		return
	}

	containerUrl := fmt.Sprintf("http://%s/%s", c.address, req.ctx.Param("subPath"))
	httpReq, err := http.NewRequestWithContext(request.Context(), request.Method, containerUrl, requestBody)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		req.done <- true
		return
	}

	httpReq.Header.Add("X-TASK-ID", req.taskMessage.TaskId) // Add task ID to header
	go rb.heartBeat(req, c.id)                              // Send heartbeat via redis for duration of request

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		req.done <- true
		return
	}

	defer resp.Body.Close()
	defer rb.afterRequest(req, c.id)

	// Write response headers
	for key, values := range resp.Header {
		for _, value := range values {
			req.ctx.Response().Writer.Header().Add(key, value)
		}
	}

	// Write status code header
	req.ctx.Response().Writer.WriteHeader(resp.StatusCode)

	// Check if we can stream the response
	streamingSupported := true
	flusher, ok := req.ctx.Response().Writer.(http.Flusher)
	if !ok {
		streamingSupported = false
	}

	// Send response to client in chunks
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			req.ctx.Response().Writer.Write(buf[:n])

			if streamingSupported {
				flusher.Flush()
			}
		}

		if err != nil {
			if err != io.EOF {
				req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
					"error": "Internal server error",
				})
			}

			break
		}
	}
}

func (rb *RequestBuffer) heartBeat(req request, containerId string) {
	ctx := req.ctx.Request().Context()
	ticker := time.NewTicker(endpointRequestHeartbeatInterval)
	defer ticker.Stop()

	rb.rdb.Set(rb.ctx, Keys.endpointRequestHeartbeat(rb.workspace.Name, rb.stubId, req.taskMessage.TaskId), containerId, endpointRequestHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rb.rdb.Set(rb.ctx, Keys.endpointRequestHeartbeat(rb.workspace.Name, rb.stubId, req.taskMessage.TaskId), containerId, endpointRequestHeartbeatInterval)
		}
	}
}

func (rb *RequestBuffer) afterRequest(req request, containerId string) {
	defer func() {
		req.done <- true
	}()

	defer rb.decrementRequestsInFlight(containerId)

	// Set keep warm lock
	if rb.stubConfig.KeepWarmSeconds == 0 {
		return
	}
	rb.rdb.SetEx(
		context.Background(),
		Keys.endpointKeepWarmLock(rb.workspace.Name, rb.stubId, containerId),
		1,
		time.Duration(rb.stubConfig.KeepWarmSeconds)*time.Second,
	)
}
